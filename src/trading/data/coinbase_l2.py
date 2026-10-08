"""Coinbase Level 2 recorders with replayable raw evidence.

Advanced Trade is the unauthenticated public feed. The legacy Exchange feed is
retained for replay compatibility, but its Level 2 channel now requires keys.
Transport interruptions always start a new snapshot epoch.
"""

import asyncio
import hashlib
import json
import os
import re
import ssl
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, TextIO
from uuid import uuid4

import certifi
import polars as pl
import websockets
import yaml
from pydantic import Field, model_validator
from websockets.exceptions import ConnectionClosed

from trading.configuration import StrictModel
from trading.data.quality import assess_orderbook
from trading.data.schema import frame_from_rows, require_utc

WS_URL = "wss://ws-feed.exchange.coinbase.com"
ADVANCED_WS_URL = "wss://advanced-trade-ws.coinbase.com"
CoinbaseSource = Literal["coinbase_exchange_public", "coinbase_advanced_trade_public"]


class BookIntegrityError(ValueError):
    """The current snapshot epoch cannot safely be used for research."""


class CoinbaseL2Config(StrictModel):
    """Bounded public feed recorder settings with no account credentials."""

    source: CoinbaseSource
    products: tuple[str, ...] = Field(min_length=1, max_length=8)
    duration_seconds: int = Field(default=60, ge=0)
    max_messages: int = Field(default=0, ge=0)
    stale_seconds: int = Field(default=15, ge=3)
    reconnect_seconds: float = Field(default=2.0, gt=0, le=60)
    proxy_mode: Literal["auto", "direct"] = "auto"
    checkpoint_updates: int = Field(default=100, ge=1)
    checkpoint_depth: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def valid_products(self) -> "CoinbaseL2Config":
        """Reject duplicate and malformed product IDs before opening a socket."""
        if len(set(self.products)) != len(self.products):
            raise ValueError("duplicate Coinbase product")
        if any(re.fullmatch(r"[A-Z0-9]{2,24}-[A-Z0-9]{2,12}", p) is None
               for p in self.products):
            raise ValueError("invalid Coinbase product ID")
        return self


def load_coinbase_l2_config(path: Path) -> CoinbaseL2Config:
    """Load a public feed specification from a safe YAML mapping."""
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Coinbase L2 config must be a YAML mapping")
    return CoinbaseL2Config.model_validate(value)


def verified_tls_context() -> ssl.SSLContext:
    """Trust public roots even when python.org macOS has no installed CA link.

    SSL_CERT_FILE can add a trusted local issuer without disabling hostname or
    certificate verification. The application never accepts an unverified peer.
    """
    bundle = certifi.where()
    context = ssl.create_default_context(cafile=bundle)
    extra = os.environ.get("SSL_CERT_FILE")
    if extra and Path(extra).resolve() != Path(bundle).resolve():
        context.load_verify_locations(cafile=extra)
    return context


def _positive_decimal(value: object, *, zero_allowed: bool = False) -> Decimal:
    """Parse exact exchange strings and reject NaN, negatives, and bad prices."""
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise BookIntegrityError(f"invalid decimal level value: {value}") from error
    if not parsed.is_finite() or parsed < 0 or (not zero_allowed and parsed == 0):
        raise BookIntegrityError(f"nonpositive or nonfinite level value: {value}")
    return parsed


def _event_time(value: object) -> datetime:
    """Parse a Coinbase engine timestamp without losing its UTC meaning."""
    if not isinstance(value, str):
        raise BookIntegrityError("update time is missing")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BookIntegrityError("invalid update time") from error
    require_utc(timestamp, "Coinbase update time")
    return timestamp


@dataclass(slots=True)
class L2Book:
    """One product's exact price-level state within a snapshot epoch."""

    product: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    ready: bool = False
    updates: int = 0
    last_exchange_time: datetime | None = None
    last_heartbeat_sequence: int | None = None
    last_l2_sequence: int | None = None

    @staticmethod
    def _validate_sides(bids: dict[Decimal, Decimal], asks: dict[Decimal, Decimal]) -> None:
        """Require a positive, two-sided, uncrossed executable book."""
        if not bids or not asks or max(bids) >= min(asks):
            raise BookIntegrityError("book is empty, one-sided, or crossed")

    def snapshot(self, message: dict[str, Any]) -> None:
        """Replace the complete book from a public Level 2 snapshot."""
        if message.get("product_id") != self.product:
            raise BookIntegrityError("snapshot product does not match book")
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for key, target in (("bids", bids), ("asks", asks)):
            levels = message.get(key)
            if not isinstance(levels, list):
                raise BookIntegrityError("snapshot levels must be a list")
            for level in levels:
                if not isinstance(level, list) or len(level) != 2:
                    raise BookIntegrityError("invalid snapshot level")
                price = _positive_decimal(level[0])
                size = _positive_decimal(level[1])
                if price in target:
                    raise BookIntegrityError("duplicate price in snapshot")
                target[price] = size
        self._validate_sides(bids, asks)
        self.bids, self.asks = bids, asks
        self.ready = True
        self.updates = 0
        self.last_exchange_time = None
        self.last_l2_sequence = None

    def update(self, message: dict[str, Any]) -> None:
        """Apply absolute level sizes atomically; zero removes a level."""
        if not self.ready or message.get("product_id") != self.product:
            raise BookIntegrityError("update arrived before matching snapshot")
        changed = message.get("changes")
        if not isinstance(changed, list) or not changed:
            raise BookIntegrityError("empty or malformed Level 2 update")
        exchange_time = _event_time(message.get("time"))
        if self.last_exchange_time is not None and exchange_time < self.last_exchange_time:
            raise BookIntegrityError("exchange update time regressed")
        bids, asks = self.bids.copy(), self.asks.copy()
        for level in changed:
            if not isinstance(level, list) or len(level) != 3 or level[0] not in {"buy", "sell"}:
                raise BookIntegrityError("malformed Level 2 change")
            target = bids if level[0] == "buy" else asks
            price = _positive_decimal(level[1])
            size = _positive_decimal(level[2], zero_allowed=True)
            if size == 0:
                target.pop(price, None)
            else:
                target[price] = size
        self._validate_sides(bids, asks)
        self.bids, self.asks = bids, asks
        self.updates += 1
        self.last_exchange_time = exchange_time

    def heartbeat(self, message: dict[str, Any]) -> None:
        """Detect a heartbeat regression without assuming contiguous L2 updates."""
        sequence = message.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise BookIntegrityError("invalid heartbeat sequence")
        if self.last_heartbeat_sequence is not None and sequence < self.last_heartbeat_sequence:
            raise BookIntegrityError("heartbeat sequence regressed")
        self.last_heartbeat_sequence = sequence

    def digest(self) -> str | None:
        """Hash every exact price and size in a complete book state."""
        if not self.ready:
            return None
        content = {
            "bids": [[str(price), str(size)] for price, size in sorted(self.bids.items())],
            "asks": [[str(price), str(size)] for price, size in sorted(self.asks.items())],
        }
        return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()

    def top_rows(self, received_at: datetime, depth: int) -> pl.DataFrame:
        """Normalize a bounded display/query checkpoint to the order-book schema."""
        if not self.ready:
            raise BookIntegrityError("cannot checkpoint before a snapshot")
        rows: list[dict[str, object]] = []
        for side, levels in (
            ("bid", sorted(self.bids.items(), reverse=True)[:depth]),
            ("ask", sorted(self.asks.items())[:depth]),
        ):
            for index, (price, size) in enumerate(levels, start=1):
                rows.append({
                    "exchange": "coinbase", "symbol": self.product.replace("-", "/"),
                    "observed_at": received_at,
                    "exchange_timestamp": self.last_exchange_time,
                    "side": side, "level": index,
                    "price": float(price), "size": float(size),
                })
        return frame_from_rows("orderbook", rows)


@dataclass(slots=True)
class AdvancedFeedState:
    """Validate observed connection-wide sequences and apply feed envelopes."""

    last_sequence_num: int | None = None
    last_heartbeat_counter: int | None = None

    def apply(
        self, payload: dict[str, Any], books: dict[str, L2Book]
    ) -> tuple[list[tuple[str, str]], int]:
        """Return applied (product, kind) events and heartbeat count."""
        channel = payload.get("channel")
        sequence = payload.get("sequence_num")
        if sequence is not None:
            if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
                raise BookIntegrityError("invalid Advanced Trade sequence")
            if (self.last_sequence_num is not None
                    and sequence != self.last_sequence_num + 1):
                raise BookIntegrityError("Advanced Trade connection sequence gap")
            self.last_sequence_num = sequence
        if channel not in {"l2_data", "heartbeats"}:
            return [], 0
        if sequence is None:
            raise BookIntegrityError(f"{channel} lacks a sequence")
        events = payload.get("events")
        if not isinstance(events, list) or not events:
            raise BookIntegrityError(f"{channel} has no events")
        if channel == "heartbeats":
            for event in events:
                if not isinstance(event, dict):
                    raise BookIntegrityError("malformed Advanced Trade heartbeat")
                counter = event.get("heartbeat_counter")
                if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
                    raise BookIntegrityError("invalid heartbeat counter")
                if (self.last_heartbeat_counter is not None
                        and counter != self.last_heartbeat_counter + 1):
                    raise BookIntegrityError("heartbeat counter gap or regression")
                self.last_heartbeat_counter = counter
            return [], len(events)
        exchange_time = _event_time(payload.get("timestamp"))
        applied: list[tuple[str, str]] = []
        for event in events:
            if not isinstance(event, dict):
                raise BookIntegrityError("malformed Advanced Trade L2 event")
            product = event.get("product_id")
            if not isinstance(product, str) or product not in books:
                continue
            book = books[product]
            kind = event.get("type")
            levels = event.get("updates")
            if kind not in {"snapshot", "update"} or not isinstance(levels, list) or not levels:
                raise BookIntegrityError("invalid Advanced Trade L2 event")
            bids: list[list[object]] = []
            asks: list[list[object]] = []
            changes: list[list[object]] = []
            for level in levels:
                if not isinstance(level, dict) or level.get("side") not in {"bid", "offer"}:
                    raise BookIntegrityError("invalid Advanced Trade price level")
                price, size = level.get("price_level"), level.get("new_quantity")
                side = "buy" if level["side"] == "bid" else "sell"
                if kind == "snapshot":
                    (bids if side == "buy" else asks).append([price, size])
                else:
                    changes.append([side, price, size])
            if kind == "snapshot":
                book.snapshot({"product_id": product, "bids": bids, "asks": asks})
                book.last_exchange_time = exchange_time
            else:
                book.update({"product_id": product, "time": payload["timestamp"],
                             "changes": changes})
            book.last_l2_sequence = sequence
            applied.append((product, str(kind)))
        return applied, 0


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Counts and final book hashes reconstructed from immutable raw messages."""

    messages: int
    snapshots: int
    updates: int
    heartbeats: int
    final_hashes: dict[str, str | None]
    integrity_errors: tuple[str, ...]


def replay_raw(
    path: Path, products: tuple[str, ...],
    source: CoinbaseSource = "coinbase_exchange_public",
) -> ReplayResult:
    """Rebuild snapshot epochs from raw JSONL without network or cached Parquet."""
    books = {product: L2Book(product) for product in products}
    advanced = AdvancedFeedState()
    counts = {"messages": 0, "snapshots": 0, "updates": 0, "heartbeats": 0}
    errors: list[str] = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            record = json.loads(line)
            event = record.get("event")
            if event == "connection_start":
                books = {product: L2Book(product) for product in products}
                advanced = AdvancedFeedState()
            if event != "message":
                continue
            counts["messages"] += 1
            raw_text = record.get("raw_text")
            if not isinstance(raw_text, str):
                errors.append(f"line {line_number}: missing raw text")
                continue
            try:
                payload = json.loads(raw_text)
            except json.JSONDecodeError:
                errors.append(f"line {line_number}: invalid feed JSON")
                continue
            if not isinstance(payload, dict):
                errors.append(f"line {line_number}: non-object payload")
                continue
            if source == "coinbase_advanced_trade_public":
                try:
                    applied, heartbeats = advanced.apply(payload, books)
                    counts["snapshots"] += sum(kind == "snapshot" for _, kind in applied)
                    counts["updates"] += sum(kind == "update" for _, kind in applied)
                    counts["heartbeats"] += heartbeats
                except BookIntegrityError as error:
                    errors.append(f"line {line_number}: {error}")
                continue
            kind = payload.get("type")
            product = payload.get("product_id")
            if not isinstance(product, str) or product not in books:
                continue
            try:
                if kind == "snapshot":
                    books[product].snapshot(payload)
                    counts["snapshots"] += 1
                elif kind == "l2update":
                    books[product].update(payload)
                    counts["updates"] += 1
                elif kind == "heartbeat":
                    books[product].heartbeat(payload)
                    counts["heartbeats"] += 1
            except BookIntegrityError as error:
                errors.append(f"line {line_number}: {error}")
    return ReplayResult(**counts, final_hashes={k: v.digest() for k, v in books.items()},
                        integrity_errors=tuple(errors))


class _CheckpointWriter:
    """Write bounded top-of-book snapshots to immutable Parquet segments."""

    def __init__(self, root: Path, segment_rows: int = 4_000) -> None:
        """Keep only a bounded batch of normalized rows in memory."""
        self.root = root
        self.segment_rows = segment_rows
        self.frames: list[pl.DataFrame] = []
        self.rows = 0
        self.paths: list[Path] = []

    def add(self, frame: pl.DataFrame) -> None:
        """Queue one checkpoint and flush when the bounded batch is full."""
        self.frames.append(frame)
        self.rows += frame.height
        if self.rows >= self.segment_rows:
            self.flush()

    def flush(self) -> None:
        """Atomically publish one compressed immutable Parquet segment."""
        if not self.frames:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"part-{len(self.paths):06d}.parquet"
        temporary = path.with_name(f".{path.name}.tmp")
        try:
            pl.concat(self.frames).write_parquet(temporary, compression="zstd")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        self.paths.append(path)
        self.frames.clear()
        self.rows = 0


@dataclass(frozen=True, slots=True)
class Recording:
    """Artifacts and quality verdict of one bounded or interrupted session."""

    raw_path: Path
    quality_path: Path
    checkpoint_paths: tuple[Path, ...]
    ok: bool


class CoinbaseL2Recorder:
    """Record public feed epochs, checkpoint books, and mark every interruption."""

    def __init__(self, config: CoinbaseL2Config, project_root: Path) -> None:
        """Bind a validated public subscription to one local project."""
        self.config = config
        self.root = project_root

    @staticmethod
    def _record_event(stream: TextIO, event: str, **payload: object) -> None:
        """Append and flush one time-stamped raw event before interpretation."""
        record = {"event": event, "received_at_utc": datetime.now(UTC).isoformat(), **payload}
        stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush()

    async def record(self) -> Recording:
        """Consume the public WebSocket until duration or message budget expires."""
        config = self.config
        session_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid4().hex[:8]
        raw_dir = self.root / "data/raw/coinbase_l2" / session_id
        raw_dir.mkdir(parents=True)
        raw_path = raw_dir / "messages.jsonl"
        checkpoint_writer = _CheckpointWriter(
            self.root / "data/processed/coinbase_l2" / session_id
        )
        quality_path = self.root / "research/reports" / f"coinbase-l2-{session_id}-quality.json"
        counts = {"messages": 0, "snapshots": 0, "updates": 0, "heartbeats": 0,
                  "checkpoints": 0, "reconnects": 0, "stale_timeouts": 0}
        interruptions: list[dict[str, str]] = []
        books = {product: L2Book(product) for product in config.products}
        advanced = AdvancedFeedState()
        started = datetime.now(UTC)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + config.duration_seconds if config.duration_seconds else None
        consecutive_failures = 0
        normal_stop = False
        fatal_error: str | None = None
        with raw_path.open("x", encoding="utf-8") as raw:
            try:
                while True:
                    if deadline is not None and loop.time() >= deadline:
                        normal_stop = True
                        break
                    if config.max_messages and counts["messages"] >= config.max_messages:
                        normal_stop = True
                        break
                    connection_id = uuid4().hex
                    books = {product: L2Book(product) for product in config.products}
                    advanced = AdvancedFeedState()
                    self._record_event(raw, "connection_start", connection_id=connection_id,
                                       source=config.source)
                    try:
                        opening_timeout = (
                            min(10.0, max(0.1, deadline - loop.time()))
                            if deadline is not None else 10.0
                        )
                        url = (ADVANCED_WS_URL if config.source == "coinbase_advanced_trade_public"
                               else WS_URL)
                        async with websockets.connect(
                            url, open_timeout=opening_timeout,
                            ping_interval=20, ping_timeout=20,
                            max_size=16_000_000,
                            proxy=True if config.proxy_mode == "auto" else None,
                            ssl=verified_tls_context() if url.startswith("wss://") else None,
                        ) as socket:
                            if config.source == "coinbase_advanced_trade_public":
                                subscriptions = [
                                    {"type": "subscribe", "product_ids": list(config.products),
                                     "channel": "level2"},
                                    {"type": "subscribe", "channel": "heartbeats"},
                                ]
                            else:
                                subscriptions = [{
                                    "type": "subscribe", "product_ids": list(config.products),
                                    "channels": ["level2", "heartbeat"],
                                }]
                            for subscribe in subscriptions:
                                await socket.send(json.dumps(subscribe))
                                self._record_event(raw, "subscription",
                                                   connection_id=connection_id,
                                                   payload=subscribe)
                            consecutive_failures = 0
                            while True:
                                remaining = deadline - loop.time() if deadline is not None else None
                                if remaining is not None and remaining <= 0:
                                    normal_stop = True
                                    break
                                if (config.max_messages
                                        and counts["messages"] >= config.max_messages):
                                    normal_stop = True
                                    break
                                timeout = (
                                    min(config.stale_seconds, remaining)
                                    if remaining else config.stale_seconds
                                )
                                try:
                                    text = await asyncio.wait_for(socket.recv(), timeout=timeout)
                                except TimeoutError:
                                    if deadline is not None and loop.time() >= deadline:
                                        normal_stop = True
                                        break
                                    counts["stale_timeouts"] += 1
                                    raise BookIntegrityError("feed receive timeout") from None
                                if not isinstance(text, str):
                                    raise BookIntegrityError("non-text feed message")
                                self._record_event(raw, "message", connection_id=connection_id,
                                                   raw_text=text)
                                counts["messages"] += 1
                                try:
                                    payload = json.loads(text)
                                except json.JSONDecodeError as error:
                                    raise BookIntegrityError("invalid feed JSON") from error
                                if not isinstance(payload, dict):
                                    raise BookIntegrityError("feed message is not an object")
                                kind, product = payload.get("type"), payload.get("product_id")
                                if kind == "error" or payload.get("channel") == "errors":
                                    raise BookIntegrityError(f"feed error: {payload}")
                                if config.source == "coinbase_advanced_trade_public":
                                    applied, heartbeat_count = advanced.apply(payload, books)
                                    counts["heartbeats"] += heartbeat_count
                                    received_at = datetime.now(UTC)
                                    for applied_product, applied_kind in applied:
                                        book = books[applied_product]
                                        if applied_kind == "snapshot":
                                            counts["snapshots"] += 1
                                            checkpoint_writer.add(book.top_rows(
                                                received_at, config.checkpoint_depth
                                            ))
                                            counts["checkpoints"] += 1
                                        elif applied_kind == "update":
                                            counts["updates"] += 1
                                            if book.updates % config.checkpoint_updates == 0:
                                                checkpoint_writer.add(book.top_rows(
                                                    received_at, config.checkpoint_depth
                                                ))
                                                counts["checkpoints"] += 1
                                    continue
                                if not isinstance(product, str) or product not in books:
                                    continue
                                book = books[product]
                                received_at = datetime.now(UTC)
                                if kind == "snapshot":
                                    book.snapshot(payload)
                                    counts["snapshots"] += 1
                                    checkpoint_writer.add(
                                        book.top_rows(received_at, config.checkpoint_depth)
                                    )
                                    counts["checkpoints"] += 1
                                elif kind == "l2update":
                                    book.update(payload)
                                    counts["updates"] += 1
                                    if book.updates % config.checkpoint_updates == 0:
                                        checkpoint_writer.add(
                                            book.top_rows(received_at, config.checkpoint_depth)
                                        )
                                        counts["checkpoints"] += 1
                                elif kind == "heartbeat":
                                    book.heartbeat(payload)
                                    counts["heartbeats"] += 1
                            self._record_event(raw, "connection_end", connection_id=connection_id,
                                               reason="normal_stop")
                            if normal_stop:
                                break
                    except (ImportError, ssl.SSLCertVerificationError) as error:
                        hint = (
                            "install locked dependencies with uv sync --locked"
                            if isinstance(error, ImportError)
                            else (
                                "check the TLS issuer chain and Python trust store, "
                                "or use a trusted network"
                            )
                        )
                        fatal_error = f"{type(error).__name__}: {error}; {hint}"
                        self._record_event(raw, "connection_end", connection_id=connection_id,
                                           reason=fatal_error)
                        interruptions.append({"at_utc": datetime.now(UTC).isoformat(),
                                              "reason": fatal_error})
                        break
                    except (OSError, ConnectionClosed, TimeoutError, BookIntegrityError) as error:
                        reason = f"{type(error).__name__}: {error}"
                        self._record_event(raw, "connection_end", connection_id=connection_id,
                                           reason=reason)
                        interruptions.append({"at_utc": datetime.now(UTC).isoformat(),
                                              "reason": reason})
                        counts["reconnects"] += 1
                        consecutive_failures += 1
                        if consecutive_failures >= 10:
                            break
                        remaining = deadline - loop.time() if deadline is not None else None
                        if remaining is not None and remaining <= 0:
                            normal_stop = True
                            break
                        await asyncio.sleep(min(config.reconnect_seconds, remaining)
                                            if remaining is not None else config.reconnect_seconds)
            finally:
                raw.flush()
                os.fsync(raw.fileno())
                checkpoint_writer.flush()
                replay = replay_raw(raw_path, config.products, config.source)
                raw_hash = hashlib.sha256(raw_path.read_bytes()).hexdigest()
                quality = {
                    "kind": "coinbase_level2", "source": config.source,
                    "generated_at_utc": datetime.now(UTC).isoformat(),
                    "started_at_utc": started.isoformat(),
                    "finished_at_utc": datetime.now(UTC).isoformat(),
                    "products": list(config.products), "counts": counts,
                    "proxy_mode": config.proxy_mode,
                    "fatal_error": fatal_error,
                    "interruptions": interruptions,
                    "raw_path": str(raw_path), "raw_sha256": raw_hash,
                    "checkpoint_parts": [
                        {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                        for path in checkpoint_writer.paths
                    ],
                    "final_book_sha256": {key: book.digest() for key, book in books.items()},
                    "final_l2_sequence": {
                        key: book.last_l2_sequence for key, book in books.items()
                    },
                    "replay_book_sha256": replay.final_hashes,
                    "replay_integrity_errors": list(replay.integrity_errors),
                    "normal_stop": normal_stop,
                    "ok": (
                        normal_stop and fatal_error is None
                        and all(book.ready for book in books.values())
                        and not interruptions and not replay.integrity_errors
                        and replay.final_hashes == {
                            key: book.digest() for key, book in books.items()
                        }
                    ),
                    "method_note": (
                        "Advanced Trade connection sequence and heartbeat counters checked "
                        "per epoch; checkpoint exchange time uses envelope timestamp while "
                        "raw per-level event times remain in JSONL"
                        if config.source == "coinbase_advanced_trade_public" else
                        "Exchange L2 has no per-update sequence; heartbeat gaps are not L2 gaps"
                    ),
                }
                quality_path.parent.mkdir(parents=True, exist_ok=True)
                quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True) + "\n")
        return Recording(
            raw_path, quality_path, tuple(checkpoint_writer.paths), bool(quality["ok"])
        )


def capture_rest_snapshot(product: str, project_root: Path, *, depth: int = 20) -> Recording:
    """Archive one public HTTPS L2 snapshot when a streaming feed is unavailable.

    This is a point observation, never a replacement for continuous WebSocket
    history. The exact response bytes and exchange sequence are retained.
    """
    if re.fullmatch(r"[A-Z0-9]{2,24}-[A-Z0-9]{2,12}", product) is None:
        raise ValueError("invalid Coinbase product ID")
    if not 1 <= depth <= 100:
        raise ValueError("snapshot depth must be in [1,100]")
    url = f"https://api.exchange.coinbase.com/products/{product}/book?level=2"
    result = subprocess.run(
        ["curl", "--fail", "--location", "--silent", "--show-error", "--retry", "2",
         "--max-time", "30", url], capture_output=True, timeout=100, check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"public Coinbase book request failed: {result.stderr.decode(errors='replace')}"
        )
    observed_at = datetime.now(UTC)
    session_id = observed_at.strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid4().hex[:8]
    stem = f"coinbase-l2-rest-{product.lower()}-{session_id}"
    raw_path = project_root / "data/raw/coinbase_l2_rest" / f"{stem}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(result.stdout)
    quality_path = project_root / "research/reports" / f"{stem}-quality.json"
    checkpoint_paths: tuple[Path, ...] = ()
    error_text: str | None = None
    sequence: int | None = None
    exchange_time: str | None = None
    levels: dict[str, int] = {}
    try:
        payload = json.loads(result.stdout)
        if not isinstance(payload, dict) or payload.get("auction_mode") is True:
            raise BookIntegrityError("REST book is malformed or in auction mode")
        sequence = payload.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise BookIntegrityError("REST book has no integer sequence")
        exchange_time = payload.get("time")
        timestamp = _event_time(exchange_time)
        converted: dict[str, object] = {"product_id": product}
        for side in ("bids", "asks"):
            rows = payload.get(side)
            if not isinstance(rows, list) or any(
                not isinstance(row, list) or len(row) < 2 for row in rows
            ):
                raise BookIntegrityError("invalid REST price levels")
            levels[side] = len(rows)
            converted[side] = [row[:2] for row in rows]
        book = L2Book(product)
        book.snapshot(converted)
        book.last_exchange_time = timestamp
        frame = book.top_rows(observed_at, depth)
        report = assess_orderbook(frame)
        if not report.ok:
            raise BookIntegrityError(f"normalized REST book failed quality: {report.issues}")
        checkpoint = project_root / "data/processed/coinbase_l2_rest" / f"{stem}.parquet"
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(checkpoint, compression="zstd")
        checkpoint_paths = (checkpoint,)
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        error_text = str(error)
    quality = {
        "kind": "coinbase_l2_rest", "ok": error_text is None,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "started_at_utc": observed_at.isoformat(),
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "products": [product], "sequence": sequence,
        "exchange_timestamp": exchange_time,
        "counts": {"messages": 1, "snapshots": int(error_text is None), "updates": 0,
                   "heartbeats": 0, "checkpoints": len(checkpoint_paths), "reconnects": 0},
        "levels": levels, "interruptions": [], "error": error_text,
        "raw_path": str(raw_path),
        "raw_sha256": hashlib.sha256(result.stdout).hexdigest(),
        "checkpoint_parts": [
            {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in checkpoint_paths
        ],
        "method_note": "Single HTTPS L2 observation; no historical or continuous coverage",
    }
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True) + "\n")
    return Recording(raw_path, quality_path, checkpoint_paths, bool(quality["ok"]))
