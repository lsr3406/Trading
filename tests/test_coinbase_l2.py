"""Offline book, replay, and WebSocket recording contract checks."""

import asyncio
import hashlib
import json
import ssl
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from websockets.asyncio.server import serve

from trading.data.coinbase_l2 import (
    AdvancedFeedState,
    BookIntegrityError,
    CoinbaseL2Config,
    CoinbaseL2Recorder,
    L2Book,
    capture_rest_snapshot,
    replay_raw,
    verified_tls_context,
)

SNAPSHOT = {"type": "snapshot", "product_id": "BTC-USD",
            "bids": [["100.00", "2.0"]], "asks": [["101.00", "3.0"]]}
UPDATE = {"type": "l2update", "product_id": "BTC-USD",
          "time": "2026-10-08T00:00:01Z",
          "changes": [["buy", "100.00", "4.0"], ["sell", "102.00", "1.0"]]}
ADVANCED_SNAPSHOT = {
    "channel": "l2_data", "sequence_num": 0,
    "timestamp": "2026-10-08T00:00:00.000000123Z",
    "events": [{"type": "snapshot", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "price_level": "100", "new_quantity": "2"},
        {"side": "offer", "price_level": "101", "new_quantity": "3"},
    ]}],
}
ADVANCED_UPDATE = {
    "channel": "l2_data", "sequence_num": 1,
    "timestamp": "2026-10-08T00:00:01.000000123Z",
    "events": [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "price_level": "100", "new_quantity": "4"},
    ]}],
}


def test_book_uses_absolute_levels_and_rejects_crossed_update() -> None:
    """Zero removes a level; invalid changes do not mutate a valid book."""
    book = L2Book("BTC-USD")
    with pytest.raises(BookIntegrityError, match="before matching snapshot"):
        book.update(UPDATE)
    book.snapshot(SNAPSHOT)
    book.update(UPDATE)
    assert book.bids[Decimal("100.00")] == Decimal("4.0")
    book.update({**UPDATE, "time": "2026-10-08T00:00:02Z",
                 "changes": [["sell", "102.00", "0"]]})
    assert Decimal("102.00") not in book.asks
    before = book.digest()
    with pytest.raises(BookIntegrityError, match="crossed"):
        book.update({**UPDATE, "time": "2026-10-08T00:00:03Z",
                     "changes": [["buy", "105", "1"]]})
    assert book.digest() == before
    book.heartbeat({"sequence": 100})
    book.heartbeat({"sequence": 105})  # Product-wide sequence is not a Level 2 sequence.
    with pytest.raises(BookIntegrityError, match="regressed"):
        book.heartbeat({"sequence": 99})
    rows = book.top_rows(datetime.now(UTC), 2)
    assert rows.height == 2 and rows["side"].to_list() == ["bid", "ask"]


def test_public_websocket_session_records_replayable_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fake public feed produces raw JSONL, Parquet, and matching replay hash."""
    async def scenario() -> None:
        async def feed(socket: object) -> None:
            request = json.loads(await socket.recv())  # type: ignore[attr-defined]
            assert request["channels"] == ["level2", "heartbeat"]
            for event in (SNAPSHOT, UPDATE,
                          {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 100},
                          {"type": "heartbeat", "product_id": "BTC-USD", "sequence": 107}):
                await socket.send(json.dumps(event))  # type: ignore[attr-defined]
            await socket.wait_closed()  # type: ignore[attr-defined]

        async with serve(feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            monkeypatch.setattr("trading.data.coinbase_l2.WS_URL", f"ws://127.0.0.1:{port}")
            config = CoinbaseL2Config.model_validate({
                "source": "coinbase_exchange_public", "products": ["BTC-USD"],
                "duration_seconds": 10, "max_messages": 4, "stale_seconds": 3,
                "checkpoint_updates": 1, "checkpoint_depth": 2,
            })
            recording = await CoinbaseL2Recorder(config, tmp_path).record()
            assert recording.ok
            assert len(recording.checkpoint_paths) == 1
            assert pl.read_parquet(recording.checkpoint_paths[0]).height == 5
            report = json.loads(recording.quality_path.read_text())
            assert report["counts"]["updates"] == 1
            assert report["interruptions"] == []
            replay = replay_raw(recording.raw_path, ("BTC-USD",))
            assert replay.final_hashes == report["final_book_sha256"]
            assert not replay.integrity_errors

    asyncio.run(scenario())


def test_rest_snapshot_archives_original_and_marks_point_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The HTTPS fallback retains exact response bytes and an explicit scope note."""
    payload = {
        "sequence": 123,
        "time": "2026-10-08T00:00:01Z",
        "bids": [["100", "2", 1]],
        "asks": [["101", "3", 2]],
    }
    original = json.dumps(payload, separators=(",", ":")).encode()

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        """Return a deterministic public response without making a network call."""
        return subprocess.CompletedProcess(args[0], 0, stdout=original, stderr=b"")

    monkeypatch.setattr("trading.data.coinbase_l2.subprocess.run", fake_run)
    recording = capture_rest_snapshot("BTC-USD", tmp_path)
    assert recording.ok
    assert recording.raw_path.read_bytes() == original
    assert len(recording.checkpoint_paths) == 1
    assert pl.read_parquet(recording.checkpoint_paths[0]).height == 2
    report = json.loads(recording.quality_path.read_text())
    assert report["sequence"] == 123
    assert report["raw_sha256"] == hashlib.sha256(original).hexdigest()
    assert report["counts"]["updates"] == 0
    assert "Single HTTPS" in report["method_note"]


def test_interrupted_feed_restarts_epoch_and_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dropped connection needs another snapshot and keeps a failed quality flag."""
    async def scenario() -> None:
        connections = 0

        async def feed(socket: object) -> None:
            nonlocal connections
            connections += 1
            await socket.recv()  # type: ignore[attr-defined]
            await socket.send(json.dumps(SNAPSHOT))  # type: ignore[attr-defined]
            if connections == 2:
                await socket.send(json.dumps(UPDATE))  # type: ignore[attr-defined]
                await socket.wait_closed()  # type: ignore[attr-defined]

        async with serve(feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            monkeypatch.setattr("trading.data.coinbase_l2.WS_URL", f"ws://127.0.0.1:{port}")
            config = CoinbaseL2Config.model_validate({
                "source": "coinbase_exchange_public", "products": ["BTC-USD"],
                "duration_seconds": 10, "max_messages": 3, "stale_seconds": 3,
                "reconnect_seconds": 0.01,
            })
            recording = await CoinbaseL2Recorder(config, tmp_path).record()
            report = json.loads(recording.quality_path.read_text())
            assert connections == 2
            assert not recording.ok
            assert report["counts"]["reconnects"] == 1
            assert report["counts"]["snapshots"] == 2
            assert report["interruptions"]
            assert replay_raw(recording.raw_path, ("BTC-USD",)).final_hashes == (
                report["final_book_sha256"]
            )

    asyncio.run(scenario())


def test_missing_proxy_dependency_yields_quality_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing SOCKS helper fails once with an actionable report."""
    def missing_dependency(*args: object, **kwargs: object) -> None:
        """Simulate the optional dependency failure before a socket opens."""
        raise ImportError("connecting through a SOCKS proxy requires python-socks")

    monkeypatch.setattr("trading.data.coinbase_l2.websockets.connect", missing_dependency)
    config = CoinbaseL2Config.model_validate({
        "source": "coinbase_exchange_public", "products": ["BTC-USD"],
        "duration_seconds": 60, "proxy_mode": "auto",
    })
    recording = asyncio.run(CoinbaseL2Recorder(config, tmp_path).record())
    report = json.loads(recording.quality_path.read_text())
    assert not recording.ok
    assert report["fatal_error"].startswith("ImportError: connecting through a SOCKS proxy")
    assert "uv sync --locked" in report["fatal_error"]
    assert report["counts"]["reconnects"] == 0
    assert report["counts"]["messages"] == 0


def test_verified_tls_context_requires_trusted_roots() -> None:
    """The macOS fallback keeps certificate and hostname verification on."""
    context = verified_tls_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert context.get_ca_certs()


def test_advanced_sequence_includes_subscription_messages() -> None:
    """The observed feed sequence spans L2 events and subscription responses."""
    books = {"BTC-USD": L2Book("BTC-USD")}
    feed = AdvancedFeedState()
    assert feed.apply(ADVANCED_SNAPSHOT, books)[0] == [("BTC-USD", "snapshot")]
    assert feed.apply(ADVANCED_UPDATE, books)[0] == [("BTC-USD", "update")]
    assert feed.apply({"channel": "subscriptions", "sequence_num": 2}, books) == ([], 0)
    assert feed.apply({"channel": "heartbeats", "sequence_num": 3,
                       "events": [{"heartbeat_counter": 20}]}, books) == ([], 1)
    with pytest.raises(BookIntegrityError, match="connection sequence gap"):
        feed.apply({**ADVANCED_UPDATE, "sequence_num": 5}, books)
    assert books["BTC-USD"].bids[Decimal("100")] == Decimal("4")


def test_advanced_public_websocket_replays_exact_book(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A public Advanced Trade stream works without credentials or TLS bypass."""
    async def scenario() -> None:
        async def feed(socket: object) -> None:
            first = json.loads(await socket.recv())  # type: ignore[attr-defined]
            second = json.loads(await socket.recv())  # type: ignore[attr-defined]
            assert first["channel"] == "level2"
            assert second["channel"] == "heartbeats"
            assert "jwt" not in first and "jwt" not in second
            for event in (ADVANCED_SNAPSHOT, ADVANCED_UPDATE,
                          {"channel": "subscriptions", "sequence_num": 2},
                          {"channel": "heartbeats", "sequence_num": 3,
                           "events": [{"heartbeat_counter": 20}]}):
                await socket.send(json.dumps(event))  # type: ignore[attr-defined]
            await socket.wait_closed()  # type: ignore[attr-defined]

        async with serve(feed, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            monkeypatch.setattr("trading.data.coinbase_l2.ADVANCED_WS_URL",
                                f"ws://127.0.0.1:{port}")
            config = CoinbaseL2Config.model_validate({
                "source": "coinbase_advanced_trade_public", "products": ["BTC-USD"],
                "duration_seconds": 10, "max_messages": 4, "stale_seconds": 3,
                "checkpoint_updates": 1,
            })
            recording = await CoinbaseL2Recorder(config, tmp_path).record()
            report = json.loads(recording.quality_path.read_text())
            assert recording.ok
            assert report["counts"]["updates"] == 1
            assert report["counts"]["heartbeats"] == 1
            assert report["final_l2_sequence"] == {"BTC-USD": 1}
            replay = replay_raw(recording.raw_path, ("BTC-USD",), config.source)
            assert replay.final_hashes == report["final_book_sha256"]
            assert not replay.integrity_errors

    asyncio.run(scenario())


def test_untrusted_proxy_certificate_stops_retrying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A TLS trust failure remains visible and never disables verification."""
    def invalid_certificate(*args: object, **kwargs: object) -> None:
        """Simulate a proxy presenting an untrusted certificate."""
        raise ssl.SSLCertVerificationError("certificate verify failed")

    monkeypatch.setattr("trading.data.coinbase_l2.websockets.connect", invalid_certificate)
    config = CoinbaseL2Config.model_validate({
        "source": "coinbase_exchange_public", "products": ["BTC-USD"],
        "duration_seconds": 60, "proxy_mode": "auto",
    })
    recording = asyncio.run(CoinbaseL2Recorder(config, tmp_path).record())
    report = json.loads(recording.quality_path.read_text())
    assert not recording.ok
    assert "TLS issuer chain" in report["fatal_error"]
    assert report["counts"]["reconnects"] == 0
