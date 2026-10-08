"""Offline book, replay, and WebSocket recording contract checks."""

import asyncio
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from websockets.asyncio.server import serve

from trading.data.coinbase_l2 import (
    BookIntegrityError,
    CoinbaseL2Config,
    CoinbaseL2Recorder,
    L2Book,
    capture_rest_snapshot,
    replay_raw,
)

SNAPSHOT = {"type": "snapshot", "product_id": "BTC-USD",
            "bids": [["100.00", "2.0"]], "asks": [["101.00", "3.0"]]}
UPDATE = {"type": "l2update", "product_id": "BTC-USD",
          "time": "2026-10-08T00:00:01Z",
          "changes": [["buy", "100.00", "4.0"], ["sell", "102.00", "1.0"]]}


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
