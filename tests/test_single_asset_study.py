"""Offline regression tests for archive integrity and causal study boundaries."""

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

from trading.data.binance_archive import BinanceMonthlyArchive
from trading.data.quality import assess_ohlcv
from trading.data.schema import frame_from_rows
from trading.research.single_asset import (
    StudyDataset,
    momentum_execution_target,
    run_study,
)
from trading.single_asset_config import load_single_asset_config


def _archive_payload() -> bytes:
    """Create a minimal official-format monthly CSV with microsecond opens."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(3):
        stamp = int((start + timedelta(hours=4 * index)).timestamp() * 1_000_000)
        rows.append(f"{stamp},100,102,99,101,1,0,0,0,0,0,0")
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("BTCUSDT-4h-2025-01.csv", "\n".join(rows))
    return stream.getvalue()


def test_archive_checksum_and_microsecond_conversion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The official checksum gates parsing and 2025 timestamps become UTC ms."""
    payload = _archive_payload()
    digest = hashlib.sha256(payload).hexdigest()

    def fake_download(url: str) -> bytes:
        """Return a fixed ZIP and its companion checksum."""
        if url.endswith(".CHECKSUM"):
            return f"{digest}  BTCUSDT-4h-2025-01.zip".encode()
        return payload

    monkeypatch.setattr(BinanceMonthlyArchive, "_download", staticmethod(fake_download))
    collector = BinanceMonthlyArchive("BTCUSDT", "4h", tmp_path)
    frame, part = collector.fetch_month("2025-01")
    assert frame.height == 3
    assert frame["timestamp"][1] == datetime(2025, 1, 1, 4, tzinfo=UTC)
    assert part.sha256 == digest
    assert Path(part.path).exists()
    rebuilt, _ = collector.fetch_month("2025-01")
    assert rebuilt.height == 3
    assert rebuilt["observed_at"].to_list() == frame["observed_at"].to_list()


def test_archive_rejects_digest_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A changed archive never becomes a normalized dataset."""
    payload = _archive_payload()

    def fake_download(url: str) -> bytes:
        """Supply a false official digest."""
        if url.endswith(".CHECKSUM"):
            return ("0" * 64 + "  BTCUSDT-4h-2025-01.zip").encode()
        return payload

    monkeypatch.setattr(BinanceMonthlyArchive, "_download", staticmethod(fake_download))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        BinanceMonthlyArchive("BTCUSDT", "4h", tmp_path).fetch_month("2025-01")


def test_gap_fails_ohlcv_quality() -> None:
    """A missing four-hour bar is not silently imputed for factor research."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "4h",
         "timestamp": start + timedelta(hours=offset),
         "open": 100.0, "high": 101.0, "low": 99.0,
         "close": 100.0, "volume": 1.0, "observed_at": start}
        for offset in (0, 8)
    ]
    report = assess_ohlcv(
        frame_from_rows("ohlcv", rows), start, start + timedelta(hours=12), "4h"
    )
    assert not report.ok
    assert report.metrics["missing_bars"] == 1


def test_momentum_signal_waits_for_next_close() -> None:
    """A price jump at t cannot cause an order at t."""
    close = pd.Series([100.0, 100.0, 100.0, 100.0, 120.0, 120.0])
    target = momentum_execution_target(close, lookback=2)
    assert target.tolist() == [False, False, False, False, False, True]


def test_study_uses_disjoint_holdout_and_a_one_bar_signal_delay(tmp_path: Path) -> None:
    """The report contains a sealed holdout and no training/test overlap."""
    base = load_single_asset_config(Path("config/single_asset.yaml"))
    config = base.model_copy(update={
        "start": datetime(2025, 1, 1, tzinfo=UTC),
        "end": datetime(2025, 7, 1, tzinfo=UTC),
        "train_bars": 300, "test_bars": 180, "bootstrap_resamples": 199,
    })
    timestamps = pl.datetime_range(config.start, config.end, interval="4h", eager=True,
                                   closed="left", time_zone="UTC")
    prices = 100 + np.sin(np.arange(len(timestamps)) / 20) * 5 + np.arange(len(timestamps)) * 0.01
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT", "timeframe": "4h",
         "timestamp": timestamp, "open": float(price), "high": float(price + 1),
         "low": float(price - 1), "close": float(price), "volume": 1.0,
         "observed_at": config.end}
        for timestamp, price in zip(timestamps, prices, strict=True)
    ]
    data_path = tmp_path / "bars.parquet"
    frame_from_rows("ohlcv", rows).write_parquet(data_path)
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    dataset = StudyDataset(data_path, tmp_path / "source.json", tmp_path / "quality.json",
                           digest, len(rows))
    report = json.loads(run_study(config, dataset, tmp_path).read_text())
    folds = report["development_oos_folds"]
    assert folds
    for fold in folds:
        assert fold["train_bars"][1] + config.embargo_bars == fold["test_bars"][0]
    assert folds[-1]["test_bars"][1] <= int(len(rows) * (1 - config.holdout_fraction))
    assert report["final_holdout"]["bars"] > 0
