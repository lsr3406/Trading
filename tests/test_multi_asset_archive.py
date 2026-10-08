"""Offline evidence checks for fixed multi-asset archive backfills."""

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from trading.alpha.factors import FactorBatch, LaggedMomentum
from trading.data.binance_archive import ArchivePart, BinanceMonthlyArchive
from trading.data.multi_asset_archive import (
    MultiAssetArchiveConfig,
    collect_multi_asset_data,
)
from trading.data.schema import frame_from_rows

START = datetime(2025, 1, 1, tzinfo=UTC)
STEP = timedelta(hours=4)


def _config() -> MultiAssetArchiveConfig:
    """Declare two markets with a different research eligibility start."""
    return MultiAssetArchiveConfig.model_validate({
        "source": "binance_archive", "selection_method": "retrospective_pilot",
        "selected_at_utc": "2025-02-01T00:00:00+00:00", "timeframe": "4h",
        "start": START.isoformat(), "end": (START + 4 * STEP).isoformat(),
        "assets": [
            {"archive_symbol": "BTCUSDT", "eligible_from": START.isoformat(),
             "eligible_until": (START + 4 * STEP).isoformat()},
            {"archive_symbol": "ETHUSDT", "eligible_from": (START + STEP).isoformat(),
             "eligible_until": (START + 4 * STEP).isoformat()},
        ],
    })


def _fake_collect(self: BinanceMonthlyArchive, start: datetime, end: datetime
                  ) -> tuple[pl.DataFrame, tuple[ArchivePart, ...]]:
    """Return canonical rows for one complete month without a network request."""
    assert start == START and end == datetime(2025, 2, 1, tzinfo=UTC)
    rows = [
        {"exchange": "binance", "symbol": f"{self.symbol[:-4]}/USDT",
         "timeframe": "4h", "timestamp": START + index * STEP,
         "open": float(100 + index), "high": float(101 + index),
         "low": float(99 + index), "close": float(100 + index),
         "volume": 1.0, "observed_at": START + timedelta(days=40)}
        for index in range(4)
    ]
    part = ArchivePart("2025-01", "https://example.invalid/archive", "a" * 64,
                       "/tmp/fake.zip", len(rows), START.isoformat())
    return frame_from_rows("ohlcv", rows), (part,)


def test_multi_asset_calendar_and_versioned_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Out-of-eligibility cells are distinct from missing eligible bars."""
    monkeypatch.setattr(BinanceMonthlyArchive, "collect", _fake_collect)
    config = _config()
    dataset = collect_multi_asset_data(config, tmp_path)
    assert dataset.rows == 7
    calendar = pl.read_parquet(dataset.calendar_path)
    assert calendar.filter(pl.col("status") == "outside_eligibility").height == 1
    assert calendar.filter(pl.col("status") == "missing").is_empty()
    quality = json.loads(dataset.quality_path.read_text())
    assert quality["ok"] and quality["selection_method"] == "retrospective_pilot"
    manifest = json.loads(dataset.manifest_path.read_text())
    assert manifest["data_sha256"] == hashlib.sha256(dataset.path.read_bytes()).hexdigest()
    assert len(manifest["source_archives"]) == 2
    features = FactorBatch([LaggedMomentum(1)]).compute(pl.read_parquet(dataset.path))
    assert features.filter(pl.col("symbol") == "BTC/USDT").height == 4
    assert collect_multi_asset_data(config, tmp_path).sha256 == dataset.sha256


def test_eligible_gap_fails_before_publishing_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing bar in an eligible interval generates a failing report."""
    def with_gap(self: BinanceMonthlyArchive, start: datetime, end: datetime
                 ) -> tuple[pl.DataFrame, tuple[ArchivePart, ...]]:
        frame, parts = _fake_collect(self, start, end)
        if self.symbol == "ETHUSDT":
            frame = frame.filter(pl.col("timestamp") != START + 2 * STEP)
        return frame, parts

    monkeypatch.setattr(BinanceMonthlyArchive, "collect", with_gap)
    with pytest.raises(ValueError, match="multi-asset quality failed"):
        collect_multi_asset_data(_config(), tmp_path)
    reports = list((tmp_path / "research/reports").glob("*-quality.json"))
    assert len(reports) == 1
    assert json.loads(reports[0].read_text())["metrics"]["missing_bars"] == 1
    assert not list((tmp_path / "data/processed").glob("*.parquet"))


def test_archive_accepts_a_midmonth_first_bar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A newly eligible market may have a legitimate partial first month."""
    timestamp = int((START + STEP).timestamp() * 1_000_000)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("ETHUSDT-4h-2025-01.csv",
                         f"{timestamp},100,101,99,100,1,0,0,0,0,0,0")
    payload = stream.getvalue()
    digest = hashlib.sha256(payload).hexdigest()

    def download(url: str) -> bytes:
        return (f"{digest} ETHUSDT-4h-2025-01.zip".encode()
                if url.endswith(".CHECKSUM") else payload)

    monkeypatch.setattr(BinanceMonthlyArchive, "_download", staticmethod(download))
    frame, _ = BinanceMonthlyArchive("ETHUSDT", "4h", tmp_path).fetch_month("2025-01")
    assert frame["timestamp"][0] == START + STEP
