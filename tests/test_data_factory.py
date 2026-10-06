"""Offline CCXT ingestion, cache, cleaning, and quality boundary tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from trading.cli import main
from trading.data.cache import ParquetCache, SnapshotArchive, TimescaleCache
from trading.data.ccxt_source import CcxtPublicSource, MultiExchangeCollector, UnsupportedMarketData
from trading.data.factory import DataFactory
from trading.data.quality import assess_ohlcv
from trading.data.schema import frame_from_rows
from trading.data.transform import (
    FeaturePipeline,
    LaggedReturn,
    align_ohlcv,
    apply_adjustments,
    clean_ohlcv,
)

START = datetime(2025, 1, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)


class FakeExchange:
    """Deterministic public-only CCXT test double."""

    def __init__(self, count: int = 3) -> None:
        """Generate a fixed number of hourly bars."""
        self.has = {"fetchOHLCV": True, "fetchOrderBook": True, "fetchFundingRateHistory": True}
        self.symbols = ["BTC/USDT"]
        self.bars = [
            [int((START + index * HOUR).timestamp() * 1000), 100 + index, 102 + index,
             99 + index, 101 + index, 1.0]
            for index in range(count)
        ]

    def load_markets(self) -> None:
        """Markets are already loaded."""

    def parse_timeframe(self, timeframe: str) -> int:
        """Support the test's one-hour cadence."""
        assert timeframe == "1h"
        return 3600

    def fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int) -> list[list[float]]:
        """Return a bounded page from the requested millisecond cursor."""
        return [row for row in self.bars if row[0] >= since][:limit]

    def fetch_order_book(self, symbol: str, limit: int) -> dict[str, Any]:
        """Return a valid present-time top of book."""
        return {"timestamp": int(START.timestamp() * 1000),
                "bids": [[100.0, 2.0]], "asks": [[101.0, 2.0]]}

    def fetch_funding_rate_history(
        self, symbol: str, since: int, limit: int
    ) -> list[dict[str, float]]:
        """Return one event in the requested window."""
        event = int(START.timestamp() * 1000)
        return [{"timestamp": event, "fundingRate": 0.0001}] if since <= event else []


def _bars(count: int = 3) -> pl.DataFrame:
    """Construct canonical bars without making an external request."""
    return frame_from_rows(
        "ohlcv",
        [
            {"exchange": "fake", "symbol": "BTC/USDT", "timeframe": "1h",
             "timestamp": START + index * HOUR,
             "open": float(100 + index), "high": float(102 + index),
             "low": float(99 + index), "close": float(101 + index),
             "volume": 1.0, "observed_at": START + 5 * HOUR}
            for index in range(count)
        ],
    )


def test_multi_exchange_ingestion_archives_and_reports(tmp_path: Path) -> None:
    """Pagination, provenance, Parquet cache, and JSON quality report work together."""
    collector = MultiExchangeCollector(
        [CcxtPublicSource("venue_a", FakeExchange()), CcxtPublicSource("venue_b", FakeExchange())]
    )
    cache = ParquetCache(tmp_path / "processed")
    report_path = tmp_path / "quality.json"
    factory = DataFactory(collector, SnapshotArchive(tmp_path / "raw"), cache)
    result = factory.ingest_ohlcv(
        "BTC/USDT", "1h", START, START + 3 * HOUR, limit=2, report_path=report_path
    )
    assert result.quality.ok
    assert result.raw_rows == result.cached_rows == 6
    assert result.raw_snapshot.is_file()
    assert result.raw_snapshot.with_suffix(".json").is_file()
    assert report_path.is_file()
    assert len(result.cache_paths) == 2
    assert cache.read("ohlcv", "venue_a", "BTC/USDT", timeframe="1h").height == 3


def test_orderbook_and_funding_cache_with_offline_venues(tmp_path: Path) -> None:
    """The other two CCXT data kinds retain exchange provenance and reports."""
    collector = MultiExchangeCollector(
        [CcxtPublicSource("venue_a", FakeExchange()), CcxtPublicSource("venue_b", FakeExchange())]
    )
    cache = ParquetCache(tmp_path / "processed")
    factory = DataFactory(collector, SnapshotArchive(tmp_path / "raw"), cache)
    book = factory.ingest_orderbooks("BTC/USDT", report_path=tmp_path / "book-quality.json")
    funding = factory.ingest_funding(
        "BTC/USDT", START, START + HOUR,
        report_path=tmp_path / "funding-quality.json",
    )
    assert book.quality.ok and book.cached_rows == 4
    assert funding.quality.ok and funding.cached_rows == 2
    assert cache.read("orderbook", "venue_a", "BTC/USDT").height == 2
    assert cache.read("funding", "venue_b", "BTC/USDT").height == 1


def test_capability_and_path_traversal_guards(tmp_path: Path) -> None:
    """Unsupported methods fail and cache keys cannot escape the cache root."""
    source = CcxtPublicSource("fake", FakeExchange())
    source.exchange.has["fetchOHLCV"] = False
    with pytest.raises(UnsupportedMarketData):
        source.fetch_ohlcv("BTC/USDT", "1h", START, START + HOUR)
    path = ParquetCache(tmp_path).path_for("ohlcv", "..", "..", "..")
    assert path.resolve().is_relative_to(tmp_path.resolve())


def test_quality_gap_and_explicit_imputation() -> None:
    """A missing bar remains visible and an optional fill is marked synthetic."""
    bars = _bars(3).filter(pl.col("timestamp") != START + HOUR)
    report = assess_ohlcv(bars, START, START + 3 * HOUR, "1h")
    assert not report.ok and report.metrics["missing_bars"] == 1
    aligned = align_ohlcv(bars, START, START + 3 * HOUR, "1h", missing="forward_close")
    filled = aligned.filter(pl.col("is_imputed"))
    assert filled.height == 1 and filled["volume"][0] == 0


def test_lagged_feature_does_not_use_current_close() -> None:
    """Changing a current close cannot change a feature assigned to that bar."""
    bars = _bars(5)
    pipeline = FeaturePipeline([LaggedReturn(1)])
    before = pipeline.run(bars)["return_1"][3]
    modified = bars.with_columns(
        pl.when(pl.col("timestamp") == START + 3 * HOUR)
        .then(pl.lit(1000.0)).otherwise(pl.col("close")).alias("close"),
        pl.when(pl.col("timestamp") == START + 3 * HOUR)
        .then(pl.lit(1001.0)).otherwise(pl.col("high")).alias("high"),
    )
    after = pipeline.run(modified)["return_1"][3]
    assert before == after


def test_future_known_adjustment_is_rejected() -> None:
    """A corporate-action factor known after its bar cannot enter point-in-time data."""
    bars = clean_ohlcv(_bars(1))
    factors = pl.DataFrame(
        {"exchange": ["fake"], "symbol": ["BTC/USDT"], "timestamp": [START],
         "known_at": [START + HOUR], "price_factor": [0.5], "volume_factor": [2.0]}
    )
    with pytest.raises(ValueError, match="future-known"):
        apply_adjustments(bars, factors)
    microsecond_late = factors.with_columns(
        pl.lit(START + timedelta(microseconds=1)).alias("known_at")
    )
    with pytest.raises(ValueError, match="future-known"):
        apply_adjustments(bars, microsecond_late)


def test_quality_command_writes_offline_json_report(tmp_path: Path) -> None:
    """The CLI emits a machine-readable quality report from local Parquet."""
    data_path = tmp_path / "bars.parquet"
    report_path = tmp_path / "quality.json"
    _bars().write_parquet(data_path)
    status = main(
        ["--config", str(Path(__file__).resolve().parents[1] / "config/base.yaml"),
         "quality", "--kind", "ohlcv", "--data", str(data_path),
         "--start", START.isoformat(), "--end", (START + 3 * HOUR).isoformat(),
         "--interval", "1h", "--output", str(report_path)]
    )
    assert status == 0
    assert '"ok": true' in report_path.read_text(encoding="utf-8")


def test_timescale_upsert_uses_time_part_of_conflict_key(monkeypatch: Any) -> None:
    """The SQL adapter builds a transactional upsert without requiring a server."""
    statements: list[str] = []

    class FakeConnection:
        """Minimal psycopg connection double for schema and writes."""

        def __enter__(self) -> "FakeConnection":
            """Enter a simulated transaction."""
            return self

        def __exit__(self, *_args: object) -> None:
            """Leave a simulated transaction."""

        def cursor(self) -> "FakeConnection":
            """Return a cursor with the same recording methods."""
            return self

        def execute(self, query: str) -> None:
            """Record a schema statement."""
            statements.append(query)

        def executemany(self, query: str, _rows: object) -> None:
            """Record an upsert statement."""
            statements.append(query)

    monkeypatch.setattr("trading.data.cache.psycopg.connect", lambda _dsn: FakeConnection())
    cache = TimescaleCache("postgresql://localhost/fake")
    cache.ensure_schema()
    assert cache.write("ohlcv", _bars(1)) == 1
    assert any("create_hypertable('market_ohlcv'" in query for query in statements)
    assert any(
        "ON CONFLICT (exchange, symbol, timeframe, timestamp)" in query
        for query in statements
    )
