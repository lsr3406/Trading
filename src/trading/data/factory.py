"""Offline-safe orchestration from CCXT retrieval to auditable local caches."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from trading.data.cache import ParquetCache, SnapshotArchive, TimescaleCache
from trading.data.ccxt_source import MultiExchangeCollector
from trading.data.quality import DataQualityReport, assess_funding, assess_ohlcv, assess_orderbook
from trading.data.schema import DataKind
from trading.data.transform import clean_ohlcv


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Paths and quality outcome for one ingestion request."""

    kind: DataKind
    raw_snapshot: Path
    cache_paths: tuple[Path, ...]
    raw_rows: int
    cached_rows: int
    quality: DataQualityReport


class DataFactory:
    """Archive raw public responses, assess them, then update local caches.

    Parquet is the local source of normalized truth. TimescaleDB is an optional,
    rebuildable query cache and receives the same cleaned rows after Parquet.
    """

    def __init__(
        self,
        collector: MultiExchangeCollector,
        archive: SnapshotArchive,
        parquet: ParquetCache,
        timescale: TimescaleCache | None = None,
    ) -> None:
        """Inject collector and caches to keep I/O boundaries testable."""
        self.collector = collector
        self.archive = archive
        self.parquet = parquet
        self.timescale = timescale

    def _store(
        self,
        kind: DataKind,
        raw: pl.DataFrame,
        cleaned: pl.DataFrame,
        report: DataQualityReport,
        request: dict[str, object],
        report_path: Path | None,
    ) -> IngestionResult:
        """Archive first, publish quality, then update rebuildable caches."""
        snapshot = self.archive.write(kind, raw, request)
        if report_path is not None:
            report.write(report_path)
        paths = tuple(self.parquet.write(kind, cleaned))
        if self.timescale is not None:
            self.timescale.write(kind, cleaned)
        return IngestionResult(kind, snapshot, paths, raw.height, cleaned.height, report)

    def ingest_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        limit: int = 500,
        report_path: Path | None = None,
    ) -> IngestionResult:
        """Fetch multiple venues, report gaps, and cache validated unique bars."""
        raw = self.collector.ohlcv(symbol, timeframe, start, end, limit=limit)
        report = assess_ohlcv(raw, start, end, timeframe)
        cleaned = clean_ohlcv(raw, duplicates="last_observed")
        return self._store(
            "ohlcv", raw, cleaned, report,
            {
                "exchanges": [source.exchange_id for source in self.collector.sources],
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "limit": limit,
            },
            report_path,
        )

    def ingest_orderbooks(
        self, symbol: str, *, depth: int = 20, report_path: Path | None = None
    ) -> IngestionResult:
        """Cache present-time snapshots only if all books have valid sides."""
        raw = self.collector.order_books(symbol, depth=depth)
        report = assess_orderbook(raw)
        if not report.ok:
            raise ValueError(f"order-book quality failed: {report.issues}")
        return self._store(
            "orderbook", raw, raw, report,
            {"exchanges": [source.exchange_id for source in self.collector.sources],
             "symbol": symbol, "depth": depth},
            report_path,
        )

    def ingest_funding(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        limit: int = 200,
        report_path: Path | None = None,
    ) -> IngestionResult:
        """Cache funding history only when event keys and rates are valid."""
        raw = self.collector.funding_history(symbol, start, end, limit=limit)
        report = assess_funding(raw)
        if not report.ok:
            raise ValueError(f"funding quality failed: {report.issues}")
        return self._store(
            "funding", raw, raw, report,
            {"exchanges": [source.exchange_id for source in self.collector.sources],
             "symbol": symbol, "start": start.isoformat(), "end": end.isoformat(),
             "limit": limit},
            report_path,
        )
