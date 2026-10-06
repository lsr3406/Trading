"""Atomic partitioned Parquet cache and optional TimescaleDB cache."""

import os
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import polars as pl
import psycopg

from trading.data.schema import KEYS, SCHEMAS, DataKind, empty_frame, frame_from_rows, require_utc


class ParquetCache:
    """Keep each venue/market/timeframe in a separate atomic Parquet file."""

    def __init__(self, root: Path) -> None:
        """Use the given local directory without creating it until a write."""
        self.root = root

    def path_for(
        self, kind: DataKind, exchange: str, symbol: str, timeframe: str | None = None
    ) -> Path:
        """Return a path with URL-encoded components, preventing path traversal."""
        if kind == "ohlcv" and not timeframe:
            raise ValueError("OHLCV cache requires timeframe")
        if not exchange or not symbol or (timeframe is not None and not timeframe):
            raise ValueError("cache keys must be nonempty")
        # Prefix each component: urllib.quote deliberately leaves dots unescaped,
        # so a literal ".." must never become a directory component.
        parts = [f"key={quote(part, safe='')}" for part in (exchange, symbol)]
        filename = (
            f"timeframe={quote(timeframe, safe='')}.parquet" if timeframe else "events.parquet"
        )
        return self.root / kind / parts[0] / parts[1] / filename

    def write(self, kind: DataKind, frame: pl.DataFrame) -> list[Path]:
        """Merge by canonical key and atomically replace affected partitions.

        `observed_at` breaks ties for revised OHLCV or funding events. This is a
        mutable cache; callers must archive raw fetches separately for provenance.
        """
        if frame.is_empty():
            return []
        required = set(SCHEMAS[kind])
        if missing := required - set(frame.columns):
            raise ValueError(f"missing {kind} columns: {sorted(missing)}")
        group_columns = ["exchange", "symbol", "timeframe"] if kind == "ohlcv" else [
            "exchange", "symbol"
        ]
        paths: list[Path] = []
        for keys, partition in frame.group_by(group_columns, maintain_order=True):
            exchange, symbol = str(keys[0]), str(keys[1])
            timeframe = str(keys[2]) if kind == "ohlcv" else None
            path = self.path_for(kind, exchange, symbol, timeframe)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                prior = pl.read_parquet(path)
                partition = pl.concat([prior, partition], how="diagonal_relaxed")
            partition = partition.sort([*KEYS[kind], "observed_at"]).unique(
                subset=list(KEYS[kind]), keep="last", maintain_order=True
            )
            temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            try:
                partition.write_parquet(temporary, compression="zstd")
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            paths.append(path)
        return paths

    def read(
        self,
        kind: DataKind,
        exchange: str,
        symbol: str,
        *,
        timeframe: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pl.DataFrame:
        """Read one partition, optionally filtering a half-open UTC window."""
        path = self.path_for(kind, exchange, symbol, timeframe)
        if not path.exists():
            return empty_frame(kind)
        time_column = "observed_at" if kind == "orderbook" else "timestamp"
        lazy = pl.scan_parquet(path)
        if start is not None:
            require_utc(start, "start")
            lazy = lazy.filter(pl.col(time_column) >= start)
        if end is not None:
            require_utc(end, "end")
            lazy = lazy.filter(pl.col(time_column) < end)
        return lazy.collect().sort(list(KEYS[kind]))


_TABLES = {"ohlcv": "market_ohlcv", "orderbook": "market_orderbook", "funding": "market_funding"}
_TIMES = {"ohlcv": "timestamp", "orderbook": "observed_at", "funding": "timestamp"}
_SQL_COLUMNS = {
    "ohlcv": "exchange, symbol, timeframe, timestamp, open, high, low, close, volume, observed_at",
    "orderbook": (
        "exchange, symbol, observed_at, exchange_timestamp, side, level, price, size"
    ),
    "funding": "exchange, symbol, timestamp, funding_rate, observed_at",
}
_CREATE = (
    "CREATE EXTENSION IF NOT EXISTS timescaledb",
    """CREATE TABLE IF NOT EXISTS market_ohlcv (
        exchange TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL, open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL, low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL, volume DOUBLE PRECISION NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (exchange, symbol, timeframe, timestamp))""",
    "SELECT create_hypertable('market_ohlcv', 'timestamp', if_not_exists => TRUE)",
    """CREATE TABLE IF NOT EXISTS market_orderbook (
        exchange TEXT NOT NULL, symbol TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL, exchange_timestamp TIMESTAMPTZ,
        side TEXT NOT NULL, level INTEGER NOT NULL,
        price DOUBLE PRECISION NOT NULL, size DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (exchange, symbol, observed_at, side, level))""",
    "SELECT create_hypertable('market_orderbook', 'observed_at', if_not_exists => TRUE)",
    """CREATE TABLE IF NOT EXISTS market_funding (
        exchange TEXT NOT NULL, symbol TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL, funding_rate DOUBLE PRECISION,
        observed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (exchange, symbol, timestamp))""",
    "SELECT create_hypertable('market_funding', 'timestamp', if_not_exists => TRUE)",
)


class TimescaleCache:
    """A local TimescaleDB cache with idempotent writes and bounded reads."""

    def __init__(self, dsn: str | None = None) -> None:
        """Take a DSN from process environment if not passed explicitly."""
        resolved_dsn = dsn or os.environ.get("TIMESCALE_DSN")
        if not resolved_dsn:
            raise ValueError("set TIMESCALE_DSN in the process environment")
        self._dsn: str = resolved_dsn

    def ensure_schema(self) -> None:
        """Create Timescale hypertables with unique keys including partition time."""
        with psycopg.connect(self._dsn) as connection:
            for statement in _CREATE:
                connection.execute(statement)

    def write(self, kind: DataKind, frame: pl.DataFrame) -> int:
        """Upsert a normalized frame; data and schema changes are transactional."""
        if frame.is_empty():
            return 0
        required = set(SCHEMAS[kind])
        if missing := required - set(frame.columns):
            raise ValueError(f"missing {kind} columns: {sorted(missing)}")
        columns = list(SCHEMAS[kind])
        values = [tuple(row[column] for column in columns) for row in frame.iter_rows(named=True)]
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join(
            f"{column} = EXCLUDED.{column}" for column in columns if column not in KEYS[kind]
        )
        query = (
            f"INSERT INTO {_TABLES[kind]} ({_SQL_COLUMNS[kind]}) "
            f"VALUES ({placeholders}) ON CONFLICT ({', '.join(KEYS[kind])}) "
            f"DO UPDATE SET {updates}"
        )
        with psycopg.connect(self._dsn) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, values)
        return len(values)

    def read(
        self,
        kind: DataKind,
        exchange: str,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        timeframe: str | None = None,
    ) -> pl.DataFrame:
        """Read one venue/market over [start, end); OHLCV requires timeframe."""
        require_utc(start, "start")
        require_utc(end, "end")
        if end <= start or (kind == "ohlcv" and not timeframe):
            raise ValueError("invalid window or missing OHLCV timeframe")
        time_column = _TIMES[kind]
        params: tuple[object, ...] = (exchange, symbol, start, end)
        predicate = f"exchange = %s AND symbol = %s AND {time_column} >= %s AND {time_column} < %s"
        if kind == "ohlcv":
            predicate += " AND timeframe = %s"
            params += (timeframe,)
        query = (
            f"SELECT {_SQL_COLUMNS[kind]} FROM {_TABLES[kind]} "
            f"WHERE {predicate} ORDER BY {time_column}"
        )
        with psycopg.connect(self._dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                columns = list(SCHEMAS[kind])
                rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        return frame_from_rows(kind, rows)


class SnapshotArchive:
    """Write an immutable, uniquely named normalized CCXT response snapshot."""

    def __init__(self, root: Path) -> None:
        """Set the raw archive directory; it is created on first write."""
        self.root = root

    def write(self, kind: DataKind, frame: pl.DataFrame, request: dict[str, object]) -> Path:
        """Archive a response plus request metadata and Parquet checksum."""
        import hashlib
        import json
        from datetime import UTC

        captured_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        directory = self.root / kind
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{captured_at}-{uuid.uuid4().hex}.parquet"
        temporary = path.with_name(f".{path.name}.tmp")
        try:
            frame.write_parquet(temporary, compression="zstd")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        metadata = {
            "kind": kind,
            "captured_at_utc": datetime.now(UTC).isoformat(),
            "request": request,
            "rows": frame.height,
            "parquet_sha256": digest,
        }
        path.with_suffix(".json").write_text(
            json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        return path
