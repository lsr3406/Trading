"""Canonical Polars schemas for public market data."""

from datetime import UTC, datetime
from typing import Any, Literal

import polars as pl

DataKind = Literal["ohlcv", "orderbook", "funding"]

OHLCV_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "exchange": pl.String,
    "symbol": pl.String,
    "timeframe": pl.String,
    "timestamp": pl.Datetime("ms", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "observed_at": pl.Datetime("us", "UTC"),
}
ORDERBOOK_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "exchange": pl.String,
    "symbol": pl.String,
    "observed_at": pl.Datetime("us", "UTC"),
    "exchange_timestamp": pl.Datetime("ms", "UTC"),
    "side": pl.String,
    "level": pl.Int64,
    "price": pl.Float64,
    "size": pl.Float64,
}
FUNDING_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "exchange": pl.String,
    "symbol": pl.String,
    "timestamp": pl.Datetime("ms", "UTC"),
    "funding_rate": pl.Float64,
    "observed_at": pl.Datetime("us", "UTC"),
}
SCHEMAS = {"ohlcv": OHLCV_SCHEMA, "orderbook": ORDERBOOK_SCHEMA, "funding": FUNDING_SCHEMA}
KEYS = {
    "ohlcv": ("exchange", "symbol", "timeframe", "timestamp"),
    "orderbook": ("exchange", "symbol", "observed_at", "side", "level"),
    "funding": ("exchange", "symbol", "timestamp"),
}


def empty_frame(kind: DataKind) -> pl.DataFrame:
    """Return a typed empty frame for one market-data kind."""
    return pl.DataFrame(schema=SCHEMAS[kind])


def frame_from_rows(kind: DataKind, rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Normalize records to a fixed schema, including empty responses."""
    if not rows:
        return empty_frame(kind)
    return pl.from_dicts(rows, schema=SCHEMAS[kind], strict=True)


def require_utc(value: datetime, name: str) -> None:
    """Require an aware UTC timestamp at an ingestion boundary."""
    offset = value.utcoffset() if value.tzinfo is not None else None
    if offset is None or offset.total_seconds():
        raise ValueError(f"{name} must be timezone-aware UTC")


def from_milliseconds(value: int | float) -> datetime:
    """Convert CCXT millisecond timestamps to aware UTC datetimes."""
    return datetime.fromtimestamp(float(value) / 1000, tz=UTC)
