"""Point-in-time-conscious OHLCV cleaning, alignment, and adjustment."""

import re
from collections.abc import Sequence
from datetime import datetime
from typing import Literal

import polars as pl

from trading.data.schema import KEYS, require_utc

MissingPolicy = Literal["leave_null", "forward_close"]
DuplicatePolicy = Literal["error", "last_observed"]
GROUPS = ["exchange", "symbol", "timeframe"]


def interval_milliseconds(interval: str) -> int:
    """Parse a fixed UTC bar interval such as 1m, 4h, or 1d."""
    match = re.fullmatch(r"([1-9][0-9]*)([smhdw])", interval)
    if match is None:
        raise ValueError("interval must be a fixed duration like 1m, 4h, or 1d")
    units = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    return int(match.group(1)) * units[match.group(2)]


def clean_ohlcv(frame: pl.DataFrame, *, duplicates: DuplicatePolicy = "error") -> pl.DataFrame:
    """Validate raw bars and optionally retain the latest observed duplicate.

    Invalid prices, volume, timestamps, or nulls raise rather than disappearing.
    Call the quality reporter on raw input before choosing a duplicate policy.
    """
    required = set(KEYS["ohlcv"]) | {"open", "high", "low", "close", "volume", "observed_at"}
    if missing := required - set(frame.columns):
        raise ValueError(f"missing OHLCV columns: {sorted(missing)}")
    if frame.is_empty():
        return frame.clone()
    if frame.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError("OHLCV contains null fields")
    invalid = frame.filter(
        (pl.col("open") <= 0)
        | (pl.col("high") <= 0)
        | (pl.col("low") <= 0)
        | (pl.col("close") <= 0)
        | (pl.col("volume") < 0)
        | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        | pl.any_horizontal(
            [~pl.col(column).is_finite() for column in ("open", "high", "low", "close", "volume")]
        )
    )
    if not invalid.is_empty():
        raise ValueError(f"OHLCV has {invalid.height} invalid row(s)")
    keys = list(KEYS["ohlcv"])
    ordered = frame.sort([*keys, "observed_at"])
    if ordered.select(pl.struct(keys).is_duplicated().any()).item():
        if duplicates == "error":
            raise ValueError("OHLCV has duplicate market/timeframe/timestamp keys")
        if duplicates != "last_observed":
            raise ValueError("unknown duplicate policy")
        ordered = ordered.unique(subset=keys, keep="last", maintain_order=True)
    return ordered.sort(keys)


def align_ohlcv(
    frame: pl.DataFrame,
    start: datetime,
    end: datetime,
    interval: str,
    *,
    missing: MissingPolicy = "leave_null",
) -> pl.DataFrame:
    """Align each venue/market to a UTC grid without silently inventing prices.

    `forward_close` is explicit and marks synthetic OHLC bars with zero volume.
    It never fills leading gaps. Default `leave_null` preserves all missing bars.
    """
    require_utc(start, "start")
    require_utc(end, "end")
    step_ms = interval_milliseconds(interval)
    if end <= start or int(start.timestamp() * 1000) % step_ms:
        raise ValueError("invalid or off-grid alignment window")
    if missing not in ("leave_null", "forward_close"):
        raise ValueError("unknown missing-value policy")
    cleaned = clean_ohlcv(frame)
    if cleaned.is_empty():
        return cleaned.with_columns(pl.lit(False).alias("is_imputed"))
    if cleaned.filter((pl.col("timestamp") < start) | (pl.col("timestamp") >= end)).height:
        raise ValueError("input bars are outside the requested window")
    if cleaned.filter(
        ((pl.col("timestamp").cast(pl.Int64) - int(start.timestamp() * 1000)) % step_ms) != 0
    ).height:
        raise ValueError("input bars are off the requested interval grid")
    periods = pl.datetime_range(
        start, end, interval=interval, closed="left", time_zone="UTC", eager=True
    ).cast(pl.Datetime("ms", "UTC"))
    grid = cleaned.select(GROUPS).unique().join(pl.DataFrame({"timestamp": periods}), how="cross")
    result = grid.join(cleaned, on=[*GROUPS, "timestamp"], how="left").sort([*GROUPS, "timestamp"])
    if missing == "leave_null":
        return result.with_columns(pl.lit(False).alias("is_imputed"))
    previous_close = pl.col("close").forward_fill().over(GROUPS)
    result = result.with_columns(
        (pl.col("close").is_null() & previous_close.is_not_null()).alias("is_imputed")
    )
    return result.with_columns(
        [
            pl.when(pl.col("is_imputed"))
            .then(previous_close)
            .otherwise(pl.col(column))
            .alias(column)
            for column in ("open", "high", "low", "close")
        ]
        + [
            pl.when(pl.col("is_imputed"))
            .then(pl.lit(0.0))
            .otherwise(pl.col("volume"))
            .alias("volume")
        ]
    )


def apply_adjustments(frame: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame:
    """Apply explicit per-bar point-in-time price and volume factors.

    Factors must have keys exchange/symbol/timestamp, positive `price_factor`
    and `volume_factor`, plus `known_at <= timestamp`. Missing factors fail.
    This function does not source or infer corporate actions.
    """
    required = {"exchange", "symbol", "timestamp", "known_at", "price_factor", "volume_factor"}
    if missing := required - set(factors.columns):
        raise ValueError(f"missing adjustment columns: {sorted(missing)}")
    if not frame.schema.get("timestamp") == pl.Datetime("ms", "UTC"):
        raise ValueError("bars must use millisecond UTC timestamps")
    normalized_timestamp = pl.col("timestamp").cast(pl.Datetime("ms", "UTC"))
    if factors.filter(
        normalized_timestamp.cast(pl.Datetime("us", "UTC"))
        != pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
    ).height:
        raise ValueError("adjustment timestamps must align exactly to millisecond bars")
    factors = factors.with_columns(
        normalized_timestamp,
        pl.col("known_at").cast(pl.Datetime("us", "UTC")),
    )
    keys = ["exchange", "symbol", "timestamp"]
    if factors.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError("duplicate adjustment factors")
    merged = frame.join(factors, on=keys, how="left")
    if merged.height != frame.height or merged.filter(
        pl.col("price_factor").is_null() | pl.col("volume_factor").is_null()
    ).height:
        raise ValueError("adjustment factors must cover every bar")
    if merged.filter(
        (pl.col("price_factor") <= 0)
        | (pl.col("volume_factor") <= 0)
        | (pl.col("known_at") > pl.col("timestamp").cast(pl.Datetime("us", "UTC")))
    ).height:
        raise ValueError("invalid or future-known adjustment factor")
    return merged.with_columns(
        [
            (pl.col(column) * pl.col("price_factor")).alias(column)
            for column in ("open", "high", "low", "close")
        ]
        + [(pl.col("volume") * pl.col("volume_factor")).alias("volume")]
    ).drop("known_at", "price_factor", "volume_factor")


class FeatureStep:
    """Base class for pure Polars feature transformations."""

    name: str

    def apply(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Return a new frame with a feature column; subclasses must implement."""
        raise NotImplementedError


class LaggedReturn(FeatureStep):
    """Return over prior completed bars, excluding the current close."""

    def __init__(self, lookback: int, name: str | None = None) -> None:
        """Require a strictly positive lookback."""
        if lookback <= 0:
            raise ValueError("lookback must be positive")
        self.lookback = lookback
        self.name = name or f"return_{lookback}"

    def apply(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Compute close[t-1] / close[t-lookback-1] - 1 per market."""
        current = pl.col("close").shift(1).over(GROUPS)
        past = pl.col("close").shift(self.lookback + 1).over(GROUPS)
        return frame.with_columns((current / past - 1).alias(self.name))


class LaggedRollingMean(FeatureStep):
    """Mean of prior completed close prices, excluding the current bar."""

    def __init__(self, window: int, name: str | None = None) -> None:
        """Require at least two bars for a rolling mean."""
        if window < 2:
            raise ValueError("window must be at least two")
        self.window = window
        self.name = name or f"mean_{window}"

    def apply(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Compute a prior-window moving average per market."""
        feature = pl.col("close").shift(1).rolling_mean(self.window).over(GROUPS)
        return frame.with_columns(feature.alias(self.name))


class FeaturePipeline:
    """Apply extensible, named, pure Polars steps in deterministic order."""

    def __init__(self, steps: Sequence[FeatureStep]) -> None:
        """Reject duplicate feature names before computation."""
        if len({step.name for step in steps}) != len(steps):
            raise ValueError("feature names must be unique")
        self.steps = tuple(steps)

    def run(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Sort within markets and apply each step without mutating input."""
        result = clean_ohlcv(frame).sort([*GROUPS, "timestamp"])
        for step in self.steps:
            if step.name in result.columns:
                raise ValueError(f"feature already exists: {step.name}")
            result = step.apply(result)
        return result
