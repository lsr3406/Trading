"""Machine-readable quality reports for normalized market data."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from trading.data.schema import KEYS, SCHEMAS, DataKind, require_utc
from trading.data.transform import GROUPS, interval_milliseconds


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    """Counts and findings for one bounded market-data assessment."""

    kind: DataKind
    generated_at_utc: str
    row_count: int
    metrics: dict[str, int]
    issues: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """Return false when any required quality check found an issue."""
        return not self.issues

    def to_json(self) -> str:
        """Return a stable, human-readable JSON report."""
        return json.dumps({**asdict(self), "ok": self.ok}, indent=2, sort_keys=True)

    def write(self, path: Path) -> None:
        """Write a JSON report to an explicit location."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json() + "\n", encoding="utf-8")


def _base(kind: DataKind, frame: pl.DataFrame) -> tuple[dict[str, int], list[str]]:
    """Count nulls, duplicates, and schema mismatches without modifying data."""
    missing = set(SCHEMAS[kind]) - set(frame.columns)
    if missing:
        raise ValueError(f"missing {kind} columns: {sorted(missing)}")
    duplicates = frame.height - frame.unique(subset=list(KEYS[kind])).height
    null_values = sum(frame.select(pl.all().null_count()).row(0)) if frame.height else 0
    metrics = {"duplicates": duplicates, "null_values": null_values}
    issues = [f"duplicate_keys:{duplicates}"] if duplicates else []
    if null_values:
        issues.append(f"null_values:{null_values}")
    return metrics, issues


def assess_ohlcv(
    frame: pl.DataFrame, start: datetime, end: datetime, interval: str
) -> DataQualityReport:
    """Check a half-open UTC OHLCV window for gaps, duplicates, and bad bars."""
    require_utc(start, "start")
    require_utc(end, "end")
    step = interval_milliseconds(interval)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    if end_ms <= start_ms or start_ms % step or (end_ms - start_ms) % step:
        raise ValueError("quality window must be aligned to a complete interval grid")
    metrics, issues = _base("ohlcv", frame)
    if frame.is_empty():
        issues.append("empty_response")
        metrics.update({"missing_bars": 0, "invalid_bars": 0, "off_grid_bars": 0})
    else:
        invalid = frame.filter(
            pl.any_horizontal(
                [
                    pl.col(column).is_null() | ~pl.col(column).is_finite()
                    for column in ("open", "high", "low", "close", "volume")
                ]
            )
            | (pl.col("open") <= 0)
            | (pl.col("high") < pl.max_horizontal("open", "low", "close"))
            | (pl.col("low") > pl.min_horizontal("open", "high", "close"))
            | (pl.col("volume") < 0)
        ).height
        off_grid = frame.filter(
            (pl.col("timestamp") < start)
            | (pl.col("timestamp") >= end)
            | ((pl.col("timestamp").cast(pl.Int64) - start_ms) % step != 0)
            | (pl.col("timeframe") != interval)
        ).height
        expected_per_market = (end_ms - start_ms) // step
        counts = frame.filter(
            (pl.col("timestamp") >= start)
            & (pl.col("timestamp") < end)
            & (pl.col("timeframe") == interval)
            & ((pl.col("timestamp").cast(pl.Int64) - start_ms) % step == 0)
        ).group_by(GROUPS).agg(pl.col("timestamp").n_unique().alias("present"))
        groups = frame.select(GROUPS).unique()
        missing_bars = int(
            groups.join(counts, on=GROUPS, how="left")
            .with_columns(pl.col("present").fill_null(0))
            .select((pl.lit(expected_per_market) - pl.col("present")).sum())
            .item()
        )
        imputed = int(frame["is_imputed"].sum()) if "is_imputed" in frame.columns else 0
        metrics.update(
            {
                "invalid_bars": invalid,
                "off_grid_bars": off_grid,
                "missing_bars": missing_bars,
                "imputed_bars": imputed,
            }
        )
        for name, value in (
            ("invalid_bars", invalid),
            ("off_grid_bars", off_grid),
            ("missing_bars", missing_bars),
            ("imputed_bars", imputed),
        ):
            if value:
                issues.append(f"{name}:{value}")
    return DataQualityReport(
        "ohlcv", datetime.now(UTC).isoformat(), frame.height, metrics, tuple(issues)
    )


def assess_orderbook(frame: pl.DataFrame) -> DataQualityReport:
    """Check present-time book levels for invalid and crossed snapshots."""
    metrics, issues = _base("orderbook", frame)
    if frame.is_empty():
        issues.append("empty_response")
        invalid = 0
        crossed = 0
    else:
        invalid = frame.filter(
            (pl.col("price") <= 0)
            | (pl.col("size") <= 0)
            | ~pl.col("price").is_finite()
            | ~pl.col("size").is_finite()
            | ~pl.col("side").is_in(["bid", "ask"])
        ).height
        quotes = frame.group_by(["exchange", "symbol", "observed_at"]).agg(
            pl.col("price").filter(pl.col("side") == "bid").max().alias("best_bid"),
            pl.col("price").filter(pl.col("side") == "ask").min().alias("best_ask"),
        )
        crossed = quotes.filter(
            pl.col("best_bid").is_null()
            | pl.col("best_ask").is_null()
            | (pl.col("best_bid") >= pl.col("best_ask"))
        ).height
    metrics.update({"invalid_levels": invalid, "crossed_or_one_sided_books": crossed})
    if invalid:
        issues.append(f"invalid_levels:{invalid}")
    if crossed:
        issues.append(f"crossed_or_one_sided_books:{crossed}")
    return DataQualityReport(
        "orderbook", datetime.now(UTC).isoformat(), frame.height, metrics, tuple(issues)
    )


def assess_funding(frame: pl.DataFrame) -> DataQualityReport:
    """Check historical funding records for missing or nonfinite rates."""
    metrics, issues = _base("funding", frame)
    invalid = (
        frame.filter(pl.col("funding_rate").is_null() | ~pl.col("funding_rate").is_finite()).height
        if not frame.is_empty()
        else 0
    )
    metrics["invalid_rates"] = invalid
    if frame.is_empty():
        issues.append("empty_response")
    if invalid:
        issues.append(f"invalid_rates:{invalid}")
    return DataQualityReport(
        "funding", datetime.now(UTC).isoformat(), frame.height, metrics, tuple(issues)
    )


def assess(kind: DataKind, frame: pl.DataFrame, **window: Any) -> DataQualityReport:
    """Dispatch the quality assessment for one canonical data kind."""
    if kind == "ohlcv":
        return assess_ohlcv(frame, window["start"], window["end"], window["interval"])
    if kind == "orderbook":
        return assess_orderbook(frame)
    return assess_funding(frame)
