"""Versioned multi-asset Binance backfill with explicit eligibility calendars."""

import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl
import yaml
from pydantic import Field, model_validator

from trading.configuration import StrictModel
from trading.data.binance_archive import ArchivePart, BinanceMonthlyArchive
from trading.data.quality import DataQualityReport, assess_ohlcv
from trading.data.schema import require_utc
from trading.data.transform import interval_milliseconds

PIPELINE_VERSION = "multi-asset-archive-v1"


class AssetEligibility(StrictModel):
    """One research eligibility interval, separate from actual listing metadata."""

    archive_symbol: str = Field(pattern=r"^[A-Z0-9]{3,24}USDT$")
    eligible_from: datetime
    eligible_until: datetime

    @model_validator(mode="after")
    def valid_interval(self) -> "AssetEligibility":
        """Reject naive, empty, and reversed eligibility windows."""
        require_utc(self.eligible_from, "eligible_from")
        require_utc(self.eligible_until, "eligible_until")
        if self.eligible_until <= self.eligible_from:
            raise ValueError("eligible_until must follow eligible_from")
        return self

    @property
    def symbol(self) -> str:
        """Return the canonical spot pair label."""
        return f"{self.archive_symbol[:-4]}/USDT"


class MultiAssetArchiveConfig(StrictModel):
    """Frozen illustrative universe; never infer historical membership from today."""

    source: Literal["binance_archive"]
    selection_method: Literal["retrospective_pilot"]
    selected_at_utc: datetime
    timeframe: Literal["1h", "4h", "1d"]
    start: datetime
    end: datetime
    assets: tuple[AssetEligibility, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def valid_protocol(self) -> "MultiAssetArchiveConfig":
        """Require aligned UTC intervals and one non-overlapping identity per asset."""
        for name in ("selected_at_utc", "start", "end"):
            require_utc(getattr(self, name), name)
        if self.end <= self.start or self.selected_at_utc < self.end:
            raise ValueError("pilot selection time must follow the bounded history")
        if len({asset.archive_symbol for asset in self.assets}) != len(self.assets):
            raise ValueError("duplicate archive symbol in universe")
        step = interval_milliseconds(self.timeframe)
        for time in (self.start, self.end, *(
            point for asset in self.assets
            for point in (asset.eligible_from, asset.eligible_until)
        )):
            if int(time.timestamp() * 1000) % step:
                raise ValueError("all eligibility and study times must lie on the bar grid")
        for asset in self.assets:
            if not self.start <= asset.eligible_from < asset.eligible_until <= self.end:
                raise ValueError("asset eligibility must be inside the study window")
        return self

    def fingerprint(self) -> str:
        """Hash the complete declared universe and data interval."""
        raw = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()


def load_multi_asset_config(path: Path) -> MultiAssetArchiveConfig:
    """Load and validate a declared multi-market backfill protocol."""
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("multi-asset config must be a YAML mapping")
    return MultiAssetArchiveConfig.model_validate(value)


@dataclass(frozen=True, slots=True)
class MultiAssetDataset:
    """Paths and identity of a completed, integrity-checked backfill."""

    path: Path
    calendar_path: Path
    quality_path: Path
    manifest_path: Path
    sha256: str
    rows: int


def _month_floor(value: datetime) -> datetime:
    """Return the first UTC instant of the containing month."""
    return datetime(value.year, value.month, 1, tzinfo=UTC)


def _month_ceil(value: datetime) -> datetime:
    """Return a month boundary at or after the timestamp."""
    floor = _month_floor(value)
    if floor == value:
        return floor
    year, month = (value.year + 1, 1) if value.month == 12 else (value.year, value.month + 1)
    return datetime(year, month, 1, tzinfo=UTC)


def _write_versioned(frame: pl.DataFrame, path: Path) -> None:
    """Atomically create immutable Parquet, rejecting conflicting existing contents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not pl.read_parquet(path).equals(frame):
            raise ValueError(f"versioned Parquet conflicts with source manifests: {path}")
        return
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary, compression="zstd")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _calendar(config: MultiAssetArchiveConfig, frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Classify every market-time cell as present, missing, or outside eligibility."""
    times = pl.datetime_range(
        config.start, config.end, interval=config.timeframe, closed="left",
        time_zone="UTC", eager=True,
    ).cast(pl.Datetime("ms", "UTC"))
    records: list[pl.DataFrame] = []
    for asset in config.assets:
        observed = frames[asset.archive_symbol].select("timestamp").with_columns(
            pl.lit(True).alias("present")
        )
        cells = pl.DataFrame({"timestamp": times}).join(observed, on="timestamp", how="left")
        records.append(cells.select(
            pl.lit(asset.symbol).alias("symbol"),
            "timestamp",
            pl.when((pl.col("timestamp") < asset.eligible_from)
                    | (pl.col("timestamp") >= asset.eligible_until))
            .then(pl.lit("outside_eligibility"))
            .when(pl.col("present").fill_null(False))
            .then(pl.lit("observed"))
            .otherwise(pl.lit("missing"))
            .alias("status"),
        ))
    return pl.concat(records).sort(["timestamp", "symbol"])


def collect_multi_asset_data(
    config: MultiAssetArchiveConfig, project_root: Path
) -> MultiAssetDataset:
    """Fetch, verify, align, and archive a fixed set of public spot markets."""
    frames: dict[str, pl.DataFrame] = {}
    parts_by_asset: dict[str, tuple[ArchivePart, ...]] = {}
    reports: dict[str, DataQualityReport] = {}
    for asset in config.assets:
        source = BinanceMonthlyArchive(
            asset.archive_symbol, config.timeframe,
            project_root / "data/raw/binance_archive",
        )
        frame, parts = source.collect(
            _month_floor(asset.eligible_from), _month_ceil(asset.eligible_until)
        )
        frame = frame.filter(
            (pl.col("timestamp") >= asset.eligible_from)
            & (pl.col("timestamp") < asset.eligible_until)
        ).sort("timestamp")
        frames[asset.archive_symbol] = frame
        parts_by_asset[asset.archive_symbol] = parts
        reports[asset.archive_symbol] = assess_ohlcv(
            frame, asset.eligible_from, asset.eligible_until, config.timeframe
        )
    calendar = _calendar(config, frames)
    source_identity = {
        "config_sha256": config.fingerprint(),
        "pipeline": PIPELINE_VERSION,
        "parts": {
            symbol: [part.sha256 for part in parts]
            for symbol, parts in parts_by_asset.items()
        },
    }
    digest = hashlib.sha256(json.dumps(source_identity, sort_keys=True).encode()).hexdigest()
    stem = f"multiasset-binance-{config.timeframe}-{digest[:16]}"
    quality_path = project_root / "research/reports" / f"{stem}-quality.json"
    metrics = {
        name: sum(report.metrics.get(name, 0) for report in reports.values())
        for name in ("missing_bars", "duplicates", "invalid_bars", "off_grid_bars",
                     "null_values", "imputed_bars")
    }
    quality = {
        "ok": all(report.ok for report in reports.values()),
        "kind": "multiasset_ohlcv",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": sum(frame.height for frame in frames.values()),
        "metrics": metrics,
        "assets": {
            symbol: json.loads(report.to_json()) for symbol, report in reports.items()
        },
        "calendar_status_counts": dict(calendar["status"].value_counts().iter_rows()),
        "selection_method": config.selection_method,
        "selection_asof_utc": config.selected_at_utc.isoformat(),
        "selection_warning": "retrospective pilot; not a survivorship-free historical universe",
    }
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True) + "\n")
    if not quality["ok"]:
        raise ValueError(f"multi-asset quality failed; inspect {quality_path}")
    combined = pl.concat(list(frames.values())).sort(["timestamp", "symbol"])
    path = project_root / "data/processed" / f"{stem}.parquet"
    calendar_path = project_root / "data/interim/multiasset" / f"{stem}-calendar.parquet"
    _write_versioned(combined, path)
    _write_versioned(calendar, calendar_path)
    data_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    calendar_hash = hashlib.sha256(calendar_path.read_bytes()).hexdigest()
    manifest_path = quality_path.with_name(f"{stem}-manifest.json")
    manifest = {
        **source_identity,
        "data_path": str(path), "data_sha256": data_hash,
        "calendar_path": str(calendar_path), "calendar_sha256": calendar_hash,
        "quality_path": str(quality_path), "rows": combined.height,
        "assets": [asset.model_dump(mode="json") for asset in config.assets],
        "source_archives": {
            symbol: [asdict(part) for part in parts]
            for symbol, parts in parts_by_asset.items()
        },
        "interval": "[start,end)",
        "bar_timestamp": "UTC open; features usable only after the bar closes",
        "selection_warning": quality["selection_warning"],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return MultiAssetDataset(path, calendar_path, quality_path, manifest_path,
                             data_hash, combined.height)
