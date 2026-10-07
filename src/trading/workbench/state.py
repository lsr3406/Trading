"""Project artifact inventory for the local research workbench.

Only named, project-owned artifacts are exposed. No user-supplied filesystem path
is read, and this module never imports or invokes a broker.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
import yaml

from trading.alpha.catalog import BAR_FAMILIES, WINDOW_FAMILIES

ReportKind = Literal["single", "catalog"]


def _read_json(path: Path | None) -> dict[str, Any] | None:
    """Read a known JSON artifact, returning no data when it does not exist."""
    if path is None or not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _latest(paths: list[Path]) -> Path | None:
    """Choose the latest modified named artifact deterministically."""
    return max(paths, key=lambda path: (path.stat().st_mtime_ns, str(path)), default=None)


def latest_report(root: Path, kind: ReportKind) -> dict[str, Any] | None:
    """Load the newest report of a known research kind."""
    prefix = "single-asset-" if kind == "single" else "catalog-"
    candidates = list((root / "research/runs").glob(f"{prefix}*/report.json"))
    return _read_json(_latest(candidates))


def _timestamp(value: object) -> str | None:
    """Convert a Polars timestamp or scalar to a JSON-friendly value."""
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else str(value)


def datasets(root: Path) -> list[dict[str, Any]]:
    """Inventory normalized local Parquet files without loading full tables."""
    inventory: list[dict[str, Any]] = []
    for path in sorted((root / "data/processed").glob("*.parquet")):
        item: dict[str, Any] = {"id": path.name, "bytes": path.stat().st_size}
        try:
            scan = pl.scan_parquet(path)
            columns = set(scan.collect_schema().names())
            expressions: list[pl.Expr] = [pl.len().alias("rows")]
            for column in ("timestamp", "symbol", "exchange", "timeframe"):
                if column in columns:
                    if column == "timestamp":
                        expressions.extend((
                            pl.col(column).min().alias("start"),
                            pl.col(column).max().alias("end"),
                        ))
                    else:
                        expressions.append(pl.col(column).drop_nulls().first().alias(column))
            row = scan.select(expressions).collect().row(0, named=True)
            item.update({key: _timestamp(value) if key in {"start", "end"} else value
                         for key, value in row.items()})
            item["columns"] = sorted(columns)
        except (OSError, ValueError, pl.exceptions.PolarsError) as error:
            item["error"] = str(error)
        quality = _read_json(root / "research/reports" / f"{path.stem}-quality.json")
        item["quality"] = quality
        inventory.append(item)
    return inventory


def primary_dataset(root: Path) -> dict[str, Any] | None:
    """Prefer the frozen BTC dataset, then the newest valid OHLCV file."""
    items = [
        item for item in datasets(root)
        if "error" not in item and "close" in item.get("columns", [])
    ]
    frozen = [item for item in items if str(item["id"]).startswith("btcusdt-4h-")]
    candidates = frozen or items
    return max(candidates, key=lambda item: str(item["id"]), default=None)


def price_series(
    root: Path, dataset_id: str | None = None, max_points: int = 480
) -> dict[str, Any]:
    """Sample closing prices from a named, inventoried dataset for display."""
    items = datasets(root)
    selected = (
        next((item for item in items if item["id"] == dataset_id), None)
        if dataset_id else primary_dataset(root)
    )
    if selected is None or "error" in selected:
        return {"dataset": None, "points": []}
    path = root / "data/processed" / str(selected["id"])
    frame = pl.read_parquet(path, columns=["timestamp", "close"]).sort("timestamp")
    if frame.is_empty():
        return {"dataset": selected["id"], "points": []}
    step = max(1, (frame.height + max_points - 1) // max_points)
    sampled = frame[::step]
    return {
        "dataset": selected["id"],
        "points": [{"time": _timestamp(time), "close": close}
                   for time, close in sampled.iter_rows()],
    }


def catalog(root: Path) -> dict[str, Any]:
    """Expose the declared candidate family and its latest measured outcomes."""
    path = root / "config/factor_strategy.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(raw, dict):
        raw = {}
    windows = raw.get("windows", [])
    if not isinstance(windows, list):
        windows = []
    factors = [f"{family}_{window}" for family in WINDOW_FAMILIES for window in windows]
    factors.extend(BAR_FAMILIES)
    strategies = raw.get("strategies", [])
    return {
        "factors": factors,
        "strategies": strategies if isinstance(strategies, list) else [],
        "correction": raw.get("correction"),
        "significance_level": raw.get("significance_level"),
        "report": latest_report(root, "catalog"),
    }


def roadmap(root: Path) -> dict[str, Any]:
    """Read the curated next-source plan rather than arbitrary YAML files."""
    path = root / "config/data_sources.yaml"
    if not path.exists():
        return {"checked_on": None, "sources": []}
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {"checked_on": None, "sources": []}


def overview(root: Path) -> dict[str, Any]:
    """Summarize current research evidence and execution readiness."""
    single = latest_report(root, "single")
    batch = latest_report(root, "catalog")
    items = datasets(root)
    primary = primary_dataset(root)
    paper_path = root / "config/execution.paper.yaml"
    paper = yaml.safe_load(paper_path.read_text(encoding="utf-8")) if paper_path.exists() else {}
    paper = paper if isinstance(paper, dict) else {}
    required = (
        "starting_cash", "maker_fee_bps", "taker_fee_bps", "limit_fill_probability",
        "slippage_probability", "latency_ms", "max_market_age_ms", "max_spread_bps",
        "max_price_deviation_bps", "max_position_fraction",
    )
    return {
        "project": root.name,
        "dataset_count": len(items),
        "primary_dataset": primary,
        "single_report": {
            "generated_at_utc": single.get("generated_at_utc"),
            "final_holdout": single.get("final_holdout"),
            "protocol": single.get("protocol"),
        } if single else None,
        "catalog_report": {
            "generated_at_utc": batch.get("generated_at_utc"),
            "factor_count": batch.get("factor_count"),
            "factor_significant_count": batch.get("factor_significant_count"),
            "strategy_count": batch.get("strategy_count"),
            "selected_strategy": batch.get("selected_strategy"),
            "holdout_status": batch.get("holdout_status"),
        } if batch else None,
        "execution": {
            "mode": "disabled",
            "paper_engine": (
                "available_unconfigured" if any(paper.get(key) is None for key in required)
                else "configured"
            ),
            "live": "unavailable",
            "missing_paper_fields": [key for key in required if paper.get(key) is None],
        },
    }
