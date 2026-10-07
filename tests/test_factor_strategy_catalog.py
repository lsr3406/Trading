"""Offline checks for catalog causality, hypothesis accounting, and integration."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from trading.alpha.catalog import BAR_FAMILIES, WINDOW_FAMILIES, standard_catalog
from trading.data.schema import frame_from_rows
from trading.research.catalog_study import _batch_fold, _folds, run_catalog_study
from trading.research.single_asset import StudyDataset
from trading.research.strategies import (
    ResearchCatalog,
    StrategySpec,
    load_research_catalog,
)
from trading.single_asset_config import load_single_asset_config


def _bars(count: int, *, markets: int = 1) -> pl.DataFrame:
    """Create two complete, nonconstant OHLCV streams without remote data."""
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for market in range(markets):
        for index in range(count):
            close = 100 + market * 50 + index * 0.02 + 3 * np.sin(index / 11)
            open_ = close - 0.25 * np.sin(index / 3)
            rows.append({
                "exchange": "binance", "symbol": f"ASSET{market}/USDT",
                "timeframe": "4h", "timestamp": start + timedelta(hours=4 * index),
                "open": float(open_), "high": float(max(open_, close) + 1),
                "low": float(min(open_, close) - 1), "close": float(close),
                "volume": float(5 + np.sin(index / 7) + market), "observed_at": start,
            })
    return frame_from_rows("ohlcv", rows)


def test_catalog_contains_stable_distinct_completed_bar_factors() -> None:
    """A spike at row t cannot alter a factor available at row t's open."""
    batch = standard_catalog((6, 12))
    assert len(batch.factors) == len(WINDOW_FAMILIES) * 2 + len(BAR_FAMILIES)
    bars = _bars(160, markets=2)
    first = batch.compute(bars)
    altered = bars.with_columns(
        pl.when((pl.col("symbol") == "ASSET0/USDT") &
                (pl.col("timestamp") == datetime(2025, 1, 2, 4, tzinfo=UTC)))
        .then(pl.col("close") * 1.01).otherwise(pl.col("close")).alias("close")
    )
    # A changed close must remain a valid OHLC bar.
    altered = altered.with_columns(
        pl.max_horizontal("high", "close").alias("high")
    )
    second = batch.compute(altered)
    market = first.filter(pl.col("symbol") == "ASSET0/USDT")
    changed = second.filter(pl.col("symbol") == "ASSET0/USDT")
    row = 7
    assert market["momentum_6"][row] == changed["momentum_6"][row]
    assert market["momentum_6"][row + 1] != changed["momentum_6"][row + 1]
    assert market["ema_gap_12"][:12].null_count() == 12
    assert first.filter(pl.col("symbol") == "ASSET1/USDT").equals(
        second.filter(pl.col("symbol") == "ASSET1/USDT")
    )


def test_yaml_declares_finite_complete_candidate_family() -> None:
    """Every configured strategy refers only to available factor columns."""
    catalog = load_research_catalog(Path("config/factor_strategy.yaml"))
    assert len(catalog.strategies) == 23
    assert len({candidate.name for candidate in catalog.strategies}) == 23
    features = standard_catalog(catalog.windows).compute(_bars(160))
    for candidate in catalog.strategies:
        entry, exit_ = candidate.signals(features)
        assert len(entry) == features.height
        assert len(exit_) == features.height
    assert catalog.bootstrap_resamples == 4999


def test_strategy_rejects_invalid_specifications() -> None:
    """Unknown parameters and reversed hysteresis thresholds fail closed."""
    with pytest.raises(ValueError, match="expected parameters"):
        StrategySpec("bad", "momentum", {"window": 12})
    with pytest.raises(ValueError, match="entry must be below exit"):
        StrategySpec("bad", "rsi_revert", {"window": 12, "entry": 60, "exit": 50})
    with pytest.raises(ValueError, match="fast must be shorter"):
        StrategySpec("bad", "ma_cross", {"fast": 60, "slow": 12})


def test_fold_boundaries_separate_training_and_holdout() -> None:
    """No development test or its label crosses into the final fifth."""
    config = load_single_asset_config(Path("config/single_asset.yaml"))
    folds, holdout = _folds(config, 3828)
    assert len(folds) == 4
    assert all(
        fold["train_end_exclusive"] + config.embargo_bars == fold["test_start"]
        for fold in folds
    )
    assert folds[-1]["test_end_exclusive"] <= holdout - config.embargo_bars


def test_catalog_bootstrap_stream_includes_initial_fee() -> None:
    """A cash-only candidate outperforms the first-bar paid baseline at bar one."""
    bars = _bars(4).with_columns(pl.lit(-1.0).alias("momentum_6"))
    candidate = StrategySpec("cash", "momentum", {"window": 6, "threshold": 0.0})
    differences, summary, _ = _batch_fold(
        bars, (candidate,), cash=10_000, notional=100, fee=0.001, slippage=0.001
    )
    assert summary["cash"]["orders"] == 0
    assert differences["cash"][0] > 0


def test_catalog_runner_keeps_holdout_unscored(tmp_path: Path) -> None:
    """A small synthetic run emits corrected families and no holdout score."""
    base = load_single_asset_config(Path("config/single_asset.yaml"))
    config = base.model_copy(update={
        "end": datetime(2025, 6, 1, tzinfo=UTC),
        "train_bars": 300, "test_bars": 180, "bootstrap_resamples": 199,
    })
    bars = _bars(906)
    data_path = tmp_path / "bars.parquet"
    bars.write_parquet(data_path)
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    dataset = StudyDataset(data_path, tmp_path / "manifest.json", tmp_path / "quality.json",
                           digest, bars.height)
    candidates = (
        StrategySpec("mom6", "momentum", {"window": 6, "threshold": 0.0}),
        StrategySpec("rev12", "reversal", {"window": 12, "threshold": 0.02}),
    )
    catalog = ResearchCatalog((6, 12), candidates, "fdr_bh", 0.05, 199)
    report_path = run_catalog_study(config, catalog, dataset, tmp_path)
    report = json.loads(report_path.read_text())
    repeated = json.loads(run_catalog_study(config, catalog, dataset, tmp_path).read_text())
    assert repeated["factor_parquet_sha256"] == report["factor_parquet_sha256"]
    assert report["strategy_count"] == 2
    assert report["factor_count"] == len(WINDOW_FAMILIES) * 2 + len(BAR_FAMILIES)
    assert len(report["strategy_trials"]) == 2
    assert len(report["factor_trials"]) == report["factor_count"]
    assert "final_holdout" not in report
    assert report["holdout_bars"] > 0
    assert all(0 <= row["adjusted_p"] <= 1 for row in report["strategy_trials"])
