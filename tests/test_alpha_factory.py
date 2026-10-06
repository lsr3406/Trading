"""Factor timing, statistical correction, and reserved-holdout tests."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import polars as pl
import pytest

from trading.alpha.evaluation import (
    adjust_pvalues,
    block_bootstrap_pvalue,
    ic_series,
    run_factor_study,
)
from trading.alpha.factors import FactorBatch, LaggedMomentum, LaggedVolatility
from trading.alpha.report import StudyContext, render_factor_report
from trading.alpha.vectorbt_scan import scan_thresholds
from trading.configuration import CostSettings, ValidationSettings
from trading.data.schema import frame_from_rows

START = datetime(2025, 1, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _panel(periods: int = 32) -> pl.DataFrame:
    """Create four complete, timestamp-aligned market histories."""
    rows = []
    for asset in range(4):
        for index in range(periods):
            close = 100.0 + asset * 10 + index * (asset + 1) + (index % 3) * asset
            rows.append(
                {"exchange": "fake", "symbol": f"A{asset}/USD", "timeframe": "1h",
                 "timestamp": START + index * HOUR,
                 "open": close, "high": close + 1, "low": close - 1,
                 "close": close, "volume": 100.0,
                 "observed_at": START + (periods + 1) * HOUR}
            )
    return frame_from_rows("ohlcv", rows)


def test_builtin_factors_are_lagged_and_require_complete_bars() -> None:
    """Current close changes do not alter its row's factor values."""
    bars = _panel()
    batch = FactorBatch([LaggedMomentum(2), LaggedVolatility(3)])
    initial = batch.compute(bars)
    point = START + 10 * HOUR
    original = initial.filter((pl.col("symbol") == "A0/USD") & (pl.col("timestamp") == point))
    changed = bars.with_columns(
        pl.when((pl.col("symbol") == "A0/USD") & (pl.col("timestamp") == point))
        .then(pl.col("close") * 2).otherwise(pl.col("close")).alias("close"),
        pl.when((pl.col("symbol") == "A0/USD") & (pl.col("timestamp") == point))
        .then(pl.col("high") * 2).otherwise(pl.col("high")).alias("high"),
    )
    revised = batch.compute(changed).filter(
        (pl.col("symbol") == "A0/USD") & (pl.col("timestamp") == point)
    )
    assert original["momentum_2"][0] == revised["momentum_2"][0]
    assert original["volatility_3"][0] == revised["volatility_3"][0]
    gap = bars.filter(~((pl.col("symbol") == "A0/USD") & (pl.col("timestamp") == point)))
    with pytest.raises(ValueError, match="incomplete bar grid"):
        batch.compute(gap)


def test_multiple_comparison_adjustments_and_bootstrap_seed() -> None:
    """Correction covers the whole candidate family and bootstrap is repeatable."""
    assert adjust_pvalues((0.01, 0.02, 0.2), "bonferroni") == pytest.approx((0.03, 0.06, 0.6))
    assert adjust_pvalues((0.01, 0.02, 0.2), "fdr_bh") == pytest.approx((0.03, 0.03, 0.2))
    values = (0.2, 0.4, 0.1, 0.3, 0.2, 0.5, 0.3, 0.1)
    assert block_bootstrap_pvalue(values, block=2, resamples=199, seed=7) == (
        block_bootstrap_pvalue(values, block=2, resamples=199, seed=7)
    )


def test_ic_and_final_holdout_are_bounded_by_split() -> None:
    """The final holdout is reserved before training-side selection."""
    bars = _panel()
    batch = FactorBatch([LaggedMomentum(1), LaggedMomentum(2)])
    validation = ValidationSettings(
        method="walk_forward", holdout_fraction=0.25,
        multiple_testing="bonferroni", significance_level=0.05,
    )
    study = run_factor_study(
        bars, batch, validation, train_periods=10, test_periods=4,
        horizon=1, embargo_periods=1, bootstrap_resamples=99, seed=13,
    )
    assert study.folds
    assert study.final_holdout_start == (START + 24 * HOUR).isoformat()
    assert all(fold.test_end_exclusive <= study.final_holdout_start for fold in study.folds)
    features = batch.compute(bars)
    assert len(ic_series(features, "momentum_1", horizon=1)) > 0
    with pytest.raises(ValueError, match="embargo"):
        run_factor_study(
            bars, batch, validation, train_periods=10, test_periods=4,
            horizon=2, embargo_periods=1, bootstrap_resamples=99,
        )
    report = render_factor_report(
        StudyContext(
            "fixture study", "fixture-v1", "data-hash", "config-hash", "four assets",
            "UTC completed bars", "fees and slippage calibrated", "two momentum factors",
            "10/4 periods, one-bar embargo, Bonferroni, 99 resamples", 13,
            "Synthetic data; no trading claim.",
        ),
        study,
    )
    assert "Reserved final holdout" in report
    assert "momentum_1" in report
    assert "Synthetic data" in report


def test_vectorbt_screen_rejects_unmodelled_costs() -> None:
    """A fast scan cannot quietly omit funding or latency assumptions."""
    costs = CostSettings(
        fee_bps=Decimal("1"), slippage_bps=Decimal("2"),
        funding_bps=Decimal("3"), latency_ms=0,
    )
    single = FactorBatch([LaggedMomentum(1)]).compute(_panel().filter(pl.col("symbol") == "A0/USD"))
    with pytest.raises(ValueError, match="funding or latency"):
        scan_thresholds(single, "momentum_1", (0.0, 0.1), costs)


def test_vectorbt_screen_runs_offline_parameter_batch() -> None:
    """The optional vectorbt adapter returns one net result per trial."""
    costs = CostSettings(
        fee_bps=Decimal("1"), slippage_bps=Decimal("2"),
        funding_bps=Decimal("0"), latency_ms=0,
    )
    single = FactorBatch([LaggedMomentum(1)]).compute(
        _panel().filter(pl.col("symbol") == "A0/USD")
    )
    results = scan_thresholds(single, "momentum_1", (0.0, 0.1), costs)
    assert len(results) == 2
    assert [result.threshold for result in results] == [0.0, 0.1]
