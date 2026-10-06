"""Cross-sectional IC, block-bootstrap inference, and walk-forward selection."""

import math
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

import polars as pl

from trading.alpha.factors import FactorBatch
from trading.configuration import ValidationSettings
from trading.data.transform import GROUPS, interval_milliseconds

Correction = Literal["bonferroni", "fdr_bh"]


@dataclass(frozen=True, slots=True)
class IcSummary:
    """Cross-sectional rank IC summary across distinct decision times."""

    mean_ic: float | None
    ic_ir: float | None
    periods: int


@dataclass(frozen=True, slots=True)
class FactorTrial:
    """One candidate's training-only statistics within one fold."""

    name: str
    train: IcSummary
    raw_p: float
    adjusted_p: float


@dataclass(frozen=True, slots=True)
class WalkForwardResult:
    """One chronological fold with a training-selected OOS candidate."""

    fold: int
    train_start: str
    train_end_exclusive: str
    test_start: str
    test_end_exclusive: str
    trials: tuple[FactorTrial, ...]
    selected: str | None
    test: IcSummary | None


def _ranks(values: list[float]) -> list[float]:
    """Assign average ranks to ties for Spearman correlation."""
    ranked = [0.0] * len(values)
    ordered = sorted(range(len(values)), key=values.__getitem__)
    position = 0
    while position < len(ordered):
        stop = position + 1
        while stop < len(ordered) and values[ordered[stop]] == values[ordered[position]]:
            stop += 1
        average = (position + 1 + stop) / 2
        for index in ordered[position:stop]:
            ranked[index] = average
        position = stop
    return ranked


def _spearman(x: list[float], y: list[float]) -> float | None:
    """Return Spearman rho or None for constant inputs."""
    left, right = _ranks(x), _ranks(y)
    mx, my = math.fsum(left) / len(left), math.fsum(right) / len(right)
    covariance = math.fsum((a - mx) * (b - my) for a, b in zip(left, right, strict=True))
    vx = math.fsum((a - mx) ** 2 for a in left)
    vy = math.fsum((b - my) ** 2 for b in right)
    return covariance / math.sqrt(vx * vy) if vx > 0 and vy > 0 else None


def ic_series(
    frame: pl.DataFrame, factor_name: str, *, horizon: int = 1, min_assets: int = 3
) -> tuple[float, ...]:
    """Calculate timestamp-wise Spearman IC using labels inside this frame only.

    A factor row at t is compared with close[t+h]/close[t]-1. Factor definitions
    must only use observations available before t. Trailing h rows per asset have
    no label and are omitted; folds must be sliced before calling this function.
    """
    if horizon < 1 or min_assets < 3:
        raise ValueError("horizon must be positive and min_assets at least three")
    required = {factor_name, "close", "timestamp", *GROUPS}
    if required - set(frame.columns):
        raise ValueError(f"missing columns: {sorted(required - set(frame.columns))}")
    if frame["timeframe"].n_unique() != 1:
        raise ValueError("IC requires one common timeframe")
    ordered = frame.sort([*GROUPS, "timestamp"])
    labeled = ordered.with_columns(
        (pl.col("close").shift(-horizon).over(GROUPS) / pl.col("close") - 1)
        .alias("_forward_return")
    )
    results: list[float] = []
    for group in labeled.partition_by("timestamp", maintain_order=True):
        pairs = list(group.select(factor_name, "_forward_return").drop_nulls().iter_rows())
        pairs = [pair for pair in pairs if all(math.isfinite(float(v)) for v in pair)]
        if len(pairs) < min_assets:
            continue
        rho = _spearman([float(pair[0]) for pair in pairs], [float(pair[1]) for pair in pairs])
        if rho is not None:
            results.append(rho)
    return tuple(results)


def summarize_ic(values: tuple[float, ...]) -> IcSummary:
    """Compute ordinary IC mean and unannualized IC information ratio."""
    if not values:
        return IcSummary(None, None, 0)
    mean = math.fsum(values) / len(values)
    variance = math.fsum((value - mean) ** 2 for value in values) / (len(values) - 1)
    standard = math.sqrt(variance) if len(values) > 1 else 0.0
    return IcSummary(mean, mean / standard if standard > 0 else None, len(values))


def block_bootstrap_pvalue(
    values: tuple[float, ...], *, block: int, resamples: int, seed: int
) -> float:
    """Test a zero-mean null by resampling centered contiguous IC blocks.

    The block must cover overlapping forward-return horizons. This is a screening
    statistic, not a claim that market IC is stationary or independent.
    """
    if block < 1 or resamples < 99:
        raise ValueError("block must be positive and resamples at least 99")
    n = len(values)
    if n < max(5, block * 2):
        return 1.0
    observed = abs(math.fsum(values) / n)
    mean = math.fsum(values) / n
    centered = [value - mean for value in values]
    rng = random.Random(seed)
    exceedances = 0
    for _ in range(resamples):
        sample: list[float] = []
        while len(sample) < n:
            start = rng.randrange(n)
            sample.extend(centered[(start + offset) % n] for offset in range(block))
        if abs(math.fsum(sample[:n]) / n) >= observed:
            exceedances += 1
    return (exceedances + 1) / (resamples + 1)


def adjust_pvalues(values: tuple[float, ...], method: Correction) -> tuple[float, ...]:
    """Adjust a complete predeclared candidate family by Bonferroni or BH-FDR."""
    if any(not 0 <= value <= 1 for value in values):
        raise ValueError("p-values must lie in [0, 1]")
    count = len(values)
    if method == "bonferroni":
        return tuple(min(1.0, value * count) for value in values)
    if method != "fdr_bh":
        raise ValueError("unknown correction method")
    adjusted = [1.0] * count
    running = 1.0
    for rank, index in reversed(list(enumerate(sorted(range(count), key=values.__getitem__), 1))):
        running = min(running, values[index] * count / rank)
        adjusted[index] = running
    return tuple(adjusted)


def walk_forward_evaluate(
    frame: pl.DataFrame,
    batch: FactorBatch,
    validation: ValidationSettings,
    *,
    train_periods: int,
    test_periods: int,
    horizon: int = 1,
    embargo_periods: int = 1,
    min_assets: int = 3,
    bootstrap_resamples: int = 999,
    seed: int = 0,
) -> tuple[WalkForwardResult, ...]:
    """Select factors using training folds and measure held-out IC once per fold.

    All candidate factors are computed from the full chronological history only
    because their definitions are stateless and strictly lagged. Selection and
    significance tests use training data alone; labels never cross fold bounds.
    """
    if train_periods < 6 or test_periods < 2 or horizon < 1 or embargo_periods < horizon:
        raise ValueError("invalid fold widths or embargo shorter than label horizon")
    features = batch.compute(frame)
    times = features["timestamp"].unique().sort().to_list()
    output: list[WalkForwardResult] = []
    stop = train_periods + embargo_periods + test_periods
    for fold, end in enumerate(range(stop, len(times) + 1, test_periods)):
        train_start = end - stop
        train_end = train_start + train_periods
        test_start = train_end + embargo_periods
        test_end = test_start + test_periods
        training = features.filter(
            (pl.col("timestamp") >= times[train_start])
            & (pl.col("timestamp") < times[train_end])
        )
        testing = features.filter(
            (pl.col("timestamp") >= times[test_start])
            & (pl.col("timestamp") <= times[test_end - 1])
        )
        trial_data: list[tuple[str, IcSummary, float]] = []
        for candidate in batch.factors:
            series = ic_series(training, candidate.name, horizon=horizon, min_assets=min_assets)
            summary = summarize_ic(series)
            raw_p = block_bootstrap_pvalue(
                series, block=horizon, resamples=bootstrap_resamples,
                seed=seed + fold * len(batch.factors) + len(trial_data),
            )
            trial_data.append((candidate.name, summary, raw_p))
        adjusted = adjust_pvalues(
            tuple(item[2] for item in trial_data), validation.multiple_testing
        )
        trials = tuple(
            FactorTrial(name, summary, raw_p, adjusted[index])
            for index, (name, summary, raw_p) in enumerate(trial_data)
        )
        eligible = [
            trial for trial in trials
            if trial.adjusted_p <= validation.significance_level and trial.train.mean_ic is not None
        ]
        selected = (
            max(eligible, key=lambda item: abs(item.train.mean_ic or 0.0))
            if eligible else None
        )
        test = (
            summarize_ic(
                ic_series(testing, selected.name, horizon=horizon, min_assets=min_assets)
            )
            if selected else None
        )
        end_exclusive = (
            times[test_end] if test_end < len(times)
            else times[-1]
            + timedelta(milliseconds=interval_milliseconds(str(features["timeframe"][0])))
        )
        output.append(
            WalkForwardResult(
                fold=fold,
                train_start=times[train_start].isoformat(),
                train_end_exclusive=times[train_end].isoformat(),
                test_start=times[test_start].isoformat(),
                test_end_exclusive=end_exclusive.isoformat(),
                trials=trials,
                selected=selected.name if selected else None,
                test=test,
            )
        )
    return tuple(output)


@dataclass(frozen=True, slots=True)
class StudyResult:
    """Development folds and one reserved final holdout assessment."""

    folds: tuple[WalkForwardResult, ...]
    final_holdout_start: str
    frozen_factor: str | None
    final_holdout: IcSummary | None


def run_factor_study(
    frame: pl.DataFrame,
    batch: FactorBatch,
    validation: ValidationSettings,
    *,
    train_periods: int,
    test_periods: int,
    horizon: int = 1,
    embargo_periods: int = 1,
    min_assets: int = 3,
    bootstrap_resamples: int = 999,
    seed: int = 0,
) -> StudyResult:
    """Reserve a final holdout before selecting on development data.

    The latest development fold's training-only adjusted p-values freeze the
    candidate. Earlier fold OOS scores are diagnostics, never inputs to this
    selection. The final holdout is evaluated once in this call.
    """
    import math

    features = batch.compute(frame)
    times = features["timestamp"].unique().sort().to_list()
    holdout_count = max(test_periods, math.ceil(len(times) * validation.holdout_fraction))
    boundary = len(times) - holdout_count
    if boundary < train_periods + embargo_periods + test_periods:
        raise ValueError("insufficient history after reserving the final holdout")
    development = frame.filter(pl.col("timestamp") < times[boundary])
    folds = walk_forward_evaluate(
        development, batch, validation,
        train_periods=train_periods, test_periods=test_periods,
        horizon=horizon, embargo_periods=embargo_periods,
        min_assets=min_assets, bootstrap_resamples=bootstrap_resamples, seed=seed,
    )
    frozen = folds[-1].selected if folds else None
    holdout_frame = features.filter(pl.col("timestamp") >= times[boundary])
    holdout = (
        summarize_ic(
            ic_series(holdout_frame, frozen, horizon=horizon, min_assets=min_assets)
        )
        if frozen is not None else None
    )
    return StudyResult(folds, times[boundary].isoformat(), frozen, holdout)
