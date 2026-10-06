"""Vectorbt threshold sweeps for training-only exploratory screening."""

import importlib
import math
from dataclasses import dataclass
from decimal import Decimal

import polars as pl

from trading.configuration import CostSettings


@dataclass(frozen=True, slots=True)
class ThresholdResult:
    """One training-window threshold's net vectorbt return."""

    threshold: float
    total_return: float


def scan_thresholds(
    frame: pl.DataFrame,
    factor_name: str,
    thresholds: tuple[float, ...],
    costs: CostSettings,
    *,
    init_cash: float = 10_000.0,
) -> tuple[ThresholdResult, ...]:
    """Run a batched long-only vectorbt screen on one market's training bars.

    The factor must have been computed from prior completed bars. Fees and
    slippage are modelled; nonzero funding or latency is rejected because this
    vectorized signal path cannot represent them faithfully. The caller must
    keep this scan inside a training fold and count every threshold as a trial.
    """
    if not thresholds or len(set(thresholds)) != len(thresholds):
        raise ValueError("thresholds must be nonempty and unique")
    if any(not math.isfinite(value) for value in thresholds) or init_cash <= 0:
        raise ValueError("thresholds and starting cash must be finite and valid")
    fee_bps = costs.fee_bps
    slippage_bps = costs.slippage_bps
    funding_bps = costs.funding_bps
    latency_ms = costs.latency_ms
    if any(value is None for value in (fee_bps, slippage_bps, funding_bps, latency_ms)):
        raise ValueError("all costs must be explicitly configured")
    assert fee_bps is not None and slippage_bps is not None
    if funding_bps != Decimal("0") or latency_ms != 0:
        raise ValueError("vectorbt screen cannot model funding or latency")
    required = {"exchange", "symbol", "timeframe", "timestamp", "close", factor_name}
    if required - set(frame.columns):
        raise ValueError(f"missing columns: {sorted(required - set(frame.columns))}")
    if any(frame[column].n_unique() != 1 for column in ("exchange", "symbol", "timeframe")):
        raise ValueError("scan requires one exchange, symbol, and timeframe")
    if frame["timestamp"].is_duplicated().any():
        raise ValueError("duplicate timestamps")
    if "is_imputed" in frame.columns and frame["is_imputed"].fill_null(False).any():
        raise ValueError("imputed bars are not accepted")
    try:
        pandas = importlib.import_module("pandas")
        vectorbt = importlib.import_module("vectorbt")
    except ImportError as error:
        raise RuntimeError("install the research extra: uv sync --extra research") from error
    ordered = frame.sort("timestamp")
    index = pandas.DatetimeIndex(ordered["timestamp"].to_list(), tz="UTC")
    close = pandas.Series(ordered["close"].to_list(), index=index, dtype="float64")
    signal = pandas.Series(ordered[factor_name].to_list(), index=index, dtype="float64")
    columns = [f"trial_{index}" for index in range(len(thresholds))]
    prices = pandas.DataFrame({column: close for column in columns})
    entries = pandas.DataFrame(
        {column: signal > threshold for column, threshold in zip(columns, thresholds, strict=True)}
    )
    exits = pandas.DataFrame(
        {column: signal <= threshold for column, threshold in zip(columns, thresholds, strict=True)}
    )
    portfolio = vectorbt.Portfolio.from_signals(
        prices,
        entries=entries,
        exits=exits,
        fees=float(fee_bps / Decimal("10000")),
        slippage=float(slippage_bps / Decimal("10000")),
        init_cash=init_cash,
    )
    returns = portfolio.total_return()
    return tuple(
        ThresholdResult(threshold, float(returns[column]))
        for threshold, column in zip(thresholds, columns, strict=True)
    )
