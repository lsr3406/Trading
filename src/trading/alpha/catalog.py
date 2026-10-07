"""Point-in-time OHLCV factor families for broad, controlled research."""

from collections.abc import Sequence
from dataclasses import dataclass, field

import polars as pl

from trading.alpha.factors import Factor, FactorBatch
from trading.data.transform import GROUPS

WINDOW_FAMILIES = (
    "momentum", "log_momentum", "ma_gap", "ema_gap", "return_volatility",
    "downside_volatility", "price_range", "donchian_position",
    "bollinger_z", "cutler_rsi", "volume_ratio", "volume_volatility",
    "dollar_volume", "volume_return_correlation", "parkinson_volatility",
    "efficiency_ratio", "amihud_proxy", "flow_proxy", "breakout_distance",
    "pullback_distance", "typical_price_gap",
)
BAR_FAMILIES = (
    "bar_body", "bar_range", "upper_wick", "lower_wick", "close_location",
    "true_range", "open_gap",
)
DEFAULT_WINDOWS = (6, 12, 24, 60, 120)


def _finite_lagged(expression: pl.Expr) -> pl.Expr:
    """Expose a completed bar's finite value at the following bar open."""
    lagged = expression.shift(1).over(GROUPS)
    return pl.when(lagged.is_finite()).then(lagged).otherwise(None)


def _raw_window(family: str, window: int) -> pl.Expr:
    """Build one current-close formula; the caller applies the causal lag."""
    close, high, low = pl.col("close"), pl.col("high"), pl.col("low")
    volume = pl.col("volume")
    prior = close.shift(1)
    simple_return = close / prior - 1
    log_return = (close / prior).log()
    mean_close = close.rolling_mean(window)
    std_close = close.rolling_std(window)
    max_high = high.rolling_max(window)
    min_low = low.rolling_min(window)
    delta = close - prior
    positive = delta.clip(lower_bound=0).rolling_sum(window)
    absolute = delta.abs().rolling_sum(window)
    dollar = close * volume
    location = (2 * close - high - low) / (high - low)
    formulas: dict[str, pl.Expr] = {
        "momentum": close / close.shift(window) - 1,
        "log_momentum": (close / close.shift(window)).log(),
        "ma_gap": close / mean_close - 1,
        "ema_gap": close / close.ewm_mean(
            span=window, adjust=False, min_samples=window
        ) - 1,
        "return_volatility": log_return.rolling_std(window),
        "downside_volatility": (
            log_return.clip(upper_bound=0).pow(2).rolling_mean(window).sqrt()
        ),
        "price_range": (max_high - min_low) / close,
        "donchian_position": (close - min_low) / (max_high - min_low),
        "bollinger_z": (close - mean_close) / std_close,
        "cutler_rsi": 100 * positive / absolute,
        "volume_ratio": volume / volume.rolling_mean(window),
        "volume_volatility": volume.rolling_std(window) / volume.rolling_mean(window),
        "dollar_volume": dollar.rolling_mean(window),
        "volume_return_correlation": pl.rolling_corr(
            simple_return, volume / volume.shift(1) - 1, window_size=window
        ),
        "parkinson_volatility": (
            (high / low).log().pow(2).rolling_mean(window) / (4 * 0.6931471805599453)
        ).sqrt(),
        "efficiency_ratio": (close - close.shift(window)).abs() / absolute,
        "amihud_proxy": (simple_return.abs() / dollar).rolling_mean(window),
        "flow_proxy": (location * volume).rolling_sum(window) / volume.rolling_sum(window),
        "breakout_distance": close / max_high.shift(1) - 1,
        "pullback_distance": close / min_low.shift(1) - 1,
        "typical_price_gap": close / ((high + low + close) / 3).rolling_mean(window) - 1,
    }
    return formulas[family]


def _raw_bar(family: str) -> pl.Expr:
    """Build a dimensionless completed-bar geometry feature."""
    open_, high, low, close = (
        pl.col("open"), pl.col("high"), pl.col("low"), pl.col("close")
    )
    previous_close = close.shift(1)
    formulas = {
        "bar_body": (close - open_) / open_,
        "bar_range": (high - low) / close,
        "upper_wick": (high - pl.max_horizontal(open_, close)) / close,
        "lower_wick": (pl.min_horizontal(open_, close) - low) / close,
        "close_location": (2 * close - high - low) / (high - low),
        "true_range": pl.max_horizontal(
            high - low, (high - previous_close).abs(), (low - previous_close).abs()
        ) / previous_close,
        "open_gap": open_ / previous_close - 1,
    }
    return formulas[family]


@dataclass(frozen=True, slots=True)
class CatalogFactor(Factor):
    """One registered formula with an explicit complete-bar availability lag."""

    family: str
    window: int | None = None
    name: str = field(init=False)

    def __post_init__(self) -> None:
        """Reject unknown or ill-defined factor specifications."""
        if self.family in WINDOW_FAMILIES:
            if self.window is None or self.window < 3:
                raise ValueError("windowed factors require a window of at least three bars")
        elif self.family in BAR_FAMILIES:
            if self.window is not None:
                raise ValueError("bar geometry factors have no window")
        else:
            raise ValueError(f"unknown factor family: {self.family}")
        object.__setattr__(
            self, "name", f"{self.family}_{self.window}"
            if self.window is not None else self.family,
        )

    def expression(self) -> pl.Expr:
        """Calculate the formula within each market, then lag one complete bar."""
        raw = (
            _raw_window(self.family, self.window)
            if self.window is not None else _raw_bar(self.family)
        )
        return _finite_lagged(raw).alias(self.name)


def standard_catalog(windows: Sequence[int] = DEFAULT_WINDOWS) -> FactorBatch:
    """Return the deterministic OHLCV catalog as the existing batch interface."""
    if not windows or len(set(windows)) != len(windows) or any(w < 3 for w in windows):
        raise ValueError("windows must be unique and at least three bars")
    factors = [CatalogFactor(family, window) for family in WINDOW_FAMILIES for window in windows]
    factors.extend(CatalogFactor(family) for family in BAR_FAMILIES)
    return FactorBatch(factors)
