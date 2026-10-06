"""Point-in-time factor definitions over complete Polars OHLCV grids."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

import polars as pl

from trading.data.transform import GROUPS, clean_ohlcv, interval_milliseconds


class Factor(ABC):
    """A named factor computed using only bars completed before its row time.

    Custom implementations must preserve row identity and avoid future-looking
    expressions. Built-in factors are lagged by one full bar.
    """

    name: str

    @abstractmethod
    def expression(self) -> pl.Expr:
        """Return a Polars expression for the factor's named output column."""


class LaggedMomentum(Factor):
    """Prior close divided by the close a fixed number of bars earlier."""

    def __init__(self, lookback: int) -> None:
        """Create a strictly past-looking momentum factor."""
        if lookback < 1:
            raise ValueError("lookback must be positive")
        self.lookback = lookback
        self.name = f"momentum_{lookback}"

    def expression(self) -> pl.Expr:
        """Compute close[t-1] / close[t-lookback-1] - 1."""
        return (
            pl.col("close").shift(1) / pl.col("close").shift(self.lookback + 1) - 1
        ).over(GROUPS).alias(self.name)


class LaggedVolatility(Factor):
    """Standard deviation of prior completed log returns."""

    def __init__(self, window: int) -> None:
        """Require enough completed returns for a sample deviation."""
        if window < 2:
            raise ValueError("window must be at least two")
        self.window = window
        self.name = f"volatility_{window}"

    def expression(self) -> pl.Expr:
        """Compute trailing volatility excluding the current bar return."""
        return (
            pl.col("close").log().diff().shift(1).rolling_std(self.window)
        ).over(GROUPS).alias(self.name)


def require_complete_grid(frame: pl.DataFrame) -> None:
    """Reject irregular or imputed bars before bar-count-based factor research."""
    if "is_imputed" in frame.columns and frame["is_imputed"].fill_null(False).any():
        raise ValueError("factor research requires non-imputed bars")
    for key, group in frame.group_by(GROUPS):
        timeframe = str(key[2])
        step = interval_milliseconds(timeframe)
        times = group["timestamp"].sort().cast(pl.Int64).to_list()
        if any(right - left != step for left, right in zip(times, times[1:], strict=False)):
            raise ValueError(f"incomplete bar grid for {key}")


class FactorBatch:
    """Evaluate several factors over the same sorted, validated bars."""

    def __init__(self, factors: Sequence[Factor]) -> None:
        """Reject empty or duplicate factor sets before a parameter scan."""
        if not factors or len({factor.name for factor in factors}) != len(factors):
            raise ValueError("factors must be nonempty with unique names")
        self.factors = tuple(factors)

    def compute(self, bars: pl.DataFrame) -> pl.DataFrame:
        """Add factor columns without modifying the original OHLCV frame."""
        cleaned = clean_ohlcv(bars)
        require_complete_grid(cleaned)
        if any(factor.name in cleaned.columns for factor in self.factors):
            raise ValueError("factor name conflicts with an input column")
        return cleaned.with_columns([factor.expression() for factor in self.factors])
