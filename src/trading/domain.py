"""Immutable domain values shared by research and execution boundaries."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal


def _require_aware(value: datetime, label: str) -> None:
    """Reject naive timestamps, which make market-data alignment ambiguous."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be in UTC")


@dataclass(frozen=True, slots=True)
class Bar:
    """One adjusted or unadjusted OHLCV observation, as defined by the provider."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self) -> None:
        """Validate timestamp and basic OHLCV consistency."""
        _require_aware(self.timestamp, "timestamp")
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("OHLC prices must be positive")
        if self.volume < 0 or self.low > min(self.open, self.close):
            raise ValueError("invalid low or volume")
        if self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("invalid high or low")


@dataclass(frozen=True, slots=True)
class Signal:
    """A signal whose feature cutoff cannot extend beyond its decision time."""

    symbol: str
    as_of: datetime
    observed_through: datetime
    score: float

    def __post_init__(self) -> None:
        """Enforce time ordering and a finite score."""
        from math import isfinite

        _require_aware(self.as_of, "as_of")
        _require_aware(self.observed_through, "observed_through")
        if self.observed_through > self.as_of:
            raise ValueError("signal uses future observations")
        if not isfinite(self.score):
            raise ValueError("signal score must be finite")


@dataclass(frozen=True, slots=True)
class TargetPosition:
    """Desired portfolio weight for one symbol at a decision time."""

    symbol: str
    as_of: datetime
    weight: Decimal

    def __post_init__(self) -> None:
        """Require an explicit time and a bounded single-asset weight."""
        _require_aware(self.as_of, "as_of")
        if not Decimal("-1") <= self.weight <= Decimal("1"):
            raise ValueError("weight must be between -1 and 1")


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """A limit order request; market orders need a separate risk policy."""

    client_order_id: str
    symbol: str
    quote_currency: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    limit_price: Decimal
    submitted_at: datetime

    def __post_init__(self) -> None:
        """Reject nonpositive order values and ambiguous timestamps."""
        _require_aware(self.submitted_at, "submitted_at")
        if not self.client_order_id or not self.symbol or not self.quote_currency:
            raise ValueError("order identifiers must not be empty")
        if self.quantity <= 0 or self.limit_price <= 0:
            raise ValueError("quantity and limit_price must be positive")

    @property
    def notional(self) -> Decimal:
        """Return the order's limit-price notional in quote currency."""
        return self.quantity * self.limit_price


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    """Portfolio state required for pre-trade risk checks."""

    as_of: datetime
    base_currency: str
    equity: Decimal
    gross_exposure: Decimal
    daily_loss: Decimal
    drawdown_fraction: Decimal

    def __post_init__(self) -> None:
        """Reject invalid state before it reaches the risk engine."""
        _require_aware(self.as_of, "as_of")
        if not self.base_currency:
            raise ValueError("base_currency must not be empty")
        if self.equity <= 0 or self.gross_exposure < 0 or self.daily_loss < 0:
            raise ValueError("invalid equity, exposure, or daily loss")
        if not Decimal("0") <= self.drawdown_fraction <= Decimal("1"):
            raise ValueError("drawdown_fraction must be between 0 and 1")
