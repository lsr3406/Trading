"""Mandatory paper pre-trade market anomaly and position checks."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from trading.configuration import Settings
from trading.domain import OrderRequest, PortfolioSnapshot
from trading.execution.gate import ExecutionDenied, authorize_paper_order
from trading.execution.monitor import ExecutionMonitor
from trading.execution.profile import PaperProfile
from trading.risk.base import RiskDecision


@dataclass(frozen=True, slots=True)
class MarketObservation:
    """Latest observed book and reference price for one exact symbol."""

    symbol: str
    observed_at: datetime
    bid: Decimal
    ask: Decimal
    reference_price: Decimal

    def __post_init__(self) -> None:
        """Reject non-UTC, nonpositive, or crossed quote data."""
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() != timedelta(0):
            raise ValueError("market timestamp must be UTC")
        if not self.symbol or min(self.bid, self.ask, self.reference_price) <= 0:
            raise ValueError("invalid market observation")
        if self.bid >= self.ask:
            raise ValueError("crossed or locked market")


class RealtimeRiskMiddleware:
    """Check market health and per-symbol exposure before the existing gate."""

    def __init__(
        self, settings: Settings, profile: PaperProfile, monitor: ExecutionMonitor | None = None
    ) -> None:
        """Require a paper profile with explicit anomaly thresholds."""
        profile.assert_calibrated()
        if settings.execution.mode != "paper":
            raise ExecutionDenied("runtime execution mode must be paper")
        self.settings = settings
        self.profile = profile
        self.monitor = monitor
        self.logger = logging.getLogger("trading.execution.risk")

    def authorize(
        self,
        order: OrderRequest,
        portfolio: PortfolioSnapshot,
        market: MarketObservation,
        *,
        current_symbol_exposure: Decimal,
    ) -> RiskDecision:
        """Fail closed on stale/crossed/anomalous quotes or concentrated exposure."""
        profile = self.profile
        assert profile.max_market_age_ms is not None
        assert profile.max_spread_bps is not None
        assert profile.max_price_deviation_bps is not None
        assert profile.max_position_fraction is not None
        reason: str | None = None
        age_ms = (order.submitted_at - market.observed_at).total_seconds() * 1000
        if market.symbol != order.symbol:
            reason = "market symbol differs from order"
        elif age_ms < 0 or age_ms > profile.max_market_age_ms:
            reason = "market observation is stale or from the future"
        elif current_symbol_exposure < 0:
            reason = "invalid current symbol exposure"
        elif (market.ask - market.bid) / market.reference_price * 10_000 > profile.max_spread_bps:
            reason = "market spread limit exceeded"
        elif (
            abs(order.limit_price - market.reference_price) / market.reference_price * 10_000
            > profile.max_price_deviation_bps
        ):
            reason = "order price deviates from reference"
        elif (
            current_symbol_exposure + order.notional
            > portfolio.equity * profile.max_position_fraction
        ):
            reason = "symbol position limit exceeded"
        if reason is not None:
            self.logger.warning("paper order denied: %s", reason)
            if self.monitor is not None:
                self.monitor.record_risk_denial(
                    "stale_market" if "stale" in reason else
                    "spread" if "spread" in reason else
                    "price_jump" if "deviates" in reason else
                    "position" if "position" in reason else "other"
                )
            raise ExecutionDenied(reason)
        try:
            decision = authorize_paper_order(self.settings, order, portfolio)
        except ExecutionDenied:
            if self.monitor is not None:
                self.monitor.record_risk_denial("portfolio")
            raise
        self.logger.info("paper order authorized: %s", order.client_order_id)
        return decision
