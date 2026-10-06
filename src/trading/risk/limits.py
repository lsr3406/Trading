"""Conservative pre-trade checks used by the paper execution gate."""

from trading.configuration import RiskSettings
from trading.domain import OrderRequest, PortfolioSnapshot
from trading.risk.base import RiskDecision, RiskEngine


class LimitRiskEngine(RiskEngine):
    """Deny if any hard limit is missing or exceeded.

    Exposure treats every order as additive. A future position-aware engine may
    account for reductions, but must preserve the same fail-closed behavior.
    """

    def __init__(self, limits: RiskSettings) -> None:
        """Store immutable limits loaded from validated configuration."""
        self._limits = limits

    def evaluate(self, order: OrderRequest, portfolio: PortfolioSnapshot) -> RiskDecision:
        """Apply order, daily loss, drawdown, and leverage limits."""
        limits = self._limits
        required = (
            limits.max_order_notional,
            limits.max_daily_loss,
            limits.max_drawdown_fraction,
            limits.max_leverage,
            limits.max_snapshot_age_ms,
        )
        reason = "approved"
        if any(value is None for value in required):
            reason = "risk limits are incomplete"
        elif order.quote_currency != portfolio.base_currency:
            reason = "order and portfolio currencies differ"
        elif limits.max_order_notional is not None and order.notional > limits.max_order_notional:
            reason = "maximum order notional exceeded"
        elif limits.max_daily_loss is not None and portfolio.daily_loss >= limits.max_daily_loss:
            reason = "daily loss limit reached"
        elif (
            limits.max_drawdown_fraction is not None
            and portfolio.drawdown_fraction >= limits.max_drawdown_fraction
        ):
            reason = "drawdown limit reached"
        elif (
            limits.max_leverage is not None
            and portfolio.gross_exposure + order.notional > portfolio.equity * limits.max_leverage
        ):
            reason = "leverage limit exceeded"
        return RiskDecision(order.client_order_id, reason == "approved", reason)
