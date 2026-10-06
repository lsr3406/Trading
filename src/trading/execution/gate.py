"""Fail-closed execution authorization boundary."""

from trading.configuration import Settings
from trading.domain import OrderRequest, PortfolioSnapshot
from trading.risk.base import RiskDecision
from trading.risk.limits import LimitRiskEngine


class ExecutionDenied(RuntimeError):
    """An order was blocked by the execution mode or pre-trade risk limits."""


def authorize_paper_order(
    settings: Settings, order: OrderRequest, portfolio: PortfolioSnapshot
) -> RiskDecision:
    """Authorize a paper order only after all configured limits pass.

    Live mode deliberately has no authorization path in this scaffold. A future
    live adapter requires separate validation evidence and additional controls.
    """
    if settings.execution.mode != "paper":
        raise ExecutionDenied("execution is disabled; live trading is unavailable")
    age_ms = (order.submitted_at - portfolio.as_of).total_seconds() * 1000
    if age_ms < 0:
        raise ExecutionDenied("portfolio snapshot is from the future")
    if settings.risk.max_snapshot_age_ms is None:
        raise ExecutionDenied("risk configuration incomplete: snapshot freshness limit is missing")
    if age_ms > settings.risk.max_snapshot_age_ms:
        raise ExecutionDenied("portfolio snapshot is stale")
    decision = LimitRiskEngine(settings.risk).evaluate(order, portfolio)
    if not decision.approved:
        raise ExecutionDenied(decision.reason)
    return decision
