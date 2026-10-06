"""Pre-trade risk decision and engine abstraction."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from trading.domain import OrderRequest, PortfolioSnapshot


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """Bound a risk approval to one specific client order identifier."""

    client_order_id: str
    approved: bool
    reason: str


class RiskEngine(ABC):
    """Evaluate orders against configured hard limits and current state."""

    @abstractmethod
    def evaluate(self, order: OrderRequest, portfolio: PortfolioSnapshot) -> RiskDecision:
        """Return a decision for the exact order before any broker submission."""
