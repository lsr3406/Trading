"""Broker abstraction for future paper adapters."""

from abc import ABC, abstractmethod

from trading.domain import OrderRequest
from trading.risk.base import RiskDecision


class Broker(ABC):
    """Submit only orders that passed an external execution and risk gate."""

    @abstractmethod
    def submit_order(self, order: OrderRequest, decision: RiskDecision) -> str:
        """Return broker order ID after verifying an order-bound approval."""

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an outstanding broker order if supported."""
