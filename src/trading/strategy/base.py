"""Target portfolio abstraction."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from trading.domain import PortfolioSnapshot, Signal, TargetPosition


class Strategy(ABC):
    """Map signals and portfolio state to desired positions."""

    @abstractmethod
    def allocate(
        self, signals: Sequence[Signal], portfolio: PortfolioSnapshot
    ) -> Sequence[TargetPosition]:
        """Return desired weights without sending orders."""
