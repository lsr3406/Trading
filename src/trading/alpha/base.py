"""Factor and signal abstraction."""

from abc import ABC, abstractmethod
from collections.abc import Sequence

from trading.domain import Bar, Signal


class Alpha(ABC):
    """Transform historical bars into time-stamped signals."""

    @abstractmethod
    def compute(self, bars: Sequence[Bar]) -> Sequence[Signal]:
        """Compute signals using only observations at or before each signal time."""
