"""Market-data source abstraction."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime

from trading.domain import Bar


class DataProvider(ABC):
    """Supply timestamped bars without changing source history in place."""

    @abstractmethod
    def get_bars(self, symbol: str, start: datetime, end: datetime) -> Sequence[Bar]:
        """Return bars in [start, end), ordered by timestamp in UTC."""
