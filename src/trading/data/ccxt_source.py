"""Rate-limited, capability-checked CCXT public-market data ingestion."""

import importlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

import polars as pl

from trading.data.schema import frame_from_rows, from_milliseconds, require_utc


class ExchangeLike(Protocol):
    """The small CCXT interface needed for public data and offline fakes."""

    has: Mapping[str, bool | str]
    symbols: Sequence[str] | None

    def load_markets(self) -> Any:
        """Load available unified market symbols."""

    def parse_timeframe(self, timeframe: str) -> int:
        """Return timeframe duration in seconds."""

    def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int, limit: int
    ) -> Sequence[Sequence[int | float | None]]:
        """Fetch a page of OHLCV rows."""

    def fetch_order_book(self, symbol: str, limit: int) -> Mapping[str, Any]:
        """Fetch a current order-book snapshot."""

    def fetch_funding_rate_history(
        self, symbol: str, since: int, limit: int
    ) -> Sequence[Mapping[str, Any]]:
        """Fetch historical funding events for a contract market."""


class UnsupportedMarketData(RuntimeError):
    """The venue does not advertise a required unified CCXT method."""


class IncompletePagination(RuntimeError):
    """A page cap or non-advancing exchange response prevented full retrieval."""


class CcxtPublicSource:
    """Normalize one exchange's public data while preserving observed times.

    A source instance reuses one exchange object so CCXT's per-instance rate limiter
    remains effective. No API credentials are accepted or loaded here.
    """

    def __init__(self, exchange_id: str, exchange: ExchangeLike | None = None) -> None:
        """Create a public CCXT exchange or accept an offline fake for tests."""
        if not exchange_id or not exchange_id.replace("_", "").isalnum():
            raise ValueError("invalid exchange id")
        self.exchange_id = exchange_id
        if exchange is None:
            ccxt = importlib.import_module("ccxt")
            if exchange_id not in ccxt.exchanges:
                raise ValueError(f"unknown CCXT exchange: {exchange_id}")
            exchange = getattr(ccxt, exchange_id)({"enableRateLimit": True, "timeout": 30_000})
        self.exchange = exchange

    def _check(self, method: str, symbol: str) -> None:
        """Check exchange capability and exact unified market symbol."""
        if not self.exchange.has.get(method):
            raise UnsupportedMarketData(f"{self.exchange_id} does not support {method}")
        if self.exchange.symbols is None:
            self.exchange.load_markets()
        if self.exchange.symbols is None or symbol not in self.exchange.symbols:
            raise ValueError(f"{self.exchange_id} has no market {symbol!r}")

    @staticmethod
    def _window(start: datetime, end: datetime, limit: int, max_pages: int) -> tuple[int, int]:
        """Validate a bounded half-open UTC retrieval window."""
        require_utc(start, "start")
        require_utc(end, "end")
        if end <= start or limit <= 0 or max_pages <= 0:
            raise ValueError("end must follow start; limit and max_pages must be positive")
        return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        limit: int = 500,
        max_pages: int = 100,
    ) -> pl.DataFrame:
        """Fetch [start, end) OHLCV pages; fail if the page cap truncates data.

        Gaps and an absent final candle remain visible for quality assessment. The
        caller must not treat a short or empty exchange response as full coverage.
        """
        since, until = self._window(start, end, limit, max_pages)
        self._check("fetchOHLCV", symbol)
        step_ms = self.exchange.parse_timeframe(timeframe) * 1000
        if step_ms <= 0:
            raise ValueError("invalid timeframe")
        rows: list[dict[str, Any]] = []
        cursor = since
        exhausted = False
        for _ in range(max_pages):
            if cursor >= until:
                exhausted = True
                break
            page = self.exchange.fetch_ohlcv(symbol, timeframe, cursor, limit)
            if not page:
                exhausted = True
                break
            observed_at = datetime.now(UTC)
            if any(not item or item[0] is None for item in page):
                raise ValueError("OHLCV page has missing timestamps")
            times = [int(item[0]) for item in page if item[0] is not None]
            new_cursor = max(times) + step_ms
            if new_cursor <= cursor:
                raise IncompletePagination(f"{self.exchange_id} OHLCV did not advance")
            for item in page:
                if item[0] is None:
                    raise ValueError("OHLCV row has missing timestamp")
                timestamp = int(item[0])
                if since <= timestamp < until:
                    rows.append(
                        {
                            "exchange": self.exchange_id,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "timestamp": from_milliseconds(timestamp),
                            "open": item[1],
                            "high": item[2],
                            "low": item[3],
                            "close": item[4],
                            "volume": item[5],
                            "observed_at": observed_at,
                        }
                    )
            cursor = new_cursor
        if not exhausted and cursor < until:
            raise IncompletePagination(f"{self.exchange_id} OHLCV exceeded max_pages")
        return frame_from_rows("ohlcv", rows)

    def fetch_order_book(self, symbol: str, *, depth: int = 20) -> pl.DataFrame:
        """Fetch a present-time snapshot; it is not historical order-book data."""
        if depth <= 0:
            raise ValueError("depth must be positive")
        self._check("fetchOrderBook", symbol)
        payload = self.exchange.fetch_order_book(symbol, depth)
        observed_at = datetime.now(UTC)
        venue_time = payload.get("timestamp")
        rows: list[dict[str, Any]] = []
        for side, key in (("bid", "bids"), ("ask", "asks")):
            for level, pair in enumerate(payload.get(key, [])[:depth]):
                rows.append(
                    {
                        "exchange": self.exchange_id,
                        "symbol": symbol,
                        "observed_at": observed_at,
                        "exchange_timestamp": (
                            from_milliseconds(venue_time) if venue_time is not None else None
                        ),
                        "side": side,
                        "level": level,
                        "price": pair[0],
                        "size": pair[1],
                    }
                )
        return frame_from_rows("orderbook", rows)

    def fetch_funding_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        limit: int = 200,
        max_pages: int = 100,
    ) -> pl.DataFrame:
        """Fetch [start, end) contract funding events where CCXT supports them."""
        since, until = self._window(start, end, limit, max_pages)
        self._check("fetchFundingRateHistory", symbol)
        rows: list[dict[str, Any]] = []
        cursor = since
        exhausted = False
        for _ in range(max_pages):
            if cursor >= until:
                exhausted = True
                break
            page = self.exchange.fetch_funding_rate_history(symbol, cursor, limit)
            if not page:
                exhausted = True
                break
            observed_at = datetime.now(UTC)
            times = [int(item["timestamp"]) for item in page if item.get("timestamp") is not None]
            if not times or max(times) < cursor:
                raise IncompletePagination(f"{self.exchange_id} funding history did not advance")
            for item in page:
                timestamp = item.get("timestamp")
                if timestamp is not None and since <= int(timestamp) < until:
                    rows.append(
                        {
                            "exchange": self.exchange_id,
                            "symbol": symbol,
                            "timestamp": from_milliseconds(int(timestamp)),
                            "funding_rate": item.get("fundingRate"),
                            "observed_at": observed_at,
                        }
                    )
            cursor = max(times) + 1
        if not exhausted and cursor < until:
            raise IncompletePagination(f"{self.exchange_id} funding exceeded max_pages")
        return frame_from_rows("funding", rows)


class MultiExchangeCollector:
    """Collect a common request from multiple explicitly named venues."""

    def __init__(self, sources: Sequence[CcxtPublicSource]) -> None:
        """Require unique source IDs so rows retain unambiguous provenance."""
        if not sources or len({source.exchange_id for source in sources}) != len(sources):
            raise ValueError("sources must be nonempty with unique exchange IDs")
        self.sources = tuple(sources)

    def ohlcv(
        self, symbol: str, timeframe: str, start: datetime, end: datetime, *, limit: int = 500
    ) -> pl.DataFrame:
        """Concatenate venue-tagged bars; fail if any venue retrieval fails."""
        return pl.concat(
            [
                source.fetch_ohlcv(symbol, timeframe, start, end, limit=limit)
                for source in self.sources
            ]
        )

    def order_books(self, symbol: str, *, depth: int = 20) -> pl.DataFrame:
        """Concatenate independently observed present-time books."""
        return pl.concat([source.fetch_order_book(symbol, depth=depth) for source in self.sources])

    def funding_history(
        self, symbol: str, start: datetime, end: datetime, *, limit: int = 200
    ) -> pl.DataFrame:
        """Concatenate funding observations without aligning venue schedules."""
        return pl.concat(
            [
                source.fetch_funding_history(symbol, start, end, limit=limit)
                for source in self.sources
            ]
        )
