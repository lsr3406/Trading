"""Risk-gated bridge from domain limit orders to Nautilus paper orders."""

from decimal import Decimal
from typing import TYPE_CHECKING

from trading.domain import OrderRequest, PortfolioSnapshot
from trading.execution.gate import ExecutionDenied
from trading.execution.middleware import MarketObservation, RealtimeRiskMiddleware
from trading.execution.monitor import ExecutionMonitor

if TYPE_CHECKING:
    from nautilus_trader.trading import Strategy


class NautilusPaperRouter:
    """Authorize a paper limit order before handing it to NautilusTrader.

    The host strategy supplies a fresh portfolio and market snapshot at every
    decision. NautilusTrader remains the owner of order state transitions.
    """

    def __init__(
        self, strategy: "Strategy", middleware: RealtimeRiskMiddleware,
        monitor: ExecutionMonitor,
    ) -> None:
        """Bind one already-registered Nautilus strategy to mandatory checks."""
        self.strategy = strategy
        self.middleware = middleware
        self.monitor = monitor

    def submit_limit(
        self,
        order: OrderRequest,
        portfolio: PortfolioSnapshot,
        market: MarketObservation,
        *,
        current_symbol_exposure: Decimal,
    ) -> str:
        """Check limits, build a native limit order, and submit to the simulator."""
        from nautilus_trader.model import ClientOrderId, InstrumentId, OrderSide

        self.middleware.authorize(
            order, portfolio, market,
            current_symbol_exposure=current_symbol_exposure,
        )
        expected_id = InstrumentId.from_str(
            f"{order.symbol}.{self.middleware.profile.venue}"
        )
        instrument = self.strategy.cache.instrument(expected_id)
        if instrument is None:
            raise ExecutionDenied("paper instrument is not registered")
        if str(instrument.quote_currency) != order.quote_currency:
            raise ExecutionDenied("paper instrument quote currency differs from order")
        native_order = self.strategy.order_factory.limit(
            instrument_id=expected_id,
            order_side=OrderSide.BUY if order.side == "buy" else OrderSide.SELL,
            quantity=instrument.make_qty(order.quantity, round_down=False),
            price=instrument.make_price(order.limit_price),
            client_order_id=ClientOrderId(order.client_order_id),
        )
        self.strategy.submit_order(native_order)
        self.monitor.record_nautilus_order(native_order)
        return str(native_order.client_order_id)

    def observe_order(self, native_order: object) -> None:
        """Record a subsequent state emitted by NautilusTrader's event callbacks."""
        self.monitor.record_nautilus_order(native_order)
