"""Structured execution event logging and local Prometheus metrics."""

import logging
from decimal import Decimal
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.exposition import start_http_server

_ORDER_STATUSES = frozenset(
    {
        "INITIALIZED", "DENIED", "EMULATED", "RELEASED", "SUBMITTED", "ACCEPTED",
        "REJECTED", "CANCELED", "EXPIRED", "TRIGGERED", "PENDING_UPDATE",
        "PENDING_CANCEL", "PARTIALLY_FILLED", "FILLED", "VOIDED",
    }
)


class ExecutionMonitor:
    """Observe Nautilus order states without replacing its state machine."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Create isolated metrics so paper test runs cannot mix counters."""
        self.registry = registry or CollectorRegistry()
        self.logger = logging.getLogger("trading.execution.monitor")
        self.order_events = Counter(
            "trading_order_events_total", "Order lifecycle events", ["status"],
            registry=self.registry,
        )
        self.risk_denials = Counter(
            "trading_risk_denials_total", "Pre-trade risk denials", ["reason"],
            registry=self.registry,
        )
        self.fills = Counter("trading_fills_total", "Observed fill events", registry=self.registry)
        self.fees = Counter(
            "trading_fees_quote_total", "Observed fees in account quote currency",
            registry=self.registry,
        )
        self.slippage = Histogram(
            "trading_slippage_bps", "Observed signed adverse slippage in basis points",
            registry=self.registry,
        )
        self.equity = Gauge(
            "trading_equity_quote", "Current account equity in quote currency",
            registry=self.registry,
        )
        self.drawdown = Gauge(
            "trading_drawdown_fraction", "Current account drawdown fraction",
            registry=self.registry,
        )
        self.order_latency = Histogram(
            "trading_order_latency_ms", "Order round-trip latency in milliseconds",
            registry=self.registry,
        )

    def record_order_status(self, order_id: str, status: str) -> None:
        """Record one status emitted by NautilusTrader's own order state machine."""
        if status not in _ORDER_STATUSES or not order_id:
            raise ValueError("unknown order status or empty order ID")
        self.order_events.labels(status=status).inc()
        self.logger.info("order_id=%s status=%s", order_id, status)

    def record_nautilus_order(self, order: Any) -> None:
        """Read a Nautilus order's canonical status, preserving engine ownership."""
        status = getattr(getattr(order, "status", None), "name", None)
        order_id = str(getattr(order, "client_order_id", ""))
        if not isinstance(status, str):
            raise ValueError("Nautilus order has no status")
        self.record_order_status(order_id, status)

    def record_risk_denial(self, reason: str) -> None:
        """Count a bounded denial reason without secret or symbol labels."""
        allowed = {"stale_market", "spread", "price_jump", "position", "portfolio", "other"}
        label = reason if reason in allowed else "other"
        self.risk_denials.labels(reason=label).inc()
        self.logger.warning("paper risk denial: %s", label)

    def record_fill(self, *, fee_quote: Decimal, slippage_bps: float, latency_ms: float) -> None:
        """Record observed execution costs; reject impossible measurements."""
        if fee_quote < 0 or latency_ms < 0:
            raise ValueError("fee and latency must be nonnegative")
        from math import isfinite

        if not isfinite(slippage_bps) or not isfinite(latency_ms):
            raise ValueError("fill metrics must be finite")
        self.fills.inc()
        self.fees.inc(float(fee_quote))
        self.slippage.observe(slippage_bps)
        self.order_latency.observe(latency_ms)
        self.logger.info(
            "fill fee_quote=%s slippage_bps=%s latency_ms=%s",
            fee_quote, slippage_bps, latency_ms,
        )

    def record_portfolio(self, *, equity_quote: Decimal, drawdown_fraction: Decimal) -> None:
        """Update current account value and drawdown gauges."""
        if equity_quote <= 0 or not Decimal("0") <= drawdown_fraction <= Decimal("1"):
            raise ValueError("invalid portfolio metrics")
        self.equity.set(float(equity_quote))
        self.drawdown.set(float(drawdown_fraction))

    def prometheus_text(self) -> str:
        """Return metrics for local inspection or a loopback HTTP endpoint."""
        return generate_latest(self.registry).decode("utf-8")

    def serve_loopback(self, port: int) -> None:
        """Expose metrics only on localhost when explicitly requested."""
        if not 1024 <= port <= 65535:
            raise ValueError("metrics port must be unprivileged")
        start_http_server(port, addr="127.0.0.1", registry=self.registry)
