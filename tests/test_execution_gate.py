"""Execution isolation and mandatory paper risk checks."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from trading.configuration import load_settings
from trading.domain import OrderRequest, PortfolioSnapshot
from trading.execution.gate import ExecutionDenied, authorize_paper_order

BASE = Path(__file__).resolve().parents[1] / "config/base.yaml"
NOW = datetime(2025, 1, 1, tzinfo=UTC)
ORDER = OrderRequest("order-1", "BTC/USD", "USD", "buy", Decimal("1"), Decimal("100"), NOW)
PORTFOLIO = PortfolioSnapshot(NOW, "USD", Decimal("1000"), Decimal("0"), Decimal("0"), Decimal("0"))
LIMITS = {
    "TRADING__RISK__MAX_ORDER_NOTIONAL": "200",
    "TRADING__RISK__MAX_DAILY_LOSS": "50",
    "TRADING__RISK__MAX_DRAWDOWN_FRACTION": "0.20",
    "TRADING__RISK__MAX_LEVERAGE": "1",
    "TRADING__RISK__MAX_SNAPSHOT_AGE_MS": "60000",
}


def test_live_mode_is_always_denied() -> None:
    """Even complete limits cannot activate live execution."""
    settings = load_settings(BASE, environ={**LIMITS, "TRADING__EXECUTION__MODE": "live"})
    with pytest.raises(ExecutionDenied, match="live trading is unavailable"):
        authorize_paper_order(settings, ORDER, PORTFOLIO)


def test_paper_mode_requires_all_risk_limits() -> None:
    """An incomplete risk configuration blocks paper orders."""
    settings = load_settings(BASE, environ={"TRADING__EXECUTION__MODE": "paper"})
    with pytest.raises(ExecutionDenied, match="incomplete"):
        authorize_paper_order(settings, ORDER, PORTFOLIO)


def test_paper_mode_checks_limits_and_order_identity() -> None:
    """The gate allows a bounded order and denies one over its notional limit."""
    settings = load_settings(BASE, environ={**LIMITS, "TRADING__EXECUTION__MODE": "paper"})
    decision = authorize_paper_order(settings, ORDER, PORTFOLIO)
    assert decision.approved and decision.client_order_id == ORDER.client_order_id
    larger = OrderRequest("order-2", "BTC/USD", "USD", "buy", Decimal("3"), Decimal("100"), NOW)
    with pytest.raises(ExecutionDenied, match="notional"):
        authorize_paper_order(settings, larger, PORTFOLIO)


def test_paper_mode_rejects_currency_mismatch() -> None:
    """Risk limits cannot compare values in different quote currencies."""
    settings = load_settings(BASE, environ={**LIMITS, "TRADING__EXECUTION__MODE": "paper"})
    other = OrderRequest("order-3", "BTC/EUR", "EUR", "buy", Decimal("1"), Decimal("100"), NOW)
    with pytest.raises(ExecutionDenied, match="currencies differ"):
        authorize_paper_order(settings, other, PORTFOLIO)


def test_paper_mode_rejects_stale_snapshot() -> None:
    """A valid limit set cannot authorize an order from old portfolio state."""
    from datetime import timedelta

    settings = load_settings(BASE, environ={**LIMITS, "TRADING__EXECUTION__MODE": "paper"})
    later = OrderRequest(
        "order-4", "BTC/USD", "USD", "buy", Decimal("1"), Decimal("100"),
        NOW + timedelta(minutes=2),
    )
    with pytest.raises(ExecutionDenied, match="stale"):
        authorize_paper_order(settings, later, PORTFOLIO)
