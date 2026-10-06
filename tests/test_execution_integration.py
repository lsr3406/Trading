"""Offline paper-engine, hard-risk, and observability checks."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from trading.configuration import ExecutionSettings, RiskSettings, load_settings
from trading.domain import OrderRequest, PortfolioSnapshot
from trading.execution.gate import ExecutionDenied
from trading.execution.middleware import MarketObservation, RealtimeRiskMiddleware
from trading.execution.monitor import ExecutionMonitor
from trading.execution.nautilus_paper import build_paper_engine
from trading.execution.nautilus_router import NautilusPaperRouter
from trading.execution.profile import PaperProfile, load_paper_profile

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2025, 1, 1, tzinfo=UTC)


def _profile() -> PaperProfile:
    """Return explicitly labelled synthetic assumptions for a local simulation."""
    return PaperProfile(
        mode="paper", engine="nautilus", venue="SIM", account_currency="USDT",
        starting_cash=Decimal("10000"), maker_fee_bps=Decimal("1"),
        taker_fee_bps=Decimal("2"), limit_fill_probability=0.8,
        slippage_probability=0.1, latency_ms=25, random_seed=42,
        max_market_age_ms=1000, max_spread_bps=Decimal("20"),
        max_price_deviation_bps=Decimal("50"), max_position_fraction=Decimal("0.2"),
    )


def test_live_profile_and_uncalibrated_paper_are_blocked() -> None:
    """A profile cannot turn simulation settings into a live connector."""
    with pytest.raises(ExecutionDenied, match="live execution is disabled"):
        load_paper_profile(ROOT / "config/execution.live.yaml")
    with pytest.raises(ExecutionDenied, match="uncalibrated"):
        build_paper_engine(load_paper_profile(ROOT / "config/execution.paper.yaml"))


def test_nautilus_simulation_engine_builds_offline() -> None:
    """The pinned optional Nautilus version accepts fee/fill/latency wiring."""
    engine = build_paper_engine(_profile())
    try:
        assert type(engine).__name__ == "BacktestEngine"
    finally:
        engine.dispose()


def test_paper_middleware_denies_anomalies_and_records_metrics() -> None:
    """A stale market or concentration breach fails before order submission."""
    settings = load_settings(ROOT / "config/base.yaml", environ={}).model_copy(
        update={
            "execution": ExecutionSettings(mode="paper"),
            "risk": RiskSettings(
                max_order_notional=Decimal("1000"), max_daily_loss=Decimal("500"),
                max_drawdown_fraction=Decimal("0.2"), max_leverage=Decimal("1"),
                max_snapshot_age_ms=1000,
            ),
        }
    )
    monitor = ExecutionMonitor()
    middleware = RealtimeRiskMiddleware(settings, _profile(), monitor)
    order = OrderRequest("order-1", "BTC/USDT", "USDT", "buy", Decimal("1"),
                         Decimal("100"), NOW)
    portfolio = PortfolioSnapshot(NOW, "USDT", Decimal("10000"), Decimal("0"),
                                  Decimal("0"), Decimal("0"))
    market = MarketObservation("BTC/USDT", NOW, Decimal("99.9"), Decimal("100.1"),
                               Decimal("100"))
    decision = middleware.authorize(
        order, portfolio, market, current_symbol_exposure=Decimal(0)
    )
    assert decision.approved
    stale = MarketObservation("BTC/USDT", NOW - timedelta(seconds=2), Decimal("99.9"),
                              Decimal("100.1"), Decimal("100"))
    with pytest.raises(ExecutionDenied, match="stale"):
        middleware.authorize(order, portfolio, stale, current_symbol_exposure=Decimal(0))
    with pytest.raises(ExecutionDenied, match="position"):
        middleware.authorize(order, portfolio, market,
                             current_symbol_exposure=Decimal("1999"))
    assert "trading_risk_denials_total" in monitor.prometheus_text()


def test_monitor_rejects_unknown_order_status() -> None:
    """Metrics accept canonical Nautilus status names only."""
    monitor = ExecutionMonitor()
    monitor.record_order_status("order-1", "ACCEPTED")
    with pytest.raises(ValueError, match="unknown order status"):
        monitor.record_order_status("order-1", "MYSTERY")


def test_router_checks_risk_before_native_order_creation() -> None:
    """The Nautilus bridge cannot submit an order with a stale market snapshot."""
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.model import (
        Currency,
        CurrencyPair,
        InstrumentId,
        Price,
        Quantity,
        Symbol,
    )
    from nautilus_trader.trading import Strategy

    engine = build_paper_engine(_profile())
    try:
        instrument = CurrencyPair(
            instrument_id=InstrumentId.from_str("BTC/USDT.SIM"),
            raw_symbol=Symbol("BTC/USDT"),
            base_currency=Currency.from_str("BTC"),
            quote_currency=Currency.from_str("USDT"),
            price_precision=2, size_precision=3,
            price_increment=Price.from_str("0.01"),
            size_increment=Quantity.from_str("0.001"),
            ts_event=0, ts_init=0,
        )
        engine.add_instrument(instrument)
        strategy = Strategy(StrategyConfig())
        engine.add_strategy(strategy)
        settings = load_settings(ROOT / "config/base.yaml", environ={}).model_copy(
            update={
                "execution": ExecutionSettings(mode="paper"),
                "risk": RiskSettings(
                    max_order_notional=Decimal("1000"), max_daily_loss=Decimal("500"),
                    max_drawdown_fraction=Decimal("0.2"), max_leverage=Decimal("1"),
                    max_snapshot_age_ms=1000,
                ),
            }
        )
        monitor = ExecutionMonitor()
        router = NautilusPaperRouter(
            strategy, RealtimeRiskMiddleware(settings, _profile(), monitor), monitor
        )
        order = OrderRequest("router-1", "BTC/USDT", "USDT", "buy", Decimal("1"),
                             Decimal("100"), NOW)
        portfolio = PortfolioSnapshot(NOW, "USDT", Decimal("10000"), Decimal("0"),
                                      Decimal("0"), Decimal("0"))
        stale = MarketObservation("BTC/USDT", NOW - timedelta(seconds=2),
                                  Decimal("99.9"), Decimal("100.1"), Decimal("100"))
        with pytest.raises(ExecutionDenied, match="stale"):
            router.submit_limit(order, portfolio, stale, current_symbol_exposure=Decimal(0))
        current = MarketObservation(
            "BTC/USDT", NOW, Decimal("99.9"), Decimal("100.1"), Decimal("100")
        )
        assert router.submit_limit(
            order, portfolio, current, current_symbol_exposure=Decimal(0)
        ) == "router-1"
    finally:
        engine.dispose()
