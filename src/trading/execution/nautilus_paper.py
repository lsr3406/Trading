"""Construct a local NautilusTrader simulated venue from a paper profile."""

from decimal import Decimal
from typing import TYPE_CHECKING

from trading.execution.profile import PaperProfile

if TYPE_CHECKING:
    from nautilus_trader.backtest import BacktestEngine


def build_paper_engine(profile: PaperProfile) -> "BacktestEngine":
    """Build an isolated backtest venue with explicit fee, fill, and latency models.

    This adapter only constructs ``BacktestEngine`` and cannot create a live
    execution client. Strategies must route order admission through the paper
    risk middleware before calling NautilusTrader's strategy submission method.
    The seeded fill model makes repeated simulation runs reproducible.
    """
    profile.assert_calibrated()
    from nautilus_trader.backtest import BacktestEngine
    from nautilus_trader.common import LogLevel
    from nautilus_trader.config import BacktestEngineConfig, LoggerConfig
    from nautilus_trader.execution import DefaultFillModel, MakerTakerFeeModel, StaticLatencyModel
    from nautilus_trader.model import AccountType, Currency, Money, OmsType, Venue

    assert profile.starting_cash is not None
    assert profile.maker_fee_bps is not None
    assert profile.taker_fee_bps is not None
    assert profile.limit_fill_probability is not None
    assert profile.slippage_probability is not None
    assert profile.latency_ms is not None
    currency = Currency.from_str(profile.account_currency)
    engine = BacktestEngine(
        config=BacktestEngineConfig(logging=LoggerConfig(stdout_level=LogLevel.ERROR)),
    )
    engine.add_venue(
        venue=Venue(profile.venue),
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        starting_balances=[Money(float(profile.starting_cash), currency)],
        base_currency=None,
        default_leverage=Decimal(1),
        fee_model=MakerTakerFeeModel(
            maker_rate=profile.maker_fee_bps / Decimal(10_000),
            taker_rate=profile.taker_fee_bps / Decimal(10_000),
        ),
        fill_model=DefaultFillModel(
            prob_fill_on_limit=profile.limit_fill_probability,
            prob_slippage=profile.slippage_probability,
            random_seed=profile.random_seed,
        ),
        latency_model=StaticLatencyModel(base_latency_nanos=profile.latency_ms * 1_000_000),
    )
    return engine
