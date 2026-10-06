"""Separate simulation and blocked-live execution profiles."""

from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field

from trading.configuration import StrictModel
from trading.execution.gate import ExecutionDenied


class PaperProfile(StrictModel):
    """Calibrated simulation controls, never exchange credentials."""

    mode: Literal["paper"]
    engine: Literal["nautilus"]
    venue: str = Field(min_length=1)
    account_currency: str = Field(min_length=1)
    starting_cash: Decimal | None = Field(default=None, gt=0)
    maker_fee_bps: Decimal | None = Field(default=None, ge=0)
    taker_fee_bps: Decimal | None = Field(default=None, ge=0)
    limit_fill_probability: float | None = Field(default=None, ge=0, le=1)
    slippage_probability: float | None = Field(default=None, ge=0, le=1)
    latency_ms: int | None = Field(default=None, ge=0)
    random_seed: int = Field(ge=0)
    max_market_age_ms: int | None = Field(default=None, gt=0)
    max_spread_bps: Decimal | None = Field(default=None, gt=0)
    max_price_deviation_bps: Decimal | None = Field(default=None, gt=0)
    max_position_fraction: Decimal | None = Field(default=None, gt=0, le=1)
    metrics_port: int | None = Field(default=None, ge=1024, le=65535)

    def assert_calibrated(self) -> None:
        """Block simulation when its fill and risk assumptions are placeholders."""
        required = (
            self.starting_cash, self.maker_fee_bps, self.taker_fee_bps,
            self.limit_fill_probability, self.slippage_probability, self.latency_ms,
            self.max_market_age_ms, self.max_spread_bps,
            self.max_price_deviation_bps, self.max_position_fraction,
        )
        if any(value is None for value in required):
            raise ExecutionDenied("paper profile has uncalibrated assumptions")


def load_paper_profile(path: Path) -> PaperProfile:
    """Load a safe YAML paper profile; reject every live profile before parsing."""
    with path.open(encoding="utf-8") as stream:
        parsed = yaml.safe_load(stream)
    if not isinstance(parsed, dict):
        raise ValueError("execution profile must be a YAML mapping")
    if parsed.get("mode") == "live":
        raise ExecutionDenied("live execution is disabled pending independent validation")
    return PaperProfile.model_validate(parsed)
