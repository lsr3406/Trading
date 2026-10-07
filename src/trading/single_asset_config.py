"""Strict, versioned settings for the first single-asset research protocol."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from trading.configuration import StrictModel
from trading.data.schema import require_utc


class SingleAssetConfig(StrictModel):
    """One predeclared spot market, factor, split, and conservative cost case."""

    source: Literal["binance_archive"]
    symbol: Literal["BTC/USDT"]
    archive_symbol: Literal["BTCUSDT"]
    timeframe: Literal["4h"]
    start: datetime
    end: datetime
    lookback_bars: int = Field(ge=2)
    train_bars: int = Field(ge=100)
    test_bars: int = Field(ge=30)
    embargo_bars: int = Field(ge=1)
    holdout_fraction: float = Field(gt=0, lt=0.5)
    initial_cash_usdt: Decimal = Field(gt=0)
    order_notional_usdt: Decimal = Field(gt=0)
    fee_bps_per_side: Decimal = Field(ge=0)
    slippage_bps_per_side: Decimal = Field(ge=0)
    latency_ms: int = Field(ge=1)
    bootstrap_block_bars: int = Field(ge=2)
    bootstrap_resamples: int = Field(ge=199)
    seed: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_protocol(self) -> "SingleAssetConfig":
        """Reject an incomplete or underfunded protocol before any download."""
        require_utc(self.start, "start")
        require_utc(self.end, "end")
        if self.end <= self.start:
            raise ValueError("end must follow start")
        if self.order_notional_usdt > self.initial_cash_usdt * Decimal("0.1"):
            raise ValueError("the first study caps one order at 10% of initial cash")
        if self.train_bars <= self.lookback_bars + self.bootstrap_block_bars:
            raise ValueError("training window is too short for warmup and bootstrap")
        return self

    def fingerprint(self) -> str:
        """Hash every assumption in canonical JSON form."""
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_single_asset_config(path: Path) -> SingleAssetConfig:
    """Load a safe YAML protocol and reject unknown or mistyped fields."""
    with path.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("single-asset config must be a YAML mapping")
    return SingleAssetConfig.model_validate(payload)
