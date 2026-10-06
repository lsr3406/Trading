"""Validated YAML and environment configuration with no secret defaults."""

import json
import os
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    """Forbid typos in configuration and prevent mutation after loading."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectSettings(StrictModel):
    """Human-readable project identity."""

    name: str = Field(min_length=1)


class ValidationSettings(StrictModel):
    """Minimum planned out-of-sample and multiple-testing controls."""

    method: Literal["walk_forward"]
    holdout_fraction: float = Field(gt=0, lt=1)
    multiple_testing: Literal["fdr_bh", "bonferroni"]
    significance_level: float = Field(gt=0, lt=1)


class ResearchSettings(StrictModel):
    """Seed and dataset identifier to record with every experiment."""

    seed: int = Field(ge=0)
    data_version: str = Field(min_length=1)
    validation: ValidationSettings


class CostSettings(StrictModel):
    """Explicit execution-cost assumptions; null means not yet calibrated."""

    fee_bps: Decimal | None = Field(default=None, ge=0)
    slippage_bps: Decimal | None = Field(default=None, ge=0)
    funding_bps: Decimal | None = Field(default=None)
    latency_ms: int | None = Field(default=None, ge=0)


class LoggingSettings(StrictModel):
    """Structured log output settings."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    json_output: bool


class ExecutionSettings(StrictModel):
    """Requested execution mode; live mode is blocked by the current gate."""

    mode: Literal["disabled", "paper", "live"]


class RiskSettings(StrictModel):
    """Hard limits; missing values prevent order authorization."""

    max_order_notional: Decimal | None = Field(default=None, gt=0)
    max_daily_loss: Decimal | None = Field(default=None, gt=0)
    max_drawdown_fraction: Decimal | None = Field(default=None, gt=0, le=1)
    max_leverage: Decimal | None = Field(default=None, gt=0)
    max_snapshot_age_ms: int | None = Field(default=None, gt=0)


class Settings(StrictModel):
    """Complete validated application configuration."""

    project: ProjectSettings
    research: ResearchSettings
    costs: CostSettings
    logging: LoggingSettings
    execution: ExecutionSettings
    risk: RiskSettings

    def fingerprint(self) -> str:
        """Return a stable SHA-256 hash of all nonsecret configuration values."""
        from hashlib import sha256

        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode("utf-8")).hexdigest()


def _apply_environment(config: dict[str, Any], variables: Mapping[str, str]) -> None:
    """Apply nested TRADING__SECTION__KEY overrides to a YAML dictionary."""
    for key, raw in variables.items():
        if not key.startswith("TRADING__"):
            continue
        parts = key.removeprefix("TRADING__").lower().split("__")
        if len(parts) < 2 or not all(parts):
            raise ValueError(f"invalid configuration override: {key}")
        section: dict[str, Any] = config
        for part in parts[:-1]:
            nested = section.get(part)
            if not isinstance(nested, dict):
                raise ValueError(f"unknown configuration section: {part}")
            section = nested
        section[parts[-1]] = yaml.safe_load(raw)


def load_settings(
    config_path: Path,
    *,
    env_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Load safe YAML, then local .env, then process environment overrides.

    The default .env location is the project root, one directory above config/.
    Loading is side-effect free: process environment variables are not modified.
    """
    with config_path.open(encoding="utf-8") as stream:
        parsed = yaml.safe_load(stream)
    if not isinstance(parsed, dict):
        raise ValueError("configuration root must be a YAML mapping")
    config: dict[str, Any] = parsed
    local_env = env_path if env_path is not None else config_path.parent.parent / ".env"
    if local_env.is_file():
        _apply_environment(
            config,
            {key: value for key, value in dotenv_values(local_env).items() if value is not None},
        )
    _apply_environment(config, os.environ if environ is None else environ)
    return Settings.model_validate(config)


def assert_research_ready(settings: Settings) -> None:
    """Reject experiments that lack a dataset version or explicit cost model."""
    if settings.research.data_version == "unset":
        raise ValueError("research.data_version must identify an immutable dataset")
    if any(value is None for value in settings.costs.model_dump().values()):
        raise ValueError("all cost assumptions must be explicitly calibrated")
