"""Configuration precedence and research-readiness checks."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from trading.configuration import assert_research_ready, load_settings

BASE = Path(__file__).resolve().parents[1] / "config/base.yaml"


def test_environment_overrides_yaml_without_process_mutation() -> None:
    """A process override wins over YAML and is validated as a typed value."""
    settings = load_settings(BASE, environ={"TRADING__RESEARCH__SEED": "7"})
    assert settings.research.seed == 7
    assert settings.execution.mode == "disabled"


def test_unknown_override_fails_closed() -> None:
    """A mistyped setting must not be silently ignored."""
    with pytest.raises(ValidationError):
        load_settings(BASE, environ={"TRADING__EXECUTION__MOD": "paper"})


def test_research_not_ready_without_dataset_and_costs() -> None:
    """Placeholder research assumptions cannot generate a run manifest."""
    with pytest.raises(ValueError, match="data_version"):
        assert_research_ready(load_settings(BASE, environ={}))


def test_environment_mode_cannot_enable_live_adapter() -> None:
    """Configuration can name live mode but cannot create an execution path."""
    settings = load_settings(BASE, environ={"TRADING__EXECUTION__MODE": "live"})
    assert settings.execution.mode == "live"


def test_dotenv_then_process_environment_precedence(tmp_path: Path) -> None:
    """Process variables override local .env, including nested validation settings."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TRADING__RESEARCH__SEED=11\n"
        "TRADING__RESEARCH__VALIDATION__HOLDOUT_FRACTION=0.30\n",
        encoding="utf-8",
    )
    settings = load_settings(
        BASE, env_path=env_file, environ={"TRADING__RESEARCH__SEED": "12"}
    )
    assert settings.research.seed == 12
    assert settings.research.validation.holdout_fraction == 0.30
