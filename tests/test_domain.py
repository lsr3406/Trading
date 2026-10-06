"""Time alignment and immutable market-domain checks."""

from datetime import UTC, datetime, timedelta

import pytest

from trading.domain import Signal


def test_signal_rejects_future_observation() -> None:
    """A feature cutoff later than decision time is lookahead leakage."""
    decision = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="future"):
        Signal("BTC/USD", decision, decision + timedelta(seconds=1), 0.1)


def test_signal_rejects_naive_time() -> None:
    """Time-zone ambiguity is rejected at the data boundary."""
    with pytest.raises(ValueError, match="timezone-aware"):
        Signal("BTC/USD", datetime(2025, 1, 1), datetime(2025, 1, 1), 0.1)
