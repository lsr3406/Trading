"""Pure long/cash research strategies over lagged, completed-bar factors."""

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl
import yaml

from trading.alpha.catalog import BAR_FAMILIES, WINDOW_FAMILIES

StrategyKind = Literal[
    "momentum", "reversal", "ma_trend", "ema_trend", "ma_cross", "ema_cross",
    "bollinger_revert", "bollinger_breakout", "rsi_revert", "donchian_breakout",
    "channel_revert", "momentum_volume", "momentum_low_vol", "body_followthrough",
    "wide_range_breakout",
]

PARAMETERS: dict[str, tuple[str, ...]] = {
    "momentum": ("window", "threshold"),
    "reversal": ("window", "threshold"),
    "ma_trend": ("window", "threshold"),
    "ema_trend": ("window", "threshold"),
    "ma_cross": ("fast", "slow"),
    "ema_cross": ("fast", "slow"),
    "bollinger_revert": ("window", "entry", "exit"),
    "bollinger_breakout": ("window", "entry", "exit"),
    "rsi_revert": ("window", "entry", "exit"),
    "donchian_breakout": ("window", "exit_position"),
    "channel_revert": ("window", "entry", "exit"),
    "momentum_volume": ("window", "volume_window", "min_volume_ratio"),
    "momentum_low_vol": ("window", "vol_window", "max_volatility"),
    "body_followthrough": ("min_body",),
    "wide_range_breakout": ("min_range", "min_close_location"),
}


@dataclass(frozen=True, slots=True)
class StrategySpec:
    """One fully specified research hypothesis with no execution capability."""

    name: str
    kind: StrategyKind
    parameters: dict[str, int | float]

    def __post_init__(self) -> None:
        """Reject missing, extra, nonfinite, and inconsistent parameters."""
        if not self.name or self.kind not in PARAMETERS:
            raise ValueError("strategy name or kind is invalid")
        expected = set(PARAMETERS[self.kind])
        if set(self.parameters) != expected:
            raise ValueError(f"{self.name}: expected parameters {sorted(expected)}")
        for key, value in self.parameters.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{self.name}: {key} must be numeric")
            if not math.isfinite(float(value)):
                raise ValueError(f"{self.name}: {key} must be finite")
            if key in {"window", "fast", "slow", "volume_window", "vol_window"}:
                if not isinstance(value, int) or value < 3:
                    raise ValueError(f"{self.name}: {key} must be an integer >= 3")
        if "fast" in self.parameters and self.parameters["fast"] >= self.parameters["slow"]:
            raise ValueError(f"{self.name}: fast must be shorter than slow")
        if self.kind in {"bollinger_revert", "rsi_revert", "channel_revert"}:
            if self.parameters["entry"] >= self.parameters["exit"]:
                raise ValueError(f"{self.name}: entry must be below exit")
        if self.kind == "bollinger_breakout":
            if self.parameters["entry"] <= self.parameters["exit"]:
                raise ValueError(f"{self.name}: entry must exceed exit")
        for key in ("exit_position", "min_close_location"):
            if key in self.parameters and not 0 <= self.parameters[key] <= 1:
                raise ValueError(f"{self.name}: {key} must lie in [0, 1]")
        if "max_volatility" in self.parameters and self.parameters["max_volatility"] <= 0:
            raise ValueError(f"{self.name}: maximum volatility must be positive")
        if "min_volume_ratio" in self.parameters and self.parameters["min_volume_ratio"] <= 0:
            raise ValueError(f"{self.name}: volume ratio must be positive")

    def required_factors(self) -> tuple[str, ...]:
        """List every point-in-time factor required for this strategy."""
        p = self.parameters
        window = p.get("window")
        if self.kind in {"momentum", "reversal"}:
            return (f"momentum_{window}",)
        if self.kind == "ma_trend":
            return (f"ma_gap_{window}",)
        if self.kind == "ema_trend":
            return (f"ema_gap_{window}",)
        if self.kind in {"ma_cross", "ema_cross"}:
            prefix = "ma_gap" if self.kind == "ma_cross" else "ema_gap"
            return (f"{prefix}_{p['fast']}", f"{prefix}_{p['slow']}")
        if self.kind.startswith("bollinger"):
            return (f"bollinger_z_{window}",)
        if self.kind == "rsi_revert":
            return (f"cutler_rsi_{window}",)
        if self.kind in {"donchian_breakout", "channel_revert"}:
            return (f"donchian_position_{window}", f"breakout_distance_{window}")
        if self.kind == "momentum_volume":
            return (f"momentum_{window}", f"volume_ratio_{p['volume_window']}")
        if self.kind == "momentum_low_vol":
            return (f"momentum_{window}", f"return_volatility_{p['vol_window']}")
        if self.kind == "body_followthrough":
            return ("bar_body",)
        return ("bar_range", "close_location")

    def conditions(self) -> tuple[pl.Expr, pl.Expr]:
        """Return entry and exit expressions; no current-bar OHLCV is accessed."""
        p = self.parameters
        window = p.get("window")
        if self.kind == "momentum":
            signal = pl.col(f"momentum_{window}") > p["threshold"]
            return signal, ~signal
        if self.kind == "reversal":
            signal = pl.col(f"momentum_{window}") < -p["threshold"]
            return signal, pl.col(f"momentum_{window}") >= 0
        if self.kind in {"ma_trend", "ema_trend"}:
            prefix = "ma_gap" if self.kind == "ma_trend" else "ema_gap"
            signal = pl.col(f"{prefix}_{window}") > p["threshold"]
            return signal, ~signal
        if self.kind in {"ma_cross", "ema_cross"}:
            prefix = "ma_gap" if self.kind == "ma_cross" else "ema_gap"
            # close / MAfast < close / MAslow exactly when MAfast > MAslow.
            signal = pl.col(f"{prefix}_{p['fast']}") < pl.col(f"{prefix}_{p['slow']}")
            return signal, ~signal
        if self.kind.startswith("bollinger"):
            z = pl.col(f"bollinger_z_{window}")
            if self.kind == "bollinger_revert":
                return z < p["entry"], z >= p["exit"]
            return z > p["entry"], z <= p["exit"]
        if self.kind == "rsi_revert":
            rsi = pl.col(f"cutler_rsi_{window}")
            return rsi < p["entry"], rsi >= p["exit"]
        if self.kind == "donchian_breakout":
            return (
                pl.col(f"breakout_distance_{window}") > 0,
                pl.col(f"donchian_position_{window}") < p["exit_position"],
            )
        if self.kind == "channel_revert":
            position = pl.col(f"donchian_position_{window}")
            return position < p["entry"], position >= p["exit"]
        if self.kind == "momentum_volume":
            momentum = pl.col(f"momentum_{window}")
            volume = pl.col(f"volume_ratio_{p['volume_window']}")
            return (momentum > 0) & (volume > p["min_volume_ratio"]), momentum <= 0
        if self.kind == "momentum_low_vol":
            momentum = pl.col(f"momentum_{window}")
            volatility = pl.col(f"return_volatility_{p['vol_window']}")
            return (momentum > 0) & (volatility < p["max_volatility"]), momentum <= 0
        if self.kind == "body_followthrough":
            body = pl.col("bar_body")
            return body > p["min_body"], body <= 0
        bar_range = pl.col("bar_range")
        location = pl.col("close_location")
        return (
            (bar_range > p["min_range"]) & (location > p["min_close_location"]),
            location <= 0,
        )

    def signals(self, features: pl.DataFrame) -> tuple[pl.Series, pl.Series]:
        """Evaluate only lagged catalog columns, treating warmup nulls as false."""
        missing = set(self.required_factors()) - set(features.columns)
        if missing:
            raise ValueError(f"{self.name}: missing factors {sorted(missing)}")
        entry, exit_ = self.conditions()
        result = features.select(
            entry.fill_null(False).alias("entry"),
            exit_.fill_null(False).alias("exit"),
        )
        return result["entry"], result["exit"]


@dataclass(frozen=True, slots=True)
class ResearchCatalog:
    """A frozen, YAML-declared factor grid and strategy hypothesis family."""

    windows: tuple[int, ...]
    strategies: tuple[StrategySpec, ...]
    correction: Literal["bonferroni", "fdr_bh"]
    significance_level: float
    bootstrap_resamples: int


def load_research_catalog(path: Path) -> ResearchCatalog:
    """Load a finite hypothesis family, rejecting unknown YAML fields."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "windows", "strategies", "correction", "significance_level", "bootstrap_resamples"
    }:
        raise ValueError("research catalog has missing or extra top-level fields")
    windows = payload["windows"]
    if not isinstance(windows, list) or not windows or any(
        isinstance(w, bool) or not isinstance(w, int) or w < 3 for w in windows
    ) or len(set(windows)) != len(windows):
        raise ValueError("research windows must be distinct integers >= 3")
    candidates = payload["strategies"]
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("at least one strategy must be declared")
    strategies: list[StrategySpec] = []
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) != {"name", "kind", "parameters"}:
            raise ValueError("candidate must contain exactly name, kind, parameters")
        if not isinstance(candidate["name"], str) or not isinstance(candidate["parameters"], dict):
            raise ValueError("candidate name or parameters have invalid types")
        strategies.append(
            StrategySpec(candidate["name"], candidate["kind"], candidate["parameters"])
        )
    if len({strategy.name for strategy in strategies}) != len(strategies):
        raise ValueError("strategy candidate names must be unique")
    correction = payload["correction"]
    level = payload["significance_level"]
    resamples = payload["bootstrap_resamples"]
    if correction not in {"bonferroni", "fdr_bh"} or isinstance(level, bool) or not (
        isinstance(level, (int, float)) and 0 < level < 1
    ):
        raise ValueError("invalid correction or significance level")
    if isinstance(resamples, bool) or not isinstance(resamples, int) or resamples < 199:
        raise ValueError("bootstrap_resamples must be an integer >= 199")
    factor_count = len(WINDOW_FAMILIES) * len(windows) + len(BAR_FAMILIES)
    family_count = max(factor_count, len(strategies))
    if correction == "bonferroni" and family_count / (resamples + 1) > level:
        raise ValueError("bootstrap resolution is too low for this Bonferroni family")
    catalog = ResearchCatalog(
        tuple(windows), tuple(strategies), correction, float(level), resamples
    )
    available = {
        f"{family}_{window}" for family in (
            "momentum", "ma_gap", "ema_gap", "bollinger_z", "cutler_rsi",
            "donchian_position", "breakout_distance", "volume_ratio", "return_volatility",
        ) for window in catalog.windows
    } | {"bar_body", "bar_range", "close_location"}
    for strategy in catalog.strategies:
        if set(strategy.required_factors()) - available:
            raise ValueError(f"{strategy.name}: required window is absent from factor grid")
    return catalog
