"""Batched factor and strategy evaluation with honest multiplicity accounting."""

import hashlib
import importlib.metadata
import json
import math
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import polars as pl
from scipy.stats import spearmanr  # type: ignore[import-untyped]

from trading.alpha.catalog import standard_catalog
from trading.alpha.evaluation import adjust_pvalues
from trading.data.quality import assess_ohlcv
from trading.research.single_asset import StudyDataset
from trading.research.strategies import ResearchCatalog, StrategySpec
from trading.single_asset_config import SingleAssetConfig


def _sha256(path: Path) -> str:
    """Hash an artifact without changing its contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _centered_block_pvalue(
    values: np.ndarray, *, block: int, resamples: int, seed: int
) -> float:
    """Two-sided circular-block test of zero mean under a centered null."""
    finite = values[np.isfinite(values)]
    if len(finite) < max(10, block * 2):
        return 1.0
    observed = abs(float(finite.mean()))
    centered = finite - finite.mean()
    rng = np.random.default_rng(seed)
    n = len(finite)
    exceed = 0
    for offset in range(0, resamples, 256):
        batch = min(256, resamples - offset)
        starts = rng.integers(0, n, size=(batch, math.ceil(n / block)))
        positions = (starts[:, :, None] + np.arange(block)) % n
        samples = centered[positions.reshape(batch, -1)[:, :n]]
        exceed += int(np.count_nonzero(np.abs(samples.mean(axis=1)) >= observed))
    return (exceed + 1) / (resamples + 1)


def _folds(config: SingleAssetConfig, count: int) -> tuple[list[dict[str, int]], int]:
    """Reserve the final fifth, then generate disjoint rolling OOS windows."""
    holdout = int(count * (1 - config.holdout_fraction))
    development_end = holdout - config.embargo_bars
    cursor = config.train_bars + config.embargo_bars
    if cursor + config.test_bars > development_end:
        raise ValueError("not enough bars for one complete development fold")
    folds: list[dict[str, int]] = []
    while cursor < development_end:
        end = min(cursor + config.test_bars, development_end)
        if end - cursor < 30:
            break
        folds.append({
            "train_start": cursor - config.embargo_bars - config.train_bars,
            "train_end_exclusive": cursor - config.embargo_bars,
            "test_start": cursor,
            "test_end_exclusive": end,
        })
        cursor = end
    return folds, holdout


def _batch_fold(
    frame: pl.DataFrame, strategies: tuple[StrategySpec, ...],
    *, cash: float, notional: float, fee: float, slippage: float,
) -> tuple[dict[str, np.ndarray], dict[str, dict[str, float | int]], np.ndarray]:
    """Simulate every candidate in one vectorbt call with identical costs."""
    import vectorbt as vbt  # type: ignore[import-untyped]

    index = pd.DatetimeIndex(frame["timestamp"].to_list()) + timedelta(hours=4)
    close = pd.Series(frame["close"].to_numpy(), index=index, dtype="float64")
    names = [spec.name for spec in strategies]
    entries = pd.DataFrame(index=index, columns=names, dtype=bool)
    exits = pd.DataFrame(index=index, columns=names, dtype=bool)
    for spec in strategies:
        entry, exit_ = spec.signals(frame)
        entries[spec.name] = entry.to_numpy()
        exits[spec.name] = exit_.to_numpy()
    entries.iloc[-1] = False
    exits.iloc[-1] = True
    prices = pd.DataFrame({name: close for name in names})
    portfolio = vbt.Portfolio.from_signals(
        prices, entries=entries, exits=exits, init_cash=cash,
        size=notional, size_type="value", fees=fee, slippage=slippage,
        freq="4h", direction="longonly", accumulate=False,
    )
    buy = pd.Series(False, index=index)
    buy.iloc[0] = True
    sell = pd.Series(False, index=index)
    sell.iloc[-1] = True
    baseline = vbt.Portfolio.from_signals(
        close, entries=buy, exits=sell, init_cash=cash,
        size=notional, size_type="value", fees=fee, slippage=slippage,
        freq="4h", direction="longonly",
    )
    value = portfolio.value()
    baseline_values = baseline.value().to_numpy(dtype=float)
    baseline_return = np.diff(np.r_[cash, baseline_values]) / np.r_[
        cash, baseline_values[:-1]
    ]
    differences = {}
    for name in names:
        model_values = value[name].to_numpy(dtype=float)
        model_return = np.diff(np.r_[cash, model_values]) / np.r_[
            cash, model_values[:-1]
        ]
        differences[name] = model_return - baseline_return
    orders = portfolio.orders.count()
    closed = portfolio.trades.closed.count()
    baseline_end = float(baseline_values[-1])
    summaries = {
        name: {
            "return_pct": float((value[name].iloc[-1] / cash - 1) * 100),
            "buy_hold_return_pct": float((baseline_end / cash - 1) * 100),
            "orders": int(orders[name]), "closed_trades": int(closed[name]),
        }
        for name in names
    }
    return differences, summaries, close.to_numpy(dtype=float)


def _factor_ic_blocks(
    features: pl.DataFrame, folds: list[dict[str, int]],
    factor_name: str, block: int,
) -> np.ndarray:
    """Compute nonoverlapping time-series IC blocks inside OOS folds only."""
    values = features[factor_name].to_numpy().astype(float)
    close = features["close"].to_numpy().astype(float)
    correlations: list[float] = []
    for fold in folds:
        start, end = fold["test_start"], fold["test_end_exclusive"]
        # Label t is close[t+1]/close[t]-1; never cross a fold boundary.
        for offset in range(start, end - block, block):
            x = values[offset:offset + block]
            y = close[offset + 1:offset + block + 1] / close[offset:offset + block] - 1
            valid = np.isfinite(x) & np.isfinite(y)
            if int(valid.sum()) < max(10, block // 2):
                continue
            result = spearmanr(x[valid], y[valid])
            if math.isfinite(float(result.statistic)):
                correlations.append(float(result.statistic))
    return np.asarray(correlations, dtype=float)


def run_catalog_study(
    config: SingleAssetConfig, catalog: ResearchCatalog,
    dataset: StudyDataset, project_root: Path,
) -> Path:
    """Compute 112 causal factors, screen all strategies, and reserve holdout."""
    if _sha256(dataset.path) != dataset.sha256:
        raise ValueError("normalized dataset hash changed")
    bars = pl.read_parquet(dataset.path)
    quality = assess_ohlcv(bars, config.start, config.end, config.timeframe)
    if not quality.ok or bars.height != dataset.rows:
        raise ValueError("catalog study requires a complete, unchanged data grid")
    batch = standard_catalog(catalog.windows)
    features = batch.compute(bars).sort("timestamp")
    factor_names = [factor.name for factor in batch.factors]
    if any(features[name].is_finite().sum() == 0 for name in factor_names):
        raise ValueError("one or more catalog factors have no finite observations")
    catalog_payload = {
        "windows": catalog.windows, "correction": catalog.correction,
        "significance_level": catalog.significance_level,
        "bootstrap_resamples": catalog.bootstrap_resamples,
        "strategies": [asdict(spec) for spec in catalog.strategies],
    }
    catalog_sha = hashlib.sha256(
        json.dumps(catalog_payload, sort_keys=True).encode()
    ).hexdigest()
    factor_implementation = hashlib.sha256()
    for source in (
        Path(__file__).parents[1] / "alpha/catalog.py",
        Path(__file__).parents[1] / "alpha/factors.py",
        Path(__file__).parents[1] / "data/transform.py",
    ):
        factor_implementation.update(source.read_bytes())
    factor_implementation.update(pl.__version__.encode())
    factor_impl_sha = factor_implementation.hexdigest()
    feature_version = hashlib.sha256(
        json.dumps({"windows": catalog.windows, "implementation": factor_impl_sha},
                   sort_keys=True).encode()
    ).hexdigest()
    feature_path = (
        project_root / "data/interim"
        / f"factor-catalog-{dataset.sha256[:12]}-{feature_version[:12]}.parquet"
    )
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    if feature_path.exists():
        previous = pl.read_parquet(feature_path)
        if not previous.equals(features):
            raise ValueError("cached factor artifact differs from recomputed factors")
    else:
        features.write_parquet(feature_path, compression="zstd")
    factor_sha = _sha256(feature_path)
    folds, holdout_start = _folds(config, features.height)
    fee = float(config.fee_bps_per_side / 10_000)
    slippage = float(config.slippage_bps_per_side / 10_000)
    cash = float(config.initial_cash_usdt)
    notional = float(config.order_notional_usdt)
    differences: dict[str, list[np.ndarray]] = {s.name: [] for s in catalog.strategies}
    fold_summaries: dict[str, list[dict[str, float | int]]] = {
        s.name: [] for s in catalog.strategies
    }
    for fold in folds:
        start, end = fold["test_start"], fold["test_end_exclusive"]
        diff, summary, _ = _batch_fold(
            features.slice(start, end - start), catalog.strategies,
            cash=cash, notional=notional, fee=fee, slippage=slippage,
        )
        for spec in catalog.strategies:
            differences[spec.name].append(diff[spec.name])
            fold_summaries[spec.name].append(summary[spec.name])
    strategy_trials: list[dict[str, Any]] = []
    for index, spec in enumerate(catalog.strategies):
        pooled = np.concatenate(differences[spec.name])
        trial: dict[str, Any] = {
            "name": spec.name, "kind": spec.kind, "parameters": spec.parameters,
            "oos_bars": len(pooled), "mean_excess_bar_return": float(pooled.mean()),
            "mean_fold_return_pct": float(np.mean([
                row["return_pct"] for row in fold_summaries[spec.name]
            ])),
            "mean_fold_buy_hold_return_pct": float(np.mean([
                row["buy_hold_return_pct"] for row in fold_summaries[spec.name]
            ])),
            "orders": sum(int(row["orders"]) for row in fold_summaries[spec.name]),
            "raw_p": _centered_block_pvalue(
                pooled, block=config.bootstrap_block_bars,
                resamples=catalog.bootstrap_resamples, seed=config.seed + index,
            ),
        }
        strategy_trials.append(trial)
    corrected = adjust_pvalues(
        tuple(float(row["raw_p"]) for row in strategy_trials), catalog.correction
    )
    for trial, adjusted in zip(strategy_trials, corrected, strict=True):
        trial["adjusted_p"] = adjusted
    eligible = [
        trial for trial in strategy_trials
        if trial["adjusted_p"] <= catalog.significance_level
        and trial["mean_excess_bar_return"] > 0
    ]
    selected = (
        max(eligible, key=lambda row: row["mean_excess_bar_return"])["name"]
        if eligible else None
    )
    block = config.bootstrap_block_bars
    factor_trials: list[dict[str, Any]] = []
    for index, name in enumerate(factor_names):
        series = _factor_ic_blocks(features, folds, name, block)
        mean = float(series.mean()) if len(series) else None
        standard = float(series.std(ddof=1)) if len(series) > 1 else 0.0
        factor_trials.append({
            "name": name, "oos_ic_blocks": len(series), "mean_block_ic": mean,
            "ic_ir": mean / standard if mean is not None and standard > 0 else None,
            "raw_p": _centered_block_pvalue(
                series, block=3, resamples=catalog.bootstrap_resamples,
                seed=config.seed + 10_000 + index,
            ),
        })
    factor_adjusted = adjust_pvalues(
        tuple(float(row["raw_p"]) for row in factor_trials), catalog.correction
    )
    for trial, adjusted in zip(factor_trials, factor_adjusted, strict=True):
        trial["adjusted_p"] = adjusted
    code_hasher = hashlib.sha256()
    code_hasher.update(factor_impl_sha.encode())
    for source in (
        Path(__file__), Path(__file__).parents[1] / "alpha/catalog.py",
        Path(__file__).with_name("strategies.py"),
        project_root / "pyproject.toml", project_root / "uv.lock",
    ):
        if source.exists():
            code_hasher.update(source.read_bytes())
    code_sha = code_hasher.hexdigest()
    report: dict[str, Any] = {
        "protocol": "btc-4h-ohlcv-factor-strategy-catalog-v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "data_sha256": dataset.sha256, "factor_parquet_sha256": factor_sha,
        "factor_parquet_path": str(feature_path),
        "config_sha256": config.fingerprint(), "catalog_sha256": catalog_sha,
        "code_and_lock_sha256": code_sha,
        "factor_implementation_sha256": factor_impl_sha,
        "vectorbt_version": importlib.metadata.version("vectorbt"),
        "polars_version": pl.__version__,
        "factor_count": len(factor_names), "strategy_count": len(catalog.strategies),
        "correction": catalog.correction, "significance_level": catalog.significance_level,
        "bootstrap_resamples": catalog.bootstrap_resamples,
        "folds": folds, "holdout_start_bar": holdout_start,
        "holdout_bars": features.height - holdout_start,
        "holdout_status": (
            "reserved, not evaluated in this catalog study; the same historical period "
            "was inspected in the prior single-rule momentum study, so it is not virgin data"
        ),
        "selection_rule": (
            "positive development OOS mean excess bar return with family-adjusted "
            "two-sided block-bootstrap p <= significance_level"
        ),
        "selected_strategy": selected,
        "factor_significant_count": sum(
            trial["adjusted_p"] <= catalog.significance_level for trial in factor_trials
        ),
        "strategy_trials": strategy_trials,
        "factor_trials": factor_trials,
        "timing": (
            "each factor row uses only completed bars before its UTC bar-open timestamp; "
            "orders fill at that bar's close, with one full bar of delay"
        ),
        "costs": {
            "cash_usdt": cash, "order_notional_usdt": notional,
            "fee_bps_per_side": float(config.fee_bps_per_side),
            "slippage_bps_per_side": float(config.slippage_bps_per_side),
            "terminal_liquidation": True,
        },
        "limitations": [
            "All 112 factor formulas are OHLCV-derived; volume flow and Amihud are proxies, "
            "not order-book imbalance or measured price impact.",
            "The full candidate family is exploratory. A fresh future holdout is required "
            "for confirmatory evidence because the historical holdout has been inspected.",
            "Bar-close fills, assumed slippage, and independent fold resets do not model "
            "intrabar execution, liquidity shocks, or live operational risk.",
        ],
    }
    run_dir = (
        project_root / "research/runs"
        / f"catalog-{dataset.sha256[:12]}-{catalog_sha[:12]}-{code_sha[:12]}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
                           encoding="utf-8")
    best = sorted(strategy_trials, key=lambda row: row["mean_excess_bar_return"], reverse=True)
    notable_factors = sorted(
        (trial for trial in factor_trials if trial["adjusted_p"] <= catalog.significance_level),
        key=lambda row: abs(row["mean_block_ic"] or 0.0), reverse=True,
    )
    lines = [
        "# BTC/USDT 四小时因子与策略目录研究", "",
        f"数据：`{dataset.sha256}`；因子矩阵：`{factor_sha}`；配置：`{catalog_sha}`。", "",
        f"计算 {len(factor_names)} 个因子、检验 {len(catalog.strategies)} 个预声明策略；"
        f"使用 {len(folds)} 个开发期样本外窗口。", "",
        f"多重比较：{catalog.correction}，阈值 {catalog.significance_level}。",
        f"符合筛选规则的策略：{selected or '无'}。", "",
        "## 开发期样本外策略排名", "",
        "| 策略 | 平均超额逐根收益 | 调整后 p | 订单数 |", "|---|---:|---:|---:|",
    ]
    lines.extend(
        f"| {trial['name']} | {trial['mean_excess_bar_return']:.8f} | "
        f"{trial['adjusted_p']:.4f} | {trial['orders']} |"
        for trial in best
    )
    lines.extend([
        "", "## 因子 IC 摘要", "",
        f"Bonferroni 校正后达阈值的因子：{len(notable_factors)} / {len(factor_names)}。"
        "这些是高度相关的探索性时间序列相关性，不代表独立策略收益。", "",
        "| 因子 | 平均分块 Spearman IC | 调整后 p |",
        "|---|---:|---:|",
    ])
    lines.extend(
        f"| {trial['name']} | {trial['mean_block_ic']:.4f} | {trial['adjusted_p']:.4f} |"
        for trial in notable_factors[:15]
    )
    lines.extend([
        "", "## 解释限制", "",
        "历史最后 20% 未在本目录研究中评估，但此前单规则研究已查看过该时期，"
        "因此不能把它称作全新保留集。需要未来新数据做确认。", "",
        *[f"- {item}" for item in report["limitations"]],
        "", "因子逐项 IC/IR、原始及调整后 p 值见 `report.json`。", "",
    ])
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return report_path
