"""A fixed, causal BTC spot research protocol with a sealed final holdout."""

import hashlib
import importlib.metadata
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import polars as pl
from scipy.stats import spearmanr  # type: ignore[import-untyped]

from trading.alpha.factors import require_complete_grid
from trading.data.binance_archive import BinanceMonthlyArchive
from trading.data.quality import DataQualityReport, assess_ohlcv
from trading.single_asset_config import SingleAssetConfig


@dataclass(frozen=True, slots=True)
class StudyDataset:
    """The immutable normalized dataset and its quality and source evidence."""

    path: Path
    manifest_path: Path
    quality_path: Path
    sha256: str
    rows: int


def collect_study_data(config: SingleAssetConfig, project_root: Path) -> StudyDataset:
    """Fetch verified complete months, reject defects, and freeze one Parquet version."""
    raw = project_root / "data/raw/binance_archive"
    collector = BinanceMonthlyArchive(config.archive_symbol, config.timeframe, raw)
    frame, parts = collector.collect(config.start, config.end)
    report = assess_ohlcv(frame, config.start, config.end, config.timeframe)
    if not report.ok:
        raise ValueError(f"source data failed quality checks: {report.issues}")
    require_complete_grid(frame)
    source = json.dumps(
        {"config": config.fingerprint(),
         "parts": [{"sha256": part.sha256, "observed_at_utc": part.observed_at_utc}
                   for part in parts],
         "pipeline": "single-asset-v2"}, sort_keys=True
    )
    version = hashlib.sha256(source.encode()).hexdigest()[:16]
    stem = f"{config.archive_symbol.lower()}-{config.timeframe}-{version}"
    data_dir = project_root / "data/processed"
    report_dir = project_root / "research/reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"{stem}.parquet"
    if not path.exists():
        frame.write_parquet(path, compression="zstd")
    else:
        cached = pl.read_parquet(path)
        cached_report = assess_ohlcv(cached, config.start, config.end, config.timeframe)
        if not cached_report.ok or cached.height != frame.height:
            raise ValueError("existing versioned dataset is incomplete or corrupt")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    quality_path = report_dir / f"{stem}-quality.json"
    report.write(quality_path)
    manifest_path = report_dir / f"{stem}-manifest.json"
    manifest_path.write_text(
        json.dumps({"config_sha256": config.fingerprint(), "data_sha256": digest,
                    "data_path": str(path), "quality_path": str(quality_path),
                    "source_archives": [asdict(part) for part in parts],
                    "rows": frame.height, "interval": "[start,end)",
                    "bar_timestamp": "UTC open; usable only at open + 4 hours"},
                   indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return StudyDataset(path, manifest_path, quality_path, digest, frame.height)


def _block_interval(values: np.ndarray, block: int, resamples: int, seed: int) -> list[float]:
    """Circular block bootstrap interval for serially dependent bar returns."""
    if len(values) < block:
        raise ValueError("bootstrap segment is shorter than one block")
    rng = np.random.default_rng(seed)
    n = len(values)
    means = np.empty(resamples)
    for sample in range(resamples):
        starts = rng.integers(0, n, size=(n + block - 1) // block)
        indices = (starts[:, None] + np.arange(block)) % n
        means[sample] = values[indices.ravel()[:n]].mean()
    return [float(x) for x in np.quantile(means, [0.025, 0.975])]


def momentum_execution_target(close: pd.Series, lookback: int) -> pd.Series:
    """Apply a factor formed at close t only to the fill at close t+1."""
    if lookback < 2:
        raise ValueError("lookback must be at least two bars")
    factor = close / close.shift(lookback) - 1
    return factor.shift(1).gt(0).fillna(False)


def _performance(
    close: pd.Series, target: pd.Series, fee: float, slippage: float,
    cash: float, notional: float,
) -> tuple[dict[str, Any], np.ndarray]:
    """Execute a prior-close decision at the next bar close with vectorbt."""
    import vectorbt as vbt  # type: ignore[import-untyped]

    if close.empty:
        raise ValueError("empty evaluation segment")
    desired = target.astype(bool)
    previous = desired.shift(1, fill_value=False)
    entries = desired & ~previous
    exits = ~desired & previous
    entries.iloc[-1] = False
    exits.iloc[-1] = True
    # target is already one bar behind execution at the caller's boundary.
    portfolio = vbt.Portfolio.from_signals(
        close, entries=entries, exits=exits, init_cash=cash,
        size=notional, size_type="value", fees=fee, slippage=slippage,
        freq="4h", direction="longonly", accumulate=False,
    )
    baseline_entries = pd.Series(False, index=close.index)
    baseline_entries.iloc[0] = True
    baseline_exits = pd.Series(False, index=close.index)
    baseline_exits.iloc[-1] = True
    baseline = vbt.Portfolio.from_signals(
        close, entries=baseline_entries, exits=baseline_exits, init_cash=cash,
        size=notional, size_type="value", fees=fee, slippage=slippage,
        freq="4h", direction="longonly",
    )
    value = portfolio.value()
    baseline_value = baseline.value()
    returns = value.pct_change().fillna(0).to_numpy(dtype=float)
    baseline_returns = baseline_value.pct_change().fillna(0).to_numpy(dtype=float)
    sd = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
    metrics = {
        "bars": len(close), "start_utc": close.index[0].isoformat(),
        "end_utc": close.index[-1].isoformat(),
        "return_pct": float((value.iloc[-1] / cash - 1) * 100),
        "buy_hold_100_usdt_return_pct": float((baseline_value.iloc[-1] / cash - 1) * 100),
        "max_drawdown_pct": float(portfolio.max_drawdown() * 100),
        "sharpe_annualized": float(np.mean(returns) / sd * np.sqrt(6 * 365)) if sd else None,
        "orders": int(portfolio.orders.count()),
        "closed_trades": int(portfolio.trades.closed.count()),
        "ending_value_usdt": float(value.iloc[-1]),
    }
    return metrics, returns - baseline_returns


def run_study(
    config: SingleAssetConfig, dataset: StudyDataset, project_root: Path
) -> Path:
    """Evaluate a prespecified momentum rule on rolling OOS and untouched holdout."""
    frame = pl.read_parquet(dataset.path).sort("timestamp")
    quality: DataQualityReport = assess_ohlcv(frame, config.start, config.end, config.timeframe)
    if not quality.ok or frame.height != dataset.rows:
        raise ValueError("study dataset changed or failed quality checks")
    if hashlib.sha256(dataset.path.read_bytes()).hexdigest() != dataset.sha256:
        raise ValueError("study dataset SHA-256 changed")
    close = pd.Series(
        frame["close"].to_numpy(),
        index=pd.DatetimeIndex(frame["timestamp"].to_list()) + timedelta(hours=4),
        name="close",
    )
    factor = close / close.shift(config.lookback_bars) - 1
    # Each row executes at its close using only the preceding close's factor.
    target_at_execution = momentum_execution_target(close, config.lookback_bars)
    n = len(close)
    holdout_start = int(n * (1 - config.holdout_fraction))
    if holdout_start < config.train_bars + config.embargo_bars + config.test_bars:
        raise ValueError("insufficient development bars for one complete OOS fold")
    fee = float(config.fee_bps_per_side / 10_000)
    slippage = float(config.slippage_bps_per_side / 10_000)
    cash = float(config.initial_cash_usdt)
    notional = float(config.order_notional_usdt)
    folds: list[dict[str, Any]] = []
    fold_differences: list[np.ndarray] = []
    cursor = config.train_bars + config.embargo_bars
    development_end = holdout_start - config.embargo_bars
    while cursor < development_end:
        end = min(cursor + config.test_bars, development_end)
        if end - cursor < 30:
            break
        metrics, difference = _performance(
            close.iloc[cursor:end], target_at_execution.iloc[cursor:end],
            fee, slippage, cash, notional,
        )
        metrics["train_bars"] = [cursor - config.embargo_bars - config.train_bars,
                                 cursor - config.embargo_bars]
        metrics["test_bars"] = [cursor, end]
        folds.append(metrics)
        fold_differences.append(difference)
        cursor = end
    if not folds:
        raise ValueError("no complete development fold")
    holdout_metrics, holdout_diff = _performance(
        close.iloc[holdout_start:], target_at_execution.iloc[holdout_start:],
        fee, slippage, cash, notional,
    )
    # Signal at close t predicts the return after the next-close entry, t+1 to t+2.
    future_return = close.shift(-2) / close.shift(-1) - 1
    valid = factor.iloc[holdout_start:-2].notna() & future_return.iloc[holdout_start:-2].notna()
    ic = spearmanr(
        factor.iloc[holdout_start:-2][valid],
        future_return.iloc[holdout_start:-2][valid],
    )
    development_diff = np.concatenate(fold_differences)
    code_hasher = hashlib.sha256()
    for source_path in (
        Path(__file__), Path(__file__).parents[1] / "data/binance_archive.py",
        project_root / "pyproject.toml", project_root / "uv.lock",
    ):
        if source_path.exists():
            code_hasher.update(source_path.read_bytes())
    report: dict[str, Any] = {
        "protocol": "single-asset-btc-4h-momentum-v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "config_sha256": config.fingerprint(), "data_sha256": dataset.sha256,
        "code_and_lock_sha256": code_hasher.hexdigest(),
        "python_version": sys.version.split()[0],
        "vectorbt_version": importlib.metadata.version("vectorbt"),
        "dataset_path": str(dataset.path), "source_manifest": str(dataset.manifest_path),
        "quality_path": str(dataset.quality_path),
        "hypothesis_count": 1, "multiple_comparison_correction": "identity (one prespecified rule)",
        "rule": (
            f"long {notional:g} USDT BTC when previous completed close / close "
            f"{config.lookback_bars} bars earlier - 1 > 0; otherwise cash"
        ),
        "bar_timing": "UTC bar open in data; signal available at close; execution at next 4h close",
        "execution_assumptions": {
            "fee_bps_per_side": float(config.fee_bps_per_side),
            "slippage_bps_per_side": float(config.slippage_bps_per_side),
            "latency_ms": config.latency_ms,
            "latency_treatment": (
                "one complete bar delay exceeds configured 1s latency"
            ),
            "position": "long only, cash funded, no short or leverage",
            "terminal_liquidation": "sell any remaining BTC at final close with costs",
        },
        "development_oos_folds": folds,
        "development_unscored_bars": int(
            config.train_bars + config.embargo_bars +
            max(0, development_end - cursor) + config.embargo_bars
        ),
        "development_oos_mean_excess_bar_return_ci95": _block_interval(
            development_diff, config.bootstrap_block_bars,
            config.bootstrap_resamples, config.seed,
        ),
        "final_holdout": holdout_metrics,
        "final_holdout_mean_excess_bar_return_ci95": _block_interval(
            holdout_diff, config.bootstrap_block_bars,
            config.bootstrap_resamples, config.seed + 1,
        ),
        "final_holdout_timeseries_spearman_ic": float(ic.statistic),
        "final_holdout_ic_observations": int(valid.sum()),
        "limitations": [
            "OHLCV has no historical bid/ask spread or order book depth; "
            "10 bps slippage is an assumed cost.",
            "Spot BTC/USDT has no funding or corporate-action adjustment in this protocol.",
            "Bar-close fills cannot establish live execution quality or intrabar risk behavior.",
            "A single asset and fixed rule cannot establish generalizable alpha.",
        ],
    }
    run_dir = (
        project_root / "research/runs"
        / f"single-asset-{dataset.sha256[:12]}-{config.fingerprint()[:12]}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "report.json"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
                         encoding="utf-8")
    markdown: list[str] = [
        "# BTC/USDT 4h 单资产研究报告", "", f"配置哈希：`{config.fingerprint()}`",
        f"数据 SHA-256：`{dataset.sha256}`", "",
        "## 预注册规则与时间约定", "", report["rule"], "",
        report["bar_timing"], "",
        "## 样本外结果", "",
        f"开发阶段完整滚动窗口：{len(folds)} 个；最终保留集：{holdout_metrics['bars']} 根。",
        f"保留集净收益：{holdout_metrics['return_pct']:.4f}%；"
        f"同额买入持有：{holdout_metrics['buy_hold_100_usdt_return_pct']:.4f}%。",
        f"保留集最大回撤：{holdout_metrics['max_drawdown_pct']:.4f}%；"
        f"年化 Sharpe：{holdout_metrics['sharpe_annualized']}。",
        f"保留集时间序列 Spearman IC：{report['final_holdout_timeseries_spearman_ic']:.4f}。",
        "保留集超额逐根收益均值的区块 bootstrap 95% 区间："
        f"{report['final_holdout_mean_excess_bar_return_ci95']}。",
        "", "## 成本与限制", "",
        f"每侧手续费 {config.fee_bps_per_side} bps、滑点 "
        f"{config.slippage_bps_per_side} bps；单次买入 {notional:g} USDT。",
        *[f"- {item}" for item in report["limitations"]],
        "", "所有数值和滚动窗口明细见 `report.json`。", "",
    ]
    (run_dir / "report.md").write_text("\n".join(markdown), encoding="utf-8")
    return json_path
