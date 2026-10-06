"""A concise factor-study report that exposes every attempted trial."""

from dataclasses import dataclass
from pathlib import Path

from trading.alpha.evaluation import StudyResult


@dataclass(frozen=True, slots=True)
class StudyContext:
    """Inputs that a reader needs to reproduce and judge a factor study."""

    title: str
    data_version: str
    data_sha256: str
    config_sha256: str
    universe: str
    bar_policy: str
    cost_policy: str
    trial_inventory: str
    validation_protocol: str
    random_seed: int
    limitations: str


def render_factor_report(context: StudyContext, study: StudyResult) -> str:
    """Render an auditable Markdown report without inventing performance data."""
    header = [
        f"# {context.title}",
        "",
        "## Reproducibility",
        "",
        f"- Dataset version: `{context.data_version}`",
        f"- Dataset SHA-256: `{context.data_sha256}`",
        f"- Configuration SHA-256: `{context.config_sha256}`",
        f"- Universe: {context.universe}",
        f"- Bar availability and adjustment policy: {context.bar_policy}",
        f"- Fee, slippage, funding, and latency policy: {context.cost_policy}",
        f"- All attempted factors and parameters: {context.trial_inventory}",
        f"- Validation windows, embargo, bootstrap and correction: {context.validation_protocol}",
        f"- Random seed: `{context.random_seed}`",
        "",
        "## Walk-forward out-of-sample evaluation",
        "",
        "| Fold | Training window | Test window | Selected factor | Test IC | "
        "Test IC/IR | Test periods |",
        "| --- | --- | --- | --- | ---: | ---: | ---: |",
    ]
    for fold in study.folds:
        test = fold.test
        header.append(
            "| {fold} | {train_start} to {train_end} | {test_start} to {test_end} | "
            "{selected} | {ic} | {ir} | {periods} |".format(
                fold=fold.fold,
                train_start=fold.train_start,
                train_end=fold.train_end_exclusive,
                test_start=fold.test_start,
                test_end=fold.test_end_exclusive,
                selected=fold.selected or "None after correction",
                ic=f"{test.mean_ic:.4f}" if test and test.mean_ic is not None else "N/A",
                ir=f"{test.ic_ir:.4f}" if test and test.ic_ir is not None else "N/A",
                periods=test.periods if test else 0,
            )
        )
    final = study.final_holdout
    header.extend(
        [
            "",
            "## Reserved final holdout",
            "",
            f"- Holdout starts: `{study.final_holdout_start}`",
            "- Factor frozen from latest development training fold: "
            f"`{study.frozen_factor or 'None'}`",
            f"- Final IC: {final.mean_ic if final else 'N/A'}",
            f"- Final IC/IR: {final.ic_ir if final else 'N/A'}",
            f"- Final evaluated periods: {final.periods if final else 0}",
            "",
            "## Training-side multiple comparisons",
            "",
        ]
    )
    for fold in study.folds:
        header.append(f"### Fold {fold.fold}")
        header.append("")
        header.append("| Factor | Training IC | Raw p | Adjusted p |")
        header.append("| --- | ---: | ---: | ---: |")
        for trial in fold.trials:
            ic = trial.train.mean_ic
            header.append(
                f"| {trial.name} | {f'{ic:.4f}' if ic is not None else 'N/A'} | "
                f"{trial.raw_p:.4f} | {trial.adjusted_p:.4f} |"
            )
        header.append("")
    header.extend(
        [
            "## Stress tests and implementation shortfall",
            "",
            "Pending: use event-driven simulation and measured execution data. The IC and",
            "vectorbt screening results alone are not a deployable performance claim.",
            "",
            "## Limitations",
            "",
            context.limitations,
            "",
        ]
    )
    return "\n".join(header)


def write_factor_report(
    path: Path, context: StudyContext, study: StudyResult
) -> None:
    """Write the complete factor-study report to an explicit local path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_factor_report(context, study), encoding="utf-8")
