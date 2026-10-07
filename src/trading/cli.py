"""Small local CLI for configuration checks and research provenance."""

import argparse
import json
import logging
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl

from trading.configuration import assert_research_ready, load_settings
from trading.data.quality import assess
from trading.data.schema import DataKind
from trading.logging_setup import configure_logging
from trading.research.manifest import build_manifest


def _parser() -> argparse.ArgumentParser:
    """Create the command-line parser without side effects."""
    parser = argparse.ArgumentParser(prog="trading")
    parser.add_argument("--config", type=Path, default=Path("config/base.yaml"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="check local project readiness")
    manifest = commands.add_parser("manifest", help="record one dataset's provenance")
    manifest.add_argument("--data", type=Path, required=True)
    manifest.add_argument("--output", type=Path, required=True)
    quality = commands.add_parser("quality", help="assess a normalized Parquet dataset")
    quality.add_argument("--kind", choices=("ohlcv", "orderbook", "funding"), required=True)
    quality.add_argument("--data", type=Path, required=True)
    quality.add_argument("--output", type=Path, required=True)
    quality.add_argument("--start", type=datetime.fromisoformat)
    quality.add_argument("--end", type=datetime.fromisoformat)
    quality.add_argument("--interval")
    single = commands.add_parser("single-study", help="collect and evaluate fixed BTC spot study")
    single.add_argument("--study", type=Path, default=Path("config/single_asset.yaml"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run a local diagnostic or write a reproducibility manifest."""
    args = _parser().parse_args(argv)
    settings = load_settings(args.config)
    configure_logging(settings.logging)
    logger = logging.getLogger("trading.cli")
    if args.command == "doctor":
        try:
            assert_research_ready(settings)
            research_state = "ready_for_research_code"
        except ValueError as error:
            research_state = f"incomplete: {error}"
        print(
            json.dumps(
                {
                    "project": settings.project.name,
                    "configuration_sha256": settings.fingerprint(),
                    "research": research_state,
                    "execution_mode": settings.execution.mode,
                    "live_execution": "unavailable",
                },
                indent=2,
            )
        )
        return 0
    if args.command == "manifest":
        project_root = args.config.resolve().parent.parent
        manifest = build_manifest(settings, args.data, project_root)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(manifest.to_json() + "\n", encoding="utf-8")
        logger.info("Wrote run manifest to %s", args.output)
        return 0
    if args.command == "quality":
        kind = cast(DataKind, args.kind)
        window = {}
        if kind == "ohlcv":
            if args.start is None or args.end is None or args.interval is None:
                raise ValueError("OHLCV quality requires --start, --end, and --interval")
            window = {"start": args.start, "end": args.end, "interval": args.interval}
        report = assess(kind, pl.read_parquet(args.data), **window)
        report.write(args.output)
        print(report.to_json())
        return 0 if report.ok else 2
    if args.command == "single-study":
        from trading.research.single_asset import collect_study_data, run_study
        from trading.single_asset_config import load_single_asset_config

        protocol = load_single_asset_config(args.study)
        project_root = args.study.resolve().parent.parent
        dataset = collect_study_data(protocol, project_root)
        result = run_study(protocol, dataset, project_root)
        print(json.dumps({"report": str(result), "dataset": str(dataset.path),
                          "quality": str(dataset.quality_path)}, indent=2))
        return 0
    raise AssertionError("unreachable command")
