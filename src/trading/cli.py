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
    commands = parser.add_subparsers(dest="command")
    workbench = commands.add_parser("workbench", help="serve the local research workbench")
    workbench.add_argument("--port", type=int, default=8765)
    commands.add_parser("doctor", help="check local project readiness")
    collect = commands.add_parser("single-collect", help="collect and validate fixed BTC spot data")
    collect.add_argument("--study", type=Path, default=Path("config/single_asset.yaml"))
    multi = commands.add_parser("multi-collect", help="collect declared Binance spot markets")
    multi.add_argument("--universe", type=Path, default=Path("config/multi_asset.yaml"))
    book = commands.add_parser("record-book", help="record public Coinbase Level 2 book")
    book.add_argument("--feed", type=Path, default=Path("config/coinbase_l2.yaml"))
    book.add_argument("--duration", type=int, help="seconds; 0 records until interrupted")
    snapshot = commands.add_parser("book-snapshot", help="capture one public Coinbase L2 snapshot")
    snapshot.add_argument("--product", default="BTC-USD")
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
    catalog = commands.add_parser(
        "catalog-study", help="evaluate the predeclared OHLCV factor and strategy catalog"
    )
    catalog.add_argument("--study", type=Path, default=Path("config/single_asset.yaml"))
    catalog.add_argument("--catalog", type=Path, default=Path("config/factor_strategy.yaml"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run a local diagnostic or write a reproducibility manifest."""
    args = _parser().parse_args(argv)
    settings = load_settings(args.config)
    configure_logging(settings.logging)
    logger = logging.getLogger("trading.cli")
    if args.command in (None, "workbench"):
        from trading.workbench.server import serve

        port = args.port if args.command == "workbench" else 8765
        if not 1 <= port <= 65535:
            parser_error = "workbench port must be between 1 and 65535"
            raise ValueError(parser_error)
        serve(args.config.resolve().parent.parent, port=port)
        return 0
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
    if args.command == "single-collect":
        from trading.research.single_asset import collect_study_data
        from trading.single_asset_config import load_single_asset_config

        protocol = load_single_asset_config(args.study)
        project_root = args.study.resolve().parent.parent
        dataset = collect_study_data(protocol, project_root)
        print(json.dumps({"dataset": str(dataset.path), "quality": str(dataset.quality_path),
                          "manifest": str(dataset.manifest_path)}, indent=2))
        return 0
    if args.command == "multi-collect":
        from trading.data.multi_asset_archive import (
            collect_multi_asset_data,
            load_multi_asset_config,
        )

        multi_protocol = load_multi_asset_config(args.universe)
        multi_dataset = collect_multi_asset_data(
            multi_protocol, args.universe.resolve().parent.parent
        )
        print(json.dumps({
            "dataset": str(multi_dataset.path), "calendar": str(multi_dataset.calendar_path),
            "quality": str(multi_dataset.quality_path),
            "manifest": str(multi_dataset.manifest_path), "rows": multi_dataset.rows,
        }, indent=2))
        return 0
    if args.command == "record-book":
        import asyncio

        from trading.data.coinbase_l2 import CoinbaseL2Recorder, load_coinbase_l2_config

        feed_protocol = load_coinbase_l2_config(args.feed)
        if args.duration is not None:
            if args.duration < 0:
                raise ValueError("duration must be nonnegative")
            feed_protocol = feed_protocol.model_copy(update={"duration_seconds": args.duration})
        recorder = CoinbaseL2Recorder(feed_protocol, args.feed.resolve().parent.parent)
        try:
            recording = asyncio.run(recorder.record())
        except KeyboardInterrupt:
            return 130
        output = {"raw": str(recording.raw_path),
                  "quality": str(recording.quality_path),
                  "checkpoints": [str(path) for path in recording.checkpoint_paths],
                  "ok": recording.ok}
        if not recording.ok:
            quality = json.loads(recording.quality_path.read_text(encoding="utf-8"))
            interruptions = quality.get("interruptions", [])
            output["reason"] = quality.get("fatal_error") or (
                interruptions[-1]["reason"] if interruptions else "no complete snapshot"
            )
        print(json.dumps(output, indent=2))
        return 0 if recording.ok else 2
    if args.command == "book-snapshot":
        from trading.data.coinbase_l2 import capture_rest_snapshot

        point = capture_rest_snapshot(args.product, args.config.resolve().parent.parent)
        print(json.dumps({"raw": str(point.raw_path), "quality": str(point.quality_path),
                          "checkpoint": [str(path) for path in point.checkpoint_paths],
                          "ok": point.ok}, indent=2))
        return 0 if point.ok else 2
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
    if args.command == "catalog-study":
        from trading.research.catalog_study import run_catalog_study
        from trading.research.single_asset import collect_study_data
        from trading.research.strategies import load_research_catalog
        from trading.single_asset_config import load_single_asset_config

        protocol = load_single_asset_config(args.study)
        catalog = load_research_catalog(args.catalog)
        project_root = args.study.resolve().parent.parent
        dataset = collect_study_data(protocol, project_root)
        result = run_catalog_study(protocol, catalog, dataset, project_root)
        from trading.alpha.catalog import standard_catalog

        print(json.dumps({"report": str(result),
                          "factor_count": len(standard_catalog(catalog.windows).factors),
                          "strategy_count": len(catalog.strategies)}, indent=2))
        return 0
    raise AssertionError("unreachable command")
