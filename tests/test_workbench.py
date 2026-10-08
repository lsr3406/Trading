"""Offline contract checks for the local, research-only web workbench."""

import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from trading.workbench.jobs import JobManager
from trading.workbench.server import create_app


def _project(tmp_path: Path) -> Path:
    """Build the smallest isolated project with one local data artifact."""
    config = tmp_path / "config"
    config.mkdir()
    (config / "base.yaml").write_text("project: {name: Trading}\n", encoding="utf-8")
    (config / "execution.paper.yaml").write_text("mode: paper\nstarting_cash: null\n")
    (config / "factor_strategy.yaml").write_text(
        "windows: [6]\ncorrection: bonferroni\nsignificance_level: 0.05\n"
        "strategies: [{name: momentum_6, kind: momentum, parameters: {window: 6}}]\n"
    )
    (config / "data_sources.yaml").write_text("checked_on: 2026-10-07\nsources: []\n")
    processed = tmp_path / "data/processed"
    processed.mkdir(parents=True)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    pl.DataFrame({
        "timestamp": [start + timedelta(hours=4 * i) for i in range(3)],
        "close": [100.0, 102.0, 101.0],
        "symbol": ["BTC/USDT"] * 3,
        "exchange": ["binance"] * 3,
        "timeframe": ["4h"] * 3,
    }).write_parquet(processed / "btcusdt-4h-test.parquet")
    reports = tmp_path / "research/reports"
    reports.mkdir(parents=True)
    (reports / "btcusdt-4h-test-quality.json").write_text(
        '{"ok": true, "metrics": {"missing_bars": 0, "duplicates": 0}}\n'
    )
    return tmp_path


def test_workbench_reads_only_project_artifacts(tmp_path: Path) -> None:
    """Local APIs show versioned data and refuse arbitrary dataset paths."""
    root = _project(tmp_path)
    with TestClient(create_app(root), base_url="http://localhost") as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "研究工作台" in page.text
        assert "__WORKBENCH_TOKEN__" not in page.text
        assert client.get("/static/app.js").status_code == 200
        overview = client.get("/api/overview").json()
        assert overview["dataset_count"] == 1
        assert overview["primary_dataset"]["quality"]["ok"]
        assert overview["execution"]["mode"] == "disabled"
        assert client.get("/api/price").json()["points"][0]["close"] == 100.0
        assert client.get("/api/price?dataset=../../.env").json()["points"] == []
        assert client.get("/api/catalog").json()["factors"]
        assert client.get("/api/reports/single").status_code == 404
        assert client.get("/api/book-recordings").json() == []


def test_workbench_lists_book_evidence(tmp_path: Path) -> None:
    """A recorded interruption appears as a failed local data session."""
    root = _project(tmp_path)
    report = root / "research/reports/coinbase-l2-example-quality.json"
    report.write_text(json.dumps({
        "ok": False, "products": ["BTC-USD"],
        "counts": {"messages": 1, "snapshots": 1, "updates": 0},
        "interruptions": [{"reason": "socket closed"}],
        "raw_sha256": "abc", "checkpoint_parts": [],
    }))
    with TestClient(create_app(root), base_url="http://localhost") as client:
        sessions = client.get("/api/book-recordings").json()
        assert len(sessions) == 1
        assert sessions[0]["id"] == "coinbase-l2-example"
        assert sessions[0]["interruptions"] == 1
        assert sessions[0]["ok"] is False


def test_workbench_jobs_require_token_and_allowlist(tmp_path: Path) -> None:
    """Browser writes need a token; unknown commands cannot run."""
    with TestClient(create_app(_project(tmp_path)), base_url="http://localhost") as client:
        assert client.post("/api/jobs", json={"command": "doctor"}).status_code == 403
        assert client.post(
            "/api/jobs", json={"command": "../../shell"},
            headers={"X-Workbench-Token": "wrong"},
        ).status_code == 422
        assert client.get("/api/jobs").json() == []


def test_job_manager_uses_fixed_python_command(tmp_path: Path, monkeypatch: object) -> None:
    """An allowlisted action becomes one Python command with no shell."""
    root = _project(tmp_path)
    calls: list[tuple[list[str], Path]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs["cwd"]))  # type: ignore[arg-type]
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)  # type: ignore[attr-defined]
    manager = JobManager(root)
    try:
        job = manager.start("doctor")
        for _ in range(100):
            if manager.list()[0]["status"] == "completed":
                break
            time.sleep(0.01)
        assert manager.list()[0]["id"] == job["id"]
        assert manager.list()[0]["status"] == "completed"
        assert calls[0][0][-3:] == ["-m", "trading", "doctor"]
        assert calls[0][1] == root
    finally:
        manager.close()
