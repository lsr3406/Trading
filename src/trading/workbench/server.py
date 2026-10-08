"""Loopback-only HTTP interface for local quantitative research artifacts."""

import json
import secrets
from pathlib import Path
from typing import Any, Literal

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.trustedhost import TrustedHostMiddleware

from trading.workbench.jobs import JobManager
from trading.workbench.state import (
    book_recordings,
    catalog,
    datasets,
    latest_report,
    overview,
    price_series,
    roadmap,
)

STATIC = Path(__file__).parent / "static"


class JobRequest(BaseModel):
    """One fixed local research action; arbitrary arguments are forbidden."""

    command: Literal[
        "doctor", "collect", "multi-collect", "record-book", "book-snapshot",
        "single-study", "catalog-study",
    ]


def create_app(root: Path) -> FastAPI:
    """Build an isolated workbench for one project checkout."""
    project_root = root.resolve()
    if not (project_root / "config/base.yaml").is_file():
        raise ValueError("workbench root must contain config/base.yaml")
    token = secrets.token_urlsafe(32)
    jobs = JobManager(project_root)
    app = FastAPI(title="Trading 研究工作台", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost"]
    )
    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.middleware("http")
    async def no_cache(request: Request, call_next: Any) -> Response:
        """Avoid showing stale report and job state after a run."""
        response: Response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        """Serve the static shell with one per-process write token."""
        html = (STATIC / "index.html").read_text(encoding="utf-8")
        html = html.replace("__WORKBENCH_TOKEN__", token)
        return HTMLResponse(html)

    @app.get("/api/overview")
    def get_overview() -> dict[str, Any]:
        """Summarize data, evidence, and the disabled execution posture."""
        return overview(project_root)

    @app.get("/api/datasets")
    def get_datasets() -> list[dict[str, Any]]:
        """List project-owned normalized datasets and quality reports."""
        return datasets(project_root)

    @app.get("/api/book-recordings")
    def get_book_recordings() -> list[dict[str, Any]]:
        """List recorded Level 2 sessions and their continuity verdicts."""
        return book_recordings(project_root)

    @app.get("/api/price")
    def get_price(dataset: str | None = Query(default=None)) -> dict[str, Any]:
        """Return a bounded sampled price line from an inventoried dataset."""
        return price_series(project_root, dataset)

    @app.get("/api/catalog")
    def get_catalog() -> dict[str, Any]:
        """Return declared factors, strategies, and evaluated trials."""
        return catalog(project_root)

    @app.get("/api/reports/{kind}")
    def get_report(kind: Literal["single", "catalog"]) -> dict[str, Any]:
        """Return the latest full report for an allowed research family."""
        report = latest_report(project_root, kind)
        if report is None:
            raise HTTPException(status_code=404, detail="research report not found")
        return report

    @app.get("/api/roadmap")
    def get_roadmap() -> dict[str, Any]:
        """Return the documented next-source plan."""
        return roadmap(project_root)

    @app.get("/api/jobs")
    def get_jobs() -> list[dict[str, Any]]:
        """List recent local research jobs."""
        return jobs.list()

    @app.post("/api/jobs", status_code=202)
    def post_job(
        payload: JobRequest, x_workbench_token: str | None = Header(default=None)
    ) -> dict[str, Any]:
        """Start one allowlisted offline command after checking the local token."""
        if x_workbench_token is None or not secrets.compare_digest(x_workbench_token, token):
            raise HTTPException(status_code=403, detail="invalid workbench token")
        try:
            return jobs.start(payload.command)
        except RuntimeError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    return app


def serve(root: Path, *, port: int = 8765) -> None:
    """Run only on loopback so no remote host can access local artifacts."""
    app = create_app(root)
    print(json.dumps({"workbench": f"http://127.0.0.1:{port}"}, ensure_ascii=False))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
