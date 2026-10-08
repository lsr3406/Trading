"""Small allowlisted job runner for local, offline research commands."""

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

JOB_COMMANDS: dict[str, tuple[str, ...]] = {
    "doctor": ("doctor",),
    "collect": ("single-collect",),
    "multi-collect": ("multi-collect",),
    "record-book": ("record-book",),
    "book-snapshot": ("book-snapshot",),
    "single-study": ("single-study",),
    "catalog-study": ("catalog-study",),
}


class JobManager:
    """Serialize fixed research jobs and retain bounded status in memory."""

    def __init__(self, root: Path) -> None:
        """Bind all jobs to one trusted project root."""
        self._root = root.resolve()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="trading-workbench")
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active: str | None = None

    def list(self) -> list[dict[str, Any]]:
        """Return recent jobs, newest first, without exposing mutable state."""
        with self._lock:
            return [dict(job) for job in reversed(list(self._jobs.values()))]

    def start(self, command: str) -> dict[str, Any]:
        """Start an allowlisted command or reject an overlapping run."""
        if command not in JOB_COMMANDS:
            raise ValueError("unknown research command")
        with self._lock:
            if self._active is not None:
                raise RuntimeError("another research job is running")
            job_id = uuid4().hex
            job: dict[str, Any] = {
                "id": job_id,
                "command": command,
                "status": "queued",
                "started_at_utc": None,
                "finished_at_utc": None,
                "returncode": None,
                "output": "",
            }
            self._jobs[job_id] = job
            self._active = job_id
            while len(self._jobs) > 20:
                self._jobs.pop(next(iter(self._jobs)))
        self._executor.submit(self._run, job_id, JOB_COMMANDS[command])
        return dict(job)

    def _run(self, job_id: str, command: tuple[str, ...]) -> None:
        """Run one fixed command in a child Python process with a timeout."""
        with self._lock:
            self._jobs[job_id]["status"] = "running"
            self._jobs[job_id]["started_at_utc"] = datetime.now(UTC).isoformat()
        try:
            result = subprocess.run(
                [sys.executable, "-m", "trading", *command],
                cwd=self._root,
                capture_output=True,
                text=True,
                timeout=3600,
                check=False,
            )
            output = (result.stdout + "\n" + result.stderr)[-40_000:]
            status = "completed" if result.returncode == 0 else "failed"
            returncode: int | None = result.returncode
        except subprocess.TimeoutExpired as error:
            output = f"Research job exceeded one hour: {error}"
            status, returncode = "failed", None
        except OSError as error:
            output = f"Could not start research job: {error}"
            status, returncode = "failed", None
        with self._lock:
            job = self._jobs[job_id]
            job.update({
                "status": status,
                "returncode": returncode,
                "output": output,
                "finished_at_utc": datetime.now(UTC).isoformat(),
            })
            self._active = None

    def close(self) -> None:
        """Release the executor after any running child has finished."""
        self._executor.shutdown(wait=False, cancel_futures=False)
