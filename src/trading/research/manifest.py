"""Reproducibility metadata for an immutable local dataset and experiment."""

import hashlib
import json
import platform
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from trading.configuration import Settings, assert_research_ready


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Configuration, data, source, and runtime identity for one experiment."""

    created_at_utc: str
    project: str
    seed: int
    data_version: str
    data_sha256: str
    config_sha256: str
    source_commit: str | None
    source_dirty: bool
    python_version: str

    def to_json(self) -> str:
        """Serialize the manifest in stable, human-readable JSON."""
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def _sha256_file(path: Path) -> str:
    """Hash raw dataset bytes in bounded chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_output(root: Path, *args: str) -> str | None:
    """Read Git provenance without mutating the repository."""
    result = subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=False
    )
    return result.stdout.strip() if result.returncode == 0 else None


def build_manifest(settings: Settings, data_file: Path, project_root: Path) -> RunManifest:
    """Build a manifest only after data version and cost assumptions are set."""
    assert_research_ready(settings)
    if not data_file.is_file():
        raise FileNotFoundError(data_file)
    dirty_output = _git_output(project_root, "status", "--porcelain")
    return RunManifest(
        created_at_utc=datetime.now(UTC).isoformat(),
        project=settings.project.name,
        seed=settings.research.seed,
        data_version=settings.research.data_version,
        data_sha256=_sha256_file(data_file),
        config_sha256=settings.fingerprint(),
        source_commit=_git_output(project_root, "rev-parse", "HEAD"),
        source_dirty=bool(dirty_output) if dirty_output is not None else True,
        python_version=platform.python_version(),
    )
