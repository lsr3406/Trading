"""Dataset hashing and source provenance tests."""

import hashlib
from pathlib import Path

import pytest

from trading.configuration import load_settings
from trading.research.manifest import build_manifest

BASE = Path(__file__).resolve().parents[1] / "config/base.yaml"
READY = {
    "TRADING__RESEARCH__DATA_VERSION": "fixture-v1",
    "TRADING__COSTS__FEE_BPS": "1",
    "TRADING__COSTS__SLIPPAGE_BPS": "2",
    "TRADING__COSTS__FUNDING_BPS": "0",
    "TRADING__COSTS__LATENCY_MS": "10",
}


def test_manifest_hashes_data_and_configuration(tmp_path: Path) -> None:
    """An experiment records exact data bytes and configuration identity."""
    source = tmp_path / "sample.csv"
    source.write_bytes(b"timestamp,price\n2025-01-01,100\n")
    settings = load_settings(BASE, environ=READY)
    manifest = build_manifest(settings, source, BASE.parents[1])
    assert manifest.data_sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
    assert manifest.config_sha256 == settings.fingerprint()
    assert manifest.data_version == "fixture-v1"


def test_manifest_refuses_missing_data(tmp_path: Path) -> None:
    """A missing dataset cannot produce an apparently valid experiment."""
    with pytest.raises(FileNotFoundError):
        build_manifest(load_settings(BASE, environ=READY), tmp_path / "missing.csv", tmp_path)
