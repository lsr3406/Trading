"""Prevent imports across the offline research and execution boundary."""

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src/trading"
OFFLINE = ("data", "alpha", "strategy", "backtest", "research")


def _imports(path: Path) -> set[str]:
    """Collect absolute and relative imported module names from a source file."""
    imported: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module)
            imported.update(f"{node.module}.{alias.name}" for alias in node.names)
            if node.level:
                imported.update(alias.name for alias in node.names)
    return imported


def test_offline_layers_cannot_import_execution() -> None:
    """Data, alpha, strategy, backtest, and research cannot reach broker code."""
    for layer in OFFLINE:
        for path in (SOURCE / layer).rglob("*.py"):
            names = _imports(path)
            assert not any(
                name == "execution" or name.startswith("trading.execution")
                for name in names
            )


def test_execution_cannot_import_offline_layers() -> None:
    """The order path may use shared contracts and risk, not research code."""
    for path in (SOURCE / "execution").rglob("*.py"):
        names = _imports(path)
        for layer in OFFLINE:
            assert not any(name == layer or name.startswith(f"trading.{layer}") for name in names)
