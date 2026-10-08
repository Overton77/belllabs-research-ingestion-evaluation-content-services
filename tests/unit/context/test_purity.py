"""The Context Packer and renderers are pure: no I/O, no clock, no randomness (SPEC-02)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DOMAIN_CONTEXT = (
    Path(__file__).resolve().parents[3] / "src" / "mission_control" / "domain" / "context"
)
IMPURE_MODULES = {
    "os",
    "pathlib",
    "io",
    "time",
    "random",
    "secrets",
    "uuid",
    "socket",
    "subprocess",
    "asyncio",
    "tempfile",
    "shutil",
}
IMPURE_CALLS = {"now", "utcnow", "today", "time", "open", "uuid4", "uuid7", "random"}
FORBIDDEN_LAYERS = {"application", "adapters", "interfaces", "bootstrap"}


def _modules() -> list[Path]:
    files = sorted(DOMAIN_CONTEXT.glob("*.py"))
    assert files, "domain/context scan found no sources"
    return files


@pytest.mark.parametrize("path", _modules(), ids=lambda path: path.name)
def test_domain_context_imports_nothing_impure(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        else:
            continue
        for name in names:
            parts = name.split(".")
            if parts[0] in IMPURE_MODULES or (
                parts[0] == "mission_control" and len(parts) > 1 and parts[1] in FORBIDDEN_LAYERS
            ):
                found.append(f"{path.name}:{node.lineno}: {name}")
    assert not found, "\n".join(found)


@pytest.mark.parametrize("path", _modules(), ids=lambda path: path.name)
def test_domain_context_reads_no_clock_and_draws_no_randomness(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = [
        f"{path.name}:{node.lineno}: {node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id}"  # noqa: E501
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Attribute) and node.func.attr in IMPURE_CALLS)
            or (isinstance(node.func, ast.Name) and node.func.id in IMPURE_CALLS)
        )
    ]
    assert not calls, "\n".join(calls)
