"""FT-A5: the seed policy template hook (scripts/hooks/policy_template)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "hooks" / "policy_template" / "policy.py"


def _run(tool: dict[str, object] | None, **extra: object) -> dict[str, object]:
    hook_input: dict[str, object] = {
        "schema_version": "mc.hook_input.v1",
        "event": "before_shell",
        "lane_profile": "cursor_local",
        "scope": {"installation_id": "i", "application_id": "biotech"},
        "workspace_root": "/work",
    }
    if tool is not None:
        hook_input["tool"] = tool
    hook_input.update(extra)
    completed = subprocess.run(
        [sys.executable, str(SCRIPT)],
        input=json.dumps(hook_input).encode(),
        capture_output=True,
        check=True,
        timeout=30,
    )
    result = json.loads(completed.stdout)
    assert result["schema_version"] == "mc.hook_result.v1"
    return result


def _shell(command: str) -> dict[str, object]:
    return {"name": "shell", "input": {"command": command}}


@pytest.mark.parametrize(
    "command",
    [
        "rm -rf /",
        "rm -fr /",
        "sudo rm -r -f /",
        "rm --recursive --force /",
        "cd /tmp && rm -rf / ",
        "rm -rf --no-preserve-root /",
        "git push --force origin main",
        "git push -f",
        "git push origin +main",
        "git -C repo push --force-with-lease",
    ],
)
def test_denies_destructive_commands(command: str) -> None:
    result = _run(_shell(command))
    assert result["decision"] == "deny", command
    assert result["reason"]


@pytest.mark.parametrize(
    "command",
    ["rm -rf build/", "rm -f /tmp/x.log", "git push origin feature", "pytest -q", "ls -la /"],
)
def test_allows_ordinary_commands(command: str) -> None:
    assert _run(_shell(command))["decision"] == "allow", command


def test_writes_outside_declared_paths_are_denied() -> None:
    inside = {"name": "write_file", "input": {"file_path": "src/app.py", "content": "x"}}
    outside = {"name": "write_file", "input": {"file_path": "/etc/passwd", "content": "x"}}
    escape = {"name": "edit_file", "input": {"file_path": "../../outside.txt"}}
    assert _run(inside)["decision"] == "allow"
    assert _run(outside)["decision"] == "deny"
    assert _run(escape)["decision"] == "deny"
    declared = {"details": {"declared_paths": ["/work/reports"]}}
    assert _run(inside, **declared)["decision"] == "deny"
    report = {"name": "Write", "input": {"file_path": "/work/reports/r.md"}}
    assert _run(report, **declared)["decision"] == "allow"


def test_unknown_input_fails_closed() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT)], input=b"[1,2]", capture_output=True, check=True, timeout=30
    )
    assert json.loads(completed.stdout)["decision"] == "deny"
    assert _run(None)["decision"] == "allow"
