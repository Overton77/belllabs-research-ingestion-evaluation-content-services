# ruff: noqa: T201 - operator-facing CLI; printing the summary is the purpose.
"""Snapshot the Mission Control local setup (stdlib only; never reads secret values).

Writes ``ledger/snapshots/<UTC timestamp>.json`` next to this skill and prints a Markdown
summary. Every probe is independent and failure-tolerant, so a missing tool or a stopped
Docker daemon shows up as a finding rather than a crash.

Usage::

    python.agents / skills / mission - control - local - setup / scripts / audit_local_setup.py
    (
        python.agents / skills / mission
        - control
        - local
        - setup / scripts / audit_local_setup.py
        - -no
        - write
    )
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SKILL = Path(__file__).resolve().parents[1]
ROOT = SKILL.parents[2]  # .agents/skills/<skill> -> repo root
LEDGER = SKILL / "ledger"
CACHES = (".mypy_cache", ".ruff_cache", ".pytest_cache", ".hypothesis", "htmlcov")
STALE = (".venv-wsl", ".uv-cache", "graphify-out", "sandbox-work")
ENV_NAME = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def run(args: list[str], timeout: int = 60) -> tuple[int, str]:
    try:
        completed = subprocess.run(
            args, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, f"{type(exc).__name__}: {exc}"
    return completed.returncode, (completed.stdout + completed.stderr).strip()


def version(args: list[str], *, first_line: bool = False) -> str:
    code, out = run(args)
    if code != 0 or not out:
        return f"unavailable ({out[:80]})"
    lines = out.splitlines()
    return lines[0] if first_line else lines[-1]


def dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def mib(size: int) -> str:
    return f"{size / (1024 * 1024):.1f} MiB"


def env_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    names: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = ENV_NAME.match(line)
        if match:
            names.add(match.group(1))
    return names


def probe_tools() -> dict[str, str]:
    uv = ["uv", "run", "--no-sync"]
    return {
        "uv": version(["uv", "--version"]),
        "python": version([*uv, "python", "--version"]),
        "ruff": version([*uv, "ruff", "--version"]),
        "mypy": version([*uv, "mypy", "--version"]),
        "ty": version([*uv, "ty", "--version"]),
        "pytest": version([*uv, "pytest", "--version"]),
        "prek": version([*uv, "prek", "--version"]),
        "make": version(["make", "--version"], first_line=True),
        "docker": version(["docker", "--version"]),
        "compose": version(["docker", "compose", "version"]),
    }


def probe_project() -> dict[str, Any]:
    lock = ROOT / "uv.lock"
    revision = None
    if lock.exists():
        for line in lock.read_text(encoding="utf-8").splitlines()[:5]:
            if line.startswith("revision"):
                revision = int(line.split("=")[1].strip())
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    section = pyproject.split("[dependency-groups]", 1)[1].split("\n[", 1)[0]
    groups = re.findall(r"^([a-z]+) = \[", section, re.MULTILINE)
    python_version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    hook = ROOT / ".git" / "hooks" / "pre-commit"
    hook_kind = None
    if hook.exists():
        text = hook.read_text(encoding="utf-8", errors="replace")
        hook_kind = "prek" if "prek" in text else "other"
    return {
        "lock_revision": revision,
        "dependency_groups": groups,
        "python_version_file": python_version,
        "pre_commit_hook": hook_kind,
        "tool_sections": sorted(set(re.findall(r"^\[tool\.([a-z]+)", pyproject, re.MULTILINE))),
    }


def probe_git() -> dict[str, Any]:
    code, out = run(["git", "status", "--short"])
    if code != 0:
        return {"error": out[:200]}
    counts: dict[str, int] = {}
    for line in out.splitlines():
        counts[line[:2].strip() or "??"] = counts.get(line[:2].strip() or "??", 0) + 1
    _, branch = run(["git", "branch", "--show-current"])
    _, head = run(["git", "rev-parse", "--short", "HEAD"])
    return {"branch": branch, "head": head, "status_counts": counts}


def probe_compose() -> dict[str, Any]:
    code, out = run(["docker", "compose", "ps", "--all", "--format", "json"], timeout=30)
    if code != 0:
        return {"available": False, "reason": out.splitlines()[-1][:160] if out else "no output"}
    rows: list[dict[str, Any]] = []
    raw = out.strip()
    if raw.startswith("["):
        rows = json.loads(raw)
    else:
        rows = [json.loads(line) for line in raw.splitlines() if line.strip().startswith("{")]
    return {
        "available": True,
        "services": {
            row.get("Service"): (row.get("Health") or row.get("State") or "unknown") for row in rows
        },
    }


def probe_filesystem() -> dict[str, Any]:
    return {
        "caches": {name: mib(dir_size(ROOT / name)) for name in CACHES if (ROOT / name).exists()},
        "stale_dirs_present": [name for name in STALE if (ROOT / name).exists()]
        + sorted(p.name for p in ROOT.glob(".tmp*") if p.is_dir()),
        "venv_present": (ROOT / ".venv").exists(),
    }


def probe_env() -> dict[str, Any]:
    example = env_names(ROOT / ".env.example")
    local = env_names(ROOT / ".env")
    return {
        "env_present": (ROOT / ".env").exists(),
        "example_names": len(example),
        "local_names": len(local),
        "missing_count": len(example - local),
        "missing_names": sorted(example - local),
    }


def summary(snapshot: dict[str, Any]) -> str:
    tools = snapshot["tools"]
    project = snapshot["project"]
    git = snapshot["git"]
    compose = snapshot["compose"]
    fs = snapshot["filesystem"]
    env = snapshot["env"]
    lines = [
        f"# Local setup snapshot {snapshot['timestamp']}",
        "",
        "## Tools",
        *(f"- {name}: {value}" for name, value in tools.items()),
        "",
        "## Project",
        f"- uv.lock revision: {project['lock_revision']}",
        f"- dependency groups: {', '.join(project['dependency_groups'])}",
        f"- .python-version: {project['python_version_file']}",
        f"- pre-commit hook: {project['pre_commit_hook'] or 'not installed'}",
        f"- tool sections: {', '.join(project['tool_sections'])}",
        "",
        "## Git worktree",
        f"- branch {git.get('branch') or '(detached)'} @ {git.get('head')}",
        f"- status counts: {git.get('status_counts') or git.get('error')}",
        "",
        "## Compose",
    ]
    if compose["available"]:
        lines += [f"- {svc}: {state}" for svc, state in compose["services"].items()] or [
            "- no containers"
        ]
    else:
        lines.append(f"- unavailable: {compose['reason']}")
    lines += [
        "",
        "## Filesystem",
        f"- caches: {fs['caches'] or 'none'}",
        f"- stale dirs present: {', '.join(fs['stale_dirs_present']) or 'none'}",
        "",
        "## Environment names (values never read)",
        f"- .env present: {env['env_present']}; example {env['example_names']} names, "
        f"local {env['local_names']}, missing {env['missing_count']}",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-write", action="store_true", help="print only")
    args = parser.parse_args()

    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%SZ")
    snapshot = {
        "timestamp": timestamp,
        "root": str(ROOT),
        "tools": probe_tools(),
        "project": probe_project(),
        "git": probe_git(),
        "compose": probe_compose(),
        "filesystem": probe_filesystem(),
        "env": probe_env(),
    }
    print(summary(snapshot))
    if not args.no_write:
        target = LEDGER / "snapshots" / f"{timestamp}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nwrote {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
