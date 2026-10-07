"""Wait until Docker Compose services are healthy and one-shot jobs have exited 0.

Long-running services must report ``healthy`` (or ``running`` when they define no
healthcheck). One-shot jobs (schema setup, namespace creation) must have exited with
code 0; a non-zero exit fails immediately and prints that service's logs.

Usage::

    uv run python scripts/dev/wait_for_services.py [--timeout 300] [-f docker-compose.yml]

Stdlib only; works from PowerShell, Git Bash, WSL and the Cursor cloud image.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from typing import Any

RUNNING_SERVICES = ("application-postgres", "redis", "temporal-postgres", "temporal", "temporal-ui")
ONE_SHOT_SERVICES = ("temporal-schema", "temporal-create-namespace")


def _compose(args: list[str], compose_files: list[str]) -> str:
    command = ["docker", "compose"]
    for path in compose_files:
        command += ["-f", path]
    completed = subprocess.run(command + args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or f"docker compose {' '.join(args)} failed")
    return completed.stdout


def _ps(compose_files: list[str]) -> dict[str, dict[str, Any]]:
    raw = _compose(["ps", "--all", "--format", "json"], compose_files).strip()
    if not raw:
        return {}
    # Compose v2 prints either a JSON array or one JSON object per line depending on version.
    rows: list[dict[str, Any]]
    if raw.startswith("["):
        rows = json.loads(raw)
    else:
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    return {row["Service"]: row for row in rows}


def _state(row: dict[str, Any]) -> tuple[str, int | None]:
    health = (row.get("Health") or "").lower()
    state = (row.get("State") or "").lower()
    exit_code = row.get("ExitCode")
    return (health or state), (int(exit_code) if exit_code is not None else None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--timeout", type=int, default=300, help="seconds to wait (default 300)")
    parser.add_argument("-f", "--file", action="append", default=[], help="compose file(s)")
    parser.add_argument(
        "--running",
        nargs="*",
        default=list(RUNNING_SERVICES),
        help="services that must be healthy/running",
    )
    parser.add_argument(
        "--one-shot",
        nargs="*",
        default=list(ONE_SHOT_SERVICES),
        help="services that must have exited 0",
    )
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    last_report = ""
    while True:
        try:
            rows = _ps(args.file)
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        pending: list[str] = []
        for service in args.running:
            row = rows.get(service)
            if row is None:
                pending.append(f"{service} (not created)")
                continue
            state, _ = _state(row)
            if state not in {"healthy", "running"}:
                pending.append(f"{service} ({state or 'unknown'})")
        for service in args.one_shot:
            row = rows.get(service)
            if row is None:
                pending.append(f"{service} (not created)")
                continue
            state, exit_code = _state(row)
            if state == "exited":
                if exit_code not in (0, None):
                    print(f"error: {service} exited with code {exit_code}", file=sys.stderr)
                    print(_compose(["logs", service], args.file), file=sys.stderr)
                    return 1
            else:
                pending.append(f"{service} ({state or 'unknown'})")
        if not pending:
            print(_compose(["ps", "--all"], args.file), end="")
            print("all services ready")
            return 0
        report = ", ".join(pending)
        if report != last_report:
            print(f"waiting for: {report}")
            last_report = report
        if time.monotonic() >= deadline:
            print(f"error: services not ready after {args.timeout}s: {report}", file=sys.stderr)
            print(_compose(["ps", "--all"], args.file), file=sys.stderr)
            return 1
        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
