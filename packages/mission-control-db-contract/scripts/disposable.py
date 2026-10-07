"""Start/stop the two disposable PostgreSQL 17 + pgvector containers used by tests.

Only the exact container names below are ever created or removed. Ports are bound to
loopback. The password is a fixed, obviously disposable value; these clusters hold
generated fixtures only and are removed with ``stop`` (``docker rm -f -v <name>``).

Usage::

    python scripts/disposable.py start   # prints the DSN environment assignments
    python scripts/disposable.py env     # prints them again (PowerShell syntax)
    python scripts/disposable.py stop    # removes both containers and their volumes
"""

from __future__ import annotations

import subprocess
import sys
import time

IMAGE = "pgvector/pgvector:pg17"
PASSWORD = "mcdb-disposable-only"
CONTAINERS = {
    # name: (loopback port, admin DSN environment variable)
    "mcdb-a": (55501, "MCDB_TEST_ADMIN_DSN_A"),
    "mcdb-b": (55502, "MCDB_TEST_ADMIN_DSN_B"),
}


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def _dsn(port: int) -> str:
    return f"postgresql://postgres:{PASSWORD}@127.0.0.1:{port}/postgres"


def start() -> None:
    for name, (port, _env) in CONTAINERS.items():
        existing = _docker("ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}")
        if existing.stdout.strip() == name:
            _docker("start", name)
            continue
        _docker(
            "run",
            "-d",
            "--name",
            name,
            "-e",
            f"POSTGRES_PASSWORD={PASSWORD}",
            "-p",
            f"127.0.0.1:{port}:5432",
            IMAGE,
        )
    for name in CONTAINERS:
        deadline = time.monotonic() + 90
        while True:
            ready = _docker(
                "exec", name, "pg_isready", "-U", "postgres", "-h", "127.0.0.1", check=False
            )
            if ready.returncode == 0:
                # The image's init phase restarts the server once; require two successes.
                time.sleep(1)
                if (
                    _docker(
                        "exec", name, "pg_isready", "-U", "postgres", "-h", "127.0.0.1", check=False
                    ).returncode
                    == 0
                ):
                    break
            if time.monotonic() > deadline:
                raise SystemExit(f"{name} did not become ready")
            time.sleep(1)
    env()


def env() -> None:
    for _name, (port, variable) in CONTAINERS.items():
        print(f"$env:{variable} = '{_dsn(port)}'")


def stop() -> None:
    for name in CONTAINERS:
        _docker("rm", "-f", "-v", name, check=False)
        print(f"removed {name}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command not in {"start", "stop", "env"}:
        raise SystemExit("usage: disposable.py start|stop|env")
    {"start": start, "stop": stop, "env": env}[command]()
