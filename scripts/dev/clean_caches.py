"""Remove tool caches and coverage output. Never touches .venv, .env or Docker volumes.

Usage::

    uv run python scripts/dev/clean_caches.py [--dry-run]
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOP_LEVEL = (".pytest_cache", ".ruff_cache", ".mypy_cache", ".hypothesis", "htmlcov")
FILES = (".coverage", "coverage.xml")
PYCACHE_ROOTS = ("src", "tests", "packages", "integrations", "scripts", "docs", "agent_server")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="print what would be removed")
    args = parser.parse_args()

    targets: list[Path] = [ROOT / name for name in TOP_LEVEL if (ROOT / name).exists()]
    targets += [ROOT / name for name in FILES if (ROOT / name).exists()]
    targets += [path for path in ROOT.glob(".coverage.*") if path.is_file()]
    for base in PYCACHE_ROOTS:
        root = ROOT / base
        if root.is_dir():
            targets += [path for path in root.rglob("__pycache__") if path.is_dir()]

    for path in targets:
        relative = path.relative_to(ROOT)
        if args.dry_run:
            print(f"would remove {relative}")
            continue
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)
        print(f"removed {relative}")
    if not targets:
        print("nothing to clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
