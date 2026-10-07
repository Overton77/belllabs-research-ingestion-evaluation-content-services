"""Compare variable *names* in .env against .env.example. Values are never read or printed.

Informational by default: many example names have defaults in Settings, so a missing
name is reported but exits 0. ``--strict`` exits 1 on any missing name. A missing .env
always exits 1.

Usage::

    uv run python scripts/dev/env_check.py [--strict] [--example .env.example] [--env .env]
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NAME = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def names(path: Path) -> set[str]:
    found: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = NAME.match(line)
        if match:
            found.add(match.group(1))
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--example", default=".env.example")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--strict", action="store_true", help="exit 1 when names are missing")
    args = parser.parse_args()

    example = ROOT / args.example
    env = ROOT / args.env
    if not example.exists():
        print(f"error: {args.example} not found")
        return 1
    if not env.exists():
        print(f"error: {args.env} not found; copy the missing names from {args.example}")
        return 1

    expected = names(example)
    present = names(env)
    missing = sorted(expected - present)
    extra = sorted(present - expected)
    print(f"{args.env}: {len(present)} names; {args.example}: {len(expected)} names")
    if missing:
        print(f"missing from {args.env} ({len(missing)}):")
        for name in missing:
            print(f"  {name}")
    if extra:
        print(f"only in {args.env} ({len(extra)}; fine, listed for awareness):")
        for name in extra:
            print(f"  {name}")
    if not missing:
        print("ok: every example name is present")
    return 1 if (missing and args.strict) else 0


if __name__ == "__main__":
    raise SystemExit(main())
