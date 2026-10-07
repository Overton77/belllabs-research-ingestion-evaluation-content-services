"""Render `make help` from `##` comments in the Makefile (stdlib only, cross-platform).

Conventions:

* ``target: deps ## description`` documents a target.
* ``##@ Section`` starts a new section heading.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

TARGET = re.compile(r"^([a-zA-Z0-9_./-]+)\s*:[^#=]*##\s*(.*)$")
SECTION = re.compile(r"^##@\s*(.*)$")


def main(paths: list[str]) -> int:
    rows: list[tuple[str, str]] = []
    width = 0
    for raw in paths:
        for line in Path(raw).read_text(encoding="utf-8").splitlines():
            section = SECTION.match(line)
            if section:
                rows.append(("", section.group(1)))
                continue
            target = TARGET.match(line)
            if target:
                name, description = target.groups()
                rows.append((name, description))
                width = max(width, len(name))
    print("Usage: make <target> [VAR=value ...]")
    for name, description in rows:
        if not name:
            print(f"\n{description}")
            continue
        print(f"  {name.ljust(width)}  {description}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:] or ["Makefile"]))
