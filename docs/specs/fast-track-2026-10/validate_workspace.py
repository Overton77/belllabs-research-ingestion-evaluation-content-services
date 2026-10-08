#!/usr/bin/env python3
"""Validate the fast-track packet: tickets, blockers, waves, specs and links.

Checks that every ticket in 00-ARCHITECTURE.md section 9 has exactly one draft under
issues/, that each draft's ``**Blocked by:**`` line matches the architecture table, that
the blocker graph is a DAG, that every SPEC-0x file named in README.md exists, and that
relative Markdown links inside this directory resolve. Prints the dispatch waves.

Usage:
  python docs/specs/fast-track-2026-10/validate_workspace.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "00-ARCHITECTURE.md"
ISSUES = HERE / "issues"
LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)#]+\.(?:md|yml|py))(?:#[^)]*)?\)")


def architecture_tickets() -> dict[str, tuple[str, set[str]]]:
    text = ARCH.read_text(encoding="utf-8")
    section = text.split("## 9. Ticket plan", 1)[1].split("\n## ", 1)[0]
    tickets: dict[str, tuple[str, set[str]]] = {}
    for line in section.splitlines():
        m = re.match(r"\| ([A-I]\d) \| [^|]+ \| ([^|]+) \| ([^|]+) \|", line)
        if not m:
            continue
        tid, title, blocked = m.group(1), m.group(2).strip(), m.group(3).strip()
        blockers = set() if blocked in {"—", "-", ""} else {b.strip() for b in blocked.split(",")}
        tickets[tid] = (title, blockers)
    return tickets


def draft_tickets() -> dict[str, tuple[Path, str, set[str]]]:
    drafts: dict[str, tuple[Path, str, set[str]]] = {}
    for path in sorted(ISSUES.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        m = re.search(r"^# \[FT-([A-I]\d)\] (.+)$", text, re.MULTILINE)
        if not m:
            print(f"ERROR {path.name}: missing '# [FT-ID] Title'")
            continue
        blocked = re.search(r"^\*\*Blocked by:\*\*\s*(.+)$", text, re.MULTILINE)
        blockers: set[str] = set()
        if blocked and "None" not in blocked.group(1):
            blockers = set(re.findall(r"FT-([A-I]\d)", blocked.group(1)))
        if m.group(1) in drafts:
            print(f"ERROR duplicate draft for {m.group(1)}: {path.name}")
        drafts[m.group(1)] = (path, m.group(2).strip(), blockers)
    return drafts


def waves(graph: dict[str, set[str]]) -> list[list[str]]:
    remaining = dict(graph)
    done: set[str] = set()
    out: list[list[str]] = []
    while remaining:
        ready = sorted(t for t, b in remaining.items() if b <= done)
        if not ready:
            raise SystemExit(f"cycle among {sorted(remaining)}")
        out.append(ready)
        done |= set(ready)
        for t in ready:
            remaining.pop(t)
    return out


def main() -> int:
    errors = 0
    arch = architecture_tickets()
    drafts = draft_tickets()
    for tid, (title, blockers) in arch.items():
        if tid not in drafts:
            print(f"ERROR no draft for {tid} ({title})")
            errors += 1
            continue
        _, dtitle, dblockers = drafts[tid]
        if dblockers != blockers:
            print(f"ERROR {tid} blockers {sorted(dblockers)} != architecture {sorted(blockers)}")
            errors += 1
        if dtitle != title:
            print(f"WARN  {tid} title differs: draft '{dtitle}' vs architecture '{title}'")
    for tid in drafts:
        if tid not in arch:
            print(f"ERROR draft {tid} not in architecture ticket plan")
            errors += 1
    for name in re.findall(
        r"\]\((SPEC-\d\d-[^)]+\.md)\)", (HERE / "README.md").read_text(encoding="utf-8")
    ):
        if not (HERE / name).exists():
            print(f"ERROR missing spec {name}")
            errors += 1
    for path in (
        list(HERE.glob("*.md")) + list(ISSUES.glob("*.md")) + list((HERE / "research").glob("*.md"))
    ):
        text = path.read_text(encoding="utf-8")
        for target in LINK.findall(text):
            if target.startswith(("http://", "https://")):
                continue
            if not (path.parent / target).resolve().exists():
                print(f"ERROR broken link in {path.relative_to(HERE)}: {target}")
                errors += 1
    if errors == 0:
        graph = {t: b for t, (_, b) in arch.items()}
        for i, wave in enumerate(waves(graph)):
            print(f"wave {i}: {', '.join(wave)}")
        print(f"OK: {len(arch)} tickets, {len(drafts)} drafts, DAG valid")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
