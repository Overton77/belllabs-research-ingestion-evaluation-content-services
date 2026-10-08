#!/usr/bin/env python3
"""Generate the compressed documentation index embedded in AGENTS.md.

The index follows the Vercel pattern (a pipe-delimited map from directories to the
files they hold) so an agent always knows where Mission Control documents are without
loading them. One line per directory; `name=short title` per file, numeric ADR slugs
kept as-is because the slug already carries the title.

Usage:
  python docs/tools/agents_docs_index.py            # rewrite the block in AGENTS.md
  python docs/tools/agents_docs_index.py --check    # exit 1 if AGENTS.md is stale
  python docs/tools/agents_docs_index.py --print    # print the block only
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS_MD = REPO_ROOT / "AGENTS.md"
BEGIN = "<!-- BEGIN:mc-docs-index -->"
END = "<!-- END:mc-docs-index -->"
TITLE_MAX = 44

# (directory, label for the index line, include titles?)
GROUPS: list[tuple[Path, str, bool]] = [
    (REPO_ROOT / "docs" / "adr", "docs/adr", False),
    (REPO_ROOT / "docs" / "knowledge", "docs/knowledge", True),
    (REPO_ROOT / "docs" / "agents", "docs/agents", True),
    (REPO_ROOT / "docs" / "research", "docs/research", True),
    (REPO_ROOT / "docs" / "specs" / "fast-track-2026-10", "docs/specs/fast-track-2026-10", True),
    (
        REPO_ROOT / "docs" / "specs" / "fast-track-2026-10" / "research",
        "docs/specs/fast-track-2026-10/research",
        True,
    ),
    (REPO_ROOT / "docs", "docs", True),
    (
        REPO_ROOT.parent / "mission-control-general" / "general-mission-control",
        "../mission-control-general/general-mission-control",
        True,
    ),
    (
        REPO_ROOT.parent / "mission-control-general" / "general-mission-control" / "expansion",
        "../mission-control-general/general-mission-control/expansion",
        True,
    ),
    (
        REPO_ROOT.parent / "mission-control-general" / "workflow-types",
        "../mission-control-general/workflow-types",
        True,
    ),
    (
        REPO_ROOT.parent / "mission-control-general" / "runtime-facts",
        "../mission-control-general/runtime-facts",
        True,
    ),
]

HEADER = [
    "[Mission Control Docs Index]|root: this directory; ../ is the sibling spec pack (normative)",
    "|IMPORTANT: Prefer retrieval-led reasoning over pre-training-led reasoning "
    "for Mission Control concepts, contracts and decisions; open the file before asserting",
    '|search: python docs/tools/okf_search.py "<terms>" '
    "(ranks glossary terms, ADRs, concepts, spec)",
    "|precedence on conflict: spec pack > docs/adr > docs/knowledge > "
    "docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md; report the conflict",
    "|GLOSSARY.md:{shared language; use its terms and honour its Avoid lists}",
]


def frontmatter_title(text: str) -> str:
    text = text.replace("\r\n", "\n")
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            for line in text[4:end].splitlines():
                if line.startswith("title:"):
                    return line.split(":", 1)[1].strip().strip("'\"")
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def short(title: str) -> str:
    title = re.sub(r"\s+", " ", title).strip()
    title = re.sub(r"\s*[|{}=,]\s*", " ", title).strip()
    if len(title) > TITLE_MAX:
        title = title[: TITLE_MAX - 1].rstrip() + "…"
    return title


def group_line(directory: Path, label: str, with_titles: bool) -> str | None:
    if not directory.is_dir():
        return None
    entries = []
    for file in sorted(directory.glob("*.md")):
        name = file.stem
        if with_titles:
            title = short(frontmatter_title(file.read_text(encoding="utf-8", errors="replace")))
            entries.append(f"{name}={title}" if title and title.lower() != name.lower() else name)
        else:
            entries.append(name)
    if not entries:
        return None
    return f"|{label}:{{{','.join(entries)}}}"


def issues_line() -> str | None:
    issues = (
        REPO_ROOT.parent
        / "mission-control-general"
        / "general-mission-control"
        / "expansion"
        / "issues"
    )
    if not issues.is_dir():
        return None
    ids = sorted(p.stem for p in issues.glob("MC-*.md"))
    if not ids:
        return None
    by_prefix: dict[str, list[str]] = {}
    for i in ids:
        by_prefix.setdefault(i[:4], []).append(i[4:])
    ranges = ",".join(f"{k}{v[0]}..{v[-1]}" for k, v in by_prefix.items())
    note = "generated views of issues.json; statuses stale since 2026-10-03; not the tracker"
    prefix = "../mission-control-general/general-mission-control/expansion/issues"
    return f"|{prefix}:{{{ranges} ({note})}}"


def subsystem_line() -> str | None:
    skip = {REPO_ROOT / "AGENTS.md", REPO_ROOT / "docs" / "AGENTS.md"}
    found = []
    for path in sorted(REPO_ROOT.rglob("AGENTS.md")):
        if path in skip or any(
            part.startswith(".") or part in {"node_modules", "app"}
            for part in path.relative_to(REPO_ROOT).parts
        ):
            continue
        found.append(str(path.parent.relative_to(REPO_ROOT)).replace("\\", "/"))
    return (
        f"|subsystem AGENTS.md (read the closest one before editing there):{{{','.join(found)}}}"
        if found
        else None
    )


def build_block() -> str:
    lines = list(HEADER)
    sub = subsystem_line()
    if sub:
        lines.append(sub)
    for directory, label, with_titles in GROUPS:
        line = group_line(directory, label, with_titles)
        if line:
            lines.append(line)
    issues = issues_line()
    if issues:
        lines.append(issues)
    body = "\n".join(lines)
    return f"{BEGIN}\n```text\n{body}\n```\n{END}"


def splice(agents_text: str, block: str) -> str:
    if BEGIN in agents_text and END in agents_text:
        start = agents_text.index(BEGIN)
        stop = agents_text.index(END) + len(END)
        return agents_text[:start] + block + agents_text[stop:]
    section = (
        "\n\n## Documentation index\n\n"
        "Generated by `python docs/tools/agents_docs_index.py`; do not edit by hand.\n\n"
    )
    return agents_text.rstrip("\n") + section + block + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--print", dest="print_only", action="store_true")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    block = build_block()
    if args.print_only:
        print(block)
        print(f"\n[{len(block.encode('utf-8'))} bytes]", file=sys.stderr)
        return 0

    current = AGENTS_MD.read_text(encoding="utf-8").replace("\r\n", "\n")
    updated = splice(current, block)
    if args.check:
        if updated != current:
            print(
                "AGENTS.md docs index is stale; run python docs/tools/agents_docs_index.py",
                file=sys.stderr,
            )
            return 1
        print("AGENTS.md docs index is current")
        return 0
    AGENTS_MD.write_text(updated, encoding="utf-8", newline="\n")
    print(f"AGENTS.md index updated ({len(block.encode('utf-8'))} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
