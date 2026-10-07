#!/usr/bin/env python3
"""Give every document in the Mission Control corpus Open Knowledge Format frontmatter.

Adds `type`, `title`, `description` and `tags` where they are missing, keeping any
existing frontmatter keys (ADRs keep `status` and `source`). Title comes from the
first H1, description from the first paragraph. Content below the frontmatter is
never changed.

Usage:
  python docs/tools/okf_frontmatter.py            # dry run: list files that would change
  python docs/tools/okf_frontmatter.py --apply    # write
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PACK = REPO_ROOT.parent / "mission-control-general"

# (root, type, extra tags)
TARGETS: list[tuple[Path, str, list[str]]] = [
    (REPO_ROOT / "GLOSSARY.md", "Glossary", ["language"]),
    (REPO_ROOT / "docs" / "adr", "Decision Record", ["adr", "decision"]),
    (REPO_ROOT / "docs" / "agents", "Agent Configuration", ["agents", "process"]),
    (
        REPO_ROOT / "docs" / "MISSION_CONTROL_IMPLEMENTATION_STATUS.md",
        "Implementation Evidence",
        ["status", "evidence"],
    ),
    (REPO_ROOT / "docs" / "MISSION_CONTROL_LOCAL_API.md", "Operator Guide", ["operations"]),
    (REPO_ROOT / "docs" / "REMOVAL_GUIDE.md", "Removal Guide", ["removal", "evidence"]),
    (
        PACK / "general-mission-control" / "expansion" / "issues",
        "Issue Packet View",
        ["issue", "backlog", "generated"],
    ),
    (PACK / "general-mission-control" / "expansion", "Specification Annex", ["spec", "expansion"]),
    (PACK / "general-mission-control", "Specification", ["spec", "normative"]),
    (PACK / "workflow-types", "Workflow Specification", ["spec", "workflow"]),
    (PACK / "runtime-facts", "Runtime Fact Sheet", ["runtime", "facts"]),
]
SKIP_NAMES = {"index.md", "log.md"}
BOILERPLATE = re.compile(
    r"^(Version|Architecture|Status|Category|Normative terms|Read in order|"
    r"Last updated|Workflow-system kind|Owner|Supersedes|Applies to)\b"
)


def split(text: str) -> tuple[list[str] | None, str]:
    text = text.replace("\r\n", "\n")
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            return text[4:end].splitlines(), text[end + 5 :]
    return None, text


def first_heading(body: str) -> str:
    for line in body.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def first_paragraph(body: str) -> str:
    lines = body.splitlines()
    i = 0
    while i < len(lines) and not lines[i].startswith("# "):
        i += 1
    para: list[str] = []
    for line in lines[i + 1 :]:
        s = line.strip()
        if not s:
            if para:
                break
            continue
        if s.startswith(("#", "```", "|", "---")):
            if para:
                break
            continue
        if not para and BOILERPLATE.match(
            re.sub(r"^[-*_\s>]+", "", s).replace("*", "").replace("_", " ")
        ):
            continue
        para.append(s)
    text = " ".join(para)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[`*]", "", text)
    text = re.sub(r"(?<![A-Za-z0-9])_([^_]+)_(?![A-Za-z0-9])", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 220:
        cut = text[:220]
        text = cut[: cut.rfind(" ")].rstrip(",;:") + "…"
    return text


def yaml_str(value: str) -> str:
    if re.search(r'[:#\[\]{}"\'|>&*!%@`,]', value) or value != value.strip():
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value


def slug_tag(path: Path) -> str:
    return re.sub(r"[^a-z0-9]+", "-", path.stem.lower()).strip("-")


def build(
    path: Path, doc_type: str, tags: list[str], text: str, refresh: bool = False
) -> str | None:
    existing, body = split(text)
    keys = {}
    if existing:
        for line in existing:
            if ":" in line and not line.startswith(" "):
                k, _, v = line.partition(":")
                keys[k.strip()] = v.strip()
    if refresh and existing:
        # Recompute the generated description; keep everything else.
        description = first_paragraph(body) or keys.get("title", "").strip("'\"") or path.stem
        new_desc = f"description: {yaml_str(description)}"
        replaced = [new_desc if line.startswith("description:") else line for line in existing]
        if "description:" not in "\n".join(existing):
            replaced.insert(min(2, len(replaced)), new_desc)
        rebuilt = "---\n" + "\n".join(replaced) + "\n---\n" + body
        return None if rebuilt == text.replace("\r\n", "\n") else rebuilt
    if "type" in keys and "title" in keys and "description" in keys and "tags" in keys:
        return None
    title = (
        keys.get("title", "").strip("'\"")
        or first_heading(body)
        or path.stem.replace("_", " ").replace("-", " ")
    )
    description = keys.get("description", "").strip("'\"") or first_paragraph(body) or title
    tag_list = ["mission-control", *tags]
    if path.name.startswith("MC-"):
        tag_list.append(path.stem.lower())
    new_lines = []
    if "type" not in keys:
        new_lines.append(f"type: {yaml_str(doc_type)}")
    if "title" not in keys:
        new_lines.append(f"title: {yaml_str(title)}")
    if "description" not in keys:
        new_lines.append(f"description: {yaml_str(description)}")
    if "tags" not in keys:
        new_lines.append("tags: [" + ", ".join(tag_list) + "]")
    merged = new_lines + (existing or [])
    return "---\n" + "\n".join(merged) + "\n---\n" + body


def iter_targets():
    seen: set[Path] = set()
    for root, doc_type, tags in TARGETS:
        if not root.exists():
            continue
        files = [root] if root.is_file() else sorted(root.glob("*.md"))
        for f in files:
            if f.name in SKIP_NAMES or f.resolve() in seen:
                continue
            seen.add(f.resolve())
            yield f, doc_type, tags


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--refresh", action="store_true", help="recompute generated descriptions on every target"
    )
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    changed = 0
    for path, doc_type, tags in iter_targets():
        text = path.read_text(encoding="utf-8", errors="replace")
        updated = build(path, doc_type, tags, text, refresh=args.refresh)
        if updated is None:
            continue
        changed += 1
        try:
            rel = path.resolve().relative_to(REPO_ROOT.parent.resolve())
        except ValueError:
            rel = path
        if args.apply:
            path.write_text(updated, encoding="utf-8", newline="\n")
            print(f"updated  {rel}")
        else:
            print(f"would update  {rel}")
    print(f"{changed} file(s) {'updated' if args.apply else 'would change'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
