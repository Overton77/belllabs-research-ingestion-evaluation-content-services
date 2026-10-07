#!/usr/bin/env python3
"""Validate the Mission Control OKF bundle (docs/knowledge) with repo-aware link rules.

Same structural rules as the Biotech catalog validator (every concept has YAML
frontmatter with a non-empty `type`; `index.md` and `log.md` are reserved navigation
files), with one difference: internal links may leave the bundle as long as they
resolve inside this repository or the sibling spec pack. Concepts cite the glossary,
ADRs and the normative spec by relative link on purpose.

Usage:
  python docs/tools/validate_okf.py            # validates docs/knowledge
  python docs/tools/validate_okf.py <bundle>   # another bundle under this repo
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWED_ROOTS = [REPO_ROOT.resolve(), (REPO_ROOT.parent / "mission-control-general").resolve()]
RESERVED_NAMES = {"index.md", "log.md"}
LINK_PATTERN = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+\.md)(?:#[^)]+)?\)")
TYPE_PATTERN = re.compile(r"(?m)^type:\s*(.+?)\s*$")
REQUIRED_KEYS = ("type", "title", "description")


def frontmatter(text: str) -> str | None:
    text = text.replace("\r\n", "\n")
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---\n", 4)
    return None if end == -1 else text[4:end]


def validate_links(text: str, source: Path, bundle: Path) -> list[str]:
    errors: list[str] = []
    for target in LINK_PATTERN.findall(text):
        if "://" in target:
            continue
        destination = (
            bundle / target.lstrip("/") if target.startswith("/") else source.parent / target
        )
        resolved = destination.resolve()
        if not any(resolved.is_relative_to(root) for root in ALLOWED_ROOTS):
            errors.append(f"internal link escapes the repository: {target}")
        elif not destination.is_file():
            errors.append(f"broken internal link: {target}")
    return errors


def validate_concept(path: Path, bundle: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    fm = frontmatter(text)
    if fm is None:
        return ["missing YAML frontmatter"]
    errors: list[str] = []
    for key in REQUIRED_KEYS:
        match = re.search(rf"(?m)^{key}:\s*(.+?)\s*$", fm)
        if not match or not match.group(1).strip(" '\""):
            errors.append(f"frontmatter must contain a non-empty {key} field")
    if len(text.replace("\r\n", "\n").splitlines()) > 160:
        errors.append("concept exceeds 160 lines; split it or push detail behind a pointer")
    if "# Citations" not in text and "## Citations" not in text:
        errors.append("concept has no Citations heading")
    errors.extend(validate_links(text.replace("\r\n", "\n"), path, bundle))
    return errors


def validate_bundle(bundle: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(bundle.rglob("*.md")):
        rel = path.relative_to(bundle)
        if path.name in RESERVED_NAMES:
            errors.extend(
                f"{rel}: {e}"
                for e in validate_links(
                    path.read_text(encoding="utf-8").replace("\r\n", "\n"), path, bundle
                )
            )
            continue
        errors.extend(f"{rel}: {e}" for e in validate_concept(path, bundle))
    index = bundle / "index.md"
    if index.is_file():
        index_text = index.read_text(encoding="utf-8")
        for path in sorted(bundle.glob("*.md")):
            if path.name not in RESERVED_NAMES and path.name not in index_text:
                errors.append(f"index.md: concept {path.name} is not linked from the index")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("bundle", nargs="?", type=Path, default=REPO_ROOT / "docs" / "knowledge")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    bundle = args.bundle.resolve()
    if not bundle.is_dir():
        parser.error(f"bundle directory does not exist: {bundle}")
    errors = validate_bundle(bundle)
    if errors:
        print("OKF validation failed:", file=sys.stderr)
        for e in errors:
            print(f"- {e}", file=sys.stderr)
        return 1
    count = sum(1 for p in bundle.rglob("*.md") if p.name not in RESERVED_NAMES)
    print(f"OKF structure valid: {count} concept document(s) in {bundle}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
