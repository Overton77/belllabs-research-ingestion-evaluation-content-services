"""Validate relative Markdown links in Mission Control documentation (stdlib only).

Scope: every ``*.md`` under ``docs/``, the root ``README.md`` and every ``AGENTS.md``
in the repository (excluding caches, virtual environments, ``.scratch``, ``.git``,
``node_modules``, ignored ``app/personal_code`` and unreadable temp trees).

Checked: inline links/images ``[text](target)`` and reference definitions
``[id]: target`` whose target is relative (no scheme, not ``mailto:``, not a bare
``#anchor``). A target must resolve to an existing file or directory. ``#fragment``
anchors on Markdown targets are checked against GitHub-style heading slugs and
explicit ``<a id/name>`` anchors. Fenced code blocks and inline code spans are ignored.

Usage::

    python docs/tools/check_links.py            # human report, exit 1 on broken links
    python docs/tools/check_links.py --json     # machine-readable report
    python docs/tools/check_links.py --no-anchors

It never follows links outside the filesystem and never modifies files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".scratch",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".uv-cache",
    ".tmp",
    ".tmp-cleanup-unit",
    ".tmp-worker-poll-20261003",
    "pytest-of-Pinda",
    "personal_code",
    ".claude",
}
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
INLINE = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*(<[^>]*>|[^)\s]+)(?:\s+[\"'(][^)]*)?\)")
REFERENCE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*(<[^>]*>|\S+)", re.MULTILINE)
FENCE = re.compile(r"^\s{0,3}(```|~~~)")
CODE_SPAN = re.compile(r"(`+)(?:(?!\1).)+?\1")
HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$")
HTML_ANCHOR = re.compile(r"<a\s+[^>]*(?:id|name)\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)


@dataclass(frozen=True)
class Broken:
    source: str
    line: int
    target: str
    reason: str


def _walk_markdown(base: Path) -> list[Path]:
    found: list[Path] = []
    for directory, dirnames, filenames in os.walk(base, onerror=lambda _error: None):
        dirnames[:] = sorted(d for d in dirnames if d not in EXCLUDED_DIRS)
        for name in sorted(filenames):
            if name.endswith(".md"):
                found.append(Path(directory) / name)
    return found


def markdown_scope(root: Path = ROOT) -> list[Path]:
    """docs/**/*.md, README.md and every AGENTS.md (deduplicated, sorted)."""
    files: set[Path] = set()
    docs = root / "docs"
    if docs.is_dir():
        files.update(_walk_markdown(docs))
    readme = root / "README.md"
    if readme.is_file():
        files.add(readme)
    for path in _walk_markdown(root):
        if path.name == "AGENTS.md":
            files.add(path)
    return sorted(files)


def _strip_code(lines: list[str]) -> list[str]:
    """Blank fenced blocks and inline code spans while preserving line numbers."""
    result: list[str] = []
    fence: str | None = None
    for line in lines:
        match = FENCE.match(line)
        if fence is None and match:
            fence = match.group(1)
            result.append("")
            continue
        if fence is not None:
            if line.lstrip().startswith(fence):
                fence = None
            result.append("")
            continue
        result.append(CODE_SPAN.sub("", line))
    return result


def slugify(heading: str) -> str:
    text = re.sub(r"<[^>]+>", "", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = text.replace("`", "").strip().lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return text.replace(" ", "-")


_ANCHOR_CACHE: dict[Path, set[str]] = {}


def anchors(path: Path) -> set[str]:
    if path in _ANCHOR_CACHE:
        return _ANCHOR_CACHE[path]
    seen: dict[str, int] = {}
    result: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        lines = []
    for line in _strip_code(lines):
        match = HEADING.match(line)
        if match:
            slug = slugify(match.group(1))
            count = seen.get(slug, 0)
            seen[slug] = count + 1
            result.add(slug if count == 0 else f"{slug}-{count}")
    for line in lines:
        result.update(HTML_ANCHOR.findall(line))
    _ANCHOR_CACHE[path] = result
    return result


def _targets(lines: list[str]) -> list[tuple[int, str]]:
    found: list[tuple[int, str]] = []
    for number, line in enumerate(lines, start=1):
        for match in INLINE.finditer(line):
            found.append((number, match.group(1)))
    text = "\n".join(lines)
    for match in REFERENCE.finditer(text):
        found.append((text.count("\n", 0, match.start()) + 1, match.group(1)))
    return found


def check_file(path: Path, *, check_anchors: bool = True, root: Path = ROOT) -> list[Broken]:
    broken: list[Broken] = []
    try:
        raw = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return [Broken(_rel(path, root), 0, "", f"unreadable: {exc.__class__.__name__}")]
    for number, target in _targets(_strip_code(raw)):
        target = target.strip("<>").strip()
        if not target or SCHEME.match(target) or target.startswith("//"):
            continue
        location, _hash, fragment = target.partition("#")
        location = unquote(location)
        if not location:
            if check_anchors and fragment and fragment.lower() not in anchors(path):
                broken.append(Broken(_rel(path, root), number, target, "missing local anchor"))
            continue
        resolved = (
            (path.parent / location).resolve()
            if not location.startswith("/")
            else (root / location.lstrip("/")).resolve()
        )
        if not resolved.exists():
            broken.append(Broken(_rel(path, root), number, target, "missing target"))
            continue
        if (
            check_anchors
            and fragment
            and resolved.is_file()
            and resolved.suffix.lower() == ".md"
            and unquote(fragment).lower() not in anchors(resolved)
        ):
            broken.append(Broken(_rel(path, root), number, target, "missing anchor"))
    return broken


def _rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def run(*, check_anchors: bool = True, root: Path = ROOT) -> dict[str, object]:
    files = markdown_scope(root)
    broken: list[Broken] = []
    for path in files:
        broken.extend(check_file(path, check_anchors=check_anchors, root=root))
    return {
        "root": root.as_posix(),
        "files_checked": len(files),
        "broken_count": len(broken),
        "broken": [asdict(item) for item in broken],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    parser.add_argument("--no-anchors", action="store_true", help="skip #fragment checks")
    args = parser.parse_args(argv)
    report = run(check_anchors=not args.no_anchors)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        broken: list[dict[str, object]] = report["broken"]  # type: ignore[assignment]
        for item in broken:
            print(f"{item['source']}:{item['line']}: {item['target']} ({item['reason']})")
        print(f"checked {report['files_checked']} files; broken links: {report['broken_count']}")
    return 1 if report["broken_count"] else 0


if __name__ == "__main__":
    sys.exit(main())
