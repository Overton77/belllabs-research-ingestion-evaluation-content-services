#!/usr/bin/env python3
"""Search the Mission Control documentation corpus.

Corpus (relative to the mission-control repo root unless overridden with --root):
  GLOSSARY.md (one virtual document per term), docs/adr, docs/knowledge, docs/agents,
  and the sibling spec pack ../mission-control-general (general-mission-control,
  workflow-types, runtime-facts).

Every document is ranked with BM25 over weighted fields: title, tags, description,
headings and body. Dependency-free; works with any Python 3.10+.

Usage:
  python docs/tools/okf_search.py "urgent stop fence"
  python docs/tools/okf_search.py "goal loop" --limit 5 --json
  python docs/tools/okf_search.py "mission-db" --type "Decision Record"
  python docs/tools/okf_search.py --list-types
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOTS = [
    REPO_ROOT / "GLOSSARY.md",
    REPO_ROOT / "docs" / "adr",
    REPO_ROOT / "docs" / "knowledge",
    REPO_ROOT / "docs" / "agents",
    REPO_ROOT.parent / "mission-control-general" / "general-mission-control",
    REPO_ROOT.parent / "mission-control-general" / "workflow-types",
    REPO_ROOT.parent / "mission-control-general" / "runtime-facts",
]

FIELD_WEIGHTS = {"title": 4, "tags": 3, "description": 2, "headings": 2, "body": 1}
TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_]+")
STOP = {
    "the",
    "and",
    "for",
    "that",
    "with",
    "this",
    "from",
    "are",
    "its",
    "not",
    "one",
    "into",
    "each",
    "than",
    "then",
    "when",
    "what",
    "which",
    "where",
    "how",
    "does",
    "can",
    "has",
    "have",
    "was",
    "were",
    "they",
    "their",
    "any",
    "all",
    "but",
    "use",
}


@dataclass
class Doc:
    path: str
    doc_type: str
    title: str
    description: str
    tags: list[str]
    fields: dict[str, list[str]]
    lines: list[str] = field(default_factory=list)


def stem(token: str) -> str:
    for suffix in ("ations", "ation", "ings", "ing", "ies", "ers", "ed", "es", "s"):
        if token.endswith(suffix) and len(token) - len(suffix) >= 3:
            return token[: -len(suffix)]
    return token


def tokenize(text: str) -> list[str]:
    return [stem(t) for t in TOKEN_RE.findall(text.lower()) if t not in STOP and len(t) > 1]


def split_frontmatter(text: str) -> tuple[dict[str, str], str]:
    text = text.replace("\r\n", "\n")
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    meta: dict[str, str] = {}
    for line in text[4:end].splitlines():
        if ":" in line and not line.startswith(" "):
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip().strip("'\"")
    return meta, text[end + 5 :]


def parse_tags(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    return [t.strip().strip("'\"") for t in raw.split(",") if t.strip()]


def first_heading(body: str) -> str:
    for line in body.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def first_paragraph(body: str) -> str:
    paragraph: list[str] = []
    seen_heading = False
    for line in body.splitlines():
        if line.startswith("#"):
            if paragraph:
                break
            seen_heading = True
            continue
        if line.strip() == "":
            if paragraph:
                break
            continue
        if seen_heading or not paragraph:
            paragraph.append(line.strip())
    return " ".join(paragraph)[:240]


def make_doc(path: Path, rel: str, text: str, default_type: str) -> Doc:
    meta, body = split_frontmatter(text)
    title = meta.get("title") or first_heading(body) or path.stem
    description = meta.get("description") or first_paragraph(body)
    tags = parse_tags(meta.get("tags", ""))
    headings = [line.lstrip("#").strip() for line in body.splitlines() if line.startswith("#")]
    return Doc(
        path=rel,
        doc_type=meta.get("type", default_type),
        title=title,
        description=description,
        tags=tags,
        fields={
            "title": tokenize(title),
            "tags": tokenize(" ".join(tags)),
            "description": tokenize(description),
            "headings": tokenize(" ".join(headings)),
            "body": tokenize(body),
        },
        lines=[line.rstrip() for line in body.splitlines()],
    )


GLOSSARY_TERM_RE = re.compile(r"^\*\*(.+?)\*\*:\s*$")


def glossary_docs(path: Path, rel: str, text: str) -> list[Doc]:
    _, body = split_frontmatter(text)
    docs: list[Doc] = []
    section = ""
    lines = body.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("### "):
            section = line[4:].strip()
        match = GLOSSARY_TERM_RE.match(line.strip())
        if match:
            term = match.group(1)
            definition: list[str] = []
            avoid = ""
            j = i + 1
            while (
                j < len(lines) and lines[j].strip() and not GLOSSARY_TERM_RE.match(lines[j].strip())
            ):
                if lines[j].strip().startswith("_Avoid_:"):
                    avoid = lines[j].split(":", 1)[1].strip()
                else:
                    definition.append(lines[j].strip())
                j += 1
            desc = " ".join(definition)
            docs.append(
                Doc(
                    path=f"{rel}#{term}",
                    doc_type="Glossary Term",
                    title=term,
                    description=desc + (f" Avoid: {avoid}." if avoid else ""),
                    tags=[section] if section else [],
                    fields={
                        "title": tokenize(term),
                        "tags": tokenize(avoid) + tokenize(section),
                        "description": tokenize(desc),
                        "headings": [],
                        "body": tokenize(desc + " " + avoid),
                    },
                    lines=[f"**{term}**: {desc}"] + ([f"_Avoid_: {avoid}"] if avoid else []),
                )
            )
            i = j
            continue
        i += 1
    return docs


def default_type_for(path: Path) -> str:
    parts = {p.lower() for p in path.parts}
    if "adr" in parts:
        return "Decision Record"
    if "knowledge" in parts:
        return "Concept"
    if "workflow-types" in parts:
        return "Workflow Specification"
    if "runtime-facts" in parts:
        return "Runtime Fact Sheet"
    if "issues" in parts:
        return "Issue Packet View"
    if "expansion" in parts:
        return "Specification Annex"
    if "general-mission-control" in parts:
        return "Specification"
    if "agents" in parts:
        return "Agent Configuration"
    return "Document"


def load_corpus(roots: list[Path]) -> list[Doc]:
    docs: list[Doc] = []
    for root in roots:
        if not root.exists():
            continue
        files = [root] if root.is_file() else sorted(root.rglob("*.md"))
        for file in files:
            try:
                rel = str(file.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
            except ValueError:
                rel = "../" + str(file.resolve().relative_to(REPO_ROOT.parent.resolve())).replace(
                    "\\", "/"
                )
            text = file.read_text(encoding="utf-8", errors="replace")
            if file.name == "GLOSSARY.md":
                docs.append(make_doc(file, rel, text, "Glossary"))
                docs.extend(glossary_docs(file, rel, text))
            else:
                docs.append(make_doc(file, rel, text, default_type_for(file)))
    return docs


def bm25(
    docs: list[Doc], query: list[str], k1: float = 1.4, b: float = 0.75
) -> list[tuple[float, Doc]]:
    weighted = []
    for d in docs:
        bag: list[str] = []
        for name, weight in FIELD_WEIGHTS.items():
            bag.extend(d.fields.get(name, []) * weight)
        weighted.append(Counter(bag))
    n = len(docs)
    avgdl = sum(sum(c.values()) for c in weighted) / max(n, 1)
    df: Counter[str] = Counter()
    for c in weighted:
        for term in c:
            df[term] += 1
    results = []
    for d, c in zip(docs, weighted, strict=True):
        dl = sum(c.values())
        score = 0.0
        for term in query:
            tf = c.get(term, 0)
            if not tf:
                continue
            idf = math.log(1 + (n - df[term] + 0.5) / (df[term] + 0.5))
            score += idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / max(avgdl, 1)))
        if score > 0:
            results.append((score, d))
    results.sort(key=lambda r: (-r[0], r[1].path))
    return results


def best_lines(doc: Doc, query: list[str], limit: int = 3) -> list[str]:
    scored = []
    for idx, line in enumerate(doc.lines):
        toks = set(tokenize(line))
        hits = sum(1 for q in query if q in toks)
        if hits:
            scored.append((-hits, idx, line.strip()))
    scored.sort()
    return [f"L{idx + 1}: {line[:160]}" for _, idx, line in scored[:limit]]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("query", nargs="*", help="search terms")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--type", dest="doc_type", help="restrict to one document type")
    parser.add_argument("--tag", help="restrict to documents carrying this tag")
    parser.add_argument(
        "--root", action="append", type=Path, help="override corpus roots (repeatable)"
    )
    parser.add_argument("--list-types", action="store_true", help="list document types and counts")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    roots = args.root or DEFAULT_ROOTS
    docs = load_corpus(roots)
    if args.list_types:
        for doc_type, count in sorted(Counter(d.doc_type for d in docs).items()):
            print(f"{count:4d}  {doc_type}")
        return 0
    if not args.query:
        parser.error("a query is required (or --list-types)")

    if args.doc_type:
        docs = [d for d in docs if d.doc_type.lower() == args.doc_type.lower()]
    if args.tag:
        docs = [d for d in docs if args.tag.lower() in {t.lower() for t in d.tags}]

    query = tokenize(" ".join(args.query))
    results = bm25(docs, query)[: args.limit]

    if args.json:
        print(
            json.dumps(
                [
                    {
                        "path": d.path,
                        "type": d.doc_type,
                        "title": d.title,
                        "description": d.description,
                        "tags": d.tags,
                        "score": round(score, 3),
                        "lines": best_lines(d, query),
                    }
                    for score, d in results
                ],
                indent=2,
            )
        )
        return 0

    if not results:
        print("no matches")
        return 1
    for rank, (score, d) in enumerate(results, 1):
        print(f"{rank}. [{d.doc_type}] {d.title}  ({d.path})  score={score:.2f}")
        if d.description:
            print(f"   {d.description[:200]}")
        for line in best_lines(d, query):
            print(f"   {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
