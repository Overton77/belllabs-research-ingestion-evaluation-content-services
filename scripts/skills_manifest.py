#!/usr/bin/env python3
"""Compute or verify ``manifest.json`` digests for every skill bundle under ``skills/``.

A bundle is a directory containing ``SKILL.md``. Its ``manifest.json`` (schema
``mc.skill_bundle.v1``) lists every other file in the bundle with its ``sha256:<hex>`` digest,
plus the bundle name, version and compatible service contract range. The digest map is what
the catalog pins and what the materializer verifies before mounting, so drift between the
files on disk and the manifest must fail ``make check``.

Usage:
  python scripts/skills_manifest.py --check            # exit 1 on any drift or missing manifest
  python scripts/skills_manifest.py --write            # rewrite manifests from the files on disk
  python scripts/skills_manifest.py --write --root skills --bundle mission-control-author

The script never touches ``name``, ``version`` or ``service_contract_range`` when a manifest
exists; a new bundle gets ``version`` ``0.1.0`` and the default contract range. Keys are
written sorted so two runs produce byte-identical output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

SCHEMA_VERSION = "mc.skill_bundle.v1"
DEFAULT_VERSION = "0.1.0"
DEFAULT_CONTRACT_RANGE = ">=0.1.0,<0.2.0"
MANIFEST_NAME = "manifest.json"
SKIP_DIRS = {".git", "node_modules", "__pycache__"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def bundle_files(bundle: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for path in sorted(bundle.rglob("*")):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(bundle).parts):
            continue
        relative = path.relative_to(bundle).as_posix()
        if relative == MANIFEST_NAME:
            continue
        files[relative] = sha256_file(path)
    return dict(sorted(files.items()))


def discover_bundles(root: Path, only: set[str] | None) -> list[Path]:
    bundles: list[Path] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir() or not (child / "SKILL.md").is_file():
            continue
        if only and child.name not in only:
            continue
        bundles.append(child)
    return bundles


NAME_RULE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def frontmatter_problem(bundle: Path) -> str | None:
    """Return why ``SKILL.md`` frontmatter breaks the Agent Skills rules, or None."""
    text = (bundle / "SKILL.md").read_text(encoding="utf-8")
    match = re.match(r"---\r?\n(.*?)\r?\n---\r?\n", text, re.DOTALL)
    if match is None:
        return "SKILL.md has no frontmatter block"
    fields = dict(
        line.split(":", 1) for line in match.group(1).splitlines() if ":" in line and line[0] != " "
    )
    name = fields.get("name", "").strip()
    if name != bundle.name:
        return f"frontmatter name {name!r} differs from directory {bundle.name!r}"
    if len(name) > 64 or not NAME_RULE.match(name):
        return f"frontmatter name {name!r} breaks the Agent Skills name rule"
    if not fields.get("description", "").strip():
        return "frontmatter description is empty"
    return None


def load_manifest(bundle: Path) -> dict | None:
    manifest_path = bundle / MANIFEST_NAME
    if not manifest_path.is_file():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def build_manifest(bundle: Path, existing: dict | None) -> dict:
    files = bundle_files(bundle)
    manifest: dict = {
        "schema_version": SCHEMA_VERSION,
        "name": (existing or {}).get("name", bundle.name),
        "version": (existing or {}).get("version", DEFAULT_VERSION),
        "service_contract_range": (existing or {}).get(
            "service_contract_range", DEFAULT_CONTRACT_RANGE
        ),
        "files": files,
    }
    for key, value in (existing or {}).items():
        if key in manifest or key == "operation_catalog_digest":
            continue
        manifest[key] = value
    operations = files.get("references/operations.json")
    if operations is not None:
        manifest["operation_catalog_digest"] = operations
    return manifest


def dump(manifest: dict) -> str:
    return json.dumps(manifest, indent=2, sort_keys=False, ensure_ascii=False) + "\n"


def check(bundles: list[Path]) -> int:
    failures = 0
    for bundle in bundles:
        existing = load_manifest(bundle)
        if existing is None:
            print(f"MISSING  {bundle.name}: no {MANIFEST_NAME}")
            failures += 1
            continue
        problem = frontmatter_problem(bundle)
        if problem:
            print(f"FRONT    {bundle.name}: {problem}")
            failures += 1
        expected = build_manifest(bundle, existing)
        if existing.get("name") != bundle.name:
            print(
                f"NAME     {bundle.name}: manifest name {existing.get('name')!r} "
                "differs from directory"
            )
            failures += 1
        if existing.get("files") != expected["files"]:
            print(f"DRIFT    {bundle.name}:")
            before, after = existing.get("files", {}), expected["files"]
            for path in sorted(set(before) | set(after)):
                if before.get(path) != after.get(path):
                    print(f"           {path}: manifest={before.get(path)} disk={after.get(path)}")
            failures += 1
        elif existing.get("operation_catalog_digest") != expected.get("operation_catalog_digest"):
            print(f"DRIFT    {bundle.name}: operation_catalog_digest")
            failures += 1
        elif not problem:
            print(f"ok       {bundle.name} ({len(expected['files'])} files)")
    return 1 if failures else 0


def write(bundles: list[Path]) -> int:
    for bundle in bundles:
        manifest = build_manifest(bundle, load_manifest(bundle))
        (bundle / MANIFEST_NAME).write_text(dump(manifest), encoding="utf-8", newline="\n")
        print(f"wrote    {bundle.name} ({len(manifest['files'])} files)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--root", default="skills", help="directory holding the bundles (default: skills)"
    )
    parser.add_argument("--bundle", action="append", help="limit to this bundle name (repeatable)")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="verify manifests; exit 1 on drift")
    mode.add_argument("--write", action="store_true", help="rewrite manifests from disk")
    args = parser.parse_args(argv)

    root = Path(args.root)
    if not root.is_dir():
        print(f"no such directory: {root}", file=sys.stderr)
        return 2
    bundles = discover_bundles(root, set(args.bundle) if args.bundle else None)
    if not bundles:
        print(f"no bundles found under {root}", file=sys.stderr)
        return 2
    return write(bundles) if args.write else check(bundles)


if __name__ == "__main__":
    raise SystemExit(main())
