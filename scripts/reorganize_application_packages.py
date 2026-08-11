#!/usr/bin/env python3
"""Move flat app/application modules into packages and rewrite imports.

Mechanical only: filesystem moves + text import rewrites. No logic changes.
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app" / "application"

# module_stem -> package
MOVE_MAP: dict[str, str] = {
    # control_plane
    "control_plane": "control_plane",
    "control_plane_repository": "control_plane",
    # run_control
    "run_control": "run_control",
    "run_control_repository": "run_control",
    "postgres_run_control_repository": "run_control",
    "schema_grounding_admission": "run_control",
    "web_research_admission": "run_control",
    # operations
    "operation_execution": "operations",
    "operation_executor": "operations",
    "operation_journal": "operations",
    "operation_journal_backfill": "operations",
    "operation_journal_read_routing": "operations",
    "journaled_operation_execution": "operations",
    "semantic_operation_bindings": "operations",
    "operation_submission": "operations",
    "mongo_operation_execution_repository": "operations",
    "mongo_operation_journal_backfill": "operations",
    "mongo_operation_authority_migration": "operations",
    "postgres_operation_journal": "operations",
    "postgres_operation_journal_backfill": "operations",
    # orchestration
    "orchestration": "orchestration",
    "orchestration_routing": "orchestration",
    "orchestration_binding_repository": "orchestration",
    "postgres_orchestration_binding_repository": "orchestration",
    "goal_directed": "orchestration",
    "mongo_goal_directed_repository": "orchestration",
    "linked_runs": "orchestration",
    "postgres_linked_run_repository": "orchestration",
    # coordinator
    "coordinator_facade": "coordinator",
    "coordinator_launch": "coordinator",
    "coordinator_composition": "coordinator",
    "coordinator_results": "coordinator",
    "coordinator_run_resources": "coordinator",
    "coordinator_semantic_bindings": "coordinator",
    "coordinator_surface_promotion": "coordinator",
    "postgres_coordinator_audit_repository": "coordinator",
    "postgres_launch_ticket_repository": "coordinator",
    "postgres_workflow_result_repository": "coordinator",
    # schema
    "schema_catalog": "schema",
    "schema_catalog_build": "schema",
    "schema_context_derivation": "schema",
    "schema_context_selection": "schema",
    "schema_context_stage_handlers": "schema",
    "schema_workspace": "schema",
    "schema_workspace_binding": "schema",
    "schema_grounding_repository": "schema",
    "schema_grounding_semantic_handlers": "schema",
    "schema_authority_issuance": "schema",
    "schema_artifact_cleanup": "schema",
    "supporting_graph_reconciliation": "schema",
    "graph_query": "schema",
    # capability
    "capability_search": "capability",
    "capability_search_repository": "capability",
    "catalog_projection": "capability",
    "catalog_projection_admin": "capability",
    "catalog_projection_events": "capability",
    "catalog_projection_generation": "capability",
    "catalog_projection_metadata": "capability",
    "postgres_capability_search_repository": "capability",
    "postgres_capability_search_generation_repository": "capability",
    "reviewed_capability_promotion": "capability",
    # web_research
    "web_research_repository": "web_research",
    "web_research_semantic_binding": "web_research",
    "web_research_semantic_handlers": "web_research",
    "external_capability_discovery": "web_research",
    "external_candidate_repository": "web_research",
    "external_candidate_inspection": "web_research",
    # runtime
    "graph_runtime_dispatch": "runtime",
    "runtime_run_plan": "runtime",
    "runtime_bootstrap": "runtime",
    "runtime_decisions": "runtime",
    "runtime_events": "runtime",
    "runtime_lineage": "runtime",
    "runtime_resources": "runtime",
    "runtime_recovery": "runtime",
    "runtime_reconciliation": "runtime",
    "runtime_interventions": "runtime",
    "runtime_repairs": "runtime",
    "runtime_execution_bindings": "runtime",
    "runtime_neutral_operations": "runtime",
    "agent_server_actions": "runtime",
    "postgres_stage3_kernel_repository": "runtime",
    "postgres_runtime_execution_repository": "runtime",
    "postgres_runtime_authority": "runtime",
    # workspaces
    "workspace_materialization": "workspaces",
    "workspace_candidates": "workspaces",
    "goal_workspace": "workspaces",
    "artifact_promotion": "workspaces",
    "sandbox_snapshots": "workspaces",
    "mongo_artifact_repository": "workspaces",
    "mongo_snapshot_repository": "workspaces",
    "mongo_workspace_repository": "workspaces",
    "postgres_artifact_repository": "workspaces",
    # async_subagents
    "async_subagents": "async_subagents",
    "mongo_async_subagent_repository": "async_subagents",
    "postgres_async_subagents": "async_subagents",
    # reference_research
    "reference_research": "reference_research",
    # runners
    "web_research_coordinator_live": "runners",
    "scenario_b_live": "runners",
}

# When module stem == package name, store as service.py so the package can own the import path.
PRIMARY_AS_SERVICE = {
    pkg for stem, pkg in MOVE_MAP.items() if stem == pkg
}


def new_module_path(stem: str) -> tuple[str, str]:
    """Return (package, filename_stem) for a former flat module stem."""
    pkg = MOVE_MAP[stem]
    if stem in PRIMARY_AS_SERVICE:
        return pkg, "service"
    return pkg, stem


def fully_qualified(stem: str) -> str:
    pkg, file_stem = new_module_path(stem)
    if file_stem == "service" and stem == pkg:
        # Importers can keep using app.application.<pkg> via package __init__ re-exports,
        # but deep path is app.application.<pkg>.service
        return f"app.application.{pkg}.service"
    return f"app.application.{pkg}.{file_stem}"


def package_public_path(stem: str) -> str:
    """Canonical import path after reorg (always deep; avoids fat package __init__)."""
    pkg, file_stem = new_module_path(stem)
    return f"app.application.{pkg}.{file_stem}"


def build_import_rewrites() -> list[tuple[str, str]]:
    """Old -> new dotted module pairs (applied via placeholder two-phase rewrite)."""
    pairs: list[tuple[str, str]] = []
    for stem in sorted(MOVE_MAP, key=len, reverse=True):
        old = f"app.application.{stem}"
        new = package_public_path(stem)
        if old != new:
            pairs.append((old, new))
    return pairs


REWRITE_ROOTS = [
    ROOT / "app",
    ROOT / "tests",
    ROOT / "scripts",
]


def iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in REWRITE_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            if path.name == "reorganize_application_packages.py":
                continue
            if path.name == "reorganize_tests.py":
                continue
            files.append(path)
    return files


def rewrite_text(text: str, pairs: list[tuple[str, str]]) -> str:
    """Two-phase rewrite so short stems cannot corrupt already-rewritten longer paths."""
    tokens: list[tuple[str, str, str]] = []
    for index, (old, new) in enumerate(pairs):
        token = f"__APP_APPLICATION_REORG_{index}__"
        tokens.append((old, token, new))
    for old, token, _new in tokens:
        pattern = re.compile(rf"{re.escape(old)}(?![A-Za-z0-9_])")
        text = pattern.sub(token, text)
    for _old, token, new in tokens:
        text = text.replace(token, new)
    return text


def write_package_init(pkg: str, dry_run: bool) -> None:
    pkg_dir = APP_DIR / pkg
    init_path = pkg_dir / "__init__.py"
    # Keep package inits thin to avoid import-time cycles from star-reexports.
    content = f'"""Application package: {pkg}."""\n'
    if dry_run:
        print(f"WRITE {init_path.relative_to(ROOT)}")
        return
    init_path.write_text(content, encoding="utf-8")


def move_modules(dry_run: bool) -> None:
    packages = sorted(set(MOVE_MAP.values()))
    for pkg in packages:
        pkg_dir = APP_DIR / pkg
        if dry_run:
            print(f"MKDIR {pkg_dir.relative_to(ROOT)}")
        else:
            pkg_dir.mkdir(parents=True, exist_ok=True)

    for stem, pkg in sorted(MOVE_MAP.items()):
        src = APP_DIR / f"{stem}.py"
        if not src.exists():
            raise SystemExit(f"Missing source module: {src}")
        _, file_stem = new_module_path(stem)
        dest = APP_DIR / pkg / f"{file_stem}.py"
        if dest.exists():
            raise SystemExit(f"Destination already exists: {dest}")
        rel_src = src.relative_to(ROOT)
        rel_dest = dest.relative_to(ROOT)
        print(f"MOVE  {rel_src} -> {rel_dest}")
        if not dry_run:
            shutil.move(str(src), str(dest))

    for pkg in packages:
        write_package_init(pkg, dry_run=dry_run)


def rewrite_imports(dry_run: bool) -> int:
    pairs = build_import_rewrites()
    print(f"Import rewrite pairs: {len(pairs)}")
    changed = 0
    for path in iter_python_files():
        original = path.read_text(encoding="utf-8")
        updated = rewrite_text(original, pairs)
        if updated != original:
            changed += 1
            print(f"REWRITE {path.relative_to(ROOT)}")
            if not dry_run:
                path.write_text(updated, encoding="utf-8")
    return changed


def validate_no_flat_leftovers() -> None:
    leftovers = sorted(
        p.name for p in APP_DIR.glob("*.py") if p.name != "__init__.py"
    )
    if leftovers:
        raise SystemExit(f"Flat modules still present: {leftovers}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--rewrite-only", action="store_true")
    args = parser.parse_args()
    dry_run = not args.apply

    if dry_run:
        print("DRY RUN (pass --apply to execute)\n")

    mapped = set(MOVE_MAP)
    existing = {p.stem for p in APP_DIR.glob("*.py") if p.stem != "__init__"}
    if not args.rewrite_only:
        missing = existing - mapped
        extra = mapped - existing
        if missing:
            raise SystemExit(f"Unmapped flat modules: {sorted(missing)}")
        if extra:
            # Allow rewrite-only after moves when flat files are gone
            if existing:
                raise SystemExit(f"Map entries missing on disk: {sorted(extra)}")

    if not args.rewrite_only:
        move_modules(dry_run=dry_run)
    changed = rewrite_imports(dry_run=dry_run)
    print(f"\nFiles with import rewrites: {changed}")
    if args.apply and not args.rewrite_only:
        validate_no_flat_leftovers()
        print("Validation OK: no flat application modules left.")


if __name__ == "__main__":
    main()
