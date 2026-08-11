#!/usr/bin/env python3
"""Move flat tests into unit/integration/experiments packages and rewrite imports."""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"

# old relative path under tests/ -> new relative path under tests/
MOVE_MAP: dict[str, str] = {
    # experiments
    "test_schema_context_run_comparison.py": "experiments/test_schema_context_run_comparison.py",
    # integration / postgres
    "test_atomic_family_admission_postgres_integration.py": "integration/postgres/test_atomic_family_admission_postgres_integration.py",
    "test_run_control_postgres_integration.py": "integration/postgres/test_run_control_postgres_integration.py",
    "test_stage3_kernel_postgres_integration.py": "integration/postgres/test_stage3_kernel_postgres_integration.py",
    "test_pre_stage3_database_authority_integration.py": "integration/postgres/test_pre_stage3_database_authority_integration.py",
    "test_operation_journal_stage1.py": "integration/postgres/test_operation_journal_stage1.py",
    "test_postgres_workflow_result_repository.py": "integration/postgres/test_postgres_workflow_result_repository.py",
    "test_artifact_promotion_postgres_integration.py": "integration/postgres/test_artifact_promotion_postgres_integration.py",
    "test_operation_journal_backfill_integration.py": "integration/postgres/test_operation_journal_backfill_integration.py",
    # integration / mongodb
    "test_control_plane_mongodb_integration.py": "integration/mongodb/test_control_plane_mongodb_integration.py",
    "test_sandbox_snapshots_mongodb_integration.py": "integration/mongodb/test_sandbox_snapshots_mongodb_integration.py",
    "test_artifact_promotion_mongodb_integration.py": "integration/mongodb/test_artifact_promotion_mongodb_integration.py",
    "test_workspace_materialization_mongodb_integration.py": "integration/mongodb/test_workspace_materialization_mongodb_integration.py",
    "test_external_candidate_mongodb_integration.py": "integration/mongodb/test_external_candidate_mongodb_integration.py",
    "test_operation_execution_mongodb_integration.py": "integration/mongodb/test_operation_execution_mongodb_integration.py",
    # integration / temporal
    "test_wp_bp_010_temporal.py": "integration/temporal/test_wp_bp_010_temporal.py",
    "test_wp_bp_010_recovery.py": "integration/temporal/test_wp_bp_010_recovery.py",
    "test_wp_bp_020_temporal.py": "integration/temporal/test_wp_bp_020_temporal.py",
    "test_coordinator_temporal_runtime.py": "integration/temporal/test_coordinator_temporal_runtime.py",
    "test_pre_stage3_temporal_contracts.py": "integration/temporal/test_pre_stage3_temporal_contracts.py",
    "test_linked_runs.py": "integration/temporal/test_linked_runs.py",
    # unit / control_plane
    "test_control_plane.py": "unit/control_plane/test_control_plane.py",
    "test_control_plane_payloads.py": "unit/control_plane/test_control_plane_payloads.py",
    "test_agentic_asset_definitions.py": "unit/control_plane/test_agentic_asset_definitions.py",
    # unit / run_control
    "test_run_control.py": "unit/run_control/test_run_control.py",
    "test_atomic_family_admission.py": "unit/run_control/test_atomic_family_admission.py",
    # unit / operations
    "test_operation_execution.py": "unit/operations/test_operation_execution.py",
    "test_operation_submission_api.py": "unit/operations/test_operation_submission_api.py",
    "test_operation_delegation.py": "unit/operations/test_operation_delegation.py",
    "test_operation_journal_backfill.py": "unit/operations/test_operation_journal_backfill.py",
    # unit / workspaces
    "test_artifact_promotion.py": "unit/workspaces/test_artifact_promotion.py",
    "test_sandbox_snapshots.py": "unit/workspaces/test_sandbox_snapshots.py",
    "test_goal_workspace.py": "unit/workspaces/test_goal_workspace.py",
    "test_workspace_materialization.py": "unit/workspaces/test_workspace_materialization.py",
    "test_workspace_candidates.py": "unit/workspaces/test_workspace_candidates.py",
    # unit / orchestration
    "test_stagegraph_v2.py": "unit/orchestration/test_stagegraph_v2.py",
    "test_wp_bp_020_goal_directed.py": "unit/orchestration/test_wp_bp_020_goal_directed.py",
    "test_generic_artifact_workflow.py": "unit/orchestration/test_generic_artifact_workflow.py",
    # unit / runtime
    "test_graph_runtime_contracts.py": "unit/runtime/test_graph_runtime_contracts.py",
    "test_graph_runtime_dispatch.py": "unit/runtime/test_graph_runtime_dispatch.py",
    "test_stage3_kernel_contracts.py": "unit/runtime/test_stage3_kernel_contracts.py",
    "test_runtime_events_stage3.py": "unit/runtime/test_runtime_events_stage3.py",
    "test_runtime_decisions_stage3.py": "unit/runtime/test_runtime_decisions_stage3.py",
    "test_runtime_lineage_stage3.py": "unit/runtime/test_runtime_lineage_stage3.py",
    "test_runtime_resources_stage3.py": "unit/runtime/test_runtime_resources_stage3.py",
    "test_runtime_recovery_stage3.py": "unit/runtime/test_runtime_recovery_stage3.py",
    "test_runtime_interventions_stage3.py": "unit/runtime/test_runtime_interventions_stage3.py",
    "test_runtime_bootstrap_stage3.py": "unit/runtime/test_runtime_bootstrap_stage3.py",
    "test_runtime_incident_reconciliation_stage3.py": "unit/runtime/test_runtime_incident_reconciliation_stage3.py",
    "test_agent_server_actions_stage3.py": "unit/runtime/test_agent_server_actions_stage3.py",
    # unit / agent_server
    "test_agent_server_block_c_unit.py": "unit/agent_server/test_agent_server_block_c_unit.py",
    "test_agent_server_block_c_persistent.py": "unit/agent_server/test_agent_server_block_c_persistent.py",
    "test_langgraph_agent_server_stage3.py": "unit/agent_server/test_langgraph_agent_server_stage3.py",
    "test_langgraph_persistence_stage3.py": "unit/agent_server/test_langgraph_persistence_stage3.py",
    # unit / schema
    "test_schema_grounding_services.py": "unit/schema/test_schema_grounding_services.py",
    "test_schema_grounding_api.py": "unit/schema/test_schema_grounding_api.py",
    "test_schema_grounding_control_plane.py": "unit/schema/test_schema_grounding_control_plane.py",
    "test_schema_grounding_surface_promotion.py": "unit/schema/test_schema_grounding_surface_promotion.py",
    "test_schema_catalog.py": "unit/schema/test_schema_catalog.py",
    "test_schema_catalog_core.py": "unit/schema/test_schema_catalog_core.py",
    "test_schema_context_stage_handlers.py": "unit/schema/test_schema_context_stage_handlers.py",
    "test_schema_context_selection.py": "unit/schema/test_schema_context_selection.py",
    "test_schema_operation_projection.py": "unit/schema/test_schema_operation_projection.py",
    "test_schema_expansion.py": "unit/schema/test_schema_expansion.py",
    "test_schema_workspace.py": "unit/schema/test_schema_workspace.py",
    "test_schema_artifact_cleanup.py": "unit/schema/test_schema_artifact_cleanup.py",
    "test_schema_authority_issuance.py": "unit/schema/test_schema_authority_issuance.py",
    "test_stage_schema_grounding_live_inputs.py": "unit/schema/test_stage_schema_grounding_live_inputs.py",
    "test_graph_query_intents.py": "unit/schema/test_graph_query_intents.py",
    # unit / coordinator
    "test_coordinator_facade.py": "unit/coordinator/test_coordinator_facade.py",
    "test_coordinator_launch_idempotency.py": "unit/coordinator/test_coordinator_launch_idempotency.py",
    "test_coordinator_launch_preparation.py": "unit/coordinator/test_coordinator_launch_preparation.py",
    "test_coordinator_semantic_binding_integration.py": "unit/coordinator/test_coordinator_semantic_binding_integration.py",
    "test_coordinator_mcp_http_deployment.py": "unit/coordinator/test_coordinator_mcp_http_deployment.py",
    "test_coordinator_mcp_read_surface.py": "unit/coordinator/test_coordinator_mcp_read_surface.py",
    "test_coordinator_acceptance_evaluator.py": "unit/coordinator/test_coordinator_acceptance_evaluator.py",
    "test_coordinator_evaluation_dataset.py": "unit/coordinator/test_coordinator_evaluation_dataset.py",
    "test_coordinator_result_completion.py": "unit/coordinator/test_coordinator_result_completion.py",
    "test_coordinator_run_resources.py": "unit/coordinator/test_coordinator_run_resources.py",
    "test_coordinator_security_audit.py": "unit/coordinator/test_coordinator_security_audit.py",
    "test_coordinator_surface_promotion.py": "unit/coordinator/test_coordinator_surface_promotion.py",
    # unit / capability
    "test_capability_hybrid_search.py": "unit/capability/test_capability_hybrid_search.py",
    "test_capability_search_projection.py": "unit/capability/test_capability_search_projection.py",
    "test_catalog_projection_reliability.py": "unit/capability/test_catalog_projection_reliability.py",
    "test_reviewed_capability_promotion.py": "unit/capability/test_reviewed_capability_promotion.py",
    "test_web_capability_catalog_seed.py": "unit/capability/test_web_capability_catalog_seed.py",
    # unit / web_research
    "test_web_research_semantic_handlers.py": "unit/web_research/test_web_research_semantic_handlers.py",
    "test_web_research_live_adapters.py": "unit/web_research/test_web_research_live_adapters.py",
    "test_run_web_research_coordinator_live.py": "unit/web_research/test_run_web_research_coordinator_live.py",
    "test_web_research_admission.py": "unit/web_research/test_web_research_admission.py",
    "test_external_capability_discovery.py": "unit/web_research/test_external_capability_discovery.py",
    "test_external_candidate_quarantine.py": "unit/web_research/test_external_candidate_quarantine.py",
    "test_quarantine_static_inspection.py": "unit/web_research/test_quarantine_static_inspection.py",
    "test_scenario_d_execution_correction.py": "unit/web_research/test_scenario_d_execution_correction.py",
    # unit / reference_research
    "test_reference_research_stage0_2.py": "unit/reference_research/test_reference_research_stage0_2.py",
    # unit / config_api
    "test_config.py": "unit/config_api/test_config.py",
    "test_server.py": "unit/config_api/test_server.py",
    # unit / integrations
    "test_langsmith_tracing.py": "unit/integrations/test_langsmith_tracing.py",
    "test_neo4j_read_executor.py": "unit/integrations/test_neo4j_read_executor.py",
    "test_neo4j_schema_snapshot_v2.py": "unit/integrations/test_neo4j_schema_snapshot_v2.py",
}


def module_name_from_rel(rel: str) -> str:
    return "tests." + rel[:-3].replace("/", ".").replace("\\", ".")


def move_tests(dry_run: bool) -> None:
    existing_root = {p.name for p in TESTS.glob("test_*.py")}
    mapped = set(MOVE_MAP)
    missing = sorted(mapped - existing_root)
    unmapped = sorted(existing_root - mapped)
    if missing:
        raise SystemExit(f"Map entries missing on disk: {missing}")
    if unmapped:
        raise SystemExit(f"Unmapped root tests: {unmapped}")

    for old_rel, new_rel in sorted(MOVE_MAP.items()):
        src = TESTS / old_rel
        dest = TESTS / new_rel
        print(f"MOVE  tests/{old_rel} -> tests/{new_rel}")
        if not dry_run:
            dest.parent.mkdir(parents=True, exist_ok=True)
            current = dest.parent
            while current != TESTS and str(current).startswith(str(TESTS)):
                init = current / "__init__.py"
                if not init.exists():
                    init.write_text("", encoding="utf-8")
                current = current.parent
            shutil.move(str(src), str(dest))


def rewrite_test_imports(dry_run: bool) -> int:
    pairs: list[tuple[re.Pattern[str], str]] = []
    for old_rel, new_rel in MOVE_MAP.items():
        old_mod = module_name_from_rel(old_rel)
        new_mod = module_name_from_rel(new_rel)
        if old_mod == new_mod:
            continue
        pairs.append((re.compile(rf"{re.escape(old_mod)}(?![A-Za-z0-9_])"), new_mod))
    pairs.sort(key=lambda p: len(p[0].pattern), reverse=True)

    changed = 0
    for path in list((ROOT / "app").rglob("*.py")) + list((ROOT / "tests").rglob("*.py")) + list(
        (ROOT / "scripts").rglob("*.py")
    ):
        if "__pycache__" in path.parts:
            continue
        if path.name in {"reorganize_tests.py", "reorganize_application_packages.py"}:
            continue
        original = path.read_text(encoding="utf-8")
        updated = original
        for pattern, new in pairs:
            updated = pattern.sub(new, updated)
        if updated != original:
            changed += 1
            print(f"REWRITE {path.relative_to(ROOT)}")
            if not dry_run:
                path.write_text(updated, encoding="utf-8")
    return changed


def archive_stale_docs(dry_run: bool) -> None:
    archive = TESTS / "archive"
    handoff = TESTS / "SCHEMA_CONTEXT_SELECTION_WORKFLOW_HANDOFF.md"
    if handoff.exists():
        dest = archive / handoff.name
        print(f"MOVE  {handoff.relative_to(ROOT)} -> {dest.relative_to(ROOT)}")
        if not dry_run:
            archive.mkdir(parents=True, exist_ok=True)
            (archive / "README.md").write_text(
                "# Archived test docs\n\nStale notes kept for history; not executed.\n",
                encoding="utf-8",
            )
            shutil.move(str(handoff), str(dest))


def cleanup_orphan_pycache(dry_run: bool) -> None:
    cache = TESTS / "__pycache__"
    if not cache.exists():
        return
    targets = [
        "test_openai_agents_runtime",
        "test_agents_compatibility",
    ]
    for pyc in cache.glob("*.pyc"):
        if any(t in pyc.name for t in targets):
            print(f"DELETE {pyc.relative_to(ROOT)}")
            if not dry_run:
                pyc.unlink()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dry_run = not args.apply
    if dry_run:
        print("DRY RUN (pass --apply to execute)\n")
    move_tests(dry_run=dry_run)
    archive_stale_docs(dry_run=dry_run)
    cleanup_orphan_pycache(dry_run=dry_run)
    changed = rewrite_test_imports(dry_run=dry_run)
    print(f"\nFiles with test-import rewrites: {changed}")


if __name__ == "__main__":
    main()
