"""RRM-015 static guard: no digest from a JSON-mode dump of a contract that holds a set.

`model_dump(mode="json")` lists `set`/`frozenset` members in per-process iteration order, so
`sha256_digest(contract.model_dump(mode="json"))` is not an identity. Use
`app.domain.control_plane.canonical.stable_json_digest` / `stable_json_dump` (set-sorted and
value-identical to the JSON dump for set-free contracts) or `sha256_digest(contract)` directly.

The scan (`tests/fixtures/digest_sites.py`) finds every JSON-mode dump in `app/` that flows into
a digest-like call, directly or through a local variable. A site may only stay on a JSON-mode
dump if it is listed in `AUDITED_NOT_AFFECTED` below with the models it dumps; the test then
verifies, from the model classes, that none of them holds a set (transitively, including
subclasses), so a later change that adds a set to an audited model fails here.
"""

from __future__ import annotations

import importlib
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import BaseModel

from tests.fixtures.digest_sites import scan_digest_sites
from tests.fixtures.set_order import model_holds_set

CD = "app.domain.control_plane.contracts"
GR = "app.domain.graph_runtime"
OE = "app.domain.operation_execution"
RC = "app.domain.run_control"
SC = "app.domain.schema_context.contracts"
SG = "app.domain.schema_grounding.contracts"
SW = "app.experiments.dynamic_research_swarm.contracts"


@dataclass(frozen=True)
class Audited:
    models: tuple[str, ...]
    reason: str


NO_SET = "no set-valued field is reachable from the dumped model, so the JSON dump is order-stable"
THIRD_PARTY = "third-party MCP `Tool` schema (JSON primitives only), not a BellLabs contract"
ANY_PAYLOAD = (
    f"{NO_SET}; `Any`/`dict[str, Any]` members are JSON payloads validated from JSON documents"
)

# (path, enclosing function, receivers dumped, model classes, reason)
_AUDIT: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...], str], ...] = (
    (
        "app/application/coordinator/postgres_workflow_result_repository.py",
        "PostgresWorkflowResultRepository.save",
        ("result",),
        ("app.domain.coordinator.launch.WorkflowResultRecord",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/operations/journaled_operation_execution.py",
        "JournaledOperationExecutionCoordinator.acquire",
        ("claim",),
        (f"{OE}.journal.OperationEffectClaim",),
        NO_SET,
    ),
    (
        "app/application/operations/operation_journal.py",
        "OperationJournalMutation.validate",
        ("self.claim",),
        (f"{OE}.journal.OperationEffectClaim",),
        NO_SET,
    ),
    (
        "app/application/operations/operation_execution.py",
        "RunControlOperationAuthority._verify_bound_authority",
        ("configuration.workflow_workspace_contract",),
        (f"{CD}.WorkflowWorkspaceContract",),
        NO_SET,
    ),
    (
        "app/application/orchestration/goal_directed.py",
        "GoalDirectedOperationResultService.reconcile",
        ("observed",),
        (f"{OE}.contracts.OperationWorkflowResult",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/orchestration/service.py",
        "RunControlLifecycleGateway.execute",
        ("item",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "app/application/orchestration/service.py",
        "StageGraphDecisionService.complete",
        ("item",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "app/domain/run_control/reducer.py",
        "_evidence_frontier",
        ("item",),
        (f"{RC}.contracts.AcceptedObligationEvidence", f"{RC}.contracts.AcceptedOutputEvidence"),
        NO_SET,
    ),
    (
        "app/domain/run_control/reducer.py",
        "_terminal_outcome",
        ("item",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "app/application/reference_research/service.py",
        "execute_reference_fixture",
        ("fixture",),
        (
            "app.domain.reference_research.contracts.QualiaFixtureInput",
            "app.domain.reference_research.contracts.DaveFixtureInput",
        ),
        NO_SET,
    ),
    (
        "app/application/reference_research/service.py",
        "execute_reference_fixture",
        ("lease_request",),
        (f"{GR}.kernel.ResourceLeaseRequest",),
        NO_SET,
    ),
    (
        "app/application/reference_research/service.py",
        "prepare_reference_implementation",
        ("resources",),
        (f"{GR}.definitions.ExecutionResourceEnvelopeV2",),
        NO_SET,
    ),
    (
        "app/application/run_control/run_control_repository.py",
        "FamilyAdmissionCommit.__post_init__",
        ("mutation",),
        (f"{RC}.family_admission.AtomicFamilyMutation",),
        NO_SET,
    ),
    (
        "app/application/runners/web_research_coordinator_live.py",
        "_launch_proposal",
        ("ref",),
        (f"{CD}.ExactDefinitionRef",),
        NO_SET,
    ),
    (
        "app/application/runners/web_research_coordinator_live.py",
        "_run_mounted_mcp_planning",
        ("tool",),
        (),
        THIRD_PARTY,
    ),
    (
        "app/integrations/web_research_runtime.py",
        "_tools_snapshot_digest",
        ("tool",),
        (),
        THIRD_PARTY,
    ),
    (
        "app/application/runtime/postgres_stage3_kernel_repository.py",
        "PostgresExecutionLineageRepository.append",
        ("parent_edge",),
        (f"{GR}.kernel.LineageParentEdge",),
        NO_SET,
    ),
    (
        "app/application/runtime/postgres_stage3_kernel_repository.py",
        "PostgresForkRepository.reserve",
        ("request",),
        (f"{GR}.contracts.ForkRequest",),
        NO_SET,
    ),
    (
        "app/application/runtime/runtime_decisions.py",
        "DurableDecisionService.create_request",
        ("request",),
        (f"{GR}.kernel.DecisionRequest",),
        NO_SET,
    ),
    (
        "app/application/runtime/runtime_lineage.py",
        "PersistedExecutionLineage.lineage_is_canonical_and_scope_bound",
        ("self",),
        ("app.application.runtime.runtime_lineage.PersistedExecutionLineage",),
        NO_SET,
    ),
    (
        "app/application/runtime/runtime_run_plan.py",
        "compile_run_plan",
        ("item",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "app/application/runtime/runtime_run_plan.py",
        "compile_run_plan_v3",
        ("item",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "app/application/runtime/runtime_run_plan.py",
        "compile_run_plan_v4",
        ("item",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "app/application/schema/graph_query.py",
        "intent_digest",
        ("intent",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/schema/schema_grounding_semantic_handlers.py",
        "SupportingGraphSemanticBindingProvider.prepare",
        ("intent",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/schema/supporting_graph_reconciliation.py",
        "SupportingGraphReconciliationWorkflow.run",
        ("intent",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/schema/supporting_graph_reconciliation.py",
        "_terminal_result",
        ("intent",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "app/application/schema/supporting_graph_reconciliation.py",
        "_reconciliation_request_digest",
        ("evidence",),
        (f"{SC}.GraphReconciliationEvidence",),
        NO_SET,
    ),
    (
        "app/application/schema/schema_catalog_build.py",
        "_request_fingerprint",
        ("request",),
        (f"{SG}.SchemaCatalogBuildRequest",),
        NO_SET,
    ),
    (
        "app/application/web_research/web_research_semantic_binding.py",
        "verify_web_research_operation_binding",
        ("profile_ref", "runtime_profile_ref", "workspace_template_ref"),
        (f"{CD}.ExactDefinitionRef",),
        NO_SET,
    ),
    (
        "app/application/workspaces/mongo_workspace_repository.py",
        "MongoWorkspaceManifestRepository.reserve_writable_slots",
        ("request",),
        (f"{OE}.contracts.WorkspaceMaterializationRequest",),
        NO_SET,
    ),
    (
        "app/application/workspaces/workspace_materialization.py",
        "InMemoryWorkspaceManifestRepository.reserve_writable_slots",
        ("request",),
        (f"{OE}.contracts.WorkspaceMaterializationRequest",),
        NO_SET,
    ),
    (
        "app/application/workspaces/workspace_materialization.py",
        "WorkspaceMaterializationService._append_revision",
        ("current.template_ref", "entry", "slot"),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "app/application/workspaces/workspace_materialization.py",
        "WorkspaceMaterializationService._initial_manifest",
        ("request.template_ref", "entry", "slot"),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "app/domain/operation_execution/materialization.py",
        "workspace_manifest_digest",
        ("manifest.template_ref", "entry", "slot"),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "app/domain/coordinator/launch.py",
        "SemanticBindingPlan.plan_content_matches_digest",
        ("self",),
        ("app.domain.coordinator.launch.SemanticBindingPlan",),
        ANY_PAYLOAD,
    ),
    (
        "app/domain/graph_runtime/contracts.py",
        "GraphExecutionSubmission.submission_digest_matches_intent",
        ("self",),
        (f"{GR}.contracts.GraphExecutionSubmission",),
        NO_SET,
    ),
    (
        "app/domain/graph_runtime/contracts.py",
        "InterventionBase.intervention_digest_matches_intent",
        ("self",),
        (f"{GR}.contracts.InterventionBase",),
        NO_SET,
    ),
    (
        "app/domain/operation_execution/checkpoint_lineage.py",
        "CheckpointTransitionObservation.content_digest",
        ("self",),
        (f"{OE}.checkpoint_lineage.CheckpointTransitionObservation",),
        NO_SET,
    ),
    (
        "app/domain/operation_execution/journal.py",
        "OperationJournalSettlement.create",
        ("draft",),
        (f"{OE}.journal.OperationJournalSettlement",),
        ANY_PAYLOAD,
    ),
    (
        "app/domain/operation_execution/journal.py",
        "OperationJournalSettlement.terminal_shape_is_consistent",
        ("self",),
        (f"{OE}.journal.OperationJournalSettlement",),
        ANY_PAYLOAD,
    ),
    (
        "app/domain/schema_catalog/parser.py",
        "parse_physical_schema",
        ("value",),
        ("app.domain.schema_catalog.models.PhysicalSchemaCatalog",),
        NO_SET,
    ),
    (
        "app/domain/schema_context/validation.py",
        "accept_selection",
        ("review", "selection"),
        (f"{SC}.SchemaSelectionReview", f"{SC}.SchemaContextSelection"),
        NO_SET,
    ),
    (
        "app/experiments/dynamic_research_swarm/evaluators.py",
        "evaluate_claim",
        ("gate",),
        (f"{SW}.GateResult",),
        NO_SET,
    ),
    (
        "app/experiments/dynamic_research_swarm/repository.py",
        "SwarmEvidenceRepository.save_plan",
        ("plan",),
        (f"{SW}.MissionPlan",),
        NO_SET,
    ),
    (
        "app/experiments/dynamic_research_swarm/temporal_activities.py",
        "execute_swarm_stage",
        ("output",),
        (
            f"{SW}.SourceBundle",
            f"{SW}.MissionPlan",
            f"{SW}.ResearchUnitResult",
            f"{SW}.FinalSynthesis",
        ),
        NO_SET,
    ),
    (
        "app/integrations/conformance_operation_runtime.py",
        "ConformanceSandbox.materialize",
        ("mount",),
        (f"{OE}.contracts.WorkspaceMount",),
        NO_SET,
    ),
)

AUDITED_NOT_AFFECTED: dict[tuple[str, str, str], Audited] = {
    (path, function, receiver): Audited(models, reason)
    for path, function, receivers, models, reason in _AUDIT
    for receiver in receivers
}


def _resolve(qualified: str) -> type[BaseModel]:
    module, _, name = qualified.rpartition(".")
    resolved = getattr(importlib.import_module(module), name)
    assert isinstance(resolved, type) and issubclass(resolved, BaseModel), qualified
    return resolved


def test_no_unaudited_json_dump_digest_sites() -> None:
    """A new `sha256_digest(<contract>.model_dump(mode="json"))` must use `stable_json_digest`."""

    in_scope, _excluded = scan_digest_sites()
    unaudited = sorted(
        {
            f"{site.path}:{site.line} {site.function}: {site.receiver}"
            for site in in_scope
            if site.key not in AUDITED_NOT_AFFECTED
        }
    )
    assert not unaudited, (
        "A JSON-mode dump of a contract feeds a digest. `model_dump(mode='json')` lists sets in "
        "per-process iteration order, so the digest is not an identity. Use "
        "`app.domain.control_plane.canonical.stable_json_digest(model)` (or `stable_json_dump`), "
        "or audit the site into AUDITED_NOT_AFFECTED with proof that no set is reachable:\n"
        + "\n".join(unaudited)
    )


def test_audit_allowlist_has_no_stale_entries() -> None:
    in_scope, excluded = scan_digest_sites()
    live = {site.key for site in in_scope}
    stale = sorted(key for key in AUDITED_NOT_AFFECTED if key not in live)
    assert not stale, f"audited sites no longer present (remove them from the allowlist): {stale}"
    # Sites in areas owned by another work package (RRM-013: async subagents, agent server) are
    # reported, not edited, by RRM-015; none currently feeds a digest from a contract dump.
    assert not excluded, f"digest sites in RRM-013 areas need a ticket entry: {excluded}"


@pytest.mark.parametrize(
    ("key", "audited"),
    sorted(AUDITED_NOT_AFFECTED.items()),
    ids=lambda value: "/".join(value[1:]) if isinstance(value, tuple) else "audit",
)
def test_audited_models_hold_no_sets(key: tuple[str, str, str], audited: Audited) -> None:
    """An audited site is only "not affected" while its models hold no set (incl. subclasses)."""

    for qualified in audited.models:
        assert not model_holds_set(_resolve(qualified)), (
            f"{qualified} now holds a set; move {key} to stable_json_digest/stable_json_dump"
        )
    assert audited.models or audited.reason == THIRD_PARTY


def test_scanner_detects_the_banned_patterns(tmp_path: Path) -> None:
    """The guard itself: direct, multi-line and via-variable JSON-dump digests are all found."""

    root = tmp_path / "app"
    root.mkdir()
    (root / "sample.py").write_text(
        textwrap.dedent(
            """
            from app.domain.control_plane.canonical import sha256_digest, stable_json_digest

            def direct(x):
                return sha256_digest(x.model_dump(mode="json"))

            def multiline(x):
                return sha256_digest(
                    x.model_dump(
                        mode="json",
                        exclude={"requested_at"},
                    )
                )

            def via_variable(x):
                payload = x.model_dump(mode="json")
                return sha256_digest(payload)

            def nested(x):
                return sha256_digest({"items": [i.model_dump(mode="json") for i in x]})

            def fingerprint_named(x):
                return _request_fingerprint(x.model_dump(mode="json"))

            def safe_stable(x):
                return stable_json_digest(x)

            def safe_python_mode(x):
                return sha256_digest(x.model_dump(mode="python"))

            def persisted_only(x):
                return store(x.model_dump(mode="json"))
            """
        ),
        encoding="utf-8",
    )
    found, _ = scan_digest_sites(root)
    assert sorted(site.function for site in found) == [
        "direct",
        "fingerprint_named",
        "multiline",
        "nested",
        "via_variable",
    ]
