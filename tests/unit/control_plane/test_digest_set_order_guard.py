"""RRM-015 static guard: no digest from a JSON-mode dump of a contract that holds a set.

`model_dump(mode="json")` lists `set`/`frozenset` members in per-process iteration order, so
`sha256_digest(contract.model_dump(mode="json"))` is not an identity. Use
`mission_control.domain.authoring.canonical.stable_json_digest` / `stable_json_dump` (set-sorted and
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

from tests.fixtures.digest_sites import scan_digest_sites, scan_unverifiable_dumps
from tests.fixtures.set_order import model_holds_set

CD = "mission_control.domain.authoring.contracts"
GR = "mission_control.domain.graph_runtime"
OE = "mission_control.domain.execution"
RC = "mission_control.domain.policies"
SC = "biotech_mission_adapters.domain.schema_context.contracts"
SG = "biotech_mission_adapters.domain.schema_grounding.contracts"
SW = "experiments.dynamic_research_swarm.contracts"


@dataclass(frozen=True)
class Audited:
    models: tuple[str, ...]
    reason: str


NO_SET = "no set-valued field is reachable from the dumped model, so the JSON dump is order-stable"
THIRD_PARTY = "third-party MCP `Tool` schema (JSON primitives only), not a BellLabs contract"
ANY_PAYLOAD = (
    f"{NO_SET}; `Any`/`dict[str, Any]` members are JSON payloads validated from JSON documents"
)


_AUDIT: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...], str], ...] = (
    (
        "src/mission_control/adapters/postgres/coordinator/workflow_result_repository.py",
        "PostgresWorkflowResultRepository.save",
        ("result.model_dump(mode='json')",),
        ("mission_control.domain.coordinator.launch.WorkflowResultRecord",),
        ANY_PAYLOAD,
    ),
    (
        "src/mission_control/application/execution/operations/journaled_operation_execution.py",
        "JournaledOperationExecutionCoordinator.acquire",
        ("claim.model_dump(mode='json')",),
        (f"{OE}.journal.OperationEffectClaim",),
        NO_SET,
    ),
    (
        "src/mission_control/application/execution/operations/operation_journal.py",
        "OperationJournalMutation.validate",
        ("self.claim.model_dump(mode='json')",),
        (f"{OE}.journal.OperationEffectClaim",),
        NO_SET,
    ),
    (
        "src/mission_control/application/execution/operations/operation_execution.py",
        "RunControlOperationAuthority._verify_bound_authority",
        ("configuration.workflow_workspace_contract.model_dump(mode='json')",),
        (f"{CD}.WorkflowWorkspaceContract",),
        NO_SET,
    ),
    (
        "src/mission_control/application/programs/goal_directed.py",
        "GoalDirectedOperationResultService.reconcile",
        ("observed.model_dump(mode='json')",),
        (f"{OE}.contracts.OperationWorkflowResult",),
        ANY_PAYLOAD,
    ),
    (
        "src/mission_control/application/programs/service.py",
        "RunControlLifecycleGateway.execute",
        ("item.model_dump(mode='json')",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "src/mission_control/application/programs/service.py",
        "StageGraphDecisionService.complete",
        ("item.model_dump(mode='json')",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/policies/reducer.py",
        "_evidence_frontier",
        ("item.model_dump(mode='json')",),
        (
            f"{RC}.contracts.AcceptedObligationEvidence",
            f"{RC}.contracts.AcceptedOutputEvidence",
        ),
        NO_SET,
    ),
    (
        "src/mission_control/domain/policies/reducer.py",
        "_terminal_outcome",
        ("item.model_dump(mode='json')",),
        (f"{RC}.contracts.AcceptedObligationEvidence",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/reference_research/service.py",
        "execute_reference_fixture",
        ("fixture.model_dump(mode='json')",),
        (
            "biotech_mission_adapters.domain.reference_research.contracts.QualiaFixtureInput",
            "biotech_mission_adapters.domain.reference_research.contracts.DaveFixtureInput",
        ),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/reference_research/service.py",
        "execute_reference_fixture",
        ("lease_request.model_dump(mode='json')",),
        (f"{GR}.kernel.ResourceLeaseRequest",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/reference_research/service.py",
        "prepare_reference_implementation",
        ("resources.model_dump(mode='json')",),
        (f"{GR}.definitions.ExecutionResourceEnvelopeV2",),
        NO_SET,
    ),
    (
        "src/mission_control/application/execution/run_control_repository.py",
        "FamilyAdmissionCommit.__post_init__",
        ("mutation.model_dump(mode='json', exclude={'decided_at'})",),
        (f"{RC}.family_admission.AtomicFamilyMutation",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/bootstrap/runners/web_research_coordinator_live.py",
        "_launch_proposal",
        ("ref.model_dump(mode='json')",),
        (f"{CD}.ExactDefinitionRef",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/bootstrap/runners/web_research_coordinator_live.py",
        "_run_mounted_mcp_planning",
        ("tool.model_dump(mode='json', exclude_none=True)",),
        (),
        THIRD_PARTY,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/adapters/infrastructure/web_research_runtime.py",
        "_tools_snapshot_digest",
        ("tool.model_dump(mode='json', exclude_none=True)",),
        (),
        THIRD_PARTY,
    ),
    (
        "src/mission_control/application/recovery/runtime_decisions.py",
        "DurableDecisionService.create_request",
        ("request.model_dump(mode='json', exclude={'request_digest'})",),
        (f"{GR}.kernel.DecisionRequest",),
        NO_SET,
    ),
    (
        "src/mission_control/application/recovery/runtime_lineage.py",
        "PersistedExecutionLineage.lineage_is_canonical_and_scope_bound",
        (
            "self.model_dump(mode='json', exclude={'lineage_digest', 'recorded_at', 'retain_until'})",
        ),
        ("mission_control.application.recovery.runtime_lineage.PersistedExecutionLineage",),
        NO_SET,
    ),
    (
        "src/mission_control/application/recovery/runtime_run_plan.py",
        "compile_run_plan",
        ("item.model_dump(mode='json')",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "src/mission_control/application/recovery/runtime_run_plan.py",
        "compile_run_plan_v3",
        ("item.model_dump(mode='json')",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "src/mission_control/application/recovery/runtime_run_plan.py",
        "compile_run_plan_v4",
        ("item.model_dump(mode='json')",),
        (f"{CD}.AliasBinding",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/graph_query.py",
        "intent_digest",
        ("intent.model_dump(mode='json')",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/schema_grounding_semantic_handlers.py",
        "SupportingGraphSemanticBindingProvider.prepare",
        ("intent.model_dump(mode='json')",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/supporting_graph_reconciliation.py",
        "SupportingGraphReconciliationWorkflow.run",
        ("intent.model_dump(mode='json')",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/supporting_graph_reconciliation.py",
        "_terminal_result",
        ("intent.model_dump(mode='json')",),
        (f"{SC}.QueryExecutionIntent",),
        ANY_PAYLOAD,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/supporting_graph_reconciliation.py",
        "_reconciliation_request_digest",
        ("evidence.model_dump(mode='json')",),
        (f"{SC}.GraphReconciliationEvidence",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/schema/schema_catalog_build.py",
        "_request_fingerprint",
        ("request.model_dump(mode='json', exclude={'requested_at'})",),
        (f"{SG}.SchemaCatalogBuildRequest",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/application/web_research/web_research_semantic_binding.py",
        "verify_web_research_operation_binding",
        (
            "profile_ref.model_dump(mode='json')",
            "runtime_profile_ref.model_dump(mode='json')",
            "workspace_template_ref.model_dump(mode='json')",
        ),
        (f"{CD}.ExactDefinitionRef",),
        NO_SET,
    ),
    (
        "src/mission_control/application/artifacts/workspace_materialization.py",
        "WorkspaceMaterializationService._append_revision",
        (
            "current.template_ref.model_dump(mode='json')",
            "entry.model_dump(mode='json')",
            "slot.model_dump(mode='json')",
        ),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "src/mission_control/application/artifacts/workspace_materialization.py",
        "WorkspaceMaterializationService._initial_manifest",
        (
            "entry.model_dump(mode='json')",
            "request.template_ref.model_dump(mode='json')",
            "slot.model_dump(mode='json')",
        ),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/execution/materialization.py",
        "workspace_manifest_digest",
        (
            "entry.model_dump(mode='json')",
            "manifest.template_ref.model_dump(mode='json')",
            "slot.model_dump(mode='json')",
        ),
        (f"{OE}.contracts.WorkspaceMaterializationManifest",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/coordinator/launch.py",
        "SemanticBindingPlan.plan_content_matches_digest",
        ("self.model_dump(mode='json', exclude={'plan_digest'})",),
        ("mission_control.domain.coordinator.launch.SemanticBindingPlan",),
        ANY_PAYLOAD,
    ),
    (
        "src/mission_control/domain/graph_runtime/contracts.py",
        "GraphExecutionSubmission.submission_digest_matches_intent",
        ("self.model_dump(mode='json', exclude={'request_digest'})",),
        (f"{GR}.contracts.GraphExecutionSubmission",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/graph_runtime/contracts.py",
        "InterventionBase.intervention_digest_matches_intent",
        ("self.model_dump(mode='json', exclude={'request_digest'})",),
        (f"{GR}.contracts.InterventionBase",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/execution/checkpoint_lineage.py",
        "CheckpointTransitionObservation.content_digest",
        ("self.model_dump(mode='json', exclude={'observed_at'})",),
        (f"{OE}.checkpoint_lineage.CheckpointTransitionObservation",),
        NO_SET,
    ),
    (
        "src/mission_control/domain/execution/journal.py",
        "OperationJournalSettlement.create",
        ("draft.model_dump(mode='json', exclude={'settlement_digest'})",),
        (f"{OE}.journal.OperationJournalSettlement",),
        ANY_PAYLOAD,
    ),
    (
        "src/mission_control/domain/execution/journal.py",
        "OperationJournalSettlement.terminal_shape_is_consistent",
        (
            "self.model_dump(mode='json', exclude={'settlement_digest', 'digest_version', 'released_usage'})",
            "self.model_dump(mode='json', exclude={'settlement_digest'})",
        ),
        (f"{OE}.journal.OperationJournalSettlement",),
        ANY_PAYLOAD,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/domain/schema_catalog/parser.py",
        "parse_physical_schema",
        ("value.model_dump(mode='json')",),
        ("biotech_mission_adapters.domain.schema_catalog.models.PhysicalSchemaCatalog",),
        NO_SET,
    ),
    (
        "integrations/biotech/src/biotech_mission_adapters/domain/schema_context/validation.py",
        "accept_selection",
        (
            "review.model_dump(mode='json')",
            "selection.model_dump(mode='json')",
        ),
        (
            f"{SC}.SchemaSelectionReview",
            f"{SC}.SchemaContextSelection",
        ),
        NO_SET,
    ),
    (
        "experiments/dynamic_research_swarm/evaluators.py",
        "evaluate_claim",
        ("gate.model_dump(mode='json')",),
        (f"{SW}.GateResult",),
        NO_SET,
    ),
    (
        "experiments/dynamic_research_swarm/repository.py",
        "SwarmEvidenceRepository.save_plan",
        ("plan.model_dump_json()",),
        (f"{SW}.MissionPlan",),
        NO_SET,
    ),
    (
        "experiments/dynamic_research_swarm/temporal_activities.py",
        "execute_swarm_stage",
        ("output.model_dump_json()",),
        (
            f"{SW}.SourceBundle",
            f"{SW}.MissionPlan",
            f"{SW}.ResearchUnitResult",
            f"{SW}.FinalSynthesis",
        ),
        NO_SET,
    ),
    (
        "src/mission_control/adapters/operations/conformance.py",
        "ConformanceSandbox.materialize",
        ("mount.model_dump(mode='json')",),
        (f"{OE}.contracts.WorkspaceMount",),
        NO_SET,
    ),
)

AUDITED_NOT_AFFECTED: dict[tuple[str, str, str], Audited] = {
    (path, function, call): Audited(models, reason)
    for path, function, calls, models, reason in _AUDIT
    for call in calls
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
            f"{site.path}:{site.line} {site.function}: {site.call}"
            for site in in_scope
            if site.key not in AUDITED_NOT_AFFECTED
        }
    )
    assert not unaudited, (
        "A JSON-mode dump of a contract feeds a digest. `model_dump(mode='json')` lists sets in "
        "per-process iteration order, so the digest is not an identity. Use "
        "`mission_control.domain.authoring.canonical.stable_json_digest(model)` (or `stable_json_dump`), "
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
            from mission_control.domain.authoring.canonical import sha256_digest, stable_json_digest

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

            def type_adapter(x):
                return sha256_digest(ADAPTER.dump_python(x, mode="json"))

            def jsonable(x):
                return sha256_digest(to_jsonable_python(x))

            def dynamic_mode(x, mode):
                return sha256_digest(x.model_dump(mode=mode))

            def kwargs_dump(x, **options):
                return sha256_digest(x.model_dump(**options))
            """
        ),
        encoding="utf-8",
    )
    found, _ = scan_digest_sites(root)
    assert sorted(site.function for site in found) == [
        "direct",
        "fingerprint_named",
        "jsonable",
        "multiline",
        "nested",
        "type_adapter",
        "via_variable",
    ]
    # Non-literal modes and **kwargs cannot be classified, so they are reported separately.
    assert sorted(site.function for site in scan_unverifiable_dumps(root)) == [
        "dynamic_mode",
        "kwargs_dump",
    ]


def test_no_unverifiable_json_dumps_in_app() -> None:
    """A dump with a computed `mode=` or `**kwargs` could hide a JSON-mode digest input."""

    unverifiable = [f"{site.path}:{site.line} {site.call}" for site in scan_unverifiable_dumps()]
    assert not unverifiable, "use a literal mode= and explicit keywords:\n" + "\n".join(
        unverifiable
    )


def _all_app_models() -> list[type[BaseModel]]:
    import pkgutil

    packages = ("mission_control", "biotech_mission_adapters", "experiments")
    for name in packages:
        package = importlib.import_module(name)
        for module in pkgutil.walk_packages(package.__path__, name + "."):
            if module.name.endswith(".__main__"):
                continue  # executable CLI modules parse arguments when executed
            try:
                importlib.import_module(module.name)
            except Exception:
                continue
    found: list[type[BaseModel]] = []

    def collect(model: type[BaseModel]) -> None:
        for subclass in model.__subclasses__():
            found.append(subclass)
            collect(subclass)

    collect(BaseModel)
    models = [model for model in found if model.__module__.startswith(packages)]
    assert models, "Digest guard found no project-owned contract models"
    return models


def _declared_models(annotation: object) -> list[type[BaseModel]]:
    import typing

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [model for arg in typing.get_args(annotation) for model in _declared_models(arg)]


# Runtime-key hierarchy (`mission_control.domain.graph_runtime.identities`): fields declared as a base key whose
# subclasses add fields. `stable_json_dump` follows pydantic's declared-type rule for them; the
# runtime-type walk of `_normalize`/`contract_fingerprint` would include the subclass fields. None
# of these contracts is an input of `contract_fingerprint` or `stable_json_dump` today.
KNOWN_EXTENDED_DECLARED_MODELS = {
    ("BellLabsRunKey", "AgentThreadKey"),
    ("BellLabsRunKey", "ExecutionEpochKey"),
    ("BellLabsRunKey", "GoalHandoffCheckpointKey"),
    ("BellLabsRunKey", "RuntimeTransportAttemptKey"),
    ("BellLabsRunKey", "SemanticOperationAttemptKey"),
    ("ExecutionEpochKey", "AgentThreadKey"),
    ("ExecutionEpochKey", "RuntimeTransportAttemptKey"),
}


def test_contract_fields_declaring_extended_base_models_are_pinned() -> None:
    """`_normalize` walks runtime types while pydantic dumps by the declared type.

    They differ only when a subclass that adds fields sits in a field declared as its base model.
    Every such (declared model, subclass) pair is pinned; a new one fails here and must be
    reviewed against `contract_fingerprint` and `stable_json_dump` before it is accepted.
    """

    pairs: set[tuple[str, str]] = set()
    for model in _all_app_models():
        for field in model.model_fields.values():
            for declared in _declared_models(field.annotation):
                for subclass in _subclasses(declared):
                    if set(subclass.model_fields) - set(declared.model_fields):
                        pairs.add((declared.__name__, subclass.__name__))
    assert pairs == KNOWN_EXTENDED_DECLARED_MODELS


def _subclasses(model: type[BaseModel]) -> list[type[BaseModel]]:
    found: list[type[BaseModel]] = []
    for subclass in model.__subclasses__():
        found.append(subclass)
        found.extend(_subclasses(subclass))
    return found
