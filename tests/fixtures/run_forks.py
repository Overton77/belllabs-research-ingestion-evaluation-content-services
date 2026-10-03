"""RRM-006 fork fixtures: family heads, journal views and an in-memory fork composition.

Technical inputs only: StageGraph stages `draft` and `review`, GoalDirected iteration 1.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from mission_control.application.execution.inspection import InMemoryInspectionReadRepository
from mission_control.application.execution.operations.checkpoint_lineage import (
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.run_control_repository import (
    InMemoryRunControlRepository,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.recovery.run_forks import (
    FamilyHeadRecord,
    ForkCommand,
    ForkPatchPolicyRegistry,
    ForkSourceFacts,
    InMemoryForkMaterializationStore,
    InMemoryRunSnapshotRepository,
    LedgerPendingCommands,
    LineageAsyncChildForkClassifier,
    LinkedRunRecord,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    RunSnapshotService,
    SemanticForkService,
)
from mission_control.application.recovery.runtime_lineage import InMemoryExecutionLineageRepository
from mission_control.application.recovery.runtime_recovery import (
    InMemoryForkRepository,
    RuntimeForkService,
)
from mission_control.application.subordinates.service import InMemoryAsyncSubagentDetailRepository
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import AsyncSubagentExecution
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.errors import RunControlNotFound
from mission_control.domain.policies.forks import (
    CognitiveSeed,
    ForkPatchChange,
    ForkPatchPolicy,
    PatchablePath,
    RunSnapshotManifest,
    stage_objective_path,
)
from mission_control.domain.policies.inspection import (
    JournalClaimInspection,
    JournalSettlementSummary,
)
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, NOW, WORKFLOW_DIGEST

FORK_PERMISSIONS = ALL_PERMISSIONS | {
    "workflow_run.read",
    "workflow_run.snapshot",
    "workflow_run.fork",
}


def fork_actor(*, permissions: frozenset[str] = FORK_PERMISSIONS) -> ActorContext:
    return ActorContext(
        actor_id="operator",
        authority_refs=frozenset({"authority:lifecycle"}),
        permissions=permissions,
    )


def stage_policy(*, workflow_digest: str = WORKFLOW_DIGEST) -> ForkPatchPolicy:
    """A Workflow Type that declares the `review` stage objective patchable."""

    del workflow_digest
    return ForkPatchPolicy(
        policy_id="fork-patch-policy:rrm-006-technical-stagegraph",
        family="stage_graph",
        patchable=(
            PatchablePath(path=stage_objective_path("review"), invalidates=("review",)),
            PatchablePath(path="input_manifest", invalidates=("*",), value_kind="input_manifest"),
        ),
    )


def stage_candidate(stage_id: str) -> dict[str, Any]:
    return {
        "stage_id": stage_id,
        "mapped_instance_presence": 0,
        "mapped_instance_id": "NO_MAPPED_INSTANCE",
        "workflow_cycle_ordinal": 0,
        "stage_cycle_ordinal": 0,
        "operation_slot_id": "default",
    }


def stagegraph_head(
    *,
    stages: Mapping[str, str],
    liabilities: tuple[str, ...] = (),
    closed_liabilities: tuple[str, ...] = (),
    accepted_results: int = 1,
    mutation_id: str = "result-head",
    family_version: int = 4,
) -> FamilyHeadRecord:
    projection = {
        "workflow_cycle_ordinal": 0,
        "stages": {
            f"stage:{stage}:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default": {
                "candidate": stage_candidate(stage),
                "status": status,
            }
            for stage, status in stages.items()
        },
        "producer_liabilities": {
            **{key: {"semantic_attempt_id": key} for key in liabilities},
            **{
                key: {
                    "semantic_attempt_id": key,
                    "child_closed_or_quiesced": True,
                    "reservations_and_usage_settled": True,
                    "effects_settled": True,
                    "cancellation_reconciled": True,
                    "result_decision": "admit",
                }
                for key in closed_liabilities
            },
        },
        "accepted_results": [{"accepted_at_order": index} for index in range(accepted_results)],
    }
    mutation = {
        "family_kind": "stagegraph",
        "mutation_kind": "decision_committed",
        "mutation_id": mutation_id,
        "decision_kind": "result_decided",
        "next_projection_digest": sha256_digest(projection),
        "decision_payload": {"projection": projection},
    }
    return FamilyHeadRecord(
        family_kind="stagegraph",
        family_version=family_version,
        mutation_fingerprint=sha256_digest(mutation),
        mutation=mutation,
    )


def goal_head(
    *,
    iteration: int,
    revision_id: str,
    role: str,
    handoff_ref: str | None = None,
) -> FamilyHeadRecord:
    mutation = {
        "family_kind": "goal_directed",
        "mutation_kind": "decision",
        "mutation_id": f"goal.{iteration}.{role}.1",
        "goal_revision_id": revision_id,
        "goal_revision_digest": sha256_digest(revision_id),
        "goal_iteration": iteration,
        "operation_role": role,
        "handoff_ref": handoff_ref,
    }
    return FamilyHeadRecord(
        family_kind="goal_directed",
        family_version=2 * iteration,
        mutation_fingerprint=sha256_digest(mutation),
        mutation=mutation,
    )


@dataclass
class FakeForkSourceReader:
    """Family heads and linked runs supplied by the test; the run version is authoritative."""

    run_control: RunControlService
    heads: dict[str, tuple[FamilyHeadRecord, ...]] = field(default_factory=dict)
    linked: dict[str, tuple[LinkedRunRecord, ...]] = field(default_factory=dict)
    version_skew: int = 0

    async def read_fork_source(self, request_scope: str, run_id: str) -> ForkSourceFacts | None:
        try:
            run = await self.run_control.get_run(request_scope, run_id)
        except RunControlNotFound:
            return None
        return ForkSourceFacts(
            run_version=run.version + self.version_skew,
            family_heads=self.heads.get(run_id, ()),
            linked_runs=self.linked.get(run_id, ()),
        )


def journal_view(
    journal: MemoryOperationJournal, lineage: InMemoryCheckpointLineageRepository
) -> dict[str, tuple[JournalClaimInspection, ...]]:
    """The inspection view of the in-memory operation journal, keyed by unit."""

    by_binding: dict[str, str] = {
        record.binding_id: record.unit_key for record in lineage.generations.values()
    }
    view: dict[str, list[JournalClaimInspection]] = {}
    for claim in journal.claims.values():
        unit_key = by_binding.get(claim.semantic_binding_id)
        if unit_key is None:
            continue
        settlement = journal.settlements.get(claim.effect_claim_id)
        view.setdefault(unit_key, []).append(
            JournalClaimInspection(
                effect_claim_id=claim.effect_claim_id,
                semantic_binding_id=claim.semantic_binding_id,
                semantic_binding_digest=claim.semantic_binding_digest,
                status="settled" if settlement is not None else "claimed",
                claimed_at=claim.claimed_at,
                settlements=(
                    (
                        JournalSettlementSummary(
                            settlement_id=settlement.settlement_id,
                            settlement_revision=settlement.settlement_revision,
                            status=settlement.status,
                            result_manifest_ref=settlement.result_manifest_ref,
                            result_manifest_digest=settlement.result_manifest_digest,
                            usage=settlement.usage,
                            settled_at=settlement.settled_at,
                        ),
                    )
                    if settlement is not None
                    else ()
                ),
            )
        )
    return {key: tuple(items) for key, items in view.items()}


class DetailChildLineage:
    """RRM-013 lifecycle data for the in-memory stack: the async-child detail repository's
    executions (`AsyncSubagentExecution`), filtered by parent run."""

    def __init__(self, details: InMemoryAsyncSubagentDetailRepository | None = None) -> None:
        self.details = details or InMemoryAsyncSubagentDetailRepository()

    async def list_children(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[AsyncSubagentExecution, ...]:
        return tuple(
            execution
            for (scope, _child), execution in sorted(self.details.executions.items())
            if scope == request_scope and execution.parent_run_id == parent_run_id
        )


@dataclass
class StaticPendingCommands:
    """Stands in for RRM-007's command ledger: receipts not yet applied, per run."""

    pending: dict[str, tuple[str, ...]] = field(default_factory=dict)

    async def unapplied_command_receipts(self, request_scope: str, run_id: str) -> tuple[str, ...]:
        del request_scope
        return self.pending.get(run_id, ())


@dataclass
class InMemoryForks:
    snapshots: RunSnapshotService
    forks: SemanticForkService
    repository: InMemoryForkRepository
    store: InMemoryForkMaterializationStore
    lineage: InMemoryExecutionLineageRepository
    snapshot_store: InMemoryRunSnapshotRepository
    sources: FakeForkSourceReader
    children: DetailChildLineage


def compose_in_memory_forks(
    run_control: RunControlService,
    repository: InMemoryRunControlRepository,
    reads: Any,
    sources: FakeForkSourceReader,
    *,
    policies: ForkPatchPolicyRegistry | None = None,
    async_children: Any = None,
    children: DetailChildLineage | None = None,
    commands: Any = None,
) -> InMemoryForks:
    snapshot_store = InMemoryRunSnapshotRepository()
    lineage = InMemoryExecutionLineageRepository()
    store = InMemoryForkMaterializationStore(lineage, transitions=run_control)
    child_lineage = children or DetailChildLineage()
    fork_repository = InMemoryForkRepository()
    saga = RuntimeForkService(
        repository=fork_repository,
        authority=RunControlForkAuthority(run_control, repository),
        materializer=RecordingForkMaterializer(store, retention=lambda at: at + timedelta(days=90)),
    )
    return InMemoryForks(
        snapshots=RunSnapshotService(
            reads=reads,
            sources=sources,
            snapshots=snapshot_store,
            async_children=async_children or LineageAsyncChildForkClassifier(child_lineage),
            commands=commands or LedgerPendingCommands(run_control),
            clock=lambda: NOW,
        ),
        forks=SemanticForkService(snapshots=snapshot_store, saga=saga, policies=policies),
        repository=fork_repository,
        store=store,
        lineage=lineage,
        snapshot_store=snapshot_store,
        sources=sources,
        children=child_lineage,
    )


@dataclass
class LiveInspectionReads:
    """In-memory inspection reads with the journal view rebuilt on every read."""

    repository: InMemoryRunControlRepository
    lineage: InMemoryCheckpointLineageRepository
    journal: MemoryOperationJournal | None = None
    async_children: Mapping[str, Any] | None = None

    def _reads(self) -> InMemoryInspectionReadRepository:
        return InMemoryInspectionReadRepository(
            self.repository,
            self.lineage,
            journal=(
                journal_view(self.journal, self.lineage) if self.journal is not None else None
            ),
            async_children=self.async_children,
            clock=lambda: NOW,
        )

    async def list_runs(self, *args: Any, **kwargs: Any) -> Any:
        return await self._reads().list_runs(*args, **kwargs)

    async def read_run(self, *args: Any, **kwargs: Any) -> Any:
        return await self._reads().read_run(*args, **kwargs)


def inspection_reads(
    repository: InMemoryRunControlRepository,
    lineage: InMemoryCheckpointLineageRepository,
    journal: MemoryOperationJournal | None = None,
    *,
    async_children: Mapping[str, Any] | None = None,
) -> LiveInspectionReads:
    return LiveInspectionReads(repository, lineage, journal, async_children)


def fork_command(
    snapshot: RunSnapshotManifest,
    *,
    request_id: str = "fork-1",
    changes: tuple[ForkPatchChange, ...] = (),
    invalidation_frontier: tuple[str, ...] = (),
    cognitive_seed: CognitiveSeed | None = None,
    baseline_reservations: dict[str, int] | None = None,
    actor: ActorContext | None = None,
    snapshot_digest: str | None = None,
    requested_at_offset: timedelta = timedelta(0),
) -> ForkCommand:
    return ForkCommand(
        request_scope=snapshot.request_scope,
        source_run_id=snapshot.source_run_id,
        request_id=request_id,
        idempotency_key=f"idempotency:{request_id}",
        snapshot_id=snapshot.snapshot_id,
        snapshot_digest=snapshot_digest or snapshot.snapshot_digest,
        changes=changes,
        invalidation_frontier=invalidation_frontier,
        cognitive_seed=cognitive_seed,
        baseline_reservations=(
            {"tokens.total": 20} if baseline_reservations is None else baseline_reservations
        ),
        sponsorship_ref="sponsorship:test",
        approval_refs=("approval:test",),
        actor=actor or fork_actor(),
        reason="technical fork",
        requested_at=NOW + timedelta(hours=1) + requested_at_offset,
    )


def review_objective_patch(text: str = "Review the draft again, strictly.") -> ForkPatchChange:
    return ForkPatchChange(path=stage_objective_path("review"), value=text)


def technical_snapshot(
    run_id: str,
    *,
    request_scope: str = "tenant-1",
    projection_version: int = 2,
    candidates: tuple[Any, ...] = (),
) -> RunSnapshotManifest:
    """A minimal, valid StageGraph snapshot of `run_id` (storage and saga suites)."""

    from mission_control.domain.policies.forks import (
        BudgetFrontier,
        FamilyPosition,
        snapshot_id_for,
    )
    from tests.unit.run_control.test_run_control import request as run_request

    admitted = run_request(request_scope=request_scope)
    boundary_ref = "family-head:stagegraph:result-head"
    return RunSnapshotManifest.create(
        snapshot_id=snapshot_id_for(request_scope, run_id, projection_version, boundary_ref),
        request_scope=request_scope,
        source_run_id=run_id,
        execution_epoch=1,
        family="stage_graph",
        projection_version=projection_version,
        run_phase="active",
        effective_configuration_digest=admitted.effective_configuration_digest,
        workflow_type_ref=admitted.workflow_type_ref,
        input_manifest=admitted.input_manifest,
        obligation_revision="obligations:1",
        evidence_frontier_digest=sha256_digest("frontier"),
        boundary_kind="stage_settled",
        boundary_ref=boundary_ref,
        family_position=FamilyPosition(
            family="stage_graph",
            family_kind="stagegraph",
            family_version=4,
            head_mutation_id="result-head",
            head_mutation_kind="decision_committed",
            head_mutation_fingerprint=sha256_digest("head"),
            accepted_projection_digest=sha256_digest("projection"),
            workflow_cycle_ordinal=0,
            accepted_stage_ids=("draft",),
        ),
        reuse_candidates=candidates,
        budget_frontier=BudgetFrontier(
            account_id=f"budget-account:{run_id}",
            limits=admitted.budget_envelope.dimensions,
            reservation_ids=("baseline",),
        ),
        taken_at=NOW,
    )
