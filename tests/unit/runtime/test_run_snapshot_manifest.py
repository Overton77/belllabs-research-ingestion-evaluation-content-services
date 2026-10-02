"""RRM-006: safe macro snapshots, patch validation, the reuse frontier and fork admission.

Units are settled through the real operation boundary (real `create_deep_agent` graph with
the deterministic scripted model, the journaled coordinator and checkpoint lineage over
in-memory run control); the snapshot is read through the inspection read repository
(REQ-CP-EXEC-016: authority only, never workflow Queries).
"""

from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from app.application.runtime.run_forks import (
    ForkPatchPolicyRegistry,
    ForkSnapshotNotFound,
    LineageAsyncChildForkClassifier,
    LinkedRunRecord,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import RuntimeUnitIdentity
from app.domain.run_control.contracts import (
    CancelAction,
    ClaimEffectAction,
    CommandStatus,
    ReserveBudgetAction,
    RunPhase,
    RunRequest,
    StartAction,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.domain.run_control.forks import (
    INVALIDATE_ALL,
    ForkPatchChange,
    ForkPatchPolicy,
    ForkRejected,
    PatchablePath,
    RunForkPatch,
    RunForkRequest,
    RunSnapshotManifest,
    compute_reuse_decisions,
    default_patch_policy,
    derived_unit_identity,
    fork_request_fingerprint,
    required_invalidation_frontier,
    run_snapshot_digest,
)
from app.domain.run_control.inspection import AsyncChildInspection
from tests.fixtures.checkpoint_lineage import goal_unit, in_doubt_incident
from tests.fixtures.checkpoint_recovery import (
    RecoveryHarness,
    recovery_harness,
    stage_recovery_unit,
)
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    InMemoryForks,
    StaticPendingCommands,
    compose_in_memory_forks,
    fork_actor,
    fork_command,
    goal_head,
    inspection_reads,
    review_objective_patch,
    stage_policy,
    stagegraph_head,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

SCOPE = "tenant-1"


async def _settle(harness: RecoveryHarness, unit: RuntimeUnitIdentity) -> None:
    result = await harness.run(await harness.request(unit))
    assert result.status == "completed"


async def _world(
    *,
    stages: dict[str, str] | None = None,
    settle: tuple[str, ...] = ("draft",),
    async_children: tuple[AsyncChildInspection, ...] = (),
) -> tuple[RecoveryHarness, InMemoryForks, dict[str, RuntimeUnitIdentity]]:
    harness = await recovery_harness()
    units = {name: stage_recovery_unit(harness.run_id, name) for name in settle}
    for unit in units.values():
        await _settle(harness, unit)
    sources = FakeForkSourceReader(
        harness.run_control,
        heads={
            harness.run_id: (
                stagegraph_head(stages=stages or {"draft": "completed", "review": "blocked"}),
            )
        },
    )
    policies = ForkPatchPolicyRegistry()
    policies.register(WORKFLOW_DIGEST, stage_policy())
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(
            harness.repository,
            harness.lineage,
            harness.journal,
            async_children={harness.run_id: async_children},
        ),
        sources,
        policies=policies,
    )
    return harness, forks, units


@pytest.mark.asyncio
async def test_snapshot_binds_authority_at_a_settled_stage_boundary() -> None:
    harness, forks, units = await _world()
    run = await harness.run_control.get_run(SCOPE, harness.run_id)
    budget = await harness.run_control.get_budget(SCOPE, harness.run_id)

    snapshot = await forks.snapshots.take(SCOPE, harness.run_id, expected_run_version=run.version)

    draft = units["draft"]
    assert snapshot.schema_version == "belllabs.run-snapshot.v1"
    assert snapshot.boundary_kind == "stage_settled"
    assert snapshot.family == "stage_graph"
    assert snapshot.projection_version == run.version
    assert snapshot.run_phase == "active"
    assert snapshot.effective_configuration_digest == run.effective_configuration_digest
    assert snapshot.workflow_type_ref == run.workflow_type_ref
    assert snapshot.input_manifest == run.input_manifest
    assert snapshot.evidence_frontier_digest == run.evidence_frontier_digest
    assert snapshot.family_position.family_version == 4
    assert snapshot.family_position.accepted_stage_ids == ("draft",)
    assert snapshot.family_position.accepted_projection_digest is not None
    # The settled draft is the one reuse candidate, by immutable refs only.
    [candidate] = snapshot.reuse_candidates
    generation = harness.lineage.generations[(SCOPE, draft.unit_key, 1)]
    result = harness.lineage.results[(SCOPE, draft.unit_key, 1)]
    assert candidate.unit == draft
    assert candidate.binding_id == generation.binding_id
    assert candidate.binding_digest == generation.binding_digest
    assert candidate.state_schema_digest == generation.state_schema_digest
    assert candidate.settlement_id == result.settlement_id
    assert candidate.result_manifest_ref == result.result_manifest_ref
    assert candidate.result_manifest_digest == result.result_manifest_digest
    assert candidate.result_checkpoint is not None
    assert candidate.result_checkpoint.thread_id.endswith(f"{draft.unit_key}/gen/1")
    assert {item.kind for item in snapshot.accepted_evidence} == {"operation_settlement"}
    assert snapshot.budget_frontier.limits == budget.limits
    assert snapshot.budget_frontier.reservation_ids == ("baseline",)
    assert snapshot.effect_frontier and all(item.settled for item in snapshot.effect_frontier)
    assert snapshot.async_children == () and snapshot.linked_runs == ()
    assert snapshot.pending_commands == ()
    # Content-addressed and immutable: the same version yields the same snapshot.
    assert snapshot.snapshot_digest == run_snapshot_digest(snapshot)
    again = await forks.snapshots.take(SCOPE, harness.run_id)
    assert again == snapshot
    tampered = snapshot.model_dump(mode="python")
    tampered["budget_frontier"]["consumed"] = {"tokens.total": 1}
    with pytest.raises(ValidationError, match="digest does not match"):
        RunSnapshotManifest.model_validate(tampered)
    assert await forks.snapshots.get(SCOPE, snapshot.snapshot_id) == snapshot
    with pytest.raises(ForkSnapshotNotFound):
        await forks.snapshots.get("tenant-2", snapshot.snapshot_id)


@pytest.mark.asyncio
async def test_snapshot_reads_authority_without_mutating_it() -> None:
    harness, forks, _units = await _world()
    before = deepcopy(harness.repository.__dict__["_runs"])
    budgets = deepcopy(harness.repository.__dict__["_budgets"])
    effects = deepcopy(harness.repository.__dict__["_effects"])
    lineage = deepcopy(harness.lineage.results)

    await forks.snapshots.take(SCOPE, harness.run_id)

    assert harness.repository.__dict__["_runs"] == before
    assert harness.repository.__dict__["_budgets"] == budgets
    assert harness.repository.__dict__["_effects"] == effects
    assert harness.lineage.results == lineage


async def _reject(forks: InMemoryForks, run_id: str) -> ForkRejected:
    with pytest.raises(ForkRejected) as caught:
        await forks.snapshots.take(SCOPE, run_id)
    return caught.value


@pytest.mark.asyncio
async def test_open_reservation_or_unsettled_effect_is_not_quiescent() -> None:
    harness, forks, _units = await _world()
    run = await harness.run_control.get_run(SCOPE, harness.run_id)
    reserved = await harness.run_control.execute(
        command(
            harness.run_id,
            run.version,
            "admitted-but-unclaimed",
            ReserveBudgetAction(reservation_id="reservation:review", amounts={"tokens.total": 5}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    claimed = await harness.run_control.execute(
        command(
            harness.run_id,
            reserved.resulting_run_version,
            "claim-tool-effect",
            ClaimEffectAction(
                effect_id="tool-effect:send",
                effect_kind="tool",
                operation_ref="binding:review",
                provider_idempotency_key="send:1",
                reservation_id="reservation:review",
            ),
        )
    )
    assert claimed.status == CommandStatus.ACCEPTED

    rejected = await _reject(forks, harness.run_id)

    assert rejected.code == "snapshot_not_quiescent"
    assert "budget_reservation_open:reservation:review" in rejected.reasons
    assert "effect_claim_unsettled:tool-effect:send" in rejected.reasons


@pytest.mark.asyncio
async def test_unsettled_or_in_doubt_units_are_not_quiescent_and_never_reusable() -> None:
    harness, forks, _units = await _world()
    review = stage_recovery_unit(harness.run_id, "review")
    request = await harness.request(review)
    # An attempt was observed (the operation is in flight) but nothing settled.
    await harness.lineage.record_attempt(
        unit=review,
        execution_generation=1,
        attempt=harness.attempt(request),
        binding_id=f"binding:{review.unit_key}:1",
        binding_digest="sha256:" + "b" * 64,
        namespace=None,
        dispatching=True,
        observed_at=harness.clock(),
    )
    other = stage_recovery_unit(harness.run_id, "other")
    await harness.lineage.record_attempt(
        unit=other,
        execution_generation=1,
        attempt=harness.attempt(await harness.request(other)),
        binding_id=f"binding:{other.unit_key}:1",
        binding_digest="sha256:" + "b" * 64,
        namespace=None,
        dispatching=True,
        observed_at=harness.clock(),
    )
    await harness.lineage.open_incident(
        in_doubt_incident(other).model_copy(update={"namespace": None, "candidates": ()})
    )

    rejected = await _reject(forks, harness.run_id)

    assert rejected.code == "snapshot_not_quiescent"
    assert f"unit_unsettled:{review.unit_key}" in rejected.reasons
    assert f"unit_in_doubt:{other.unit_key}" in rejected.reasons
    assert "budget_reservation_open:reservation:" + review.unit_key in rejected.reasons


def _child_service(details: Any) -> Any:
    from datetime import timedelta as delta

    from app.application.async_subagents.service import (
        AsyncSubagentService,
        InMemoryAsyncSubagentAuthority,
    )
    from app.integrations.agents.deep_agents.async_subagents import (
        DeepAgentsAsyncSubagentAdapter,
    )
    from tests.acceptance.control_plane.test_wp_cp_045 import NOW as CHILD_NOW

    return AsyncSubagentService(
        details,
        InMemoryAsyncSubagentAuthority(),
        DeepAgentsAsyncSubagentAdapter(
            now=lambda: CHILD_NOW,
            secrets={"environment:AGENT_SERVER_TOKEN": "offline-token"},
            request_scope=SCOPE,
        ),
        allow_new_spawns=True,
        submitter_identity="rrm006-worker",
        submission_lease=delta(seconds=30),
        now=lambda: CHILD_NOW,
    )


@pytest.mark.asyncio
async def test_active_async_child_from_rrm013_lifecycle_data_prohibits_the_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-001 §8 #6 / EXEC-016 with RRM-013's real service, lifecycle data and classifier."""

    from tests.acceptance.control_plane.test_wp_cp_045 import SERVED
    from tests.acceptance.control_plane.test_wp_cp_045 import request as spawn_request
    from tests.fixtures.fake_agent_protocol import FakeAgentProtocolClient, install

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    harness, forks, _units = await _world()
    service = _child_service(forks.children.details)
    running = await service.spawn(
        spawn_request().model_copy(update={"request_scope": SCOPE, "parent_run_id": harness.run_id})
    )
    assert running.lifecycle == "running"
    forks.sources.linked[harness.run_id] = (
        LinkedRunRecord(link_id="link-a", child_run_id="child-a", terminal_status=None),
        LinkedRunRecord(link_id="link-b", child_run_id="child-b", terminal_status="completed"),
    )

    rejected = await _reject(forks, harness.run_id)

    assert rejected.code == "snapshot_not_quiescent"
    assert f"async_child_active:{running.child_execution_id}" in rejected.reasons
    assert "linked_run_active:child-a" in rejected.reasons
    assert "linked_run_active:child-b" not in rejected.reasons

    # Once the child completes and the parent reconciles it, it no longer blocks.
    forks.sources.linked[harness.run_id] = ()
    client.complete(running.child_execution_id, "done")
    completed = await service.reconcile(SCOPE, running.child_execution_id)
    assert completed.lifecycle == "completed"
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    assert [(item.child_execution_id, item.disposition) for item in snapshot.async_children] == [
        (running.child_execution_id, "terminal")
    ]


@pytest.mark.asyncio
async def test_child_missing_from_the_lineage_is_classified_active() -> None:
    harness, forks, _units = await _world()
    authority_only = AsyncChildInspection(
        child_execution_id="async-child-unknown",
        parent_operation_id="binding:draft",
        link_id="link-1",
        contract_id="contract-1",
        binding_digest="sha256:" + "d" * 64,
        execution_generation=1,
        dependency_class="nonblocking",
        lifecycle="completed",
    )
    dispositions = await LineageAsyncChildForkClassifier(
        forks.children
    ).classify_async_children_for_fork(SCOPE, harness.run_id, (authority_only,))
    assert [(item.child_execution_id, item.disposition) for item in dispositions] == [
        ("async-child-unknown", "active")
    ]


@pytest.mark.asyncio
async def test_unapplied_command_receipt_makes_the_snapshot_not_quiescent() -> None:
    harness, forks, _units = await _world()
    commands = StaticPendingCommands({harness.run_id: ("receipt:pause-1",)})
    gated = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        forks.sources,
        commands=commands,
    )

    rejected = await _reject(gated, harness.run_id)
    assert rejected.code == "snapshot_not_quiescent"
    assert rejected.reasons == ("command_unapplied:receipt:pause-1",)

    commands.pending[harness.run_id] = ()
    assert (await gated.snapshots.take(SCOPE, harness.run_id)).pending_commands == ()


@pytest.mark.asyncio
async def test_rrm007_ledger_pending_command_blocks_the_snapshot_and_is_never_copied() -> None:
    """RRM-007's durable ledger: an accepted, unapplied pause (family execution target bound)
    makes the snapshot not quiescent; once the boundary applies it, the snapshot is taken,
    and the fork copies neither the command nor its receipts."""

    from tests.unit.run_control.test_boundary_commands import (
        TARGET,
        apply,
        boundary_command,
        pause,
    )

    harness, forks, _units = await _world()
    run_control = harness.run_control
    admitted = await run_control.admit(run_request(request_id="rrm006-ledger"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    started = await run_control.execute(
        command(run_id, 1, "ledger-start", StartAction(execution_target=TARGET))
    )
    assert started.status == CommandStatus.ACCEPTED
    forks.sources.heads[run_id] = (stagegraph_head(stages={"draft": "completed"}),)
    accepted = await run_control.execute(command(run_id, 2, "pause", pause()))
    assert accepted.reason_code == "accepted_pending_application"

    rejected = await _reject(forks, run_id)
    assert rejected.code == "snapshot_not_quiescent"
    assert "command_unapplied:operator:pause:accepted" in rejected.reasons

    applied = await run_control.execute(
        boundary_command(run_id, 2, "apply:pause", apply("pause", pause()))
    )
    assert applied.status == CommandStatus.ACCEPTED
    snapshot = await forks.snapshots.take(SCOPE, run_id)
    assert snapshot.pending_commands == ()
    receipt = await forks.forks.fork(fork_command(snapshot, request_id="fork-ledger"))
    derived = await run_control.get_run(SCOPE, receipt.target_run_id)
    assert await run_control.list_boundary_commands(SCOPE, receipt.target_run_id) == ()
    assert derived.execution_target is None and derived.active_pauses == ()
    assert [
        item.command.command_id for item in await run_control.list_boundary_commands(SCOPE, run_id)
    ] == ["pause"]


@pytest.mark.asyncio
async def test_reserved_stage_alone_is_active_work() -> None:
    harness, forks, _units = await _world()
    forks.sources.heads[harness.run_id] = (
        stagegraph_head(stages={"draft": "completed", "review": "reserved"}),
    )
    rejected = await _reject(forks, harness.run_id)
    assert rejected.code == "snapshot_not_quiescent"
    assert rejected.reasons == (
        "stage_active:stage:review:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default",
    )
    for status in ("waiting", "paused"):
        forks.sources.heads[harness.run_id] = (
            stagegraph_head(stages={"draft": "completed", "review": status}),
        )
        assert (await _reject(forks, harness.run_id)).reasons[0].startswith("stage_active:")
    # A stage held back by a declared wait has no admitted work: it is not active.
    forks.sources.heads[harness.run_id] = (
        stagegraph_head(stages={"draft": "completed", "review": "blocked"}),
    )
    assert (await forks.snapshots.take(SCOPE, harness.run_id)).boundary_kind == "stage_settled"


@pytest.mark.asyncio
async def test_same_version_and_boundary_with_another_digest_is_a_digest_conflict() -> None:
    harness, forks, _units = await _world()
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    different = RunSnapshotManifest.create(
        **{
            **snapshot.model_dump(mode="python", exclude={"snapshot_digest"}),
            "obligation_revision": "obligations:other",
        }
    )
    assert different.snapshot_id == snapshot.snapshot_id
    with pytest.raises(ForkRejected) as caught:
        await forks.snapshot_store.put(different)
    assert caught.value.code == "snapshot_digest_conflict"
    assert caught.value.reasons == (snapshot.snapshot_digest, different.snapshot_digest)


@pytest.mark.asyncio
async def test_open_stage_liability_cancelling_run_and_unsupported_boundaries() -> None:
    harness, forks, _units = await _world(stages={"draft": "completed", "review": "running"})
    forks.sources.heads[harness.run_id] = (
        stagegraph_head(
            stages={"draft": "completed", "review": "running"}, liabilities=("review-attempt",)
        ),
    )
    rejected = await _reject(forks, harness.run_id)
    assert "stage_liability_open:review-attempt" in rejected.reasons
    # A decided liability stays in the projection, closed; it does not block the boundary.
    forks.sources.heads[harness.run_id] = (
        stagegraph_head(stages={"draft": "completed"}, closed_liabilities=("draft-attempt",)),
    )
    assert (await forks.snapshots.take(SCOPE, harness.run_id)).boundary_kind == "stage_settled"
    forks.sources.heads[harness.run_id] = (
        stagegraph_head(
            stages={"draft": "completed", "review": "running"}, liabilities=("review-attempt",)
        ),
    )
    assert any(reason.startswith("stage_active:stage:review") for reason in rejected.reasons)

    forks.sources.heads[harness.run_id] = ()
    unsupported = await _reject(forks, harness.run_id)
    assert unsupported.code == "unsupported_boundary"

    forks.sources.heads[harness.run_id] = (
        stagegraph_head(stages={"draft": "running"}, accepted_results=0),
    )
    assert (await _reject(forks, harness.run_id)).code == "unsupported_boundary"

    forks.sources.heads[harness.run_id] = (stagegraph_head(stages={"draft": "completed"}),)
    run = await harness.run_control.get_run(SCOPE, harness.run_id)
    cancelled = await harness.run_control.execute(
        command(harness.run_id, run.version, "cancel-source", CancelAction())
    )
    assert cancelled.status == CommandStatus.ACCEPTED
    cancelling = await _reject(forks, harness.run_id)
    assert cancelling.code == "snapshot_not_quiescent"
    assert "run_cancelling" in cancelling.reasons


@pytest.mark.asyncio
async def test_stale_expected_version_moving_source_and_unknown_run_fail_safely() -> None:
    harness, forks, _units = await _world()
    run = await harness.run_control.get_run(SCOPE, harness.run_id)

    with pytest.raises(ForkRejected) as stale:
        await forks.snapshots.take(SCOPE, harness.run_id, expected_run_version=run.version - 1)
    assert stale.value.code == "stale_expected_version"

    forks.sources.version_skew = 1
    with pytest.raises(ForkRejected) as moving:
        await forks.snapshots.take(SCOPE, harness.run_id)
    assert moving.value.code == "snapshot_source_moving"

    with pytest.raises(ForkSnapshotNotFound):
        await forks.snapshots.take("tenant-2", harness.run_id)
    assert forks.snapshot_store.snapshots == {}


@pytest.mark.asyncio
async def test_goal_directed_boundary_requires_the_settled_verifier() -> None:
    harness = await recovery_harness()
    executor = goal_unit(
        request_scope=SCOPE, run_id=harness.run_id, operation_id="goal/1/executor", goal_iteration=1
    )
    verifier = goal_unit(
        request_scope=SCOPE,
        run_id=harness.run_id,
        operation_id="goal/1/verifier",
        goal_iteration=1,
        role="verifier",
    )
    await _settle(harness, executor)
    revision = "goal-revision:1"
    sources = FakeForkSourceReader(
        harness.run_control,
        heads={harness.run_id: (goal_head(iteration=1, revision_id=revision, role="executor"),)},
    )
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        sources,
    )
    in_progress = await _reject(forks, harness.run_id)
    assert in_progress.code == "snapshot_not_quiescent"
    assert in_progress.reasons == ("goal_iteration_in_progress",)

    sources.heads[harness.run_id] = (goal_head(iteration=1, revision_id=revision, role="verifier"),)
    unsettled = await _reject(forks, harness.run_id)
    assert unsettled.reasons == ("goal_verifier_unsettled",)

    await _settle(harness, verifier)
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    assert snapshot.boundary_kind == "goal_verifier_settled"
    assert snapshot.family == "goal_directed"
    assert snapshot.family_position.goal_iteration == 1
    assert snapshot.family_position.goal_revision_id == revision
    assert snapshot.family_position.head_operation_role == "verifier"
    assert {item.unit_key for item in snapshot.reuse_candidates} == {
        executor.unit_key,
        verifier.unit_key,
    }
    # GoalDirected revision identity is run-bound: no source unit is ever reusable, so the
    # derived run's cognition starts fresh (RRM-001 §8 #5).
    decisions = compute_reuse_decisions(
        snapshot, fork_request_id="fork-goal", derived_run_id="derived", frontier=frozenset()
    )
    assert {item.decision for item in decisions} == {"not_reusable"}
    assert {item.reason for item in decisions} == {"goal_revision_identity_is_run_bound"}


def _patch(snapshot: RunSnapshotManifest, **values: Any) -> RunForkPatch:
    return RunForkPatch.create(
        source_snapshot_id=snapshot.snapshot_id,
        source_snapshot_digest=snapshot.snapshot_digest,
        target_admission_request_ref="run-request:tenant-1:operator:fork-1",
        **values,
    )


@pytest.mark.parametrize(
    "path",
    [
        "identity.run_id",
        "request_scope",
        "authority_refs",
        "capability_grants.tools",
        "budget.tokens.total",
        "budget_ceilings",
        "accepted_results.draft",
        "accepted_evidence",
        "effects",
        "settlements",
        "terminal_outcome",
        "terminality",
        "binding_digests.draft",
        "workflow_type_ref",
    ],
)
@pytest.mark.asyncio
async def test_protected_fields_cannot_be_patched(path: str) -> None:
    _harness, forks, _units = await _world()
    snapshot = await forks.snapshots.take(SCOPE, _harness.run_id)
    patch = _patch(snapshot, changes=(ForkPatchChange(path=path, value="anything"),))

    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(patch, stage_policy())
    assert caught.value.code == "protected_field"
    assert caught.value.reasons == (path,)

    with pytest.raises(ForkRejected) as through_service:
        await forks.forks.fork(fork_command(snapshot, changes=patch.changes))
    assert through_service.value.code == "protected_field"
    assert forks.repository._requests == {}  # nothing reserved, nothing admitted


@pytest.mark.asyncio
async def test_undeclared_values_frontier_and_seed_are_validated() -> None:
    harness, forks, _units = await _world()
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    policy = stage_policy()

    undeclared = _patch(
        snapshot, changes=(ForkPatchChange(path="stage_objectives.draft", value="x"),)
    )
    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(undeclared, policy)
    assert (caught.value.code, caught.value.reasons) == (
        "field_not_patchable",
        ("stage_objectives.draft",),
    )

    uncovered = _patch(
        snapshot,
        changes=(review_objective_patch(),),
        invalidation_frontier=("draft",),
    )
    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(uncovered, policy)
    assert (caught.value.code, caught.value.reasons) == ("invalid_patch", ("review",))

    empty_text = _patch(
        snapshot,
        changes=(ForkPatchChange(path="stage_objectives.review", value="  "),),
        invalidation_frontier=("review",),
    )
    with pytest.raises(ForkRejected, match="non-empty string"):
        required_invalidation_frontier(empty_text, policy)

    bad_manifest = _patch(
        snapshot,
        changes=(ForkPatchChange(path="input_manifest", value={"manifest_id": "m"}),),
        invalidation_frontier=(INVALIDATE_ALL,),
    )
    with pytest.raises(ForkRejected, match="manifest ref"):
        required_invalidation_frontier(bad_manifest, policy)

    candidate = snapshot.reuse_candidates[0]
    assert candidate.result_checkpoint is not None
    seeded = _patch(
        snapshot,
        changes=(review_objective_patch(),),
        invalidation_frontier=("review",),
        cognitive_seed={"unit_key": candidate.unit_key, "checkpoint": candidate.result_checkpoint},
    )
    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(seeded, policy)
    assert caught.value.code == "cognitive_seed_not_supported"

    erc = "sha256:" + "e" * 64
    with pytest.raises(ValidationError, match="must invalidate every unit"):
        ForkPatchPolicy(
            policy_id="fork-patch-policy:partial-erc",
            family="stage_graph",
            patchable=(
                PatchablePath(
                    path="effective_configuration_digest",
                    invalidates=("review",),
                    value_kind="digest",
                ),
            ),
        )
    partial_erc = _patch(
        snapshot,
        changes=(ForkPatchChange(path="effective_configuration_digest", value=erc),),
        invalidation_frontier=("review",),
    )
    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(partial_erc, default_patch_policy("stage_graph"))
    assert (caught.value.code, caught.value.reasons) == (
        "invalid_patch",
        ("effective_configuration_digest",),
    )
    whole_erc = _patch(
        snapshot,
        changes=(ForkPatchChange(path="effective_configuration_digest", value=erc),),
        invalidation_frontier=(INVALIDATE_ALL,),
    )
    assert required_invalidation_frontier(
        whole_erc, default_patch_policy("stage_graph")
    ) == frozenset({INVALIDATE_ALL})

    covered = _patch(
        snapshot, changes=(review_objective_patch(),), invalidation_frontier=("review",)
    )
    assert required_invalidation_frontier(covered, policy) == frozenset({"review"})
    # The family default declares no stage objective (only whole-run fields).
    with pytest.raises(ForkRejected) as caught:
        required_invalidation_frontier(covered, default_patch_policy("stage_graph"))
    assert caught.value.code == "field_not_patchable"
    with pytest.raises(ValidationError, match="digest does not match"):
        RunForkPatch.model_validate(
            {**covered.model_dump(mode="python"), "invalidation_frontier": ()}
        )


@pytest.mark.asyncio
async def test_reuse_frontier_reuses_only_settled_compatible_results() -> None:
    harness, forks, units = await _world(
        stages={"draft": "completed", "review": "completed"}, settle=("draft", "review")
    )
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    assert len(snapshot.reuse_candidates) == 2

    decisions = compute_reuse_decisions(
        snapshot,
        fork_request_id="fork-1",
        derived_run_id="derived-run",
        frontier=frozenset({"review"}),
    )
    by_source = {item.source_unit_key: item for item in decisions}
    draft = by_source[units["draft"].unit_key]
    review = by_source[units["review"].unit_key]
    assert (draft.decision, draft.reason) == ("reuse", "settled_compatible_outside_frontier")
    assert (review.decision, review.reason) == ("invalidated", "inside_invalidation_frontier")
    assert review.candidate is None
    # The derived unit is the source identity with only the run and epoch substituted.
    expected = derived_unit_identity(units["draft"], "derived-run")
    assert draft.derived_unit == expected
    assert draft.derived_unit_key == expected.unit_key != units["draft"].unit_key
    assert expected.model_dump(exclude={"belllabs_run_id"}) == units["draft"].model_dump(
        exclude={"belllabs_run_id"}
    )
    assert (
        draft.candidate
        == snapshot.reuse_candidates[
            [item.unit_key for item in snapshot.reuse_candidates].index(units["draft"].unit_key)
        ]
    )

    everything = compute_reuse_decisions(
        snapshot,
        fork_request_id="fork-1",
        derived_run_id="derived-run",
        frontier=frozenset({INVALIDATE_ALL}),
    )
    assert {item.decision for item in everything} == {"invalidated"}
    with pytest.raises(ForkRejected, match="new BellLabs run"):
        compute_reuse_decisions(
            snapshot,
            fork_request_id="fork-1",
            derived_run_id=snapshot.source_run_id,
            frontier=frozenset(),
        )


@pytest.mark.asyncio
async def test_semantic_fork_admits_an_independent_run_at_epoch_one() -> None:
    harness, forks, units = await _world()
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)
    source_before = (
        await harness.run_control.get_run(SCOPE, harness.run_id),
        await harness.run_control.get_budget(SCOPE, harness.run_id),
        await harness.run_control.get_effects(SCOPE, harness.run_id),
    )

    receipt = await forks.forks.fork(
        fork_command(
            snapshot,
            changes=(review_objective_patch(),),
            invalidation_frontier=("review",),
            baseline_reservations={"tokens.total": 15},
        )
    )

    assert receipt.target_execution_epoch == 1
    assert receipt.target_run_id != harness.run_id
    assert receipt.source_run_id == harness.run_id
    assert receipt.snapshot_digest == snapshot.snapshot_digest
    derived = await harness.run_control.get_run(SCOPE, receipt.target_run_id)
    derived_budget = await harness.run_control.get_budget(SCOPE, receipt.target_run_id)
    # Independently admitted: pending at version 1, its own budget account and reservation.
    assert (derived.phase, derived.version) == (RunPhase.PENDING, 1)
    assert derived.effective_configuration_digest == snapshot.effective_configuration_digest
    assert derived_budget.account_id != source_before[1].account_id
    assert derived_budget.limits == snapshot.budget_frontier.limits
    assert derived_budget.reservations == {"baseline": {"tokens.total": 15}}
    assert derived_budget.consumed == {} and derived_budget.parent_account_id is None
    effects = await harness.run_control.get_effects(SCOPE, receipt.target_run_id)
    assert effects.claims == {}  # nothing implicit is copied
    assert derived.accepted_operation_settlement_evidence == ()
    assert derived.async_children == () and derived.active_waits == ()
    # The source run is unchanged by the fork.
    assert (
        await harness.run_control.get_run(SCOPE, harness.run_id),
        await harness.run_control.get_budget(SCOPE, harness.run_id),
        await harness.run_control.get_effects(SCOPE, harness.run_id),
    ) == source_before
    # Lineage and the recorded reuse frontier.
    lineage = receipt.lineage
    draft_key = derived_unit_identity(units["draft"], receipt.target_run_id).unit_key
    assert lineage.reused_unit_keys == (draft_key,)
    assert lineage.derived_execution_epoch == 1 and lineage.seed_checkpoint is None
    decision = await forks.store.get_reuse_decision(SCOPE, receipt.target_run_id, draft_key)
    assert decision is not None and decision.decision == "reuse"
    record = forks.lineage._records[(SCOPE, f"fork-lineage:{receipt.request_id}")]
    relationships = {
        (edge.child.kind.value, edge.relationship, edge.parent.kind.value)
        for edge in record.parent_edges
    }
    assert relationships == {
        ("belllabs_run", "derived_from", "run_snapshot"),
        ("run_snapshot", "contains", "belllabs_run"),
        ("belllabs_run", "reuses", "result_manifest"),
    }
    # One durable receipt; a later replay of the same intent returns it unchanged.
    replay = await forks.forks.fork(
        fork_command(
            snapshot,
            changes=(review_objective_patch(),),
            invalidation_frontier=("review",),
            baseline_reservations={"tokens.total": 15},
            requested_at_offset=timedelta(minutes=5),
        )
    )
    assert replay == receipt
    with pytest.raises(IdempotencyConflict):
        await forks.forks.fork(
            fork_command(
                snapshot,
                changes=(review_objective_patch("A different objective."),),
                invalidation_frontier=("review",),
                baseline_reservations={"tokens.total": 15},
            )
        )


@pytest.mark.asyncio
async def test_unauthorized_stale_unknown_and_rejected_forks_fail_safely() -> None:
    harness, forks, _units = await _world()
    snapshot = await forks.snapshots.take(SCOPE, harness.run_id)

    with pytest.raises(ForkRejected) as unauthorized:
        await forks.forks.fork(
            fork_command(snapshot, actor=fork_actor(permissions=frozenset({"workflow_run.admit"})))
        )
    assert unauthorized.value.code == "unauthorized"
    assert unauthorized.value.reasons == ("workflow_run.fork",)
    # A `fork_operator`-only caller (no `workflow_run.admit`) is refused up front, so no
    # reservation is left in `admitting`.
    fork_only = fork_actor(
        permissions=frozenset({"workflow_run.read", "workflow_run.snapshot", "workflow_run.fork"})
    )
    with pytest.raises(ForkRejected) as no_admit:
        await forks.forks.fork(fork_command(snapshot, request_id="fork-no-admit", actor=fork_only))
    assert (no_admit.value.code, no_admit.value.reasons) == (
        "unauthorized",
        ("workflow_run.admit",),
    )
    assert forks.repository._requests == {}

    with pytest.raises(ForkRejected) as stale:
        await forks.forks.fork(fork_command(snapshot, snapshot_digest="sha256:" + "0" * 64))
    assert stale.value.code == "stale_snapshot"

    cross_scope = fork_command(snapshot).model_copy(update={"request_scope": "tenant-2"})
    with pytest.raises(ForkSnapshotNotFound):
        await forks.forks.fork(cross_scope)

    # A baseline reservation above the protected ceiling is never admitted.
    with pytest.raises(ForkRejected) as invalid:
        await forks.forks.fork(
            fork_command(
                snapshot, request_id="fork-over", baseline_reservations={"tokens.total": 500}
            )
        )
    assert (invalid.value.code, invalid.value.reasons) == (
        "fork_admission_rejected",
        ("invalid_admission_request",),
    )
    assert await forks.repository.get(SCOPE, "fork-over") is None

    # Run control's own admission rejects: no receipt, and a retry is rejected again.
    rejecting, _repository = run_control_service(harness.repository, reject_input=True)
    rejected_forks = compose_in_memory_forks(
        rejecting,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        forks.sources,
    )
    rejected_snapshot = await rejected_forks.snapshots.take(SCOPE, harness.run_id)
    for _attempt in range(2):
        with pytest.raises(ForkRejected) as rejected:
            await rejected_forks.forks.fork(
                fork_command(rejected_snapshot, request_id="fork-rejected")
            )
        assert rejected.value.code == "fork_admission_rejected"
        assert rejected.value.reasons == ("admission_rejected",)
    assert await rejected_forks.repository.get(SCOPE, "fork-rejected") is None
    assert rejected_forks.store.materializations == {}


def test_fork_request_binds_scope_snapshot_patch_and_admission() -> None:
    from tests.unit.run_control.test_run_control import request as run_request

    target = run_request(request_id="fork-1")
    patch = RunForkPatch.create(
        source_snapshot_id="run-snapshot:" + "1" * 64,
        source_snapshot_digest="sha256:" + "2" * 64,
        target_admission_request_ref="run-request:tenant-1:operator:fork-1",
    )
    values: dict[str, Any] = {
        "request_id": "fork-1",
        "idempotency_key": "fork-1",
        "request_scope": "tenant-1",
        "source_run_id": "source",
        "source_execution_epoch": 1,
        "snapshot_id": patch.source_snapshot_id,
        "snapshot_digest": patch.source_snapshot_digest,
        "patch": patch,
        "target": target,
        "derived_run_id": "derived",
        "actor_id": "operator",
        "reason": "technical",
        "requested_at": target.requested_at,
    }
    request = RunForkRequest(**values)
    later = request.model_copy(
        update={
            "requested_at": target.requested_at + timedelta(hours=1),
            "target": target.model_copy(
                update={"requested_at": target.requested_at + timedelta(hours=1)}
            ),
        }
    )
    assert fork_request_fingerprint(later) == fork_request_fingerprint(request)
    assert fork_request_fingerprint(
        request.model_copy(update={"reason": "other"})
    ) != fork_request_fingerprint(request)

    with pytest.raises(ValidationError, match="cross request scopes"):
        RunForkRequest(**{**values, "target": run_request(request_scope="tenant-2")})
    with pytest.raises(ValidationError, match="new BellLabs run"):
        RunForkRequest(**{**values, "derived_run_id": "source"})
    linked = RunRequest.model_validate(
        {
            **target.model_dump(mode="python"),
            "parent_run_id": "source",
            "budget_envelope": {
                **target.budget_envelope.model_dump(mode="python"),
                "parent_account_id": "budget-account:source",
            },
        }
    )
    with pytest.raises(ValidationError, match="linked child"):
        RunForkRequest(**{**values, "target": linked})
    with pytest.raises(ValidationError, match="another snapshot"):
        RunForkRequest(**{**values, "snapshot_digest": "sha256:" + "3" * 64})
    with pytest.raises(ValidationError, match="another admission request"):
        RunForkRequest(**{**values, "target": run_request(request_id="fork-2")})
    assert isinstance(target, RunRequest)
    assert sha256_digest(patch.patch_digest)


def test_fork_root_parent_run_is_carried_on_the_root_only() -> None:
    from dataclasses import asdict

    from app.domain.orchestration.contracts import BellLabsRunInput
    from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow

    values: dict[str, Any] = {
        "schema_version": "belllabs.temporal-root.v1",
        "run_id": "derived",
        "request_scope": SCOPE,
        "effective_configuration_digest": "sha256:" + "a" * 64,
        "workflow_type_digest": "sha256:" + "b" * 64,
        "family": "StageGraph",
        "family_input": {},
        "family_task_queue": "queue",
    }
    ordinary = BellLabsRunInput(**values)
    assert ordinary.parent_run_id is None
    # Captured histories have no parent field; decoding them keeps the default.
    assert BellLabsRunInput(**{**asdict(ordinary), "continuity": ordinary.continuity}) == ordinary
    fork_root = BellLabsRunInput(**values, parent_run_id="source")
    root = BellLabsRunWorkflow._attributes(fork_root, "root").as_mapping()
    family = BellLabsRunWorkflow._attributes(fork_root, "family").as_mapping()
    assert root["BellLabsParentRunId"] == "source"
    assert "BellLabsParentRunId" not in family
    assert (
        "BellLabsParentRunId" not in BellLabsRunWorkflow._attributes(ordinary, "root").as_mapping()
    )
    with pytest.raises(ValueError, match="another BellLabs run"):
        BellLabsRunInput(**values, parent_run_id="derived")
