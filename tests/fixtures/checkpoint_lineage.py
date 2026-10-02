"""Shared runtime-unit and checkpoint-lineage helpers for adapter-level harnesses.

Harnesses that call `DeepAgentRuntimeAdapter.execute` directly (instead of the full
`OperationExecutionService`) use `execute_with_checkpoint_lineage` so every invocation is
still observed, pinned, stamped, and recorded by compare-and-set (REQ-CP-DA-016/017).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import pytest

from app.application.operations.checkpoint_lineage import (
    CheckpointLineageRepository,
    CheckpointLineageService,
    lease_holder_id,
)
from app.application.operations.operation_execution import bind_operation_execution_request
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import (
    NO_MAPPED_INSTANCE,
    GoalDirectedUnitLocation,
    QualifiedCheckpointKey,
    RuntimeUnitIdentity,
    StageGraphUnitLocation,
)
from app.domain.operation_execution.checkpoint_lineage import (
    AttemptAdmission,
    CheckpointClassification,
    CheckpointLineageConflict,
    CheckpointNamespaceBusy,
    CheckpointNamespaceOwnershipError,
    CheckpointTransitionObservation,
    IncompatibleCheckpointSchema,
    NamespaceClaim,
    OperationActivityAttempt,
    StaleClaimFence,
    UnitReconciliationIncident,
    UnitResultObservation,
    checkpoint_transition_id,
    cognitive_session_namespace,
    namespace_owner,
    submission_invocation_id,
    unit_incident_id,
    unit_result_observation_id,
)
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    MaterializedWorkspace,
    OperationExecutionRequest,
    RuntimeInvocation,
    RuntimeResult,
)
from app.domain.run_control.contracts import UnitReconciliationDecision


def stage_unit(
    *,
    request_scope: str,
    run_id: str,
    operation_id: str,
    semantic_attempt: int = 1,
    stage_id: str = "fixture-stage",
    operation_slot_id: str = "default",
) -> RuntimeUnitIdentity:
    return RuntimeUnitIdentity(
        request_scope=request_scope,
        belllabs_run_id=run_id,
        execution_epoch=1,
        family="stage_graph",
        unit_kind="stage_operation",
        semantic_operation_id=operation_id,
        semantic_attempt=semantic_attempt,
        location=StageGraphUnitLocation(
            stage_id=stage_id,
            mapped_instance_id=NO_MAPPED_INSTANCE,
            workflow_cycle_ordinal=0,
            stage_cycle_ordinal=0,
            operation_slot_id=operation_slot_id,
        ),
    )


def goal_unit(
    *,
    request_scope: str,
    run_id: str,
    operation_id: str,
    goal_iteration: int,
    role: Literal["executor", "verifier"] = "executor",
    session_generation: int = 1,
    semantic_attempt: int = 1,
) -> RuntimeUnitIdentity:
    return RuntimeUnitIdentity(
        request_scope=request_scope,
        belllabs_run_id=run_id,
        execution_epoch=1,
        family="goal_directed",
        unit_kind=f"goal_{role}",
        semantic_operation_id=operation_id,
        semantic_attempt=semantic_attempt,
        location=GoalDirectedUnitLocation(
            goal_iteration=goal_iteration,
            goal_revision_id="goal-revision:1",
            operation_role=role,
            agent_run=goal_iteration,
            session_generation=session_generation,
        ),
    )


def bind_unit(
    binding: DeepAgentExecutionBinding, unit: RuntimeUnitIdentity, **updates: Any
) -> DeepAgentExecutionBinding:
    """Re-create an exact binding frozen to one runtime unit and its namespace."""

    values = {
        **binding.model_dump(mode="python", exclude={"binding_digest"}),
        "run_id": unit.belllabs_run_id,
        "operation_id": unit.semantic_operation_id,
        "operation_attempt": unit.semantic_attempt,
        **updates,
    }
    generation = int(values["execution_generation"])
    values["runtime_unit"] = unit
    values["cognitive_session_namespace"] = cognitive_session_namespace(unit, generation)
    return DeepAgentExecutionBinding.create(**values)


def activity_attempt(
    attempt: int = 1, *, workflow_id: str = "operation/fixture"
) -> OperationActivityAttempt:
    return OperationActivityAttempt(
        workflow_id=workflow_id,
        workflow_run_id="fixture-run",
        activity_id="1",
        attempt=attempt,
        worker_identity="fixture-worker",
    )


def materialized_workspace(request: OperationExecutionRequest, seed: str) -> MaterializedWorkspace:
    return MaterializedWorkspace(
        workspace_id=request.workspace.workspace_id,
        namespace_id=request.workspace.namespace_id,
        provider=request.workspace.provider,
        runtime_digest=request.workspace.runtime_digest,
        image_digest=request.workspace.image_digest,
        mount_manifest_digest=sha256_digest(seed),
    )


async def execute_with_checkpoint_lineage(
    adapter: Any,
    lineage: CheckpointLineageService,
    request: OperationExecutionRequest,
    *,
    workspace: MaterializedWorkspace,
    secrets: Mapping[str, str],
    attempt: OperationActivityAttempt,
    resolved_secret_names: tuple[str, ...] = (),
) -> RuntimeResult:
    """Observe the attempt, invoke pinned and stamped, then record the transition by CAS."""

    binding = bind_operation_execution_request(request)
    plan = await lineage.observe_attempt(binding, attempt, dispatching=True)
    result: RuntimeResult = await adapter.execute(
        RuntimeInvocation(
            binding=binding,
            prompt_segments=request.prompt_segments,
            workspace=workspace,
            resolved_secret_names=resolved_secret_names,
            checkpoint_plan=plan,
        ),
        secrets,
    )
    assert plan is not None and result.checkpoint is not None
    await lineage.record_transition(
        plan,
        result.checkpoint,
        result_manifest_ref=f"harness-manifest:{binding.binding_id}",
        result_manifest_digest=sha256_digest(
            result.model_dump(mode="json", exclude={"event_payloads"})
        ),
    )
    return result


# --- Repository contract scenario (shared by in-memory and PostgreSQL suites) ---

CHECKPOINTER = "sha256:" + "c" * 64
BINDING = "sha256:" + "b" * 64
SCHEMA = "sha256:" + "5" * 64
OTHER_SCHEMA = "sha256:" + "6" * 64
LINEAGE_NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def checkpoint_key(namespace: str, checkpoint_id: str, parent: str) -> QualifiedCheckpointKey:
    return QualifiedCheckpointKey(
        checkpointer_ref_digest=CHECKPOINTER,
        thread_id=namespace,
        checkpoint_id=checkpoint_id,
        parent_checkpoint_id=parent,
    )


def namespace_claim(
    unit: RuntimeUnitIdentity,
    generation: int = 1,
    *,
    schema: str = SCHEMA,
    binding_digest: str = BINDING,
) -> NamespaceClaim:
    owner_kind, owner_digest = namespace_owner(unit, generation)
    return NamespaceClaim(
        namespace=cognitive_session_namespace(unit, generation),
        owner_kind=owner_kind,
        owner_digest=owner_digest,
        binding_digest=binding_digest,
        state_schema_digest=schema,
    )


def transition(
    unit: RuntimeUnitIdentity,
    *,
    source: QualifiedCheckpointKey | None,
    result_id: str,
    generation: int = 1,
    fence: int = 1,
    schema: str = SCHEMA,
) -> CheckpointTransitionObservation:
    namespace = cognitive_session_namespace(unit, generation)
    return CheckpointTransitionObservation(
        transition_id=checkpoint_transition_id(unit.request_scope, unit.unit_key, generation),
        request_scope=unit.request_scope,
        unit_key=unit.unit_key,
        execution_generation=generation,
        claim_fence=fence,
        namespace=namespace,
        source_key=source,
        result_key=checkpoint_key(namespace, result_id, f"{result_id}-parent"),
        ancestry_verified=True,
        binding_digest=BINDING,
        state_schema_digest=schema,
        classification=CheckpointClassification.NOT_SUBMITTED,
        invocation_id=submission_invocation_id(unit.unit_key, generation),
        result_manifest_ref=f"manifest:{unit.unit_key}:{generation}",
        result_manifest_digest=sha256_digest(f"manifest:{unit.unit_key}:{generation}"),
        redacted_summary_digest=sha256_digest(f"summary:{result_id}"),
        observed_at=LINEAGE_NOW,
    )


async def assert_checkpoint_lineage_repository_contract(
    repository: CheckpointLineageRepository, *, request_scope: str, run_id: str
) -> None:
    """REQ-CP-DA-017 / REQ-BP-GD-012 / REQ-CP-EXEC-005/014 / REQ-CP-CS-007 storage rules."""

    def unit(iteration: int, **kwargs: Any) -> RuntimeUnitIdentity:
        return goal_unit(
            request_scope=request_scope,
            run_id=run_id,
            operation_id=f"goal-iteration/{iteration}/executor",
            goal_iteration=iteration,
            **kwargs,
        )

    async def attempt_for(
        target: RuntimeUnitIdentity,
        *,
        generation: int = 1,
        number: int = 1,
        claim: NamespaceClaim | None = None,
    ) -> AttemptAdmission:
        claim = claim or namespace_claim(target, generation)
        return await repository.record_attempt(
            unit=target,
            execution_generation=generation,
            attempt=activity_attempt(number, workflow_id=f"operation/{target.unit_key}"),
            binding_id=f"binding:{target.unit_key}:{generation}",
            binding_digest=claim.binding_digest,
            namespace=claim,
            dispatching=True,
            observed_at=LINEAGE_NOW,
        )

    first, second = unit(1), unit(2)
    namespace = cognitive_session_namespace(first, 1)
    assert cognitive_session_namespace(second, 1) == namespace

    # Attempt observation and serialized session ordering (EXEC-014, GD-012).
    admitted = await attempt_for(first)
    assert admitted.observation.expected_source is None
    assert admitted.observation.claim_fence == 1
    assert await attempt_for(first) == admitted, "exact attempt replay is idempotent"
    with pytest.raises(CheckpointNamespaceBusy):
        await attempt_for(second)

    # Accepted, idempotent, and conflicting transition observations (DA-017).
    first_transition = transition(first, source=None, result_id="r1")
    assert await repository.record_transition(first_transition) == first_transition
    duplicate = first_transition.model_copy(
        update={"observed_at": LINEAGE_NOW + timedelta(seconds=5)}
    )
    assert await repository.record_transition(duplicate) == first_transition
    with pytest.raises(CheckpointLineageConflict, match="different transition"):
        await repository.record_transition(transition(first, source=None, result_id="r1-other"))
    assert await repository.get_namespace_head(request_scope, namespace) == (
        first_transition.result_key
    )

    # The next shared-session unit expects the head; CAS and claim fences are enforced.
    second_admission = await attempt_for(second)
    assert second_admission.observation.expected_source == first_transition.result_key
    with pytest.raises(CheckpointLineageConflict, match="compare-and-set"):
        await repository.record_transition(transition(second, source=None, result_id="r2"))
    advanced = await repository.advance_claim_fence(
        request_scope, second.unit_key, 1, expected_fence=1
    )
    assert advanced == 2
    with pytest.raises(StaleClaimFence):
        await repository.record_transition(
            transition(second, source=first_transition.result_key, result_id="r2")
        )
    rejections = await repository.list_rejections(request_scope, second.unit_key)
    assert [(item.reason, item.presented_fence, item.current_fence) for item in rejections] == [
        ("stale_claim_fence", 1, 2)
    ]
    second_transition = transition(
        second, source=first_transition.result_key, result_id="r2", fence=2
    )
    assert await repository.record_transition(second_transition) == second_transition
    assert await repository.list_transitions(request_scope, namespace) == (
        first_transition,
        second_transition,
    )

    # Frozen binding and namespace ownership.
    with pytest.raises(CheckpointLineageConflict, match="frozen"):
        await attempt_for(first, claim=namespace_claim(first, binding_digest=SCHEMA))
    foreign = namespace_claim(first).model_copy(update={"owner_digest": OTHER_SCHEMA})
    with pytest.raises(CheckpointNamespaceOwnershipError):
        await attempt_for(unit(3), claim=foreign)

    # Schema compatibility across one namespace (CS-007).
    drifted = unit(4)
    await attempt_for(drifted, claim=namespace_claim(drifted, schema=OTHER_SCHEMA))
    with pytest.raises(IncompatibleCheckpointSchema):
        await repository.record_transition(
            transition(
                drifted,
                source=second_transition.result_key,
                result_id="r4",
                schema=OTHER_SCHEMA,
            )
        )

    # Rollover starts a new empty namespace (GD-012).
    rollover_admission = await attempt_for(unit(5, session_generation=2))
    assert rollover_admission.observation.namespace != namespace
    assert rollover_admission.observation.expected_source is None

    # A later generation fences the earlier one (EXEC-005).
    fenced = unit(6, session_generation=3)
    await attempt_for(fenced)
    await attempt_for(fenced, generation=2)
    with pytest.raises(StaleClaimFence):
        await attempt_for(fenced, number=2)
    with pytest.raises(StaleClaimFence):
        await repository.record_transition(transition(fenced, source=None, result_id="r6"))
    reasons = [
        item.reason for item in await repository.list_rejections(request_scope, fenced.unit_key)
    ]
    assert reasons == ["stale_execution_generation"]
    attempts = await repository.list_attempts(request_scope, fenced.unit_key)
    assert [(item.execution_generation, item.attempt.attempt) for item in attempts] == [
        (1, 1),
        (2, 1),
    ]


# --- RRM-004 recovery contract (lease, fenced result, incidents, reconciliation) ---


def unit_result(
    unit: RuntimeUnitIdentity,
    *,
    fence: int,
    generation: int = 1,
    manifest: str = "manifest",
    status: Literal["completed", "failed", "cancelled", "timed_out"] = "completed",
    transition_id: str | None = None,
    binding_id: str | None = None,
) -> UnitResultObservation:
    return UnitResultObservation(
        observation_id=unit_result_observation_id(unit.request_scope, unit.unit_key, generation),
        request_scope=unit.request_scope,
        unit_key=unit.unit_key,
        execution_generation=generation,
        claim_fence=fence,
        binding_id=binding_id or f"binding:{unit.unit_key}:{generation}",
        settlement_id=f"settlement:{unit.unit_key}:{generation}",
        status=status,
        result_manifest_ref=f"{manifest}:{unit.unit_key}:{generation}",
        result_manifest_digest=sha256_digest(f"{manifest}:{unit.unit_key}:{generation}"),
        result_manifest_size_bytes=64,
        checkpoint_transition_id=transition_id,
        observed_at=LINEAGE_NOW,
    )


def in_doubt_incident(
    unit: RuntimeUnitIdentity, *, generation: int = 1, reason: str = "multiple_stamped_leaves"
) -> UnitReconciliationIncident:
    namespace = cognitive_session_namespace(unit, generation)
    return UnitReconciliationIncident(
        incident_id=unit_incident_id(unit.request_scope, unit.unit_key, generation),
        request_scope=unit.request_scope,
        belllabs_run_id=unit.belllabs_run_id,
        unit_key=unit.unit_key,
        execution_generation=generation,
        binding_id=f"binding:{unit.unit_key}:{generation}",
        operation_workflow_id=f"operation/{unit.unit_key}",
        reason=reason,  # type: ignore[arg-type]
        namespace=namespace,
        candidates=(
            checkpoint_key(namespace, "leaf-a", "parent"),
            checkpoint_key(namespace, "leaf-b", "parent"),
        ),
        recorded_at=LINEAGE_NOW,
    )


def reconciliation(
    incident: UnitReconciliationIncident,
    decision: Literal["accept_descendant", "abandon_unit", "start_new_generation"],
    *,
    decision_id: str = "reconcile-1",
) -> UnitReconciliationDecision:
    return UnitReconciliationDecision(
        decision_id=decision_id,
        unit_key=incident.unit_key,
        execution_generation=incident.execution_generation,
        incident_id=incident.incident_id,
        decision=decision,
        accepted_checkpoint=(
            incident.candidates[0] if decision == "accept_descendant" else None
        ),
        actor_id="operator",
        decided_at=LINEAGE_NOW,
    )


async def assert_checkpoint_recovery_repository_contract(
    repository: CheckpointLineageRepository, *, request_scope: str, run_id: str
) -> None:
    """REQ-CP-EXEC-005/014, REQ-CP-DA-017/018, REQ-CP-RUN-007 storage rules (RRM-004)."""

    def unit(iteration: int, **kwargs: Any) -> RuntimeUnitIdentity:
        return goal_unit(
            request_scope=request_scope,
            run_id=run_id,
            operation_id=f"recovery/{iteration}",
            goal_iteration=100 + iteration,
            **kwargs,
        )

    async def leased(
        target: RuntimeUnitIdentity,
        number: int,
        *,
        at: timedelta,
        lease: timedelta = timedelta(minutes=5),
        generation: int = 1,
    ) -> AttemptAdmission:
        claim = namespace_claim(target, generation)
        return await repository.record_attempt(
            unit=target,
            execution_generation=generation,
            attempt=activity_attempt(number, workflow_id=f"operation/{target.unit_key}"),
            binding_id=f"binding:{target.unit_key}:{generation}",
            binding_digest=claim.binding_digest,
            namespace=claim,
            dispatching=True,
            observed_at=LINEAGE_NOW + at,
            lease_expires_at=LINEAGE_NOW + at + lease,
        )

    # Claim lease and takeover (EXEC-014): a live lease is honored; an expired one is taken
    # over only by advancing the fence; the taking attempt learns an earlier one dispatched.
    holder = unit(1, session_generation=10)
    first = await leased(holder, 1, at=timedelta(0))
    assert (first.lease_granted, first.took_over, first.prior_dispatch) == (True, False, False)
    assert first.observation.claim_fence == 1
    standing_down = await leased(holder, 2, at=timedelta(minutes=1))
    assert not standing_down.lease_granted
    assert not standing_down.observation.dispatching
    assert standing_down.observation.claim_fence == 1
    taken = await leased(holder, 3, at=timedelta(minutes=6))
    assert (taken.lease_granted, taken.took_over, taken.prior_dispatch) == (True, True, True)
    assert taken.observation.claim_fence == 2
    assert taken.observation.dispatching
    # A released lease is taken over at once, again advancing the fence.
    await repository.release_lease(
        request_scope,
        holder.unit_key,
        1,
        holder=lease_holder_id(request_scope, holder.unit_key, 1, taken.observation.attempt),
        released_at=LINEAGE_NOW + timedelta(minutes=6, seconds=1),
    )
    after_release = await leased(holder, 4, at=timedelta(minutes=6, seconds=2))
    assert after_release.took_over and after_release.observation.claim_fence == 3

    # The fenced result observation (EXEC-014): a superseded fence is rejected and recorded,
    # the current fence fixes the manifest once, and a different manifest conflicts.
    with pytest.raises(StaleClaimFence):
        await repository.record_result(unit_result(holder, fence=2, status="failed"))
    rejections = await repository.list_rejections(request_scope, holder.unit_key)
    assert [(item.reason, item.presented_fence, item.current_fence) for item in rejections] == [
        ("stale_claim_fence", 2, 3)
    ]
    recorded = await repository.record_result(unit_result(holder, fence=3, status="failed"))
    assert await repository.get_result(request_scope, holder.unit_key, 1) == recorded
    duplicate = unit_result(holder, fence=3, status="failed").model_copy(
        update={"observed_at": LINEAGE_NOW + timedelta(hours=1)}
    )
    assert await repository.record_result(duplicate) == recorded
    with pytest.raises(CheckpointLineageConflict, match="different result manifest"):
        await repository.record_result(
            unit_result(holder, fence=3, status="failed", manifest="other")
        )
    # A result without a transition ends the in-flight invocation: the namespace is free.
    holder_namespace = cognitive_session_namespace(holder, 1)
    assert await repository.get_namespace_in_flight(request_scope, holder_namespace) is None
    assert await repository.get_namespace_head(request_scope, holder_namespace) is None
    later = await leased(holder, 5, at=timedelta(minutes=30))
    assert later.existing_result == recorded, "a later attempt finds observed_unsettled"

    # A completed result and its checkpoint transition are one atomic, fenced write.
    cognitive = unit(2, session_generation=11)
    admitted = await leased(cognitive, 1, at=timedelta(0))
    namespace = cognitive_session_namespace(cognitive, 1)
    assert await repository.get_namespace_in_flight(request_scope, namespace) == (
        cognitive.unit_key,
        1,
    )
    linked = transition(cognitive, source=None, result_id="recovered", fence=1)
    linked = linked.model_copy(
        update={
            "result_manifest_ref": f"manifest:{cognitive.unit_key}:1",
            "result_manifest_digest": sha256_digest(f"manifest:{cognitive.unit_key}:1"),
            "classification": CheckpointClassification.TERMINAL_UNOBSERVED,
        }
    )
    mismatched = unit_result(cognitive, fence=1, transition_id=linked.transition_id).model_copy(
        update={"result_manifest_digest": sha256_digest("not-the-transition-manifest")}
    )
    with pytest.raises(CheckpointLineageConflict, match="one write"):
        await repository.record_result(mismatched, transition=linked)
    assert await repository.get_transition(request_scope, cognitive.unit_key, 1) is None
    accepted = await repository.record_result(
        unit_result(cognitive, fence=1, transition_id=linked.transition_id), transition=linked
    )
    assert accepted.checkpoint_transition_id == linked.transition_id
    assert await repository.get_transition(request_scope, cognitive.unit_key, 1) == linked
    assert await repository.get_namespace_head(request_scope, namespace) == linked.result_key
    assert await repository.get_namespace_in_flight(request_scope, namespace) is None
    assert admitted.incident is None

    # Typed in_doubt incidents are idempotent per unit generation (DA-018, RUN-007).
    doubtful = unit(3, session_generation=12)
    await leased(doubtful, 1, at=timedelta(0))
    incident = in_doubt_incident(doubtful)
    assert await repository.open_incident(incident) == incident
    assert await repository.open_incident(
        in_doubt_incident(doubtful, reason="foreign_descendant")
    ) == incident, "the first incident of a unit generation is kept"
    assert await repository.get_incident(request_scope, doubtful.unit_key, 1) == incident
    seen = await leased(doubtful, 2, at=timedelta(minutes=6))
    assert seen.incident == incident
    doubtful_namespace = cognitive_session_namespace(doubtful, 1)
    assert await repository.get_namespace_in_flight(request_scope, doubtful_namespace) == (
        doubtful.unit_key,
        1,
    ), "an in_doubt unit holds its namespace until reconciliation"

    # `abandon_unit` resolves the incident and releases the stranded namespace, once.
    resolved = await repository.apply_reconciliation(
        request_scope, reconciliation(incident, "abandon_unit")
    )
    assert resolved.status == "resolved" and resolved.decision == "abandon_unit"
    assert await repository.get_namespace_in_flight(request_scope, doubtful_namespace) is None
    assert (
        await repository.apply_reconciliation(
            request_scope, reconciliation(incident, "abandon_unit")
        )
        == resolved
    )
    with pytest.raises(CheckpointLineageConflict, match="resolved differently"):
        await repository.apply_reconciliation(
            request_scope, reconciliation(incident, "accept_descendant", decision_id="other")
        )

    # `start_new_generation` fences the generation (EXEC-005): its attempts and late
    # results are refused, the late write is recorded, and generation 2 is admitted.
    superseded = unit(4, session_generation=13)
    old = await leased(superseded, 1, at=timedelta(0))
    old_incident = in_doubt_incident(superseded)
    await repository.open_incident(old_incident)
    await repository.apply_reconciliation(
        request_scope, reconciliation(old_incident, "start_new_generation")
    )
    with pytest.raises(StaleClaimFence):
        await leased(superseded, 2, at=timedelta(minutes=6))
    with pytest.raises(StaleClaimFence):
        await repository.record_result(
            unit_result(superseded, fence=old.observation.claim_fence)
        )
    reasons = [
        item.reason
        for item in await repository.list_rejections(request_scope, superseded.unit_key)
    ]
    assert reasons == ["stale_execution_generation"]
    assert await repository.get_result(request_scope, superseded.unit_key, 1) is None
    next_generation = await leased(superseded, 1, at=timedelta(minutes=7), generation=2)
    assert next_generation.lease_granted
    assert next_generation.observation.execution_generation == 2
