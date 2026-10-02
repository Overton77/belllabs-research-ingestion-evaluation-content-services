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
    checkpoint_transition_id,
    cognitive_session_namespace,
    namespace_owner,
    submission_invocation_id,
)
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    MaterializedWorkspace,
    OperationExecutionRequest,
    RuntimeInvocation,
    RuntimeResult,
)


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
