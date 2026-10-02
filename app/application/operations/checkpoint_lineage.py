"""Checkpoint lineage authority for runtime units (REQ-CP-EXEC-013/014, REQ-CP-DA-016/017).

The repository records Activity attempt observations before dispatch and accepts checkpoint
transition observations only by compare-and-set on the cognitive namespace head. The
checkpointer's own latest checkpoint is evidence only; the namespace head is the result key
of the last accepted transition. One namespace admits at most one in-flight invocation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    AttemptAdmission,
    CheckpointCapture,
    CheckpointClassification,
    CheckpointInvocationPlan,
    CheckpointLineageConflict,
    CheckpointLineageInDoubt,
    CheckpointNamespaceBusy,
    CheckpointNamespaceOwnershipError,
    CheckpointTransitionObservation,
    IncompatibleCheckpointSchema,
    LineageWriteRejection,
    NamespaceClaim,
    OperationActivityAttempt,
    StaleClaimFence,
    checkpoint_transition_id,
    cognitive_session_namespace,
    namespace_owner,
    submission_invocation_id,
)
from app.domain.operation_execution.contracts import OperationExecutionBinding


@dataclass(frozen=True)
class UnitGenerationRecord:
    unit_key: str
    execution_generation: int
    claim_fence: int
    binding_id: str
    binding_digest: str
    namespace: str | None
    state_schema_digest: str | None


@dataclass(frozen=True)
class NamespaceRecord:
    namespace: str
    owner_kind: str
    owner_digest: str
    head: QualifiedCheckpointKey | None = None
    head_transition_id: str | None = None
    head_state_schema_digest: str | None = None
    in_flight_unit_key: str | None = None
    in_flight_generation: int | None = None


class CheckpointLineageRepository(Protocol):
    async def record_attempt(
        self,
        *,
        unit: RuntimeUnitIdentity,
        execution_generation: int,
        attempt: OperationActivityAttempt,
        binding_id: str,
        binding_digest: str,
        namespace: NamespaceClaim | None,
        dispatching: bool,
        observed_at: datetime,
    ) -> AttemptAdmission: ...

    async def record_transition(
        self, transition: CheckpointTransitionObservation
    ) -> CheckpointTransitionObservation: ...

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None: ...

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None: ...

    async def list_attempts(
        self, request_scope: str, unit_key: str
    ) -> tuple[ActivityAttemptObservation, ...]: ...

    async def list_transitions(
        self, request_scope: str, namespace: str
    ) -> tuple[CheckpointTransitionObservation, ...]: ...

    async def list_rejections(
        self, request_scope: str, unit_key: str
    ) -> tuple[LineageWriteRejection, ...]: ...

    async def advance_claim_fence(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        expected_fence: int,
    ) -> int: ...


# --- Shared decision rules (pure; both repositories apply them under their own lock) ---


def check_generation_admission(
    *,
    unit_key: str,
    execution_generation: int,
    binding_id: str,
    binding_digest: str,
    namespace: NamespaceClaim | None,
    current: UnitGenerationRecord | None,
    max_generation: int | None,
) -> None:
    if max_generation is not None and execution_generation < max_generation:
        raise StaleClaimFence(
            "a later execution generation fences this unit generation (REQ-CP-EXEC-005)"
        )
    if current is None:
        return
    if (
        current.binding_id != binding_id
        or current.binding_digest != binding_digest
        or current.namespace != (namespace.namespace if namespace else None)
        or current.state_schema_digest != (namespace.state_schema_digest if namespace else None)
    ):
        raise CheckpointLineageConflict(
            f"unit generation {unit_key}/{execution_generation} is frozen to another binding"
        )


def check_namespace_owner(claim: NamespaceClaim, record: NamespaceRecord) -> None:
    if record.owner_kind != claim.owner_kind or record.owner_digest != claim.owner_digest:
        raise CheckpointNamespaceOwnershipError(
            "the unit generation does not own the cognitive namespace it addressed"
        )


def reserve_in_flight(
    record: NamespaceRecord, *, unit_key: str, execution_generation: int
) -> NamespaceRecord:
    holder = (record.in_flight_unit_key, record.in_flight_generation)
    if holder not in {(None, None), (unit_key, execution_generation)}:
        raise CheckpointNamespaceBusy(
            "the cognitive namespace already has an in-flight invocation (REQ-BP-GD-012)"
        )
    return replace(record, in_flight_unit_key=unit_key, in_flight_generation=execution_generation)


TransitionDecision = Literal["duplicate", "accept"]


def decide_transition(
    transition: CheckpointTransitionObservation,
    *,
    generation: UnitGenerationRecord | None,
    max_generation: int | None,
    namespace: NamespaceRecord | None,
    existing: CheckpointTransitionObservation | None,
    rejected_at: datetime,
) -> TransitionDecision | LineageWriteRejection:
    """Return the decision, or a rejection that must be recorded and then raised."""

    if existing is not None:
        if existing.content_digest == transition.content_digest:
            return "duplicate"
        raise CheckpointLineageConflict(
            "a different transition observation already exists for this unit generation"
        )
    if generation is None:
        raise CheckpointLineageConflict("no attempt observation precedes this transition")
    current_generation = max(max_generation or 1, generation.execution_generation)
    if transition.claim_fence != generation.claim_fence or (
        transition.execution_generation < current_generation
    ):
        return LineageWriteRejection(
            request_scope=transition.request_scope,
            unit_key=transition.unit_key,
            execution_generation=transition.execution_generation,
            presented_fence=transition.claim_fence,
            current_fence=generation.claim_fence,
            current_generation=current_generation,
            reason=(
                "stale_claim_fence"
                if transition.claim_fence != generation.claim_fence
                else "stale_execution_generation"
            ),
            payload_digest=transition.content_digest,
            rejected_at=rejected_at,
        )
    if (
        transition.binding_digest != generation.binding_digest
        or transition.state_schema_digest != generation.state_schema_digest
        or transition.namespace != generation.namespace
    ):
        raise CheckpointLineageConflict(
            "transition binding, schema, or namespace differs from the frozen unit generation"
        )
    if namespace is None:
        raise CheckpointLineageConflict("transition addressed an unknown cognitive namespace")
    if (namespace.in_flight_unit_key, namespace.in_flight_generation) != (
        transition.unit_key,
        transition.execution_generation,
    ):
        raise CheckpointLineageConflict(
            "transition is out of order: its unit generation holds no in-flight invocation"
        )
    if namespace.head != transition.source_key:
        raise CheckpointLineageConflict(
            "compare-and-set failed: the namespace head is not the transition's source"
        )
    if (
        namespace.head_state_schema_digest is not None
        and namespace.head_state_schema_digest != transition.state_schema_digest
    ):
        raise IncompatibleCheckpointSchema(
            "the namespace head was written under a different cognitive state schema"
        )
    return "accept"


def advance_namespace(
    record: NamespaceRecord, transition: CheckpointTransitionObservation
) -> NamespaceRecord:
    return replace(
        record,
        head=transition.result_key,
        head_transition_id=transition.transition_id,
        head_state_schema_digest=transition.state_schema_digest,
        in_flight_unit_key=None,
        in_flight_generation=None,
    )


# --- In-memory repository (tests and single-process composition) ---


class InMemoryCheckpointLineageRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.units: dict[tuple[str, str], RuntimeUnitIdentity] = {}
        self.generations: dict[tuple[str, str, int], UnitGenerationRecord] = {}
        self.namespaces: dict[tuple[str, str], NamespaceRecord] = {}
        self.attempts: dict[tuple[str, str], ActivityAttemptObservation] = {}
        self.transitions: dict[tuple[str, str, int], CheckpointTransitionObservation] = {}
        self.rejections: list[LineageWriteRejection] = []

    async def record_attempt(
        self,
        *,
        unit: RuntimeUnitIdentity,
        execution_generation: int,
        attempt: OperationActivityAttempt,
        binding_id: str,
        binding_digest: str,
        namespace: NamespaceClaim | None,
        dispatching: bool,
        observed_at: datetime,
    ) -> AttemptAdmission:
        scope = unit.request_scope
        unit_key = unit.unit_key
        async with self._lock:
            self.units.setdefault((scope, unit_key), unit)
            generation_key = (scope, unit_key, execution_generation)
            current = self.generations.get(generation_key)
            check_generation_admission(
                unit_key=unit_key,
                execution_generation=execution_generation,
                binding_id=binding_id,
                binding_digest=binding_digest,
                namespace=namespace,
                current=current,
                max_generation=self._max_generation(scope, unit_key),
            )
            if current is None:
                current = UnitGenerationRecord(
                    unit_key=unit_key,
                    execution_generation=execution_generation,
                    claim_fence=1,
                    binding_id=binding_id,
                    binding_digest=binding_digest,
                    namespace=namespace.namespace if namespace else None,
                    state_schema_digest=namespace.state_schema_digest if namespace else None,
                )
                self.generations[generation_key] = current
            existing = self.transitions.get(generation_key)
            expected_source: QualifiedCheckpointKey | None = None
            if namespace is not None:
                namespace_key = (scope, namespace.namespace)
                record = self.namespaces.get(namespace_key) or NamespaceRecord(
                    namespace=namespace.namespace,
                    owner_kind=namespace.owner_kind,
                    owner_digest=namespace.owner_digest,
                )
                check_namespace_owner(namespace, record)
                if dispatching and existing is None:
                    record = reserve_in_flight(
                        record, unit_key=unit_key, execution_generation=execution_generation
                    )
                self.namespaces[namespace_key] = record
                expected_source = existing.source_key if existing is not None else record.head
            observation = ActivityAttemptObservation(
                request_scope=scope,
                unit_key=unit_key,
                execution_generation=execution_generation,
                claim_fence=current.claim_fence,
                attempt=attempt,
                binding_id=binding_id,
                namespace=namespace.namespace if namespace else None,
                expected_source=expected_source,
                dispatching=dispatching,
                observed_at=observed_at,
            )
            stored = self.attempts.setdefault((scope, observation.observation_id), observation)
            return AttemptAdmission(observation=deepcopy(stored), existing_transition=existing)

    async def record_transition(
        self, transition: CheckpointTransitionObservation
    ) -> CheckpointTransitionObservation:
        scope = transition.request_scope
        generation_key = (scope, transition.unit_key, transition.execution_generation)
        async with self._lock:
            namespace_key = (scope, transition.namespace)
            decision = decide_transition(
                transition,
                generation=self.generations.get(generation_key),
                max_generation=self._max_generation(scope, transition.unit_key),
                namespace=self.namespaces.get(namespace_key),
                existing=self.transitions.get(generation_key),
                rejected_at=datetime.now(UTC),
            )
            if isinstance(decision, LineageWriteRejection):
                self.rejections.append(decision)
                raise StaleClaimFence(
                    f"transition presented a superseded {decision.reason.removeprefix('stale_')}"
                )
            if decision == "duplicate":
                return deepcopy(self.transitions[generation_key])
            self.transitions[generation_key] = transition
            self.namespaces[namespace_key] = advance_namespace(
                self.namespaces[namespace_key], transition
            )
            return deepcopy(transition)

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None:
        return deepcopy(self.transitions.get((request_scope, unit_key, execution_generation)))

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None:
        record = self.namespaces.get((request_scope, namespace))
        return record.head if record is not None else None

    async def list_attempts(
        self, request_scope: str, unit_key: str
    ) -> tuple[ActivityAttemptObservation, ...]:
        return tuple(
            sorted(
                (
                    item
                    for (scope, _), item in self.attempts.items()
                    if scope == request_scope and item.unit_key == unit_key
                ),
                key=lambda item: (item.execution_generation, item.attempt.attempt),
            )
        )

    async def list_transitions(
        self, request_scope: str, namespace: str
    ) -> tuple[CheckpointTransitionObservation, ...]:
        found = [
            item
            for (scope, _unit, _generation), item in self.transitions.items()
            if scope == request_scope and item.namespace == namespace
        ]
        return tuple(linear_transition_order(found))

    async def list_rejections(
        self, request_scope: str, unit_key: str
    ) -> tuple[LineageWriteRejection, ...]:
        return tuple(
            item
            for item in self.rejections
            if item.request_scope == request_scope and item.unit_key == unit_key
        )

    async def advance_claim_fence(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        expected_fence: int,
    ) -> int:
        """Claim-takeover seam for RRM-004: advance the fence only from the expected value."""

        key = (request_scope, unit_key, execution_generation)
        async with self._lock:
            current = self.generations.get(key)
            if current is None or current.claim_fence != expected_fence:
                raise CheckpointLineageConflict("claim fence changed concurrently")
            self.generations[key] = replace(current, claim_fence=expected_fence + 1)
            return expected_fence + 1

    def _max_generation(self, request_scope: str, unit_key: str) -> int | None:
        generations = [
            generation
            for (scope, key, generation) in self.generations
            if scope == request_scope and key == unit_key
        ]
        return max(generations) if generations else None


def linear_transition_order(
    transitions: list[CheckpointTransitionObservation],
) -> list[CheckpointTransitionObservation]:
    by_source = {
        (item.source_key.checkpoint_id if item.source_key else None): item for item in transitions
    }
    ordered: list[CheckpointTransitionObservation] = []
    cursor: str | None = None
    while cursor in by_source or (cursor is None and None in by_source):
        item = by_source.pop(cursor)
        ordered.append(item)
        cursor = item.result_key.checkpoint_id
    return [*ordered, *by_source.values()]


# --- Application service used by operation execution ---


class CheckpointLineageService:
    """Builds unit attempt observations, invocation plans, and CAS transition writes."""

    def __init__(
        self,
        repository: CheckpointLineageRepository,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._clock = clock or (lambda: datetime.now(UTC))

    @property
    def repository(self) -> CheckpointLineageRepository:
        return self._repository

    async def observe_attempt(
        self,
        binding: OperationExecutionBinding,
        attempt: OperationActivityAttempt,
        *,
        dispatching: bool,
    ) -> CheckpointInvocationPlan | None:
        """Record the attempt before dispatch; return the submission plan for Deep Agents."""

        unit = binding.runtime_unit
        if unit is None:
            raise ValueError("lineage-qualified execution requires a runtime unit (EXEC-013)")
        deep_binding = binding.deep_agent_binding
        namespace: NamespaceClaim | None = None
        generation = 1
        binding_digest = sha256_digest(binding)
        if deep_binding is not None:
            generation = deep_binding.execution_generation
            bound_namespace = deep_binding.cognitive_session_namespace
            if deep_binding.runtime_unit != unit or bound_namespace is None:
                raise ValueError("Deep Agent binding lacks its frozen runtime unit namespace")
            if bound_namespace != cognitive_session_namespace(unit, generation):
                raise CheckpointNamespaceOwnershipError(
                    "binding namespace differs from the unit generation's namespace"
                )
            owner_kind, owner_digest = namespace_owner(unit, generation)
            binding_digest = deep_binding.binding_digest
            namespace = NamespaceClaim(
                namespace=bound_namespace,
                owner_kind=owner_kind,
                owner_digest=owner_digest,
                binding_digest=binding_digest,
                state_schema_digest=deep_binding.cognitive_state_schema.schema_digest,
            )
        admission = await self._repository.record_attempt(
            unit=unit,
            execution_generation=generation,
            attempt=attempt,
            binding_id=binding.binding_id,
            binding_digest=binding_digest,
            namespace=namespace,
            dispatching=dispatching,
            observed_at=self._clock(),
        )
        if not dispatching or deep_binding is None or namespace is None:
            return None
        if admission.existing_transition is not None:
            raise CheckpointLineageInDoubt(
                "an observed but unsettled transition requires settlement recovery, "
                "not a new invocation"
            )
        observation = admission.observation
        return CheckpointInvocationPlan(
            request_scope=unit.request_scope,
            unit_key=unit.unit_key,
            execution_generation=generation,
            claim_fence=observation.claim_fence,
            namespace=namespace.namespace,
            invocation_id=submission_invocation_id(unit.unit_key, generation),
            expected_source=observation.expected_source,
            checkpointer_ref_digest=deep_binding.checkpointer_ref.digest,
            binding_digest=namespace.binding_digest,
            state_schema_digest=namespace.state_schema_digest,
        )

    async def record_transition(
        self,
        plan: CheckpointInvocationPlan,
        capture: CheckpointCapture,
        *,
        result_manifest_ref: str,
        result_manifest_digest: str,
    ) -> CheckpointTransitionObservation:
        if (
            capture.namespace != plan.namespace
            or capture.invocation_id != plan.invocation_id
            or capture.source_key != plan.expected_source
        ):
            raise CheckpointLineageConflict(
                "captured checkpoint lineage is not the planned invocation's"
            )
        transition = CheckpointTransitionObservation(
            transition_id=transition_id_for(plan),
            request_scope=plan.request_scope,
            unit_key=plan.unit_key,
            execution_generation=plan.execution_generation,
            claim_fence=plan.claim_fence,
            namespace=plan.namespace,
            source_key=capture.source_key,
            result_key=capture.result_key,
            ancestry_verified=capture.ancestry_verified,
            binding_digest=plan.binding_digest,
            state_schema_digest=plan.state_schema_digest,
            classification=CheckpointClassification.NOT_SUBMITTED,
            invocation_id=plan.invocation_id,
            result_manifest_ref=result_manifest_ref,
            result_manifest_digest=result_manifest_digest,
            redacted_summary_digest=capture.redacted_summary_digest,
            observed_at=self._clock(),
        )
        return await self._repository.record_transition(transition)


def transition_id_for(plan: CheckpointInvocationPlan) -> str:
    return checkpoint_transition_id(plan.request_scope, plan.unit_key, plan.execution_generation)
