"""Checkpoint lineage authority for runtime units (REQ-CP-EXEC-013/014, REQ-CP-DA-016/017/018).

The repository records Activity attempt observations before dispatch and accepts checkpoint
transition observations only by compare-and-set on the cognitive namespace head. The
checkpointer's own latest checkpoint is evidence only; the namespace head is the result key
of the last accepted transition. One namespace admits at most one in-flight invocation.

RRM-004 adds the recovery half of the protocol:

- a claim lease per unit generation: an attempt dispatches only while it holds the lease,
  and a later attempt takes over an expired or released lease only by advancing the fence;
- the fenced unit result observation, which fixes the result manifest before authority
  settlement so a later holder settles exactly that manifest without provider work;
- typed `in_doubt` incidents and the application of `reconcile_unit` decisions, including
  the generation boundary and the release of stranded namespaces.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    AttemptAdmission,
    CheckpointCapture,
    CheckpointInvocationPlan,
    CheckpointLineageConflict,
    CheckpointLineageInDoubt,
    CheckpointNamespaceBusy,
    CheckpointNamespaceOwnershipError,
    CheckpointTransitionObservation,
    IncompatibleCheckpointSchema,
    InDoubtReason,
    LineageWriteRejection,
    NamespaceClaim,
    OperationActivityAttempt,
    StaleClaimFence,
    UnitReconciliationIncident,
    UnitResultObservation,
    activity_attempt_observation_id,
    checkpoint_transition_id,
    cognitive_session_namespace,
    namespace_owner,
    submission_invocation_id,
    unit_incident_id,
    unit_result_observation_id,
)
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    OperationExecutionBinding,
)
from app.domain.run_control.contracts import UnitReconciliationDecision

DEFAULT_CLAIM_LEASE = timedelta(minutes=5)


@dataclass(frozen=True)
class UnitGenerationRecord:
    unit_key: str
    execution_generation: int
    claim_fence: int
    binding_id: str
    binding_digest: str
    namespace: str | None
    state_schema_digest: str | None
    lease_holder: str | None = None
    lease_expires_at: datetime | None = None
    superseded: bool = False


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
        lease_expires_at: datetime | None = None,
    ) -> AttemptAdmission: ...

    async def record_transition(
        self, transition: CheckpointTransitionObservation
    ) -> CheckpointTransitionObservation: ...

    async def record_result(
        self,
        result: UnitResultObservation,
        *,
        transition: CheckpointTransitionObservation | None = None,
    ) -> UnitResultObservation: ...

    async def get_result(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitResultObservation | None: ...

    async def release_lease(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        holder: str,
        released_at: datetime,
    ) -> None: ...

    async def open_incident(
        self, incident: UnitReconciliationIncident
    ) -> UnitReconciliationIncident: ...

    async def get_incident(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitReconciliationIncident | None: ...

    async def apply_reconciliation(
        self, request_scope: str, decision: UnitReconciliationDecision
    ) -> UnitReconciliationIncident: ...

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None: ...

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None: ...

    async def get_namespace_in_flight(
        self, request_scope: str, namespace: str
    ) -> tuple[str, int] | None: ...

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


def effective_max_generation(
    max_generation: int | None, max_generation_superseded: bool
) -> int | None:
    """The current generation: an accepted generation boundary fences the latest row."""

    if max_generation is None:
        return None
    return max_generation + 1 if max_generation_superseded else max_generation


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


def release_in_flight(
    record: NamespaceRecord, *, unit_key: str, execution_generation: int
) -> NamespaceRecord:
    """Release a reservation held by exactly this unit generation; otherwise no change."""

    if (record.in_flight_unit_key, record.in_flight_generation) != (
        unit_key,
        execution_generation,
    ):
        return record
    return replace(record, in_flight_unit_key=None, in_flight_generation=None)


@dataclass(frozen=True)
class LeaseDecision:
    granted: bool
    took_over: bool
    record: UnitGenerationRecord


def decide_lease(
    record: UnitGenerationRecord,
    *,
    holder: str,
    requested_until: datetime,
    now: datetime,
) -> LeaseDecision:
    """REQ-CP-EXEC-014: hold, renew, or take over the unit generation's claim lease.

    A lease is free when it was never held, is held by this exact attempt, or has expired
    or been released (`lease_expires_at <= now`). Taking a lease from another holder
    advances the claim fence, so every write the superseded holder still presents fails.
    """

    free = (
        record.lease_holder is None
        or record.lease_holder == holder
        or (record.lease_expires_at is not None and record.lease_expires_at <= now)
    )
    if not free:
        return LeaseDecision(granted=False, took_over=False, record=record)
    took_over = record.lease_holder is not None and record.lease_holder != holder
    return LeaseDecision(
        granted=True,
        took_over=took_over,
        record=replace(
            record,
            claim_fence=record.claim_fence + 1 if took_over else record.claim_fence,
            lease_holder=holder,
            lease_expires_at=max(requested_until, now),
        ),
    )


def stale_write_rejection(
    *,
    request_scope: str,
    unit_key: str,
    execution_generation: int,
    presented_fence: int,
    generation: UnitGenerationRecord,
    max_generation: int | None,
    payload_digest: str,
    rejected_at: datetime,
) -> LineageWriteRejection | None:
    current_generation = max(max_generation or 1, generation.execution_generation)
    if presented_fence == generation.claim_fence and execution_generation >= current_generation:
        return None
    return LineageWriteRejection(
        request_scope=request_scope,
        unit_key=unit_key,
        execution_generation=execution_generation,
        presented_fence=presented_fence,
        current_fence=generation.claim_fence,
        current_generation=current_generation,
        reason=(
            "stale_claim_fence"
            if presented_fence != generation.claim_fence
            else "stale_execution_generation"
        ),
        payload_digest=payload_digest,
        rejected_at=rejected_at,
    )


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
    rejection = stale_write_rejection(
        request_scope=transition.request_scope,
        unit_key=transition.unit_key,
        execution_generation=transition.execution_generation,
        presented_fence=transition.claim_fence,
        generation=generation,
        max_generation=max_generation,
        payload_digest=transition.content_digest,
        rejected_at=rejected_at,
    )
    if rejection is not None:
        return rejection
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


ResultDecision = Literal["duplicate", "accept"]


def decide_result(
    result: UnitResultObservation,
    *,
    generation: UnitGenerationRecord | None,
    max_generation: int | None,
    existing: UnitResultObservation | None,
    rejected_at: datetime,
) -> ResultDecision | LineageWriteRejection:
    """The result manifest of a unit generation is fixed once, by its current fence holder."""

    if generation is None:
        raise CheckpointLineageConflict("no attempt observation precedes this result")
    if result.binding_id != generation.binding_id:
        raise CheckpointLineageConflict("result belongs to another frozen binding")
    if existing is not None and existing.content_digest == result.content_digest:
        return "duplicate"
    rejection = stale_write_rejection(
        request_scope=result.request_scope,
        unit_key=result.unit_key,
        execution_generation=result.execution_generation,
        presented_fence=result.claim_fence,
        generation=generation,
        max_generation=max_generation,
        payload_digest=result.content_digest,
        rejected_at=rejected_at,
    )
    if rejection is not None:
        return rejection
    if existing is not None:
        raise CheckpointLineageConflict(
            "a different result manifest is already recorded for this unit generation"
        )
    return "accept"


def decide_incident_opening(
    incident: UnitReconciliationIncident, latest: UnitReconciliationIncident | None
) -> bool:
    """Whether `incident` becomes the unit generation's current incident.

    The first incident opens revision 1. A later revision opens only on top of a resolved
    one (an accepted decision that could not be applied). Any other opener is answered
    with the current incident, so concurrent openers converge on one revision.
    """

    if latest is None:
        return incident.revision == 1
    return incident.revision == latest.revision + 1 and latest.status == "resolved"


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


def resolve_incident(
    incident: UnitReconciliationIncident, decision: UnitReconciliationDecision
) -> UnitReconciliationIncident:
    """Apply an accepted `reconcile_unit` decision to its incident, idempotently."""

    if (
        decision.incident_id != incident.incident_id
        or decision.unit_key != incident.unit_key
        or decision.execution_generation != incident.execution_generation
    ):
        raise CheckpointLineageConflict("reconciliation decision targets another incident")
    resolved = incident.model_copy(
        update={
            "status": "resolved",
            "decision": decision.decision,
            "decision_id": decision.decision_id,
            "accepted_checkpoint": decision.accepted_checkpoint,
        }
    )
    resolved = UnitReconciliationIncident.model_validate(resolved.model_dump(mode="python"))
    if incident.status == "resolved":
        if incident != resolved:
            raise CheckpointLineageConflict("the incident was already resolved differently")
        return incident
    return resolved


# --- In-memory repository (tests and single-process composition) ---


class InMemoryCheckpointLineageRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.units: dict[tuple[str, str], RuntimeUnitIdentity] = {}
        self.generations: dict[tuple[str, str, int], UnitGenerationRecord] = {}
        self.namespaces: dict[tuple[str, str], NamespaceRecord] = {}
        self.attempts: dict[tuple[str, str], ActivityAttemptObservation] = {}
        self.transitions: dict[tuple[str, str, int], CheckpointTransitionObservation] = {}
        self.results: dict[tuple[str, str, int], UnitResultObservation] = {}
        self.incidents: dict[tuple[str, str, int], UnitReconciliationIncident] = {}
        self.incident_history: list[UnitReconciliationIncident] = []
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
        lease_expires_at: datetime | None = None,
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
            holder = lease_holder_id(scope, unit_key, execution_generation, attempt)
            lease: LeaseDecision | None = None
            if lease_expires_at is not None and dispatching:
                lease = decide_lease(
                    current, holder=holder, requested_until=lease_expires_at, now=observed_at
                )
                dispatching = lease.granted
            existing = self.transitions.get(generation_key)
            expected_source: QualifiedCheckpointKey | None = None
            namespace_update: tuple[tuple[str, str], NamespaceRecord] | None = None
            if namespace is not None:
                namespace_key = (scope, namespace.namespace)
                record = self.namespaces.get(namespace_key) or NamespaceRecord(
                    namespace=namespace.namespace,
                    owner_kind=namespace.owner_kind,
                    owner_digest=namespace.owner_digest,
                )
                check_namespace_owner(namespace, record)
                if dispatching and existing is None and generation_key not in self.results:
                    record = reserve_in_flight(
                        record, unit_key=unit_key, execution_generation=execution_generation
                    )
                namespace_update = (namespace_key, record)
                expected_source = existing.source_key if existing is not None else record.head
            if namespace_update is not None:
                self.namespaces[namespace_update[0]] = namespace_update[1]
            self.generations[generation_key] = lease.record if lease is not None else current
            stored_generation = self.generations[generation_key]
            prior_dispatch = any(
                item.dispatching and item.observation_id != holder
                for (item_scope, _), item in self.attempts.items()
                if item_scope == scope
                and item.unit_key == unit_key
                and item.execution_generation == execution_generation
            )
            observation = ActivityAttemptObservation(
                request_scope=scope,
                unit_key=unit_key,
                execution_generation=execution_generation,
                claim_fence=stored_generation.claim_fence,
                attempt=attempt,
                binding_id=binding_id,
                namespace=namespace.namespace if namespace else None,
                expected_source=expected_source,
                dispatching=dispatching,
                observed_at=observed_at,
            )
            stored = self.attempts.setdefault((scope, observation.observation_id), observation)
            return AttemptAdmission(
                observation=deepcopy(stored),
                existing_transition=existing,
                lease_granted=lease.granted if lease is not None else True,
                took_over=lease.took_over if lease is not None else False,
                prior_dispatch=prior_dispatch,
                existing_result=deepcopy(self.results.get(generation_key)),
                incident=deepcopy(self.incidents.get(generation_key)),
            )

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
                raise stale_claim_error(decision)
            if decision == "duplicate":
                return deepcopy(self.transitions[generation_key])
            self.transitions[generation_key] = transition
            self.namespaces[namespace_key] = advance_namespace(
                self.namespaces[namespace_key], transition
            )
            return deepcopy(transition)

    async def record_result(
        self,
        result: UnitResultObservation,
        *,
        transition: CheckpointTransitionObservation | None = None,
    ) -> UnitResultObservation:
        scope = result.request_scope
        generation_key = (scope, result.unit_key, result.execution_generation)
        async with self._lock:
            generation = self.generations.get(generation_key)
            max_generation = self._max_generation(scope, result.unit_key)
            now = datetime.now(UTC)
            decision = decide_result(
                result,
                generation=generation,
                max_generation=max_generation,
                existing=self.results.get(generation_key),
                rejected_at=now,
            )
            if isinstance(decision, LineageWriteRejection):
                self.rejections.append(decision)
                raise stale_claim_error(decision)
            if decision == "duplicate":
                return deepcopy(self.results[generation_key])
            assert generation is not None
            namespaces = dict(self.namespaces)
            transitions = dict(self.transitions)
            if transition is not None:
                require_linked_result(result, transition)
                namespace_key = (scope, transition.namespace)
                transition_decision = decide_transition(
                    transition,
                    generation=generation,
                    max_generation=max_generation,
                    namespace=namespaces.get(namespace_key),
                    existing=transitions.get(generation_key),
                    rejected_at=now,
                )
                if isinstance(transition_decision, LineageWriteRejection):
                    self.rejections.append(transition_decision)
                    raise stale_claim_error(transition_decision)
                if transition_decision == "accept":
                    transitions[generation_key] = transition
                    namespaces[namespace_key] = advance_namespace(
                        namespaces[namespace_key], transition
                    )
            elif generation.namespace is not None:
                # A result without a transition (failed, abandoned) never advances the head,
                # but it ends the unit's in-flight invocation: the namespace is not stranded.
                namespace_key = (scope, generation.namespace)
                if namespace_key in namespaces:
                    namespaces[namespace_key] = release_in_flight(
                        namespaces[namespace_key],
                        unit_key=result.unit_key,
                        execution_generation=result.execution_generation,
                    )
            self.namespaces = namespaces
            self.transitions = transitions
            self.results[generation_key] = result
            return deepcopy(result)

    async def get_result(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitResultObservation | None:
        return deepcopy(self.results.get((request_scope, unit_key, execution_generation)))

    async def release_lease(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        holder: str,
        released_at: datetime,
    ) -> None:
        key = (request_scope, unit_key, execution_generation)
        async with self._lock:
            current = self.generations.get(key)
            if current is None or current.lease_holder != holder:
                return
            self.generations[key] = replace(current, lease_expires_at=released_at)

    async def open_incident(
        self, incident: UnitReconciliationIncident
    ) -> UnitReconciliationIncident:
        key = (incident.request_scope, incident.unit_key, incident.execution_generation)
        async with self._lock:
            if decide_incident_opening(incident, self.incidents.get(key)):
                self.incidents[key] = incident
                self.incident_history.append(incident)
            return deepcopy(self.incidents[key])

    async def get_incident(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitReconciliationIncident | None:
        return deepcopy(self.incidents.get((request_scope, unit_key, execution_generation)))

    async def apply_reconciliation(
        self, request_scope: str, decision: UnitReconciliationDecision
    ) -> UnitReconciliationIncident:
        key = (request_scope, decision.unit_key, decision.execution_generation)
        async with self._lock:
            incident = self.incidents.get(key)
            if incident is None:
                raise CheckpointLineageConflict("no in_doubt incident exists for the decision")
            resolved = resolve_incident(incident, decision)
            generation = self.generations.get(key)
            if decision.decision in {"abandon_unit", "start_new_generation"} and (
                generation is not None and generation.namespace is not None
            ):
                namespace_key = (request_scope, generation.namespace)
                if namespace_key in self.namespaces:
                    self.namespaces[namespace_key] = release_in_flight(
                        self.namespaces[namespace_key],
                        unit_key=decision.unit_key,
                        execution_generation=decision.execution_generation,
                    )
            if decision.decision == "start_new_generation" and generation is not None:
                self.generations[key] = replace(generation, superseded=True)
            self.incidents[key] = resolved
            return deepcopy(resolved)

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None:
        return deepcopy(self.transitions.get((request_scope, unit_key, execution_generation)))

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None:
        record = self.namespaces.get((request_scope, namespace))
        return record.head if record is not None else None

    async def get_namespace_in_flight(
        self, request_scope: str, namespace: str
    ) -> tuple[str, int] | None:
        record = self.namespaces.get((request_scope, namespace))
        if record is None or record.in_flight_unit_key is None:
            return None
        assert record.in_flight_generation is not None
        return record.in_flight_unit_key, record.in_flight_generation

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
        """Advance the fence only from the expected value (operator or test takeover seam)."""

        key = (request_scope, unit_key, execution_generation)
        async with self._lock:
            current = self.generations.get(key)
            if current is None or current.claim_fence != expected_fence:
                raise CheckpointLineageConflict("claim fence changed concurrently")
            self.generations[key] = replace(current, claim_fence=expected_fence + 1)
            return expected_fence + 1

    def _max_generation(self, request_scope: str, unit_key: str) -> int | None:
        records = [
            record
            for (scope, key, _generation), record in self.generations.items()
            if scope == request_scope and key == unit_key
        ]
        if not records:
            return None
        latest = max(records, key=lambda record: record.execution_generation)
        return effective_max_generation(latest.execution_generation, latest.superseded)


def lease_holder_id(
    request_scope: str, unit_key: str, execution_generation: int, attempt: OperationActivityAttempt
) -> str:
    """The claim-lease holder identity: the exact Activity attempt observation."""

    return activity_attempt_observation_id(request_scope, unit_key, execution_generation, attempt)


def require_linked_result(
    result: UnitResultObservation, transition: CheckpointTransitionObservation
) -> None:
    if (
        transition.request_scope != result.request_scope
        or transition.unit_key != result.unit_key
        or transition.execution_generation != result.execution_generation
        or transition.claim_fence != result.claim_fence
        or transition.transition_id != result.checkpoint_transition_id
        or transition.result_manifest_ref != result.result_manifest_ref
        or transition.result_manifest_digest != result.result_manifest_digest
    ):
        raise CheckpointLineageConflict("transition and result observation are not one write")


def stale_claim_error(rejection: LineageWriteRejection) -> StaleClaimFence:
    return StaleClaimFence(
        f"write presented a superseded {rejection.reason.removeprefix('stale_')}"
    )


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


@dataclass(frozen=True)
class UnitAttempt:
    """One Activity attempt admitted against its unit generation (EXEC-014, DA-018)."""

    unit: RuntimeUnitIdentity
    execution_generation: int
    attempt: OperationActivityAttempt
    holder: str
    admission: AttemptAdmission
    namespace: NamespaceClaim | None
    deep_binding: DeepAgentExecutionBinding | None
    acquired_at: datetime | None = None
    lease_expires_at: datetime | None = None

    @property
    def fence(self) -> int:
        return self.admission.observation.claim_fence

    def work_budget(self, now: datetime) -> float:
        """Seconds this holder may still work before its lease could be taken over.

        The lease ends at the scheduler's deadline for the attempt (Temporal `started_time`
        plus start-to-close, a server timestamp) or the composition default. The holder
        stops a safety margin earlier: 20% of the lease length, at least 1 s and at most
        30 s. The margin absorbs clock skew between the Temporal server and this worker,
        and the time to release the lease. It is computed once from the wall clock; the
        caller enforces it with the event loop's monotonic timer.
        """

        if self.lease_expires_at is None:
            return float("inf")
        total = (self.lease_expires_at - (self.acquired_at or now)).total_seconds()
        margin = min(max(total * 0.2, 1.0), 30.0)
        return (self.lease_expires_at - now).total_seconds() - margin


class CheckpointLineageService:
    """Builds unit attempt observations, invocation plans, and fenced CAS writes."""

    def __init__(
        self,
        repository: CheckpointLineageRepository,
        *,
        clock: Callable[[], datetime] | None = None,
        default_lease: timedelta = DEFAULT_CLAIM_LEASE,
    ) -> None:
        self._repository = repository
        self._clock = clock or (lambda: datetime.now(UTC))
        self._default_lease = default_lease

    @property
    def repository(self) -> CheckpointLineageRepository:
        return self._repository

    def now(self) -> datetime:
        return self._clock()

    def unit_generation(
        self, binding: OperationExecutionBinding
    ) -> tuple[RuntimeUnitIdentity, int]:
        unit, generation, _namespace, _deep_binding = self._resolve(binding)
        return unit, generation

    async def observe_attempt(
        self,
        binding: OperationExecutionBinding,
        attempt: OperationActivityAttempt,
        *,
        dispatching: bool,
    ) -> CheckpointInvocationPlan | None:
        """RRM-003 seam for adapter-level harnesses: observe, then plan a submission only."""

        unit, generation, namespace, deep_binding = self._resolve(binding)
        admission = await self._repository.record_attempt(
            unit=unit,
            execution_generation=generation,
            attempt=attempt,
            binding_id=binding.binding_id,
            binding_digest=_binding_digest(binding, deep_binding),
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
        return _plan(unit, generation, admission, namespace, deep_binding, None)

    async def admit_attempt(
        self,
        binding: OperationExecutionBinding,
        attempt: OperationActivityAttempt,
    ) -> UnitAttempt:
        """Record the attempt and try to hold the claim lease before any provider work."""

        unit, generation, namespace, deep_binding = self._resolve(binding)
        if namespace is not None:
            await self._refuse_sealed_head(unit, generation, namespace)
        now = self._clock()
        lease_until = attempt.lease_expires_at or (now + self._default_lease)
        admission = await self._repository.record_attempt(
            unit=unit,
            execution_generation=generation,
            attempt=attempt,
            binding_id=binding.binding_id,
            binding_digest=_binding_digest(binding, deep_binding),
            namespace=namespace,
            dispatching=True,
            observed_at=now,
            lease_expires_at=lease_until,
        )
        return UnitAttempt(
            unit=unit,
            execution_generation=generation,
            attempt=attempt,
            holder=lease_holder_id(unit.request_scope, unit.unit_key, generation, attempt),
            admission=admission,
            namespace=namespace,
            deep_binding=deep_binding,
            acquired_at=now,
            lease_expires_at=lease_until,
        )

    async def _refuse_sealed_head(
        self, unit: RuntimeUnitIdentity, generation: int, namespace: NamespaceClaim
    ) -> None:
        """RRM-008 (REQ-BP-GD-012, REQ-CP-DA-017): never pin a unit to unverified cognition.

        A unit's expected source is the namespace head. When the transition that produced
        it is not seedable (a `failed` or `timed_out` settlement advanced the head for
        bookkeeping only), a later unit is refused before its attempt is recorded, so it
        holds no lease and reserves no in-flight marker: a shared session continues only in
        a new session generation, never by branching the sealed thread.

        The unit generation that sealed the head is not a later unit: its retry must still
        settle exactly its recorded result (`observed_unsettled`, REQ-CP-EXEC-011/014) when
        the worker was lost between the result observation and the journal settlement. It is
        admitted and classifies from its own transition and result; it never re-dispatches.
        """

        expected = await self._repository.get_namespace_head(
            unit.request_scope, namespace.namespace
        )
        if expected is None:
            return
        head = next(
            (
                transition
                for transition in await self._repository.list_transitions(
                    unit.request_scope, namespace.namespace
                )
                if transition.result_key == expected
            ),
            None,
        )
        if head is None or head.seedable:
            return
        if head.unit_key == unit.unit_key and head.execution_generation == generation:
            return
        raise CheckpointLineageConflict(
            "the session namespace head is sealed: the previous unit settled "
            f"{head.result_manifest_ref!r} without verified cognition; the session continues "
            "only in a new session generation (REQ-BP-GD-012)"
        )

    def plan(
        self,
        admitted: UnitAttempt,
        *,
        accepted_leaf: QualifiedCheckpointKey | None = None,
    ) -> CheckpointInvocationPlan | None:
        if admitted.deep_binding is None or admitted.namespace is None:
            return None
        return _plan(
            admitted.unit,
            admitted.execution_generation,
            admitted.admission,
            admitted.namespace,
            admitted.deep_binding,
            accepted_leaf,
        ).model_copy(update={"attempt_ref": admitted.holder})

    async def release(self, admitted: UnitAttempt) -> None:
        await self._repository.release_lease(
            admitted.unit.request_scope,
            admitted.unit.unit_key,
            admitted.execution_generation,
            holder=admitted.holder,
            released_at=self._clock(),
        )

    async def record_result(
        self,
        admitted: UnitAttempt,
        *,
        binding_id: str,
        settlement_id: str,
        status: Literal["completed", "failed", "cancelled", "timed_out"],
        result_manifest_ref: str,
        result_manifest_digest: str,
        result_manifest_size_bytes: int,
        plan: CheckpointInvocationPlan | None = None,
        capture: CheckpointCapture | None = None,
    ) -> UnitResultObservation:
        """Fix the unit generation's result manifest by its current fence (EXEC-014)."""

        transition = (
            self._transition(
                plan,
                capture,
                result_manifest_ref=result_manifest_ref,
                result_manifest_digest=result_manifest_digest,
                # A completed unit's result and a cancelled unit's partial evidence
                # (REQ-CP-EXEC-008) may seed the next unit; failed cognition may not.
                seedable=status in {"completed", "cancelled"},
            )
            if plan is not None and capture is not None
            else None
        )
        unit = admitted.unit
        result = UnitResultObservation(
            observation_id=unit_result_observation_id(
                unit.request_scope, unit.unit_key, admitted.execution_generation
            ),
            request_scope=unit.request_scope,
            unit_key=unit.unit_key,
            execution_generation=admitted.execution_generation,
            claim_fence=admitted.fence,
            binding_id=binding_id,
            settlement_id=settlement_id,
            status=status,
            result_manifest_ref=result_manifest_ref,
            result_manifest_digest=result_manifest_digest,
            result_manifest_size_bytes=result_manifest_size_bytes,
            checkpoint_transition_id=(transition.transition_id if transition is not None else None),
            observed_at=self._clock(),
        )
        return await self._repository.record_result(result, transition=transition)

    async def open_incident(
        self,
        admitted: UnitAttempt,
        *,
        binding: OperationExecutionBinding,
        reason: InDoubtReason,
        candidates: tuple[QualifiedCheckpointKey, ...] = (),
        unsettled_effect_ids: tuple[str, ...] = (),
    ) -> UnitReconciliationIncident:
        unit = admitted.unit
        prior = admitted.admission.incident
        # An in_doubt classification after an accepted decision (for example an accepted
        # descendant that no longer classifies) opens the next revision with its own wait.
        revision = (
            1
            if prior is None
            else prior.revision + 1
            if prior.status == "resolved"
            else prior.revision
        )
        return await self._repository.open_incident(
            UnitReconciliationIncident(
                incident_id=unit_incident_id(
                    unit.request_scope, unit.unit_key, admitted.execution_generation, revision
                ),
                revision=revision,
                request_scope=unit.request_scope,
                belllabs_run_id=unit.belllabs_run_id,
                unit_key=unit.unit_key,
                execution_generation=admitted.execution_generation,
                binding_id=binding.binding_id,
                operation_workflow_id=admitted.attempt.workflow_id,
                reason=reason,
                namespace=admitted.namespace.namespace if admitted.namespace else None,
                expected_source=admitted.admission.observation.expected_source,
                candidates=candidates[:64],
                unsettled_effect_ids=unsettled_effect_ids[:64],
                recorded_at=self._clock(),
            )
        )

    async def record_transition(
        self,
        plan: CheckpointInvocationPlan,
        capture: CheckpointCapture,
        *,
        result_manifest_ref: str,
        result_manifest_digest: str,
    ) -> CheckpointTransitionObservation:
        return await self._repository.record_transition(
            self._transition(
                plan,
                capture,
                result_manifest_ref=result_manifest_ref,
                result_manifest_digest=result_manifest_digest,
            )
        )

    def _transition(
        self,
        plan: CheckpointInvocationPlan,
        capture: CheckpointCapture,
        *,
        result_manifest_ref: str,
        result_manifest_digest: str,
        seedable: bool = True,
    ) -> CheckpointTransitionObservation:
        if (
            capture.namespace != plan.namespace
            or capture.invocation_id != plan.invocation_id
            or capture.source_key != plan.expected_source
        ):
            raise CheckpointLineageConflict(
                "captured checkpoint lineage is not the planned invocation's"
            )
        return CheckpointTransitionObservation(
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
            classification=capture.classification,
            invocation_id=plan.invocation_id,
            result_manifest_ref=result_manifest_ref,
            result_manifest_digest=result_manifest_digest,
            redacted_summary_digest=capture.redacted_summary_digest,
            seedable=seedable,
            observed_at=self._clock(),
        )

    @staticmethod
    def _resolve(
        binding: OperationExecutionBinding,
    ) -> tuple[RuntimeUnitIdentity, int, NamespaceClaim | None, DeepAgentExecutionBinding | None]:
        unit = binding.runtime_unit
        if unit is None:
            raise ValueError("lineage-qualified execution requires a runtime unit (EXEC-013)")
        deep_binding = binding.deep_agent_binding
        if deep_binding is None:
            return unit, 1, None, None
        generation = deep_binding.execution_generation
        bound_namespace = deep_binding.cognitive_session_namespace
        if deep_binding.runtime_unit != unit or bound_namespace is None:
            raise ValueError("Deep Agent binding lacks its frozen runtime unit namespace")
        if bound_namespace != cognitive_session_namespace(unit, generation):
            raise CheckpointNamespaceOwnershipError(
                "binding namespace differs from the unit generation's namespace"
            )
        owner_kind, owner_digest = namespace_owner(unit, generation)
        return (
            unit,
            generation,
            NamespaceClaim(
                namespace=bound_namespace,
                owner_kind=owner_kind,
                owner_digest=owner_digest,
                binding_digest=deep_binding.binding_digest,
                state_schema_digest=deep_binding.cognitive_state_schema.schema_digest,
            ),
            deep_binding,
        )


def _binding_digest(
    binding: OperationExecutionBinding, deep_binding: DeepAgentExecutionBinding | None
) -> str:
    return deep_binding.binding_digest if deep_binding is not None else sha256_digest(binding)


def _plan(
    unit: RuntimeUnitIdentity,
    generation: int,
    admission: AttemptAdmission,
    namespace: NamespaceClaim,
    deep_binding: DeepAgentExecutionBinding,
    accepted_leaf: QualifiedCheckpointKey | None,
) -> CheckpointInvocationPlan:
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
        accepted_leaf=accepted_leaf,
    )


def transition_id_for(plan: CheckpointInvocationPlan) -> str:
    return checkpoint_transition_id(plan.request_scope, plan.unit_key, plan.execution_generation)
