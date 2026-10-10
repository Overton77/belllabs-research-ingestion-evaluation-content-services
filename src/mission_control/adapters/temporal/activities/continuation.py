"""The continuation activities (SPEC-02 B4; MP-12 phase machine).

A family workflow calls ``continuation.seal`` at the next safe boundary after a trigger
(workflow-types/08 section 9 up to the seal), then ``continuation.transfer`` to provision and
hydrate the fresh session through the lane's :class:`SessionHydrator`. Both are idempotent:
the transfer row and the checkpoint id make a retried activity return the recorded outcome.
``continuation.release`` re-checks hydration and releases held mailbox commands once the
target session's first ``session_init`` frame exists.

MP-12 adds ``continuation.advance``: the operation workflow's call site for the persisted
phase machine (``requested -> frozen -> snapshotted -> sealed -> target_prepared -> hydrated
-> verified -> activated``). One call enters exactly one phase and returns the transfer's
phase and status; a retried call after a crash finds the recorded phase and continues from
it, so every phase is idempotent and only one target generation ever activates.

The activities hold no lane logic: ``services`` resolves the scoped
:class:`ContinuationService`, ``hydrators`` the lane's hydrator (the per-lane registry of
``application/context/hydrators.py``) and ``phases`` the scoped phase service.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity
from temporalio.exceptions import ApplicationError

from mission_control.application.context.continuation import (
    ContinuationRejected,
    ContinuationService,
    ContinuationTransfer,
    SealOutcome,
    SealTarget,
    SessionHydrator,
    TransferOutcome,
)
from mission_control.application.context.facts import LaneFactsContext
from mission_control.application.context.lane_continuation import LaneContinuationCoordinator
from mission_control.application.context.phases import ContinuationPhaseService
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.state import LaneExecutionStateStore
from mission_control.application.frames.sink import FrameReader
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    ContinuationFacts,
    ContinuationTrigger,
)
from mission_control.domain.context.phases import ContinuationPhase
from mission_control.domain.execution.contracts import OperationExecutionRequest

CONTINUATION_REQUEST_ACTIVITY = "continuation.request"
CONTINUATION_SEAL_ACTIVITY = "continuation.seal"
CONTINUATION_TRANSFER_ACTIVITY = "continuation.transfer"
CONTINUATION_RELEASE_ACTIVITY = "continuation.release"
CONTINUATION_ADVANCE_ACTIVITY = "continuation.advance"
CONTINUATION_PENDING_ACTIVITY = "continuation.pending"
NON_RETRYABLE_CODES = frozenset(
    {
        CHECKPOINT_INVALID,
        "unsupported_control",
        "not_found",
        "continuation_generation_conflict",
        "continuation_phase_rejected",
    }
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContinuationRequestInput(_Input):
    request_scope: str = Field(min_length=1)
    run_key: str = Field(min_length=1)
    activation_key: str = Field(min_length=1)
    logical_execution_id: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    source_session_ref: str = Field(min_length=1)
    trigger: ContinuationTrigger


class ContinuationSealInput(_Input):
    request_scope: str = Field(min_length=1)
    transfer_id: str = Field(min_length=1)
    facts: ContinuationFacts
    target: SealTarget


class ContinuationTransferInput(_Input):
    request_scope: str = Field(min_length=1)
    transfer_id: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)


class ContinuationPendingInput(_Input):
    """Which transfer the lane reported at the safe boundary (the workflow learns the id)."""

    request_scope: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    operation: OperationExecutionRequest
    generation: int = Field(ge=1)
    turn_no: int = Field(ge=1)
    source_session_ref: str | None = Field(default=None, min_length=1)

    @property
    def identity(self) -> LaneExecutionIdentity:
        return LaneExecutionIdentity.of(self.operation, self.lane_profile, self.generation)


class ContinuationPendingOutcome(_Input):
    transfer_id: str | None = None
    phase: ContinuationPhase | None = None
    status: str | None = None


class ContinuationAdvanceInput(ContinuationPendingInput):
    """One phase of one transfer, from the operation workflow's safe boundary."""

    transfer_id: str = Field(min_length=1)


class ContinuationAdvanceOutcome(_Input):
    """What the workflow needs to decide its next step; never the checkpoint body."""

    transfer_id: str
    phase: ContinuationPhase
    status: str
    activated: bool
    ended: bool
    target_session_ref: str | None = None
    target_turn_no: int | None = None
    packet_digest: str | None = None
    workspace_manifest_digest: str | None = None
    failure_reason: str | None = None

    @classmethod
    def of(cls, transfer: ContinuationTransfer) -> ContinuationAdvanceOutcome:
        return cls(
            transfer_id=transfer.transfer_id,
            phase=transfer.phase,
            status=transfer.status.value,
            activated=transfer.activated,
            ended=transfer.ended,
            target_session_ref=transfer.target_session_ref,
            target_turn_no=transfer.target_turn_no,
            packet_digest=transfer.packet_digest,
            workspace_manifest_digest=transfer.workspace_manifest_digest,
            failure_reason=transfer.failure_reason,
        )


class ContinuationServices(Protocol):
    def __call__(self, request_scope: str) -> ContinuationService: ...


class LaneHydrators(Protocol):
    def __call__(self, lane_profile: str, request_scope: str) -> SessionHydrator: ...


class PhaseServices(Protocol):
    def __call__(self, request_scope: str) -> ContinuationPhaseService: ...


class LaneCoordinators(Protocol):
    def __call__(self, request_scope: str) -> LaneContinuationCoordinator: ...


class ContinuationActivities:
    def __init__(
        self,
        services: ContinuationServices | Callable[[str], ContinuationService],
        hydrators: LaneHydrators | Callable[[str, str], SessionHydrator] | None = None,
        *,
        phases: PhaseServices | Callable[[str], ContinuationPhaseService] | None = None,
        coordinators: LaneCoordinators | Callable[[str], LaneContinuationCoordinator] | None = None,
        states: LaneExecutionStateStore | None = None,
        frames: FrameReader | None = None,
    ) -> None:
        self._services = services
        self._hydrators = hydrators
        self._phases = phases
        self._coordinators = coordinators
        self._states = states
        self._frames = frames

    @activity.defn(name=CONTINUATION_REQUEST_ACTIVITY)
    async def request(self, request: ContinuationRequestInput) -> ContinuationTransfer:
        return await _guard(
            self._services(request.request_scope).request(
                request.trigger,
                request_scope=request.request_scope,
                run_key=request.run_key,
                activation_key=request.activation_key,
                logical_execution_id=request.logical_execution_id,
                lane_profile=request.lane_profile,
                source_session_ref=request.source_session_ref,
            )
        )

    @activity.defn(name=CONTINUATION_SEAL_ACTIVITY)
    async def seal(self, request: ContinuationSealInput) -> SealOutcome:
        return await _guard(
            self._services(request.request_scope).seal(
                request.transfer_id,
                request.facts,
                request.target,
                request_scope=request.request_scope,
            )
        )

    @activity.defn(name=CONTINUATION_TRANSFER_ACTIVITY)
    async def transfer(self, request: ContinuationTransferInput) -> TransferOutcome:
        if self._hydrators is None:
            raise ApplicationError(
                "no lane hydrator is registered in this worker",
                type="unsupported_control",
                non_retryable=True,
            )
        hydrator = self._hydrators(request.lane_profile, request.request_scope)
        return await _guard(
            self._services(request.request_scope).transfer(
                request.transfer_id, hydrator, request_scope=request.request_scope
            )
        )

    @activity.defn(name=CONTINUATION_RELEASE_ACTIVITY)
    async def release(self, request: ContinuationTransferInput) -> ContinuationTransfer:
        return await _guard(
            self._services(request.request_scope).release_if_hydrated(
                request.transfer_id, request_scope=request.request_scope
            )
        )

    @activity.defn(name=CONTINUATION_PENDING_ACTIVITY)
    async def pending(self, request: ContinuationPendingInput) -> ContinuationPendingOutcome:
        """The transfer the lane reported at the safe boundary (MP-12): the one requested for
        the source session, else the one already in flight for this harness execution."""

        if self._coordinators is None:
            raise ApplicationError(
                "no continuation coordinator is composed in this worker",
                type="unsupported_control",
                non_retryable=True,
            )
        coordinator = self._coordinators(request.request_scope)
        identity = request.identity
        found = None
        if request.source_session_ref is not None:
            found = await coordinator.pending(
                request.request_scope,
                identity.run_key,
                source_session_ref=request.source_session_ref,
            )
        if found is None:
            latest = await coordinator.latest_for_execution(
                request.request_scope,
                identity.run_key,
                harness_execution_id=str(identity.harness_execution_id),
            )
            found = latest if latest is not None and latest.open else None
        if found is None:
            return ContinuationPendingOutcome()
        return ContinuationPendingOutcome(
            transfer_id=found.transfer_id, phase=found.phase, status=found.status.value
        )

    @activity.defn(name=CONTINUATION_ADVANCE_ACTIVITY)
    async def advance(self, request: ContinuationAdvanceInput) -> ContinuationAdvanceOutcome:
        """Enter the next phase of the transfer once (MP-12)."""

        if self._phases is None:
            raise ApplicationError(
                "no continuation phase service is composed in this worker",
                type="unsupported_control",
                non_retryable=True,
            )
        phases = self._phases(request.request_scope)
        heid = request.identity.harness_execution_id
        state = (
            await self._states.load(request.request_scope, heid)
            if self._states is not None
            else None
        )
        frames = (
            await self._frames.frames_for_execution(
                request.request_scope, heid, request.generation, limit=10_000
            )
            if self._frames is not None
            else ()
        )
        context = LaneFactsContext(
            operation=request.operation,
            harness_execution_id=str(heid),
            generation=request.generation,
            turn_no=request.turn_no,
            lane_state=state,
            frames=frames,
        )
        transfer = await _guard(
            phases.advance(request.transfer_id, context, request_scope=request.request_scope)
        )
        activity.heartbeat({"phase": transfer.phase.value})
        return ContinuationAdvanceOutcome.of(transfer)

    def all(self) -> tuple[Callable[..., object], ...]:
        return (
            self.request,
            self.seal,
            self.transfer,
            self.release,
            self.pending,
            self.advance,
        )


async def _guard[T](operation: Awaitable[T]) -> T:
    try:
        return await operation
    except ContinuationRejected as error:
        raise ApplicationError(
            str(error), type=error.code, non_retryable=error.code in NON_RETRYABLE_CODES
        ) from error
