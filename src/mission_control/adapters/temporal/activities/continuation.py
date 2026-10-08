"""``continuation.seal`` and ``continuation.transfer`` as Temporal activities (SPEC-02, B4).

A family workflow calls ``continuation.seal`` at the next safe boundary after a trigger
(workflow-types/08 section 9 up to the seal), then ``continuation.transfer`` to provision and
hydrate the fresh session through the lane's :class:`SessionHydrator`. Both are idempotent:
the transfer row and the checkpoint id make a retried activity return the recorded outcome.
``continuation.release`` re-checks hydration and releases held mailbox commands once the
target session's first ``session_init`` frame exists.

The activities hold no lane logic: ``services`` resolves the scoped
:class:`ContinuationService` and ``hydrators`` the lane's hydrator (Deep Agents in B4; the
Cursor profiles register theirs in FT-G4).
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
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    ContinuationFacts,
    ContinuationTrigger,
)

CONTINUATION_REQUEST_ACTIVITY = "continuation.request"
CONTINUATION_SEAL_ACTIVITY = "continuation.seal"
CONTINUATION_TRANSFER_ACTIVITY = "continuation.transfer"
CONTINUATION_RELEASE_ACTIVITY = "continuation.release"
NON_RETRYABLE_CODES = frozenset({CHECKPOINT_INVALID, "unsupported_control", "not_found"})


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


class ContinuationServices(Protocol):
    def __call__(self, request_scope: str) -> ContinuationService: ...


class LaneHydrators(Protocol):
    def __call__(self, lane_profile: str, request_scope: str) -> SessionHydrator: ...


class ContinuationActivities:
    def __init__(
        self,
        services: ContinuationServices | Callable[[str], ContinuationService],
        hydrators: LaneHydrators | Callable[[str, str], SessionHydrator] | None = None,
    ) -> None:
        self._services = services
        self._hydrators = hydrators

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

    def all(self) -> tuple[Callable[..., object], ...]:
        return (self.request, self.seal, self.transfer, self.release)


async def _guard[T](operation: Awaitable[T]) -> T:
    try:
        return await operation
    except ContinuationRejected as error:
        raise ApplicationError(
            str(error), type=error.code, non_retryable=error.code in NON_RETRYABLE_CODES
        ) from error
