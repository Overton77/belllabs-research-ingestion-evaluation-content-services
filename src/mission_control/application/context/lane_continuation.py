"""What ``lane.turn`` reads and records about continuations (MP-12).

The lane turn service does not drive the phase machine (the operation workflow does,
through ``continuation.advance``); it observes context pressure at every safe boundary,
records the worker-side triggers, honours the fence of an in-flight transfer and sends the
activated target's first turn. This coordinator is its one door to the continuation ledger:

- :meth:`pending` - a requested transfer for the session that just reached a safe boundary
  (an API ``request_continuation``, or a trigger this worker recorded);
- :meth:`fencing` - the transfer that currently freezes a harness execution, if any;
- :meth:`activated_for_turn` - the activated transfer whose first turn is ``turn_no``;
- :meth:`hydration_prompt` - the continuation turn's text (the packet's ``admitted_input``);
- :meth:`request` - a ``context_health_soft`` / ``context_health_hard`` trigger;
- :meth:`assess` - the context policy over the lane's occupancy (or its absence).

:class:`ContinuationHoldOracle` is the mailbox's view: while any transfer of a run fences
its source, the boundary delivers nothing (the held commands wait for the activation).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from mission_control.application.context.continuation import (
    CheckpointRepository,
    ContinuationRejected,
    ContinuationService,
    ContinuationTransfer,
    ContinuationTransferRepository,
    ContinuationTriggers,
    PacketReader,
)
from mission_control.application.execution.harness.state import (
    LaneExecutionStateStore,
    LaneExecutionUpdate,
)
from mission_control.domain.context.checkpoint import ContinuationTrigger, ContinuationTriggerKind
from mission_control.domain.context.phases import ContinuationPhase
from mission_control.domain.context.pressure import (
    ContextOccupancy,
    ContextPressurePolicy,
    PressureAssessment,
    assess_pressure,
)
from mission_control.domain.context.render import render_prompt_segment

CONTINUATION_INSTRUCTION_PREFIX = "continuation:"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class LaneContinuationCoordinator:
    def __init__(
        self,
        transfers: ContinuationTransferRepository,
        *,
        policy: ContextPressurePolicy | None = None,
        triggers: ContinuationTriggers | ContinuationService | None = None,
        checkpoints: CheckpointRepository | None = None,
        packets: PacketReader | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._transfers = transfers
        self._policy = policy or ContextPressurePolicy()
        self._triggers = triggers or ContinuationTriggers(transfers, clock=clock)
        self._checkpoints = checkpoints
        self._packets = packets
        self._clock = clock

    @property
    def policy(self) -> ContextPressurePolicy:
        return self._policy

    def assess(self, occupancy: ContextOccupancy, *, turns_in_session: int) -> PressureAssessment:
        return assess_pressure(self._policy, occupancy, turns_in_session=turns_in_session)

    async def pending(
        self, request_scope: str, run_key: str, *, source_session_ref: str
    ) -> ContinuationTransfer | None:
        """The requested (not yet frozen) transfer of this native session, oldest first."""

        for item in await self._transfers.for_run(request_scope, run_key):
            if (
                item.open
                and item.phase == ContinuationPhase.REQUESTED
                and item.source_session_ref == source_session_ref
            ):
                return item
        return None

    async def fencing(
        self, request_scope: str, run_key: str, *, harness_execution_id: str
    ) -> ContinuationTransfer | None:
        for item in await self._transfers.for_run(request_scope, run_key):
            if item.fencing and item.harness_execution_id == harness_execution_id:
                return item
        return None

    async def activated_for_turn(
        self, request_scope: str, run_key: str, *, harness_execution_id: str, turn_no: int
    ) -> ContinuationTransfer | None:
        for item in await self._transfers.for_run(request_scope, run_key):
            if (
                item.activated
                and item.harness_execution_id == harness_execution_id
                and item.target_turn_no == turn_no
            ):
                return item
        return None

    async def latest_for_execution(
        self, request_scope: str, run_key: str, *, harness_execution_id: str
    ) -> ContinuationTransfer | None:
        found = [
            item
            for item in await self._transfers.for_run(request_scope, run_key)
            if item.harness_execution_id == harness_execution_id
        ]
        return max(found, key=lambda item: item.requested_at) if found else None

    async def request(
        self,
        kind: ContinuationTriggerKind,
        ref: str,
        *,
        request_scope: str,
        run_key: str,
        activation_key: str,
        logical_execution_id: str,
        lane_profile: str,
        source_session_ref: str,
    ) -> ContinuationTransfer:
        return await self._triggers.request(
            ContinuationTrigger(kind=kind, ref=ref[:1_024], observed_at=self._clock()),
            request_scope=request_scope,
            run_key=run_key,
            activation_key=activation_key,
            logical_execution_id=logical_execution_id,
            lane_profile=lane_profile,
            source_session_ref=source_session_ref,
        )

    async def hydration_prompt(self, transfer: ContinuationTransfer) -> str | None:
        """The continuation turn's text, rendered from the sealed packet; ``None`` when the
        coordinator has no checkpoint or packet reader composed."""

        if self._checkpoints is None or self._packets is None or transfer.checkpoint_id is None:
            return None
        stored = await self._checkpoints.get(
            transfer.request_scope, transfer.run_key, transfer.checkpoint_id
        )
        if stored is None:
            raise ContinuationRejected("not_found", "the sealed checkpoint is missing")
        packet_id = stored.checkpoint.context_packet_ref.removeprefix("context_packet:").rsplit(
            "#", 1
        )[0]
        packet = await self._packets.get(packet_id, request_scope=transfer.request_scope)
        if packet is None:
            raise ContinuationRejected("not_found", "the continuation packet is missing")
        return render_prompt_segment(packet).content


class LaneStateActivation:
    """``LaneSessionActivation`` over the lane execution state store: the activated target
    becomes the execution's native session by an explicit supersession of the source."""

    def __init__(self, states: LaneExecutionStateStore) -> None:
        self._states = states

    async def activate(
        self,
        request_scope: str,
        harness_execution_id: str,
        *,
        source_session_ref: str,
        target_session_ref: str,
    ) -> None:
        heid = UUID(harness_execution_id)
        current = await self._states.load(request_scope, heid)
        if current is not None and current.native_session_ref == target_session_ref:
            return  # recorded by an earlier attempt of the same activation
        await self._states.record(
            request_scope,
            heid,
            LaneExecutionUpdate(
                native_session_ref=target_session_ref, supersedes_session_ref=source_session_ref
            ),
        )


class ContinuationHoldOracle:
    """``HeldCommandsPort`` for the mailbox: the transfer currently holding a run's commands."""

    def __init__(self, transfers: ContinuationTransferRepository) -> None:
        self._transfers = transfers

    async def open_hold(self, request_scope: str, run_id: str) -> str | None:
        for item in await self._transfers.for_run(request_scope, run_id):
            if item.fencing:
                return item.transfer_id
        return None


def continuation_instruction_ref(transfer_id: str) -> str:
    return f"{CONTINUATION_INSTRUCTION_PREFIX}{transfer_id}"


def is_continuation_instruction(instruction_ref: str | None) -> bool:
    return instruction_ref is not None and instruction_ref.startswith(
        CONTINUATION_INSTRUCTION_PREFIX
    )


__all__ = [
    "CONTINUATION_INSTRUCTION_PREFIX",
    "ContinuationHoldOracle",
    "LaneContinuationCoordinator",
    "LaneStateActivation",
    "continuation_instruction_ref",
    "is_continuation_instruction",
]
