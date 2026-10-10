"""What a lane may expose to the continuation runtime (MP-12; implemented by MP-07/08/09).

Three optional, runtime-checkable protocols beside the lane's :class:`SessionHydrator`:

- :class:`ContextOccupancyLane` reports the live context occupancy of a session when the
  provider exposes it with a known model window (Codex ``thread/tokenUsage/updated`` with
  ``modelContextWindow``; Claude's last-turn input tokens against the model window). A lane
  that cannot read it returns ``None`` and the policy reports ``unknown``: a cumulative
  billed total is never passed as occupancy.
- :class:`CompactingLane` runs the provider's *explicit* compaction operation. It is taken
  only when the lane's describe says ``compaction_control == "native"`` (Codex documents
  explicit thread compaction; Claude and Cursor stay ``unqualified`` and fall back to the
  sealed checkpoint). An observed ``before_compaction`` hook is not a control API and does
  not satisfy this protocol.
- :class:`LaneSessionActivation` is the lane-neutral record of the activated target: the
  lane execution state's native session moves from the source to the target by an explicit
  supersession, which is what fences the old generation after activation.

Pure application code: no provider SDK, no Temporal.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.context.pressure import ContextOccupancy
from mission_control.domain.execution.lanes import SessionHandle, TurnHandle


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CompactionRequest(_Contract):
    harness_execution_id: str = Field(min_length=1)
    session: SessionHandle
    turn: TurnHandle | None = None
    epoch: int = Field(ge=1)
    """The compaction epoch this request opens (1 + completed compactions so far)."""
    reason: str = Field(default="context_pressure", min_length=1, max_length=128)


class CompactionReceipt(_Contract):
    """What the provider reported for an explicit compaction; nothing here is inferred."""

    completed: bool
    native_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    summary_digest: str | None = None
    occupancy_after: ContextOccupancy | None = None
    detail: str = Field(default="", max_length=512)


@runtime_checkable
class ContextOccupancyLane(Protocol):
    async def context_occupancy(
        self, harness_execution_id: str, session: SessionHandle, turn: TurnHandle | None
    ) -> ContextOccupancy | None:
        """The live context occupancy, or ``None`` when the provider exposes none."""
        ...


@runtime_checkable
class CompactingLane(Protocol):
    async def compact(self, request: CompactionRequest) -> CompactionReceipt:
        """The provider's explicit compaction of the session (serialized with turn admission
        by the caller: never while a turn runs)."""
        ...


class LaneSessionActivation(Protocol):
    async def activate(
        self,
        request_scope: str,
        harness_execution_id: str,
        *,
        source_session_ref: str,
        target_session_ref: str,
    ) -> None:
        """Record the activated target as the execution's native session, superseding the
        source (compare-and-set on the recorded identity)."""
        ...


class ContinuationInFlight(RuntimeError):
    """``continuation_in_flight``: the session is frozen for a continuation; no new agent
    action is dispatched to it until the target is activated or the transfer ends."""

    code = "continuation_in_flight"

    def __init__(self, transfer_id: str, phase: str) -> None:
        super().__init__(f"continuation {transfer_id} is {phase}; the session is frozen")
        self.transfer_id = transfer_id
        self.phase = phase


__all__ = [
    "CompactingLane",
    "CompactionReceipt",
    "CompactionRequest",
    "ContextOccupancyLane",
    "ContinuationInFlight",
    "LaneSessionActivation",
]
