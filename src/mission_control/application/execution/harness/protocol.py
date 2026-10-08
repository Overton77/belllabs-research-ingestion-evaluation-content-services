"""The provider-neutral AgentHarness protocol (SPEC-07 section 1, ADR-0018; FT-G1).

Every lane implements `describe` plus nine operations. A lane that declares an operation
`unsupported` (or `unqualified`) in its describe raises `HarnessUnsupported` for it; any other
error from such an operation is a conformance failure. `observe` is resumable by an opaque,
lane-specific cursor (LangGraph checkpoint id, bridge offset, SSE event id).

`OperationLane` is what `OperationExecutionService` needs from a registered lane today: the
bounded `execute` path of the `RuntimePort` family (the Deep Agents lane keeps it until the
`lane.turn` activities of FT-G2 drive every lane through the protocol).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol, runtime_checkable

from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    RuntimeInvocation,
    RuntimeResult,
)
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    EndSessionRequest,
    LaneDescribe,
    LaneFrame,
    ObserveRequest,
    PreparedSession,
    PrepareRequest,
    ProviderStatus,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    SnapshotManifest,
    SnapshotRequest,
    StartRequest,
    StatusRequest,
    TurnHandle,
    UsageReport,
    UsageRequest,
)


class HarnessUnsupported(NotImplementedError):
    """The lane profile does not (or not yet, qualified) support this operation."""

    def __init__(self, operation: str, lane_profile: str, reason: str) -> None:
        super().__init__(f"{operation} is {reason} on lane profile {lane_profile}")
        self.operation = operation
        self.lane_profile = lane_profile
        self.reason = reason


class NativeTurnLost(LookupError):
    """The provider no longer knows the native turn (a local bridge or its store is gone).

    `lane.turn` never re-sends a lost turn: the unit becomes `in_doubt` and reconciliation
    decides (SPEC-07 section 4.1 step 2)."""


@runtime_checkable
class AgentHarness(Protocol):
    def describe(self) -> LaneDescribe: ...

    async def prepare(self, request: PrepareRequest) -> PreparedSession: ...

    async def start(self, request: StartRequest) -> SessionHandle: ...

    async def reattach(self, request: ReattachRequest) -> SessionHandle: ...

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle: ...

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt: ...

    def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]: ...

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest: ...

    async def usage(self, request: UsageRequest) -> UsageReport: ...

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt: ...


@runtime_checkable
class SessionLane(AgentHarness, Protocol):
    """A lane `lane.turn` drives through the protocol itself, segment by segment (FT-G2).

    Beside the ten operations it reads its provider's terminal frame into `ClosingFacts`,
    reconciles a session by native identity (`lane.status`), and turns a persisted frame's
    provider key back into its resumable cursor, so a resumed segment observes from the
    persisted frames (the truth) rather than from a throttled heartbeat (a hint).
    """

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts: ...

    async def status(self, request: StatusRequest) -> ProviderStatus: ...

    def resume_cursor(self, provider_key: str) -> str | None: ...


class OperationLane(AgentHarness, Protocol):
    """A lane `OperationExecutionService` dispatches a bound operation attempt to."""

    def requires_checkpoint_lineage(self, request: OperationExecutionRequest) -> bool:
        """Whether executing `request` on this lane needs checkpoint lineage composition."""
        ...

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult: ...


HARNESS_PROTOCOL_METHODS = (
    "describe",
    "prepare",
    "start",
    "reattach",
    "send_turn",
    "cancel_turn",
    "observe",
    "snapshot",
    "usage",
    "end_session",
)


class UnsupportedHarnessOperations:
    """Mixin: every protocol operation raises `HarnessUnsupported` with its declared reason.

    A lane overrides exactly the operations its describe reports `native` or `emulated`; the
    conformance test checks that no implemented control falls through to these defaults.
    """

    def describe(self) -> LaneDescribe:  # pragma: no cover - concrete lanes override
        raise NotImplementedError

    def _unsupported(self, operation: str) -> HarnessUnsupported:
        describe = self.describe()
        return HarnessUnsupported(
            operation, describe.lane_profile, describe.controls.get(operation, "unsupported")
        )

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        raise self._unsupported("prepare")

    async def start(self, request: StartRequest) -> SessionHandle:
        raise self._unsupported("start")

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        raise self._unsupported("reattach")

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        raise self._unsupported("send_turn")

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        raise self._unsupported("cancel_turn")

    def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        raise self._unsupported("observe")

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        raise self._unsupported("snapshot")

    async def usage(self, request: UsageRequest) -> UsageReport:
        raise self._unsupported("usage")

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        raise self._unsupported("end_session")


DEFAULT_STUBS = {
    name: getattr(UnsupportedHarnessOperations, name)
    for name in HARNESS_PROTOCOL_METHODS
    if name != "describe"
}


def implements(harness: object, operation: str) -> bool:
    """True when `harness` overrides `operation` instead of inheriting the unsupported stub."""

    implementation = getattr(type(harness), operation, None)
    return implementation is not None and implementation is not DEFAULT_STUBS.get(operation)


__all__ = [
    "DEFAULT_STUBS",
    "HARNESS_PROTOCOL_METHODS",
    "AgentHarness",
    "HarnessUnsupported",
    "NativeTurnLost",
    "OperationLane",
    "SessionLane",
    "UnsupportedHarnessOperations",
    "implements",
]
