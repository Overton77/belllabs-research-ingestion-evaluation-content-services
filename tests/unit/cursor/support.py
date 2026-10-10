"""Shared helpers for the MP-09 Cursor suites (FIXTURE providers only)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.application.execution.harness.dispatch import (
    DispatchRecord,
    SessionOwner,
    instruction_digest,
)
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.execution.lanes import SendTurnRequest, SessionHandle
from tests.fixtures.cursor_cloud import CloudStack
from tests.fixtures.cursor_controls import harness_fields
from tests.fixtures.lane_turns import LaneStack, RecordingSignals, lane_stack


def identity(stack: Any, profile: str) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, profile, 1)


def heid_of(stack: Any, profile: str) -> str:
    return str(identity(stack, profile).harness_execution_id)


def turn(stack: Any, profile: str, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {"operation": stack.operation, "lane_profile": profile, "generation": 1, **changes}
    )


def lanes_for(stack: Any) -> LaneStack:
    return lane_stack(stack.harness, operation=stack.operation)


def signals(lanes: LaneStack, stack: Any, profile: str) -> RecordingSignals:
    return RecordingSignals(lanes.frames, identity(stack, profile).harness_execution_id)


def frames_of(lanes: LaneStack, stack: Any, profile: str) -> list[Any]:
    execution = lanes.frames._executions[identity(stack, profile).harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


def send_key(heid: str, turn_no: int) -> str:
    return f"{heid}:1:turn:{turn_no}"


def session_handle(stack: Any, profile: str, heid: str, agent_id: str) -> SessionHandle:
    return SessionHandle(
        lane_profile=profile,  # type: ignore[arg-type]
        harness_execution_id=heid,
        generation=1,
        native_session_ref=agent_id,
    )


def send_request(
    stack: Any, profile: str, heid: str, agent_id: str, turn_no: int, *, key: str | None = None
) -> SendTurnRequest:
    fields = harness_fields(stack.operation, heid, profile)
    fields["idempotency_key"] = key or send_key(heid, turn_no)
    return SendTurnRequest(
        **fields,
        session=session_handle(stack, profile, heid, agent_id),
        turn_no=turn_no,
        instruction_ref=f"instruction:{turn_no}",
    )


def dispatch_record(
    stack: Any,
    *,
    kind: str,
    key: str,
    owner: SessionOwner | None = None,
    turn_no: int = 1,
    instruction_ref: str | None = None,
) -> DispatchRecord:
    binding = stack.operation.cursor_binding
    assert binding is not None
    return DispatchRecord(
        kind=kind,  # type: ignore[arg-type]
        idempotency_key=key,
        expected_generation=1,
        instruction_digest=instruction_digest(
            kind=kind,  # type: ignore[arg-type]
            instruction_ref=instruction_ref,
            binding_digest=binding.binding_digest,
            turn_no=turn_no,
        ),
        owner_ref=owner.owner_ref if owner else "worker-fixture",
        owner_epoch=owner.epoch if owner else 1,
        intended_at=datetime.now(UTC),
    )


def fresh_cloud_harness(stack: CloudStack, mirrors: Any) -> CursorCloudHarness:
    """A `cursor_cloud` harness in a 'new process': the same fake API, ledger and artifact
    store, none of the in-memory session state."""

    return CursorCloudHarness(
        client=stack.client,
        publisher=GitBranchPublisher(mirrors),
        projections=stack.harness._projections,
        artifacts=stack.artifacts,
        workspaces=stack.workspaces,
        expired_poll_interval_s=0.01,
    )
