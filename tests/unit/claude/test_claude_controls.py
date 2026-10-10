"""MP-07 controls on the `claude_agent_sdk` lane (FIXTURE client): the two-turn interrupt and
replacement reads the intended response and never the interrupted turn's leftover messages;
Kernel Hooks fail closed on the Stop Fence and the binding's disallowed tools; native
permission requests bind to `mc.approval_binding.v1` through the `PermissionBindingPort`;
pause is a typed rejection. No Claude Code runs."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from mission_control.adapters.claude.harness import turn_reference
from mission_control.adapters.claude.hooks import effect_kind_of
from mission_control.adapters.claude.permissions import (
    NO_GATEWAY,
    DenyWithoutGateway,
    PermissionOutcome,
    PermissionRequest,
    PermissionScope,
    to_permission_result,
)
from mission_control.application.execution.approvals import tool_input_digest
from mission_control.application.execution.harness.controls import pause_decision
from mission_control.application.execution.harness.inject import cancel_and_replace_turn
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    harness_scope,
)
from mission_control.domain.capabilities.hooks import HookEvent
from mission_control.domain.execution.approvals import ApprovalBinding
from mission_control.domain.execution.lane_turns import LaneSegmentBounds, LaneTurnRequest
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    ObserveRequest,
    PrepareRequest,
    SendTurnRequest,
    StartRequest,
    StatusRequest,
    TurnHandle,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.stop_fence import StopFence
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.claude.fixtures import (
    PROFILE,
    SESSION_ID,
    ClaudeStack,
    claude_binding,
    claude_operation,
    claude_stack,
    materialization_digest_for,
)

SMALL = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=20,
    start_to_close_s=40,
    heartbeat_timeout_s=3,
    status_poll_limit=2,
    status_poll_interval_s=1,
    busy_wait_s=10,
)


def _identity(stack: ClaudeStack) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, PROFILE, 1)


def _fields(stack: ClaudeStack, key: str) -> dict[str, Any]:
    identity = _identity(stack)
    assert stack.operation.provider_binding is not None
    return {
        "scope": harness_scope(identity.request_scope),
        "lane_profile": PROFILE,
        "harness_execution_id": str(identity.harness_execution_id),
        "binding_digest": stack.operation.provider_binding.binding_digest,
        "idempotency_key": f"{identity.harness_execution_id}:1:{key}",
        "generation": 1,
    }


def _turn(stack: ClaudeStack, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {
            "operation": stack.operation,
            "lane_profile": PROFILE,
            "generation": 1,
            "segment": SMALL,
            **changes,
        }
    )


def _frames(stack: ClaudeStack) -> list[Any]:
    execution = stack.frames._executions[_identity(stack).harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


async def _started(stack: ClaudeStack) -> tuple[Any, Any]:
    harness = stack.harness
    identity = _identity(stack)
    harness.stage(str(identity.harness_execution_id), stack.operation)
    prepared = await harness.prepare(
        PrepareRequest(
            **_fields(stack, "prepare"),
            run_id=stack.operation.identity.run_id,
            operation_id=stack.operation.identity.operation_id,
            attempt_no=1,
        )
    )
    session = await harness.start(StartRequest(**_fields(stack, "turn:1"), prepared=prepared))
    assert session.native_session_ref == SESSION_ID
    return harness, session


# --- two-turn interrupt / replacement ------------------------------------------------------


async def test_the_replacement_turn_reads_the_intended_response_not_the_leftovers(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path, script="interrupted_then_replaced")
    harness, session = await _started(stack)
    first = await harness.send_turn(
        SendTurnRequest(
            **_fields(stack, "turn:1"), session=session, turn_no=1, instruction_ref="op:turn:1"
        )
    )
    assert first.native_turn_ref == turn_reference(_fields(stack, "turn:1")["idempotency_key"])
    client = stack.factory.last
    await asyncio.wait_for(client.held.wait(), timeout=10)
    # While the turn runs, a second send is busy: nothing is written.
    busy = await harness.send_turn(
        SendTurnRequest(
            **_fields(stack, "replace:x"), session=session, turn_no=2, instruction_ref="op:x"
        )
    )
    assert busy.status == "busy" and len(client.sent) == 1
    receipt = await harness.cancel_turn(
        CancelTurnRequest(**_fields(stack, "cancel"), turn=first, reason="interrupt_and_inject")
    )
    assert receipt.acknowledged and not receipt.already_terminal
    assert receipt.native_status == "cancelled", "the interrupted response was drained"
    assert client.interrupts == 1
    drained = [
        frame
        async for frame in harness.observe(ObserveRequest(**_fields(stack, "observe"), turn=first))
    ]
    texts = [json.dumps(frame.body) for frame in drained]
    assert any("LEFTOVER: interrupted mid-scan." in text for text in texts)
    assert not any("scan finished (never delivered" in text for text in texts)
    assert drained[-1].terminal and drained[-1].raw_kind == "result"
    facts = harness.closing_facts(first, drained[-1])
    assert facts.native_status == "cancelled" and facts.error_code == "cancelled_by_command"
    status = await harness.status(StatusRequest(**_fields(stack, "status"), session=session))
    assert status.idle, "the session is idle: the drained turn is terminal, nothing runs"
    harness.stage_turn(
        str(_identity(stack).harness_execution_id), "inject:cmd-1", "Use release/2.3."
    )
    second = await harness.send_turn(
        SendTurnRequest(
            **_fields(stack, "replace:1"),
            session=session,
            turn_no=2,
            instruction_ref="inject:cmd-1",
        )
    )
    assert second.status == "accepted" and second.native_turn_ref != first.native_turn_ref
    assert client.sent[1][1] == "Use release/2.3."
    replacement = [
        frame
        async for frame in harness.observe(ObserveRequest(**_fields(stack, "observe"), turn=second))
    ]
    bodies = [json.dumps(frame.body) for frame in replacement]
    assert not any("LEFTOVER" in body for body in bodies), "no interrupted message leaks"
    assert {frame.native_turn_ref for frame in replacement} == {second.native_turn_ref}
    assert replacement[-1].terminal
    facts = harness.closing_facts(second, replacement[-1])
    assert facts.native_status == "finished"
    assert facts.result_excerpt == "INTENDED: using release/2.3 as instructed."
    # Cancelling a terminal turn is idempotent and never interrupts again.
    again = await harness.cancel_turn(
        CancelTurnRequest(**_fields(stack, "cancel"), turn=first, reason="command")
    )
    assert again.already_terminal and client.interrupts == 1


async def test_cancel_and_replace_drains_the_interrupted_turn_before_the_replacement(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path, script="interrupted_then_replaced")
    harness, session = await _started(stack)
    first = await harness.send_turn(
        SendTurnRequest(
            **_fields(stack, "turn:1"), session=session, turn_no=1, instruction_ref="op:turn:1"
        )
    )
    client = stack.factory.last
    await asyncio.wait_for(client.held.wait(), timeout=10)
    harness.stage_turn(
        str(_identity(stack).harness_execution_id), "inject:cmd-2", "Use release/2.3."
    )
    drained: list[Any] = []

    async def on_frame(frame: Any) -> None:
        drained.append(frame)

    async def unsettled() -> tuple[str, ...]:
        return ()

    replaced = await cancel_and_replace_turn(
        harness,
        cancel=CancelTurnRequest(
            **_fields(stack, "cancel"), turn=first, reason="interrupt_and_inject"
        ),
        replacement=SendTurnRequest(
            **_fields(stack, "replace:1"),
            session=session,
            turn_no=2,
            instruction_ref="inject:cmd-2",
        ),
        unsettled=unsettled,
        on_frame=on_frame,
    )
    assert replaced.cancelled_turn_ref == first.native_turn_ref
    assert replaced.handle.native_turn_ref not in {None, first.native_turn_ref}
    assert drained and drained[-1].terminal
    assert drained[-1].body["terminal_reason"] == "aborted_tools"
    assert [kind for kind in (frame.raw_kind for frame in drained) if kind == "result"] == [
        "result"
    ]
    assert client.sent[1][1] == "Use release/2.3." and client.interrupts == 1
    turn2 = TurnHandle(session=session, turn_no=2, native_turn_ref=replaced.handle.native_turn_ref)
    final = [
        frame
        async for frame in harness.observe(ObserveRequest(**_fields(stack, "observe"), turn=turn2))
    ]
    assert harness.closing_facts(turn2, final[-1]).result_excerpt.startswith("INTENDED")


async def test_interrupt_and_inject_through_lane_turn_replaces_inside_the_segment(
    tmp_path: Path,
) -> None:
    from mission_control.domain.execution.contracts import OperationExecutionRequest
    from tests.fixtures.cursor_controls import FAST, _admitted_unit, _compose

    run_control, repository, run_id, unit, changes = await _admitted_unit((), None)
    operation = OperationExecutionRequest.model_validate(
        {**claude_operation().model_dump(mode="python"), **changes}
    )
    stack = claude_stack(tmp_path, script="interrupted_then_replaced", operation=operation)
    control = _compose(
        stack,
        stack.harness,
        stack.frames,
        operation,
        run_control=run_control,
        repository=repository,
        run_id=run_id,
        unit=unit,
        settings=FAST,
        profile=PROFILE,
    )
    identity = LaneExecutionIdentity.of(operation, PROFILE, 1)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id)
    running = asyncio.create_task(control.service.turn(control.turn(segment=SMALL), signals))
    await asyncio.wait_for((await stack.factory.connected()).held.wait(), timeout=10)
    receipt = await control.command("interrupt_and_inject", "Use release/2.3.")
    result = await asyncio.wait_for(running, timeout=30)
    assert result.done and result.closing_facts is not None
    assert result.closing_facts.native_status == "finished"
    assert result.closing_facts.result_excerpt.startswith("INTENDED")
    client = stack.factory.last
    assert client.interrupts == 1 and len(client.sent) == 2
    assert "Use release/2.3." in client.sent[1][1]
    execution = stack.frames._executions[identity.harness_execution_id]
    results = [
        json.loads(frame.body_excerpt)
        for frame in execution.frames.values()
        if frame.kind is FrameKind.RUN_RESULT
    ]
    assert [body["terminal_reason"] for body in results] == ["aborted_tools", "completed"]
    assert result.native.turn_ref == client.sent[1][0]
    report = (await control.status(receipt)).receipts[-1].delivery_report
    assert report is not None and report.delivered_semantics == "cancel_and_replace"


# --- kernel hooks ------------------------------------------------------------------------------


async def test_a_stop_fenced_run_denies_the_tool_through_the_in_process_kernel_hook(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path)
    await stack.fences.persist(
        StopFence(
            request_scope=stack.operation.request_scope,
            run_id=stack.operation.identity.run_id,
            generation=1,
            command_id="cancel-now",
            reason="operator immediate cancel",
            requested_at=datetime.now(UTC),
        )
    )
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done
    client = stack.factory.last
    assert client.hook_calls == [("PreToolUse", "toolu_bash_01", True)]
    results = [
        json.loads(frame.body_excerpt)
        for frame in _frames(stack)
        if frame.kind is FrameKind.HOOK_RESULT
    ]
    denied = [body for body in results if body["decision"] == "deny"]
    assert denied and denied[0]["reason_code"] == "STOP_FENCED"
    assert denied[0]["fence_command_id"] == "cancel-now"
    assert denied[0]["hook_id"] in {"mc.stop_fence", "mc.operation_intent"}
    failed = [frame for frame in _frames(stack) if frame.kind is FrameKind.TOOL_CALL_FAILED]
    assert failed and failed[0].tool_call_ref == "toolu_bash_01"
    assert stack.intents.intents() == (), "no Operation Intent for a fenced effect"


async def test_a_disallowed_tool_is_refused_by_the_operation_intent_hook(tmp_path: Path) -> None:
    base = claude_operation()
    binding = claude_binding(
        materialization_digest=materialization_digest_for(base),
        provider_options={"provider": PROFILE, "disallowed_tools": ["Bash"]},
    )
    stack = claude_stack(tmp_path, operation=claude_operation(binding=binding))
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    await stack.lanes.service.turn(_turn(stack), signals)
    client = stack.factory.last
    assert client.options.disallowed_tools == ["Bash"]
    assert client.hook_calls == [("PreToolUse", "toolu_bash_01", True)]
    results = [
        json.loads(frame.body_excerpt)
        for frame in _frames(stack)
        if frame.kind is FrameKind.HOOK_RESULT
        and json.loads(frame.body_excerpt)["decision"] == "deny"
    ]
    assert results and results[0]["reason_code"] == "DISALLOWED_TOOL"


def test_effect_kinds_follow_the_tool_family() -> None:
    assert effect_kind_of("Bash", HookEvent.BEFORE_SHELL) == "shell"
    assert effect_kind_of("mcp__tavily__search", HookEvent.BEFORE_MCP) == "mcp"
    assert effect_kind_of("Write", HookEvent.AFTER_FILE_EDIT) == "file"
    assert effect_kind_of("Task", HookEvent.BEFORE_TOOL) == "task"
    assert effect_kind_of(None, HookEvent.SUBAGENT_START) == "task"
    assert effect_kind_of("Read", HookEvent.BEFORE_TOOL) == "other"


# --- permission binding ------------------------------------------------------------------------


async def test_permission_requests_bind_to_approval_bindings_and_default_to_deny(
    tmp_path: Path,
) -> None:
    port = DenyWithoutGateway()
    stack = claude_stack(tmp_path, script="subagent_task", permissions=port)
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and result.closing_facts is not None
    client = stack.factory.last
    assert client.permission_calls == [("Bash", False)]
    (binding,) = port.denied
    identity = _identity(stack)
    assert binding.origin == "provider_permission" and binding.lane_profile == PROFILE
    assert binding.harness_execution_id == str(identity.harness_execution_id)
    assert binding.native.native_session_ref == SESSION_ID
    assert binding.native.native_turn_ref == client.sent[0][0]
    # MP-11 Claude mapping: the stable tool call, never a connection-scoped handle.
    assert binding.native.tool_call_ref == "toolu_sub_bash_31"
    assert binding.native.native_request_ref is None
    assert binding.native.connection_scoped is False
    assert binding.tool_name == "Bash" and binding.replay_strategy == "reissue_native_request"
    assert binding.input_digest == tool_input_digest("Bash", {"command": "ls papers"})
    # The execution's binding digest (what the PostgreSQL context probe revalidates against).
    assert binding.policy_digest == stack.operation.effective_configuration_digest
    assert binding.human_task_id.startswith("claude-permission:")
    frames = _frames(stack)
    requested = next(frame for frame in frames if frame.kind is FrameKind.APPROVAL_REQUESTED)
    resolved = next(frame for frame in frames if frame.kind is FrameKind.APPROVAL_RESOLVED)
    assert requested.tool_call_ref == "toolu_sub_bash_31" and not requested.closing
    assert resolved.closing
    body = json.loads(resolved.body_excerpt)
    assert (body["decision"], body["behavior"], body["message"]) == ("deny", "deny", NO_GATEWAY)
    assert body["interrupt"] is False
    # Subagent frames carry the task identity through the MP-13 mapping.
    sub = next(frame for frame in frames if frame.provider_key.startswith("claude:a-3003"))
    assert sub.native_session_ref == SESSION_ID
    lifecycle = [frame for frame in frames if frame.raw_kind.startswith("system.task_")]
    assert [frame.kind for frame in lifecycle] == [FrameKind.STATUS] * 4


async def test_an_approving_port_allows_and_edited_input_travels_to_the_sdk(
    tmp_path: Path,
) -> None:
    class Approving:
        def __init__(self) -> None:
            self.seen: list[tuple[ApprovalBinding, PermissionRequest, PermissionScope]] = []

        async def resolve(
            self, binding: ApprovalBinding, request: PermissionRequest, *, scope: PermissionScope
        ) -> PermissionOutcome:
            self.seen.append((binding, request, scope))
            return PermissionOutcome(decision="approve", human_task_ref="human_task:approved-1")

    port = Approving()
    stack = claude_stack(tmp_path, script="subagent_task", permissions=port)
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done
    assert stack.factory.last.permission_calls == [("Bash", True)]
    ((binding, request, scope),) = port.seen
    assert request.tool_input == {"command": "ls papers"} and request.agent_id == "agent-31"
    identity = _identity(stack)
    assert (scope.request_scope, scope.run_id) == (identity.request_scope, identity.run_key)
    assert binding.input_digest.startswith("sha256:")
    completed = [frame for frame in _frames(stack) if frame.kind is FrameKind.TOOL_CALL_COMPLETED]
    assert any(frame.tool_call_ref == "toolu_sub_bash_31" for frame in completed)
    edited = to_permission_result(
        binding, PermissionOutcome(decision="approve_edited", updated_input={"command": "ls docs"})
    )
    assert isinstance(edited, PermissionResultAllow) and edited.updated_input == {
        "command": "ls docs"
    }
    cancelled = to_permission_result(binding, PermissionOutcome(decision="cancel", message="stop"))
    assert isinstance(cancelled, PermissionResultDeny) and cancelled.interrupt
    denied = to_permission_result(binding, PermissionOutcome(decision="deny", message="no"))
    assert isinstance(denied, PermissionResultDeny) and not denied.interrupt
    refused = to_permission_result(
        binding, PermissionOutcome(decision="deny", message="expired", interrupt=True)
    )
    assert isinstance(refused, PermissionResultDeny) and refused.interrupt
    not_admitted = to_permission_result(binding, PermissionOutcome(decision="request_changes"))
    assert isinstance(not_admitted, PermissionResultDeny) and "not admitted" in not_admitted.message


def test_pause_is_a_typed_rejection_mid_run(tmp_path: Path) -> None:
    describe = claude_stack(tmp_path).harness.describe()
    decision = pause_decision(describe, turn_in_flight=True)
    assert not decision.accepted and decision.reason_code == "unsupported_control"
    boundary = pause_decision(describe, turn_in_flight=False)
    assert boundary.accepted and boundary.delivery_semantics == "turn_boundary_guaranteed"


@pytest.mark.parametrize("script", ["full_run", "subagent_task", "unknown_frames"])
async def test_every_frame_of_a_run_is_keyed_once_and_names_the_session(
    tmp_path: Path, script: str
) -> None:
    stack = claude_stack(tmp_path, script=script)
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done
    keys = [frame.provider_key for frame in _frames(stack)]
    assert len(keys) == len(set(keys))
    assert all(frame.native_session_ref == SESSION_ID for frame in _frames(stack))
