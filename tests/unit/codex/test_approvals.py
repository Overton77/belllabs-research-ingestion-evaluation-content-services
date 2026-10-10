"""MP-08 x MP-11: codex approval server requests over the real `ApprovalBroker`.

FIXTURES: MP-11's in-memory approval store (the FIXTURE semantics of
`adapters/postgres/approvals`), a static context probe and the FIXTURE app-server; the
reviewer resolves through the real `HumanTaskService`. Proves: binding (task + live
correlation) persisted before any wait or hold; the handoff's native mapping (origin, request
and tool-call refs, connection scope, `park_for_reconciliation`, the execution binding digest
as policy digest, the turn generation); the connection epoch in the broker's
`connection_ref`; the reply mapping per decision through a full `lane.turn`; fail closed
without a broker; pause binds first and holds only the answer; the event pump never waits on
a human; one task per Codex question answered together; secret questions refused; recovery of
a relaunched connection's correlations before the thread is resumed.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.codex.approvals import (
    ApprovalContext,
    ApprovalNotBindable,
    CodexApprovals,
    native_requests,
)
from mission_control.adapters.codex.transport import InboundEvent
from mission_control.application.execution.approvals import ApprovalResolutionRequest
from mission_control.application.execution.harness.lane_turns import execution_start
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lanes import LaneSegmentBounds, ReattachRequest
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.lane_turns import SCOPE, RecordingSignals
from tests.unit.codex.drive import fields, sent, started
from tests.unit.codex.support import REVIEWER, CodexStack, approval_rig, codex_stack

BOUNDS = LaneSegmentBounds(max_duration_s=20, start_to_close_s=60, heartbeat_timeout_s=3)
POLICY = "sha256:" + "a" * 64


async def _turn(stack: CodexStack) -> Any:
    return await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
    )


async def _until(predicate: Any, *, seconds: float = 5.0) -> None:
    for _ in range(int(seconds / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("the condition was not reached in time")


# --- the full turn: bound before the wait, mapped exactly as the handoff says ----------------


async def test_a_command_approval_is_bound_before_the_wait_and_answered_natively(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_with_approval", decisions=["approve"])
    assert stack.rig is not None
    result = await _turn(stack)
    assert result.done
    assert OperationExecutionResult.model_validate(result.operation_result).status == "completed"

    (task,) = stack.rig.tasks()
    binding = task.packet.binding
    assert task.kind == "approval:provider_permission" and task.lifecycle == "resolved"
    assert binding.lane_profile == "codex" and binding.tool_name == "commandExecution"
    assert binding.native.native_session_ref == "thr-0001"
    assert binding.native.native_turn_ref == "turn-0002"
    assert binding.native.native_request_ref == "1" and binding.native.connection_scoped
    server = stack.launcher.server
    item_id = binding.native.tool_call_ref
    assert item_id is not None and item_id.startswith("item-"), "tool_call_ref = itemId"
    assert binding.replay_strategy == "park_for_reconciliation"
    assert binding.generation == 1 and task.packet.effect_kind == "shell"
    assert task.packet.reviewers == (REVIEWER,)
    assert task.packet.preview is not None
    assert task.packet.preview.arguments["command"] == "rm -rf build"
    assert "startedAtMs" not in task.packet.preview.arguments, "volatile fields are not approved"

    # The policy digest is the execution's recorded binding digest (what the production
    # `PostgresApprovalContextProbe` reads), not the binding's own `policy_digest`.
    recorded = execution_start(stack.operation, stack.identity, 1, "thr-0001")
    assert binding.policy_digest == recorded.intended_binding_digest
    provider = stack.operation.provider_binding
    assert provider is not None and binding.policy_digest != provider.policy_digest

    # The correlation names this app-server connection (owner + epoch) and was answered.
    (correlation,) = stack.rig.store.correlations()
    epoch = stack.launcher.launches[0].connection.epoch
    assert correlation.connection_ref == f"worker-a#1:codex:{epoch}"
    assert correlation.state == "answered" and correlation.reply is not None
    assert correlation.reply.action == "allow"

    # Persisted first, then resolved, and only then answered natively.
    assert [kind for kind, _id in stack.rig.store.order] == ["persisted", "resolved"]
    ((method, answer),) = server.approvals
    assert method == "item/commandExecution/requestApproval"
    assert answer == {"result": {"decision": "accept"}}
    kinds = [frame.kind for frame in stack.frames()]
    assert FrameKind.APPROVAL_REQUESTED in kinds and FrameKind.APPROVAL_RESOLVED in kinds
    assert any(frame.kind == FrameKind.TOOL_CALL_COMPLETED for frame in stack.frames())


@pytest.mark.parametrize(
    ("decision", "native"),
    [
        ("deny", {"result": {"decision": "decline"}}),
        ("cancel", {"result": {"decision": "cancel"}}),
        (
            {"decision": "approve_edited", "edited_arguments": {"command": "rm -rf build/tmp"}},
            {
                "error": {
                    "code": -32001,
                    "message": "edited arguments cannot be applied to a native approval",
                }
            },
        ),
    ],
)
async def test_each_decision_maps_to_its_pinned_response(
    tmp_path: Path, decision: Any, native: dict[str, Any]
) -> None:
    stack = codex_stack(tmp_path, "turn_with_approval", decisions=[decision])
    result = await _turn(stack)
    assert result.done
    ((_method, answer),) = stack.launcher.server.approvals
    assert answer == native
    assert stack.rig is not None
    (task,) = stack.rig.tasks()
    assert task.lifecycle == "resolved"


async def test_without_a_broker_every_request_fails_closed(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_with_approval", approvals=False)
    result = await _turn(stack)
    assert result.done
    ((_method, answer),) = stack.launcher.server.approvals
    assert answer == {"result": {"decision": "decline"}}
    assert [f for f in stack.frames() if f.kind == FrameKind.TOOL_CALL_FAILED], "declined item"
    (served,) = stack.harness.approvals.served
    assert served.refused == "no approval broker composed" and served.bound == ()


# --- pause and the non-blocking pump -----------------------------------------------------------


async def test_pause_binds_first_and_holds_only_the_native_answer(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_with_approval", decisions=["approve"])
    assert stack.rig is not None
    session = await started(stack)
    assert await stack.harness.hold_approvals(stack.heid, True) == 0
    turn = await sent(stack, session)
    rig = stack.rig
    await _until(lambda: len(rig.store.order) >= 2)  # persisted, then resolved
    await asyncio.sleep(0.1)
    server = stack.launcher.server
    assert server.approvals == [], "held: the turn waits at its tool gate"
    (task,) = rig.tasks()
    assert task.lifecycle == "resolved", "the human may decide while the lane is paused"
    assert stack.harness.active_turn(stack.heid) == turn.native_turn_ref
    released = await stack.harness.hold_approvals(stack.heid, False)
    assert released == 1
    await asyncio.wait_for(server._tasks[0], timeout=5)
    assert server.approvals == [
        ("item/commandExecution/requestApproval", {"result": {"decision": "accept"}})
    ]


async def test_the_event_pump_keeps_absorbing_while_an_approval_waits(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_with_approval", review=False)
    assert stack.rig is not None
    session = await started(stack)
    await sent(stack, session)
    rig = stack.rig
    await _until(lambda: bool(rig.tasks()))
    staged = stack.harness._sessions[stack.heid]
    connection = stack.launcher.launches[0].connection
    server = stack.launcher.server
    # While the human has not answered, later events still reach the pump.
    await server.notify("warning", {"threadId": "thr-0001", "message": "fixture warning"})
    await _until(lambda: staged.absorbed_seq >= connection.last_seq)
    assert server.approvals == [] and len(staged.approval_tasks) == 1
    (task,) = rig.tasks()
    await rig.service.resolve_approval(
        task.human_task_id,
        ApprovalResolutionRequest(
            request_id="late-review",
            expected_task_version=task.version,
            decision="deny",
            reviewed_digest=task.packet.review_digest,
        ),
        ActorContext(actor_id=REVIEWER),
    )
    await asyncio.wait_for(server._tasks[0], timeout=5)
    assert server.approvals[0][1] == {"result": {"decision": "decline"}}


async def test_an_unanswered_approval_expires_its_correlation_not_the_task(
    tmp_path: Path,
) -> None:
    from tests.unit.codex.support import fixture_settings

    stack = codex_stack(
        tmp_path,
        "turn_with_approval",
        review=False,
        settings=fixture_settings(tmp_path, approval_wait_seconds=0.2),
    )
    result = await _turn(stack)
    assert result.done
    ((_method, answer),) = stack.launcher.server.approvals
    assert answer == {"result": {"decision": "cancel"}}, "a system reply interrupts the turn"
    assert stack.rig is not None
    (task,) = stack.rig.tasks()
    assert task.lifecycle == "open", "timeout never approves and never closes the task"
    (correlation,) = stack.rig.store.correlations()
    assert correlation.state == "expired"


# --- questions, elicitations and identities (the adapter on its own) -------------------------


def _context(epoch: str = "epoch-1") -> ApprovalContext:
    return ApprovalContext(
        request_scope=SCOPE,
        run_id="run-1",
        harness_execution_id="heid-1",
        generation=1,
        native_session_ref="thr-1",
        policy_digest=POLICY,
        connection_epoch=epoch,
    )


def _user_input(request_id: int = 7, **question_changes: Any) -> InboundEvent:
    questions = [
        {
            "id": "q1",
            "header": "Branch",
            "question": "Which branch?",
            "options": [
                {"label": "main", "description": "the default"},
                {"label": "dev", "description": "integration"},
            ],
        },
        {"id": "q2", "header": "Tests", "question": "Run the slow tests?", **question_changes},
    ]
    return InboundEvent(
        5,
        "server_request",
        "item/tool/requestUserInput",
        {
            "threadId": "thr-1",
            "turnId": "turn-1",
            "itemId": "item-q",
            "isBlocking": True,
            "questions": questions,
        },
        request_id,
    )


async def test_codex_questions_bind_one_task_each_and_are_answered_together() -> None:
    rig = approval_rig(
        POLICY,
        decisions=[
            {"decision": "approve", "selected": ["main"]},
            {"decision": "approve", "answer": "only the fast ones"},
        ],
    )
    approvals = CodexApprovals(broker=rig.broker, wait_seconds=5, reviewers=(REVIEWER,))
    response = await approvals.serve(_user_input(), _context())
    assert response.result == {
        "answers": {"q1": {"answers": ["main"]}, "q2": {"answers": ["only the fast ones"]}}
    }
    tasks = sorted(rig.tasks(), key=lambda task: task.packet.prompt)
    assert len(tasks) == 2
    refs = {task.packet.binding.native.tool_call_ref for task in tasks}
    assert refs == {"item-q:q:q1", "item-q:q:q2"}
    assert all(task.kind == "approval:provider_question" for task in tasks)
    assert tasks[0].packet.question is not None
    assert tasks[0].packet.question.options == ("main", "dev")


async def test_a_question_left_unanswered_refuses_the_whole_request() -> None:
    rig = approval_rig(POLICY, decisions=[{"decision": "approve", "answer": "main"}, "cancel"])
    approvals = CodexApprovals(broker=rig.broker, wait_seconds=5, reviewers=(REVIEWER,))
    response = await approvals.serve(_user_input(), _context())
    assert response.is_error and response.error_code == -32002


async def test_a_secret_question_is_refused_without_a_task() -> None:
    rig = approval_rig(POLICY)
    approvals = CodexApprovals(broker=rig.broker, wait_seconds=5, reviewers=(REVIEWER,))
    event = _user_input(isSecret=True)
    with pytest.raises(ApprovalNotBindable, match="secret"):
        native_requests(event, _context())
    response = await approvals.serve(event, _context())
    assert response.is_error and response.error_code == -32002
    assert rig.tasks() == [], "a credential is never collected through a Human Task"


def _elicitation(request_id: int = 3) -> InboundEvent:
    return InboundEvent(
        9,
        "server_request",
        "mcpServer/elicitation/request",
        {
            "threadId": "thr-1",
            "turnId": "turn-1",
            "serverName": "fixture-mcp",
            "mode": "form",
            "message": "Pick an environment",
            "requestedSchema": {
                "type": "object",
                "properties": {"environment": {"type": "string"}},
                "required": ["environment"],
            },
        },
        request_id,
    )


async def test_an_elicitation_binds_with_the_connection_epoch_and_answers_its_content() -> None:
    rig = approval_rig(POLICY, decisions=["approve", "deny"])
    approvals = CodexApprovals(broker=rig.broker, wait_seconds=5, reviewers=(REVIEWER,))
    first = await approvals.serve(_elicitation(), _context("epoch-1"))
    assert first.result == {"action": "accept", "content": {"environment": "fixture"}}
    # The same JSON-RPC id on the next app-server process is another request, another task.
    second = await approvals.serve(_elicitation(), _context("epoch-2"))
    assert second.result == {"action": "decline"}
    tasks = rig.tasks()
    assert len(tasks) == 2
    assert {task.packet.connection_ref for task in tasks} == {
        "worker-a#1:codex:epoch-1",
        "worker-a#1:codex:epoch-2",
    }
    assert all(task.packet.binding.native.tool_call_ref is None for task in tasks)
    assert all(task.packet.binding.replay_strategy == "park_for_reconciliation" for task in tasks)


async def test_an_elicitation_form_collecting_a_credential_is_declined_without_a_task() -> None:
    rig = approval_rig(POLICY)
    approvals = CodexApprovals(broker=rig.broker, wait_seconds=5, reviewers=(REVIEWER,))
    event = _elicitation()
    event.params["requestedSchema"] = {
        "type": "object",
        "properties": {"api_key": {"type": "string"}},
    }
    response = await approvals.serve(event, _context())
    assert response.result == {"action": "decline"} and rig.tasks() == []


def test_zsh_bridge_callbacks_of_one_item_are_distinct_tasks() -> None:
    def command(approval_id: str | None) -> InboundEvent:
        return InboundEvent(
            1,
            "server_request",
            "item/commandExecution/requestApproval",
            {
                "threadId": "thr-1",
                "turnId": "turn-1",
                "itemId": "item-9",
                "approvalId": approval_id,
                "command": "git status",
                "startedAtMs": 1,
            },
            11,
        )

    (plain,) = native_requests(command(None), _context())
    (sub,) = native_requests(command("cb-1"), _context())
    assert plain.native.tool_call_ref == "item-9"
    assert sub.native.tool_call_ref == "item-9:cb-1"
    assert plain.policy_digest == POLICY and plain.generation == 1
    assert plain.origin == "provider_permission" and plain.effect_kind == "shell"


# --- relaunch: correlations of the dead connection are lost before the thread resumes ------


async def test_a_relaunch_recovers_the_previous_connections_correlations(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_hold", review=False)
    assert stack.rig is not None
    session = await started(stack)
    old = stack.launcher.launches[0].connection
    # A request the old app-server process asked and nobody answered yet.
    pending = asyncio.create_task(
        stack.harness.approvals.serve(
            InboundEvent(
                3,
                "server_request",
                "item/fileChange/requestApproval",
                {"threadId": "thr-0001", "turnId": "turn-x", "itemId": "item-f", "startedAtMs": 1},
                4,
            ),
            ApprovalContext(
                request_scope=stack.identity.request_scope,
                run_id=stack.identity.run_key,
                harness_execution_id=stack.heid,
                generation=1,
                native_session_ref="thr-0001",
                policy_digest=stack.operation.effective_configuration_digest,
                connection_epoch=old.epoch,
            ),
        )
    )
    rig = stack.rig
    await _until(lambda: bool(rig.store.correlations()))
    assert rig.store.correlations()[0].state == "live"
    await old.close("the app-server died")
    await stack.harness.reattach(
        ReattachRequest(**fields(stack), native_session_ref=session.native_session_ref or "")
    )
    assert len(stack.launcher.launches) == 2
    (correlation,) = rig.store.correlations()
    assert correlation.state == "lost", "never reused by the new connection"
    (item,) = stack.harness.approvals.recovered
    assert item.action == "park_for_reconciliation"
    assert item.correlation.connection_ref == f"worker-a#1:codex:{old.epoch}"
    new = stack.launcher.launches[1].server
    assert "thread/resume" in [m for m, _p in new.records]
    answer = await asyncio.wait_for(pending, 5)
    assert answer.result == {"decision": "cancel"}, "a lost correlation never approves"
    (task,) = rig.tasks()
    assert task.lifecycle == "open", "the durable task stays for reconciliation"
