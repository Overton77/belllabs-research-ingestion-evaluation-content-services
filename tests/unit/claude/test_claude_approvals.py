"""MP-07 x MP-11: `can_use_tool` through the durable approval broker (in-memory FIXTURE stores).

The broker, the Human Task service and the approval rules are MP-11's production code; the
stores are `application/execution/approvals_memory` (FIXTURE semantics of the PostgreSQL
repositories, which `tests/integration/claude/test_claude_broker_postgres.py` proves on a real
database). The SDK client is the MP-07 fixture client: no Claude Code runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from mission_control.adapters.claude.approvals import BrokerPermissionBinding, permission_outcome
from mission_control.adapters.claude.compose import broker_permissions, compose_claude_local
from mission_control.adapters.claude.harness import StaticAuthAdmitter, approval_policy_digest
from mission_control.adapters.claude.host import HostUnsupported, host_gate
from mission_control.adapters.claude.permissions import (
    DenyWithoutGateway,
    PermissionRequest,
    PermissionScope,
    RecoveringPermissionPort,
)
from mission_control.adapters.cursor.projection import RenderedProjectionSource, static_rows
from mission_control.application.execution.approvals import (
    ApprovalTaskView,
    tool_input_digest,
)
from mission_control.application.execution.approvals_broker import (
    MAX_WAIT_SECONDS,
    ApprovalBroker,
)
from mission_control.application.execution.approvals_memory import (
    InMemoryApprovalStore,
    StaticApprovalContext,
)
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.domain.execution.approvals import ApprovalBinding, NativeApprovalCorrelation
from mission_control.domain.execution.lane_turns import LaneSegmentBounds, LaneTurnRequest
from mission_control.domain.execution.lanes import ReattachRequest
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import StopFence
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.approvals.fixtures import answer
from tests.unit.claude.fixtures import (
    FIXTURE_ENVIRON,
    PROFILE,
    SESSION_ID,
    ClaudeStack,
    FixtureWorkspace,
    claude_operation,
    claude_stack,
    fixture_admission,
    projection_rows,
)

REVIEWER = "owner"
OWNER = ActorContext(actor_id=REVIEWER)
TOOL_USE = "toolu_sub_bash_31"
SEGMENT = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=20,
    start_to_close_s=40,
    heartbeat_timeout_s=3,
    status_poll_limit=2,
    status_poll_interval_s=1,
    busy_wait_s=10,
)


@dataclass
class BrokerStack:
    claude: ClaudeStack
    port: BrokerPermissionBinding
    broker: ApprovalBroker
    service: HumanTaskService
    store: InMemoryApprovalStore
    context: StaticApprovalContext

    @property
    def identity(self) -> LaneExecutionIdentity:
        return LaneExecutionIdentity.of(self.claude.operation, PROFILE, 1)

    @property
    def scope(self) -> str:
        return self.identity.request_scope


def _broker_stack(
    tmp_path: Path,
    *,
    wait_seconds: float = 5.0,
    policy_digest: str | None = None,
    generation: int = 1,
    fences: InMemoryStopFenceRepository | None = None,
    store: InMemoryApprovalStore | None = None,
    connection_ref: str = "worker-a#1",
    script: str = "subagent_task",
    registry: dict[UUID, Any] | None = None,
) -> BrokerStack:
    store = store or InMemoryApprovalStore()
    operation = claude_operation()
    # The probe answers what PostgreSQL would: the execution's binding digest and generation.
    context = StaticApprovalContext(generation, policy_digest or approval_policy_digest(operation))
    broker = ApprovalBroker(
        store,
        store,
        probe=context,
        connection_ref=connection_ref,
        fences=fences,
        poll_seconds=0.01,
    )
    port = BrokerPermissionBinding(broker, wait_seconds=wait_seconds, reviewers=(REVIEWER,))
    claude = claude_stack(
        tmp_path,
        script=script,
        operation=operation,
        permissions=port,
        fences=fences,
        registry=registry,
    )
    service = HumanTaskService(
        InMemoryHumanTaskRepository(),
        request_scope=LaneExecutionIdentity.of(operation, PROFILE, 1).request_scope,
        approvals=store,
        approval_wake=broker.hub,
    )
    return BrokerStack(claude, port, broker, service, store, context)


def _turn(stack: ClaudeStack) -> LaneTurnRequest:
    return LaneTurnRequest(
        operation=stack.operation, lane_profile=PROFILE, generation=1, segment=SEGMENT
    )


def _frames(stack: ClaudeStack) -> list[Any]:
    identity = LaneExecutionIdentity.of(stack.operation, PROFILE, 1)
    execution = stack.frames._executions[identity.harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


async def _open_task(stack: BrokerStack, *, within_s: float = 10.0) -> ApprovalTaskView:
    async with asyncio.timeout(within_s):
        while True:
            open_tasks = await stack.store.list_tasks(stack.scope, lifecycle="open")
            if open_tasks:
                return open_tasks[0]
            await asyncio.sleep(0.01)


async def _reviewed(
    stack: BrokerStack, review: Callable[[ApprovalTaskView], Awaitable[None]] | None
) -> Any:
    signals = RecordingSignals(stack.claude.frames, stack.identity.harness_execution_id)
    running = asyncio.create_task(stack.claude.lanes.service.turn(_turn(stack.claude), signals))
    if review is not None:
        await review(await _open_task(stack))
    return await asyncio.wait_for(running, timeout=30)


def _resolve(decision: str, **extra: Any) -> Callable[[BrokerStack], Any]:
    def bind(stack: BrokerStack) -> Callable[[ApprovalTaskView], Awaitable[None]]:
        async def review(task: ApprovalTaskView) -> None:
            await stack.service.resolve(
                task.human_task_id, answer(task, decision=decision, **extra), OWNER
            )

        return review

    return bind


# --- the production path through the harness ---------------------------------------------------


async def test_a_reviewer_approval_reaches_can_use_tool_through_the_broker(tmp_path: Path) -> None:
    stack = _broker_stack(tmp_path)
    result = await _reviewed(stack, _resolve("approve")(stack))
    assert result.done
    client = stack.claude.factory.last
    assert client.permission_calls == [("Bash", True)]
    (allowed,) = client.permission_results
    assert isinstance(allowed, PermissionResultAllow) and allowed.updated_input is None
    (task,) = await stack.store.list_tasks(stack.scope)
    binding = task.packet.binding
    identity = stack.identity
    # The MP-11 Claude mapping, field by field.
    assert task.packet.origin == "provider_permission" and task.lifecycle == "resolved"
    assert binding.lane_profile == PROFILE and binding.tool_name == "Bash"
    assert binding.native.tool_call_ref == TOOL_USE and binding.native.native_request_ref is None
    assert binding.native.connection_scoped is False and task.packet.connection_ref is None
    assert binding.native.native_session_ref == SESSION_ID
    assert binding.native.native_turn_ref == client.sent[0][0]
    assert binding.replay_strategy == "reissue_native_request"
    assert binding.generation == 1
    assert binding.harness_execution_id == str(identity.harness_execution_id)
    assert task.packet.run_id == identity.run_key
    assert binding.policy_digest == stack.claude.operation.effective_configuration_digest
    assert binding.input_digest == tool_input_digest("Bash", {"command": "ls papers"})
    assert task.packet.preview is not None and task.packet.effect_kind == "shell"
    (correlation,) = stack.store.correlations()
    assert correlation.state == "answered" and correlation.reply is not None
    assert correlation.reply.reason == "human_decision" and correlation.reply.action == "allow"
    assert correlation.connection_ref == "worker-a#1"
    (outcome,) = stack.port.outcomes
    assert outcome.status == "approved"
    frames = _frames(stack.claude)
    requested = next(frame for frame in frames if frame.kind is FrameKind.APPROVAL_REQUESTED)
    resolved = next(frame for frame in frames if frame.kind is FrameKind.APPROVAL_RESOLVED)
    assert requested.tool_call_ref == TOOL_USE and resolved.tool_call_ref == TOOL_USE
    completed = [frame for frame in frames if frame.kind is FrameKind.TOOL_CALL_COMPLETED]
    assert any(frame.tool_call_ref == TOOL_USE for frame in completed)


@pytest.mark.parametrize(
    ("decision", "extra", "interrupt"),
    [
        ("deny", {"comment": "use the cached list instead"}, False),
        ("cancel", {}, True),
    ],
)
async def test_deny_continues_with_feedback_and_cancel_interrupts(
    tmp_path: Path, decision: str, extra: dict[str, Any], interrupt: bool
) -> None:
    stack = _broker_stack(tmp_path)
    result = await _reviewed(stack, _resolve(decision, **extra)(stack))
    assert result.done
    client = stack.claude.factory.last
    assert client.permission_calls == [("Bash", False)]
    (denied,) = client.permission_results
    assert isinstance(denied, PermissionResultDeny) and denied.interrupt is interrupt
    assert f"Reviewer {REVIEWER}" in denied.message
    if decision == "deny":
        assert "use the cached list instead" in denied.message


async def test_edited_arguments_travel_as_updated_input(tmp_path: Path) -> None:
    stack = _broker_stack(tmp_path)
    edited = {"command": "ls papers/trials"}
    result = await _reviewed(stack, _resolve("approve_edited", edited_arguments=edited)(stack))
    assert result.done
    (allowed,) = stack.claude.factory.last.permission_results
    assert isinstance(allowed, PermissionResultAllow) and allowed.updated_input == edited


async def test_the_wait_is_bounded_and_expiry_denies_with_interrupt_while_the_task_stays_open(
    tmp_path: Path,
) -> None:
    stack = _broker_stack(tmp_path, wait_seconds=0.2)
    result = await _reviewed(stack, None)
    assert result.done
    (denied,) = stack.claude.factory.last.permission_results
    assert isinstance(denied, PermissionResultDeny) and denied.interrupt
    (outcome,) = stack.port.outcomes
    assert outcome.status == "expired" and outcome.reply.reason == "wait_expired"
    (task,) = await stack.store.list_tasks(stack.scope)
    assert task.lifecycle == "open", "timeout never approves; the durable task stays pending"
    (correlation,) = stack.store.correlations()
    assert correlation.state == "expired"


async def test_a_changed_policy_or_generation_never_allows(tmp_path: Path) -> None:
    stack = _broker_stack(tmp_path, policy_digest="sha256:" + "f" * 64)
    result = await _reviewed(stack, _resolve("approve")(stack))
    assert result.done
    (denied,) = stack.claude.factory.last.permission_results
    assert isinstance(denied, PermissionResultDeny) and denied.interrupt
    assert stack.port.outcomes[0].reply.reason == "policy_changed"

    stale = _broker_stack(tmp_path / "g", generation=2)
    await _reviewed(stale, _resolve("approve")(stale))
    assert stale.port.outcomes[0].reply.reason == "stale_generation"


async def test_a_stop_fenced_run_refuses_an_approved_effect(tmp_path: Path) -> None:
    fences = InMemoryStopFenceRepository()
    stack = _broker_stack(tmp_path, fences=fences)

    async def review(task: ApprovalTaskView) -> None:
        await fences.persist(
            StopFence(
                request_scope=stack.scope,
                run_id=stack.identity.run_key,
                generation=1,
                command_id="cancel-now",
                reason="operator immediate cancel",
                requested_at=datetime.now(UTC),
            )
        )
        await stack.service.resolve(task.human_task_id, answer(task), OWNER)

    await _reviewed(stack, review)
    (denied,) = stack.claude.factory.last.permission_results
    assert isinstance(denied, PermissionResultDeny) and denied.interrupt
    assert stack.port.outcomes[0].reply.reason == "stop_fenced"


# --- the port on its own: replay, recovery, bounds ---------------------------------------------


def _binding(stack: BrokerStack, *, tool_input: dict[str, Any]) -> ApprovalBinding:
    return ApprovalBinding(
        human_task_id="claude-permission:fixture",
        origin="provider_permission",
        lane_profile=PROFILE,
        harness_execution_id=str(stack.identity.harness_execution_id),
        generation=1,
        native=NativeApprovalCorrelation(
            native_session_ref=SESSION_ID, tool_call_ref=TOOL_USE, connection_scoped=False
        ),
        tool_name="Bash",
        input_digest=tool_input_digest("Bash", tool_input),
        policy_digest=approval_policy_digest(stack.claude.operation),
        opened_at=datetime.now(UTC),
        replay_strategy="reissue_native_request",
    )


def _request(tool_input: dict[str, Any]) -> PermissionRequest:
    return PermissionRequest(tool_name="Bash", tool_input=tool_input, tool_use_id=TOOL_USE)


async def test_a_reissued_tool_call_replays_the_recorded_decision_only_for_the_same_input(
    tmp_path: Path,
) -> None:
    stack = _broker_stack(tmp_path, wait_seconds=0.05)
    scope = PermissionScope(request_scope=stack.scope, run_id=stack.identity.run_key)
    original = {"command": "ls papers"}
    first = await stack.port.resolve(
        _binding(stack, tool_input=original), _request(original), scope=scope
    )
    assert first.decision == "deny" and first.interrupt and first.reason == "wait_expired"
    task = await _open_task(stack)
    await stack.service.resolve(task.human_task_id, answer(task), OWNER)
    # The CLI asks again for the same tool call (a deferred tool use resumed): the recorded
    # approval replays after revalidation, without a second review.
    again = await stack.port.resolve(
        _binding(stack, tool_input=original), _request(original), scope=scope
    )
    assert again.decision == "approve" and again.human_task_ref == task.human_task_id
    assert stack.port.outcomes[-1].replayed
    # Changed arguments are a different approval: a new task, never the old decision.
    changed = {"command": "rm -rf papers"}
    other = await stack.port.resolve(
        _binding(stack, tool_input=changed), _request(changed), scope=scope
    )
    assert other.decision == "deny" and other.reason == "wait_expired"
    tasks = await stack.store.list_tasks(stack.scope)
    assert len(tasks) == 2 and {item.lifecycle for item in tasks} == {"resolved", "open"}


async def test_a_new_process_recovers_lost_correlations_before_it_resumes_the_session(
    tmp_path: Path,
) -> None:
    store = InMemoryApprovalStore()
    registry: dict[UUID, Any] = {}
    first = _broker_stack(
        tmp_path / "a",
        store=store,
        connection_ref="worker-a#1",
        registry=registry,
        script="interrupted_then_replaced",
    )
    scope = PermissionScope(request_scope=first.scope, run_id=first.identity.run_key)
    # The first process connects (lease, mirrored history) and holds a turn ...
    signals = RecordingSignals(first.claude.frames, first.identity.harness_execution_id)
    running = asyncio.create_task(first.claude.lanes.service.turn(_turn(first.claude), signals))
    await asyncio.wait_for((await first.claude.factory.connected()).held.wait(), timeout=10)
    # ... with a native request live (its wait never returns) when the process dies.
    payload = {"command": "ls drafts"}
    bound = await first.broker.bind(
        first.port.native_request(_binding(first, tool_input=payload), _request(payload), scope)
    )
    assert bound.correlation.state == "live"
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    second = _broker_stack(
        tmp_path / "a", store=store, connection_ref="worker-b#1", registry=registry
    )
    assert isinstance(second.port, RecoveringPermissionPort)
    heid = str(first.identity.harness_execution_id)
    second.claude.harness.stage(heid, second.claude.operation)
    assert second.claude.operation.provider_binding is not None
    handle = await second.claude.harness.reattach(
        ReattachRequest(
            scope=_harness_scope(first),
            lane_profile=PROFILE,
            harness_execution_id=heid,
            binding_digest=second.claude.operation.provider_binding.binding_digest,
            idempotency_key=f"{heid}:1:turn:2",
            generation=1,
            native_session_ref=SESSION_ID,
        )
    )
    assert handle.native_session_ref == SESSION_ID
    assert second.claude.factory.last.options.resume == SESSION_ID
    execution = second.claude.harness.execution_view(heid)
    assert execution is not None
    (item,) = execution.recovered
    assert item.correlation.correlation_id == bound.correlation.correlation_id
    assert item.action == "await_fresh_request"
    lost = await store.get_correlation(first.scope, bound.correlation.correlation_id)
    assert lost is not None and lost.state == "lost", "never reused by the new process"


def _harness_scope(stack: BrokerStack) -> Any:
    from mission_control.application.execution.harness.lane_turns import harness_scope

    return harness_scope(stack.scope)


def test_the_broker_wait_is_bounded_below_the_segment_budget() -> None:
    store = InMemoryApprovalStore()
    broker = ApprovalBroker(
        store, store, probe=StaticApprovalContext(1, "sha256:" + "a" * 64), connection_ref="w#1"
    )
    for wait in (0.0, -1.0, MAX_WAIT_SECONDS + 1):
        with pytest.raises(ValueError, match="bounded"):
            BrokerPermissionBinding(broker, wait_seconds=wait, reviewers=(REVIEWER,))
    with pytest.raises(ValueError, match="segment budget"):
        broker_permissions(broker, wait_seconds=120, reviewers=(REVIEWER,), segment_budget_s=60)
    with pytest.raises(ValueError, match="reviewer"):
        BrokerPermissionBinding(broker, wait_seconds=5, reviewers=())
    port = broker_permissions(broker, wait_seconds=300, reviewers=(REVIEWER,))
    assert port.wait_seconds == 300 and port.broker is broker


def test_system_replies_never_allow_and_deny_differs_from_cancel() -> None:
    from mission_control.application.execution.approvals import NativeReply
    from mission_control.application.execution.approvals_broker import ApprovalOutcome

    def outcome(reply: NativeReply, status: Any) -> ApprovalOutcome:
        return ApprovalOutcome(
            status=status,
            reply=reply,
            human_task_id="task-1",
            correlation_id="corr-1",
        )

    human = {"reason": "human_decision", "human_task_id": "task-1"}
    allow = permission_outcome(outcome(NativeReply(action="allow", **human), "approved"))
    assert (allow.decision, allow.interrupt) == ("approve", False)
    deny = permission_outcome(outcome(NativeReply(action="deny", **human), "denied"))
    assert (deny.decision, deny.interrupt) == ("deny", False)
    cancel = permission_outcome(
        outcome(NativeReply(action="cancel", interrupt=True, **human), "cancelled")
    )
    assert (cancel.decision, cancel.interrupt) == ("cancel", True)
    reasons: tuple[Any, ...] = ("wait_expired", "stale_generation", "policy_changed", "stop_fenced")
    for reason in reasons:
        system = permission_outcome(
            outcome(
                NativeReply(action="deny", reason=reason, interrupt=True, human_task_id="task-1"),
                "expired",
            )
        )
        assert (system.decision, system.interrupt, system.reason) == ("deny", True, reason)


# --- composition -------------------------------------------------------------------------------


def _compose_inputs(tmp_path: Path) -> dict[str, Any]:
    return {
        "workspaces": FixtureWorkspace(tmp_path / "leases"),
        "projections": RenderedProjectionSource(static_rows(projection_rows())),
        "auth": StaticAuthAdmitter(fixture_admission()),
        "child_environment": provider_child_environment,
        "environ": dict(FIXTURE_ENVIRON),
    }


def test_composition_refuses_a_windows_host_and_fails_closed_without_a_broker(
    tmp_path: Path,
) -> None:
    with pytest.raises(HostUnsupported):
        compose_claude_local(
            **_compose_inputs(tmp_path),
            host=host_gate(system="Windows", release="11", event_loop="SelectorEventLoop"),
        )
    linux = host_gate(system="Linux", release="6.6.87.2-microsoft-standard-WSL2", event_loop="none")
    harness = compose_claude_local(**_compose_inputs(tmp_path), host=linux)
    assert isinstance(harness._permissions, DenyWithoutGateway)
    store = InMemoryApprovalStore()
    broker = ApprovalBroker(
        store, store, probe=StaticApprovalContext(1, "sha256:" + "a" * 64), connection_ref="w#1"
    )
    port = broker_permissions(broker, wait_seconds=60, reviewers=(REVIEWER,))
    composed = compose_claude_local(**_compose_inputs(tmp_path), host=linux, permissions=port)
    assert composed._permissions is port
