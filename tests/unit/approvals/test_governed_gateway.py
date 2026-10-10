"""MP-11 governed prepare / review / execute and MCP elicitation negotiation.

In-memory stores and FIXTURE executors; the MCP tests run the real fastmcp/mcp protocol over
the in-memory transport (a fastmcp ``Client`` with or without an elicitation handler, which
controls whether the client declares the elicitation capability). No provider is called.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.elicitation import ElicitResult

from mission_control.application.execution.approvals import ElicitationPrompt
from mission_control.application.execution.approvals_governed import (
    GOVERNED_EFFECT_PERMISSION,
    ElicitedInput,
    GovernedEffectService,
    GovernedPrepareRequest,
    GovernedRejected,
    GovernedTool,
    GovernedToolPolicy,
    GovernedToolRegistry,
)
from mission_control.application.execution.approvals_memory import (
    InMemoryApprovalStore,
    InMemoryGovernedIntents,
    StaticApprovalContext,
)
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import StopFence
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.governed_elicitation import (
    client_elicitation_modes,
    negotiate_elicitation,
    to_elicit_result,
)
from mission_control.interfaces.mcp.governed_gateway import (
    APPROVAL_RESOLVE_TOOL,
    GOVERNED_EXECUTE_TOOL,
    GOVERNED_PREPARE_TOOL,
    GOVERNED_STATUS_TOOL,
    ScopedGovernedEffects,
    register_governed_tools,
)
from mission_control.interfaces.mcp.human_task_tools import (
    HUMAN_TASK_RESOLVE_TOOL,
    ScopedHumanTasks,
    register_human_task_tools,
)
from tests.fixtures.provider_frames import SCOPE
from tests.unit.approvals.fixtures import POLICY, REVIEWER, RUN, RecordingExecutor, answer

AGENT = ActorContext(actor_id="agent:lane", permissions=frozenset({GOVERNED_EFFECT_PERMISSION}))
OWNER = ActorContext(actor_id=REVIEWER)
ENV_PROMPT = ElicitationPrompt(
    message="Which environment?",
    requested_schema={
        "type": "object",
        "properties": {"environment": {"type": "string"}},
        "required": ["environment"],
    },
)


class Harness:
    def __init__(self, *, executor: RecordingExecutor | None = None, **policy: Any) -> None:
        self.executor = executor or RecordingExecutor()
        values: dict[str, Any] = {"reviewers": (REVIEWER,)}
        values.update(policy)
        self.registry = GovernedToolRegistry(
            (
                GovernedTool(
                    name="publish_report",
                    executor=self.executor,
                    policy=GovernedToolPolicy(**values),
                ),
            )
        )
        self.store = InMemoryApprovalStore()
        self.intents = InMemoryGovernedIntents()
        self.context = StaticApprovalContext(1, POLICY)
        self.fences = InMemoryStopFenceRepository()
        self.service = GovernedEffectService(
            self.intents,
            self.store,
            self.registry,
            request_scope=SCOPE,
            probe=self.context,
            fences=self.fences,
        )
        self.tasks = HumanTaskService(
            InMemoryHumanTaskRepository(), request_scope=SCOPE, approvals=self.store
        )

    async def approve(self, human_task_id: str, decision: str = "approve", **extra: Any) -> None:
        task = await self.store.get_task(SCOPE, human_task_id)
        assert task is not None
        await self.tasks.resolve(human_task_id, answer(task, decision=decision, **extra), OWNER)


def prepare_request(**arguments: Any) -> GovernedPrepareRequest:
    return GovernedPrepareRequest(
        run_id=RUN,
        harness_execution_id="harness-exec-1",
        generation=1,
        lane_profile="claude_agent_sdk",
        tool_name="publish_report",
        arguments=arguments or {"report": "q3", "channel": "internal"},
    )


async def test_prepare_returns_pending_without_executing_and_repeats_are_stable() -> None:
    harness = Harness()
    first = await harness.service.prepare(prepare_request(), AGENT)
    assert first.status == "pending_approval" and first.human_task is not None
    assert harness.executor.calls == []
    again = await harness.service.prepare(prepare_request(), AGENT)
    assert again == first
    pending = await harness.service.execute(first.intent_id, AGENT)
    assert pending.status == "pending_approval" and pending.human_task == first.human_task
    assert harness.executor.calls == []
    assert len(await harness.store.list_tasks(SCOPE)) == 1


async def test_execute_consumes_the_approval_once_and_repeats_return_the_receipt() -> None:
    harness = Harness(executor=RecordingExecutor(delay=0.02))
    state = await harness.service.prepare(prepare_request(), AGENT)
    assert state.human_task is not None
    await harness.approve(state.human_task["human_task_id"])
    results = await asyncio.gather(
        *(harness.service.execute(state.intent_id, AGENT) for _ in range(5))
    )
    assert harness.executor.calls == [state.intent_id]
    executed = [item for item in results if item.status == "executed"]
    assert executed and all(item.status in {"executed", "executing"} for item in results)
    receipt = executed[0].receipt
    assert receipt is not None and receipt.resolution_ref is not None
    for _ in range(3):
        repeat = await harness.service.execute(state.intent_id, AGENT)
        assert repeat.status == "executed" and repeat.receipt == receipt
    assert harness.executor.calls == [state.intent_id]
    assert (await harness.service.status(state.intent_id, AGENT)).receipt == receipt


async def test_edited_arguments_are_a_new_intent_and_review() -> None:
    harness = Harness()
    first = await harness.service.prepare(prepare_request(), AGENT)
    assert first.human_task is not None
    await harness.approve(first.human_task["human_task_id"])
    with pytest.raises(GovernedRejected) as changed:
        await harness.service.execute(
            first.intent_id, AGENT, arguments={"report": "q3", "channel": "public"}
        )
    assert changed.value.code == "arguments_changed"
    edited = await harness.service.prepare(prepare_request(report="q3", channel="public"), AGENT)
    assert edited.intent_id != first.intent_id and edited.status == "pending_approval"
    assert edited.human_task is not None
    assert edited.human_task["human_task_id"] != first.human_task["human_task_id"]
    assert (await harness.service.execute(edited.intent_id, AGENT)).status == "pending_approval"
    assert harness.executor.calls == []


async def test_deny_and_cancel_settle_distinctly_and_never_execute() -> None:
    harness = Harness()
    denied = await harness.service.prepare(prepare_request(report="a"), AGENT)
    cancelled = await harness.service.prepare(prepare_request(report="b"), AGENT)
    assert denied.human_task is not None and cancelled.human_task is not None
    await harness.approve(denied.human_task["human_task_id"], "deny", comment="not this quarter")
    await harness.approve(cancelled.human_task["human_task_id"], "cancel")
    first = await harness.service.execute(denied.intent_id, AGENT)
    second = await harness.service.execute(cancelled.intent_id, AGENT)
    assert (first.status, first.reason) == ("denied", "reviewer_denied")
    assert (second.status, second.reason) == ("cancelled", "reviewer_cancelled")
    assert harness.executor.calls == []


async def test_fence_generation_and_executor_failure_are_honest() -> None:
    harness = Harness()
    state = await harness.service.prepare(prepare_request(report="fenced"), AGENT)
    assert state.human_task is not None
    await harness.approve(state.human_task["human_task_id"])
    await harness.fences.persist(
        StopFence(
            request_scope=SCOPE,
            run_id=RUN,
            generation=1,
            command_id="cancel-1",
            reason="stop",
            requested_at=datetime(2026, 10, 8, 12, 1, tzinfo=UTC),
        )
    )
    fenced = await harness.service.execute(state.intent_id, AGENT)
    assert (fenced.status, fenced.reason) == ("fenced", "stop_fenced")
    with pytest.raises(GovernedRejected) as refused:
        await harness.service.prepare(prepare_request(report="after-fence"), AGENT)
    assert refused.value.code == "stop_fenced"

    stale = Harness()
    pending = await stale.service.prepare(prepare_request(), AGENT)
    assert pending.human_task is not None
    await stale.approve(pending.human_task["human_task_id"])
    stale.context.state = stale.context.state.model_copy(update={"generation": 2})
    moved = await stale.service.execute(pending.intent_id, AGENT)
    assert (moved.status, moved.reason) == ("stale", "stale_generation")

    failing = Harness(executor=RecordingExecutor(fail=True))
    doomed = await failing.service.prepare(prepare_request(), AGENT)
    assert doomed.human_task is not None
    await failing.approve(doomed.human_task["human_task_id"])
    doubt = await failing.service.execute(doomed.intent_id, AGENT)
    assert (doubt.status, doubt.reason) == ("in_doubt", "executor_error")
    assert (await failing.service.execute(doomed.intent_id, AGENT)).status == "in_doubt"
    assert len(failing.executor.calls) == 1


async def test_permission_and_unknown_tools_are_typed_refusals() -> None:
    harness = Harness()
    with pytest.raises(GovernedRejected) as missing:
        await harness.service.prepare(prepare_request(), ActorContext(actor_id="nobody"))
    assert missing.value.code == "not_permitted"
    with pytest.raises(GovernedRejected) as unknown:
        await harness.service.prepare(
            prepare_request().model_copy(update={"tool_name": "rm_rf"}), AGENT
        )
    assert unknown.value.code == "unknown_tool"


async def test_elicitation_fallbacks_are_explicit() -> None:
    strict = Harness(elicitation=ENV_PROMPT)
    with pytest.raises(GovernedRejected) as rejected:
        await strict.service.prepare(prepare_request(), AGENT)
    assert rejected.value.code == "elicitation_unsupported"
    review = Harness(elicitation=ENV_PROMPT, on_missing_elicitation="review_arguments")
    with pytest.raises(GovernedRejected) as incomplete:
        await review.service.prepare(prepare_request(), AGENT)
    assert incomplete.value.code == "invalid_arguments"
    pending = await review.service.prepare(prepare_request(report="q3", environment="prod"), AGENT)
    assert pending.status == "pending_approval"
    decline = await strict.service.prepare(
        prepare_request(), AGENT, elicited=ElicitedInput("decline")
    )
    cancel = await strict.service.prepare(
        prepare_request(), AGENT, elicited=ElicitedInput("cancel")
    )
    assert (decline.status, cancel.status) == ("denied", "cancelled")
    assert (
        strict.executor.calls == [] and await strict.intents.get(SCOPE, decline.intent_id) is None
    )


def test_negotiation_reads_declared_client_capabilities() -> None:
    from mcp import types as mcp_types

    assert client_elicitation_modes(None) == frozenset()
    assert client_elicitation_modes(mcp_types.ClientCapabilities()) == frozenset()
    implicit = mcp_types.ClientCapabilities(elicitation=mcp_types.ElicitationCapability())
    assert client_elicitation_modes(implicit) == frozenset({"form"})
    with pytest.raises(GovernedRejected) as missing:
        negotiate_elicitation(mcp_types.ClientCapabilities(), "form")
    assert missing.value.code == "elicitation_unsupported"
    with pytest.raises(GovernedRejected) as mode:
        negotiate_elicitation(implicit, "url")
    assert mode.value.code == "elicitation_mode_unsupported"


def test_lane_forwarded_elicitation_replies_map_onto_elicit_result() -> None:
    from mission_control.application.execution.approvals import NativeReply

    accept = NativeReply(
        action="allow",
        reason="human_decision",
        elicitation_action="accept",
        elicitation_content={"environment": "prod"},
        human_task_id="t",
    )
    decline = NativeReply(
        action="deny", reason="human_decision", elicitation_action="decline", human_task_id="t"
    )
    expired = NativeReply(action="deny", reason="wait_expired", interrupt=True, human_task_id="t")
    assert to_elicit_result(accept).model_dump(exclude_none=True) == {
        "action": "accept",
        "content": {"environment": "prod"},
    }
    assert to_elicit_result(decline).action == "decline"
    assert (
        to_elicit_result(expired).action == "cancel" and to_elicit_result(expired).content is None
    )


def _server(harness: Harness) -> FastMCP:
    principal = CoordinatorPrincipal(
        actor_id="agent:lane",
        tenant_scope=SCOPE,
        roles=frozenset(),
        permissions=frozenset({GOVERNED_EFFECT_PERMISSION}),
        request_scope=SCOPE,
    )
    reviewer = CoordinatorPrincipal(
        actor_id=REVIEWER,
        tenant_scope=SCOPE,
        roles=frozenset(),
        permissions=frozenset(),
        request_scope=SCOPE,
    )

    class Principals:
        def __init__(self) -> None:
            self.current = principal

        async def resolve(self, context: Any) -> CoordinatorPrincipal:
            del context
            return self.current

    principals = Principals()
    server = FastMCP("governed-test")
    scoped_tasks = ScopedHumanTasks({SCOPE: harness.tasks})
    register_governed_tools(
        server,
        ScopedGovernedEffects({SCOPE: harness.service}),
        principals,
        call=_principal_call,
        human_tasks=scoped_tasks,
    )
    register_human_task_tools(server, scoped_tasks, principals, call=_principal_call)
    server.reviewer = reviewer  # type: ignore[attr-defined]
    server.agent = principal  # type: ignore[attr-defined]
    server.principals = principals  # type: ignore[attr-defined]
    return server


def _args(**arguments: Any) -> dict[str, Any]:
    return {
        "tool_name": "publish_report",
        "arguments": arguments or {"report": "q3"},
        "run_id": RUN,
        "harness_execution_id": "harness-exec-1",
        "generation": 1,
        "lane_profile": "claude_agent_sdk",
    }


async def test_mcp_prepare_review_execute_round_trip() -> None:
    harness = Harness()
    server = _server(harness)
    async with Client(server) as client:
        names = {tool.name for tool in await client.list_tools()}
        assert {GOVERNED_PREPARE_TOOL, GOVERNED_EXECUTE_TOOL, GOVERNED_STATUS_TOOL} <= names
        prepared = (await client.call_tool(GOVERNED_PREPARE_TOOL, _args())).structured_content
        assert prepared is not None and prepared["ok"] is True
        data = prepared["data"]
        assert data["status"] == "pending_approval" and harness.executor.calls == []
        pending = await client.call_tool(GOVERNED_EXECUTE_TOOL, {"intent_id": data["intent_id"]})
        assert pending.structured_content["data"]["status"] == "pending_approval"  # type: ignore[index]
        # The reviewer answers through the frozen Human Task body (MCP parity with HTTP).
        server.principals.current = server.reviewer  # type: ignore[attr-defined]
        task = await harness.store.get_task(SCOPE, data["human_task"]["human_task_id"])
        assert task is not None
        resolved = await client.call_tool(
            HUMAN_TASK_RESOLVE_TOOL,
            {
                "human_task_id": task.human_task_id,
                "request": {
                    "request_id": "mcp-1",
                    "expected_task_version": 1,
                    "decision": "approve",
                    "reviewed_packet_digest": task.packet.review_digest,
                },
            },
        )
        assert resolved.structured_content["data"]["status"] == "accepted"  # type: ignore[index]
        server.principals.current = server.agent  # type: ignore[attr-defined]
        first = await client.call_tool(GOVERNED_EXECUTE_TOOL, {"intent_id": data["intent_id"]})
        second = await client.call_tool(GOVERNED_EXECUTE_TOOL, {"intent_id": data["intent_id"]})
        body = first.structured_content["data"]  # type: ignore[index]
        assert body["status"] == "executed" and body["receipt_digest"].startswith("sha256:")
        assert second.structured_content["data"] == body  # type: ignore[index]
        assert harness.executor.calls == [data["intent_id"]]
        changed = await client.call_tool(
            GOVERNED_EXECUTE_TOOL,
            {"intent_id": data["intent_id"], "arguments": {"report": "q4"}},
            raise_on_error=False,
        )
        error = changed.structured_content
        assert error is not None and error["ok"] is False
        assert error["error"]["details"]["code"] == "arguments_changed"


async def test_mcp_missing_client_elicitation_capability_is_a_typed_rejection() -> None:
    harness = Harness(elicitation=ENV_PROMPT)
    async with Client(_server(harness)) as client:  # no elicitation handler: no capability
        refused = await client.call_tool(GOVERNED_PREPARE_TOOL, _args(), raise_on_error=False)
    body = refused.structured_content
    assert body is not None and body["ok"] is False
    assert body["error"]["code"] == "CAPABILITY_INCOMPATIBLE"
    assert body["error"]["details"]["code"] == "elicitation_unsupported"
    assert harness.executor.calls == [] and await harness.store.list_tasks(SCOPE) == ()


@pytest.mark.parametrize(
    ("action", "status"),
    [("accept", "pending_approval"), ("decline", "denied"), ("cancel", "cancelled")],
)
async def test_mcp_elicitation_accept_decline_cancel_are_distinct(action: str, status: str) -> None:
    harness = Harness(elicitation=ENV_PROMPT)
    seen: list[str] = []

    async def handler(message: str, response_type: Any, params: Any, context: Any) -> Any:
        del response_type, params, context
        seen.append(message)
        if action == "accept":
            return ElicitResult(action="accept", content={"environment": "staging"})
        return ElicitResult(action=action)  # type: ignore[arg-type]

    async with Client(_server(harness), elicitation_handler=handler) as client:
        result = await client.call_tool(GOVERNED_PREPARE_TOOL, _args())
    data = result.structured_content["data"]  # type: ignore[index]
    assert seen == ["Which environment?"] and data["status"] == status
    if action == "accept":
        intent = await harness.intents.get(SCOPE, data["intent_id"])
        assert intent is not None and intent.arguments["environment"] == "staging"
    else:
        assert data["reason"] == f"elicitation_{action}"
    assert harness.executor.calls == []


async def test_mcp_extended_approval_resolution_cancel_differs_from_deny() -> None:
    harness = Harness()
    server = _server(harness)
    first = await harness.service.prepare(prepare_request(report="x"), AGENT)
    second = await harness.service.prepare(prepare_request(report="y"), AGENT)
    server.principals.current = server.reviewer  # type: ignore[attr-defined]
    async with Client(server) as client:
        for state, decision in ((first, "deny"), (second, "cancel")):
            assert state.human_task is not None
            task = await harness.store.get_task(SCOPE, state.human_task["human_task_id"])
            assert task is not None
            reply = await client.call_tool(
                APPROVAL_RESOLVE_TOOL,
                {
                    "human_task_id": task.human_task_id,
                    "request": answer(task, decision=decision).model_dump(mode="json"),
                },
            )
            assert reply.structured_content["ok"] is True  # type: ignore[index]
    denied = await harness.store.get_task(SCOPE, first.human_task["human_task_id"])  # type: ignore[index]
    cancelled = await harness.store.get_task(SCOPE, second.human_task["human_task_id"])  # type: ignore[index]
    assert denied is not None and denied.resolution is not None
    assert cancelled is not None and cancelled.resolution is not None
    assert denied.resolution.resolution_action == "denied"
    assert cancelled.resolution.resolution_action == "cancelled"


async def test_review_free_tools_still_revalidate_and_execute_once() -> None:
    harness = Harness(requires_approval=False, reviewers=())
    ready = await harness.service.prepare(prepare_request(), AGENT)
    assert ready.status == "ready" and ready.human_task is None
    first = await harness.service.execute(ready.intent_id, AGENT)
    again = await harness.service.execute(ready.intent_id, AGENT)
    assert first.status == "executed" and again.receipt == first.receipt
    assert harness.executor.calls == [ready.intent_id]

    moved = Harness(requires_approval=False, reviewers=())
    pending = await moved.service.prepare(prepare_request(), AGENT)
    moved.context.state = moved.context.state.model_copy(update={"generation": 2})
    stale = await moved.service.execute(pending.intent_id, AGENT)
    assert (stale.status, stale.reason) == ("stale", "stale_generation")
    assert moved.executor.calls == []
