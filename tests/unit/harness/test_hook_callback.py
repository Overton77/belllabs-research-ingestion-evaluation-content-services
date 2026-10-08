"""FT-G3: the Kernel Hook callback (`POST /v1/applications/{app}/internal/hook-callback`).

Task tokens (missing, unknown, expired, revoked, wrong generation, foreign execution), the
Stop Fence deny with its fenced frame, the Operation Intent written before `allow`, a binding
that disallows a tool, the hook frames, `sessionStart` context, the HTTP surface, and one
mapping case per Cursor hook event of SPEC-07 section 5.4.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from mission_control.adapters.cursor.hooks_callback import CURSOR_HOOK_EVENTS, CursorHookMapper
from mission_control.application.execution.harness.hook_callbacks import (
    DISALLOWED_TOOL,
    HookCallbackRejected,
    HookCallbackService,
    HookTokenContext,
    InMemoryHookIntentLedger,
    InMemoryHookTokenStore,
    KernelHookCall,
)
from mission_control.application.execution.stop_fence import (
    InMemoryStopFenceRepository,
    KernelHookFenceGate,
)
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.capabilities.hooks import HookDecision, HookEvent
from mission_control.domain.frames.contracts import FrameKind, HarnessExecutionStart, LaneProfile
from mission_control.domain.policies.stop_fence import STOP_FENCED, StopFence
from mission_control.interfaces.http.hook_callback import create_hook_callback_app
from tests.fixtures.lane_turns import ACTIVATION_UUID, INSTALLATION, RUN_UUID, SCOPE, TENANT

HEID = UUID("55555555-5555-4555-8555-555555555555")
RUN = "run-operation"
SCOPE_BLOCK = {
    "installation_id": str(INSTALLATION),
    "application_id": "biotech",
    "tenant_id": str(TENANT),
}


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 8, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def _context(generation: int = 1, **changes: Any) -> HookTokenContext:
    return HookTokenContext.model_validate(
        {
            "request_scope": SCOPE,
            "run_id": RUN,
            "attempt_no": generation,
            "generation": generation,
            "harness_execution_id": HEID,
            "lane_profile": "cursor_local",
            "execution_start": HarnessExecutionStart(
                harness_execution_id=HEID,
                request_scope=SCOPE,
                run_key=RUN,
                activation_key="sandbox-agent",
                attempt_no=generation,
                generation=generation,
                lane_profile=LaneProfile.CURSOR_LOCAL,
                native_session_ref="agent-1",
                runtime_kind="cursor",
                provider_kind="cursor_local",
                placement_kind="worker_hosted",
                intended_binding_digest="sha256:" + "c" * 64,
            ),
            "workspace_root": "/lease",
            **changes,
        }
    )


class Stack:
    def __init__(self) -> None:
        self.clock = Clock()
        self.tokens = InMemoryHookTokenStore()
        self.intents = InMemoryHookIntentLedger()
        self.fences = InMemoryStopFenceRepository()
        self.frames = InMemoryFrameStore()
        self.frames.register_run(SCOPE, RUN, RUN_UUID, {"sandbox-agent": ACTIVATION_UUID})
        self.service = HookCallbackService(
            tokens=self.tokens,
            intents=self.intents,
            mapper=CursorHookMapper(),
            fences=KernelHookFenceGate(self.fences),
            frames=self.frames,
            context_reader=lambda _context: "| binding | source |\n| --- | --- |",
            clock=self.clock,
        )

    def call(
        self, hook: str, event: str, payload: dict[str, Any], generation: int = 1
    ) -> KernelHookCall:
        return KernelHookCall(
            kernel_hook_id=hook,
            event=HookEvent(event),
            scope=SCOPE_BLOCK,  # type: ignore[arg-type]
            harness_execution_id=str(HEID),
            generation=generation,
            payload=payload,
        )


PRE_TOOL = {
    "conversation_id": "conv-1",
    "generation_id": "gen-1",
    "hook_event_name": "preToolUse",
    "tool_name": "Shell",
    "tool_input": {"command": "rm -rf build"},
    "tool_use_id": "call-1",
    "cwd": "/lease",
}


async def test_tokens_are_verified_before_anything_else() -> None:
    stack = Stack()
    token = await stack.service.issue(_context(), ttl=timedelta(minutes=10))
    call = stack.call("mc.stop_fence", "before_tool", PRE_TOOL)
    for bearer, code in ((None, "missing_task_token"), ("forged", "unknown_task_token")):
        with pytest.raises(HookCallbackRejected) as rejected:
            await stack.service.handle(call, bearer)
        assert rejected.value.code == code and rejected.value.status_code == 401
    with pytest.raises(HookCallbackRejected) as foreign:
        await stack.service.handle(stack.call("mc.stop_fence", "before_tool", PRE_TOOL, 2), token)
    assert foreign.value.code == "task_token_scope_mismatch"
    with pytest.raises(HookCallbackRejected) as other_app:
        await stack.service.handle(call, token, application_id="ai-engineer")
    assert other_app.value.code == "application_mismatch"
    assert (await stack.service.handle(call, token)).decision == HookDecision.ALLOW
    # A newer generation supersedes this token; then expiry and revocation reject.
    await stack.service.issue(_context(generation=2), ttl=timedelta(minutes=10))
    with pytest.raises(HookCallbackRejected) as stale:
        await stack.service.handle(call, token)
    assert stale.value.code == "STALE_GENERATION" and stale.value.status_code == 409
    fresh = Stack()
    expiring = await fresh.service.issue(_context(), ttl=timedelta(minutes=1))
    fresh.clock.now += timedelta(minutes=2)
    with pytest.raises(HookCallbackRejected) as expired:
        await fresh.service.handle(fresh.call("mc.usage", "stop", {}), expiring)
    assert expired.value.code == "expired_task_token"
    revoked = Stack()
    value = await revoked.service.issue(_context(), ttl=timedelta(minutes=10))
    await revoked.service.revoke(_context())
    with pytest.raises(HookCallbackRejected):
        await revoked.service.handle(revoked.call("mc.usage", "stop", {}), value)


async def test_the_operation_intent_is_written_before_allow_and_hooks_become_frames() -> None:
    stack = Stack()
    token = await stack.service.issue(_context(), ttl=timedelta(minutes=10))
    fence = await stack.service.handle(stack.call("mc.stop_fence", "before_tool", PRE_TOOL), token)
    assert fence.decision == HookDecision.ALLOW and stack.intents.intents() == ()
    intent = await stack.service.handle(
        stack.call("mc.operation_intent", "before_tool", PRE_TOOL), token
    )
    assert intent.decision == HookDecision.ALLOW
    (recorded,) = stack.intents.intents()
    assert (recorded.effect_ref, recorded.effect_kind) == ("tool_use:call-1", "shell")
    frames = list(stack.frames._executions[HEID].frames.values())
    kinds = sorted(frame.kind for frame in frames)
    assert kinds.count(FrameKind.HOOK_INVOKED) == 2 and kinds.count(FrameKind.HOOK_RESULT) == 2
    assert all(frame.tool_call_ref == "call-1" for frame in frames)
    # A repeated invocation (Cursor retry) stores nothing twice.
    await stack.service.handle(stack.call("mc.operation_intent", "before_tool", PRE_TOOL), token)
    assert len(stack.frames._executions[HEID].frames) == 4 and len(stack.intents.intents()) == 1


async def test_a_stop_fence_denies_with_a_fenced_frame_and_writes_no_intent() -> None:
    stack = Stack()
    token = await stack.service.issue(_context(), ttl=timedelta(minutes=10))
    await stack.fences.persist(
        StopFence(
            request_scope=SCOPE,
            run_id=RUN,
            generation=1,
            command_id="cancel-1",
            reason="wrong repository",
            requested_at=stack.clock.now,
        )
    )
    shell = {
        "conversation_id": "conv-1",
        "generation_id": "gen-1",
        "command": "git push --force",
        "cwd": "/lease",
    }
    for hook in ("mc.stop_fence", "mc.operation_intent"):
        result = await stack.service.handle(stack.call(hook, "before_shell", shell), token)
        assert result.decision == HookDecision.DENY
    assert stack.intents.intents() == ()
    results = [
        frame
        for frame in stack.frames._executions[HEID].frames.values()
        if frame.kind == FrameKind.HOOK_RESULT
    ]
    assert results and all("fenced" in frame.body_excerpt for frame in results)
    assert all(STOP_FENCED in frame.body_excerpt for frame in results)


async def test_a_tool_the_binding_disallows_is_denied_without_an_intent() -> None:
    stack = Stack()
    token = await stack.service.issue(
        _context(disallowed_tools=("shell",)), ttl=timedelta(minutes=10)
    )
    result = await stack.service.handle(
        stack.call("mc.operation_intent", "before_tool", PRE_TOOL), token
    )
    assert result.decision == HookDecision.DENY
    assert stack.intents.intents() == ()
    assert any(
        DISALLOWED_TOOL in frame.body_excerpt
        for frame in stack.frames._executions[HEID].frames.values()
    )


async def test_session_start_returns_the_packet_index_as_additional_context() -> None:
    stack = Stack()
    token = await stack.service.issue(_context(), ttl=timedelta(minutes=10))
    result = await stack.service.handle(
        stack.call(
            "mc.frame_capture",
            "session_start",
            {"session_id": "s-1", "is_background_agent": False, "composer_mode": "agent"},
        ),
        token,
    )
    assert result.additional_context and result.additional_context.startswith("| binding")


def test_the_http_callback_authenticates_with_the_bearer_token() -> None:
    stack = Stack()
    client = TestClient(create_hook_callback_app(stack.service))
    import asyncio

    token = asyncio.run(stack.service.issue(_context(), ttl=timedelta(minutes=10)))
    body = stack.call("mc.stop_fence", "before_tool", PRE_TOOL).model_dump(mode="json")
    path = "/v1/applications/biotech/internal/hook-callback"
    assert client.post(path, json=body).json()["detail"]["code"] == "missing_task_token"
    denied = client.post(path, json=body, headers={"Authorization": "Basic abc"})
    assert denied.status_code == 401
    accepted = client.post(path, json=body, headers={"Authorization": f"Bearer {token}"})
    assert accepted.status_code == 200 and accepted.json()["decision"] == "allow"
    assert token not in accepted.text
    wrong_app = client.post(
        "/v1/applications/ai-engineer/internal/hook-callback",
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert wrong_app.status_code == 403


# One case per Cursor hook event (SPEC-07 section 5.4): native payload -> mc.hook_input.v1.
CASES: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
    (
        "sessionStart",
        {"session_id": "s-1", "is_background_agent": True, "composer_mode": "agent"},
        {"details": {"session_id": "s-1", "is_background_agent": True}},
    ),
    (
        "beforeSubmitPrompt",
        {"prompt": "do it", "attachments": []},
        {"details_keys": {"prompt_digest"}},
    ),
    (
        "preToolUse",
        {"tool_name": "Write", "tool_input": {"path": "a.py"}, "tool_use_id": "t-1"},
        {"tool": ("Write", "t-1", "file"), "effect": ("tool_use:t-1", "file")},
    ),
    (
        "beforeShellExecution",
        {"command": "ls", "cwd": "/lease", "sandbox": False},
        {"tool": ("shell", None, "shell"), "effect_kind": "shell"},
    ),
    (
        "beforeMCPExecution",
        {"tool_name": "tavily_search", "tool_input": '{"q": "x"}', "mcp_server_name": "tavily"},
        {"tool": ("tavily_search", None, "mcp"), "effect_kind": "mcp", "input": {"q": "x"}},
    ),
    (
        "postToolUse",
        {"tool_name": "Read", "tool_input": {}, "tool_output": '{"ok": 1}', "tool_use_id": "t-2"},
        {"tool": ("Read", "t-2", "other"), "output": True},
    ),
    (
        "afterShellExecution",
        {"command": "ls", "output": "a\nb", "duration": 5, "sandbox": False},
        {"tool": ("shell", None, "shell"), "output": True},
    ),
    (
        "afterMCPExecution",
        {"tool_name": "tavily_search", "tool_input": "{}", "result_json": "{}", "duration": 9},
        {"tool": ("tavily_search", None, "other")},
    ),
    (
        "postToolUseFailure",
        {
            "tool_name": "Shell",
            "tool_input": {},
            "tool_use_id": "t-3",
            "error_message": "boom",
            "failure_type": "error",
            "is_interrupt": False,
        },
        {"tool": ("Shell", "t-3", "shell"), "error": "boom"},
    ),
    (
        "afterFileEdit",
        {"file_path": "src/a.py", "edits": [{"old_string": "a", "new_string": "b"}]},
        {"tool": ("edit_file", None, "file"), "input": {"file_path": "src/a.py", "edit_count": 1}},
    ),
    (
        "subagentStart",
        {
            "subagent_id": "sa-1",
            "subagent_type": "verifier",
            "task": "check",
            "tool_call_id": "t-4",
        },
        {"tool": ("task", "t-4", "task"), "effect": ("subagent:sa-1", "task")},
    ),
    (
        "subagentStop",
        {"subagent_type": "verifier", "status": "completed", "modified_files": ["a.py"]},
        {
            "tool": ("task", None, "task"),
            "details": {"status": "completed", "modified_files": ["a.py"]},
        },
    ),
    (
        "preCompact",
        {"trigger": "auto", "context_usage_percent": 91, "message_count": 40},
        {"details": {"trigger": "auto", "context_usage_percent": 91, "message_count": 40}},
    ),
    (
        "stop",
        {"status": "completed", "loop_count": 1},
        {"details": {"status": "completed", "loop_count": 1}},
    ),
    (
        "sessionEnd",
        {"session_id": "s-1", "reason": "completed", "final_status": "finished"},
        {"details": {"reason": "completed", "final_status": "finished"}},
    ),
    ("afterAgentResponse", {"text": "done"}, {}),
]


def test_the_mapping_covers_every_documented_cursor_hook_event() -> None:
    assert {case[0] for case in CASES} == set(CURSOR_HOOK_EVENTS)


@pytest.mark.parametrize(("native", "payload", "expected"), CASES, ids=[case[0] for case in CASES])
def test_each_cursor_hook_event_maps_to_its_hook_input(
    native: str, payload: dict[str, Any], expected: dict[str, Any]
) -> None:
    mapper = CursorHookMapper()
    event = CURSOR_HOOK_EVENTS[native]
    common = {
        "conversation_id": "conv-1",
        "generation_id": "gen-1",
        "hook_event_name": native,
        "cursor_version": "2.6.0",
        "workspace_roots": ["/lease"],
        "user_email": None,
    }
    hook_input = mapper.hook_input(event, {**common, **payload}, _context())
    assert hook_input.event == event and hook_input.lane_profile.value == "cursor_local"
    assert hook_input.native_session_ref == "conv-1" and hook_input.native_turn_ref == "gen-1"
    assert hook_input.details["provider_event"] == native
    assert hook_input.provider_payload_digest is not None
    if "tool" in expected:
        assert hook_input.tool is not None
        name, call_id, kind = expected["tool"]
        assert (hook_input.tool.name, hook_input.tool.call_id) == (name, call_id)
        assert hook_input.tool.side_effect_class == kind
    else:
        assert hook_input.tool is None
    for key, value in expected.get("details", {}).items():
        assert hook_input.details[key] == value
    assert expected.get("details_keys", set()) <= set(hook_input.details)
    if "input" in expected:
        assert hook_input.tool is not None and hook_input.tool.input == expected["input"]
    if expected.get("output"):
        assert hook_input.tool is not None and hook_input.tool.output_excerpt
    if "error" in expected:
        assert hook_input.tool is not None and hook_input.tool.error == expected["error"]
    effect = mapper.effect(event, {**common, **payload})
    if "effect" in expected:
        assert effect == expected["effect"]
    elif "effect_kind" in expected:
        assert effect is not None and effect[1] == expected["effect_kind"]
    elif event not in {HookEvent.BEFORE_TOOL, HookEvent.BEFORE_SHELL, HookEvent.BEFORE_MCP}:
        assert effect is None
    # The raw payload never rides along: only its digest and bounded excerpts.
    assert "user_email" not in hook_input.model_dump_json()
