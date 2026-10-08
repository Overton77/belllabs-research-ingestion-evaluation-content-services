"""FT-A5: hook contracts, dispatcher, HookScriptMiddleware and the compaction wrapper."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from pydantic import ValidationError

import mission_control.adapters.deep_agents.hooks as hooks_module
from mission_control.adapters.capabilities.capability_bundles import bundle_digest
from mission_control.adapters.deep_agents.hooks import (
    HookContext,
    HookDispatcher,
    HookScriptMiddleware,
    KernelHookPorts,
    MissionSummarizationMiddleware,
    RecordingFrameSink,
    ResolvedHookScript,
    ScriptOutcome,
    SubprocessHookScriptRunner,
    scrubbed_environment,
)
from mission_control.adapters.deep_agents.materializer import (
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    ResolvedHookScriptBundle,
)
from mission_control.contracts.hooks import (
    ADDITIONAL_CONTEXT_LIMIT,
    HookInput,
    HookResult,
    HookScope,
    HookToolCall,
    hook_contract_schemas,
    merge_results,
    parse_script_output,
)
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.capabilities.hooks import KERNEL_HOOK_IDS, HookDecision, HookEvent
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.execution.contracts import DeepAgentHookScriptComponent

SECRETS = ("sk-FIXTURE-openai-0001", "tvly-FIXTURE-0002")
POLICY_ROOT = Path(__file__).resolve().parents[3] / "scripts" / "hooks" / "policy_template"
SCOPE = HookScope(installation_id="inst", application_id="biotech", tenant_id="tenant")


# --- contracts -------------------------------------------------------------------------------


def test_hook_result_contract_rules() -> None:
    with pytest.raises(ValidationError, match="reason is required"):
        HookResult(decision=HookDecision.DENY)
    with pytest.raises(ValidationError, match="reason is required"):
        HookResult(decision=HookDecision.DEFER)
    with pytest.raises(ValidationError):
        HookResult.model_validate({"decision": "ask"})
    with pytest.raises(ValidationError):
        HookResult(additional_context="x" * (ADDITIONAL_CONTEXT_LIMIT + 1))
    schemas = hook_contract_schemas()
    assert set(schemas) == {"mc.hook_input.v1", "mc.hook_result.v1", "mc.hook_frame.v1"}
    assert schemas["mc.hook_result.v1"]["properties"]["decision"]


def test_merge_rules() -> None:
    merged = merge_results(
        [
            HookResult(additional_context="a", updated_input={"command": "x"}),
            HookResult(decision=HookDecision.DEFER, reason="review", additional_context="b"),
            HookResult(updated_input={"command": "y"}),
        ]
    )
    assert merged.decision is HookDecision.DEFER
    assert merged.additional_context == "a\n\nb"
    assert merged.updated_input == {"command": "y"}
    denied = merge_results(
        [HookResult(decision=HookDecision.DEFER, reason="r"), HookResult.deny("no")]
    )
    assert denied.decision is HookDecision.DENY and "no" in (denied.reason or "")
    assert merge_results([]).decision is HookDecision.ALLOW


def test_parse_script_output() -> None:
    assert parse_script_output(b"", 0, hook_id="h").decision is HookDecision.ALLOW
    assert parse_script_output(b"", 2, hook_id="h").decision is HookDecision.DENY
    result = parse_script_output(
        b'{"schema_version":"mc.hook_result.v1","decision":"allow","reason":"x"}', 2, hook_id="h"
    )
    assert result.decision is HookDecision.DENY
    with pytest.raises(hooks_module.HookScriptFailure):
        parse_script_output(b"not json", 0, hook_id="h")
    with pytest.raises(hooks_module.HookScriptFailure):
        parse_script_output(b"", 1, hook_id="h")


# --- dispatcher ------------------------------------------------------------------------------


@dataclass
class FakeRunner:
    """Records stdin per hook and answers with a scripted outcome."""

    answers: dict[str, Callable[[dict[str, Any]], ScriptOutcome]]
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def run(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome:
        return self.run_sync(script, stdin, cwd)

    def run_sync(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome:
        payload = json.loads(stdin)
        self.calls.append((script.hook_id, payload))
        return self.answers[script.hook_id](payload)


def _ok(result: HookResult | None = None) -> Callable[[dict[str, Any]], ScriptOutcome]:
    body = (result or HookResult()).model_dump_json().encode()
    return lambda _payload: ScriptOutcome(exit_code=0, stdout=body)


def _script(hook_id: str, *events: HookEvent, **values: Any) -> ResolvedHookScript:
    return ResolvedHookScript(
        hook_id=hook_id,
        events=frozenset(events),
        interpreter="python",
        entrypoint=Path("unused.py"),
        **values,
    )


@dataclass
class FencedGate:
    reason: str | None = None

    async def fenced(self, hook_input: HookInput) -> str | None:
        return self.reason


def _dispatcher(
    runner: FakeRunner,
    *scripts: ResolvedHookScript,
    gate: FencedGate | None = None,
    frames: RecordingFrameSink | None = None,
    allowed: frozenset[str] | None = None,
) -> HookDispatcher:
    ports = KernelHookPorts(
        stop_fence=gate or FencedGate(),
        frames=frames or RecordingFrameSink(),
        allowed_side_effect_classes=allowed,
    )
    return HookDispatcher(
        context=HookContext(scope=SCOPE, run_id="run-1", workspace_root="/work"),
        kernel_hooks=ports.build(),
        scripts=scripts,
        runner=runner,
        frames=ports.frames,
        cwd=Path(),
    )


@pytest.mark.asyncio
async def test_kernel_hooks_run_first_and_stop_fence_short_circuits() -> None:
    frames = RecordingFrameSink()
    runner = FakeRunner({"hook.a": _ok()})
    dispatcher = _dispatcher(
        runner,
        _script("hook.a", HookEvent.BEFORE_SHELL),
        gate=FencedGate("stop fence"),
        frames=frames,
    )
    tool = HookToolCall(name="shell", call_id="c1", input={"command": "ls"})
    result = await dispatcher.dispatch(HookEvent.BEFORE_SHELL, tool=tool)
    assert result.decision is HookDecision.DENY and result.reason == "stop fence"
    assert runner.calls == []  # catalog hook never ran after the kernel deny
    assert [frame.hook_id for frame in frames.frames] == ["mc.stop_fence"]
    assert dispatcher.order == (*KERNEL_HOOK_IDS, "hook.a")


@pytest.mark.asyncio
async def test_operation_intent_denies_disallowed_side_effects() -> None:
    dispatcher = _dispatcher(FakeRunner({}), allowed=frozenset({"read_only"}))
    tool = HookToolCall(name="write_file", side_effect_class="workspace_write")
    result = await dispatcher.dispatch(HookEvent.BEFORE_TOOL, tool=tool)
    assert result.decision is HookDecision.DENY and "workspace_write" in (result.reason or "")


@pytest.mark.asyncio
async def test_fail_closed_timeout_denies_and_fail_open_allows() -> None:
    timed_out = lambda _p: ScriptOutcome(exit_code=-1, stdout=b"", timed_out=True)  # noqa: E731
    frames = RecordingFrameSink()
    closed = _dispatcher(
        FakeRunner({"hook.c": timed_out}),
        _script("hook.c", HookEvent.BEFORE_TOOL, fail_closed=True, timeout_seconds=1),
        frames=frames,
    )
    denied = await closed.dispatch(HookEvent.BEFORE_TOOL, tool=HookToolCall(name="x"))
    assert denied.decision is HookDecision.DENY and "timed out" in (denied.reason or "")
    assert frames.frames[-1].failed is True
    opened = _dispatcher(
        FakeRunner({"hook.o": timed_out}), _script("hook.o", HookEvent.BEFORE_TOOL)
    )
    allowed = await opened.dispatch(HookEvent.BEFORE_TOOL, tool=HookToolCall(name="x"))
    assert allowed.decision is HookDecision.ALLOW


def test_dispatcher_refuses_reordered_or_missing_kernel_hooks() -> None:
    kernel = KernelHookPorts().build()
    with pytest.raises(ValueError, match="kernel hooks must be exactly"):
        HookDispatcher(context=HookContext(scope=SCOPE), kernel_hooks=tuple(reversed(kernel)))
    with pytest.raises(ValueError, match="kernel hooks must be exactly"):
        HookDispatcher(context=HookContext(scope=SCOPE), kernel_hooks=kernel[1:])
    with pytest.raises(ValueError, match="kernel hook ids"):
        HookDispatcher(
            context=HookContext(scope=SCOPE),
            kernel_hooks=kernel,
            scripts=(_script("mc.stop_fence", HookEvent.STOP),),
        )


@pytest.mark.asyncio
async def test_stdin_and_environment_never_carry_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", SECRETS[0])
    monkeypatch.setenv("TAVILY_API_KEY", SECRETS[1])
    runner = FakeRunner({"hook.a": _ok()})
    dispatcher = _dispatcher(runner, _script("hook.a", HookEvent.BEFORE_TOOL))
    await dispatcher.dispatch(
        HookEvent.BEFORE_TOOL, tool=HookToolCall(name="search", input={"q": "x"})
    )
    stdin = json.dumps(runner.calls[0][1])
    assert all(secret not in stdin for secret in SECRETS)
    env = scrubbed_environment()
    assert "OPENAI_API_KEY" not in env and "TAVILY_API_KEY" not in env
    assert all(secret not in json.dumps(env) for secret in SECRETS)


@pytest.mark.asyncio
async def test_real_subprocess_runner_runs_the_policy_template(tmp_path: Path) -> None:
    root = POLICY_ROOT
    script = ResolvedHookScript(
        hook_id="hook.mc-policy-template",
        events=frozenset({HookEvent.BEFORE_SHELL}),
        interpreter="python",
        entrypoint=root / "policy.py",
        timeout_seconds=30,
        fail_closed=True,
    )
    dispatcher = HookDispatcher(
        context=HookContext(scope=SCOPE, workspace_root=str(tmp_path)),
        kernel_hooks=KernelHookPorts().build(),
        scripts=(script,),
        runner=SubprocessHookScriptRunner(),
        cwd=tmp_path,
    )
    rm = HookToolCall(name="shell", input={"command": "rm -rf /"})
    assert (
        await dispatcher.dispatch(HookEvent.BEFORE_SHELL, tool=rm)
    ).decision is HookDecision.DENY
    ok = HookToolCall(name="shell", input={"command": "pytest -q"})
    assert (
        await dispatcher.dispatch(HookEvent.BEFORE_SHELL, tool=ok)
    ).decision is HookDecision.ALLOW
    assert dispatcher.dispatch_sync(HookEvent.BEFORE_SHELL, tool=rm).decision is HookDecision.DENY


# --- middleware: tool calls ------------------------------------------------------------------


@dataclass
class Request:
    tool_call: dict[str, Any]
    state: dict[str, Any] = field(default_factory=dict)

    def override(self, **values: Any) -> Request:
        return Request(tool_call=values.get("tool_call", self.tool_call), state=self.state)


def _call(name: str, args: dict[str, Any] | None = None) -> Request:
    return Request(
        tool_call={"name": name, "args": args or {}, "id": "call-1", "type": "tool_call"}
    )


def _handler(seen: list[dict[str, Any]], content: str = "done", status: str = "success"):
    def handle(request: Request) -> ToolMessage:
        seen.append(request.tool_call["args"])
        return ToolMessage(content=content, tool_call_id="call-1", status=status)

    return handle


def test_before_shell_deny_stops_the_tool() -> None:
    runner = FakeRunner({"hook.p": _ok(HookResult.deny("no rm"))})
    middleware = HookScriptMiddleware(
        _dispatcher(runner, _script("hook.p", HookEvent.BEFORE_SHELL))
    )
    seen: list[dict[str, Any]] = []
    result = middleware.wrap_tool_call(_call("shell", {"command": "rm -rf /"}), _handler(seen))
    assert isinstance(result, ToolMessage) and result.status == "error"
    assert "no rm" in str(result.content)
    assert seen == []
    assert runner.calls[0][1]["event"] == "before_shell"
    assert runner.calls[0][1]["tool"]["input"] == {"command": "rm -rf /"}


def test_updated_input_and_additional_context() -> None:
    runner = FakeRunner(
        {
            "hook.rewrite": _ok(HookResult(updated_input={"command": "pytest -q -x"})),
            "hook.context": _ok(HookResult(additional_context="remember the budget")),
        }
    )
    middleware = HookScriptMiddleware(
        _dispatcher(
            runner,
            _script("hook.rewrite", HookEvent.BEFORE_SHELL),
            _script("hook.context", HookEvent.AFTER_SHELL),
        )
    )
    seen: list[dict[str, Any]] = []
    result = middleware.wrap_tool_call(_call("shell", {"command": "pytest -q"}), _handler(seen))
    assert seen == [{"command": "pytest -q -x"}]
    assert "remember the budget" in str(result.content)


def test_defer_interrupts_and_reject_denies(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[Any] = []

    def fake_interrupt(payload: Any) -> dict[str, Any]:
        requests.append(payload)
        return {"decisions": [{"type": "reject"}]}

    monkeypatch.setattr(hooks_module, "interrupt", fake_interrupt)
    runner = FakeRunner(
        {"hook.d": _ok(HookResult(decision=HookDecision.DEFER, reason="ask a human"))}
    )
    middleware = HookScriptMiddleware(_dispatcher(runner, _script("hook.d", HookEvent.BEFORE_TOOL)))
    seen: list[dict[str, Any]] = []
    result = middleware.wrap_tool_call(_call("search", {"q": "x"}), _handler(seen))
    assert result.status == "error" and seen == []
    assert requests[0]["action_requests"][0]["name"] == "search"
    assert requests[0]["review_configs"][0]["allowed_decisions"] == ["approve", "reject"]
    monkeypatch.setattr(hooks_module, "interrupt", lambda _p: {"decisions": [{"type": "approve"}]})
    approved = middleware.wrap_tool_call(_call("search", {"q": "x"}), _handler(seen))
    assert approved.status == "success" and seen == [{"q": "x"}]


@pytest.mark.parametrize(
    ("tool", "pre", "post"),
    [
        ("shell", {"before_tool", "before_shell"}, {"after_tool", "after_shell"}),
        ("write_file", {"before_tool"}, {"after_tool", "after_file_edit"}),
        ("task", {"before_tool", "subagent_start"}, {"after_tool", "subagent_stop"}),
        ("mcp__pubmed__search", {"before_tool", "before_mcp"}, {"after_tool"}),
        ("tavily_search", {"before_tool", "before_mcp"}, {"after_tool"}),
    ],
)
def test_tool_events_are_mapped(tool: str, pre: set[str], post: set[str]) -> None:
    every = tuple(HookEvent)
    runner = FakeRunner({"hook.all": _ok()})
    middleware = HookScriptMiddleware(
        _dispatcher(runner, _script("hook.all", *every)), mcp_tools=frozenset({"tavily_search"})
    )
    middleware.wrap_tool_call(_call(tool), _handler([]))
    events = [payload["event"] for _, payload in runner.calls]
    assert set(events) == pre | post
    assert events.index(sorted(post)[0]) > max(events.index(item) for item in pre)


def test_tool_failure_fires_after_tool_failure() -> None:
    runner = FakeRunner({"hook.f": _ok()})
    middleware = HookScriptMiddleware(
        _dispatcher(runner, _script("hook.f", HookEvent.AFTER_TOOL_FAILURE, HookEvent.AFTER_TOOL))
    )

    def boom(_request: Request) -> ToolMessage:
        raise RuntimeError("tool exploded")

    with pytest.raises(RuntimeError):
        middleware.wrap_tool_call(_call("search"), boom)
    assert [p["event"] for _, p in runner.calls] == ["after_tool_failure"]
    assert runner.calls[0][1]["tool"]["error"] == "tool exploded"
    runner.calls.clear()
    middleware.wrap_tool_call(_call("search"), _handler([], status="error"))
    assert [p["event"] for _, p in runner.calls] == ["after_tool_failure"]


@pytest.mark.asyncio
async def test_async_tool_path_matches_sync() -> None:
    runner = FakeRunner({"hook.p": _ok(HookResult.deny("blocked"))})
    middleware = HookScriptMiddleware(
        _dispatcher(runner, _script("hook.p", HookEvent.BEFORE_SHELL))
    )

    async def handler(_request: Request) -> ToolMessage:
        raise AssertionError("must not run")

    result = await middleware.awrap_tool_call(_call("shell", {"command": "rm -rf /"}), handler)
    assert result.status == "error"


# --- middleware: model calls -----------------------------------------------------------------


@dataclass
class ModelRequest:
    messages: list[Any]
    state: dict[str, Any] = field(default_factory=dict)

    def override(self, **values: Any) -> ModelRequest:
        return ModelRequest(messages=values.get("messages", self.messages), state=self.state)


def test_model_events_session_prompt_and_stop() -> None:
    runner = FakeRunner({"hook.all": _ok(HookResult(additional_context="context from hook"))})
    middleware = HookScriptMiddleware(_dispatcher(runner, _script("hook.all", *tuple(HookEvent))))
    seen: list[list[Any]] = []

    def model(request: ModelRequest) -> AIMessage:
        seen.append(request.messages)
        return AIMessage(content="final answer")

    middleware.wrap_model_call(ModelRequest(messages=[HumanMessage(content="go")]), model)
    events = [payload["event"] for _, payload in runner.calls]
    assert events == [
        "session_start",
        "before_prompt",
        "before_model",
        "after_model",
        "stop",
        "session_end",
    ]
    assert isinstance(seen[0][-1], HumanMessage) and "context from hook" in str(seen[0][-1].content)
    runner.calls.clear()
    tool_call = AIMessage(content="", tool_calls=[{"name": "x", "args": {}, "id": "1"}])
    middleware.wrap_model_call(
        ModelRequest(messages=[HumanMessage(content="go"), tool_call]), lambda _r: tool_call
    )
    assert [p["event"] for _, p in runner.calls] == ["before_model", "after_model"]


def test_before_model_deny_skips_the_model() -> None:
    runner = FakeRunner({"hook.m": _ok(HookResult.deny("budget exhausted"))})
    middleware = HookScriptMiddleware(
        _dispatcher(runner, _script("hook.m", HookEvent.BEFORE_MODEL))
    )
    result = middleware.wrap_model_call(
        ModelRequest(messages=[HumanMessage(content="go")]),
        lambda _r: (_ for _ in ()).throw(AssertionError("model must not run")),
    )
    assert isinstance(result, AIMessage) and "budget exhausted" in str(result.content)
    assert not result.tool_calls


# --- compaction ------------------------------------------------------------------------------


def test_summarization_wrapper_keeps_name_and_emits_compaction_events() -> None:
    runner = FakeRunner({"hook.c": _ok()})
    dispatcher = _dispatcher(
        runner, _script("hook.c", HookEvent.BEFORE_COMPACTION, HookEvent.AFTER_COMPACTION)
    )
    middleware = MissionSummarizationMiddleware.__new__(MissionSummarizationMiddleware)
    middleware.dispatcher = dispatcher
    assert middleware.name == "SummarizationMiddleware"
    event = {"cutoff_index": 7, "summary_message": HumanMessage(content="s"), "file_path": "/h.md"}
    response = hooks_module.ExtendedModelResponse(
        model_response=AIMessage(content="x"),
        command=hooks_module.Command(update={"_summarization_event": event}),
    )
    assert hooks_module._new_summarization_event(response, None) == {
        "cutoff_index": 7,
        "file_path": "/h.md",
    }
    assert hooks_module._new_summarization_event(response, event) is None

    class Parent:
        def wrap_model_call(self, request: Any, handler: Any) -> Any:
            return response

        def _create_summary(self, messages: list[Any]) -> str:
            return "summary"

    original_wrap = hooks_module.SummarizationMiddleware.wrap_model_call
    original_summary = hooks_module.SummarizationMiddleware._create_summary
    try:
        hooks_module.SummarizationMiddleware.wrap_model_call = Parent.wrap_model_call  # type: ignore[method-assign]
        hooks_module.SummarizationMiddleware._create_summary = Parent._create_summary  # type: ignore[method-assign]
        assert middleware._create_summary([HumanMessage(content="a")]) == "summary"
        middleware.wrap_model_call(SimpleNamespace(state={}), lambda _r: None)
    finally:
        hooks_module.SummarizationMiddleware.wrap_model_call = original_wrap  # type: ignore[method-assign]
        hooks_module.SummarizationMiddleware._create_summary = original_summary  # type: ignore[method-assign]
    events = [payload["event"] for _, payload in runner.calls]
    assert events == ["before_compaction", "after_compaction"]
    assert runner.calls[1][1]["details"] == {"cutoff_index": 7, "file_path": "/h.md"}


# --- materializer composition ----------------------------------------------------------------


def _component(hook_id: str, order: int, files: tuple[tuple[str, bytes], ...]) -> Any:
    return DeepAgentHookScriptComponent(
        ref=ExactDefinitionRef(
            kind=DefinitionKind.HOOK_SCRIPT,
            logical_id=hook_id,
            revision=1,
            digest="sha256:" + "1" * 64,
        ),
        hook_id=hook_id,
        order=order,
        events=("before_shell",),
        interpreter="python",
        entrypoint="hook.py",
        manifest_digest=bundle_digest(files, require_skill_md=False),
    )


def test_materializer_composes_kernel_hooks_before_three_catalog_hooks() -> None:
    from contextlib import AsyncExitStack, ExitStack

    bundles: dict[str, ResolvedHookScriptBundle] = {}
    components = []
    for order, hook_id in ((2, "hook.c"), (0, "hook.a"), (1, "hook.b")):
        files = (("hook.py", f"# {hook_id}\n".encode()),)
        component = _component(hook_id, order, files)
        bundles[component.manifest_digest] = ResolvedHookScriptBundle(
            component.manifest_digest, files
        )
        components.append(component)
    registry = ExactComponentRegistry(model_factories={}, hook_scripts=bundles)
    materializer = ExactDeepAgentMaterializer(registry)
    binding = SimpleNamespace(
        hook_scripts=tuple(components),
        run_id="run-1",
        operation_id="op-1",
        operation_attempt=1,
        execution_generation=1,
        binding_id="binding-1",
        runtime_unit=None,
    )

    class SyncStack(ExitStack):
        pass

    stack = AsyncExitStack()
    with ExitStack() as cleanup:
        cleanup.callback(lambda: None)
        dispatcher = materializer._hook_dispatcher(binding, stack)  # type: ignore[arg-type]
        assert dispatcher.order == (*KERNEL_HOOK_IDS, "hook.a", "hook.b", "hook.c")
        assert all(script.entrypoint.is_file() for script in dispatcher.scripts)
        assert dispatcher.context.lane_profile is LaneProfile.DEEP_AGENTS
    tampered = dict(bundles)
    key = next(iter(tampered))
    tampered[key] = ResolvedHookScriptBundle(key, (("hook.py", b"# tampered\n"),))
    with pytest.raises(Exception, match="drifted"):
        ExactDeepAgentMaterializer(
            ExactComponentRegistry(model_factories={}, hook_scripts=tampered)
        )._hook_dispatcher(binding, AsyncExitStack())  # type: ignore[arg-type]
