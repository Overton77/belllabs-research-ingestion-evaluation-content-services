"""Hook Scripts on the Deep Agents lane (ADR-0026, SPEC-01 "HookScriptMiddleware").

One ``HookScriptMiddleware`` per attempt runs Mission Control's four Kernel Hooks (stop
fence, operation intent, frame capture, usage) first and in fixed order, then the catalog
hook scripts, at the Deep Agents lifecycle points the Hook Event vocabulary maps to. Scripts
run as subprocesses on the worker: ``mc.hook_input.v1`` on stdin, ``mc.hook_result.v1`` on
stdout, exit code 2 denies. Results merge ``deny`` > ``defer`` > ``allow``. A ``deny`` before
a tool returns a ``ToolMessage(status="error")`` without calling it; ``updated_input``
rewrites the call; ``additional_context`` reaches the model; ``defer`` interrupts through the
``HumanInTheLoopMiddleware`` request/decision shape. ``MissionSummarizationMiddleware``
replaces the Deep Agents summarization middleware in place (same ``.name``) and emits
``before_compaction`` and ``after_compaction``. Every invocation is handed to the frame sink.

Unlike ``deepagents-code``'s ``ServerHooksMiddleware`` (which round-trips through
``interrupt()`` so a client runs the command), the worker owns the process and runs the
scripts directly.
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Protocol

from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.summarization import (
    SummarizationMiddleware,
    create_summarization_middleware,
)
from langchain.agents.middleware.types import AgentMiddleware, ExtendedModelResponse
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage
from langgraph.types import Command, interrupt

from mission_control.contracts.hooks import (
    HookCallbackEndpoint,
    HookFrame,
    HookFrameSink,
    HookInput,
    HookResult,
    HookScope,
    HookScriptFailure,
    HookToolCall,
    digest_bytes,
    merge_results,
    parse_script_output,
)
from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.capabilities.hooks import (
    KERNEL_HOOK_EVENTS,
    KERNEL_HOOK_IDS,
    HookDecision,
    HookEvent,
)
from mission_control.domain.capabilities.host_support import LaneProfile

DEFAULT_SHELL_TOOLS = frozenset({"shell", "execute", "bash"})
DEFAULT_FILE_EDIT_TOOLS = frozenset({"write_file", "edit_file"})
TASK_TOOL = "task"
_SAFE_ENV = ("PATH", "SYSTEMROOT", "SystemRoot", "TEMP", "TMP", "HOME", "USERPROFILE", "LANG")


# ------------------------------------------------------------------------------------------
# Scripts and the subprocess runner
# ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedHookScript:
    """A catalog hook script resolved to verified files on the worker's disk."""

    hook_id: str
    events: frozenset[HookEvent]
    interpreter: str
    entrypoint: Path
    timeout_seconds: int = 30
    fail_closed: bool = False
    matcher: str | None = None
    callback: bool = False

    def matches(self, event: HookEvent, tool_name: str | None) -> bool:
        if event not in self.events:
            return False
        if self.matcher is None or tool_name is None:
            return True
        return re.search(self.matcher, tool_name) is not None


@dataclass(frozen=True)
class ScriptOutcome:
    exit_code: int
    stdout: bytes
    stderr: bytes = b""
    timed_out: bool = False


class HookScriptRunner(Protocol):
    async def run(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome: ...

    def run_sync(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome: ...


def _interpreter_command(script: ResolvedHookScript) -> list[str]:
    if script.interpreter == "python":
        return [sys.executable, str(script.entrypoint)]
    return [script.interpreter, str(script.entrypoint)]


def scrubbed_environment(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Hook scripts get no service credentials: only the variables a process needs to start."""
    env = {name: os.environ[name] for name in _SAFE_ENV if name in os.environ}
    env.update(extra or {})
    return env


class SubprocessHookScriptRunner:
    """Runs each script as a child process with a scrubbed environment and a hard timeout."""

    async def run(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome:
        process = await asyncio.create_subprocess_exec(
            *_interpreter_command(script),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(cwd),
            env=scrubbed_environment(),
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin), timeout=script.timeout_seconds
            )
        except TimeoutError:
            process.kill()
            await process.wait()
            return ScriptOutcome(exit_code=-1, stdout=b"", timed_out=True)
        return ScriptOutcome(exit_code=process.returncode or 0, stdout=stdout, stderr=stderr)

    def run_sync(self, script: ResolvedHookScript, stdin: bytes, cwd: Path) -> ScriptOutcome:
        try:
            completed = subprocess.run(
                _interpreter_command(script),
                input=stdin,
                capture_output=True,
                cwd=str(cwd),
                env=scrubbed_environment(),
                timeout=script.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return ScriptOutcome(exit_code=-1, stdout=b"", timed_out=True)
        return ScriptOutcome(
            exit_code=completed.returncode, stdout=completed.stdout, stderr=completed.stderr
        )


# ------------------------------------------------------------------------------------------
# Kernel hooks and their ports
# ------------------------------------------------------------------------------------------


class KernelHook(Protocol):
    @property
    def hook_id(self) -> str: ...

    @property
    def events(self) -> frozenset[HookEvent]: ...

    async def __call__(self, hook_input: HookInput) -> HookResult: ...


class StopFenceGate(Protocol):
    """Port: the persisted Stop Fence for the run generation (SPEC-06 F3 provides it)."""

    async def fenced(self, hook_input: HookInput) -> str | None: ...


class OperationIntentLedger(Protocol):
    """Port: write the Operation Intent keyed on the tool-call id before the effect."""

    async def record_intent(self, hook_input: HookInput) -> None: ...


class UsageRecorder(Protocol):
    async def record_usage(self, hook_input: HookInput) -> None: ...


class NoStopFence:
    async def fenced(self, hook_input: HookInput) -> str | None:
        return None


class NullIntentLedger:
    async def record_intent(self, hook_input: HookInput) -> None:
        return None


class NullUsageRecorder:
    async def record_usage(self, hook_input: HookInput) -> None:
        return None


class NullFrameSink:
    async def record_hook_frame(self, frame: HookFrame) -> None:
        return None


@dataclass
class RecordingFrameSink:
    """In-memory sink (tests and local runs without the provider frame store)."""

    frames: list[HookFrame] = field(default_factory=list)

    async def record_hook_frame(self, frame: HookFrame) -> None:
        self.frames.append(frame)


@dataclass(frozen=True)
class _Kernel:
    hook_id: str
    events: frozenset[HookEvent]
    handler: Callable[[HookInput], Awaitable[HookResult]]

    async def __call__(self, hook_input: HookInput) -> HookResult:
        return await self.handler(hook_input)


@dataclass(frozen=True)
class KernelHookPorts:
    """The ports behind the four kernel hooks; bootstrap wires real ones."""

    stop_fence: StopFenceGate = field(default_factory=NoStopFence)
    intents: OperationIntentLedger = field(default_factory=NullIntentLedger)
    usage: UsageRecorder = field(default_factory=NullUsageRecorder)
    frames: HookFrameSink = field(default_factory=NullFrameSink)
    allowed_side_effect_classes: frozenset[str] | None = None

    def build(self, lane_profile: LaneProfile = LaneProfile.DEEP_AGENTS) -> tuple[KernelHook, ...]:
        async def stop_fence(hook_input: HookInput) -> HookResult:
            reason = await self.stop_fence.fenced(hook_input)
            return HookResult.allow() if reason is None else HookResult.deny(reason)

        async def operation_intent(hook_input: HookInput) -> HookResult:
            tool = hook_input.tool
            allowed = self.allowed_side_effect_classes
            if (
                tool is not None
                and allowed is not None
                and tool.side_effect_class is not None
                and tool.side_effect_class not in allowed
            ):
                return HookResult.deny(
                    f"side-effect class {tool.side_effect_class} is not allowed by the binding"
                )
            await self.intents.record_intent(hook_input)
            return HookResult.allow()

        async def frame_capture(hook_input: HookInput) -> HookResult:
            payload = hook_input.to_stdin()
            await self.frames.record_hook_frame(
                HookFrame(
                    hook_id="mc.frame_capture",
                    kernel=True,
                    event=hook_input.event,
                    lane_profile=lane_profile,
                    decision=HookDecision.ALLOW,
                    input_digest=digest_bytes(payload),
                    result_digest=digest_bytes(b""),
                    tool_call_id=None if hook_input.tool is None else hook_input.tool.call_id,
                    duration_ms=0,
                )
            )
            return HookResult.allow()

        async def usage(hook_input: HookInput) -> HookResult:
            await self.usage.record_usage(hook_input)
            return HookResult.allow()

        handlers = {
            "mc.stop_fence": stop_fence,
            "mc.operation_intent": operation_intent,
            "mc.frame_capture": frame_capture,
            "mc.usage": usage,
        }
        return tuple(
            _Kernel(hook_id, frozenset(KERNEL_HOOK_EVENTS[hook_id]), handlers[hook_id])
            for hook_id in KERNEL_HOOK_IDS
        )


# ------------------------------------------------------------------------------------------
# Dispatcher: kernel first, then scripts; frames for every invocation
# ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class HookContext:
    """The non-secret identity every ``mc.hook_input.v1`` carries for this attempt."""

    scope: HookScope
    lane_profile: LaneProfile = LaneProfile.DEEP_AGENTS
    run_id: str | None = None
    activation_id: str | None = None
    attempt_no: int | None = None
    generation: int | None = None
    harness_execution_id: str | None = None
    native_session_ref: str | None = None
    workspace_root: str | None = None
    callback: HookCallbackEndpoint | None = None


def _run_coroutine[T](factory: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run a coroutine from a sync middleware hook, even when a loop is already running."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    box: dict[str, Any] = {}

    def target() -> None:
        try:
            box["value"] = asyncio.run(factory())
        except BaseException as error:
            box["error"] = error

    thread = threading.Thread(target=target, name="mc-hook-dispatch")
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    value: T = box["value"]
    return value


class HookDispatcher:
    """Runs the composed hook list for one event and merges the results."""

    def __init__(
        self,
        *,
        context: HookContext,
        kernel_hooks: Sequence[KernelHook],
        scripts: Sequence[ResolvedHookScript] = (),
        runner: HookScriptRunner | None = None,
        frames: HookFrameSink | None = None,
        cwd: Path | None = None,
    ) -> None:
        ids = [hook.hook_id for hook in kernel_hooks]
        if tuple(ids) != KERNEL_HOOK_IDS:
            raise ValueError(f"kernel hooks must be exactly {KERNEL_HOOK_IDS} in order; got {ids}")
        overlap = {script.hook_id for script in scripts} & set(KERNEL_HOOK_IDS)
        if overlap:
            raise ValueError(f"catalog hooks cannot take kernel hook ids: {sorted(overlap)}")
        self.context = context
        self.kernel_hooks = tuple(kernel_hooks)
        self.scripts = tuple(scripts)
        self._runner = runner or SubprocessHookScriptRunner()
        self._frames = frames or NullFrameSink()
        self._cwd = cwd or Path(context.workspace_root or ".")

    @property
    def order(self) -> tuple[str, ...]:
        return (*(hook.hook_id for hook in self.kernel_hooks), *(s.hook_id for s in self.scripts))

    def hook_input(
        self,
        event: HookEvent,
        *,
        tool: HookToolCall | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> HookInput:
        context = self.context
        return HookInput(
            event=event,
            lane_profile=context.lane_profile,
            scope=context.scope,
            run_id=context.run_id,
            activation_id=context.activation_id,
            attempt_no=context.attempt_no,
            generation=context.generation,
            harness_execution_id=context.harness_execution_id,
            native_session_ref=context.native_session_ref,
            tool=tool,
            workspace_root=context.workspace_root,
            callback=context.callback,
            details=dict(details or {}),
        )

    async def dispatch(
        self,
        event: HookEvent,
        *,
        tool: HookToolCall | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> HookResult:
        hook_input = self.hook_input(event, tool=tool, details=details)
        payload = hook_input.to_stdin()
        results: list[HookResult] = []
        for kernel in self.kernel_hooks:
            if event not in kernel.events:
                continue
            started = time.monotonic()
            try:
                result = await kernel(hook_input)
                failed = False
            except Exception as error:
                result, failed = (
                    HookResult.deny(f"kernel hook {kernel.hook_id} failed: {error}"),
                    True,
                )
            await self._frame(kernel.hook_id, True, hook_input, payload, result, started, failed)
            results.append(result)
            if result.decision is HookDecision.DENY:
                return merge_results(results)
        tool_name = None if tool is None else tool.name
        for script in self.scripts:
            if not script.matches(event, tool_name):
                continue
            started = time.monotonic()
            outcome = await self._runner.run(script, payload, self._cwd)
            result, failed = _interpret(script, outcome)
            await self._frame(script.hook_id, False, hook_input, payload, result, started, failed)
            results.append(result)
            if result.decision is HookDecision.DENY:
                break
        return merge_results(results)

    def dispatch_sync(
        self,
        event: HookEvent,
        *,
        tool: HookToolCall | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> HookResult:
        return _run_coroutine(lambda: self.dispatch(event, tool=tool, details=details))

    async def _frame(
        self,
        hook_id: str,
        kernel: bool,
        hook_input: HookInput,
        payload: bytes,
        result: HookResult,
        started: float,
        failed: bool,
    ) -> None:
        await self._frames.record_hook_frame(
            HookFrame(
                hook_id=hook_id,
                kernel=kernel,
                event=hook_input.event,
                lane_profile=hook_input.lane_profile,
                decision=result.decision,
                reason=result.reason,
                input_digest=digest_bytes(payload),
                result_digest=stable_json_digest(result),
                tool_call_id=None if hook_input.tool is None else hook_input.tool.call_id,
                duration_ms=int((time.monotonic() - started) * 1000),
                failed=failed,
            )
        )


def _interpret(script: ResolvedHookScript, outcome: ScriptOutcome) -> tuple[HookResult, bool]:
    if outcome.timed_out:
        failure = f"hook {script.hook_id} timed out after {script.timeout_seconds}s"
    else:
        try:
            return parse_script_output(
                outcome.stdout, outcome.exit_code, hook_id=script.hook_id
            ), False
        except HookScriptFailure as error:
            failure = str(error)
    if script.fail_closed:
        return HookResult.deny(failure), True
    return HookResult.allow(), True


# ------------------------------------------------------------------------------------------
# Middleware
# ------------------------------------------------------------------------------------------


class HookScriptMiddleware(AgentMiddleware[Any, Any, Any]):
    """Maps Deep Agents lifecycle points to Hook Events and applies merged hook results."""

    serialized_name: ClassVar[str] = "MissionControlHookScriptMiddleware"

    def __init__(
        self,
        dispatcher: HookDispatcher,
        *,
        shell_tools: frozenset[str] = DEFAULT_SHELL_TOOLS,
        file_edit_tools: frozenset[str] = DEFAULT_FILE_EDIT_TOOLS,
        mcp_tools: frozenset[str] = frozenset(),
        side_effect_classes: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.dispatcher = dispatcher
        self._shell = shell_tools
        self._file_edit = file_edit_tools
        self._mcp = mcp_tools
        self._side_effects = dict(side_effect_classes or {})

    @property
    def name(self) -> str:
        return self.serialized_name

    # -- events per tool ------------------------------------------------------------------

    def _pre_events(self, tool_name: str) -> list[HookEvent]:
        events = [HookEvent.BEFORE_TOOL]
        if tool_name in self._shell:
            events.append(HookEvent.BEFORE_SHELL)
        if tool_name in self._mcp or tool_name.startswith("mcp__"):
            events.append(HookEvent.BEFORE_MCP)
        if tool_name == TASK_TOOL:
            events.append(HookEvent.SUBAGENT_START)
        return events

    def _post_events(self, tool_name: str, *, failed: bool) -> list[HookEvent]:
        if failed:
            return [HookEvent.AFTER_TOOL_FAILURE]
        events = [HookEvent.AFTER_TOOL]
        if tool_name in self._shell:
            events.append(HookEvent.AFTER_SHELL)
        if tool_name in self._file_edit:
            events.append(HookEvent.AFTER_FILE_EDIT)
        if tool_name == TASK_TOOL:
            events.append(HookEvent.SUBAGENT_STOP)
        return events

    def _tool(self, tool_call: Mapping[str, Any], **extra: Any) -> HookToolCall:
        name = str(tool_call.get("name", ""))
        args = tool_call.get("args") or {}
        return HookToolCall(
            name=name,
            call_id=tool_call.get("id"),
            input=dict(args) if isinstance(args, Mapping) else {"value": args},
            side_effect_class=self._side_effects.get(name),
            **extra,
        )

    # -- model calls ------------------------------------------------------------------------
    # Only wrap-style hooks are used: node-style hooks would add graph nodes and so change
    # the checkpoint topology that recovery and fork rely on. Session and prompt events are
    # derived from the model call; stop and session_end fire when the model answers without
    # tool calls (the agent loop ends).

    def _pre_model_events(self, request: Any) -> list[HookEvent]:
        messages = list(getattr(request, "messages", []) or [])
        events: list[HookEvent] = []
        if not any(isinstance(message, AIMessage) for message in messages):
            events.append(HookEvent.SESSION_START)
        if messages and isinstance(messages[-1], HumanMessage):
            events.append(HookEvent.BEFORE_PROMPT)
        events.append(HookEvent.BEFORE_MODEL)
        return events

    def _review(self, name: str, args: Mapping[str, Any], reason: str | None) -> bool:
        decision = interrupt(
            {
                "action_requests": [
                    {"name": name, "args": dict(args), "description": reason or ""}
                ],
                "review_configs": [
                    {"action_name": name, "allowed_decisions": ["approve", "reject"]}
                ],
            }
        )
        return _approved(decision)

    def _model_pre(self, request: Any, merged: HookResult) -> tuple[Any, AIMessage | None]:
        if merged.decision is HookDecision.DEFER and not self._review(
            "model_call", {}, merged.reason
        ):
            return request, _stopped(f"rejected after review: {merged.reason}")
        if merged.decision is HookDecision.DENY:
            return request, _stopped(merged.reason or "denied")
        if merged.additional_context:
            request = request.override(
                messages=[*request.messages, HumanMessage(content=merged.additional_context)]
            )
        return request, None

    def _model_post(self, response: Any, merged: HookResult) -> Any:
        if merged.decision is HookDecision.DENY:
            return _stopped(merged.reason or "denied")
        return response

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        pre = merge_results(
            [self.dispatcher.dispatch_sync(event) for event in self._pre_model_events(request)]
        )
        request, stopped = self._model_pre(request, pre)
        if stopped is not None:
            return stopped
        response = handler(request)
        events = [HookEvent.AFTER_MODEL]
        if _final_answer(response):
            events += [HookEvent.STOP, HookEvent.SESSION_END]
        post = merge_results([self.dispatcher.dispatch_sync(event) for event in events])
        return self._model_post(response, post)

    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        pre = merge_results(
            [await self.dispatcher.dispatch(event) for event in self._pre_model_events(request)]
        )
        request, stopped = self._model_pre(request, pre)
        if stopped is not None:
            return stopped
        response = await handler(request)
        events = [HookEvent.AFTER_MODEL]
        if _final_answer(response):
            events += [HookEvent.STOP, HookEvent.SESSION_END]
        post = merge_results([await self.dispatcher.dispatch(event) for event in events])
        return self._model_post(response, post)

    # -- tool calls -------------------------------------------------------------------------

    def _pre(self, request: Any, results: Sequence[HookResult]) -> tuple[Any, ToolMessage | None]:
        merged = merge_results(results)
        tool_call = request.tool_call
        if merged.decision is HookDecision.DEFER:
            if not self._review(tool_call["name"], tool_call.get("args") or {}, merged.reason):
                return request, _denied(tool_call, f"rejected after review: {merged.reason}")
        elif merged.decision is HookDecision.DENY:
            return request, _denied(tool_call, merged.reason or "denied")
        if merged.updated_input is not None:
            request = request.override(tool_call={**tool_call, "args": merged.updated_input})
        # Pre-tool additional_context reaches the model appended to the tool result (_post).
        return request, None

    def _post(self, response: Any, pre_context: str | None, post: HookResult) -> Any:
        contexts = [item for item in (pre_context, post.additional_context) if item]
        if post.decision is HookDecision.DENY and post.reason:
            contexts.append(f"[Mission Control hook] {post.reason}")
        if not contexts or not isinstance(response, ToolMessage):
            return response
        suffix = "\n\n".join(contexts)
        content = response.content
        text = content if isinstance(content, str) else str(content)
        return response.model_copy(update={"content": f"{text}\n\n{suffix}"})

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        name = str(request.tool_call.get("name", ""))
        tool = self._tool(request.tool_call)
        pre = [self.dispatcher.dispatch_sync(e, tool=tool) for e in self._pre_events(name)]
        pre_merged = merge_results(pre)
        request, denied = self._pre(request, pre)
        if denied is not None:
            return denied
        try:
            response = handler(request)
        except Exception as error:
            failure = self._tool(request.tool_call, error=str(error)[:4000])
            self.dispatcher.dispatch_sync(HookEvent.AFTER_TOOL_FAILURE, tool=failure)
            raise
        failed = isinstance(response, ToolMessage) and response.status == "error"
        done = self._tool(request.tool_call, output_excerpt=_excerpt(response))
        post = merge_results(
            self.dispatcher.dispatch_sync(e, tool=done)
            for e in self._post_events(name, failed=failed)
        )
        return self._post(response, pre_merged.additional_context, post)

    async def awrap_tool_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        name = str(request.tool_call.get("name", ""))
        tool = self._tool(request.tool_call)
        pre = [await self.dispatcher.dispatch(e, tool=tool) for e in self._pre_events(name)]
        pre_merged = merge_results(pre)
        request, denied = self._pre(request, pre)
        if denied is not None:
            return denied
        try:
            response = await handler(request)
        except Exception as error:
            failure = self._tool(request.tool_call, error=str(error)[:4000])
            await self.dispatcher.dispatch(HookEvent.AFTER_TOOL_FAILURE, tool=failure)
            raise
        failed = isinstance(response, ToolMessage) and response.status == "error"
        done = self._tool(request.tool_call, output_excerpt=_excerpt(response))
        post = merge_results(
            [
                await self.dispatcher.dispatch(e, tool=done)
                for e in self._post_events(name, failed=failed)
            ]
        )
        return self._post(response, pre_merged.additional_context, post)


def _approved(decision: Any) -> bool:
    decisions = decision.get("decisions") if isinstance(decision, Mapping) else None
    if not decisions:
        return False
    first = decisions[0]
    return isinstance(first, Mapping) and first.get("type") == "approve"


def _stopped(reason: str) -> AIMessage:
    return AIMessage(content=f"Stopped by Mission Control hook: {reason}")


def _final_answer(response: Any) -> bool:
    """True when the model produced no tool calls, so the agent loop ends after it."""
    if isinstance(response, ExtendedModelResponse):
        response = response.model_response
    messages = [response] if isinstance(response, AIMessage) else getattr(response, "result", [])
    ai = [message for message in messages if isinstance(message, AIMessage)]
    return bool(ai) and not ai[-1].tool_calls


def _denied(tool_call: Mapping[str, Any], reason: str) -> ToolMessage:
    return ToolMessage(
        content=f"Denied by Mission Control hook: {reason}",
        tool_call_id=str(tool_call.get("id", "")),
        name=str(tool_call.get("name", "")),
        status="error",
    )


def _excerpt(response: Any) -> str | None:
    if isinstance(response, ToolMessage):
        content = response.content
        text = content if isinstance(content, str) else str(content)
        return text[:4000]
    if isinstance(response, Command):
        return None
    return None


# ------------------------------------------------------------------------------------------
# Compaction
# ------------------------------------------------------------------------------------------


class MissionSummarizationMiddleware(SummarizationMiddleware):
    """Deep Agents summarization that emits ``before_compaction`` and ``after_compaction``.

    Keeps the parent's ``.name`` (``SummarizationMiddleware``) so ``create_deep_agent``
    replaces its default in place rather than stacking a second summarizer.
    """

    dispatcher: HookDispatcher

    @property
    def name(self) -> str:
        # deepagents reports type(self).__name__ for subclasses; keep the public alias so
        # create_deep_agent replaces its default summarizer in place.
        return "SummarizationMiddleware"

    @classmethod
    def from_default(
        cls, model: BaseChatModel, backend: BackendProtocol, dispatcher: HookDispatcher
    ) -> MissionSummarizationMiddleware:
        """Adopt the model-aware defaults of ``create_summarization_middleware``."""
        base = create_summarization_middleware(model, backend)
        instance = cls.__new__(cls)
        instance.__dict__.update(base.__dict__)
        instance.dispatcher = dispatcher
        return instance

    def _create_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        self.dispatcher.dispatch_sync(
            HookEvent.BEFORE_COMPACTION, details={"message_count": len(messages_to_summarize)}
        )
        return super()._create_summary(messages_to_summarize)

    async def _acreate_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        await self.dispatcher.dispatch(
            HookEvent.BEFORE_COMPACTION, details={"message_count": len(messages_to_summarize)}
        )
        return await super()._acreate_summary(messages_to_summarize)

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        previous = request.state.get("_summarization_event")
        response = super().wrap_model_call(request, handler)
        event = _new_summarization_event(response, previous)
        if event is not None:
            self.dispatcher.dispatch_sync(HookEvent.AFTER_COMPACTION, details=event)
        return response

    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        previous = request.state.get("_summarization_event")
        response = await super().awrap_model_call(request, handler)
        event = _new_summarization_event(response, previous)
        if event is not None:
            await self.dispatcher.dispatch(HookEvent.AFTER_COMPACTION, details=event)
        return response


def _new_summarization_event(response: Any, previous: Any) -> dict[str, Any] | None:
    if not isinstance(response, ExtendedModelResponse) or response.command is None:
        return None
    update = response.command.update
    if not isinstance(update, Mapping) or "_summarization_event" not in update:
        return None
    event = update["_summarization_event"]
    if not isinstance(event, Mapping) or event == previous:
        return None
    return {
        "cutoff_index": event.get("cutoff_index"),
        "file_path": event.get("file_path"),
    }
