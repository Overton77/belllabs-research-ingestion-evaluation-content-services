"""Kernel Hooks as in-process Claude Agent SDK callbacks (SPEC-07 sections 5.4/5.5; MP-03/MP-07).

On `claude_agent_sdk` the MP-03 projection keeps the Kernel Hooks out of the agent-writable
settings file and hands them to the lane as `send_options["hook_callbacks"]`: one entry per
(kernel hook, Mission Control event) with the native SDK event (`PreToolUse`, ...), an
optional tool matcher, a timeout and `fail_closed=True`, restricted to the events the pinned
Python SDK exposes as callbacks (`domain/capabilities/hooks.CLAUDE_SDK_CALLBACK_EVENTS`).
This module turns those entries into `ClaudeAgentOptions.hooks` (`types.HookMatcher` lists
keyed by `types.HookEvent`) whose callbacks (`types.HookCallback`) do what the command-hook
lanes' `.mission/hooks/kernel.py` does through the loopback callback:

- every invocation is a `hook.invoked` / `hook.result` frame of the harness execution;
- `mc.stop_fence` and `mc.operation_intent` on a permission event (`before_tool`,
  `before_shell`, `before_mcp`, `subagent_start`) consult the Stop Fence
  (`KernelHookFenceGate.before_effect`) and deny a fenced effect; `mc.operation_intent` also
  refuses a tool the binding disallows and writes the Operation Intent keyed on the effect
  ref *before* answering allow;
- `mc.frame_capture` and `mc.usage` observe (`PreCompact` is the honest before-compaction
  observation; no control is claimed);
- any failure inside a callback answers deny (fail closed).

Decision shapes come from `types.PreToolUseHookSpecificOutput` (`permissionDecision` /
`permissionDecisionReason`) and `types.SyncHookJSONOutput` (`continue_` / `stopReason` /
`decision: "block"` for events without a permission decision). The CLI dispatches matchers of
one event concurrently (`types.ClaudeAgentOptions.hooks`), so each callback is independent.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Protocol, cast, get_args
from uuid import UUID

from claude_agent_sdk.types import HookContext, HookEvent, HookInput, HookJSONOutput, HookMatcher

from mission_control.application.execution.harness.hook_callbacks import (
    DISALLOWED_TOOL,
    GATED_EVENTS,
    HookEffectIntent,
    HookIntentLedger,
)
from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.capabilities.hooks import HookEvent as McHookEvent
from mission_control.domain.execution.lanes import LaneFrame
from mission_control.domain.policies.stop_fence import EffectAdmission, EffectKind

_LOGGER = logging.getLogger(__name__)
# `types.HookEvent` is a union of one-value Literals: flatten it to the event names.
SDK_HOOK_EVENTS: Final[frozenset[str]] = frozenset(
    str(name) for literal in get_args(HookEvent) for name in get_args(literal)
)
FENCING_HOOKS: Final = frozenset({"mc.stop_fence", "mc.operation_intent"})
_FILE_TOOLS: Final = frozenset({"Edit", "Write", "MultiEdit", "NotebookEdit"})
_TASK_TOOLS: Final = frozenset({"Task", "Agent"})
HOOK_INVOKED: Final = "hook.invoked"
HOOK_RESULT: Final = "hook.result"


class FrameEmitter(Protocol):
    async def emit(
        self, raw_kind: str, body: Mapping[str, Any], *, tool_call_ref: str | None = None
    ) -> LaneFrame: ...


@dataclass(frozen=True)
class KernelHookContext:
    """What a Kernel Hook callback is bound to; secret-free."""

    request_scope: str
    run_id: str
    generation: int
    harness_execution_id: UUID
    lane_profile: str
    disallowed_tools: tuple[str, ...] = ()


@dataclass(frozen=True)
class HookCallbackSpec:
    """One `send_options["hook_callbacks"]` entry (MP-03 `_kernel_callbacks`)."""

    hook_id: str
    mc_event: McHookEvent
    native_event: str
    matcher: str | None
    timeout: float | None
    fail_closed: bool

    @classmethod
    def parse(cls, entry: Mapping[str, Any]) -> HookCallbackSpec:
        native = str(entry["native_event"])
        if native not in SDK_HOOK_EVENTS:
            raise ValueError(f"{native} is not a claude_agent_sdk hook callback event")
        timeout = entry.get("timeout")
        return cls(
            hook_id=str(entry["hook_id"]),
            mc_event=McHookEvent(str(entry["mc_event"])),
            native_event=native,
            matcher=str(entry["matcher"]) if entry.get("matcher") else None,
            timeout=float(timeout) if isinstance(timeout, int | float) else None,
            fail_closed=bool(entry.get("fail_closed", True)),
        )


def effect_kind_of(tool_name: str | None, event: McHookEvent) -> EffectKind:
    if event is McHookEvent.SUBAGENT_START or tool_name in _TASK_TOOLS:
        return "task"
    if tool_name == "Bash":
        return "shell"
    if tool_name is not None and tool_name.startswith("mcp__"):
        return "mcp"
    if tool_name in _FILE_TOOLS:
        return "file"
    return "other"


def _deny(native_event: str, reason: str) -> HookJSONOutput:
    if native_event == "PreToolUse":
        return cast(
            HookJSONOutput,
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                }
            },
        )
    return cast(
        HookJSONOutput,
        {"continue_": False, "stopReason": reason, "decision": "block", "reason": reason},
    )


def _allow() -> HookJSONOutput:
    return cast(HookJSONOutput, {})


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


class KernelHookCallbacks:
    """Builds the SDK hook matchers of one session from the projected callback specs."""

    def __init__(
        self,
        specs: Sequence[HookCallbackSpec],
        *,
        context: KernelHookContext,
        emitter: FrameEmitter,
        fences: KernelHookFenceGate | None,
        intents: HookIntentLedger | None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._specs = tuple(specs)
        self._context = context
        self._emitter = emitter
        self._fences = fences
        self._intents = intents
        self._clock = clock or (lambda: datetime.now(UTC))
        self.invocations: list[tuple[str, str, str]] = []

    @classmethod
    def from_send_options(
        cls,
        send_options: Mapping[str, Any],
        *,
        context: KernelHookContext,
        emitter: FrameEmitter,
        fences: KernelHookFenceGate | None,
        intents: HookIntentLedger | None,
        clock: Callable[[], datetime] | None = None,
    ) -> KernelHookCallbacks:
        raw = send_options.get("hook_callbacks")
        entries = raw if isinstance(raw, list | tuple) else ()
        specs = [HookCallbackSpec.parse(entry) for entry in entries if isinstance(entry, Mapping)]
        return cls(
            specs, context=context, emitter=emitter, fences=fences, intents=intents, clock=clock
        )

    @property
    def specs(self) -> tuple[HookCallbackSpec, ...]:
        return self._specs

    def matchers(self) -> dict[HookEvent, list[HookMatcher]]:
        """`ClaudeAgentOptions.hooks`: one matcher per spec, in projection order."""

        hooks: dict[HookEvent, list[HookMatcher]] = {}
        for spec in self._specs:
            event = cast(HookEvent, spec.native_event)
            hooks.setdefault(event, []).append(
                HookMatcher(
                    matcher=spec.matcher, hooks=[self._callback(spec)], timeout=spec.timeout
                )
            )
        return hooks

    def _callback(
        self, spec: HookCallbackSpec
    ) -> Callable[[HookInput, str | None, HookContext], Awaitable[HookJSONOutput]]:
        async def callback(
            hook_input: HookInput, tool_use_id: str | None, _context: HookContext
        ) -> HookJSONOutput:
            return await self._invoke(spec, dict(hook_input), tool_use_id)

        return callback

    async def _invoke(
        self, spec: HookCallbackSpec, payload: dict[str, Any], tool_use_id: str | None
    ) -> HookJSONOutput:
        tool_name = _text(payload.get("tool_name"))
        tool_use = tool_use_id or _text(payload.get("tool_use_id"))
        agent_id = _text(payload.get("agent_id"))
        effect_ref = tool_use or (agent_id if spec.mc_event is McHookEvent.SUBAGENT_START else None)
        body: dict[str, Any] = {
            "event": spec.mc_event.value,
            "hook_id": spec.hook_id,
            "native_event": spec.native_event,
            "tool_name": tool_name,
            "tool_use_id": tool_use,
            "agent_id": agent_id,
            "session_id": _text(payload.get("session_id")),
            "input": payload.get("tool_input"),
        }
        if spec.native_event == "PreCompact":
            body["trigger"] = payload.get("trigger")
            body["custom_instructions"] = payload.get("custom_instructions")
        self.invocations.append((spec.hook_id, spec.mc_event.value, effect_ref or ""))
        try:
            await self._emitter.emit(HOOK_INVOKED, body, tool_call_ref=tool_use)
            decision, reason, detail = await self._decide(spec, payload, tool_name, effect_ref)
        except Exception as error:  # fail closed: a broken kernel hook never allows
            _LOGGER.exception("kernel hook %s failed closed", spec.hook_id)
            decision, reason, detail = (
                "deny",
                f"kernel hook {spec.hook_id} failed closed",
                {"error": f"{type(error).__name__}: {error}"[:512]},
            )
        try:
            await self._emitter.emit(
                HOOK_RESULT,
                {**body, "decision": decision, "reason": reason, **detail},
                tool_call_ref=tool_use,
            )
        except Exception:
            _LOGGER.exception("kernel hook result frame of %s was not emitted", spec.hook_id)
            decision, reason = "deny", f"kernel hook {spec.hook_id} could not record its result"
        if decision == "deny":
            return _deny(spec.native_event, reason or "denied by Mission Control")
        return _allow()

    async def _decide(
        self,
        spec: HookCallbackSpec,
        payload: Mapping[str, Any],
        tool_name: str | None,
        effect_ref: str | None,
    ) -> tuple[str, str | None, dict[str, Any]]:
        if spec.hook_id not in FENCING_HOOKS or spec.mc_event not in GATED_EVENTS:
            return "allow", None, {}
        if effect_ref is None:
            return "deny", "permission event without a stable effect reference", {}
        kind = effect_kind_of(tool_name, spec.mc_event)
        context = self._context
        if spec.hook_id == "mc.operation_intent" and tool_name in context.disallowed_tools:
            return (
                "deny",
                f"{DISALLOWED_TOOL}: {tool_name} is disallowed by the binding",
                {"reason_code": DISALLOWED_TOOL},
            )
        if self._fences is None:
            return "deny", "no Stop Fence gate is composed for this lane", {}
        verdict = await self._fences.before_effect(
            EffectAdmission(
                request_scope=context.request_scope,
                run_id=context.run_id,
                generation=context.generation,
                effect_ref=effect_ref[:512],
                effect_kind=kind,
                lane_profile=context.lane_profile,
            )
        )
        if not verdict.allowed:
            return (
                "deny",
                "the run is stop-fenced",
                {
                    "reason_code": verdict.verdict.reason_code,
                    "fence_command_id": verdict.verdict.fence_command_id,
                    "fenced_frame": verdict.frame,
                },
            )
        if spec.hook_id == "mc.operation_intent":
            if self._intents is None:
                return "deny", "no Operation Intent ledger is composed for this lane", {}
            intent = await self._intents.record(
                HookEffectIntent(
                    request_scope=context.request_scope,
                    run_id=context.run_id,
                    generation=context.generation,
                    harness_execution_id=context.harness_execution_id,
                    effect_ref=effect_ref[:512],
                    effect_kind=kind,
                    hook_event=spec.mc_event.value,
                    lane_profile=context.lane_profile,
                    input_digest=sha256_digest(payload.get("tool_input")),
                    recorded_at=self._clock(),
                )
            )
            return "allow", None, {"intent_digest": intent.input_digest}
        return "allow", None, {}


__all__ = [
    "FENCING_HOOKS",
    "HOOK_INVOKED",
    "HOOK_RESULT",
    "SDK_HOOK_EVENTS",
    "FrameEmitter",
    "HookCallbackSpec",
    "KernelHookCallbacks",
    "KernelHookContext",
    "effect_kind_of",
]
