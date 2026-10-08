"""Kernel Hook callbacks for command-hook lanes (SPEC-07 sections 5.4 and 5.5; FT-G3).

File-based lanes (Cursor) run Kernel Hooks as commands (`.mission/hooks/kernel.py <event>
<kernel_hook_id>`, fail-closed on permission events). Each invocation calls the worker back
on its loopback listener with a task token minted at `prepare` and bound to (scope, run,
attempt, generation, harness execution). The service:

1. verifies the token (present, known in the claimed scope, unexpired, unrevoked, of the
   execution's current generation, naming the same execution);
2. maps the provider payload to `mc.hook_input.v1` (a lane-specific `NativeHookMapper`);
3. for permission events consults the Stop Fence (`KernelHookFenceGate.before_effect`) and
   denies a fenced effect; `mc.operation_intent` refuses a tool the binding disallows and
   writes the Operation Intent keyed on the effect ref *before* answering `allow`;
4. persists the invocation as Provider Frames (`hook.invoked`, `hook.result`) of the same
   harness execution, then returns `mc.hook_result.v1`.

The token value is returned once (to be written to the lease with mode 0600) and only its
digest is stored; it never appears in a frame, a transcript or a log line.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.application.frames.kinds import classify, hook_key
from mission_control.application.frames.sink import FrameStore
from mission_control.application.frames.writer import FrameWriter
from mission_control.contracts.hooks import HookInput, HookResult, HookScope
from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.capabilities.hooks import HookDecision, HookEvent
from mission_control.domain.frames.contracts import (
    FrameObservation,
    HarnessExecutionStart,
    LaneProfile,
)
from mission_control.domain.policies.stop_fence import EffectAdmission, EffectKind

KERNEL_HOOK_CALL_SCHEMA: Final = "mc.kernel_hook_call.v1"
GATED_EVENTS: Final = frozenset(
    {
        HookEvent.BEFORE_TOOL,
        HookEvent.BEFORE_SHELL,
        HookEvent.BEFORE_MCP,
        HookEvent.SUBAGENT_START,
    }
)
MAX_CONTEXT_CHARS: Final = 10_000
DISALLOWED_TOOL: Final = "DISALLOWED_TOOL"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class KernelHookCall(_Strict):
    """`mc.kernel_hook_call.v1`: what `.mission/hooks/kernel.py` posts (never the token)."""

    schema_version: Literal["mc.kernel_hook_call.v1"] = KERNEL_HOOK_CALL_SCHEMA
    kernel_hook_id: str = Field(pattern=r"^mc\.[a-z_]+$")
    event: HookEvent
    scope: HookScope
    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class HookTokenContext(_Strict):
    """What a task token is bound to; secret-free."""

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    generation: int = Field(ge=1)
    harness_execution_id: UUID
    lane_profile: str = Field(min_length=1)
    execution_start: HarnessExecutionStart | None = None
    disallowed_tools: tuple[str, ...] = ()
    workspace_root: str | None = None


class HookTaskToken(_Strict):
    token_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    context: HookTokenContext
    issued_at: AwareDatetime
    expires_at: AwareDatetime
    revoked_at: AwareDatetime | None = None


class HookEffectIntent(_Strict):
    """The Operation Intent a permission Kernel Hook writes before it answers `allow`."""

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    harness_execution_id: UUID
    effect_ref: str = Field(min_length=1, max_length=512)
    effect_kind: EffectKind
    hook_event: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    input_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    recorded_at: AwareDatetime


class HookTokenStore(Protocol):
    async def put(self, token: HookTaskToken) -> None: ...

    async def get(self, request_scope: str, token_hash: str) -> HookTaskToken | None: ...

    async def latest_generation(
        self, request_scope: str, harness_execution_id: UUID
    ) -> int | None: ...

    async def revoke(
        self, request_scope: str, harness_execution_id: UUID, generation: int, at: datetime
    ) -> None: ...


class HookIntentLedger(Protocol):
    async def record(self, intent: HookEffectIntent) -> HookEffectIntent:
        """Insert once per (run, generation, effect ref); a repeat returns the stored one."""
        ...


class NativeHookMapper(Protocol):
    """Provider payload to `mc.hook_input.v1` and the effect a permission event gates."""

    def hook_input(
        self, event: HookEvent, payload: Mapping[str, Any], context: HookTokenContext
    ) -> HookInput: ...

    def effect(
        self, event: HookEvent, payload: Mapping[str, Any]
    ) -> tuple[str, EffectKind] | None: ...


class HookCallbackRejected(Exception):
    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


def token_digest(token: str) -> str:
    return "sha256:" + hashlib.sha256(token.encode("utf-8")).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(UTC)


class InMemoryHookTokenStore:
    def __init__(self) -> None:
        self._tokens: dict[tuple[str, str], HookTaskToken] = {}

    async def put(self, token: HookTaskToken) -> None:
        self._tokens.setdefault((token.context.request_scope, token.token_hash), token)

    async def get(self, request_scope: str, token_hash: str) -> HookTaskToken | None:
        return self._tokens.get((request_scope, token_hash))

    async def latest_generation(self, request_scope: str, harness_execution_id: UUID) -> int | None:
        generations = [
            token.context.generation
            for (scope, _hash), token in self._tokens.items()
            if scope == request_scope and token.context.harness_execution_id == harness_execution_id
        ]
        return max(generations) if generations else None

    async def revoke(
        self, request_scope: str, harness_execution_id: UUID, generation: int, at: datetime
    ) -> None:
        for key, token in list(self._tokens.items()):
            context = token.context
            if (
                key[0] == request_scope
                and context.harness_execution_id == harness_execution_id
                and context.generation == generation
                and token.revoked_at is None
            ):
                self._tokens[key] = token.model_copy(update={"revoked_at": at})


class InMemoryHookIntentLedger:
    def __init__(self) -> None:
        self._intents: dict[tuple[str, str, int, str], HookEffectIntent] = {}
        self._lock = asyncio.Lock()

    async def record(self, intent: HookEffectIntent) -> HookEffectIntent:
        async with self._lock:
            key = (intent.request_scope, intent.run_id, intent.generation, intent.effect_ref)
            return self._intents.setdefault(key, intent)

    def intents(self) -> tuple[HookEffectIntent, ...]:
        return tuple(self._intents.values())


def _tool_disallowed(name: str | None, kind: EffectKind, disallowed: tuple[str, ...]) -> bool:
    if not disallowed:
        return False
    lowered = {item.lower() for item in disallowed}
    if name is not None and name.lower() in lowered:
        return True
    # Cursor tool groups: `shell` and `mcp` disallow every tool of that class.
    return kind in {"shell", "mcp"} and kind in lowered


class HookCallbackService:
    def __init__(
        self,
        *,
        tokens: HookTokenStore,
        intents: HookIntentLedger,
        mapper: NativeHookMapper,
        fences: KernelHookFenceGate | None = None,
        frames: FrameStore | None = None,
        context_reader: Callable[[HookTokenContext], str | None] | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._tokens = tokens
        self._intents = intents
        self._mapper = mapper
        self._fences = fences
        self._frames = frames
        self._context_reader = context_reader
        self._clock = clock

    # --- tokens ------------------------------------------------------------------------------

    async def issue(self, context: HookTokenContext, *, ttl: timedelta) -> str:
        """Mint a task token for `context`; the value is returned once, its digest stored."""

        token = secrets.token_urlsafe(32)
        now = self._clock()
        await self._tokens.put(
            HookTaskToken(
                token_hash=token_digest(token),
                context=context,
                issued_at=now,
                expires_at=now + ttl,
            )
        )
        return token

    async def revoke(self, context: HookTokenContext) -> None:
        await self._tokens.revoke(
            context.request_scope, context.harness_execution_id, context.generation, self._clock()
        )

    async def _verify(self, call: KernelHookCall, bearer: str | None) -> HookTokenContext:
        if not bearer:
            raise HookCallbackRejected(401, "missing_task_token")
        scope = call.scope
        request_scope = f"mc/{scope.installation_id}/{scope.application_id}/{scope.tenant_id}"
        stored = await self._tokens.get(request_scope, token_digest(bearer))
        if stored is None or not hmac.compare_digest(stored.token_hash, token_digest(bearer)):
            raise HookCallbackRejected(401, "unknown_task_token")
        if stored.revoked_at is not None or stored.expires_at <= self._clock():
            raise HookCallbackRejected(401, "expired_task_token")
        context = stored.context
        if (
            str(context.harness_execution_id) != call.harness_execution_id
            or context.generation != call.generation
        ):
            raise HookCallbackRejected(403, "task_token_scope_mismatch")
        latest = await self._tokens.latest_generation(
            context.request_scope, context.harness_execution_id
        )
        if latest is not None and latest > context.generation:
            raise HookCallbackRejected(409, "STALE_GENERATION")
        return context

    # --- callback ----------------------------------------------------------------------------

    async def handle(
        self, call: KernelHookCall, bearer: str | None, *, application_id: str | None = None
    ) -> HookResult:
        if application_id is not None and application_id != call.scope.application_id:
            raise HookCallbackRejected(403, "application_mismatch")
        context = await self._verify(call, bearer)
        hook_input = self._mapper.hook_input(call.event, call.payload, context)
        effect = self._mapper.effect(call.event, call.payload)
        result, frame_extra = await self._decide(call, context, hook_input, effect)
        await self._persist(call, context, hook_input, effect, result, frame_extra)
        return result

    async def _decide(
        self,
        call: KernelHookCall,
        context: HookTokenContext,
        hook_input: HookInput,
        effect: tuple[str, EffectKind] | None,
    ) -> tuple[HookResult, dict[str, Any]]:
        hook = call.kernel_hook_id
        gated = call.event in GATED_EVENTS
        if hook == "mc.stop_fence" or (hook == "mc.operation_intent" and gated):
            if not gated:
                return HookResult.allow(), {}
            if effect is None:
                return HookResult.deny("the side effect cannot be identified"), {}
            effect_ref, effect_kind = effect
            if self._fences is not None:
                decision = await self._fences.before_effect(
                    EffectAdmission(
                        request_scope=context.request_scope,
                        run_id=context.run_id,
                        generation=context.generation,
                        effect_ref=effect_ref,
                        effect_kind=effect_kind,
                        lane_profile=context.lane_profile,
                    )
                )
                if not decision.allowed:
                    return (
                        HookResult(
                            decision=HookDecision.DENY,
                            reason=decision.verdict.message or "run is stop-fenced",
                        ),
                        dict(decision.frame or {}),
                    )
            if hook == "mc.stop_fence":
                return HookResult.allow(), {}
            tool = hook_input.tool
            if _tool_disallowed(tool.name if tool else None, effect_kind, context.disallowed_tools):
                return (
                    HookResult.deny(f"tool is not allowed by the binding ({effect_kind})"),
                    {"reason_code": DISALLOWED_TOOL},
                )
            await self._intents.record(
                HookEffectIntent(
                    request_scope=context.request_scope,
                    run_id=context.run_id,
                    generation=context.generation,
                    harness_execution_id=context.harness_execution_id,
                    effect_ref=effect_ref,
                    effect_kind=effect_kind,
                    hook_event=call.event.value,
                    lane_profile=context.lane_profile,
                    input_digest=stable_json_digest(hook_input),
                    recorded_at=self._clock(),
                )
            )
            return HookResult.allow(), {"intent": effect_ref}
        if hook == "mc.operation_intent":
            return HookResult.allow(), {}
        if hook == "mc.frame_capture":
            if call.event == HookEvent.SESSION_START and self._context_reader is not None:
                index = self._context_reader(context)
                if index:
                    return HookResult(additional_context=index[:MAX_CONTEXT_CHARS]), {}
            return HookResult.allow(), {}
        if hook == "mc.usage":
            return HookResult.allow(), {}
        return HookResult.deny(f"unknown kernel hook {hook}"), {}

    async def _persist(
        self,
        call: KernelHookCall,
        context: HookTokenContext,
        hook_input: HookInput,
        effect: tuple[str, EffectKind] | None,
        result: HookResult,
        extra: Mapping[str, Any],
    ) -> None:
        if self._frames is None or context.execution_start is None:
            return
        handle = await self._frames.open_execution(context.execution_start)
        writer = FrameWriter(self._frames, handle)
        lane = LaneProfile(context.lane_profile)
        tool = hook_input.tool
        ref = (
            effect[0]
            if effect is not None
            else (tool.call_id if tool and tool.call_id else hook_input.provider_payload_digest)
        )
        invocation = f"{call.kernel_hook_id}|{ref or 'none'}"
        invoked_body = {
            "kernel_hook_id": call.kernel_hook_id,
            "hook_input": hook_input.model_dump(mode="json", exclude_none=True),
        }
        result_body = {
            "kernel_hook_id": call.kernel_hook_id,
            "kind": "hook",
            "event": call.event.value,
            "decision": result.decision.value,
            "reason": result.reason,
            "effect_ref": effect[0] if effect else None,
            "tool_call_id": tool.call_id if tool else None,
            **dict(extra),
        }
        observations = [
            FrameObservation(
                provider_key=hook_key(call.event.value, invocation, 0, phase),
                raw_kind=raw_kind,
                kind=classify(lane, raw_kind, body).kind,
                body=body,
                # The execution's own session identity (the agent); the provider's
                # conversation and generation ids stay in the hook input body.
                tool_call_ref=tool.call_id if tool else None,
            )
            for phase, raw_kind, body in (
                ("invoked", "hook.invoked", invoked_body),
                ("result", "hook.result", result_body),
            )
        ]
        await writer.write(observations)


__all__ = [
    "DISALLOWED_TOOL",
    "GATED_EVENTS",
    "KERNEL_HOOK_CALL_SCHEMA",
    "HookCallbackRejected",
    "HookCallbackService",
    "HookEffectIntent",
    "HookIntentLedger",
    "HookTaskToken",
    "HookTokenContext",
    "HookTokenStore",
    "InMemoryHookIntentLedger",
    "InMemoryHookTokenStore",
    "KernelHookCall",
    "NativeHookMapper",
    "token_digest",
]
