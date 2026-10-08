"""Hook Script stdin/stdout contracts: ``mc.hook_input.v1`` and ``mc.hook_result.v1``.

A hook script is written once: it reads ``mc.hook_input.v1`` JSON on stdin and writes
``mc.hook_result.v1`` JSON on stdout; exit code 2 is a deny (ADR-0026, SPEC-01). Stdin never
carries secret values; a script that needs the service uses the ``callback`` block and a
task-scoped token file. Results from several hooks merge with ``deny`` beating ``defer``
beating ``allow``, ``additional_context`` concatenated and the last ``updated_input`` winning.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any, Final, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.capabilities.hooks import HookDecision, HookEvent
from mission_control.domain.capabilities.host_support import LaneProfile

HOOK_INPUT_SCHEMA: Final = "mc.hook_input.v1"
HOOK_RESULT_SCHEMA: Final = "mc.hook_result.v1"
HOOK_FRAME_SCHEMA: Final = "mc.hook_frame.v1"
ADDITIONAL_CONTEXT_LIMIT = 10_000
DENY_EXIT_CODE = 2


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HookScope(_Strict):
    installation_id: str = Field(min_length=1)
    application_id: str = Field(min_length=1)
    tenant_id: str | None = None


class HookToolCall(_Strict):
    name: str = Field(min_length=1)
    call_id: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    side_effect_class: str | None = None
    # Present on after_tool / after_shell / after_tool_failure only.
    output_excerpt: str | None = Field(default=None, max_length=4000)
    error: str | None = Field(default=None, max_length=4000)


class HookCallbackEndpoint(_Strict):
    url: str = Field(min_length=1)
    token_path: str = Field(min_length=1)


class HookInput(_Strict):
    schema_version: Literal["mc.hook_input.v1"] = HOOK_INPUT_SCHEMA
    event: HookEvent
    lane_profile: LaneProfile
    scope: HookScope
    run_id: str | None = None
    activation_id: str | None = None
    attempt_no: int | None = Field(default=None, ge=1)
    generation: int | None = Field(default=None, ge=1)
    harness_execution_id: str | None = None
    native_session_ref: str | None = None
    native_turn_ref: str | None = None
    tool: HookToolCall | None = None
    workspace_root: str | None = None
    provider_payload_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    callback: HookCallbackEndpoint | None = None
    # Event-specific extras that carry no secrets (e.g. compaction cutoff, prompt digest).
    details: dict[str, Any] = Field(default_factory=dict)

    def to_stdin(self) -> bytes:
        return (
            json.dumps(self.model_dump(mode="json", exclude_none=True), sort_keys=True) + "\n"
        ).encode("utf-8")


class HookResult(_Strict):
    schema_version: Literal["mc.hook_result.v1"] = HOOK_RESULT_SCHEMA
    decision: HookDecision = HookDecision.ALLOW
    reason: str | None = Field(default=None, min_length=1)
    updated_input: dict[str, Any] | None = None
    additional_context: str | None = Field(default=None, max_length=ADDITIONAL_CONTEXT_LIMIT)
    message: str | None = None

    @model_validator(mode="after")
    def reason_required_to_refuse(self) -> HookResult:
        if self.decision in {HookDecision.DENY, HookDecision.DEFER} and not self.reason:
            raise ValueError("reason is required when the decision is deny or defer")
        return self

    @classmethod
    def allow(cls) -> HookResult:
        return cls()

    @classmethod
    def deny(cls, reason: str) -> HookResult:
        return cls(decision=HookDecision.DENY, reason=reason)


_RANK = {HookDecision.ALLOW: 0, HookDecision.DEFER: 1, HookDecision.DENY: 2}


def merge_results(results: Iterable[HookResult]) -> HookResult:
    """``deny`` beats ``defer`` beats ``allow``; contexts concatenate; last update wins."""
    decision = HookDecision.ALLOW
    reasons: list[str] = []
    contexts: list[str] = []
    messages: list[str] = []
    updated: dict[str, Any] | None = None
    for result in results:
        if _RANK[result.decision] > _RANK[decision]:
            decision = result.decision
        if result.decision is not HookDecision.ALLOW and result.reason:
            reasons.append(result.reason)
        if result.additional_context:
            contexts.append(result.additional_context)
        if result.message:
            messages.append(result.message)
        if result.updated_input is not None:
            updated = result.updated_input
    refusing = reasons if decision is not HookDecision.ALLOW else []
    context = "\n\n".join(contexts)[:ADDITIONAL_CONTEXT_LIMIT] or None
    return HookResult(
        decision=decision,
        reason="; ".join(refusing) or None,
        updated_input=updated,
        additional_context=context,
        message="\n".join(messages) or None,
    )


def parse_script_output(stdout: bytes, exit_code: int, *, hook_id: str) -> HookResult:
    """Interpret one script run: exit 2 denies; otherwise stdout must be a hook result.

    Empty stdout with exit 0 is an allow. Any other exit code or unparsable stdout is a
    failure the caller resolves with the hook's ``fail_closed`` flag.
    """
    text = stdout.decode("utf-8", "replace").strip()
    if exit_code == DENY_EXIT_CODE:
        reason = f"hook {hook_id} denied (exit 2)"
        if text:
            try:
                parsed = HookResult.model_validate_json(text)
            except ValueError:
                return HookResult.deny(reason)
            return parsed.model_copy(
                update={"decision": HookDecision.DENY, "reason": parsed.reason or reason}
            )
        return HookResult.deny(reason)
    if exit_code != 0:
        raise HookScriptFailure(f"hook {hook_id} exited with {exit_code}")
    if not text:
        return HookResult.allow()
    try:
        return HookResult.model_validate_json(text)
    except ValueError as error:
        raise HookScriptFailure(f"hook {hook_id} wrote an invalid mc.hook_result.v1") from error


class HookScriptFailure(RuntimeError):
    """A hook script crashed, timed out or wrote an invalid result."""


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class HookFrame(_Strict):
    """One hook invocation handed to the Provider Frame sink (persistence is SPEC-03 C1)."""

    schema_version: Literal["mc.hook_frame.v1"] = HOOK_FRAME_SCHEMA
    hook_id: str
    kernel: bool
    event: HookEvent
    lane_profile: LaneProfile
    decision: HookDecision
    reason: str | None = None
    input_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    result_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    tool_call_id: str | None = None
    duration_ms: int = Field(ge=0)
    failed: bool = False


class HookFrameSink(Protocol):
    """Port: receives every hook invocation as a Provider Frame (C1 persists them)."""

    async def record_hook_frame(self, frame: HookFrame) -> None: ...


def hook_contract_schemas() -> dict[str, dict[str, Any]]:
    """JSON Schemas for the published hook contracts."""
    return {
        HOOK_INPUT_SCHEMA: HookInput.model_json_schema(),
        HOOK_RESULT_SCHEMA: HookResult.model_json_schema(),
        HOOK_FRAME_SCHEMA: HookFrame.model_json_schema(),
    }
