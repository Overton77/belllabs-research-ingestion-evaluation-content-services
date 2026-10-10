"""Claude Agent SDK messages to Provider Frames and closing facts (SPEC-01 "Claude local"; MP-07).

Every member of the pinned `claude_agent_sdk.types.Message` union is first turned into the
JSON payload the CLI wrote (`message_payload`: the dataclass fields plus the wire `type`
tag and, for content blocks, the block `type` the dataclasses drop), then handed to the
MP-13 mapping `application/frames/provider_mapping.claude_agent_sdk_observations`, which owns
the dedupe keys (`claude:<uuid>[:<block>]:<raw_kind>`), the raw-kind table
(`application/frames/kinds.CLAUDE_AGENT_SDK_KINDS`) and the subordinate identity
(`claude:task:<tool_use_id>`). This module adds what the lane owns: the session-log cursor,
the native turn reference, the terminal marker, the synthesized frame for a subprocess that
died mid-turn, and the `ClosingFacts` read from the terminal `result` frame.

Message types (claude_agent_sdk.types, 0.2.165): `UserMessage`, `AssistantMessage`,
`SystemMessage` and its subclasses `TaskStartedMessage`, `TaskProgressMessage`,
`TaskNotificationMessage`, `TaskUpdatedMessage`, `MirrorErrorMessage`, `HookEventMessage`,
`ResultMessage`, `StreamEvent`, `RateLimitEvent`, `ConversationResetMessage`. Content blocks:
`TextBlock`, `ThinkingBlock`, `ToolUseBlock`, `ToolResultBlock`, `ServerToolUseBlock`,
`ServerToolResultBlock` (`_internal.message_parser.parse_message` is the inverse).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any, Final, cast

from claude_agent_sdk.types import (
    AssistantMessage,
    ConversationResetMessage,
    HookEventMessage,
    Message,
    RateLimitEvent,
    ResultMessage,
    ServerToolResultBlock,
    ServerToolUseBlock,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from mission_control.application.frames.kinds import (
    UnknownKindCounter,
    classify,
    claude_agent_sdk_key,
)
from mission_control.application.frames.provider_mapping import claude_agent_sdk_observations
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.lane_turns import (
    MAX_EXCERPT_CHARS,
    ClosingFacts,
    NativeStatus,
)
from mission_control.domain.execution.lanes import LaneFrame, UsageReport
from mission_control.domain.frames.contracts import FrameKind, FrameObservation, LaneProfile

RESULT_RAW_KIND: Final = "result"
TURN_ENDED_RAW_KIND: Final = "result.turn_ended"
# `ResultMessage.terminal_reason` values that mean the turn was interrupted (types.ResultMessage).
ABORTED_TERMINAL_REASONS: Final = frozenset({"aborted_streaming", "aborted_tools"})
# HTTP statuses on `ResultMessage.api_error_status` that are capacity, not a provider fault.
CAPACITY_STATUSES: Final = frozenset({429, 529})
_BLOCK_TYPES: Final = {
    TextBlock: "text",
    ThinkingBlock: "thinking",
    ToolUseBlock: "tool_use",
    ToolResultBlock: "tool_result",
    ServerToolUseBlock: "server_tool_use",
    # The parser builds this block from the wire type `advisor_tool_result`.
    ServerToolResultBlock: "advisor_tool_result",
}


def _block_payload(block: Any) -> dict[str, Any]:
    payload = (
        dataclasses.asdict(cast(Any, block))
        if dataclasses.is_dataclass(block) and not isinstance(block, type)
        else dict(block)
    )
    payload["type"] = _BLOCK_TYPES.get(type(block), "unknown")
    return payload


def _content_payload(content: Any) -> Any:
    if isinstance(content, str):
        return content
    return [_block_payload(block) for block in content]


def message_payload(message: Message) -> dict[str, Any]:
    """The wire-shaped JSON payload of one SDK message (the dataclass plus its `type`)."""

    if isinstance(message, AssistantMessage):
        return {
            "type": "assistant",
            "uuid": message.uuid,
            "session_id": message.session_id,
            "message_id": message.message_id,
            "model": message.model,
            "parent_tool_use_id": message.parent_tool_use_id,
            "error": message.error,
            "stop_reason": message.stop_reason,
            "usage": message.usage,
            "content": _content_payload(message.content),
        }
    if isinstance(message, UserMessage):
        return {
            "type": "user",
            "uuid": message.uuid,
            "parent_tool_use_id": message.parent_tool_use_id,
            "tool_use_result": message.tool_use_result,
            "origin": message.origin,
            "content": _content_payload(message.content),
        }
    if isinstance(message, ResultMessage):
        payload = dataclasses.asdict(message)
        payload["type"] = "result"
        return payload
    if isinstance(message, HookEventMessage):
        return {
            "type": "system",
            "subtype": message.subtype,
            "hook_event_name": message.hook_event_name,
            "session_id": message.session_id,
            "uuid": message.uuid,
            "data": dict(message.data),
        }
    if isinstance(message, SystemMessage):
        payload = dataclasses.asdict(message)
        payload["type"] = "system"
        data = message.data if isinstance(message.data, Mapping) else {}
        if "session_id" not in payload and isinstance(data.get("session_id"), str):
            payload["session_id"] = data["session_id"]
        return payload
    if isinstance(message, StreamEvent):
        payload = dataclasses.asdict(message)
        payload["type"] = "stream_event"
        return payload
    if isinstance(message, RateLimitEvent):
        payload = dataclasses.asdict(message)
        payload["type"] = "rate_limit_event"
        return payload
    if isinstance(message, ConversationResetMessage):
        payload = dataclasses.asdict(message)
        payload["type"] = "conversation_reset"
        return payload
    raise TypeError(f"not a claude_agent_sdk message: {type(message).__name__}")


def session_id_of(message: Message) -> str | None:
    """The session identity a message carries (`system/init` names it first)."""

    if isinstance(message, SystemMessage) and not isinstance(message, HookEventMessage):
        value = getattr(message, "session_id", None) or message.data.get("session_id")
        return value if isinstance(value, str) and value else None
    value = getattr(message, "session_id", None)
    return value if isinstance(value, str) and value else None


def observations(
    message: Message, *, counter: UnknownKindCounter | None = None
) -> tuple[FrameObservation, ...]:
    """The MP-13 observations of one SDK message (keys, kinds, tool and subordinate refs)."""

    return claude_agent_sdk_observations(message_payload(message), counter=counter)


def lane_frame(
    observation: FrameObservation,
    *,
    harness_execution_id: str,
    generation: int,
    ordinal: int,
    turn_ref: str | None,
) -> LaneFrame:
    """One observation as the harness yields it; the cursor is the session-log ordinal."""

    body = observation.body if observation.body is not None else {}
    return LaneFrame(
        harness_execution_id=harness_execution_id,
        generation=generation,
        provider_key=observation.provider_key,
        cursor=str(ordinal),
        kind=observation.kind.value,
        raw_kind=observation.raw_kind,
        body=body,
        terminal=observation.raw_kind == RESULT_RAW_KIND,
        digest=sha256_digest(body),
        native_turn_ref=turn_ref,
        tool_call_ref=observation.tool_call_ref,
        subordinate_ref=observation.subordinate_ref,
    )


def synthesized_result(
    *,
    session_id: str | None,
    turn_ref: str,
    reason: str,
    detail: str,
) -> tuple[FrameObservation, ...]:
    """The terminal frames of a turn whose subprocess ended without a `result` message.

    The body says `synthesized_from: "process_exit"`: it is Mission Control's record that the
    turn cannot finish in this process, never a provider statement about what happened.
    """

    payload: dict[str, Any] = {
        "type": "result",
        "subtype": "error_during_execution",
        "is_error": True,
        "session_id": session_id or "",
        "uuid": f"mc-synth-{turn_ref}",
        "errors": [detail[:1_024]],
        "terminal_reason": reason,
        "synthesized_from": "process_exit",
    }
    ref = str(payload["uuid"])
    return tuple(
        FrameObservation(
            provider_key=claude_agent_sdk_key(ref, raw_kind),
            raw_kind=raw_kind,
            kind=kind,
            body=payload,
            native_session_ref=session_id,
        )
        for raw_kind, kind in (
            (TURN_ENDED_RAW_KIND, _kind(TURN_ENDED_RAW_KIND)),
            (RESULT_RAW_KIND, _kind(RESULT_RAW_KIND)),
        )
    )


def _kind(raw_kind: str) -> FrameKind:
    return classify(LaneProfile.CLAUDE_AGENT_SDK, raw_kind, None, counter=None).kind


def _int(value: Any) -> int:
    return int(value) if isinstance(value, int | float) and value >= 0 else 0


def usage_report(usage: Mapping[str, Any] | None) -> UsageReport:
    """`ResultMessage.usage` -> settled tokens of the turn (main loop only; cost stays out:
    `total_cost_usd` is a client-side estimate, reported as `cost_disposition=estimated`)."""

    if not isinstance(usage, Mapping):
        return UsageReport(disposition="unknown")
    inputs = _int(usage.get("input_tokens"))
    outputs = _int(usage.get("output_tokens"))
    cached = _int(usage.get("cache_read_input_tokens")) + _int(
        usage.get("cache_creation_input_tokens")
    )
    if not (inputs or outputs or cached):
        return UsageReport(disposition="unknown")
    return UsageReport(
        disposition="settled",
        input_tokens=inputs + cached,
        output_tokens=outputs,
        total_tokens=inputs + cached + outputs,
    )


def native_status(body: Mapping[str, Any]) -> tuple[NativeStatus, str | None]:
    """Status and error code of a `result` body, never read from the result text."""

    reason = body.get("terminal_reason")
    if isinstance(reason, str) and reason in ABORTED_TERMINAL_REASONS:
        return "cancelled", "cancelled_by_command"
    subtype = str(body.get("subtype") or "")
    if body.get("is_error") or subtype.startswith("error"):
        status = body.get("api_error_status")
        if isinstance(status, int) and status in CAPACITY_STATUSES:
            return "error", "capacity"
        if subtype == "error_max_turns":
            return "error", "max_turns"
        if subtype == "error_max_budget_usd":
            return "error", "budget"
        if body.get("synthesized_from") == "process_exit":
            return "error", "process_exit"
        return "error", (subtype or "provider_error")[:128]
    return "finished", None


def closing_facts(body: Mapping[str, Any], *, model: str | None = None) -> ClosingFacts:
    """The terminal `result` frame to the closing facts the reducer and settlement read."""

    status, code = native_status(body)
    result = body.get("result")
    excerpt = result if isinstance(result, str) else ""
    errors = body.get("errors")
    message = None
    if status == "error":
        if isinstance(errors, list) and errors:
            message = "; ".join(str(item) for item in errors)[:1_024]
        elif excerpt:
            message = excerpt[:1_024]
    usage = usage_report(body.get("usage"))
    duration = body.get("duration_ms")
    return ClosingFacts(
        native_status=status,
        result_excerpt=excerpt[:MAX_EXCERPT_CHARS],
        usage=usage,
        cost_disposition="estimated" if usage.disposition != "unknown" else "unknown",
        error_code=code,
        error_message=message,
        duration_ms=int(duration) if isinstance(duration, int) and duration >= 0 else None,
        model=model,
    )


__all__ = [
    "ABORTED_TERMINAL_REASONS",
    "CAPACITY_STATUSES",
    "RESULT_RAW_KIND",
    "TURN_ENDED_RAW_KIND",
    "closing_facts",
    "lane_frame",
    "message_payload",
    "native_status",
    "observations",
    "session_id_of",
    "synthesized_result",
    "usage_report",
]
