"""Provider events -> `FrameObservation`s for the Claude Agent SDK and Codex lanes.

Pure functions over JSON-compatible provider payloads (the SDK dataclass or JSON-RPC params
already converted with `jsonable`), so recorded fixtures exercise exactly what a lane
writer persists: raw kind, classified kind, dedupe key, native refs, tool call ref and
subordinate ref. Lane writers (MP-07, MP-08) call these and hand the result to
`FrameWriter`; nothing here talks to a provider.

Subordinate refs are stable native identifiers, never inferred from message text:

- Claude: `claude:task:<tool_use_id>`, from `parent_tool_use_id` on messages a subagent
  produced and `tool_use_id` on `Task*Message` lifecycle messages. A task message without
  a `tool_use_id` uses `claude:task_id:<task_id>` and stays unresolved against its spawn.
- Codex: `codex:thread:<threadId>` for any event of a thread other than the lane's root
  thread; the root thread remains the frame's `native_session_ref`.

Event shapes: claude_agent_sdk.types (ctx7 /anthropics/claude-agent-sdk-python); codex-rs
app-server-protocol v2 and `codex exec --json` (ctx7 /openai/codex and learn.chatgpt.com).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from mission_control.application.frames.kinds import (
    UNKNOWN_KINDS,
    UnknownKindCounter,
    bounded_key,
    classify,
    claude_agent_sdk_key,
    codex_key,
    codex_usage_key,
)
from mission_control.domain.frames.contracts import FrameObservation, LaneProfile

CLAUDE_TASK_PREFIX = "claude:task:"
CLAUDE_TASK_ID_PREFIX = "claude:task_id:"
CODEX_THREAD_PREFIX = "codex:thread:"


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _digest(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]


# --- Claude Agent SDK -----------------------------------------------------------------------


def claude_subordinate_ref(message: Mapping[str, Any]) -> str | None:
    parent = _text(message.get("parent_tool_use_id"))
    if parent:
        return CLAUDE_TASK_PREFIX + parent
    if message.get("type") == "system" and str(message.get("subtype", "")).startswith("task_"):
        tool_use = _text(message.get("tool_use_id"))
        if tool_use:
            return CLAUDE_TASK_PREFIX + tool_use
        task = _text(message.get("task_id"))
        if task:
            return CLAUDE_TASK_ID_PREFIX + task
    return None


def _claude_raw_kind(message: Mapping[str, Any]) -> str:
    kind = _text(message.get("type")) or "unknown"
    subtype = _text(message.get("subtype"))
    return f"system.{subtype}" if kind == "system" and subtype else kind


def _claude_ref(message: Mapping[str, Any]) -> str:
    uuid = _text(message.get("uuid"))
    if uuid:
        return uuid
    session = _text(message.get("session_id")) or ""
    message_id = _text(message.get("message_id")) or _digest(message)
    return f"{session}:{message_id}"


def claude_agent_sdk_observations(
    message: Mapping[str, Any],
    *,
    counter: UnknownKindCounter | None = UNKNOWN_KINDS,
) -> tuple[FrameObservation, ...]:
    """One SDK `Message` -> its frames (content blocks and result split as documented)."""

    lane = LaneProfile.CLAUDE_AGENT_SDK
    raw = _claude_raw_kind(message)
    ref = _claude_ref(message)
    subordinate = claude_subordinate_ref(message)
    session = _text(message.get("session_id"))
    if raw == "system.init" and session is None:
        data = message.get("data")
        session = _text(data.get("session_id")) if isinstance(data, Mapping) else None

    def observe(
        raw_kind: str,
        body: Any,
        *,
        block: int | None = None,
        tool_call_ref: str | None = None,
    ) -> FrameObservation:
        return FrameObservation(
            provider_key=claude_agent_sdk_key(ref, raw_kind, block),
            raw_kind=raw_kind,
            kind=classify(lane, raw_kind, body, counter=counter).kind,
            body=body,
            native_session_ref=session,
            subordinate_ref=subordinate,
            tool_call_ref=tool_call_ref,
        )

    content = message.get("content")
    blocks = (
        [item for item in content if isinstance(item, Mapping)] if isinstance(content, list) else []
    )
    if raw == "assistant":
        observations = [observe(raw, message)]
        for index, block in enumerate(blocks):
            if block.get("type") == "tool_use":
                observations.append(
                    observe(
                        "assistant.tool_use",
                        dict(block),
                        block=index,
                        tool_call_ref=_text(block.get("id")),
                    )
                )
        return tuple(observations)
    if raw == "user":
        results = [
            observe(
                "user.tool_result",
                dict(block),
                block=index,
                tool_call_ref=_text(block.get("tool_use_id")),
            )
            for index, block in enumerate(blocks)
            if block.get("type") == "tool_result"
        ]
        return tuple(results) if results else (observe(raw, message),)
    if raw == "result":
        return (observe("result.turn_ended", message), observe("result", message))
    return (observe(raw, message),)


# --- Codex app-server / exec ----------------------------------------------------------------


def _codex_thread(params: Mapping[str, Any]) -> str | None:
    thread = params.get("thread")
    return _text(params.get("threadId")) or (
        _text(thread.get("id")) if isinstance(thread, Mapping) else None
    )


def _codex_turn(params: Mapping[str, Any]) -> str | None:
    turn = params.get("turn")
    return _text(params.get("turnId")) or (
        _text(turn.get("id")) if isinstance(turn, Mapping) else None
    )


def _codex_item(params: Mapping[str, Any]) -> Mapping[str, Any]:
    item = params.get("item")
    return item if isinstance(item, Mapping) else {}


def codex_subordinate_ref(thread_id: str | None, root_thread_id: str) -> str | None:
    return CODEX_THREAD_PREFIX + thread_id if thread_id and thread_id != root_thread_id else None


_CODEX_TOOL_TYPES = frozenset(
    {
        "commandExecution",
        "fileChange",
        "mcpToolCall",
        "webSearch",
        "command_execution",
        "file_change",
        "mcp_tool_call",
        "web_search",
    }
)


def codex_observations(
    method: str,
    params: Mapping[str, Any],
    *,
    root_thread_id: str,
    counter: UnknownKindCounter | None = UNKNOWN_KINDS,
) -> tuple[FrameObservation, ...]:
    """One app-server notification or server request (method + params) -> its frames."""

    lane = LaneProfile.CODEX
    thread = _codex_thread(params) or root_thread_id
    turn = _codex_turn(params)
    item = _codex_item(params)
    item_id = _text(item.get("id")) or _text(params.get("itemId")) or _text(params.get("requestId"))
    subordinate = codex_subordinate_ref(thread, root_thread_id)
    tool_ref = item_id if item.get("type") in _CODEX_TOOL_TYPES or "Approval" in method else None

    def observe(raw_kind: str, key: str) -> FrameObservation:
        return FrameObservation(
            provider_key=key,
            raw_kind=raw_kind,
            kind=classify(lane, raw_kind, params, counter=counter).kind,
            body=dict(params),
            native_session_ref=root_thread_id,
            native_turn_ref=turn,
            subordinate_ref=subordinate,
            tool_call_ref=tool_ref,
        )

    if method == "thread/tokenUsage/updated":
        usage = params.get("tokenUsage")
        total = usage.get("total") if isinstance(usage, Mapping) else None
        marker = total.get("totalTokens") if isinstance(total, Mapping) else None
        return (observe(method, codex_usage_key(thread, turn, marker or _digest(params))),)
    key = codex_key(thread, turn, item_id, method)
    if method == "turn/completed":
        result = f"{method}.result"
        return (observe(method, key), observe(result, codex_key(thread, turn, None, result)))
    return (observe(method, key),)


def codex_exec_observations(
    event: Mapping[str, Any],
    *,
    thread_id: str,
    turn_index: int,
    counter: UnknownKindCounter | None = UNKNOWN_KINDS,
) -> tuple[FrameObservation, ...]:
    """One `codex exec --json` line -> its frames. Exec turns carry no id: the caller counts."""

    lane = LaneProfile.CODEX
    raw = _text(event.get("type")) or "unknown"
    item = _codex_item(event)
    item_id = _text(item.get("id"))
    turn = f"exec-turn-{turn_index}"
    session = _text(event.get("thread_id")) or thread_id
    tool_ref = item_id if item.get("type") in _CODEX_TOOL_TYPES else None

    def observe(raw_kind: str, key: str) -> FrameObservation:
        return FrameObservation(
            provider_key=key,
            raw_kind=raw_kind,
            kind=classify(lane, raw_kind, event, counter=counter).kind,
            body=dict(event),
            native_session_ref=session,
            native_turn_ref=turn,
            tool_call_ref=tool_ref,
        )

    if raw == "item.updated":
        return (observe(raw, bounded_key("codex", session, turn, item_id, raw, _digest(event))),)
    key = codex_key(session, turn, item_id, raw)
    if raw in {"turn.completed", "turn.failed"}:
        result = f"{raw}.result"
        return (observe(raw, key), observe(result, codex_key(session, turn, None, result)))
    return (observe(raw, key),)


__all__ = [
    "CLAUDE_TASK_ID_PREFIX",
    "CLAUDE_TASK_PREFIX",
    "CODEX_THREAD_PREFIX",
    "claude_agent_sdk_observations",
    "claude_subordinate_ref",
    "codex_exec_observations",
    "codex_observations",
    "codex_subordinate_ref",
]
