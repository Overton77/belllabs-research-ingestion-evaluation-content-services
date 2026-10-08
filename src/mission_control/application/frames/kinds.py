"""Per-lane raw-kind tables and dedupe keys (SPEC-03 "FrameSink port and writers").

Every writer maps its provider's `raw_kind` to a provider-neutral `FrameKind` here, so a
new lane is new rows rather than new branches. Raw kinds whose kind depends on the
payload (a Cursor `tool_call` is started, completed or failed by its `status`) name the
body field that decides. Unmapped kinds classify as `unknown` (never closing) and are
counted so a new provider event surfaces instead of disappearing.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from mission_control.domain.frames.contracts import (
    MAX_PROVIDER_KEY_LENGTH,
    FrameKind,
    LaneProfile,
)

logger = logging.getLogger(__name__)

K = FrameKind


@dataclass(frozen=True)
class ByField:
    """A raw kind whose frame kind is decided by one body field's value."""

    field: str
    values: Mapping[str, FrameKind | ByField]
    default: FrameKind = FrameKind.UNKNOWN


KindRule = FrameKind | ByField

_TOOL_STATUS = ByField(
    "status",
    {
        "running": K.TOOL_CALL_STARTED,
        "started": K.TOOL_CALL_STARTED,
        "pending": K.TOOL_CALL_STARTED,
        "completed": K.TOOL_CALL_COMPLETED,
        "success": K.TOOL_CALL_COMPLETED,
        "ok": K.TOOL_CALL_COMPLETED,
        "error": K.TOOL_CALL_FAILED,
        "failed": K.TOOL_CALL_FAILED,
    },
)
_TOOL_RESULT_STATUS = ByField(
    "status",
    {
        "success": K.TOOL_CALL_COMPLETED,
        "completed": K.TOOL_CALL_COMPLETED,
        "error": K.TOOL_CALL_FAILED,
        "failed": K.TOOL_CALL_FAILED,
    },
    default=K.TOOL_CALL_COMPLETED,
)

# Hook invocations are frames on every lane (SPEC-01 hook scripts, kernel hooks).
HOOK_KINDS: dict[str, KindRule] = {
    "hook.invoked": K.HOOK_INVOKED,
    "hook.result": K.HOOK_RESULT,
}

# Deep Agents: raw kinds the Deep Agents frame writer emits from `astream(...,
# stream_mode=["updates", "messages", "custom"], subgraphs=True, version="v2")`.
DEEP_AGENTS_KINDS: dict[str, KindRule] = {
    "graph.session_init": K.SESSION_INIT,
    "graph.invocation_started": K.TURN_STARTED,
    "messages.ai_chunk": K.MESSAGE_DELTA,
    "messages.reasoning_chunk": K.THINKING_DELTA,
    "updates.ai_message": K.MESSAGE,
    "updates.ai_tool_call": K.TOOL_CALL_STARTED,
    "updates.tool_message": _TOOL_RESULT_STATUS,
    "checkpoint_backfill": _TOOL_RESULT_STATUS,
    "updates.interrupt": K.APPROVAL_REQUESTED,
    "updates.summarization_event": K.AFTER_COMPACTION,
    "custom.mc.before_compaction": K.BEFORE_COMPACTION,
    "custom.mc.after_compaction": K.AFTER_COMPACTION,
    "custom.mc.approval_resolved": K.APPROVAL_RESOLVED,
    "custom.mc.hook_invoked": K.HOOK_INVOKED,
    "custom.mc.hook_result": K.HOOK_RESULT,
    "model.usage": K.USAGE,
    "graph.invocation_ended": K.TURN_ENDED,
    "graph.run_result": K.RUN_RESULT,
    "graph.status": K.STATUS,
    "graph.error": K.ERROR,
}

# Cursor SDK InteractionUpdate types (local `on_delta`/`run.observe`, cloud
# `interaction_update`), shared by both Cursor profiles.
_CURSOR_INTERACTION: dict[str, KindRule] = {
    "text-delta": K.MESSAGE_DELTA,
    "thinking-delta": K.THINKING_DELTA,
    "tool-call-started": K.TOOL_CALL_STARTED,
    "tool-call-delta": K.TOOL_CALL_DELTA,
    "tool-call-completed": _TOOL_RESULT_STATUS,
    "step-started": K.STATUS,
    "step-completed": K.STATUS,
    "turn-ended": K.TURN_ENDED,
    "summary-started": K.BEFORE_COMPACTION,
    "summary-completed": K.AFTER_COMPACTION,
}

# Cursor Local: bridge `RunStreamEvent{kind, offset}` and SDK message kinds, plus the
# synthetic lifecycle frames the lane writes from `Agent.create` / `send` returns.
CURSOR_LOCAL_KINDS: dict[str, KindRule] = {
    **_CURSOR_INTERACTION,
    "agent.created": K.SESSION_INIT,
    "send.accepted": K.TURN_STARTED,
    "status": K.STATUS,
    "system": K.STATUS,
    "assistant": K.MESSAGE,
    "thinking": K.THINKING_DELTA,
    "user": K.MESSAGE,
    "tool_call": _TOOL_STATUS,
    "request": K.APPROVAL_REQUESTED,
    "request.resolved": K.APPROVAL_RESOLVED,
    "TurnEndedUpdate": K.TURN_ENDED,
    "usage": K.USAGE,
    "RunResult": K.RUN_RESULT,
    "result": K.RUN_RESULT,
    "error": K.ERROR,
    "heartbeat": K.HEARTBEAT,
}

# Cursor Cloud: SSE `GET /v1/agents/{id}/runs/{runId}/stream` events, plus synthetic
# frames (`run.final` after `410 stream_expired`, `usage` from `get_usage`).
CURSOR_CLOUD_KINDS: dict[str, KindRule] = {
    "agent.created": K.SESSION_INIT,
    "run.created": K.TURN_STARTED,
    "status": K.STATUS,
    "assistant": K.MESSAGE_DELTA,
    "thinking": K.THINKING_DELTA,
    "tool_call": _TOOL_STATUS,
    "interaction_update": ByField("type", _CURSOR_INTERACTION),
    "heartbeat": K.HEARTBEAT,
    "result": K.RUN_RESULT,
    "run.final": K.RUN_RESULT,
    "usage": K.USAGE,
    "error": K.ERROR,
    "done": K.STATUS,
}

KIND_TABLES: dict[LaneProfile, dict[str, KindRule]] = {
    LaneProfile.DEEP_AGENTS: DEEP_AGENTS_KINDS,
    LaneProfile.CURSOR_LOCAL: CURSOR_LOCAL_KINDS,
    LaneProfile.CURSOR_CLOUD: CURSOR_CLOUD_KINDS,
    # Reserved lanes (ADR-0018 order): rows are added when their writers are built.
    LaneProfile.CLAUDE_AGENT_SDK: {},
    LaneProfile.CODEX: {},
}

# How a dedupe key is formed, per lane (documented for lane implementers; SPEC-03 table).
DEDUPE_KEY_RULES: dict[LaneProfile, str] = {
    LaneProfile.DEEP_AGENTS: (
        "thread_id:checkpoint_ns:checkpoint_id:step:message_id|tool_call_id:kind "
        "(empty components when the stream metadata does not carry them)"
    ),
    LaneProfile.CURSOR_LOCAL: "bridge:<RunStreamEvent.offset> (durable for ObserveRun)",
    LaneProfile.CURSOR_CLOUD: "sse:<event id>; run:<run_id>:final after 410 stream_expired",
    LaneProfile.CLAUDE_AGENT_SDK: "message.uuid (fallback session_id:message_id:block_index)",
    LaneProfile.CODEX: "threadId:turnId:itemId:method",
}


@dataclass(frozen=True)
class Classification:
    kind: FrameKind
    known: bool


class UnknownKindCounter:
    """Counts unmapped raw kinds per lane; a metric surface, never an error."""

    def __init__(self) -> None:
        self._counts: Counter[tuple[str, str]] = Counter()

    def record(self, lane: LaneProfile, raw_kind: str) -> None:
        self._counts[(lane.value, raw_kind)] += 1
        logger.warning(
            "unmapped provider frame kind", extra={"lane_profile": lane.value, "raw_kind": raw_kind}
        )

    def snapshot(self) -> dict[tuple[str, str], int]:
        return dict(self._counts)


UNKNOWN_KINDS = UnknownKindCounter()


def _resolve(rule: KindRule, body: Any) -> Classification:
    if isinstance(rule, FrameKind):
        return Classification(rule, True)
    value = body.get(rule.field) if isinstance(body, Mapping) else None
    nested = rule.values.get(str(value).lower() if value is not None else "")
    if nested is None:
        nested = rule.values.get(str(value)) if value is not None else None
    if isinstance(nested, ByField):
        return _resolve(nested, body)
    if nested is not None:
        return Classification(nested, True)
    return Classification(rule.default, rule.default != FrameKind.UNKNOWN)


def classify(
    lane: LaneProfile,
    raw_kind: str,
    body: Any = None,
    *,
    counter: UnknownKindCounter | None = UNKNOWN_KINDS,
) -> Classification:
    """Map one provider raw kind (and, where the table says so, its body) to a FrameKind."""

    rule = KIND_TABLES[lane].get(raw_kind) or HOOK_KINDS.get(raw_kind)
    result = Classification(FrameKind.UNKNOWN, False) if rule is None else _resolve(rule, body)
    if not result.known and counter is not None:
        counter.record(lane, raw_kind)
    return result


def bounded_key(*parts: object) -> str:
    """Join key components with `:`; keys beyond the column bound keep a digest suffix."""

    key = ":".join("" if part is None else str(part) for part in parts)
    if len(key) <= MAX_PROVIDER_KEY_LENGTH:
        return key
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"{key[: MAX_PROVIDER_KEY_LENGTH - 72]}:sha256:{digest}"


def deep_agents_key(
    *,
    thread_id: str,
    checkpoint_ns: str = "",
    checkpoint_id: str | None = None,
    step: int | None = None,
    item_id: str,
    kind: str,
) -> str:
    return bounded_key(thread_id, checkpoint_ns, checkpoint_id, step, item_id, kind)


def cursor_local_key(offset: int | str) -> str:
    return bounded_key("bridge", offset)


def cursor_cloud_key(event_id: str) -> str:
    return bounded_key("sse", event_id)


def cursor_cloud_final_key(run_id: str) -> str:
    return bounded_key("run", run_id, "final")


def hook_key(event: str, ref: str, invocation_ordinal: int, phase: str) -> str:
    if phase not in {"invoked", "result"}:
        raise ValueError("hook frame phase is invoked or result")
    return bounded_key("hook", event, ref, invocation_ordinal, phase)
