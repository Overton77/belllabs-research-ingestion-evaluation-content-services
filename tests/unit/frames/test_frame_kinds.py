"""Per-lane kind tables and dedupe keys (SPEC-03 "FrameSink port and writers")."""

from __future__ import annotations

import pytest

from mission_control.application.frames.kinds import (
    DEDUPE_KEY_RULES,
    KIND_TABLES,
    UnknownKindCounter,
    bounded_key,
    classify,
    cursor_cloud_final_key,
    cursor_cloud_key,
    cursor_local_key,
    deep_agents_key,
    hook_key,
)
from mission_control.domain.frames.contracts import (
    MAX_PROVIDER_KEY_LENGTH,
    FrameKind,
    LaneProfile,
    is_closing,
)

K = FrameKind

# Documented provider events per lane (research notes) -> expected FrameKind.
DEEP_AGENTS_EVENTS = [
    ("graph.session_init", None, K.SESSION_INIT),
    ("graph.invocation_started", None, K.TURN_STARTED),
    ("messages.ai_chunk", None, K.MESSAGE_DELTA),
    ("messages.reasoning_chunk", None, K.THINKING_DELTA),
    ("updates.ai_message", None, K.MESSAGE),
    ("updates.ai_tool_call", None, K.TOOL_CALL_STARTED),
    ("updates.tool_message", {"status": "success"}, K.TOOL_CALL_COMPLETED),
    ("updates.tool_message", {"status": "error"}, K.TOOL_CALL_FAILED),
    ("checkpoint_backfill", {"status": "success"}, K.TOOL_CALL_COMPLETED),
    ("updates.interrupt", None, K.APPROVAL_REQUESTED),
    ("updates.summarization_event", None, K.AFTER_COMPACTION),
    ("custom.mc.before_compaction", None, K.BEFORE_COMPACTION),
    ("custom.mc.after_compaction", None, K.AFTER_COMPACTION),
    ("custom.mc.hook_invoked", None, K.HOOK_INVOKED),
    ("custom.mc.hook_result", None, K.HOOK_RESULT),
    ("custom.mc.approval_resolved", None, K.APPROVAL_RESOLVED),
    ("model.usage", None, K.USAGE),
    ("graph.invocation_ended", None, K.TURN_ENDED),
    ("graph.run_result", None, K.RUN_RESULT),
    ("graph.error", None, K.ERROR),
]
CURSOR_LOCAL_EVENTS = [
    ("agent.created", None, K.SESSION_INIT),
    ("send.accepted", None, K.TURN_STARTED),
    ("status", {"status": "RUNNING"}, K.STATUS),
    ("text-delta", None, K.MESSAGE_DELTA),
    ("thinking-delta", None, K.THINKING_DELTA),
    ("assistant", None, K.MESSAGE),
    ("tool_call", {"callId": "c1", "status": "running"}, K.TOOL_CALL_STARTED),
    ("tool_call", {"callId": "c1", "status": "completed"}, K.TOOL_CALL_COMPLETED),
    ("tool_call", {"callId": "c1", "status": "error"}, K.TOOL_CALL_FAILED),
    ("tool-call-started", None, K.TOOL_CALL_STARTED),
    ("tool-call-completed", {"status": "completed"}, K.TOOL_CALL_COMPLETED),
    ("request", {"request_id": "r1"}, K.APPROVAL_REQUESTED),
    ("summary-started", None, K.BEFORE_COMPACTION),
    ("summary-completed", None, K.AFTER_COMPACTION),
    ("TurnEndedUpdate", None, K.TURN_ENDED),
    ("turn-ended", None, K.TURN_ENDED),
    ("usage", {"inputTokens": 1}, K.USAGE),
    ("RunResult", {"status": "finished"}, K.RUN_RESULT),
    ("error", None, K.ERROR),
    ("heartbeat", None, K.HEARTBEAT),
]
CURSOR_CLOUD_EVENTS = [
    ("agent.created", None, K.SESSION_INIT),
    ("run.created", None, K.TURN_STARTED),
    ("status", {"runId": "r", "status": "RUNNING"}, K.STATUS),
    ("assistant", {"text": "hi"}, K.MESSAGE_DELTA),
    ("thinking", {"text": "hmm"}, K.THINKING_DELTA),
    ("tool_call", {"callId": "c", "name": "edit", "status": "running"}, K.TOOL_CALL_STARTED),
    ("tool_call", {"callId": "c", "name": "edit", "status": "completed"}, K.TOOL_CALL_COMPLETED),
    ("interaction_update", {"type": "turn-ended"}, K.TURN_ENDED),
    ("interaction_update", {"type": "summary-completed"}, K.AFTER_COMPACTION),
    ("interaction_update", {"type": "text-delta"}, K.MESSAGE_DELTA),
    ("heartbeat", {}, K.HEARTBEAT),
    ("result", {"runId": "r", "status": "finished"}, K.RUN_RESULT),
    ("run.final", {"status": "expired"}, K.RUN_RESULT),
    ("usage", {"charged_cents": 12}, K.USAGE),
    ("error", {"code": "x"}, K.ERROR),
    ("done", {}, K.STATUS),
]


@pytest.mark.parametrize(
    ("lane", "events"),
    [
        (LaneProfile.DEEP_AGENTS, DEEP_AGENTS_EVENTS),
        (LaneProfile.CURSOR_LOCAL, CURSOR_LOCAL_EVENTS),
        (LaneProfile.CURSOR_CLOUD, CURSOR_CLOUD_EVENTS),
    ],
)
def test_every_documented_provider_event_maps_to_a_frame_kind(lane, events) -> None:  # type: ignore[no-untyped-def]
    counter = UnknownKindCounter()
    for raw_kind, body, expected in events:
        result = classify(lane, raw_kind, body, counter=counter)
        assert result.kind == expected, (lane, raw_kind, body)
        assert result.known
    assert counter.snapshot() == {}


@pytest.mark.parametrize("lane", list(LaneProfile))
def test_hook_frames_classify_on_every_lane(lane: LaneProfile) -> None:
    assert classify(lane, "hook.invoked", counter=None).kind == K.HOOK_INVOKED
    assert classify(lane, "hook.result", counter=None).kind == K.HOOK_RESULT


def test_unmapped_kinds_are_unknown_never_closing_and_counted() -> None:
    counter = UnknownKindCounter()
    result = classify(LaneProfile.CURSOR_CLOUD, "brand_new_event", {"x": 1}, counter=counter)
    assert result.kind == K.UNKNOWN and not result.known and not is_closing(result.kind)
    tool = classify(LaneProfile.CURSOR_LOCAL, "tool_call", {"status": "weird"}, counter=counter)
    assert tool.kind == K.UNKNOWN
    assert counter.snapshot() == {
        ("cursor_cloud", "brand_new_event"): 1,
        ("cursor_local", "tool_call"): 1,
    }


def test_every_lane_documents_its_dedupe_key_and_reserved_lanes_have_tables() -> None:
    assert set(DEDUPE_KEY_RULES) == set(LaneProfile) == set(KIND_TABLES)


def test_dedupe_keys_are_stable_and_bounded() -> None:
    first = deep_agents_key(
        thread_id="belllabs/stage/u/gen/1",
        checkpoint_ns="tools:abc",
        checkpoint_id=None,
        step=3,
        item_id="call-1",
        kind="tool_call_completed",
    )
    again = deep_agents_key(
        thread_id="belllabs/stage/u/gen/1",
        checkpoint_ns="tools:abc",
        step=3,
        item_id="call-1",
        kind="tool_call_completed",
    )
    assert first == again == "belllabs/stage/u/gen/1:tools:abc::3:call-1:tool_call_completed"
    assert cursor_local_key(42) == "bridge:42"
    assert cursor_cloud_key("evt-9") == "sse:evt-9"
    assert cursor_cloud_final_key("run-1") == "run:run-1:final"
    assert hook_key("preToolUse", "call-1", 2, "invoked") != hook_key(
        "preToolUse", "call-1", 2, "result"
    )
    with pytest.raises(ValueError):
        hook_key("preToolUse", "call-1", 2, "other")
    long = bounded_key("x" * 3_000, "kind")
    assert len(long) <= MAX_PROVIDER_KEY_LENGTH and long == bounded_key("x" * 3_000, "kind")
