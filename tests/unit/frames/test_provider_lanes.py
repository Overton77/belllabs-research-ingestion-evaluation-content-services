"""MP-13: Claude Agent SDK and Codex frame mapping, child lineage, settle-once derivation,
usage aggregation and per-stream cursors.

The JSONL inputs under `fixtures/` are hand-written FIXTURES of documented provider shapes
(each file's first line says so and cites its source); they are not live recordings and do
not prove live provider behaviour. They do prove what this code does with those shapes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.frames.lineage import (
    ChildUsage,
    agent_server_child,
    aggregate_usage,
    linked_mission,
    provider_subordinates,
    usage_rule,
)
from mission_control.application.frames.provider_mapping import (
    claude_agent_sdk_observations,
    codex_exec_observations,
    codex_observations,
)
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    DeriveContext,
    derive,
)
from mission_control.application.frames.streams import (
    StreamCursorError,
    frame_cursor,
    frame_envelope,
    frame_passes,
    mission_cursor,
    parse_frame_cursor,
    parse_mission_cursor,
    subordinate_index,
)
from mission_control.application.frames.transcript import summarize_sessions
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import (
    CLOSING_KINDS,
    FrameKind,
    FrameObservation,
    LaneProfile,
    ProviderFrame,
)
from mission_control.domain.frames.facts import (
    ExecutionOutcomeFact,
    SessionStartedFact,
    ToolEffectFact,
    TurnCompletedFact,
    TurnStartedFact,
    UnitInDoubtFact,
)
from mission_control.domain.frames.usage import UsageDimension, UsageDisposition, UsageReport
from mission_control.domain.policies.contracts import (
    AsyncChildAuthorityState,
    AsyncChildDependencyClass,
)
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamFilters,
    StreamScope,
)
from tests.fixtures.provider_frames import (
    SCOPE,
    StepClock,
    harness_start,
    in_memory_store,
    provider_frame,
)

K = FrameKind
FIXTURES = Path(__file__).parent / "fixtures"
COVERED = CompletionEvidence(declared_outputs=frozenset({"o"}), registered_outputs=frozenset({"o"}))


def load(relative: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lines = (FIXTURES / relative).read_text(encoding="utf-8").splitlines()
    header, *rows = (json.loads(line) for line in lines if line.strip())
    return header, rows


def claude_observations(counter: UnknownKindCounter) -> list[FrameObservation]:
    _header, rows = load("claude_agent_sdk/task_subagent_session.jsonl")
    return [item for row in rows for item in claude_agent_sdk_observations(row, counter=counter)]


def codex_app_observations(counter: UnknownKindCounter) -> list[FrameObservation]:
    _header, rows = load("codex/app_server_child_thread.jsonl")
    return [
        item
        for row in rows
        for item in codex_observations(
            row["method"], row["params"], root_thread_id="thr_root", counter=counter
        )
    ]


async def persisted(
    observations: list[FrameObservation], lane: LaneProfile, session: str
) -> tuple[list[ProviderFrame], Any, Any]:
    """Write through the real writer and in-memory store (dedupe, ordinals, identity)."""

    store, _run_id = in_memory_store()
    handle = await store.open_execution(harness_start(lane=lane, native_session_ref=session))
    writer = FrameWriter(store, handle, clock=StepClock())
    await writer.write(observations)
    frames = await store.frames_for_execution(SCOPE, handle.harness_execution_id, 1)
    return list(frames), store, handle


def context(lane: LaneProfile) -> DeriveContext:
    return DeriveContext(lane=lane, current_generation=1, completion=COVERED)


# --- fixture labelling -------------------------------------------------------------------


@pytest.mark.parametrize(
    "relative",
    [
        "claude_agent_sdk/task_subagent_session.jsonl",
        "codex/app_server_child_thread.jsonl",
        "codex/exec_json.jsonl",
    ],
)
def test_fixtures_are_labelled_as_fixtures_with_sources(relative: str):
    header, rows = load(relative)
    assert header["recorded"] is False
    assert header["note"].startswith("FIXTURE, not a live recording")
    assert "ctx7" in header["note"] and rows


# --- Claude Agent SDK --------------------------------------------------------------------


def test_claude_mapping_names_kinds_tool_refs_and_task_subordinates():
    counter = UnknownKindCounter()
    observations = claude_observations(counter)
    summary = [
        (item.raw_kind, item.kind, item.tool_call_ref, item.subordinate_ref)
        for item in observations
    ]
    task = "claude:task:toolu_task_01"
    assert summary[:9] == [
        ("system.init", K.SESSION_INIT, None, None),
        ("assistant", K.MESSAGE, None, None),
        ("assistant.tool_use", K.TOOL_CALL_STARTED, "toolu_task_01", None),
        ("system.task_started", K.STATUS, None, task),
        ("assistant", K.MESSAGE, None, task),
        ("assistant.tool_use", K.TOOL_CALL_STARTED, "toolu_sub_bash_01", task),
        ("user.tool_result", K.TOOL_CALL_COMPLETED, "toolu_sub_bash_01", task),
        ("system.task_progress", K.STATUS, None, task),
        ("system.task_notification", K.STATUS, None, task),
    ]
    assert ("user.tool_result", K.TOOL_CALL_COMPLETED, "toolu_task_01", None) in summary
    assert ("stream_event", K.MESSAGE_DELTA, None, None) in summary
    assert ("future_message_kind", K.UNKNOWN, None, None) in summary
    assert counter.snapshot() == {("claude_agent_sdk", "future_message_kind"): 1}
    # Each ResultMessage is a turn end then a run result, in that order.
    results = [item.raw_kind for item in observations if item.raw_kind.startswith("result")]
    assert results == ["result.turn_ended", "result"] * 2
    assert observations[0].native_session_ref == "sess-fixture-1"


async def test_claude_session_derives_once_with_provider_totals_and_no_child_closure():
    counter = UnknownKindCounter()
    frames, store, handle = await persisted(
        claude_observations(counter), LaneProfile.CLAUDE_AGENT_SDK, "sess-fixture-1"
    )
    # The duplicated ResultMessage (same uuid) is deduplicated by the store.
    assert sum(1 for frame in frames if frame.kind == K.RUN_RESULT) == 1
    facts = derive(frames, context(LaneProfile.CLAUDE_AGENT_SDK))
    kinds = [type(fact).__name__ for fact in facts]
    assert kinds == [
        "SessionStartedFact",
        "TurnStartedFact",
        "ToolEffectFact",
        "ToolEffectFact",
        "TurnCompletedFact",
        "ExecutionOutcomeFact",
    ]
    session, turn_started, child_tool, parent_tool, turn, outcome = facts
    assert isinstance(session, SessionStartedFact) and session.subordinate_ref is None
    assert isinstance(turn_started, TurnStartedFact) and turn_started.subordinate_ref is None
    assert isinstance(child_tool, ToolEffectFact)
    assert child_tool.subordinate_ref == "claude:task:toolu_task_01"
    assert isinstance(parent_tool, ToolEffectFact) and parent_tool.subordinate_ref is None
    assert isinstance(turn, TurnCompletedFact)
    # ResultMessage.usage is the top-level loop only; total_cost_usd includes subagents.
    assert turn.usage.value("input_tokens") == 2000
    assert turn.usage.value("cached_input_tokens") == 500
    assert turn.usage.value("output_tokens") == 640
    assert turn.usage.dimensions["cost_micros"].value == 42_100
    assert turn.usage.dimensions["cost_micros"].disposition == UsageDisposition.ESTIMATED
    assert isinstance(outcome, ExecutionOutcomeFact)
    assert (outcome.provider_status, outcome.outcome) == ("success", outcome.outcome.SUCCEEDED)
    assert store.turns(handle.harness_execution_id)[0]["turn_no"] == 1
    assert len(store.turns(handle.harness_execution_id)) == 1


def test_claude_result_statuses_map_through_subtype_not_result_text():
    lane = LaneProfile.CLAUDE_AGENT_SDK
    cases = [
        ({"subtype": "success", "is_error": False, "result": "failed to find"}, "success"),
        ({"subtype": "error_max_turns", "is_error": True, "result": ""}, "failed"),
        ({"subtype": "error_during_execution", "api_error_status": 429}, "failed"),
    ]
    for body, status in cases:
        frames = [provider_frame(1, K.RUN_RESULT, body, lane=lane, raw_kind="result")]
        facts = derive(frames, context(lane))
        outcome = next(fact for fact in facts if isinstance(fact, ExecutionOutcomeFact))
        assert outcome.provider_status == status
    capacity = derive(
        [provider_frame(1, K.RUN_RESULT, cases[2][0], lane=lane, raw_kind="result")],
        context(lane),
    )
    failure = next(fact for fact in capacity if isinstance(fact, ExecutionOutcomeFact))
    assert failure.failure_class is not None and failure.failure_class.value == "capacity"


# --- Codex ---------------------------------------------------------------------------------


def test_codex_mapping_keeps_root_session_and_tags_child_threads():
    counter = UnknownKindCounter()
    observations = codex_app_observations(counter)
    child = [item for item in observations if item.subordinate_ref == "codex:thread:thr_child"]
    assert {item.native_session_ref for item in observations} == {"thr_root"}
    assert [item.kind for item in child] == [
        K.TURN_STARTED,
        K.MESSAGE,
        K.USAGE,
        K.TURN_ENDED,
        K.RUN_RESULT,
    ]
    tools = [(item.kind, item.tool_call_ref) for item in observations if item.tool_call_ref]
    assert (K.TOOL_CALL_COMPLETED, "item_cmd_1") in tools
    # An unmapped method and an item that has not finished are explicit unknowns.
    unknown = [item.raw_kind for item in observations if item.kind == K.UNKNOWN]
    assert unknown == ["item/futureThing/updated", "item/completed"]
    assert all(item.kind not in CLOSING_KINDS for item in observations if item.kind == K.UNKNOWN)


async def test_codex_child_thread_never_closes_the_parent_and_usage_folds_once():
    counter = UnknownKindCounter()
    frames, store, handle = await persisted(
        codex_app_observations(counter), LaneProfile.CODEX, "thr_root"
    )
    # The resent turn/completed (same thread, turn and method) was stored once.
    assert sum(1 for f in frames if f.kind == K.TURN_ENDED and f.subordinate_ref is None) == 1
    facts = derive(frames, context(LaneProfile.CODEX))
    turns = [fact for fact in facts if isinstance(fact, TurnCompletedFact)]
    outcomes = [fact for fact in facts if isinstance(fact, ExecutionOutcomeFact)]
    assert len(turns) == 1 and turns[0].turn_ordinal == 1
    assert len(outcomes) == 1 and outcomes[0].provider_status == "completed"
    assert outcomes[0].subordinate_ref is None
    # Root `last` (800 + 500) plus the child thread's own call (400); never `total`.
    usage = turns[0].usage
    assert usage.value("input_tokens") == 1700
    assert usage.value("cached_input_tokens") == 300
    assert usage.value("output_tokens") == 400
    assert usage.value("reasoning_tokens") == 80
    assert len(turns[0].usage_frame_ids) == 3
    assert len(store.turns(handle.harness_execution_id)) == 1


async def test_codex_exec_json_maps_tools_and_turn_usage():
    _header, rows = load("codex/exec_json.jsonl")
    counter = UnknownKindCounter()
    observations = [
        item
        for row in rows
        for item in codex_exec_observations(
            row, thread_id="thr_exec_1", turn_index=1, counter=counter
        )
    ]
    assert counter.snapshot() == {}
    frames, _store, _handle = await persisted(observations, LaneProfile.CODEX, "thr_exec_1")
    facts = derive(frames, context(LaneProfile.CODEX))
    tool = next(fact for fact in facts if isinstance(fact, ToolEffectFact))
    turn = next(fact for fact in facts if isinstance(fact, TurnCompletedFact))
    outcome = next(fact for fact in facts if isinstance(fact, ExecutionOutcomeFact))
    assert (tool.tool_call_ref, tool.status) == ("item_0", "completed")
    assert turn.usage.value("input_tokens") == 24763
    assert turn.usage.value("cached_input_tokens") == 24448
    assert turn.usage.value("reasoning_tokens") == 64
    assert outcome.provider_status == "completed"


# --- settle once / unknown never terminal ------------------------------------------------------


def test_duplicate_and_reordered_closing_frames_settle_once():
    lane = LaneProfile.CODEX
    end = {"threadId": "t", "turn": {"id": "turn-1", "status": "completed"}}
    frames = [
        provider_frame(
            1, K.TOOL_CALL_COMPLETED, {"status": "completed"}, lane=lane, tool_call_ref="c1"
        ),
        provider_frame(2, K.RUN_RESULT, end, lane=lane, raw_kind="turn/completed.result"),
        provider_frame(3, K.TURN_ENDED, end, lane=lane, raw_kind="turn/completed"),
        provider_frame(4, K.TURN_ENDED, end, lane=lane, raw_kind="turn/completed"),
        provider_frame(
            5, K.TOOL_CALL_COMPLETED, {"status": "completed"}, lane=lane, tool_call_ref="c1"
        ),
        provider_frame(6, K.RUN_RESULT, end, lane=lane, raw_kind="turn/completed.result"),
    ]
    facts = derive(frames, context(lane))
    assert sum(isinstance(fact, TurnCompletedFact) for fact in facts) == 1
    assert sum(isinstance(fact, ExecutionOutcomeFact) for fact in facts) == 1
    assert sum(isinstance(fact, ToolEffectFact) for fact in facts) == 1
    assert sum(isinstance(fact, TurnStartedFact) for fact in facts) == 1
    # Derivation is order-independent of input order (it sorts by arrival ordinal).
    assert derive(list(reversed(frames)), context(lane)) == facts


def test_unknown_and_delta_frames_cannot_terminalize():
    lane = LaneProfile.CLAUDE_AGENT_SDK
    terminal_body = {"status": "completed", "subtype": "success", "result": "done"}
    frames = [
        provider_frame(1, K.UNKNOWN, terminal_body, lane=lane, raw_kind="future_message_kind"),
        provider_frame(2, K.MESSAGE_DELTA, terminal_body, lane=lane, raw_kind="stream_event"),
        provider_frame(3, K.STATUS, terminal_body, lane=lane, raw_kind="system.task_notification"),
    ]
    assert derive(frames, context(lane)) == ()
    # The frame contract refuses an unknown (or any non-closing kind) marked closing.
    for frame in frames:
        with pytest.raises(ValidationError):
            ProviderFrame.model_validate({**frame.model_dump(), "closing": True})


def test_child_lifecycle_frames_never_start_end_or_settle_the_parent():
    lane = LaneProfile.DEEP_AGENTS
    child = "tools:task-7"
    frames = [
        provider_frame(1, K.TURN_ENDED, {"outcome": "succeeded"}, lane=lane, subordinate_ref=child),
        provider_frame(2, K.RUN_RESULT, {"status": "finished"}, lane=lane, subordinate_ref=child),
        provider_frame(
            3,
            K.ERROR,
            {"unknown_state": True, "reason": "child lost"},
            lane=lane,
            subordinate_ref=child,
        ),
    ]
    facts = derive(frames, context(lane))
    assert not any(isinstance(fact, TurnCompletedFact | ExecutionOutcomeFact) for fact in facts)
    in_doubt = [fact for fact in facts if isinstance(fact, UnitInDoubtFact)]
    assert len(in_doubt) == 1 and in_doubt[0].subordinate_ref == child


# --- lineage and usage ---------------------------------------------------------------------


async def test_provider_subagents_resolve_by_native_refs_with_visibility():
    claude, _s, _h = await persisted(
        claude_observations(UnknownKindCounter()), LaneProfile.CLAUDE_AGENT_SDK, "sess-fixture-1"
    )
    (node,) = provider_subordinates(claude)
    assert node.ref.kind == "provider_subagent"
    assert (node.ref.native_child_ref, node.ref.spawn_correlation) == ("task-a1", "toolu_task_01")
    assert node.ref.native_parent_ref == "sess-fixture-1"
    assert (node.resolved, node.lifecycle, node.ref.visibility) == (True, "ended", "full")
    codex, _s, _h = await persisted(
        codex_app_observations(UnknownKindCounter()), LaneProfile.CODEX, "thr_root"
    )
    (child,) = provider_subordinates(codex)
    assert (child.ref.native_child_ref, child.ref.spawn_correlation) == (
        "thr_child",
        "item_spawn_1",
    )
    assert (child.resolved, child.lifecycle, child.ref.visibility) == (True, "ended", "full")


def test_unmatched_children_stay_unresolved_and_spawn_only_children_are_unavailable():
    lane = LaneProfile.CODEX
    spawn = {
        "threadId": "root",
        "item": {"type": "collabAgentToolCall", "id": "spawn-9", "receiverThreadIds": ["quiet"]},
    }
    frames = [
        provider_frame(1, K.STATUS, spawn, lane=lane, raw_kind="item/completed", session="root"),
        provider_frame(
            2,
            K.STATUS,
            {"threadId": "orphan"},
            lane=lane,
            raw_kind="turn/started",
            session="root",
            subordinate_ref="codex:thread:orphan",
        ),
    ]
    nodes = {node.ref.native_child_ref: node for node in provider_subordinates(frames)}
    assert nodes["orphan"].resolved is False
    assert nodes["orphan"].ref.visibility == "lifecycle_only"
    assert nodes["quiet"].ref.visibility == "unavailable"
    assert nodes["quiet"].ref.spawn_correlation == "spawn-9"


def _usage(tokens: int | None, cost: int | None) -> UsageReport:
    def dim(value: int | None) -> UsageDimension:
        if value is None:
            return UsageDimension(value=None, disposition=UsageDisposition.UNKNOWN)
        return UsageDimension(value=value, disposition=UsageDisposition.SETTLED)

    return UsageReport(
        dimensions={
            "input_tokens": dim(tokens),
            "cached_input_tokens": dim(0 if tokens is not None else None),
            "output_tokens": dim(tokens),
            "reasoning_tokens": dim(None),
            "cost_micros": dim(cost),
        }
    )


def test_usage_aggregation_keeps_kinds_distinct_and_never_double_counts():
    provider_child = provider_subordinates(
        [
            provider_frame(
                1,
                K.MESSAGE,
                {"uuid": "m"},
                lane=LaneProfile.CLAUDE_AGENT_SDK,
                subordinate_ref="claude:task:toolu_1",
                session="s",
            ),
        ]
    )[0]
    state = AsyncChildAuthorityState(
        child_execution_id="as-child-1",
        parent_operation_ref="harness_execution:parent",
        dependency_class=AsyncChildDependencyClass.REQUIRED_BLOCKING,
        reservation_id="res-1",
    )
    server_child = agent_server_child(state)
    linked = linked_mission(
        link_id="L1", from_mission_id="m-1", to_mission_id="m-2", released_run_id="r-2"
    )
    assert {provider_child.ref.kind, server_child.ref.kind, linked.ref.kind} == {
        "provider_subagent",
        "agent_server_child",
        "linked_mission",
    }
    parent = _usage(1000, 50_000)
    aggregate = aggregate_usage(
        LaneProfile.CLAUDE_AGENT_SDK,
        parent,
        [
            ChildUsage(provider_child, _usage(300, 9_000)),
            ChildUsage(server_child, _usage(200, 4_000)),
            ChildUsage(server_child, _usage(200, 4_000)),  # listed twice: counted once
            ChildUsage(linked, _usage(5_000, 99_000)),
        ],
    )
    assert aggregate.total is not None
    # Parent + the Agent Server child once; the Claude subagent's tokens are not attributable
    # and its cost is already inside the parent's provider total; linked missions never add.
    assert aggregate.total.value("input_tokens") == 1200
    assert aggregate.total.value("cost_micros") == 54_000
    assert aggregate.total.value("reasoning_tokens") is None
    assert aggregate.coverage["input_tokens"] == "excludes_subordinates"
    assert aggregate.coverage["cost_micros"] == "complete"
    assert [item.ref.kind for item in aggregate.children] == [
        "provider_subagent",
        "agent_server_child",
    ]
    assert [item.ref.kind for item in aggregate.separate] == ["linked_mission"]
    assert usage_rule(LaneProfile.CODEX, "provider_subagent").tokens == "folded"
    assert usage_rule(LaneProfile.CLAUDE_CLOUD, "provider_subagent").tokens == "unattributable"


async def test_session_summary_reports_parent_counts_children_and_coverage():
    frames, _s, _h = await persisted(
        claude_observations(UnknownKindCounter()), LaneProfile.CLAUDE_AGENT_SDK, "sess-fixture-1"
    )
    (summary,) = summarize_sessions(frames)
    assert summary.turns_closed == 1
    assert summary.tool_calls == 1  # the parent's Task call; the child's Bash is the child's
    assert summary.run_result_status == "success"
    assert summary.usage is not None and summary.usage.value("input_tokens") == 2000
    assert summary.usage_coverage == "excludes_subordinates"
    assert [node.ref.native_child_ref for node in summary.subordinates] == ["task-a1"]
    codex, _s, _h = await persisted(
        codex_app_observations(UnknownKindCounter()), LaneProfile.CODEX, "thr_root"
    )
    (codex_summary,) = summarize_sessions(codex)
    assert codex_summary.turns_closed == 1
    assert codex_summary.usage is not None
    assert codex_summary.usage.value("input_tokens") == 1700
    assert codex_summary.usage_coverage == "complete"


# --- per-stream cursors --------------------------------------------------------------------


def test_frame_cursors_are_per_execution_and_generation():
    frame = provider_frame(7, K.MESSAGE, {"x": 1})
    cursor = frame_cursor(frame)
    assert cursor.stream == "provider_frames" and cursor.generation == 1
    position = parse_frame_cursor(
        cursor,
        harness_execution_id=frame.harness_execution_id,
        current_generation=1,
        high_watermark=7,
    )
    assert position.arrival_ordinal == 7
    for kwargs, code in (
        ({"current_generation": 2, "high_watermark": 9}, "STALE_GENERATION"),
        ({"current_generation": 1, "high_watermark": 6}, "CURSOR_AHEAD"),
    ):
        with pytest.raises(StreamCursorError) as error:
            parse_frame_cursor(cursor, harness_execution_id=frame.harness_execution_id, **kwargs)
        assert error.value.code == code
    with pytest.raises(StreamCursorError) as other:
        parse_frame_cursor(
            cursor, harness_execution_id=uuid4(), current_generation=1, high_watermark=9
        )
    assert other.value.code == "SCOPE_MISMATCH"
    with pytest.raises(StreamCursorError) as crossed:
        parse_mission_cursor(cursor, high_watermark=10)
    assert crossed.value.code == "SCOPE_MISMATCH"
    assert parse_mission_cursor(mission_cursor(12), high_watermark=12) == 12
    with pytest.raises(StreamCursorError):
        parse_mission_cursor(
            StreamCursor(stream="mission_events", position="13"), high_watermark=12
        )


async def test_frame_envelopes_carry_lineage_and_respect_filters():
    frames, _s, _h = await persisted(
        claude_observations(UnknownKindCounter()), LaneProfile.CLAUDE_AGENT_SDK, "sess-fixture-1"
    )
    index = subordinate_index(provider_subordinates(frames))
    scope = StreamScope(installation_id="i", application_id="biotech", tenant_id="t")
    child = next(
        frame for frame in frames if frame.raw_kind == "user.tool_result" and frame.subordinate_ref
    )
    envelope = frame_envelope(child, scope=scope, subordinates=index)
    assert envelope.subordinate_ref is not None
    assert envelope.subordinate_ref.native_child_ref == "task-a1"
    assert envelope.subordinate_ref.spawn_correlation == "toolu_task_01"
    assert envelope.cursor == frame_cursor(child)
    assert "body_excerpt" not in envelope.payload  # tool detail "summary"
    delta = next(frame for frame in frames if frame.kind == K.MESSAGE_DELTA)
    assert not frame_passes(delta, StreamFilters())
    assert frame_passes(delta, StreamFilters(exclude_deltas=False))
    assert not frame_passes(child, StreamFilters(tool_detail="none"))
    assert isinstance(UUID(envelope.event_id), UUID)
