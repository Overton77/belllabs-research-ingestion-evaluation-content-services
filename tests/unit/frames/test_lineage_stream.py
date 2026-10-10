"""Incremental lineage and the common frame projection (SPEC-04, ADR-0040; V16 offline).

The provider inputs are the hand-written FIXTURES under `tests/unit/frames/fixtures/`
(labelled there as not live recordings).
"""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.frames.lineage import provider_subordinates
from mission_control.application.frames.lineage_stream import LineageTracker
from mission_control.application.frames.projections import (
    NORMALIZED_FACTS,
    normalized_facts,
    projection,
    usage_attribution,
)
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, ProviderFrame
from tests.fixtures.provider_frames import provider_frame
from tests.unit.frames.test_provider_lanes import (
    claude_observations,
    codex_app_observations,
    persisted,
)

K = FrameKind


async def claude_frames() -> list[ProviderFrame]:
    frames, _store, _handle = await persisted(
        claude_observations(UnknownKindCounter()), LaneProfile.CLAUDE_AGENT_SDK, "sess-fixture-1"
    )
    return frames


async def codex_frames() -> list[ProviderFrame]:
    frames, _store, _handle = await persisted(
        codex_app_observations(UnknownKindCounter()), LaneProfile.CODEX, "thr_root"
    )
    return frames


def spawn_only_frames() -> list[ProviderFrame]:
    lane = LaneProfile.CODEX
    spawn = {
        "threadId": "root",
        "item": {"type": "collabAgentToolCall", "id": "spawn-9", "receiverThreadIds": ["quiet"]},
    }
    return [
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


@pytest.mark.parametrize("source", ["claude", "codex", "spawn_only"])
async def test_the_tracker_equals_provider_subordinates_after_every_frame(source: str) -> None:
    frames = {
        "claude": claude_frames,
        "codex": codex_frames,
    }.get(source)
    ordered = await frames() if frames is not None else spawn_only_frames()
    tracker = LineageTracker()
    for count, frame in enumerate(ordered, start=1):
        tracker.observe(frame)
        assert tracker.nodes() == provider_subordinates(ordered[:count]), count
    assert tracker.nodes(), "the fixture has at least one subordinate"


async def test_claude_child_starts_once_widens_and_ends_once_from_native_refs() -> None:
    frames = await claude_frames()
    tracker = LineageTracker()
    transitions: list[tuple[int, tuple[str, ...]]] = []
    for frame in frames:
        change = tracker.observe(frame)
        if change.transitions:
            transitions.append((frame.arrival_ordinal, change.transitions))
            assert frame.subordinate_ref is not None  # parents never move lineage
    flat = [item for _ordinal, items in transitions for item in items]
    assert flat.count("started") == 1 and flat.count("ended") == 1
    assert flat.index("started") < flat.index("ended")
    (node,) = tracker.nodes()
    assert (node.ref.native_child_ref, node.ref.spawn_correlation) == ("task-a1", "toolu_task_01")
    assert (node.ref.visibility, node.lifecycle, node.resolved) == ("full", "ended", True)
    assert node.frame_count == sum(1 for frame in frames if frame.subordinate_ref)


async def test_a_lifecycle_only_child_is_never_reported_with_detail() -> None:
    """Partial transcript access: only the provider's task lifecycle frames exist."""

    frames = [
        frame
        for frame in await claude_frames()
        if frame.subordinate_ref is None or frame.kind in {K.STATUS, K.SESSION_STATE}
    ]
    tracker = LineageTracker()
    for frame in frames:
        tracker.observe(frame)
    (node,) = tracker.nodes()
    assert tracker.nodes() == provider_subordinates(frames)
    # task_started / task_progress / task_notification: lifecycle, ended, nothing more.
    assert (node.ref.visibility, node.lifecycle, node.frame_count) == ("lifecycle_only", "ended", 3)
    assert (node.ref.native_child_ref, node.ref.spawn_correlation) == ("task-a1", "toolu_task_01")


async def test_an_incomplete_tracker_observes_but_never_claims_a_start() -> None:
    frames = await claude_frames()
    first_child = next(index for index, frame in enumerate(frames) if frame.subordinate_ref)
    tracker = LineageTracker(complete=False)
    seen: list[str] = []
    for frame in frames[first_child + 1 :]:
        seen.extend(tracker.observe(frame).transitions)
    assert "started" not in seen and "observed" in seen


def test_spawn_evidence_alone_is_unavailable_and_a_late_spawn_resolves_a_child() -> None:
    lane = LaneProfile.CODEX
    child = provider_frame(
        1,
        K.MESSAGE,
        {"threadId": "late"},
        lane=lane,
        raw_kind="item/completed",
        session="root",
        subordinate_ref="codex:thread:late",
    )
    spawn = provider_frame(
        2,
        K.STATUS,
        {"item": {"type": "collabAgentToolCall", "id": "spawn-1", "receiverThreadIds": ["late"]}},
        lane=lane,
        raw_kind="item/completed",
        session="root",
    )
    tracker = LineageTracker()
    first = tracker.observe(child)
    assert first.transitions == ("started",) and first.nodes[0].resolved is False
    resolved = tracker.observe(spawn)
    assert resolved.transitions == ("updated",)
    assert resolved.nodes[0].resolved is True
    assert resolved.nodes[0].ref.spawn_correlation == "spawn-1"
    assert tracker.nodes() == provider_subordinates([child, spawn])


async def test_projection_normalizes_stable_facts_and_keeps_unknown_kinds_visible() -> None:
    frames = await claude_frames()
    parent_init = next(f for f in frames if f.kind == K.SESSION_INIT and not f.subordinate_ref)
    assert normalized_facts(parent_init) == ("execution.started",)
    parent_result = next(f for f in frames if f.kind == K.RUN_RESULT and not f.subordinate_ref)
    assert normalized_facts(parent_result) == ("execution.ended",)
    child_tool = next(f for f in frames if f.kind == K.TOOL_CALL_COMPLETED and f.subordinate_ref)
    assert normalized_facts(child_tool, ("started",)) == ("tool.completed", "subordinate.started")
    child_result = provider_frame(
        9, K.RUN_RESULT, {"subtype": "success"}, subordinate_ref="claude:task:x"
    )
    # A child's result is not the execution's end.
    assert normalized_facts(child_result, ("ended",)) == ("subordinate.ended",)
    unknown = provider_frame(10, K.UNKNOWN, {"kind": "vendor.new_thing"}, raw_kind="vendor.new")
    body = projection(unknown)
    assert body["normalized"] == [] and body["known_kind"] is False
    delta = provider_frame(11, K.MESSAGE_DELTA, {"text": "x"})
    assert normalized_facts(delta) == ()
    assert set(NORMALIZED_FACTS) >= {
        "execution.started",
        "execution.ended",
        "tool.started",
        "tool.completed",
        "tool.failed",
        "approval.pending",
        "approval.resolved",
        "compaction.observed",
        "usage",
        "subordinate.started",
        "subordinate.ended",
    }


@pytest.mark.parametrize(
    ("lane", "tokens", "counted"),
    [
        (LaneProfile.CLAUDE_AGENT_SDK, "unattributable", False),
        (LaneProfile.CODEX, "folded", True),
        (LaneProfile.DEEP_AGENTS, "folded", True),
    ],
)
def test_usage_attribution_never_invites_double_counting(
    lane: LaneProfile, tokens: str, counted: bool
) -> None:
    parent = provider_frame(1, K.USAGE, {"input_tokens": 1}, lane=lane)
    child = provider_frame(2, K.USAGE, {"input_tokens": 1}, lane=lane, subordinate_ref="c")
    assert usage_attribution(parent) == {
        "scope": "parent",
        "counted_in_parent_turn": True,
        "add_to_parent": False,
    }
    attribution: dict[str, Any] | None = usage_attribution(child)
    assert attribution is not None
    assert attribution["tokens"] == tokens
    assert attribution["counted_in_parent_turn"] is counted
    assert attribution["add_to_parent"] is False
    assert usage_attribution(provider_frame(3, K.MESSAGE, {})) is None
