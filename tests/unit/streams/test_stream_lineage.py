"""Subordinate lineage and the common projection on the live stream (SPEC-04; V16 offline).

The source is the in-memory FIXTURE (`fakes.py`) holding the hand-written Claude Agent SDK
and Codex fixture frames (`tests/unit/frames/fixtures/`, not live recordings) re-keyed to the
fixture execution; PostgreSQL and the socket are proven in
`tests/integration/postgres/test_realtime_acceptance_postgres.py`.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

from mission_control.application.frames.lineage import provider_subordinates
from mission_control.application.streams.flow import ConnectionBudget, InflightWindow
from mission_control.application.streams.ports import LinkedMission
from mission_control.application.streams.pump import SubscriptionPump
from mission_control.domain.frames.contracts import ProviderFrame
from mission_control.domain.subscriptions.streams import StreamFilters
from tests.fixtures.provider_frames import FIXTURE_HARNESS
from tests.unit.frames.test_lineage_stream import claude_frames, codex_frames
from tests.unit.streams.fakes import MISSION, FakeStreamSource
from tests.unit.streams.test_stream_pump import FAST, Recorder, until
from tests.unit.streams.test_stream_service import (
    READER,
    frames_at,
    mission_at,
    service,
    subscription,
)


def rekeyed(frames: list[ProviderFrame]) -> list[ProviderFrame]:
    return [frame.model_copy(update={"harness_execution_id": FIXTURE_HARNESS}) for frame in frames]


def source_with(frames: list[ProviderFrame]) -> FakeStreamSource:
    source = FakeStreamSource()
    source.stored_frames[1] = rekeyed(frames)
    return source


def frame_sub(svc: Any, cursor: int | None, **extra: Any) -> Any:
    return subscription(
        svc,
        kind="execution",
        target_id=str(FIXTURE_HARNESS),
        streams=("provider_frames",),
        cursors=(frames_at(cursor),) if cursor is not None else (),
        **extra,
    )


async def run_pump(
    svc: Any, opened: Any, recorder: Recorder, *, max_bytes: int = 1_048_576
) -> asyncio.Task[str]:
    window = InflightWindow(max_bytes, ConnectionBudget(max_bytes))
    pump = SubscriptionPump(svc, opened, recorder, window, FAST, authorized=lambda: True)
    return asyncio.create_task(pump.run())


async def stop(task: asyncio.Task[str]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_replay_from_the_start_reports_lineage_and_normalized_child_facts() -> None:
    frames = await claude_frames()
    svc, _source = service(source_with(frames))
    opened = await svc.open(
        frame_sub(
            svc,
            0,
            include_descendants=True,
            filters=StreamFilters(exclude_deltas=False, tool_detail="none"),
        ),
        READER,
    )
    assert opened.ack.snapshot_versions["provider_frames.lineage"] == "complete"
    assert svc.lineage_nodes(opened) == ()  # nothing before the cursor
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    total = len(frames)
    # tool_detail="none" filters tool frames out of delivery, never out of lineage.
    await until(recorder, lambda: any(e.kind == "run_result" for e in recorder.envelopes))
    await stop(task)
    children = [e for e in recorder.envelopes if e.subordinate_ref is not None]
    assert children and all(e.kind != "tool_call_completed" for e in recorder.envelopes)
    normalized = [fact for e in children for fact in e.payload["normalized"]]
    assert normalized.count("subordinate.started") == 1
    assert normalized.count("subordinate.ended") == 1
    # The final ref equals the batch lineage of everything persisted (complete visibility).
    (expected,) = provider_subordinates(rekeyed(frames))
    assert children[-1].subordinate_ref == expected.ref
    assert expected.ref.visibility == "full"
    changed = [node for notice in recorder.lineages for node in notice["subordinates"]]
    assert changed[-1]["lifecycle"] == "ended"
    assert changed[-1]["usage"] == {"tokens": "unattributable", "cost": "provider_inclusive"}
    parent_end = [e for e in recorder.envelopes if e.kind == "run_result" and not e.subordinate_ref]
    assert parent_end[0].payload["normalized"] == ["execution.ended"]
    unknown = [e for e in recorder.envelopes if e.kind == "unknown"]
    assert unknown and unknown[0].payload["known_kind"] is False
    assert unknown[0].payload["raw_kind"] == "future_message_kind"
    assert sum(1 for _ in recorder.envelopes) <= total


async def test_resuming_after_the_child_started_never_fabricates_a_start() -> None:
    frames = await claude_frames()
    first_child = next(f.arrival_ordinal for f in frames if f.subordinate_ref)
    svc, _source = service(source_with(frames))
    opened = await svc.open(frame_sub(svc, first_child + 1, include_descendants=True), READER)
    # Seeded from the execution's first frame: the child is already known and complete.
    (seeded,) = svc.lineage_nodes(opened)
    assert seeded.lifecycle == "started" and seeded.frame_count == 2
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    await until(recorder, lambda: any(e.kind == "run_result" for e in recorder.envelopes))
    await stop(task)
    facts = [fact for e in recorder.envelopes for fact in e.payload["normalized"]]
    assert "subordinate.started" not in facts and facts.count("subordinate.ended") == 1
    (final,) = svc.lineage_nodes(opened)
    assert final == provider_subordinates(rekeyed(frames))[0]


async def test_without_descendants_child_frames_are_withheld_and_no_lineage_is_sent() -> None:
    frames = await codex_frames()
    svc, _source = service(source_with(frames))
    opened = await svc.open(frame_sub(svc, 0), READER)
    assert "provider_frames.lineage" not in opened.ack.snapshot_versions
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    await until(recorder, lambda: any(e.kind == "run_result" for e in recorder.envelopes))
    await stop(task)
    assert recorder.envelopes and all(e.subordinate_ref is None for e in recorder.envelopes)
    assert recorder.lineages == []


async def test_codex_child_usage_is_marked_folded_and_never_added_again() -> None:
    frames = await codex_frames()
    svc, _source = service(source_with(frames))
    opened = await svc.open(
        frame_sub(
            svc,
            0,
            include_descendants=True,
            filters=StreamFilters(exclude_deltas=False),
            delta_coalesce_ms=0,
        ),
        READER,
    )
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    await until(recorder, lambda: any(e.kind == "run_result" for e in recorder.envelopes))
    await stop(task)
    usage = [e for e in recorder.envelopes if e.kind == "usage"]
    child_usage = [e for e in usage if e.subordinate_ref is not None]
    assert child_usage, "the fixture has a child thread usage notification"
    for envelope in child_usage:
        attribution = envelope.payload["usage_attribution"]
        assert attribution["tokens"] == "folded"
        assert attribution["counted_in_parent_turn"] is True
        assert attribution["add_to_parent"] is False
    (node,) = svc.lineage_nodes(opened)
    assert node.ref.spawn_correlation == "item_spawn_1" and node.resolved


async def test_a_blocked_window_rereads_pages_without_double_counting_lineage() -> None:
    frames = await claude_frames()
    svc, _source = service(source_with(frames))
    opened = await svc.open(
        frame_sub(svc, 0, include_descendants=True, filters=StreamFilters(exclude_deltas=False)),
        READER,
    )
    # A window of about two envelopes stops the pump mid-page, so it reads pages again.
    recorder = Recorder()
    window = InflightWindow(2_500, ConnectionBudget(2_500))
    pump = SubscriptionPump(svc, opened, recorder, window, FAST, authorized=lambda: True)
    task = asyncio.create_task(pump.run())
    async with asyncio.timeout(10):
        while not any(e.kind == "run_result" for e in recorder.envelopes):
            if recorder.envelopes:
                pump.acknowledge((recorder.envelopes[-1].cursor,))
            await asyncio.sleep(0.02)
    await stop(task)
    assert recorder.resyncs == []
    (node,) = svc.lineage_nodes(opened)
    assert node.frame_count == sum(1 for frame in frames if frame.subordinate_ref)
    assert node == provider_subordinates(rekeyed(frames))[0]
    facts = [fact for e in recorder.envelopes for fact in e.payload["normalized"]]
    assert facts.count("subordinate.started") == 1 and facts.count("subordinate.ended") == 1


async def test_mission_descendants_are_linked_missions_by_lifecycle_only() -> None:
    source = FakeStreamSource()
    source.commit_events(2)
    released = LinkedMission(
        link_id=uuid4(),
        link_key="ingest",
        from_mission_id=MISSION,
        to_mission_id=uuid4(),
        state="released",
        released_run_key="run-ingest",
    )
    armed = LinkedMission(
        link_id=uuid4(),
        link_key="publish",
        from_mission_id=MISSION,
        to_mission_id=uuid4(),
        state="armed",
    )
    foreign = LinkedMission(
        link_id=uuid4(),
        link_key="elsewhere",
        from_mission_id=uuid4(),
        to_mission_id=MISSION,
        state="released",
        released_run_key="run-upstream",
    )
    source.links = [released, armed, foreign]
    svc, _ = service(source)
    opened = await svc.open(
        subscription(svc, cursors=(mission_at(0),), include_descendants=True), READER
    )
    assert opened.ack.snapshot_versions["mission_events.descendants"] == "lifecycle_only"
    nodes = {node.ref.spawn_correlation: node for node in svc.lineage_nodes(opened)}
    assert set(nodes) == {f"chain_link:{released.link_id}", f"chain_link:{armed.link_id}"}
    ingest = nodes[f"chain_link:{released.link_id}"]
    assert (ingest.ref.kind, ingest.ref.visibility, ingest.ref.native_child_ref) == (
        "linked_mission",
        "lifecycle_only",
        "run:run-ingest",
    )
    assert nodes[f"chain_link:{armed.link_id}"].ref.visibility == "unavailable"
    recorder = Recorder()
    window = InflightWindow(1_048_576, ConnectionBudget(1_048_576))
    pump = SubscriptionPump(svc, opened, recorder, window, FAST, authorized=lambda: True)
    task = asyncio.create_task(pump.run())
    await until(recorder, lambda: recorder.seqs() == [1, 2])
    # The armed link is released later: the next committed page reports the change.
    source.links = [released, LinkedMission(**{**armed.__dict__, "released_run_key": "run-pub"})]
    source.commit_events(1)
    await until(recorder, lambda: bool(recorder.lineages))
    await stop(task)
    (change,) = recorder.lineages[-1]["subordinates"]
    assert change["ref"]["native_child_ref"] == "run:run-pub"
    assert change["ref"]["visibility"] == "lifecycle_only"
    assert change["usage"] == {"tokens": "separate_mission", "cost": "separate_mission"}
    # Descendant journals are never merged into this cursor domain.
    assert all(e.mission_ref == f"mission:{MISSION}" for e in recorder.envelopes)
