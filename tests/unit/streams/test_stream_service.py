"""MissionStreamService: authorization, cursor planning, alias filters and acks (fixture
source; see `fakes.py`)."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from mission_control.application.streams.service import (
    FULL_DETAIL_PERMISSION,
    MissionStreamService,
    StreamFailure,
)
from mission_control.application.subscriptions.aliases import derived_event_id
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamFilters,
    StreamSubscription,
    StreamTarget,
)
from tests.fixtures.provider_frames import FIXTURE_HARNESS, OTHER_SCOPE, SCOPE
from tests.unit.streams.fakes import MISSION, RUN_KEY, FakeStreamSource

READER = ActorContext(actor_id="viewer", permissions=frozenset({"workflow_run.read"}))
FULL_READER = ActorContext(
    actor_id="auditor",
    permissions=frozenset({"workflow_run.read", FULL_DETAIL_PERMISSION}),
)


def service(source: FakeStreamSource | None = None) -> tuple[MissionStreamService, Any]:
    source = source or FakeStreamSource()
    return MissionStreamService(source, request_scope=source.request_scope), source


def subscription(
    svc: MissionStreamService,
    *,
    kind: str = "run",
    target_id: str = RUN_KEY,
    streams: tuple[str, ...] = ("mission_events",),
    cursors: tuple[StreamCursor, ...] = (),
    filters: StreamFilters | None = None,
    **extra: Any,
) -> StreamSubscription:
    return StreamSubscription(
        subscription_id=f"sub-{uuid4()}",
        request_id="req-1",
        scope=svc.scope,
        target=StreamTarget(kind=kind, id=target_id),  # type: ignore[arg-type]
        streams=streams,  # type: ignore[arg-type]
        cursors=cursors,
        filters=filters or StreamFilters(),
        **extra,
    )


def mission_at(seq: int) -> StreamCursor:
    return StreamCursor(stream="mission_events", position=str(seq))


def frames_at(ordinal: int, generation: int | None = 1, execution: Any = None) -> StreamCursor:
    return StreamCursor(
        stream="provider_frames",
        position=f"{execution or FIXTURE_HARNESS}:{ordinal}",
        generation=generation,
    )


async def drain(
    svc: MissionStreamService, opened: Any, stream: str = "mission_events", limit: int = 3
) -> list:
    items = []
    while True:
        page = await svc.next_page(opened, stream, limit=limit)  # type: ignore[arg-type]
        assert page.resync is None
        position = opened.positions[stream]
        if page.scanned_to <= position.after:
            return items
        items.extend(page.items)
        position.after = page.scanned_to


async def test_no_cursor_starts_at_the_high_watermark_behind_a_snapshot() -> None:
    svc, source = service()
    source.commit_events(5)
    opened = await svc.open(subscription(svc), READER)
    assert opened.ack.replay_from == (mission_at(5),)
    assert opened.ack.high_watermarks == (mission_at(5),)
    assert opened.ack.gap is False
    assert [notice.reason for notice in opened.snapshots] == ["no_cursor"]
    assert opened.ack.snapshot_ref == f"mc://applications/biotech/runs/{RUN_KEY}/inspection"
    assert await drain(svc, opened) == []
    source.commit_events(2)
    assert [item.position for item in await drain(svc, opened)] == [6, 7]


async def test_replay_covers_cursor_to_high_watermark_in_pages_without_duplicates() -> None:
    svc, source = service()
    source.commit_events(10)
    opened = await svc.open(subscription(svc, cursors=(mission_at(4),)), READER)
    assert opened.snapshots == () and opened.ack.snapshot_ref is None
    items = await drain(svc, opened)
    assert [item.position for item in items] == [5, 6, 7, 8, 9, 10]
    envelope = items[0].envelope
    assert envelope.stream == "mission_events" and envelope.scope == svc.scope
    assert envelope.payload["schema_version"] == "mc.event.v1"
    assert envelope.payload_ref is None and envelope.mission_ref == f"mission:{MISSION}"


@pytest.mark.parametrize(
    ("cursor", "code"),
    [
        (StreamCursor(stream="mission_events", position="9"), "CURSOR_AHEAD"),
        (StreamCursor(stream="mission_events", position="not-a-seq"), "CURSOR_EXPIRED"),
    ],
)
async def test_forged_mission_cursors_are_typed_errors(cursor: StreamCursor, code: str) -> None:
    svc, source = service()
    source.commit_events(3)
    with pytest.raises(StreamFailure) as raised:
        await svc.open(subscription(svc, cursors=(cursor,)), READER)
    assert raised.value.code == code


async def test_public_alias_filters_receive_the_derived_notification_once() -> None:
    svc, source = service()
    source.commit_events(2)
    source.commit_events(1, "workflow_run.terminalize")
    opened = await svc.open(
        subscription(
            svc, cursors=(mission_at(0),), filters=StreamFilters(kinds=("run.completed",))
        ),
        READER,
    )
    items = await drain(svc, opened)
    assert [(item.position, item.envelope.kind) for item in items] == [(3, "run.completed")]
    canonical = source.events[2].event_id
    assert items[0].envelope.event_id == str(derived_event_id("run.completed", canonical))
    assert items[0].envelope.causation_id == f"mission_event:{canonical}"


async def test_authorization_and_scope_are_checked_before_any_read() -> None:
    svc, _source = service()
    nobody = ActorContext(actor_id="nobody")
    with pytest.raises(StreamFailure) as raised:
        await svc.open(subscription(svc), nobody)
    assert raised.value.code == "UNAUTHORIZED"
    full = StreamFilters(tool_detail="full")
    with pytest.raises(StreamFailure) as raised:
        await svc.open(subscription(svc, filters=full), READER)
    assert raised.value.code == "UNAUTHORIZED"
    assert (await svc.open(subscription(svc, filters=full), FULL_READER)).ack
    foreign = subscription(svc).model_copy(
        update={"scope": svc.scope.model_copy(update={"tenant_id": str(uuid4())})}
    )
    with pytest.raises(StreamFailure) as raised:
        await svc.open(foreign, READER)
    assert raised.value.code == "SCOPE_MISMATCH"


async def test_foreign_and_absent_targets_look_the_same() -> None:
    other, _ = service(FakeStreamSource(request_scope=OTHER_SCOPE))
    svc, _source = service()
    for candidate, target in ((other, RUN_KEY), (svc, "run-absent")):
        with pytest.raises(StreamFailure) as raised:
            await candidate.open(subscription(candidate, target_id=target), READER)
        assert (raised.value.code, raised.value.detail) == ("TARGET_NOT_FOUND", "target not found")


async def test_run_targets_carry_frames_and_chains_and_unknown_filters_are_typed() -> None:
    svc, _source = service()
    # Additive v1: a run target now carries provider frames, one domain per execution.
    opened = await svc.open(subscription(svc, streams=("provider_frames",)), READER)
    assert opened.channels() == (f"provider_frames:{FIXTURE_HARNESS}",)
    with pytest.raises(StreamFailure) as raised:
        await svc.open(subscription(svc, filters=StreamFilters(kinds=("Bad Kind!",))), READER)
    assert raised.value.code == "UNSUPPORTED_FILTER"
    with pytest.raises(StreamFailure) as raised:
        await svc.open(subscription(svc, kind="chain", target_id="chain-1"), READER)
    assert raised.value.code == "TARGET_NOT_FOUND"


def execution_subscription(svc: MissionStreamService, **kwargs: Any) -> StreamSubscription:
    return subscription(
        svc,
        kind="execution",
        target_id=str(FIXTURE_HARNESS),
        streams=("mission_events", "provider_frames"),
        **kwargs,
    )


async def test_frame_cursors_replay_one_execution_and_reject_forgeries() -> None:
    svc, source = service()
    source.commit_events(1)
    source.commit_frames(FrameKind.TURN_STARTED, FrameKind.MESSAGE, FrameKind.TURN_ENDED)
    opened = await svc.open(
        execution_subscription(svc, cursors=(mission_at(1), frames_at(1))), READER
    )
    items = await drain(svc, opened, "provider_frames")
    assert [item.position for item in items] == [2, 3]
    assert items[0].envelope.cursor == frames_at(2)
    for forged, code in (
        (frames_at(1, execution=uuid4()), "SCOPE_MISMATCH"),
        (frames_at(9), "CURSOR_AHEAD"),
        (frames_at(1, generation=2), "CURSOR_AHEAD"),
        (StreamCursor(stream="provider_frames", position="garbage"), "CURSOR_EXPIRED"),
    ):
        with pytest.raises(StreamFailure) as raised:
            await svc.open(execution_subscription(svc, cursors=(forged,)), READER)
        assert raised.value.code == code, forged


async def test_a_relaunched_execution_replays_the_new_generation_with_a_gap() -> None:
    svc, source = service()
    source.commit_frames(FrameKind.TURN_STARTED, FrameKind.MESSAGE)
    source.generation = 2
    source.commit_frames(FrameKind.TURN_STARTED, generation=2)
    opened = await svc.open(execution_subscription(svc, cursors=(frames_at(1),)), READER)
    assert opened.ack.gap is True
    assert [notice.reason for notice in opened.snapshots if notice.stream == "provider_frames"] == [
        "stale_generation"
    ]
    items = await drain(svc, opened, "provider_frames")
    assert [(item.position, item.envelope.generation) for item in items] == [(3, 2)]


async def test_live_generation_change_is_a_resync_notice() -> None:
    svc, source = service()
    source.commit_frames(FrameKind.TURN_STARTED)
    opened = await svc.open(execution_subscription(svc, cursors=(frames_at(0),)), READER)
    source.generation = 2
    page = await svc.next_page(opened, "provider_frames", limit=10)
    assert page.resync is not None and page.resync.code == "STALE_GENERATION"
    assert page.resync.replay_from.generation == 2
    svc.apply_resync(opened, page.resync)
    assert opened.positions["provider_frames"].generation == 2


async def test_descendant_frames_follow_include_descendants_and_deltas_coalesce() -> None:
    svc, source = service()
    source.commit_frames(FrameKind.TURN_STARTED)
    source.commit_frames(FrameKind.MESSAGE, subordinate_ref="codex:thread:child")
    source.commit_frames(FrameKind.MESSAGE_DELTA, FrameKind.MESSAGE_DELTA, FrameKind.MESSAGE_DELTA)
    source.commit_frames(FrameKind.TURN_ENDED)
    parent_only = await svc.open(execution_subscription(svc, cursors=(frames_at(0),)), READER)
    kinds = [item.envelope.kind for item in await drain(svc, parent_only, "provider_frames")]
    assert kinds == ["turn_started", "turn_ended"]  # deltas excluded by default
    everything = await svc.open(
        execution_subscription(
            svc,
            cursors=(frames_at(0),),
            include_descendants=True,
            filters=StreamFilters(exclude_deltas=False),
        ),
        READER,
    )
    # Coalescing joins consecutive deltas within one read page.
    items = await drain(svc, everything, "provider_frames", limit=10)
    assert [item.envelope.kind for item in items] == [
        "turn_started",
        "message",
        "message_delta",
        "turn_ended",
    ]
    assert items[1].envelope.subordinate_ref is not None
    delta = items[2]
    assert delta.envelope.coalesced == 3 and delta.position == 5
    assert delta.envelope.cursor == frames_at(5)


async def test_acks_are_monotone_and_bounded_by_what_the_server_sent() -> None:
    svc, source = service()
    source.commit_events(4)
    opened = await svc.open(subscription(svc, cursors=(mission_at(1),)), READER)
    assert svc.acknowledge(opened, (mission_at(3),)) == {"mission_events": 3}
    assert svc.acknowledge(opened, (mission_at(2),)) == {"mission_events": 3}
    with pytest.raises(StreamFailure) as raised:
        svc.acknowledge(opened, (mission_at(5),))
    assert raised.value.code == "CURSOR_AHEAD"
    with pytest.raises(StreamFailure) as raised:
        svc.acknowledge(opened, (frames_at(1),))
    assert raised.value.code == "SCOPE_MISMATCH"
    assert svc.acked_cursors(opened) == (mission_at(3),)


def test_service_and_source_scopes_must_agree() -> None:
    with pytest.raises(ValueError, match="scope"):
        MissionStreamService(FakeStreamSource(), request_scope=OTHER_SCOPE)
    assert MissionStreamService(FakeStreamSource(), request_scope=SCOPE).scope.application_id == (
        "biotech"
    )
