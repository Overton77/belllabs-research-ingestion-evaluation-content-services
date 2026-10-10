"""Per-execution and per-mission cursor domains, chain targets and frame retention expiry
(additive `mc.stream_subscription.v1`; SPEC-04 replay step 3). In-memory FIXTURE source."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from mission_control.application.streams.flow import ConnectionBudget, InflightWindow
from mission_control.application.streams.ports import ChainState, LinkedMission
from mission_control.application.streams.pump import SubscriptionPump
from mission_control.application.streams.service import StreamFailure
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.subscriptions.streams import (
    MAX_STREAM_CURSORS,
    StreamCursor,
    StreamSubscription,
    StreamTarget,
)
from tests.fixtures.provider_frames import FIXTURE_HARNESS
from tests.unit.streams.fakes import MISSION, FakeStreamSource
from tests.unit.streams.test_stream_lineage import run_pump, stop
from tests.unit.streams.test_stream_pump import FAST, Recorder, until
from tests.unit.streams.test_stream_service import (
    READER,
    frames_at,
    mission_at,
    service,
    subscription,
)

K = FrameKind
SECOND = UUID("7d4f0c1e-1111-4a2b-9c3d-0000000000b2")
THIRD = UUID("7d4f0c1e-1111-4a2b-9c3d-0000000000b3")


def keyed(mission: UUID, seq: int) -> StreamCursor:
    return StreamCursor(stream="mission_events", position=f"{mission}:{seq}")


# --- contract --------------------------------------------------------------------------------


def contract(**extra: Any) -> StreamSubscription:
    body: dict[str, Any] = {
        "subscription_id": "s",
        "request_id": "r",
        "scope": {"installation_id": "i", "application_id": "biotech", "tenant_id": "t"},
        "target": {"kind": "run", "id": "run-1"},
        "streams": ["mission_events", "provider_frames"],
    }
    body.update(extra)
    return StreamSubscription.model_validate(body)


def test_v1_requests_validate_unchanged_and_cursor_domains_are_keyed() -> None:
    # A request with at most one cursor per stream validates exactly as before.
    old = contract(cursors=[mission_at(3).model_dump(), frames_at(2).model_dump()])
    assert [cursor.key for cursor in old.cursors] == ["", str(FIXTURE_HARNESS)]
    # Additive: one frame cursor per execution, one mission cursor per member mission.
    many = contract(
        cursors=[frames_at(1).model_dump(), frames_at(4, execution=SECOND).model_dump()]
    )
    assert len(many.cursors) == 2
    with pytest.raises(ValidationError, match="at most one cursor"):
        contract(cursors=[frames_at(1).model_dump(), frames_at(2).model_dump()])
    with pytest.raises(ValidationError, match="at most one cursor"):
        contract(cursors=[mission_at(1).model_dump(), mission_at(2).model_dump()])
    with pytest.raises(ValidationError, match="cursors"):
        contract(
            cursors=[
                frames_at(1, execution=uuid4()).model_dump() for _ in range(MAX_STREAM_CURSORS + 1)
            ]
        )
    with pytest.raises(ValidationError, match="per mission, run or execution"):
        contract(target={"kind": "chain", "id": "c"})


# --- run and mission targets carry provider frames -------------------------------------------


def run_frames(svc: Any, *cursors: StreamCursor, kind: str = "run", **extra: Any) -> Any:
    target = {"run": "run-fixture", "mission": str(MISSION)}[kind]
    return subscription(
        svc, kind=kind, target_id=target, streams=("provider_frames",), cursors=cursors, **extra
    )


async def test_run_targets_read_every_execution_in_its_own_cursor_domain() -> None:
    source = FakeStreamSource()
    source.commit_frames(K.TURN_STARTED, K.MESSAGE, K.TURN_ENDED)
    source.commit_other_frames(SECOND, K.TURN_STARTED, K.TURN_ENDED)
    svc, _ = service(source)
    fresh = await svc.open(run_frames(svc), READER)
    assert fresh.channels() == (
        f"provider_frames:{FIXTURE_HARNESS}",
        f"provider_frames:{SECOND}",
    )
    # No cursor at all: every execution starts at its own high-watermark behind a snapshot.
    assert [notice.reason for notice in fresh.snapshots] == ["no_cursor", "no_cursor"]
    assert set(fresh.ack.high_watermarks) == {frames_at(3), frames_at(2, execution=SECOND)}
    assert fresh.ack.snapshot_versions["provider_frames.executions"] == "2"
    # Resuming with a cursor for one execution: the other is new to the client, from start.
    resumed = await svc.open(run_frames(svc, frames_at(1)), READER)
    assert resumed.snapshots == ()
    assert set(resumed.ack.replay_from) == {frames_at(1), frames_at(0, execution=SECOND)}
    for channel in resumed.channels():
        while (page := await svc.next_page(resumed, channel, limit=10)).items:
            resumed.positions[channel].after = page.scanned_to
            resumed.positions[channel].sent = page.scanned_to
    acked = svc.acknowledge(resumed, (frames_at(3), frames_at(2, execution=SECOND)))
    assert acked == {f"provider_frames:{FIXTURE_HARNESS}": 3, f"provider_frames:{SECOND}": 2}
    assert set(svc.acked_cursors(resumed)) == {frames_at(3), frames_at(2, execution=SECOND)}
    # A cursor for an execution outside the run (forged or foreign) is refused.
    with pytest.raises(StreamFailure) as raised:
        await svc.open(run_frames(svc, frames_at(0, execution=uuid4())), READER)
    assert raised.value.code == "SCOPE_MISMATCH"


async def test_executions_that_start_later_are_discovered_and_replayed_from_their_start() -> None:
    source = FakeStreamSource()
    source.commit_frames(K.TURN_STARTED)
    svc, _ = service(source)
    opened = await svc.open(run_frames(svc, frames_at(0), kind="mission"), READER)
    recorder = Recorder()
    discovered: list[tuple[UUID, ...]] = []
    window = InflightWindow(1_048_576, ConnectionBudget(1_048_576))
    pump = SubscriptionPump(
        svc,
        opened,
        recorder,
        window,
        FAST,
        authorized=lambda: True,
        on_executions=discovered.append,
    )
    task = asyncio.create_task(pump.run())
    await until(recorder, lambda: len(recorder.envelopes) == 1)
    source.commit_other_frames(THIRD, K.TURN_STARTED, K.MESSAGE, K.TURN_ENDED)
    await until(recorder, lambda: len(recorder.envelopes) == 4)
    await stop(task)
    assert discovered == [(THIRD,)]
    third = [e for e in recorder.envelopes if e.execution_ref == f"harness_execution:{THIRD}"]
    assert [e.cursor.position for e in third] == [f"{THIRD}:1", f"{THIRD}:2", f"{THIRD}:3"]


# --- chain targets ---------------------------------------------------------------------------


def chain_source() -> tuple[FakeStreamSource, ChainState, UUID]:
    source = FakeStreamSource()
    downstream = uuid4()
    link = LinkedMission(
        link_id=uuid4(),
        link_key="ingest",
        from_mission_id=MISSION,
        to_mission_id=downstream,
        state="armed",
    )
    chain = ChainState(
        chain_id=uuid4(),
        chain_key="research-ingest",
        lifecycle="running",
        phase="releasing",
        terminal_outcome=None,
        version=1,
        links=(link,),
    )
    source.chains[chain.chain_id] = chain
    source.commit_events(2)
    source.commit_member_events(downstream, 3)
    return source, chain, downstream


def chain_sub(svc: Any, target: str, *cursors: StreamCursor, **extra: Any) -> Any:
    return subscription(svc, kind="chain", target_id=target, cursors=cursors, **extra)


async def test_chain_targets_replay_every_member_journal_with_keyed_cursors() -> None:
    source, chain, downstream = chain_source()
    svc, _ = service(source)
    opened = await svc.open(chain_sub(svc, chain.chain_key, keyed(MISSION, 1)), READER)
    members = sorted([MISSION, downstream])
    assert opened.channels() == tuple(f"mission_events:{m}" for m in members)
    assert opened.ack.snapshot_versions["chain.lifecycle"] == "running"
    assert opened.ack.snapshot_versions["mission_events.members"] == "2"
    # The member without a cursor starts at its high-watermark behind a snapshot.
    assert [n.covered for n in opened.snapshots] == [keyed(downstream, 3)]
    page = await svc.next_page(opened, f"mission_events:{MISSION}", limit=10)
    assert [item.envelope.cursor for item in page.items] == [keyed(MISSION, 2)]
    assert svc.chain_body(opened)["links"][0]["state"] == "armed"
    assert {node.ref.kind for node in svc.lineage_nodes(opened)} == {"linked_mission"}
    for forged in (mission_at(1), keyed(uuid4(), 1)):
        with pytest.raises(StreamFailure) as raised:
            await svc.open(chain_sub(svc, str(chain.chain_id), forged), READER)
        assert raised.value.code == "SCOPE_MISMATCH"
    foreign, _ = service(FakeStreamSource(request_scope=source.request_scope))
    with pytest.raises(StreamFailure) as raised:
        await foreign.open(chain_sub(foreign, str(chain.chain_id)), READER)
    assert raised.value.code == "TARGET_NOT_FOUND"


async def test_a_chain_lifecycle_change_is_reported_with_the_next_member_event() -> None:
    source, chain, downstream = chain_source()
    svc, _ = service(source)
    opened = await svc.open(
        chain_sub(svc, str(chain.chain_id), keyed(MISSION, 2), keyed(downstream, 3)), READER
    )
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    await asyncio.sleep(0.15)
    released = chain.links[0].__class__(**{**chain.links[0].__dict__, "state": "released"})
    source.chains[chain.chain_id] = ChainState(
        **{**chain.__dict__, "version": 2, "phase": "draining", "links": (released,)}
    )
    source.commit_member_events(downstream, 1)
    await until(recorder, lambda: bool(recorder.lineages))
    await stop(task)
    notice = recorder.lineages[-1]
    assert notice["chain"]["version"] == 2 and notice["chain"]["phase"] == "draining"
    assert notice["chain"]["links"][0]["state"] == "released"
    assert [e.cursor for e in recorder.envelopes] == [keyed(downstream, 4)]


# --- frame retention -------------------------------------------------------------------------


def execution_frames(svc: Any, cursor: StreamCursor) -> Any:
    return subscription(
        svc,
        kind="execution",
        target_id=str(FIXTURE_HARNESS),
        streams=("provider_frames",),
        cursors=(cursor,),
    )


async def test_a_cursor_behind_retention_is_expired_with_a_snapshot_at_the_high_watermark() -> None:
    source = FakeStreamSource()
    source.commit_frames(K.TURN_STARTED, K.MESSAGE_DELTA, K.MESSAGE_DELTA, K.TURN_ENDED)
    svc, _ = service(source)
    # Fresh executions: an ordinal gap (a deduplicated key) is not expiry.
    source.stored_frames[1] = [f for f in source.stored_frames[1] if f.arrival_ordinal != 2]
    plain = await svc.open(execution_frames(svc, frames_at(1)), READER)
    assert plain.snapshots == () and plain.ack.gap is False
    # Retention deleted the frames after the cursor of an old execution.
    source.expire_frames(2, 3)
    expired = await svc.open(execution_frames(svc, frames_at(1)), READER)
    assert [(n.reason, n.covered) for n in expired.snapshots] == [("cursor_expired", frames_at(4))]
    assert expired.ack.gap is True and expired.ack.replay_from == (frames_at(4),)
    # The cursor's own frame is gone: expired too (not a silent partial replay).
    gone = await svc.open(execution_frames(svc, frames_at(3)), READER)
    assert gone.snapshots[0].reason == "cursor_expired"
    # A cursor past H stays CURSOR_AHEAD unless retention can explain it.
    source.retention_horizon_passed = False
    with pytest.raises(StreamFailure) as raised:
        await svc.open(execution_frames(svc, frames_at(9)), READER)
    assert raised.value.code == "CURSOR_AHEAD"


async def test_a_live_subscription_behind_retention_is_resynced_at_the_high_watermark() -> None:
    source = FakeStreamSource()
    source.commit_frames(*([K.MESSAGE] * 6))
    svc, _ = service(source)
    opened = await svc.open(execution_frames(svc, frames_at(1)), READER)
    source.expire_frames(2, 3, 4)
    recorder = Recorder()
    task = await run_pump(svc, opened, recorder)
    await until(recorder, lambda: bool(recorder.resyncs))
    source.commit_frames(K.TURN_ENDED)
    await until(recorder, lambda: any(e.kind == "turn_ended" for e in recorder.envelopes))
    await stop(task)
    notice = recorder.resyncs[0]
    assert (notice["code"], notice["detached"]) == ("CURSOR_EXPIRED", False)
    assert notice["replay_from"] == [frames_at(6).model_dump(mode="json")]
    # Nothing behind the high-watermark was delivered after the gap; the next frame was.
    assert [e.cursor for e in recorder.envelopes] == [frames_at(7)]


def test_stream_targets_accept_chain_kind() -> None:
    assert StreamTarget(kind="chain", id="c").kind == "chain"
