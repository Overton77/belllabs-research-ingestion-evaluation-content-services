"""SubscriptionPump: store-driven live delivery, lost hints, slow consumers, expiry and
bounded windows (fixture source; see `fakes.py`)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from mission_control.application.streams.flow import ConnectionBudget, InflightWindow, TokenBucket
from mission_control.application.streams.hub import StreamWakeups, target_keys
from mission_control.application.streams.ports import StreamHint
from mission_control.application.streams.pump import (
    PumpSettings,
    SubscriptionPump,
    envelope_size,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.subscriptions.streams import StreamEnvelope, StreamError
from tests.fixtures.provider_frames import OTHER_SCOPE, SCOPE
from tests.unit.streams.fakes import MISSION, FakeStreamSource
from tests.unit.streams.test_stream_service import (
    READER,
    execution_subscription,
    frames_at,
    mission_at,
    service,
    subscription,
)

FAST = PumpSettings(page_size=2, poll_interval=0.05, slow_consumer_grace=0.3)


class Recorder:
    def __init__(self) -> None:
        self.envelopes: list[StreamEnvelope] = []
        self.resyncs: list[dict[str, Any]] = []
        self.errors: list[StreamError] = []
        self.lineages: list[dict[str, Any]] = []
        self.changed = asyncio.Event()

    async def envelope(self, envelope: StreamEnvelope) -> None:
        self.envelopes.append(envelope)
        self.changed.set()

    async def resync_required(self, notice: dict[str, Any]) -> None:
        self.resyncs.append(notice)
        self.changed.set()

    async def error(self, error: StreamError) -> None:
        self.errors.append(error)
        self.changed.set()

    async def lineage(self, notice: dict[str, Any]) -> None:
        self.lineages.append(notice)
        self.changed.set()

    def seqs(self) -> list[int]:
        return [int(e.cursor.position) for e in self.envelopes if e.stream == "mission_events"]


async def started(
    source: FakeStreamSource,
    *,
    cursors: tuple = (),
    max_bytes: int = 1_048_576,
    settings: PumpSettings = FAST,
    authorized: Any = lambda: True,
    sub: Any = None,
) -> tuple[SubscriptionPump, Recorder, asyncio.Task[str], InflightWindow]:
    svc, _ = service(source)
    request = sub(svc) if sub is not None else subscription(svc, cursors=cursors)
    opened = await svc.open(request, READER)
    recorder = Recorder()
    window = InflightWindow(max_bytes, ConnectionBudget(max_bytes))
    pump = SubscriptionPump(svc, opened, recorder, window, settings, authorized=authorized)
    return pump, recorder, asyncio.create_task(pump.run()), window


async def until(recorder: Recorder, predicate: Any, within: float = 3.0) -> None:
    async with asyncio.timeout(within):
        while not predicate():
            recorder.changed.clear()
            if not predicate():
                await recorder.changed.wait()


async def test_replay_then_live_without_a_hint_is_delivered_by_the_poll() -> None:
    source = FakeStreamSource()
    source.commit_events(5)
    _pump, recorder, task, _window = await started(source, cursors=(mission_at(2),))
    await until(recorder, lambda: recorder.seqs() == [3, 4, 5])
    source.commit_events(3)  # committed, but nobody publishes a hint (lost fanout)
    await until(recorder, lambda: recorder.seqs() == [3, 4, 5, 6, 7, 8])
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert len({e.event_id for e in recorder.envelopes}) == 6


async def test_a_hint_wakes_the_pump_before_the_poll_interval() -> None:
    source = FakeStreamSource()
    slow_poll = PumpSettings(page_size=50, poll_interval=30.0, slow_consumer_grace=30.0)
    pump, recorder, task, _ = await started(source, cursors=(mission_at(0),), settings=slow_poll)
    wakeups = StreamWakeups()
    keys = target_keys(SCOPE, pump.opened.target)
    wakeups.register(keys, pump.wake)
    await asyncio.sleep(0.05)
    source.commit_events(2)
    wakeups.deliver(StreamHint(request_scope=OTHER_SCOPE, mission_id=MISSION))
    await asyncio.sleep(0.1)
    assert recorder.seqs() == []  # another tenant's hint wakes nothing
    wakeups.deliver(StreamHint(request_scope=SCOPE, mission_id=MISSION))
    await until(recorder, lambda: recorder.seqs() == [1, 2], within=1.0)
    wakeups.unregister(keys, pump.wake)
    assert len(wakeups) == 0
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_a_consumer_that_stops_acknowledging_is_detached_with_bounded_memory() -> None:
    source = FakeStreamSource()
    source.commit_events(400)
    _pump, recorder, task, window = await started(
        source, cursors=(mission_at(0),), max_bytes=65_536
    )
    reason = await asyncio.wait_for(task, timeout=5)
    assert reason == "SLOW_CONSUMER"
    sent = sum(envelope_size(e) for e in recorder.envelopes)
    assert sent <= 65_536 + envelope_size(recorder.envelopes[0])
    assert len(recorder.envelopes) < 400
    notice = recorder.resyncs[-1]
    assert notice["code"] == "SLOW_CONSUMER" and notice["detached"] is True
    assert notice["replay_from"] == [mission_at(0).model_dump(mode="json")]
    assert window.used == 0 and window.budget.used == 0


async def test_acknowledging_consumers_receive_everything_through_a_small_window() -> None:
    source = FakeStreamSource()
    source.commit_events(120)
    pump, recorder, task, window = await started(source, cursors=(mission_at(0),), max_bytes=65_536)

    async def ack_loop() -> None:
        while recorder.seqs()[-1:] != [120]:
            if recorder.envelopes:
                pump.acknowledge((recorder.envelopes[-1].cursor,))
            await asyncio.sleep(0.01)

    await asyncio.wait_for(ack_loop(), timeout=10)
    assert recorder.seqs() == list(range(1, 121))
    assert recorder.resyncs == [] and window.used <= 65_536
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_an_expired_credential_stops_delivery() -> None:
    source = FakeStreamSource()
    valid = {"ok": True}
    _pump, recorder, task, _ = await started(
        source, cursors=(mission_at(0),), authorized=lambda: valid["ok"]
    )
    source.commit_events(1)
    await until(recorder, lambda: recorder.seqs() == [1])
    valid["ok"] = False
    source.commit_events(1)
    assert await asyncio.wait_for(task, timeout=2) == "UNAUTHORIZED"
    assert recorder.seqs() == [1]
    assert recorder.errors[-1].code == "UNAUTHORIZED"


async def test_a_failing_store_is_retried_then_reported_not_hidden() -> None:
    source = FakeStreamSource()
    source.commit_events(2)
    _pump, recorder, task, _ = await started(source, cursors=(mission_at(0),))
    source.fail_reads = 1
    await until(recorder, lambda: recorder.seqs() == [1, 2])
    source.fail_reads = 10
    assert await asyncio.wait_for(task, timeout=3) == "UNAVAILABLE"
    assert recorder.resyncs[-1]["code"] == "UNAVAILABLE"
    assert recorder.resyncs[-1]["replay_from"] == [mission_at(0).model_dump(mode="json")]


async def test_a_relaunch_during_live_delivery_resyncs_and_continues() -> None:
    source = FakeStreamSource()
    source.commit_frames(FrameKind.TURN_STARTED)
    _pump, recorder, task, _ = await started(
        source, sub=lambda svc: execution_subscription(svc, cursors=(mission_at(0), frames_at(0)))
    )
    await until(recorder, lambda: len(recorder.envelopes) == 1)
    source.generation = 2
    source.commit_frames(FrameKind.TURN_STARTED, generation=2)
    await until(recorder, lambda: len(recorder.envelopes) == 2)
    assert recorder.resyncs[0]["code"] == "STALE_GENERATION"
    assert recorder.resyncs[0]["detached"] is False
    assert recorder.envelopes[-1].generation == 2
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_token_bucket_and_window_bounds() -> None:
    now = [0.0]
    bucket = TokenBucket(rate=1.0, burst=2, clock=lambda: now[0])
    assert [bucket.take() for _ in range(3)] == [True, True, False]
    now[0] = 1.0
    assert bucket.take() is True
    budget = ConnectionBudget(100)
    first, second = InflightWindow(80, budget), InflightWindow(80, budget)
    assert first.fits(500)  # one envelope always fits an idle connection
    first.add("mission_events", 1, 60)
    assert not second.fits(50) and second.fits(40)
    second.add("mission_events", 1, 40)
    assert budget.used == 100 and not first.fits(1)
    assert first.release("mission_events", 1) == 60 and budget.used == 40
    second.close()
    assert budget.used == 0


def test_pump_settings_are_bounded() -> None:
    with pytest.raises(ValueError):
        PumpSettings(page_size=0)
    with pytest.raises(ValueError):
        PumpSettings(poll_interval=0)
