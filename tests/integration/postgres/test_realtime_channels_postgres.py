"""SPEC-04 follow-up on PostgreSQL 17 + uvicorn + python-socketio (real verifier, registry,
forced RLS, frame writer, retention job): the additive `mc.stream_subscription.v1` cursor
domains.

- `run` / `mission` targets carry provider frames across their executions, one cursor
  domain per execution; a later execution is discovered live; foreign executions are refused.
- `chain` targets replay every member mission's journal with keyed cursors and report the
  chain lifecycle and links; another tenant's chain is absent.
- Frame retention expiry (`PostgresFrameRetention.expire`) is reported as `CURSOR_EXPIRED`
  with a snapshot at the high-watermark on subscribe, and as `resync_required` live.

Provider frames are the deterministic fixtures of `tests/fixtures/provider_frames.py` and
synthetic message frames (not provider recordings).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.retention import PostgresFrameRetention
from mission_control.application.frames.writer import FrameWriter
from mission_control.application.streams.pump import PumpSettings
from mission_control.contracts.realtime import LineageNotice
from mission_control.domain.frames.contracts import FrameKind, FrameObservation, LaneProfile
from mission_control.domain.policies.contracts import CommandStatus, StartAction
from mission_control.interfaces.socketio.server import MissionSocketLimits
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock, turn_observations
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_command, scoped_request
from tests.integration.postgres.test_mission_socket_postgres import auth, stack, subscribe
from tests.integration.postgres.test_realtime_acceptance_postgres import (
    RealtimeClient,
    frames_subscription,
    link_missions,
    mission_of,
    release_link,
)

pytestmark = pytest.mark.common_db


def frame_cursor(execution: Any, ordinal: int, generation: int = 1) -> dict[str, Any]:
    return {
        "stream": "provider_frames",
        "position": f"{execution}:{ordinal}",
        "generation": generation,
    }


def keyed(mission: UUID, seq: int) -> dict[str, Any]:
    return {"stream": "mission_events", "position": f"{mission}:{seq}", "generation": None}


def by_execution(client: RealtimeClient) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for envelope in client.events["provider_frame"]:
        execution, _sep, ordinal = envelope["cursor"]["position"].rpartition(":")
        result.setdefault(execution, []).append(int(ordinal))
    return result


async def test_run_targets_stream_frames_of_every_execution_in_separate_cursor_domains(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        repository = PostgresFrameRepository(s.pool)
        first = await repository.open_execution(s.admitted.start(lane=LaneProfile.DEEP_AGENTS))
        await FrameWriter(repository, first, clock=StepClock()).write(turn_observations())
        second = await repository.open_execution(s.admitted.start(lane=LaneProfile.CODEX))
        await FrameWriter(repository, second, clock=StepClock()).write(turn_observations("t-2"))
        a, b = str(first.harness_execution_id), str(second.harness_execution_id)
        viewer = RealtimeClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        body = subscribe(
            s.admitted.run_key, streams=["provider_frames"], filters={"exclude_deltas": False}
        )
        # Resume one execution from ordinal 3; the other is new to this client (from start).
        body["cursors"] = [frame_cursor(a, 3)]
        reply = await viewer.call("subscribe", body)
        assert reply["ok"] is True, reply
        assert reply["snapshot_versions"]["provider_frames.executions"] == "2"
        assert {c["position"].rpartition(":")[0] for c in reply["replay_from"]} == {a, b}
        total_a = len(turn_observations())
        await viewer.wait_for(
            lambda: (
                len(by_execution(viewer).get(a, [])) == total_a - 3
                and len(by_execution(viewer).get(b, [])) == len(turn_observations("t-2"))
            ),
            within=20,
        )
        seen = by_execution(viewer)
        assert seen[a] == list(range(4, total_a + 1))
        assert seen[b] == sorted(seen[b]) and seen[b][0] == 1
        # Acknowledgements are per execution domain.
        ack = await viewer.call(
            "ack",
            {
                "subscription_id": reply["subscription_id"],
                "cursors": [frame_cursor(a, seen[a][-1]), frame_cursor(b, seen[b][-1])],
            },
        )
        assert ack["ok"] is True, ack
        assert set(ack["acked"]) == {f"provider_frames:{a}", f"provider_frames:{b}"}
        # A third execution of the same run starts later: discovered and streamed from 1.
        third = await repository.open_execution(s.admitted.start(lane=LaneProfile.CLAUDE_AGENT_SDK))
        await FrameWriter(repository, third, clock=StepClock()).write(turn_observations("t-3"))
        c = str(third.harness_execution_id)
        await viewer.wait_for(
            lambda: len(by_execution(viewer).get(c, [])) == len(turn_observations("t-3")),
            within=20,
        )
        assert by_execution(viewer)[c][0] == 1
        # A forged cursor naming an execution outside the run is refused.
        forged = subscribe(s.admitted.run_key, streams=["provider_frames"])
        forged["cursors"] = [frame_cursor(uuid4(), 0)]
        assert (await viewer.call("subscribe", forged))["error"]["code"] == "SCOPE_MISMATCH"
        await viewer.close()

        intruder = RealtimeClient()
        await intruder.connect(s.url, auth(s.token("intruder", tenant="tenant-2")))
        denied = await intruder.call(
            "subscribe", subscribe(s.admitted.run_key, streams=["provider_frames"])
        )
        absent = await intruder.call(
            "subscribe", subscribe("run-absent", streams=["provider_frames"])
        )
        assert denied["error"]["code"] == absent["error"]["code"] == "TARGET_NOT_FOUND"
        await asyncio.sleep(0.3)
        assert intruder.events["provider_frame"] == []
        await intruder.close()


async def chain_of(db: CommonDatabase, link_id: UUID) -> tuple[UUID, str]:
    rows = await owner_rows(
        db,
        "SELECT c.chain_id, c.chain_key FROM mission_control.chain_link l "
        "JOIN mission_control.mission_chain c USING (chain_id) WHERE l.link_id = $1",
        link_id,
    )
    return UUID(str(rows[0]["chain_id"])), str(rows[0]["chain_key"])


async def bump_chain(db: CommonDatabase, chain_id: UUID) -> None:
    connection = await asyncpg.connect(db.owner_dsn)
    try:
        await connection.execute(
            "UPDATE mission_control.mission_chain SET phase = 'draining', "
            "version = version + 1, updated_at = now() WHERE chain_id = $1",
            chain_id,
        )
    finally:
        await connection.close()


async def last_seq_of(db: CommonDatabase, mission: UUID) -> int:
    rows = await owner_rows(
        db, "SELECT last_event_seq FROM mission_control.mission WHERE mission_id = $1", mission
    )
    return int(rows[0]["last_event_seq"])


async def test_chain_targets_replay_member_journals_and_report_the_chain_lifecycle(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with stack(common_db, tmp_path, monkeypatch) as s:
        upstream = await mission_of(s.db, s.admitted.run_key)
        downstream_run = await s.admitted.run_control.admit(
            scoped_request(s.db, request_id=f"chain-down-{uuid4()}")
        )
        assert downstream_run.run_id is not None
        downstream = await mission_of(s.db, downstream_run.run_id)
        link_id = await link_missions(s.db, upstream, downstream)
        chain_id, chain_key = await chain_of(s.db, link_id)
        up_start = await last_seq_of(s.db, upstream)
        down_high = await last_seq_of(s.db, downstream)

        viewer = RealtimeClient()
        await viewer.connect(s.url, auth(s.token("viewer")))
        body = subscribe(s.admitted.run_key)
        body.pop("cursors", None)
        body["target"] = {"kind": "chain", "id": chain_key}
        body["cursors"] = [keyed(upstream, up_start)]
        reply = await viewer.call("subscribe", body)
        assert reply["ok"] is True, reply
        assert reply["snapshot_versions"]["chain.lifecycle"] == "running"
        assert reply["snapshot_versions"]["mission_events.members"] == "2"
        assert keyed(downstream, down_high) in reply["high_watermarks"]
        await viewer.wait_for(
            lambda: bool(viewer.events["lineage"]) and bool(viewer.events["snapshot"])
        )
        listing = LineageNotice.model_validate(viewer.events["lineage"][0])
        assert listing.full and listing.chain is not None
        assert listing.chain["chain_key"] == chain_key and listing.chain["version"] == 1
        assert [n.ref.kind for n in listing.subordinates] == ["linked_mission"]
        assert viewer.events["snapshot"][0]["chain"]["chain_id"] == str(chain_id)

        # Both member journals stream live in their own keyed cursor domains.
        await s.record_usage(2)
        started = await s.admitted.run_control.execute(
            scoped_command(s.db, downstream_run.run_id, 1, f"start-{uuid4()}", StartAction())
        )
        assert started.status == CommandStatus.ACCEPTED
        up_end, down_end = await last_seq_of(s.db, upstream), await last_seq_of(s.db, downstream)

        def positions(mission: UUID) -> list[int]:
            return [
                int(e["cursor"]["position"].rpartition(":")[2])
                for e in viewer.events["mission_event"]
                if e["cursor"]["position"].startswith(f"{mission}:")
            ]

        await viewer.wait_for(
            lambda: (
                positions(upstream) == list(range(up_start + 1, up_end + 1))
                and positions(downstream) == list(range(down_high + 1, down_end + 1))
            ),
            within=15,
        )
        # The chain moves: its next member event carries the new lifecycle and link state.
        await bump_chain(s.db, chain_id)
        await release_link(s.db, link_id, downstream_run.run_id)
        await s.record_usage(1)
        await viewer.wait_for(
            lambda: any(
                (n.get("chain") or {}).get("version", 1) >= 2 for n in viewer.events["lineage"]
            ),
            within=15,
        )
        moved = [
            n for n in viewer.events["lineage"] if (n.get("chain") or {}).get("version", 1) >= 2
        ]
        assert moved[-1]["chain"]["phase"] == "draining"
        assert moved[-1]["chain"]["links"][0]["state"] == "released"
        await viewer.close()

        # Another tenant: the chain is absent, exactly like an unknown chain; a cursor naming a
        # non-member mission is a scope mismatch.
        intruder = RealtimeClient()
        await intruder.connect(s.url, auth(s.token("intruder", tenant="tenant-2")))
        foreign = {**body, "request_id": f"req-{uuid4()}", "cursors": []}
        unknown = {
            **foreign,
            "request_id": f"req-{uuid4()}",
            "target": {"kind": "chain", "id": str(uuid4())},
        }
        denied, missing = (
            await intruder.call("subscribe", foreign),
            await intruder.call("subscribe", unknown),
        )
        assert denied["error"] == {**missing["error"], "request_id": denied["error"]["request_id"]}
        assert denied["error"]["code"] == "TARGET_NOT_FOUND"
        await intruder.close()
        checker = RealtimeClient()
        await checker.connect(s.url, auth(s.token("viewer")))
        stray = {**body, "request_id": f"req-{uuid4()}", "cursors": [keyed(uuid4(), 0)]}
        assert (await checker.call("subscribe", stray))["error"]["code"] == "SCOPE_MISMATCH"
        await checker.close()


def old_messages(count: int, *, size: int = 2_000) -> list[FrameObservation]:
    return [
        FrameObservation(
            provider_key=f"old-{index}",
            raw_kind="fixture.message",
            kind=FrameKind.MESSAGE,
            body={"text": "x" * size, "index": index},
            native_turn_ref="turn-0",
            native_session_ref="thread-1",
        )
        for index in range(count)
    ]


async def test_frame_retention_expiry_is_cursor_expired_with_a_snapshot_and_a_live_resync(
    common_db: CommonDatabase, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    patient = PumpSettings(page_size=50, poll_interval=0.2, slow_consumer_grace=60.0)
    limits = MissionSocketLimits(max_connection_queue_bytes=65_536, pump=patient)
    async with stack(common_db, tmp_path, monkeypatch, limits=limits) as s:
        repository = PostgresFrameRepository(s.pool)
        handle = await repository.open_execution(s.admitted.start())
        long_ago = datetime.now(UTC) - timedelta(days=40)
        writer = FrameWriter(repository, handle, clock=StepClock(long_ago))
        await writer.write(old_messages(120))
        # Recent closing frames after them (always retained).
        await FrameWriter(repository, handle, clock=StepClock(datetime.now(UTC))).write(
            turn_observations()
        )
        execution = str(handle.harness_execution_id)

        # Live: a subscription that lags behind (it has not acknowledged its window yet).
        lagging = RealtimeClient()
        await lagging.connect(s.url, auth(s.token("viewer")))
        live = await lagging.call("subscribe", frames_subscription(execution, 0))
        assert live["ok"] is True, live
        await lagging.wait_for(lambda: len(lagging.events["provider_frame"]) >= 10)
        await asyncio.sleep(0.5)
        received = len(lagging.events["provider_frame"])
        assert received < 120  # blocked by its 64 KiB window

        report = await PostgresFrameRetention(s.pool).expire(s.db.scope(), now=datetime.now(UTC))
        assert report.deleted_non_closing == 120 and report.deleted_closing == 0
        high = 120 + len(turn_observations())

        # Subscribe with a cursor whose frames were deleted: expired, snapshot at H, gap.
        fresh = RealtimeClient()
        await fresh.connect(s.url, auth(s.token("viewer")))
        expired = await fresh.call("subscribe", frames_subscription(execution, 5))
        assert expired["ok"] is True, expired
        assert expired["gap"] is True
        assert expired["replay_from"] == [frame_cursor(execution, high)]
        await fresh.wait_for(lambda: bool(fresh.events["snapshot"]))
        assert fresh.events["snapshot"][0]["reason"] == "cursor_expired"
        assert fresh.events["snapshot"][0]["covered"] == frame_cursor(execution, high)
        await fresh.close()

        # The lagging client acknowledges what it has; the pump finds the deleted range and
        # tells it, then continues at H without pretending the gap was delivered.
        last = lagging.events["provider_frame"][-1]["cursor"]
        assert (
            await lagging.call(
                "ack", {"subscription_id": live["subscription_id"], "cursors": [last]}
            )
        )["ok"]
        await lagging.wait_for(lambda: bool(lagging.events["resync_required"]), within=15)
        notice = lagging.events["resync_required"][0]
        assert (notice["code"], notice["detached"]) == ("CURSOR_EXPIRED", False)
        assert notice["replay_from"] == [frame_cursor(execution, high)]
        ordinals = [
            int(e["cursor"]["position"].rpartition(":")[2])
            for e in lagging.events["provider_frame"]
        ]
        assert max(ordinals) <= 120  # nothing from the deleted range after the resync
        await lagging.close()
