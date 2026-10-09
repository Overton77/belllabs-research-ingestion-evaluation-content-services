"""MP-06 on a disposable PostgreSQL 17: the session owner and the native dispatch journal on
`harness_execution.native_identity` (runtime role, forced RLS, no new migration).

- One owner holds a session; a live foreign claim is refused, an expired one is taken over
  with a higher epoch, and the fenced-out owner can neither journal nor settle.
- Frame appends (which rewrite `native_identity` under the same row lock) keep the journal.
- A send the provider accepted before the local receipt was lost is reconciled from the
  persisted journal and never sent twice; an unreconcilable one parks `in_doubt`.

The provider is a labelled fixture (`tests/fixtures/mp06_lanes.py`), not a live provider.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from mission_control.adapters.postgres.lanes.execution_state import DISPATCH_KEY, OWNER_KEY
from mission_control.application.execution.harness.dispatch import (
    DispatchRecord,
    SessionOwnedElsewhere,
    StaleSessionOwner,
)
from mission_control.application.execution.harness.lane_turns import execution_start
from mission_control.application.frames.kinds import cursor_local_key
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import FrameKind, FrameObservation
from tests.fixtures.lane_turns import RecordingSignals, ScriptedSessionLane, scripted_frames
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.mp06_lanes import LossyReceiptLane, ReconcilingLossyLane
from tests.integration.postgres.mp06_common import pg_lane_unit
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.harness.test_mp06_dispatch_recovery import turn_request

pytestmark = pytest.mark.common_db
LEASE = timedelta(seconds=30)


async def _identity_json(db: CommonDatabase, heid: object) -> dict[str, object]:
    owner = await asyncpg.connect(db.owner_dsn)
    try:
        raw = await owner.fetchval(
            "SELECT native_identity FROM mission_control.harness_execution "
            "WHERE harness_execution_id = $1",
            heid,
        )
    finally:
        await owner.close()
    return json.loads(raw) if isinstance(raw, str) else dict(raw)


async def test_ownership_and_the_journal_are_fenced_and_survive_frame_appends(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        unit = await pg_lane_unit(pool, common_db, ScriptedSessionLane(frames=scripted_frames()))
        identity = unit.identity
        scope, heid = identity.request_scope, identity.harness_execution_id
        handle = await unit.frames.open_execution(
            execution_start(unit.operation, identity, 1, "agent-1")
        )
        now = datetime.now(UTC)
        a = await unit.states.claim_owner(
            scope, heid, owner_ref="worker-a", generation=1, now=now, lease=LEASE
        )
        with pytest.raises(SessionOwnedElsewhere):
            await unit.states.claim_owner(
                scope, heid, owner_ref="worker-b", generation=1, now=now, lease=LEASE
            )
        record = DispatchRecord(
            kind="send",
            idempotency_key=unit.send_key,
            expected_generation=1,
            instruction_digest="sha256:" + "a" * 64,
            owner_ref=a.owner_ref,
            owner_epoch=a.epoch,
            intended_at=now,
        )
        claim = await unit.states.intend_dispatch(scope, heid, record, owner=a)
        assert claim.fresh
        assert not (await unit.states.intend_dispatch(scope, heid, record, owner=a)).fresh

        # A frame append rewrites native_identity under the same row lock: the journal stays.
        await FrameWriter(unit.frames, handle).write(
            [
                FrameObservation(
                    provider_key=cursor_local_key("run-1:0"),
                    raw_kind="turn_started",
                    kind=FrameKind.TURN_STARTED,
                    body={"runId": "run-1"},
                    native_turn_ref="run-1",
                    native_session_ref="agent-1",
                )
            ]
        )
        stored = await _identity_json(common_db, heid)
        assert OWNER_KEY in stored and DISPATCH_KEY in stored
        assert "open_turn" in stored, "the frame writer's own keys are kept too"
        state = await unit.states.load(scope, heid)
        assert state is not None and state.owner == a
        pending = state.dispatch("send", unit.send_key)
        assert pending is not None and pending.phase == "intended"

        # Expiry takeover: a higher epoch fences the old owner out of every write.
        b = await unit.states.claim_owner(
            scope,
            heid,
            owner_ref="worker-b",
            generation=1,
            now=a.lease_expires_at + timedelta(seconds=1),
            lease=LEASE,
        )
        assert (b.epoch, b.previous_owner_ref) == (2, "worker-a")
        with pytest.raises(StaleSessionOwner):
            await unit.states.assert_owner(scope, heid, a)
        with pytest.raises(StaleSessionOwner):
            await unit.states.resolve_dispatch(
                scope,
                heid,
                "send",
                unit.send_key,
                outcome="acknowledged",
                owner=a,
                at=now,
                native_ref="run-1",
            )
        with pytest.raises(StaleSessionOwner):
            await unit.states.renew_owner(scope, heid, a, expires_at=now + LEASE)
        acked = await unit.states.resolve_dispatch(
            scope,
            heid,
            "send",
            unit.send_key,
            outcome="acknowledged",
            owner=b,
            at=now,
            native_ref="run-1",
        )
        assert acked.phase == "acknowledged" and acked.owner_epoch == 2

        # Another tenant sees nothing.
        other = await unit.states.load(common_db.scope("tenant-2"), heid)
        assert other is None
    finally:
        await pool.close()


async def test_a_lost_receipt_is_reconciled_from_the_persisted_journal(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        lane = ReconcilingLossyLane(frames=scripted_frames(), loss="raise")
        unit = await pg_lane_unit(pool, common_db, lane)
        request = turn_request(unit.operation)
        with pytest.raises(ConnectionError):
            await unit.service("worker-a").turn(request, RecordingSignals(unit.stack.frames))
        scope, heid = unit.identity.request_scope, unit.identity.harness_execution_id
        stored = await _identity_json(common_db, heid)
        journal = stored[DISPATCH_KEY]
        assert isinstance(journal, dict)
        assert journal[f"send:{unit.send_key}"]["phase"] == "intended"

        # Temporal's retry of the attempt, from the journal persisted in PostgreSQL by a
        # fresh service instance: reconcile, never resend.
        result = await unit.service("worker-a").turn(request, RecordingSignals(unit.stack.frames))
        assert result.operation_result is not None
        assert result.operation_result["status"] == "completed"
        assert lane.native_sends == [unit.send_key]
        state = await unit.states.load(scope, heid)
        assert state is not None and state.native_turn_ref == "run-fake-1"
        record = state.dispatch("send", unit.send_key)
        assert record is not None and record.phase == "acknowledged" and record.attempts == 1
    finally:
        await pool.close()


async def test_an_unreconcilable_dispatch_parks_in_doubt_with_one_send(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        lane = LossyReceiptLane(frames=scripted_frames(), loss="raise")
        unit = await pg_lane_unit(pool, common_db, lane)
        request = turn_request(unit.operation)
        turns = unit.service("worker-a")
        with pytest.raises(ConnectionError):
            await turns.turn(request, RecordingSignals(unit.stack.frames))
        parked = await turns.turn(request, RecordingSignals(unit.stack.frames))
        assert parked.operation_result is not None
        assert parked.operation_result["status"] == "in_doubt"
        assert lane.native_sends == [unit.send_key]
        state = await unit.states.load(
            unit.identity.request_scope, unit.identity.harness_execution_id
        )
        assert state is not None
        record = state.dispatch("send", unit.send_key)
        assert record is not None and (record.phase, record.reason) == (
            "in_doubt",
            "send_dispatch_ambiguous",
        )
    finally:
        await pool.close()
