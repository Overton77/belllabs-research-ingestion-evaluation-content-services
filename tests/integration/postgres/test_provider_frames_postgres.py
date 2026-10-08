"""Native Event Store on the common mission_control component (SPEC-03, ticket C1).

Every repository call runs under a restricted login holding one capability role, with
forced row-level security: append idempotency (`ON CONFLICT`), stale-generation fencing,
arrival-ordinal continuation on resume, identity writers in the frame transaction,
cross-scope denial and retention.
"""

from __future__ import annotations

import json
from datetime import timedelta

import asyncpg
import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.retention import (
    FrameRetentionPolicy,
    PostgresFrameRetention,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.frames.sink import HarnessExecutionNotFound
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import FRAME_NOW, StepClock, observation, turn_observations
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows

pytestmark = pytest.mark.common_db

NEW_TABLES = {
    ("mission_control", "provider_frame"),
    ("mission_control", "frame_retention_policy"),
    ("mission_control", "context_selection"),
    ("mission_control_search", "transcript_document"),
}


@pytest.mark.asyncio
async def test_migration_0027_objects_force_rls_and_grant_the_capability_roles(
    common_db: CommonDatabase,
) -> None:
    rows = await owner_rows(
        common_db,
        """
        SELECT n.nspname, c.relname, c.relrowsecurity, c.relforcerowsecurity
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND (n.nspname, c.relname) IN
            (('mission_control', 'provider_frame'), ('mission_control', 'frame_retention_policy'),
             ('mission_control', 'context_selection'),
             ('mission_control_search', 'transcript_document'))
        """,
    )
    assert {(row["nspname"], row["relname"]) for row in rows} == NEW_TABLES
    assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in rows)
    privileges = await owner_rows(
        common_db,
        """
        SELECT
          has_table_privilege('mission_control_runtime', 'mission_control.provider_frame',
                              'INSERT') AS runtime_insert,
          has_table_privilege('mission_control_runtime', 'mission_control.provider_frame',
                              'DELETE') AS runtime_delete,
          has_table_privilege('mission_control_runtime', 'mission_control.provider_frame',
                              'UPDATE') AS runtime_update,
          has_table_privilege('mission_control_readonly', 'mission_control.provider_frame',
                              'SELECT') AS readonly_select,
          has_table_privilege('mission_control_readonly', 'mission_control.provider_frame',
                              'INSERT') AS readonly_insert,
          has_table_privilege('mission_control_runtime', 'mission_control.session_turn',
                              'INSERT') AS runtime_turn,
          has_table_privilege('mission_control_runtime', 'mission_control.session_turn',
                              'UPDATE') AS runtime_turn_update,
          has_table_privilege('mission_control_family_writer',
                              'mission_control.context_selection', 'INSERT') AS family_selection,
          has_table_privilege('mission_control_outbox_worker',
                              'mission_control_search.transcript_document', 'INSERT')
              AS projection_insert
        """,
    )
    granted = dict(privileges[0])
    assert granted == {
        "runtime_insert": True,
        "runtime_delete": True,
        "runtime_update": False,
        "readonly_select": True,
        "readonly_insert": False,
        "runtime_turn": True,
        "runtime_turn_update": False,
        "family_selection": True,
        "projection_insert": True,
    }


@pytest.mark.asyncio
async def test_append_is_idempotent_and_identity_records_land_in_the_frame_transaction(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        repository = PostgresFrameRepository(pool)
        handle = await repository.open_execution(admitted.start())
        writer = FrameWriter(repository, handle, clock=StepClock())
        receipt = await writer.write(turn_observations())
        assert (receipt.new, receipt.duplicate, receipt.stale) == (9, 0, 0)
        frames = await repository.frames_for_run(common_db.scope(), handle.run_id)
        assert [frame.arrival_ordinal for frame in frames] == list(range(1, 10))
        assert [frame.kind for frame in frames][-2:] == [FrameKind.TURN_ENDED, FrameKind.RUN_RESULT]
        closing = await repository.frames_for_execution(
            common_db.scope(), handle.harness_execution_id, 1, closing_only=True
        )
        assert [frame.kind for frame in closing] == [
            FrameKind.USAGE,
            FrameKind.TOOL_CALL_COMPLETED,
            FrameKind.USAGE,
            FrameKind.TURN_ENDED,
            FrameKind.RUN_RESULT,
        ]
        cursor = await repository.last_cursor(
            handle.harness_execution_id, 1, request_scope=common_db.scope()
        )
        assert cursor is not None and cursor.arrival_ordinal == 9

        execution = await owner_rows(
            common_db,
            "SELECT lane_profile, generation, lifecycle, native_identity, observation_cursor "
            "FROM mission_control.harness_execution WHERE harness_execution_id = $1",
            handle.harness_execution_id,
        )
        assert execution[0]["lane_profile"] == "deep_agents" and execution[0]["generation"] == 1
        assert execution[0]["lifecycle"] == "ended"
        identity = json.loads(execution[0]["native_identity"])
        assert identity["native_turn_refs"] == ["turn-1"] and "open_turn" not in identity
        assert json.loads(execution[0]["observation_cursor"])["arrival_ordinal"] == 9
        sessions = await owner_rows(
            common_db,
            "SELECT state, ended_at, native_session_ref FROM mission_control.agent_session "
            "WHERE harness_execution_id = $1",
            handle.harness_execution_id,
        )
        assert [row["state"] for row in sessions] == ["closed"]
        turns = await owner_rows(
            common_db,
            "SELECT turn_no, native_turn_ref, started_frame_id, ended_frame_id, usage, "
            "stop_reason, execution_outcome, usage_refs FROM mission_control.session_turn",
        )
        assert len(turns) == 1
        turn = turns[0]
        assert turn["started_frame_id"] == frames[1].frame_id
        assert turn["ended_frame_id"] == frames[7].frame_id
        assert turn["stop_reason"] == "end_turn" and turn["execution_outcome"] == "succeeded"
        usage = json.loads(turn["usage"])["dimensions"]
        assert usage["input_tokens"] == {
            "value": 8,
            "disposition": "settled",
            "source_frame_id": str(frames[6].frame_id),
        }
        assert usage["cost_micros"]["disposition"] == "unknown"
        assert usage["cost_micros"]["value"] is None
        assert len(turn["usage_refs"]) == 2

        # Re-running the writer over the same stream (a resumed activity) adds zero rows.
        replay = await FrameWriter(
            repository, await repository.open_execution(admitted.start()), clock=StepClock()
        ).write(turn_observations())
        assert (replay.new, replay.duplicate, replay.stale) == (0, 9, 0)
        assert len(await repository.frames_for_run(common_db.scope(), handle.run_id)) == 9
        turns = await owner_rows(
            common_db, "SELECT count(*) AS n FROM mission_control.session_turn"
        )
        assert turns[0]["n"] == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_resume_continues_ordinals_and_older_generations_are_fenced(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        repository = PostgresFrameRepository(pool)
        first = FrameWriter(
            repository, await repository.open_execution(admitted.start()), clock=StepClock()
        )
        await first.write(turn_observations()[:3])
        resumed_handle = await repository.open_execution(admitted.start())
        assert resumed_handle.last_cursor is not None
        assert resumed_handle.last_cursor.arrival_ordinal == 3
        resumed = FrameWriter(repository, resumed_handle, clock=StepClock())
        receipt = await resumed.write(turn_observations()[2:5])
        assert (receipt.new, receipt.duplicate) == (2, 1)
        frames = await repository.frames_for_execution(
            common_db.scope(), resumed_handle.harness_execution_id, 1
        )
        assert [frame.arrival_ordinal for frame in frames] == [1, 2, 3, 5, 6]

        # A relaunch advances the generation; the old process's late frames are stale.
        newer = await repository.open_execution(admitted.start(generation=2))
        assert (
            await repository.current_generation(common_db.scope(), newer.harness_execution_id) == 2
        )
        stale = await first.write([observation("late-tool", FrameKind.TOOL_CALL_COMPLETED)])
        assert (stale.new, stale.duplicate, stale.stale) == (0, 0, 1)
        fresh = await FrameWriter(repository, newer, clock=StepClock()).write(
            [observation("gen2-start", FrameKind.TURN_STARTED, turn="turn-2")]
        )
        assert fresh.new == 1
        with pytest.raises(HarnessExecutionNotFound):
            await repository.open_execution(
                admitted.start().model_copy(update={"activation_key": "missing-unit"})
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_rls_denies_cross_scope_reads_and_restricted_roles_cannot_mutate(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    readonly = await common_db.pool("mission_control_readonly")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        repository = PostgresFrameRepository(pool)
        handle = await repository.open_execution(admitted.start())
        await FrameWriter(repository, handle, clock=StepClock()).write(turn_observations())
        other_scope = common_db.scope("tenant-2")
        assert await repository.frames_for_run(other_scope, handle.run_id) == ()
        assert (
            await repository.frames_for_execution(other_scope, handle.harness_execution_id, 1) == ()
        )
        reader = PostgresFrameRepository(readonly)
        assert len(await reader.frames_for_run(common_db.scope(), handle.run_id)) == 9
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, common_db.scope())
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute("UPDATE mission_control.provider_frame SET kind = 'x'")
        async with pool.acquire() as connection, connection.transaction():
            # Without scope, forced RLS shows nothing.
            assert (
                await connection.fetchval("SELECT count(*) FROM mission_control.provider_frame")
                == 0
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await reader.append(
                [
                    frame.model_copy(update={"provider_key": "x"})
                    for frame in await reader.frames_for_run(common_db.scope(), handle.run_id)
                ][:1]
            )
    finally:
        await pool.close()
        await readonly.close()


@pytest.mark.asyncio
async def test_retention_deletes_only_expired_non_closing_frames_unless_configured(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        repository = PostgresFrameRepository(pool)
        handle = await repository.open_execution(admitted.start())
        await FrameWriter(repository, handle, clock=StepClock(FRAME_NOW)).write(turn_observations())
        events_before = await owner_rows(
            common_db, "SELECT count(*) AS n FROM mission_control.mission_event"
        )
        retention = PostgresFrameRetention(pool)
        early = await retention.expire(common_db.scope(), now=FRAME_NOW + timedelta(days=29))
        assert (early.deleted_non_closing, early.deleted_closing) == (0, 0)
        report = await retention.expire(common_db.scope(), now=FRAME_NOW + timedelta(days=31))
        assert (report.deleted_non_closing, report.deleted_closing) == (4, 0)
        remaining = await repository.frames_for_run(common_db.scope(), handle.run_id)
        assert all(frame.closing for frame in remaining) and len(remaining) == 5

        await retention.set_policy(
            common_db.scope(), FrameRetentionPolicy(retain_days=30, keep_closing_frames=False)
        )
        assert (await retention.policy(common_db.scope())).keep_closing_frames is False
        purge = await retention.expire(common_db.scope(), now=FRAME_NOW + timedelta(days=31))
        assert (purge.deleted_non_closing, purge.deleted_closing) == (0, 5)
        events_after = await owner_rows(
            common_db, "SELECT count(*) AS n FROM mission_control.mission_event"
        )
        assert events_after[0]["n"] == events_before[0]["n"]
        # Identity records outlive their frames.
        turns = await owner_rows(
            common_db, "SELECT count(*) AS n FROM mission_control.session_turn"
        )
        assert turns[0]["n"] == 1
    finally:
        await pool.close()
