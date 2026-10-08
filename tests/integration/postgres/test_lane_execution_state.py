"""FT-G2 migration 0030 (G2 section) on a disposable PostgreSQL 17: harness execution lane
state written through the runtime role under forced RLS, after the frame store opened the row.

Native identity is written once (a different agent or run is refused), the provider cursor and
segment time advance per segment, the usage disposition is recorded at settlement, frames
persist before the cursor that names them, and another tenant reads nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import (
    HarnessExecutionNotOpened,
    PostgresLaneExecutionStateStore,
)
from mission_control.application.execution.harness.state import (
    LaneExecutionUpdate,
    NativeIdentityConflict,
    segment_update,
)
from mission_control.application.frames.kinds import classify, cursor_local_key
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import FrameObservation, LaneProfile
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401

pytestmark = pytest.mark.common_db


async def test_lane_state_is_written_once_and_advanced_per_segment(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        start = admitted.start(lane=LaneProfile.CURSOR_LOCAL, native_session_ref="agent-1")
        states = PostgresLaneExecutionStateStore(pool)
        scope, heid = start.request_scope, start.harness_execution_id
        with pytest.raises(HarnessExecutionNotOpened):
            await states.record(scope, heid, LaneExecutionUpdate(native_session_ref="agent-1"))

        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(start)
        assert await states.load(scope, heid) is not None
        await states.record(scope, heid, LaneExecutionUpdate(native_session_ref="agent-1"))
        await states.record(scope, heid, LaneExecutionUpdate(native_turn_ref="run-1"))
        with pytest.raises(NativeIdentityConflict):
            await states.record(scope, heid, LaneExecutionUpdate(native_turn_ref="run-2"))

        # Frames persist first; the segment cursor names only persisted frames.
        writer = FrameWriter(frames, handle)
        observations = [
            FrameObservation(
                provider_key=cursor_local_key(f"run-1:{offset}"),
                raw_kind="tool_call",
                kind=classify(LaneProfile.CURSOR_LOCAL, "tool_call", body).kind,
                body=body,
                native_turn_ref="run-1",
            )
            for offset, body in enumerate(
                [{"call_id": "c1", "status": "running"}, {"call_id": "c1", "status": "completed"}]
            )
        ]
        receipt = await writer.write(observations)
        assert receipt.new == 2
        last = await frames.last_cursor(heid, 1, request_scope=scope)
        assert last is not None and last.provider_key == cursor_local_key("run-1:1")
        at = datetime.now(UTC)
        await states.record(scope, heid, segment_update("1", at))
        await states.record(scope, heid, LaneExecutionUpdate(usage_disposition="settled"))
        state = await states.load(scope, heid)
        assert state is not None
        assert (state.native_session_ref, state.native_turn_ref) == ("agent-1", "run-1")
        assert state.provider_cursor == "1" and state.usage_disposition == "settled"
        assert state.last_segment_at is not None
        # A replayed write of the same identity is idempotent.
        await states.record(scope, heid, LaneExecutionUpdate(native_turn_ref="run-1"))

        other = await common_db.pool("mission_control_runtime")
        try:
            assert (
                await PostgresLaneExecutionStateStore(other).load(common_db.scope("tenant-2"), heid)
                is None
            )
        finally:
            await other.close()
    finally:
        await pool.close()


async def test_the_readonly_role_cannot_write_lane_state(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        columns = {
            row["column_name"]
            for row in await owner.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'mission_control' AND table_name = 'harness_execution'"
            )
        }
        assert {
            "native_session_ref",
            "native_turn_ref",
            "provider_cursor",
            "usage_disposition",
            "last_segment_at",
            "cursor_sdk_version",
            "bridge_state_root",
            "cloud_branch",
            "cloud_agent_url",
        } <= columns
        granted = {
            row["column_name"]
            for row in await owner.fetch(
                "SELECT column_name FROM information_schema.column_privileges "
                "WHERE table_schema = 'mission_control' AND table_name = 'harness_execution' "
                "AND privilege_type = 'UPDATE' AND grantee = 'mission_control_runtime'"
            )
        }
        assert {"native_turn_ref", "provider_cursor", "usage_disposition"} <= granted
        readonly = {
            row["column_name"]
            for row in await owner.fetch(
                "SELECT column_name FROM information_schema.column_privileges "
                "WHERE table_schema = 'mission_control' AND table_name = 'harness_execution' "
                "AND privilege_type = 'UPDATE' AND grantee = 'mission_control_readonly'"
            )
        }
        assert readonly == set()
    finally:
        await owner.close()
