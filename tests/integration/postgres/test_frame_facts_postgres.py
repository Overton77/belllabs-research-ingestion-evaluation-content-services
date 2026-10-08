"""C2 on the common component: closing frames -> facts -> mission events with resolvable refs.

Run control and the Native Event Store both run as the restricted runtime login under
forced RLS. Every mission event the reducer writes from frames carries
`source.native_event_ref`, and every such reference resolves to a stored provider frame.
"""

from __future__ import annotations

import json

import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    FrameFactProjector,
    FrameFactTarget,
)
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import frame_id_from_native_event_ref
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock, turn_observations
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor

pytestmark = pytest.mark.common_db

FRAME_EVENTS = (
    "session.started",
    "session.turn_started",
    "tool_call.completed",
    "session.turn_completed",
    "attempt.completed",
    "session.ended",
)


def projector_actor() -> ActorContext:
    return actor().model_copy(
        update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
    )


@pytest.mark.asyncio
async def test_frame_facts_become_mission_events_whose_refs_resolve(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(admitted.start())
        await FrameWriter(frames, handle, clock=StepClock()).write(turn_observations())
        projector = FrameFactProjector(frames, admitted.run_control, actor=projector_actor())
        target = FrameFactTarget.from_handle(handle, run_key=admitted.run_key)
        completion = CompletionEvidence(
            declared_outputs=frozenset({"report"}), registered_outputs=frozenset({"report"})
        )
        first = await projector.project(target, completion=completion)
        assert first.applied == first.derived == 6 and first.rejected_reason is None
        again = await projector.project(target, completion=completion)
        assert again.applied == 0

        rows = await owner_rows(
            common_db,
            """
            SELECT e.seq, e.event_type, e.payload
            FROM mission_control.mission_event e
            JOIN mission_control.mission_run r
              ON r.installation_id = e.installation_id AND r.application_id = e.application_id
             AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
            WHERE r.run_key = $1
            ORDER BY e.seq
            """,
            admitted.run_key,
        )
        frame_rows = [row for row in rows if row["event_type"] in FRAME_EVENTS]
        assert [row["event_type"] for row in frame_rows] == list(FRAME_EVENTS)
        seqs = [row["seq"] for row in rows]
        assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
        stored = {
            row["frame_id"]
            for row in await owner_rows(
                common_db,
                "SELECT frame_id FROM mission_control.provider_frame "
                "WHERE harness_execution_id = $1",
                handle.harness_execution_id,
            )
        }
        for row in frame_rows:
            envelope = json.loads(row["payload"])
            payload = envelope["payload"]
            ref = payload["source"]["native_event_ref"]
            assert frame_id_from_native_event_ref(ref) in stored, row["event_type"]
            assert payload["execution"]["harness_execution_id"] == str(handle.harness_execution_id)
            assert "content" not in json.dumps(payload), "events carry refs and digests only"
        turn = json.loads(frame_rows[3]["payload"])["payload"]
        assert turn["usage"]["dimensions"]["input_tokens"]["value"] == 8
        assert turn["usage"]["dimensions"]["cost_micros"]["disposition"] == "unknown"
        attempt = json.loads(frame_rows[4]["payload"])["payload"]
        assert attempt["outcome"] == "succeeded"
        run = await admitted.run_control.get_run(common_db.scope(), admitted.run_key)
        assert run.frame_fact_cursors[0].through_ordinal == 9
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_superseded_generation_produces_no_facts(common_db: CommonDatabase) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        old = await frames.open_execution(admitted.start())
        await FrameWriter(frames, old, clock=StepClock()).write(turn_observations()[:6])
        newer = await frames.open_execution(admitted.start(generation=2))
        await FrameWriter(frames, newer, clock=StepClock()).write(
            [
                item.model_copy(update={"provider_key": f"g2:{item.provider_key}"})
                for item in turn_observations()[:3]
            ]
        )
        projector = FrameFactProjector(frames, admitted.run_control, actor=projector_actor())
        result = await projector.project(FrameFactTarget.from_handle(old, run_key=admitted.run_key))
        assert result.generation == 2 and result.derived == 0 and result.applied == 0
    finally:
        await pool.close()
