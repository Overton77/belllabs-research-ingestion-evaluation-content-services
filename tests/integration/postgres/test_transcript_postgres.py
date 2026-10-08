"""C3 on the common component: the transcript of a C1-style run with C2 facts applied.

Frames are written through the Postgres Native Event Store, facts are projected into the
ledger through run control, and the transcript is materialized from the real
`mission_event` and `provider_frame` rows under the restricted runtime login. Markdown
and JSONL match committed goldens modulo ids, digests, cursors and timestamps.
Set `MC_UPDATE_GOLDENS=1` to rewrite the goldens after an intended change.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_reads import PostgresMissionEventReader
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    FrameFactProjector,
    FrameFactTarget,
)
from mission_control.application.frames.transcript import (
    TranscriptRunNotFound,
    TranscriptService,
)
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.render import to_jsonl, to_markdown
from mission_control.domain.frames.transcript import TranscriptQuery
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock, turn_observations
from tests.fixtures.transcripts import normalize
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor

pytestmark = pytest.mark.common_db

GOLDENS = Path(__file__).parent / "goldens"
READER = ActorContext(
    actor_id="reader", authority_refs=frozenset(), permissions=frozenset({"workflow_run.read"})
)


def _golden(name: str, text: str) -> None:
    path = GOLDENS / name
    if os.environ.get("MC_UPDATE_GOLDENS") == "1":
        GOLDENS.mkdir(exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    assert path.read_text(encoding="utf-8") == text


@pytest.mark.asyncio
async def test_transcript_of_a_projected_run_matches_goldens(common_db: CommonDatabase) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(admitted.start())
        await FrameWriter(frames, handle, clock=StepClock()).write(turn_observations())
        projector = FrameFactProjector(
            frames,
            admitted.run_control,
            actor=actor().model_copy(
                update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
            ),
        )
        await projector.project(
            FrameFactTarget.from_handle(handle, run_key=admitted.run_key),
            completion=CompletionEvidence(),
        )
        service = TranscriptService(
            PostgresMissionEventReader(pool), frames, request_scope=common_db.scope()
        )
        page = await service.materialize(admitted.run_key, actor=READER)
        entries = page.entries
        cursors = [entry.cursor for entry in entries]
        assert cursors == sorted(cursors) and len(set(cursors)) == len(cursors)
        kinds = [entry.kind for entry in entries]
        for expected in (
            "session.started",
            "tool_call.completed",
            "session.turn_completed",
            "attempt.completed",
            "tool_call_completed",
            "run_result",
        ):
            assert expected in kinds
        canonical = sum(1 for entry in entries if entry.canonical)
        assert canonical >= 7 and len(entries) - canonical == 9
        _golden("transcript_c1_run.md", normalize(to_markdown(entries, run_id="<run>")))
        _golden(
            "transcript_c1_run.jsonl",
            normalize(to_jsonl(entries)).replace(admitted.run_key, "<run>"),
        )

        newer = await service.materialize(
            admitted.run_key, actor=READER, query=TranscriptQuery(since=entries[-3].cursor)
        )
        assert [entry.cursor for entry in newer.entries] == cursors[-2:]
        for line in to_jsonl(newer.entries).splitlines():
            json.loads(line)

        other = TranscriptService(
            PostgresMissionEventReader(pool), frames, request_scope=common_db.scope("tenant-2")
        )
        with pytest.raises(TranscriptRunNotFound):
            await other.materialize(admitted.run_key, actor=READER)
    finally:
        await pool.close()
