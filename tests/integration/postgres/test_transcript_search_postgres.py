"""C4 on the common component: the transcript search projection and ranked search.

A C1-style run (frames through the Postgres Native Event Store, C2 facts in the ledger)
is projected into ``mission_control_search.transcript_document`` under the restricted
runtime login and forced RLS: one row per canonical entry and per closing-frame entry,
rebuilt idempotently. ``websearch_to_tsquery`` + ``ts_rank_cd`` rank a known phrase in a
tool result excerpt first, and the returned cursor opens that entry through
``run transcript --since``. Another tenant finds nothing.
"""

from __future__ import annotations

import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_projection import (
    PostgresRunMissionIds,
    PostgresTranscriptDocuments,
)
from mission_control.adapters.postgres.frames.transcript_reads import PostgresMissionEventReader
from mission_control.application.frames.reducer import (
    CompletionEvidence,
    FrameFactProjector,
    FrameFactTarget,
)
from mission_control.application.frames.search import (
    TranscriptProjector,
    TranscriptSearchService,
    projected,
)
from mission_control.application.frames.transcript import (
    TranscriptRunNotFound,
    TranscriptService,
)
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.frames.transcript import TranscriptQuery
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock, observation, turn_observations
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor

pytestmark = pytest.mark.common_db

READER = ActorContext(
    actor_id="reader", authority_refs=frozenset(), permissions=frozenset({"workflow_run.read"})
)
PHRASE = "source_manifest registered with 180 PubMed abstracts on muscle aging"


@pytest.mark.asyncio
async def test_projection_rebuilds_idempotently_and_ranks_a_known_phrase_first(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(admitted.start())
        observations = [
            *turn_observations(),
            observation(
                "turn-1:call-2:completed",
                FrameKind.TOOL_CALL_COMPLETED,
                {
                    "tool_call_id": "call-2",
                    "name": "register_artifact",
                    "status": "success",
                    "content": PHRASE,
                },
                tool_call_ref="call-2",
            ),
        ]
        await FrameWriter(frames, handle, clock=StepClock()).write(observations)
        await FrameFactProjector(
            frames,
            admitted.run_control,
            actor=actor().model_copy(
                update={"permissions": ALL_PERMISSIONS | {"workflow_run.apply_frame_facts"}}
            ),
        ).project(
            FrameFactTarget.from_handle(handle, run_key=admitted.run_key),
            completion=CompletionEvidence(),
        )
        transcripts = TranscriptService(
            PostgresMissionEventReader(pool), frames, request_scope=common_db.scope()
        )
        documents = PostgresTranscriptDocuments(pool)
        projector = TranscriptProjector(transcripts, documents)

        first = await projector.project(admitted.run_key)
        entries = await transcripts.projection_entries(admitted.run_key)
        expected = [entry for entry in entries if projected(entry)]
        assert first.documents == first.upserted == len(expected)
        assert first.deleted == 0
        # Every canonical entry and every closing-frame entry; no deltas, no starts.
        kinds = {entry.kind for entry in expected}
        assert "message_delta" not in kinds and "tool_call_started" not in kinds
        assert {"tool_call_completed", "run_result", "session.turn_completed"} <= kinds
        again = await projector.project(admitted.run_key)
        assert (again.upserted, again.deleted, again.documents) == (0, 0, first.documents)
        rows = await owner_rows(
            common_db,
            "SELECT count(*) AS n FROM mission_control_search.transcript_document",
        )
        assert rows[0]["n"] == len(expected)

        search = TranscriptSearchService(transcripts, documents)
        page = await search.search(admitted.run_key, '"source manifest" pubmed', actor=READER)
        assert page.hits, "the known phrase must be found"
        best = page.hits[0]
        assert best.entry.kind == "tool_call_completed"
        assert "PubMed abstracts" in (best.entry.body_excerpt or "")
        assert all(best.rank >= hit.rank for hit in page.hits)
        # The cursor opens the entry through `run transcript --since`.
        opened = await transcripts.materialize(
            admitted.run_key, actor=READER, query=TranscriptQuery(since=best.open_cursor, limit=1)
        )
        assert opened.entries[0].cursor == best.entry.cursor
        assert (await search.search(admitted.run_key, "nonexistentword", actor=READER)).hits == ()

        # A stale row (an entry the transcript no longer has) is removed by a rebuild.
        await owner_rows(
            common_db,
            """
            INSERT INTO mission_control_search.transcript_document
                (installation_id, application_id, tenant_id, run_id, cursor, kind, role,
                 title, body_excerpt, canonical, recorded_at)
            SELECT installation_id, application_id, tenant_id, run_id,
                   'tc1:999999999999:00000000000000000:000000000000:zzzzzzzz', 'stale', NULL,
                   'stale', 'stale row', true, now()
            FROM mission_control_search.transcript_document LIMIT 1
            """,
        )
        rebuilt = await projector.project(admitted.run_key)
        assert rebuilt.deleted == 1 and rebuilt.upserted == 0

        mission_ids = PostgresRunMissionIds(pool)
        assert await mission_ids(common_db.scope(), admitted.run_key) is not None
        assert await mission_ids(common_db.scope(), "missing-run") is None

        other = TranscriptSearchService(
            TranscriptService(
                PostgresMissionEventReader(pool), frames, request_scope=common_db.scope("tenant-2")
            ),
            documents,
        )
        await common_db.add_tenants(["tenant-2"])
        with pytest.raises(TranscriptRunNotFound):
            await other.search(admitted.run_key, "pubmed", actor=READER)
    finally:
        await pool.close()
