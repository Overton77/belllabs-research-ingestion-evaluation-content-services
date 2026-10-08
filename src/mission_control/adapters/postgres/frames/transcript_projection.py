"""PostgreSQL transcript search projection (SPEC-03 "Run list and search", ticket C4).

``mission_control_search.transcript_document`` (created by migration 0027) holds one row
per projected transcript entry with a generated, weighted ``fts`` column (title A, body
excerpt B). :meth:`PostgresTranscriptDocuments.replace` makes a run's rows equal the
current projection in one transaction (upsert by cursor, delete the rest), so a rebuild is
idempotent; :meth:`PostgresTranscriptDocuments.search` ranks with ``ts_rank_cd`` over
``websearch_to_tsquery('english', ...)``. Rows are scoped by forced RLS and every
statement also filters on the three scope columns. The table is a rebuildable index,
never an authorization store.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.frames.search import ProjectionReceipt, TranscriptDocument

MAX_EXCERPT_CHARS = 16_384


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("\x00", "")[:MAX_EXCERPT_CHARS]


class PostgresTranscriptDocuments:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def replace(
        self, request_scope: str, run_key: str, documents: Sequence[TranscriptDocument]
    ) -> ProjectionReceipt:
        if any(document.run_key != run_key for document in documents):
            raise ValueError("projected documents belong to exactly one run")
        cursors = [document.cursor for document in documents]
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            run_id = await mc.run_uuid(connection, args, run_key)
            await mc.advisory_lock(connection, f"transcript-projection:{run_id}")
            changed = await connection.fetch(
                """
                INSERT INTO mission_control_search.transcript_document AS existing (
                    installation_id, application_id, tenant_id, run_id, cursor, kind, role,
                    title, body_excerpt, canonical, recorded_at)
                SELECT $1, $2, $3, $4, d.cursor, d.kind, d.role, d.title, d.body_excerpt,
                       d.canonical, d.recorded_at
                FROM unnest($5::text[], $6::text[], $7::text[], $8::text[], $9::text[],
                            $10::boolean[], $11::timestamptz[])
                     AS d(cursor, kind, role, title, body_excerpt, canonical, recorded_at)
                ON CONFLICT (installation_id, application_id, tenant_id, run_id, cursor)
                DO UPDATE SET kind = EXCLUDED.kind, role = EXCLUDED.role,
                    title = EXCLUDED.title, body_excerpt = EXCLUDED.body_excerpt,
                    canonical = EXCLUDED.canonical, recorded_at = EXCLUDED.recorded_at
                WHERE (existing.kind, existing.role, existing.title, existing.body_excerpt,
                       existing.canonical, existing.recorded_at)
                      IS DISTINCT FROM
                      (EXCLUDED.kind, EXCLUDED.role, EXCLUDED.title, EXCLUDED.body_excerpt,
                       EXCLUDED.canonical, EXCLUDED.recorded_at)
                RETURNING existing.cursor
                """,
                *args,
                run_id,
                cursors,
                [document.kind for document in documents],
                [document.role for document in documents],
                [_clean(document.title) or document.kind for document in documents],
                [_clean(document.body_excerpt) for document in documents],
                [document.canonical for document in documents],
                [document.recorded_at for document in documents],
            )
            deleted = await connection.fetch(
                f"""
                DELETE FROM mission_control_search.transcript_document
                WHERE {SCOPE} AND run_id = $4 AND NOT (cursor = ANY($5::text[]))
                RETURNING cursor
                """,
                *args,
                run_id,
                cursors,
            )
        return ProjectionReceipt(
            run_id=run_key,
            upserted=len(changed),
            deleted=len(deleted),
            documents=len(documents),
        )

    async def search(
        self, request_scope: str, run_key: str, text: str, *, limit: int
    ) -> tuple[tuple[str, float], ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            run_id = await mc.run_uuid(connection, args, run_key)
            rows = await connection.fetch(
                f"""
                SELECT document.cursor, ts_rank_cd(document.fts, query) AS rank
                FROM mission_control_search.transcript_document AS document,
                     websearch_to_tsquery('english'::regconfig, $5) AS query
                WHERE {mc.scoped("document")} AND document.run_id = $4
                  AND document.fts @@ query
                ORDER BY rank DESC, document.cursor
                LIMIT $6
                """,
                *args,
                run_id,
                text,
                limit,
            )
        return tuple((row["cursor"], float(row["rank"])) for row in rows)


class PostgresRunMissionIds:
    """FT-C4: the ledger mission id of a run (for `mc_mission_id` at root start)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def __call__(self, request_scope: str, run_id: str) -> str | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            value = await connection.fetchval(
                f"SELECT mission_id FROM mission_control.mission_run WHERE {SCOPE} "
                "AND run_key = $4",
                *args,
                run_id,
            )
        return str(value) if value is not None else None
