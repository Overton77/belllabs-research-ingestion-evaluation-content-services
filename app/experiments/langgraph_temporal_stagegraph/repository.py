from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from .contracts import CompletionRecord, completion_identity


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    run_id: str
    thread_id: str
    payload: dict[str, Any]


class ExperimentRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> ExperimentRepository:
        return cls(await asyncpg.create_pool(dsn, min_size=1, max_size=8, command_timeout=30))

    async def close(self) -> None:
        await self.pool.close()

    async def setup(self) -> None:
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        async with self.pool.acquire() as connection:
            await connection.execute(schema)

    async def create_run(self, run_id: str, thread_id: str) -> None:
        await self.pool.execute(
            """INSERT INTO stagegraph_temporal_experiment.runs(run_id, thread_id, status)
               VALUES($1, $2, 'RUNNING') ON CONFLICT (run_id) DO NOTHING""",
            run_id,
            thread_id,
        )

    async def reserve_attempt(
        self, run_id: str, stage_id: str, prompt: str, delay_seconds: float
    ) -> dict[str, Any]:
        attempt_id = f"attempt:{run_id}:{stage_id}:1"
        await self.pool.execute(
            """INSERT INTO stagegraph_temporal_experiment.stage_attempts
                   (attempt_id, run_id, stage_id, attempt_number, status, prompt, delay_seconds)
               VALUES($1, $2, $3, 1, 'RESERVED', $4, $5)
               ON CONFLICT (run_id, stage_id, attempt_number) DO NOTHING""",
            attempt_id,
            run_id,
            stage_id,
            prompt,
            delay_seconds,
        )
        row = await self.pool.fetchrow(
            """SELECT attempt_id, stage_id, prompt, delay_seconds
               FROM stagegraph_temporal_experiment.stage_attempts
               WHERE run_id=$1 AND stage_id=$2 AND attempt_number=1""",
            run_id,
            stage_id,
        )
        if row is None:
            raise RuntimeError(f"failed to reserve {stage_id}")
        return dict(row)

    async def bind_temporal_execution(
        self, attempt_id: str, temporal_workflow_id: str, temporal_run_id: str | None
    ) -> None:
        result = await self.pool.execute(
            """UPDATE stagegraph_temporal_experiment.stage_attempts
               SET status=CASE WHEN status='RESERVED' THEN 'LAUNCHED' ELSE status END,
                   temporal_workflow_id=COALESCE(temporal_workflow_id, $2),
                   temporal_run_id=COALESCE(temporal_run_id, $3),
                   launched_at=COALESCE(launched_at, clock_timestamp())
               WHERE attempt_id=$1
                 AND (temporal_workflow_id IS NULL OR temporal_workflow_id=$2)""",
            attempt_id,
            temporal_workflow_id,
            temporal_run_id,
        )
        if result == "UPDATE 0":
            raise RuntimeError(f"conflicting Temporal binding for {attempt_id}")

    async def record_completion_and_wake(self, completion: CompletionRecord) -> None:
        digest = completion.output_digest or f"{completion.disposition}:{completion.error_type}"
        event_id = completion_identity(completion.attempt_id, digest)
        output_ref = f"experiment-result:{completion.attempt_id}:{digest[:16]}"
        async with self.pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT status, temporal_workflow_id, output_digest
                   FROM stagegraph_temporal_experiment.stage_attempts
                   WHERE attempt_id=$1 FOR UPDATE""",
                completion.attempt_id,
            )
            if row is None:
                raise RuntimeError(f"unknown attempt {completion.attempt_id}")
            if row["temporal_workflow_id"] not in (None, completion.temporal_workflow_id):
                raise RuntimeError("completion came from a conflicting Temporal workflow")
            if completion.disposition == "succeeded":
                if completion.output_text is None or completion.output_digest is None:
                    raise ValueError("successful completion requires output and digest")
                if row["output_digest"] not in (None, completion.output_digest):
                    raise RuntimeError("attempt already has a different semantic output")
                await connection.execute(
                    """INSERT INTO stagegraph_temporal_experiment.results
                           (output_ref, attempt_id, output_digest, output_text)
                       VALUES($1, $2, $3, $4) ON CONFLICT (attempt_id) DO NOTHING""",
                    output_ref,
                    completion.attempt_id,
                    completion.output_digest,
                    completion.output_text,
                )
                await connection.execute(
                    """UPDATE stagegraph_temporal_experiment.stage_attempts
                       SET status=CASE WHEN status IN ('RESERVED','LAUNCHED')
                                       THEN 'READY_TO_RECONCILE' ELSE status END,
                           temporal_workflow_id=COALESCE(temporal_workflow_id, $2),
                           temporal_run_id=COALESCE(temporal_run_id, $3),
                           output_ref=COALESCE(output_ref, $4),
                           output_digest=COALESCE(output_digest, $5),
                           completed_at=COALESCE(completed_at, clock_timestamp())
                       WHERE attempt_id=$1""",
                    completion.attempt_id,
                    completion.temporal_workflow_id,
                    completion.temporal_run_id,
                    output_ref,
                    completion.output_digest,
                )
            else:
                status = "FAILED" if completion.disposition == "failed" else "CANCELLED"
                await connection.execute(
                    """UPDATE stagegraph_temporal_experiment.stage_attempts
                       SET status=CASE WHEN status IN ('RESERVED','LAUNCHED')
                                       THEN $2 ELSE status END,
                           error_type=COALESCE(error_type, $3),
                           completed_at=COALESCE(completed_at, clock_timestamp())
                       WHERE attempt_id=$1""",
                    completion.attempt_id,
                    status,
                    completion.error_type,
                )
            payload = json.dumps(
                {"thread_id": completion.thread_id, "attempt_id": completion.attempt_id}
            )
            await connection.execute(
                """INSERT INTO stagegraph_temporal_experiment.outbox
                       (event_id, run_id, event_type, payload)
                   VALUES($1, $2, 'WORKFLOW_WAKE_REQUESTED', $3::jsonb)
                   ON CONFLICT (event_id) DO NOTHING""",
                event_id,
                completion.run_id,
                payload,
            )

    async def admit_success_idempotently(self, attempt_id: str) -> str | None:
        row = await self.pool.fetchrow(
            """UPDATE stagegraph_temporal_experiment.stage_attempts
               SET status='ADMITTED', admitted_at=COALESCE(admitted_at, clock_timestamp())
               WHERE attempt_id=$1 AND status='READY_TO_RECONCILE'
               RETURNING output_ref""",
            attempt_id,
        )
        if row:
            return row["output_ref"]
        return await self.pool.fetchval(
            """SELECT output_ref FROM stagegraph_temporal_experiment.stage_attempts
               WHERE attempt_id=$1 AND status='ADMITTED'""",
            attempt_id,
        )

    async def load_attempts(self, run_id: str) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT a.*, r.output_text
               FROM stagegraph_temporal_experiment.stage_attempts a
               LEFT JOIN stagegraph_temporal_experiment.results r USING (attempt_id)
               WHERE a.run_id=$1 ORDER BY a.reserved_at, a.stage_id""",
            run_id,
        )
        return [dict(row) for row in rows]

    async def load_result_text(self, output_ref: str) -> str:
        value = await self.pool.fetchval(
            "SELECT output_text FROM stagegraph_temporal_experiment.results WHERE output_ref=$1",
            output_ref,
        )
        if value is None:
            raise RuntimeError(f"missing result {output_ref}")
        return value

    async def record_graph_event(
        self, event_id: str, run_id: str, event_type: str, payload: dict[str, Any] | None = None
    ) -> None:
        await self.pool.execute(
            """INSERT INTO stagegraph_temporal_experiment.graph_events
                   (event_id, run_id, event_type, payload)
               VALUES($1, $2, $3, $4::jsonb) ON CONFLICT (event_id) DO NOTHING""",
            event_id,
            run_id,
            event_type,
            json.dumps(payload or {}),
        )

    async def pending_events(self, limit: int = 20) -> list[OutboxEvent]:
        rows = await self.pool.fetch(
            """SELECT o.event_id, o.run_id, r.thread_id, o.payload
               FROM stagegraph_temporal_experiment.outbox o
               JOIN stagegraph_temporal_experiment.runs r USING (run_id)
               WHERE o.delivered_at IS NULL ORDER BY o.created_at LIMIT $1""",
            limit,
        )
        return [
            OutboxEvent(row["event_id"], row["run_id"], row["thread_id"], row["payload"])
            for row in rows
        ]

    async def increment_delivery_attempt(self, event_id: str) -> None:
        await self.pool.execute(
            """UPDATE stagegraph_temporal_experiment.outbox
               SET delivery_attempts=delivery_attempts+1 WHERE event_id=$1""",
            event_id,
        )

    async def mark_outbox_delivered(self, event_id: str) -> None:
        await self.pool.execute(
            """UPDATE stagegraph_temporal_experiment.outbox
               SET delivered_at=COALESCE(delivered_at, clock_timestamp()) WHERE event_id=$1""",
            event_id,
        )

    async def finish_run(self, run_id: str) -> None:
        await self.pool.execute(
            """UPDATE stagegraph_temporal_experiment.runs
               SET status='COMPLETED', completed_at=COALESCE(completed_at, clock_timestamp())
               WHERE run_id=$1""",
            run_id,
        )

    async def timeline(self, run_id: str) -> dict[str, Any]:
        run = await self.pool.fetchrow(
            "SELECT * FROM stagegraph_temporal_experiment.runs WHERE run_id=$1", run_id
        )
        attempts = await self.load_attempts(run_id)
        outbox = await self.pool.fetch(
            """SELECT event_id, created_at, delivered_at, delivery_attempts
               FROM stagegraph_temporal_experiment.outbox WHERE run_id=$1 ORDER BY created_at""",
            run_id,
        )
        events = await self.pool.fetch(
            """SELECT event_id, event_type, created_at, payload
               FROM stagegraph_temporal_experiment.graph_events
               WHERE run_id=$1 ORDER BY created_at""",
            run_id,
        )
        return {
            "run": dict(run) if run else None,
            "attempts": attempts,
            "outbox": [dict(row) for row in outbox],
            "graph_events": [dict(row) for row in events],
        }


async def prepare_database(migration_dsn: str) -> None:
    """Run experiment and LangGraph setup with migration authority, then grant runtime DML."""
    migration_repository = await ExperimentRepository.connect(migration_dsn)
    try:
        await migration_repository.setup()
        async with AsyncPostgresSaver.from_conn_string(migration_dsn) as saver:
            await saver.setup()
        await migration_repository.pool.execute(
            """DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
                    GRANT SELECT, INSERT, UPDATE, DELETE ON
                        public.checkpoints,
                        public.checkpoint_blobs,
                        public.checkpoint_writes,
                        public.checkpoint_migrations
                    TO belllabs_app;
                END IF;
            END $$;"""
        )
    finally:
        await migration_repository.close()
