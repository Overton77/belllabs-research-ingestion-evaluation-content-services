"""PostgreSQL Native Event Store: `mission_control.provider_frame` (SPEC-03, ADR-0028).

`append` writes one batch per (harness execution, generation) in a single
`INSERT ... SELECT FROM unnest(...) ON CONFLICT (harness_execution_id, generation,
provider_key) DO NOTHING RETURNING frame_id` statement, inside one transaction that first
locks the harness execution row: frames of a generation older than the execution's
current one are counted `stale` and never stored, and the native identity records
(`harness_execution`, `agent_session`, `session_turn`) advance in the same transaction as
the frames that cause them. An arrival-ordinal collision (another writer) raises
`FrameOrdinalConflict` so the writer reseeds from `last_cursor`.

Every statement runs after `apply_scope` under forced RLS; the runtime role writes, the
read-only role reads.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.frames import identity
from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.application.frames.sink import (
    FrameOrdinalConflict,
    HarnessExecutionNotFound,
)
from mission_control.domain.frames.contracts import (
    IDENTITY_KINDS,
    AppendReceipt,
    FrameKind,
    FrameRedaction,
    FrameScope,
    HarnessExecutionHandle,
    HarnessExecutionStart,
    LaneProfile,
    ProviderCursor,
    ProviderFrame,
)

WRITER_ACTOR = "mission-control-frame-writer"
ORDINAL_CONSTRAINT = "provider_frame_arrival_key"

FRAME_COLUMNS = """
    frame_id, installation_id, application_id, tenant_id, run_id, activation_id, attempt_no,
    harness_execution_id, generation, lane_profile, native_session_ref, native_turn_ref,
    provider_key, arrival_ordinal, observed_at, provider_timestamp, kind, closing, raw_kind,
    subordinate_ref, tool_call_ref, body_digest, body_bytes, body_media_type, body_excerpt,
    body_artifact_ref, redactions
"""


def frame_from_row(row: asyncpg.Record) -> ProviderFrame:
    redactions = row["redactions"]
    if isinstance(redactions, str):
        redactions = json.loads(redactions)
    return ProviderFrame(
        frame_id=row["frame_id"],
        scope=FrameScope(
            installation_id=row["installation_id"],
            application_id=row["application_id"],
            tenant_id=row["tenant_id"],
        ),
        run_id=row["run_id"],
        activation_id=row["activation_id"],
        attempt_no=int(row["attempt_no"]),
        harness_execution_id=row["harness_execution_id"],
        generation=int(row["generation"]),
        lane_profile=LaneProfile(row["lane_profile"]),
        native_session_ref=row["native_session_ref"],
        native_turn_ref=row["native_turn_ref"],
        provider_key=row["provider_key"],
        arrival_ordinal=int(row["arrival_ordinal"]),
        observed_at=row["observed_at"],
        provider_timestamp=row["provider_timestamp"],
        kind=FrameKind(row["kind"]),
        closing=bool(row["closing"]),
        subordinate_ref=row["subordinate_ref"],
        tool_call_ref=row["tool_call_ref"],
        body_digest=row["body_digest"],
        body_bytes=int(row["body_bytes"]),
        body_media_type=row["body_media_type"],
        body_excerpt=bytes(row["body_excerpt"]).decode("utf-8", errors="replace"),
        body_artifact_ref=row["body_artifact_ref"],
        redactions=tuple(FrameRedaction.model_validate(item) for item in redactions or ()),
        raw_kind=row["raw_kind"],
    )


class PostgresFrameRepository:
    """FrameStore + FrameReader over the common component."""

    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = WRITER_ACTOR) -> None:
        self._pool = pool
        self._actor = actor_ref

    # --- FrameStore -----------------------------------------------------------------

    async def open_execution(self, start: HarnessExecutionStart) -> HarnessExecutionHandle:
        now = datetime.now(UTC)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, start.request_scope)
            run_id = await connection.fetchval(
                f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
                *args,
                start.run_key,
            )
            activation_id = (
                await connection.fetchval(
                    f"""
                    SELECT activation_id FROM mission_control.activation
                    WHERE {SCOPE} AND activation_key = $4 AND run_id = $5
                    """,
                    *args,
                    start.activation_key,
                    run_id,
                )
                if run_id is not None
                else None
            )
            attempt_id = (
                await connection.fetchval(
                    f"""
                    SELECT attempt_id FROM mission_control.attempt
                    WHERE {SCOPE} AND activation_id = $4 AND attempt_no = $5
                    """,
                    *args,
                    activation_id,
                    start.attempt_no,
                )
                if activation_id is not None
                else None
            )
            if run_id is None or activation_id is None or attempt_id is None:
                raise HarnessExecutionNotFound(
                    "harness execution names a run, activation or attempt that is not recorded"
                )
            row = await connection.fetchrow(
                """
                INSERT INTO mission_control.harness_execution (
                    installation_id, application_id, tenant_id, harness_execution_id, run_id,
                    attempt_id, runtime_kind, provider_kind, placement_kind,
                    intended_binding_digest, actual_binding_digest, native_execution_refs,
                    observation_cursor, recovery_state, version, updated_at, created_at,
                    created_by_actor_ref, lane_profile, generation, native_identity, lifecycle
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, NULL, 'live', 1,
                        $12, $12, $13, $14, $15, $16::jsonb, 'live')
                ON CONFLICT (harness_execution_id) DO UPDATE
                SET generation = GREATEST(mission_control.harness_execution.generation,
                                          EXCLUDED.generation),
                    lifecycle = 'live',
                    version = mission_control.harness_execution.version + 1,
                    updated_at = EXCLUDED.updated_at
                RETURNING run_id, attempt_id, lane_profile, intended_binding_digest, generation
                """,
                *args,
                start.harness_execution_id,
                run_id,
                attempt_id,
                start.runtime_kind,
                start.provider_kind,
                start.placement_kind,
                start.intended_binding_digest,
                [start.launch_key] if start.launch_key else [],
                now,
                self._actor,
                start.lane_profile.value,
                start.generation,
                json.dumps(
                    {
                        "agent_runtime_kind": start.runtime_kind,
                        "native_session_ref": start.native_session_ref,
                        "native_turn_refs": [],
                        "launch_key": start.launch_key,
                        **start.native_identity,
                    },
                    sort_keys=True,
                ),
            )
            assert row is not None
            if (
                row["run_id"] != run_id
                or row["attempt_id"] != attempt_id
                or row["lane_profile"] != start.lane_profile.value
                or row["intended_binding_digest"] != start.intended_binding_digest
            ):
                raise ValueError("harness execution identity is reused with a different binding")
            cursor = await _last_cursor(connection, start.harness_execution_id, start.generation)
        scope = FrameScope.from_request_scope(start.request_scope)
        return HarnessExecutionHandle(
            harness_execution_id=start.harness_execution_id,
            scope=scope,
            run_id=run_id,
            activation_id=activation_id,
            attempt_no=start.attempt_no,
            generation=start.generation,
            lane_profile=start.lane_profile,
            native_session_ref=start.native_session_ref,
            last_cursor=cursor,
        )

    async def append(self, frames: Sequence[ProviderFrame]) -> AppendReceipt:
        if not frames:
            return AppendReceipt(new=0, duplicate=0, stale=0)
        scopes = {frame.scope for frame in frames}
        if len(scopes) != 1:
            raise ValueError("one append batch carries exactly one scope")
        scope = next(iter(scopes))
        groups: dict[UUID, list[ProviderFrame]] = {}
        for frame in sorted(frames, key=lambda item: item.arrival_ordinal):
            groups.setdefault(frame.harness_execution_id, []).append(frame)
        receipt = AppendReceipt(new=0, duplicate=0, stale=0)
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                args = await begin(connection, scope.request_scope)
                for harness_execution_id, batch in groups.items():
                    receipt = receipt + await self._append_execution(
                        connection, args, harness_execution_id, batch
                    )
        except asyncpg.UniqueViolationError as error:
            if getattr(error, "constraint_name", None) == ORDINAL_CONSTRAINT:
                raise FrameOrdinalConflict("arrival ordinal already claimed") from None
            raise
        return receipt

    async def _append_execution(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        harness_execution_id: UUID,
        frames: list[ProviderFrame],
    ) -> AppendReceipt:
        execution = await connection.fetchrow(
            f"""
            SELECT harness_execution_id, run_id, attempt_id, generation, native_identity
            FROM mission_control.harness_execution
            WHERE {SCOPE} AND harness_execution_id = $4
            FOR UPDATE
            """,
            *args,
            harness_execution_id,
        )
        if execution is None:
            raise HarnessExecutionNotFound("frame names an unopened harness execution")
        current = int(execution["generation"] or 1)
        fresh = [frame for frame in frames if frame.generation >= current]
        stale = len(frames) - len(fresh)
        if not fresh:
            return AppendReceipt(new=0, duplicate=0, stale=stale)
        if any(frame.run_id != execution["run_id"] for frame in fresh):
            raise ValueError("frame run identity differs from its harness execution")
        newest = max(frame.generation for frame in fresh)
        if newest > current:
            await connection.execute(
                f"""
                UPDATE mission_control.harness_execution
                SET generation = $5, version = version + 1, updated_at = clock_timestamp()
                WHERE {SCOPE} AND harness_execution_id = $4
                """,
                *args,
                harness_execution_id,
                newest,
            )
        inserted = await connection.fetch(
            """
            INSERT INTO mission_control.provider_frame (
                installation_id, application_id, tenant_id, frame_id, run_id, activation_id,
                attempt_no, harness_execution_id, generation, lane_profile, native_session_ref,
                native_turn_ref, provider_key, arrival_ordinal, observed_at, provider_timestamp,
                kind, closing, raw_kind, subordinate_ref, tool_call_ref, body_digest, body_bytes,
                body_media_type, body_excerpt, body_artifact_ref, redactions, created_at,
                created_by_actor_ref
            )
            SELECT $1, $2, $3, f.frame_id, f.run_id, f.activation_id, f.attempt_no,
                   f.harness_execution_id, f.generation, f.lane_profile, f.native_session_ref,
                   f.native_turn_ref, f.provider_key, f.arrival_ordinal, f.observed_at,
                   f.provider_timestamp, f.kind, f.closing, f.raw_kind, f.subordinate_ref,
                   f.tool_call_ref, f.body_digest, f.body_bytes, f.body_media_type,
                   f.body_excerpt, f.body_artifact_ref, f.redactions::jsonb, clock_timestamp(),
                   $28
            FROM unnest(
                $4::uuid[], $5::uuid[], $6::uuid[], $7::bigint[], $8::uuid[], $9::bigint[],
                $10::text[], $11::text[], $12::text[], $13::text[], $14::bigint[],
                $15::timestamptz[], $16::timestamptz[], $17::text[], $18::boolean[],
                $19::text[], $20::text[], $21::text[], $22::text[], $23::integer[],
                $24::text[], $25::bytea[], $26::text[], $27::text[]
            ) AS f(
                frame_id, run_id, activation_id, attempt_no, harness_execution_id, generation,
                lane_profile, native_session_ref, native_turn_ref, provider_key,
                arrival_ordinal, observed_at, provider_timestamp, kind, closing, raw_kind,
                subordinate_ref, tool_call_ref, body_digest, body_bytes, body_media_type,
                body_excerpt, body_artifact_ref, redactions
            )
            ON CONFLICT (harness_execution_id, generation, provider_key) DO NOTHING
            RETURNING frame_id
            """,
            *args,
            [frame.frame_id for frame in fresh],
            [frame.run_id for frame in fresh],
            [frame.activation_id for frame in fresh],
            [frame.attempt_no for frame in fresh],
            [frame.harness_execution_id for frame in fresh],
            [frame.generation for frame in fresh],
            [frame.lane_profile.value for frame in fresh],
            [frame.native_session_ref for frame in fresh],
            [frame.native_turn_ref for frame in fresh],
            [frame.provider_key for frame in fresh],
            [frame.arrival_ordinal for frame in fresh],
            [frame.observed_at for frame in fresh],
            [frame.provider_timestamp for frame in fresh],
            [frame.kind.value for frame in fresh],
            [frame.closing for frame in fresh],
            [frame.raw_kind for frame in fresh],
            [frame.subordinate_ref for frame in fresh],
            [frame.tool_call_ref for frame in fresh],
            [frame.body_digest for frame in fresh],
            [frame.body_bytes for frame in fresh],
            [frame.body_media_type for frame in fresh],
            [frame.body_excerpt.encode("utf-8") for frame in fresh],
            [frame.body_artifact_ref for frame in fresh],
            [
                json.dumps([item.model_dump(mode="json") for item in frame.redactions])
                for frame in fresh
            ],
            self._actor,
        )
        new_ids = {row["frame_id"] for row in inserted}
        stored = [frame for frame in fresh if frame.frame_id in new_ids]
        for frame in stored:
            if frame.kind in IDENTITY_KINDS:
                await identity.apply(connection, args, execution, frame, actor_ref=self._actor)
        if stored:
            last = stored[-1]
            await connection.execute(
                f"""
                UPDATE mission_control.harness_execution
                SET observation_cursor = $5, updated_at = clock_timestamp()
                WHERE {SCOPE} AND harness_execution_id = $4
                """,
                *args,
                harness_execution_id,
                json.dumps(
                    {
                        "generation": last.generation,
                        "arrival_ordinal": last.arrival_ordinal,
                        "provider_key": last.provider_key,
                    },
                    sort_keys=True,
                ),
            )
        return AppendReceipt(
            new=len(stored),
            duplicate=len(fresh) - len(stored),
            stale=stale,
            new_frame_ids=tuple(frame.frame_id for frame in stored),
        )

    async def last_cursor(
        self, harness_execution_id: UUID, generation: int, *, request_scope: str
    ) -> ProviderCursor | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await begin(connection, request_scope)
            return await _last_cursor(connection, harness_execution_id, generation)

    # --- FrameReader ------------------------------------------------------------------

    async def frames_for_execution(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int = 0,
        closing_only: bool = False,
        limit: int = 1_000,
    ) -> tuple[ProviderFrame, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT {FRAME_COLUMNS} FROM mission_control.provider_frame
                WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5
                  AND arrival_ordinal > $6 AND (closing OR NOT $7)
                ORDER BY arrival_ordinal
                LIMIT $8
                """,
                *args,
                harness_execution_id,
                generation,
                after_ordinal,
                closing_only,
                limit,
            )
        return tuple(frame_from_row(row) for row in rows)

    async def frames_for_run(
        self,
        request_scope: str,
        run_id: UUID,
        *,
        closing_only: bool = False,
        limit: int = 10_000,
    ) -> tuple[ProviderFrame, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT {FRAME_COLUMNS} FROM mission_control.provider_frame
                WHERE {SCOPE} AND run_id = $4 AND (closing OR NOT $5)
                ORDER BY observed_at, harness_execution_id, generation, arrival_ordinal
                LIMIT $6
                """,
                *args,
                run_id,
                closing_only,
                limit,
            )
        return tuple(frame_from_row(row) for row in rows)

    async def frames_by_id(
        self, request_scope: str, frame_ids: Sequence[UUID]
    ) -> tuple[ProviderFrame, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT {FRAME_COLUMNS} FROM mission_control.provider_frame
                WHERE {SCOPE} AND frame_id = ANY($4::uuid[])
                """,
                *args,
                list(frame_ids),
            )
        return tuple(frame_from_row(row) for row in rows)

    async def current_generation(
        self, request_scope: str, harness_execution_id: UUID
    ) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            value = await connection.fetchval(
                f"""
                SELECT generation FROM mission_control.harness_execution
                WHERE {SCOPE} AND harness_execution_id = $4
                """,
                *args,
                harness_execution_id,
            )
        return int(value) if value is not None else None

    async def run_uuid(self, request_scope: str, run_key: str) -> UUID | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            value = await connection.fetchval(
                f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
                *args,
                run_key,
            )
        return value if isinstance(value, UUID) else None


async def _last_cursor(
    connection: asyncpg.Connection, harness_execution_id: UUID, generation: int
) -> ProviderCursor | None:
    row = await connection.fetchrow(
        """
        SELECT arrival_ordinal, provider_key FROM mission_control.provider_frame
        WHERE harness_execution_id = $1 AND generation = $2
        ORDER BY arrival_ordinal DESC
        LIMIT 1
        """,
        harness_execution_id,
        generation,
    )
    if row is None:
        return None
    return ProviderCursor(
        harness_execution_id=harness_execution_id,
        generation=generation,
        arrival_ordinal=int(row["arrival_ordinal"]),
        provider_key=row["provider_key"],
    )
