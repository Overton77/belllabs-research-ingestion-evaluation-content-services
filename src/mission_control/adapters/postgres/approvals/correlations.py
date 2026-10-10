"""Native approval correlations (PROPOSED 0033 ``approval_correlation``; MP-11).

One row per native request delivery and connection. A correlation is closed exactly once
(compare-and-set from ``live``) as ``answered`` (with the reply actually sent), ``expired``
(the bounded wait elapsed; the durable task stays open), ``superseded`` (a newer delivery of
the same task opened) or ``lost`` (its process died; ``mark_lost`` after a restart). A closed
correlation never reopens (trigger in the proposed DDL), so an opaque connection-scoped
handle is never reused.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.execution.approvals import (
    ApprovalCorrelation,
    CorrelationState,
    NativeReply,
)
from mission_control.domain.execution.approvals import NativeApprovalCorrelation

ACTOR_REF = "mission-control-runtime/approval-broker"
_COLUMNS = """
    approval_correlation_id, human_task_id, run_key, harness_execution_id, generation,
    connection_ref, native, input_digest, state, reply, close_reason, replayed_from,
    opened_at, wait_deadline_at, closed_at, version
"""


class PostgresApprovalCorrelationRepository:
    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = ACTOR_REF) -> None:
        self._pool = pool
        self._actor_ref = actor_ref

    async def open_correlation(self, correlation: ApprovalCorrelation) -> ApprovalCorrelation:
        scope = correlation.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            # Serialize with resolution/wait on the task row (the same lock reviewers take).
            await connection.execute(
                f"""
                SELECT 1 FROM mission_control.human_task
                WHERE {SCOPE} AND human_task_id = $4
                FOR UPDATE
                """,
                *args,
                UUID(correlation.human_task_id),
            )
            await connection.execute(
                f"""
                UPDATE mission_control.approval_correlation
                SET state = 'superseded', closed_at = $5,
                    close_reason = 'correlation_superseded', version = version + 1
                WHERE {SCOPE} AND human_task_id = $4 AND state = 'live'
                """,
                *args,
                UUID(correlation.human_task_id),
                correlation.opened_at,
            )
            await connection.execute(
                """
                INSERT INTO mission_control.approval_correlation (
                    installation_id, application_id, tenant_id, approval_correlation_id,
                    human_task_id, run_key, harness_execution_id, generation, connection_ref,
                    native, input_digest, state, opened_at, wait_deadline_at, version,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, 'live', $12, $13,
                        1, $14)
                """,
                *args,
                UUID(correlation.correlation_id),
                UUID(correlation.human_task_id),
                correlation.run_id,
                correlation.harness_execution_id,
                correlation.generation,
                correlation.connection_ref,
                _dump(correlation.native),
                correlation.input_digest,
                correlation.opened_at,
                correlation.wait_deadline_at,
                self._actor_ref,
            )
            row = await _row(connection, args, correlation.correlation_id)
        assert row is not None
        return _correlation(row, scope)

    async def get_correlation(
        self, request_scope: str, correlation_id: str
    ) -> ApprovalCorrelation | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _row(connection, args, correlation_id)
        return _correlation(row, request_scope) if row is not None else None

    async def close_correlation(
        self,
        request_scope: str,
        correlation_id: str,
        *,
        state: CorrelationState,
        at: datetime,
        reason: str,
        reply: NativeReply | None = None,
        replayed_from: str | None = None,
    ) -> ApprovalCorrelation | None:
        if state == "live":
            raise ValueError("a correlation closes into a non-live state")
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            updated = await connection.fetchval(
                f"""
                UPDATE mission_control.approval_correlation
                SET state = $5, closed_at = $6, close_reason = $7, reply = $8::jsonb,
                    reply_digest = $9, replayed_from = $10, version = version + 1
                WHERE {SCOPE} AND approval_correlation_id = $4 AND state = 'live'
                RETURNING version
                """,
                *args,
                UUID(correlation_id),
                state,
                at,
                reason,
                _dump(reply) if reply is not None else None,
                reply.digest if reply is not None else None,
                replayed_from,
            )
            if updated is None:
                return None
            row = await _row(connection, args, correlation_id)
        assert row is not None
        return _correlation(row, request_scope)

    async def mark_lost(
        self,
        request_scope: str,
        *,
        harness_execution_id: str,
        except_connection_ref: str,
        at: datetime,
    ) -> tuple[ApprovalCorrelation, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                UPDATE mission_control.approval_correlation
                SET state = 'lost', closed_at = $6, close_reason = 'correlation_lost',
                    version = version + 1
                WHERE {SCOPE} AND harness_execution_id = $4 AND connection_ref <> $5
                  AND state = 'live'
                RETURNING {_COLUMNS}
                """,
                *args,
                harness_execution_id,
                except_connection_ref,
                at,
            )
        return tuple(
            sorted(
                (_correlation(row, request_scope) for row in rows),
                key=lambda item: item.opened_at,
            )
        )


async def _row(
    connection: asyncpg.Connection, args: tuple[Any, ...], correlation_id: str
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT {_COLUMNS} FROM mission_control.approval_correlation
        WHERE {SCOPE} AND approval_correlation_id = $4
        """,
        *args,
        UUID(correlation_id),
    )


def _correlation(row: asyncpg.Record, request_scope: str) -> ApprovalCorrelation:
    reply = _json(row["reply"])
    return ApprovalCorrelation(
        correlation_id=str(row["approval_correlation_id"]),
        request_scope=request_scope,
        human_task_id=str(row["human_task_id"]),
        run_id=row["run_key"],
        harness_execution_id=row["harness_execution_id"],
        generation=int(row["generation"]),
        connection_ref=row["connection_ref"],
        native=NativeApprovalCorrelation.model_validate(_json(row["native"])),
        input_digest=row["input_digest"],
        state=row["state"],
        opened_at=row["opened_at"],
        wait_deadline_at=row["wait_deadline_at"],
        closed_at=row["closed_at"],
        close_reason=row["close_reason"],
        reply=NativeReply.model_validate(reply) if reply is not None else None,
        replayed_from=row["replayed_from"],
        version=int(row["version"]),
    )


def _dump(value: Any) -> str:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


__all__ = ["PostgresApprovalCorrelationRepository"]
