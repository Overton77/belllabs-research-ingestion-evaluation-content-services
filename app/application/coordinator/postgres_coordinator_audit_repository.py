from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.application.coordinator.coordinator_facade import CoordinatorAuditEvent
from app.application.capability.postgres_capability_search_repository import PostgresPool


class PostgresCoordinatorAuditSink:
    """Durably append digest-only coordinator audit events under tenant RLS."""

    def __init__(self, pool: PostgresPool) -> None:
        self._pool = pool

    async def emit(self, event: CoordinatorAuditEvent) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)",
                event.tenant_scope,
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.coordinator_audit_events (
                    event_id,
                    occurred_at,
                    operation,
                    actor_id,
                    tenant_scope,
                    outcome,
                    correlation_id,
                    request_digest,
                    response_digest,
                    error_code
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (event_id) DO NOTHING
                """,
                UUID(event.event_id),
                event.occurred_at,
                event.operation,
                event.actor_id,
                event.tenant_scope,
                event.outcome,
                event.correlation_id,
                event.request_digest,
                event.response_digest,
                event.error_code,
            )

    async def list_events(
        self,
        *,
        tenant_scope: str,
        actor_id: str,
        occurred_since: datetime,
    ) -> tuple[CoordinatorAuditEvent, ...]:
        """Read back non-secret audit metadata for an acceptance or support trace."""

        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)",
                tenant_scope,
            )
            rows = await connection.fetch(
                """
                SELECT
                    event_id,
                    occurred_at,
                    operation,
                    actor_id,
                    tenant_scope,
                    outcome,
                    correlation_id,
                    request_digest,
                    response_digest,
                    error_code
                FROM belllabs_control.coordinator_audit_events
                WHERE tenant_scope = $1
                  AND actor_id = $2
                  AND occurred_at >= $3
                ORDER BY occurred_at ASC, event_id ASC
                """,
                tenant_scope,
                actor_id,
                occurred_since,
            )
        return tuple(
            CoordinatorAuditEvent(
                event_id=str(row["event_id"]),
                occurred_at=row["occurred_at"],
                operation=str(row["operation"]),
                actor_id=str(row["actor_id"]),
                tenant_scope=str(row["tenant_scope"]),
                outcome=str(row["outcome"]),
                correlation_id=str(row["correlation_id"]),
                request_digest=str(row["request_digest"]),
                response_digest=(
                    str(row["response_digest"]) if row["response_digest"] is not None else None
                ),
                error_code=(str(row["error_code"]) if row["error_code"] is not None else None),
            )
            for row in rows
        )


__all__ = ["PostgresCoordinatorAuditSink"]
