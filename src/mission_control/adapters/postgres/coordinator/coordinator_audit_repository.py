from __future__ import annotations

from datetime import datetime
from uuid import UUID

from mission_control.adapters.postgres.capability.capability_search_repository import PostgresPool
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.coordinator.coordinator_facade import CoordinatorAuditEvent
from mission_control.contracts.identities import uuid7


class PostgresCoordinatorAuditSink:
    """Durably append digest-only coordinator audit events under forced tenant RLS."""

    def __init__(self, pool: PostgresPool) -> None:
        self._pool = pool

    async def emit(self, event: CoordinatorAuditEvent) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, event.tenant_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.coordinator_audit_event (
                    installation_id, application_id, tenant_id, coordinator_audit_event_id,
                    event_key, tenant_scope, occurred_at, operation, actor_key, outcome,
                    correlation_key, request_digest, response_digest, error_code, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $7, $9)
                ON CONFLICT (installation_id, application_id, tenant_id, event_key) DO NOTHING
                """,
                *args,
                uuid7(),
                str(UUID(event.event_id)),
                event.tenant_scope,
                event.occurred_at,
                event.operation,
                event.actor_id,
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
            args = await mc.begin(connection, tenant_scope)
            rows = await connection.fetch(
                f"""
                SELECT event_key, occurred_at, operation, actor_key, tenant_scope, outcome,
                       correlation_key, request_digest, response_digest, error_code
                FROM mission_control.coordinator_audit_event
                WHERE {SCOPE} AND tenant_scope = $4 AND actor_key = $5 AND occurred_at >= $6
                ORDER BY occurred_at ASC, event_key ASC
                """,
                *args,
                tenant_scope,
                actor_id,
                occurred_since,
            )
        return tuple(
            CoordinatorAuditEvent(
                event_id=str(row["event_key"]),
                occurred_at=row["occurred_at"],
                operation=str(row["operation"]),
                actor_id=str(row["actor_key"]),
                tenant_scope=str(row["tenant_scope"]),
                outcome=str(row["outcome"]),
                correlation_id=str(row["correlation_key"]),
                request_digest=str(row["request_digest"]),
                response_digest=(
                    str(row["response_digest"]) if row["response_digest"] is not None else None
                ),
                error_code=(str(row["error_code"]) if row["error_code"] is not None else None),
            )
            for row in rows
        )


__all__ = ["PostgresCoordinatorAuditSink"]
