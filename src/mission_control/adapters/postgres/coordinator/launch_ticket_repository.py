from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.contracts.identities import uuid7
from mission_control.domain.coordinator.launch import (
    LaunchIdempotencyConflict,
    LaunchTicketNotFound,
    LaunchTicketState,
    LaunchTicketUnavailable,
    PreparedLaunchTicket,
)

TICKET_CONTRACT = "mc.coordinator-launch-ticket/1"


class PostgresLaunchTicketRepository:
    """Tenant-scoped CAS persistence for caller-bound coordinator launch tickets."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(self, ticket: PreparedLaunchTicket) -> PreparedLaunchTicket:
        lock_key = (
            f"coordinator-ticket:{ticket.tenant_scope}:{ticket.caller_id}:"
            f"{ticket.idempotency_issuer}:{ticket.idempotency_key}"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, ticket.request_scope)
            await mc.advisory_lock(connection, lock_key)
            prior = await connection.fetchrow(
                f"""
                SELECT proposal_digest, ticket_payload
                FROM mission_control.coordinator_launch_ticket
                WHERE {SCOPE} AND tenant_scope = $4 AND caller_key = $5
                  AND idempotency_issuer = $6 AND idempotency_key = $7
                """,
                *args,
                ticket.tenant_scope,
                ticket.caller_id,
                ticket.idempotency_issuer,
                ticket.idempotency_key,
            )
            if prior is not None:
                persisted = PreparedLaunchTicket.model_validate(_json(prior["ticket_payload"]))
                if (
                    prior["proposal_digest"] != ticket.proposal_digest
                    or persisted.semantic_binding_plan_digest != ticket.semantic_binding_plan_digest
                ):
                    raise LaunchIdempotencyConflict(
                        "launch idempotency identity was reused with a changed proposal "
                        "or semantic binding plan"
                    )
                return persisted
            await connection.execute(
                """
                INSERT INTO mission_control.coordinator_launch_ticket (
                    installation_id, application_id, tenant_id, coordinator_launch_ticket_id,
                    ticket_key, tenant_scope, caller_key, state, prepared_at, expires_at,
                    proposal_digest, blueprint_family, initial_goal_digest,
                    effective_configuration_digest, run_request_digest, idempotency_issuer,
                    idempotency_key, ticket_contract, ticket_payload, version, updated_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                        $17, $18, $19::jsonb, 1, $9, $9, $7)
                """,
                *args,
                uuid7(),
                str(UUID(ticket.ticket_id)),
                ticket.tenant_scope,
                ticket.caller_id,
                ticket.state.value,
                ticket.prepared_at,
                ticket.expires_at,
                ticket.proposal_digest,
                ticket.blueprint_family.value,
                ticket.initial_goal_digest,
                ticket.effective_configuration_digest,
                ticket.run_request_digest,
                ticket.idempotency_issuer,
                ticket.idempotency_key,
                TICKET_CONTRACT,
                _dump(ticket),
            )
        return ticket

    async def get(
        self,
        ticket_id: str,
        *,
        request_scope: str,
    ) -> PreparedLaunchTicket | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT ticket_payload FROM mission_control.coordinator_launch_ticket
                WHERE {SCOPE} AND ticket_key = $4
                """,
                *args,
                str(UUID(ticket_id)),
            )
        return PreparedLaunchTicket.model_validate(_json(payload)) if payload is not None else None

    async def expire(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        observed_at: datetime,
    ) -> PreparedLaunchTicket:
        return await self._transition(
            ticket_id,
            request_scope=request_scope,
            target=LaunchTicketState.EXPIRED,
            observed_at=observed_at,
        )

    async def invalidate(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        reason: str,
    ) -> PreparedLaunchTicket:
        if not reason.strip():
            raise ValueError("ticket invalidation requires a reason")
        return await self._transition(
            ticket_id,
            request_scope=request_scope,
            target=LaunchTicketState.INVALIDATED,
            invalidation_reason=reason,
        )

    async def consume(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        run_id: str,
        consumed_at: datetime,
    ) -> PreparedLaunchTicket:
        return await self._transition(
            ticket_id,
            request_scope=request_scope,
            target=LaunchTicketState.CONSUMED,
            consumed_run_id=run_id,
            consumed_at=consumed_at,
        )

    async def _transition(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        target: LaunchTicketState,
        observed_at: datetime | None = None,
        consumed_run_id: str | None = None,
        consumed_at: datetime | None = None,
        invalidation_reason: str | None = None,
    ) -> PreparedLaunchTicket:
        ticket_key = str(UUID(ticket_id))
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"coordinator-ticket-transition:{ticket_key}")
            row = await connection.fetchrow(
                f"""
                SELECT ticket_payload, version FROM mission_control.coordinator_launch_ticket
                WHERE {SCOPE} AND ticket_key = $4
                FOR UPDATE
                """,
                *args,
                ticket_key,
            )
            if row is None:
                raise LaunchTicketNotFound(f"launch ticket not found: {ticket_id}")
            ticket = PreparedLaunchTicket.model_validate(_json(row["ticket_payload"]))
            if ticket.state == target:
                if (
                    target == LaunchTicketState.CONSUMED
                    and ticket.consumed_run_id != consumed_run_id
                ):
                    raise LaunchIdempotencyConflict(
                        "launch ticket was consumed by a different Workflow Run"
                    )
                return ticket
            if ticket.state != LaunchTicketState.PREPARED:
                raise LaunchTicketUnavailable(
                    f"cannot transition a {ticket.state.value} launch ticket"
                )
            if target == LaunchTicketState.CONSUMED:
                updated = ticket.model_copy(
                    update={
                        "state": target,
                        "consumed_run_id": consumed_run_id,
                        "consumed_at": consumed_at,
                    }
                )
            elif target == LaunchTicketState.INVALIDATED:
                updated = ticket.model_copy(
                    update={
                        "state": target,
                        "invalidation_reason": invalidation_reason,
                    }
                )
            else:
                updated = ticket.model_copy(update={"state": target})
            await connection.execute(
                f"""
                UPDATE mission_control.coordinator_launch_ticket
                SET state = $5, consumed_run_key = $6, consumed_at = $7,
                    invalidation_reason = $8, ticket_payload = $9::jsonb,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE {SCOPE} AND ticket_key = $4 AND state = 'prepared' AND version = $10
                """,
                *args,
                ticket_key,
                updated.state.value,
                updated.consumed_run_id,
                updated.consumed_at,
                updated.invalidation_reason,
                _dump(updated),
                row["version"],
            )
            return updated


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, tuple):
        value = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in value
        ]
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
