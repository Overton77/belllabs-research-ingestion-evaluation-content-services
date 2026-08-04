from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from app.domain.coordinator.launch import (
    LaunchIdempotencyConflict,
    LaunchTicketNotFound,
    LaunchTicketState,
    LaunchTicketUnavailable,
    PreparedLaunchTicket,
)


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
            await _set_scope(connection, ticket.request_scope)
            await _advisory_lock(connection, lock_key)
            prior = await connection.fetchrow(
                """
                SELECT proposal_digest, ticket_payload
                FROM belllabs_control.coordinator_launch_tickets
                WHERE tenant_scope = $1 AND caller_id = $2
                  AND idempotency_issuer = $3 AND idempotency_key = $4
                """,
                ticket.tenant_scope,
                ticket.caller_id,
                ticket.idempotency_issuer,
                ticket.idempotency_key,
            )
            if prior is not None:
                persisted = PreparedLaunchTicket.model_validate(
                    _json(prior["ticket_payload"])
                )
                if (
                    prior["proposal_digest"] != ticket.proposal_digest
                    or persisted.semantic_binding_plan_digest
                    != ticket.semantic_binding_plan_digest
                ):
                    raise LaunchIdempotencyConflict(
                        "launch idempotency identity was reused with a changed proposal "
                        "or semantic binding plan"
                    )
                return persisted
            await connection.execute(
                """
                INSERT INTO belllabs_control.coordinator_launch_tickets (
                    ticket_id, tenant_scope, caller_id, request_scope, state,
                    prepared_at, expires_at, proposal_digest, workflow_type_ref,
                    blueprint_ref, blueprint_family, initial_goal, initial_goal_digest,
                    effective_configuration_digest, run_request_digest,
                    resolved_asset_refs, authority_decisions, availability_decisions,
                    approval_refs, policy_snapshot_digest, environment_snapshot_digest,
                    warnings, launchable, idempotency_issuer, idempotency_key,
                    frozen_run_request, ticket_payload
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb,
                    $10::jsonb, $11, $12, $13, $14, $15,
                    $16::jsonb, $17::jsonb, $18::jsonb, $19::jsonb, $20, $21,
                    $22::jsonb, $23, $24, $25, $26::jsonb, $27::jsonb
                )
                """,
                UUID(ticket.ticket_id),
                ticket.tenant_scope,
                ticket.caller_id,
                ticket.request_scope,
                ticket.state.value,
                ticket.prepared_at,
                ticket.expires_at,
                ticket.proposal_digest,
                _dump(ticket.workflow_type_ref),
                _dump(ticket.blueprint_ref),
                ticket.blueprint_family.value,
                ticket.initial_goal,
                ticket.initial_goal_digest,
                ticket.effective_configuration_digest,
                ticket.run_request_digest,
                _dump(ticket.resolved_asset_refs),
                _dump(ticket.authority_decisions),
                _dump(ticket.availability_decisions),
                _dump(ticket.approval_refs),
                ticket.policy_snapshot_digest,
                ticket.environment_snapshot_digest,
                _dump(ticket.warnings),
                ticket.launchable,
                ticket.idempotency_issuer,
                ticket.idempotency_key,
                _dump(ticket.frozen_run_request),
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
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT ticket_payload
                FROM belllabs_control.coordinator_launch_tickets
                WHERE ticket_id = $1 AND request_scope = $2
                """,
                UUID(ticket_id),
                request_scope,
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
        lock_key = f"coordinator-ticket-transition:{ticket_id}"
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _advisory_lock(connection, lock_key)
            row = await connection.fetchrow(
                """
                SELECT ticket_payload
                FROM belllabs_control.coordinator_launch_tickets
                WHERE ticket_id = $1 AND request_scope = $2
                FOR UPDATE
                """,
                UUID(ticket_id),
                request_scope,
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
                """
                UPDATE belllabs_control.coordinator_launch_tickets
                SET state = $2, consumed_run_id = $3, consumed_at = $4,
                    invalidation_reason = $5, ticket_payload = $6::jsonb
                WHERE ticket_id = $1 AND state = 'prepared'
                """,
                UUID(ticket_id),
                updated.state.value,
                updated.consumed_run_id,
                updated.consumed_at,
                updated.invalidation_reason,
                _dump(updated),
            )
            return updated


async def _advisory_lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key)


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, tuple):
        value = [
            item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            for item in value
        ]
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
