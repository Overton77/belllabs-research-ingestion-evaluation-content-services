from __future__ import annotations

import hmac
import json
from collections.abc import Awaitable
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, cast

import asyncpg
from redis.asyncio import Redis

from app.domain.operation_execution.contracts import (
    RuntimeApprovalDecision,
    RuntimeApprovalRequest,
    RuntimeEventEnvelope,
)
from app.domain.run_control.errors import IdempotencyConflict


class PostgresRedisRuntimeEventBus:
    """Durable lifecycle events in Postgres with Redis fan-out for realtime clients."""

    def __init__(self, pool: asyncpg.Pool, redis: Redis) -> None:
        self._pool = pool
        self._redis = redis

    async def latest_sequence(self, request_scope: str, binding_id: str) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            value = await connection.fetchval(
                """
                SELECT COALESCE(MAX(sequence), 0)
                FROM belllabs_control.agent_runtime_events
                WHERE request_scope = $1 AND binding_id = $2
                """,
                request_scope,
                binding_id,
            )
        return int(value)

    async def publish(self, request_scope: str, envelope: RuntimeEventEnvelope) -> None:
        payload = envelope.model_dump(mode="json")
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            prior = await connection.fetchval(
                """
                SELECT envelope FROM belllabs_control.agent_runtime_events
                WHERE request_scope = $1 AND event_id = $2
                """,
                request_scope,
                envelope.event_id,
            )
            if prior is not None:
                if _json(prior) != payload:
                    raise IdempotencyConflict("runtime event identity has conflicting payload")
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.agent_runtime_events
                        (event_id, request_scope, binding_id, run_id, operation_id,
                         sequence, event_type, envelope, occurred_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
                    """,
                    envelope.event_id,
                    request_scope,
                    envelope.binding_id,
                    envelope.run_id,
                    envelope.operation_id,
                    envelope.sequence,
                    envelope.event_type,
                    json.dumps(payload),
                    envelope.occurred_at,
                )
        await self._redis.publish(
            _event_channel(request_scope, envelope.run_id),
            json.dumps(payload, separators=(",", ":")),
        )

    async def publish_ephemeral(
        self,
        *,
        request_scope: str,
        run_id: str,
        payload: dict[str, object],
    ) -> None:
        await self._redis.publish(
            _event_channel(request_scope, run_id),
            json.dumps(payload, separators=(",", ":")),
        )


class PostgresRedisApprovalGateway:
    """Durable approval decisions with Redis notification; no process-local wait state."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        redis: Redis,
        *,
        checkpoint_signing_key: bytes,
    ) -> None:
        if len(checkpoint_signing_key) < 32:
            raise ValueError("checkpoint signing key must contain at least 32 bytes")
        self._pool = pool
        self._redis = redis
        self._checkpoint_signing_key = checkpoint_signing_key

    async def request(self, request: RuntimeApprovalRequest) -> RuntimeApprovalDecision:
        payload = request.model_dump(mode="json")
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request.request_scope)
            prior = await connection.fetchval(
                """
                SELECT request_payload
                FROM belllabs_control.agent_runtime_approval_requests
                WHERE request_scope = $1 AND approval_id = $2
                """,
                request.request_scope,
                request.approval_id,
            )
            if prior is not None:
                persisted = RuntimeApprovalRequest.model_validate(_json(prior))
                comparable = persisted.model_copy(
                    update={
                        "requested_at": request.requested_at,
                        "expires_at": request.expires_at,
                    }
                )
                if comparable != request:
                    raise IdempotencyConflict("approval identity has conflicting request")
                request = persisted
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.agent_runtime_approval_requests
                        (approval_id, request_scope, binding_id, request_payload,
                         requested_at, expires_at)
                    VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                    """,
                    request.approval_id,
                    request.request_scope,
                    request.binding_id,
                    json.dumps(payload),
                    request.requested_at,
                    request.expires_at,
                )
        await self._redis.publish(
            _approval_channel(request.request_scope, request.binding_id),
            json.dumps(payload, separators=(",", ":")),
        )

        timeout = max(0, int((request.expires_at - datetime.now(UTC)).total_seconds()))
        while timeout > 0:
            decision = await self.get_decision(request.request_scope, request.approval_id)
            if decision is not None:
                return decision
            started = datetime.now(UTC)
            await cast(
                Awaitable[Any],
                self._redis.blpop(
                    [_decision_key(request.approval_id)],
                    timeout=min(timeout, 5),
                ),
            )
            timeout -= max(1, int((datetime.now(UTC) - started).total_seconds()))

        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request.request_scope)
            await connection.execute(
                """
                UPDATE belllabs_control.agent_runtime_approval_requests
                SET status = 'expired'
                WHERE request_scope = $1 AND approval_id = $2 AND status = 'pending'
                """,
                request.request_scope,
                request.approval_id,
            )
        raise TimeoutError("runtime approval expired")

    async def decide(self, decision: RuntimeApprovalDecision) -> RuntimeApprovalDecision:
        payload = decision.model_dump(mode="json")
        expired = False
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, decision.request_scope)
            request = await connection.fetchrow(
                """
                SELECT binding_id, status, expires_at
                FROM belllabs_control.agent_runtime_approval_requests
                WHERE request_scope = $1 AND approval_id = $2
                FOR UPDATE
                """,
                decision.request_scope,
                decision.approval_id,
            )
            if request is None or request["binding_id"] != decision.binding_id:
                raise ValueError("approval request was not found in the authorized scope")
            if request["expires_at"] <= datetime.now(UTC):
                await connection.execute(
                    """
                    UPDATE belllabs_control.agent_runtime_approval_requests
                    SET status = 'expired'
                    WHERE request_scope = $1 AND approval_id = $2
                    """,
                    decision.request_scope,
                    decision.approval_id,
                )
                expired = True
            else:
                prior = await connection.fetchval(
                    """
                    SELECT decision_payload
                    FROM belllabs_control.agent_runtime_approval_decisions
                    WHERE request_scope = $1 AND approval_id = $2
                    """,
                    decision.request_scope,
                    decision.approval_id,
                )
                if prior is not None:
                    persisted = RuntimeApprovalDecision.model_validate(_json(prior))
                    comparable = persisted.model_copy(update={"decided_at": decision.decided_at})
                    if comparable != decision:
                        raise IdempotencyConflict("approval already has a different decision")
                    return persisted
                if request["status"] != "pending":
                    raise ValueError("approval request is no longer pending")
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.agent_runtime_approval_decisions
                        (approval_id, request_scope, decision_payload, decided_at)
                    VALUES ($1, $2, $3::jsonb, $4)
                    """,
                    decision.approval_id,
                    decision.request_scope,
                    json.dumps(payload),
                    decision.decided_at,
                )
                await connection.execute(
                    """
                    UPDATE belllabs_control.agent_runtime_approval_requests
                    SET status = $3 WHERE request_scope = $1 AND approval_id = $2
                    """,
                    decision.request_scope,
                    decision.approval_id,
                    decision.decision,
                )
        if expired:
            raise ValueError("approval request has expired")
        await cast(
            Awaitable[Any],
            self._redis.lpush(_decision_key(decision.approval_id), decision.decision),
        )
        await self._redis.expire(_decision_key(decision.approval_id), 300)
        return decision

    async def get_decision(
        self, request_scope: str, approval_id: str
    ) -> RuntimeApprovalDecision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT decision_payload
                FROM belllabs_control.agent_runtime_approval_decisions
                WHERE request_scope = $1 AND approval_id = $2
                """,
                request_scope,
                approval_id,
            )
        return RuntimeApprovalDecision.model_validate(_json(payload)) if payload else None

    async def save_checkpoint(
        self,
        *,
        request_scope: str,
        binding_id: str,
        state_json: str,
        status: str = "awaiting_approval",
    ) -> None:
        state_mac = hmac.new(
            self._checkpoint_signing_key,
            f"{request_scope}:{binding_id}:".encode() + state_json.encode(),
            sha256,
        ).hexdigest()
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """
                INSERT INTO belllabs_control.agent_runtime_checkpoints
                    (binding_id, request_scope, state_json, state_mac, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (request_scope, binding_id) DO UPDATE
                SET state_json = EXCLUDED.state_json,
                    state_mac = EXCLUDED.state_mac,
                    status = EXCLUDED.status,
                    updated_at = clock_timestamp()
                """,
                binding_id,
                request_scope,
                state_json,
                state_mac,
                status,
            )

    async def load_checkpoint(self, request_scope: str, binding_id: str) -> str | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT state_json, state_mac, status
                FROM belllabs_control.agent_runtime_checkpoints
                WHERE request_scope = $1 AND binding_id = $2
                """,
                request_scope,
                binding_id,
            )
        if row is None or row["status"] == "completed":
            return None
        actual = hmac.new(
            self._checkpoint_signing_key,
            f"{request_scope}:{binding_id}:".encode() + row["state_json"].encode(),
            sha256,
        ).hexdigest()
        if not hmac.compare_digest(actual, row["state_mac"]):
            raise ValueError("runtime checkpoint authentication failed")
        return row["state_json"]

    async def complete_checkpoint(self, request_scope: str, binding_id: str) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """
                UPDATE belllabs_control.agent_runtime_checkpoints
                SET status = 'completed', updated_at = clock_timestamp()
                WHERE request_scope = $1 AND binding_id = $2
                """,
                request_scope,
                binding_id,
            )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _event_channel(request_scope: str, run_id: str) -> str:
    return f"belllabs:runtime:{request_scope}:{run_id}"


def _approval_channel(request_scope: str, binding_id: str) -> str:
    return f"belllabs:approval:{request_scope}:{binding_id}"


def _decision_key(approval_id: str) -> str:
    return f"belllabs:approval-decision:{approval_id}"


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
