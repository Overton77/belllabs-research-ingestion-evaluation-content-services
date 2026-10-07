from __future__ import annotations

import json
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

import asyncpg
from redis.asyncio import Redis

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.execution.contracts import (
    RuntimeApprovalDecision,
    RuntimeApprovalRequest,
)
from mission_control.domain.policies.errors import IdempotencyConflict

# Durable approvals are canonical mission_control.human_task rows (typed inline request
# packet) answered once by mission_control.human_resolution; Redis only notifies.
# The former PostgreSQL+Redis runtime event bus and the HMAC checkpoint store had no
# production caller and are retired; LangGraph checkpoints belong to the private runtime
# persistence schema, not to business authority.
_SCOPE = "installation_id = $1 AND application_id = $2 AND tenant_id = $3"
APPROVAL_KIND = "runtime_approval"
APPROVAL_PREFIX = "runtime-approval:"
_WRITER = "mission-control-runtime/1"


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
            args = await _scope(connection, request.request_scope)
            prior = await _approval_row(connection, args, request.approval_id)
            if prior is not None:
                persisted = RuntimeApprovalRequest.model_validate(_json(prior["request_packet"]))
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
                    INSERT INTO mission_control.human_task (
                        installation_id, application_id, tenant_id, human_task_id, task_key,
                        target_ref, kind, request_packet_ref, assignee_scope, deadline_at,
                        on_timeout, lifecycle, request_packet, version, updated_at, created_at,
                        created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'expire', 'open',
                            $11::jsonb, 1, $12, $12, $13)
                    """,
                    *args,
                    uuid7(),
                    APPROVAL_PREFIX + request.approval_id,
                    f"binding:{request.binding_id}",
                    APPROVAL_KIND,
                    f"runtime-approval-request@{sha256_digest(stable_json_dump(request))}",
                    request.request_scope,
                    request.expires_at,
                    json.dumps(payload),
                    request.requested_at,
                    _WRITER,
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
            args = await _scope(connection, request.request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.human_task
                SET lifecycle = 'expired', version = version + 1, updated_at = clock_timestamp()
                WHERE {_SCOPE} AND task_key = $4 AND kind = $5 AND lifecycle = 'open'
                """,
                *args,
                APPROVAL_PREFIX + request.approval_id,
                APPROVAL_KIND,
            )
        raise TimeoutError("runtime approval expired")

    async def decide(self, decision: RuntimeApprovalDecision) -> RuntimeApprovalDecision:
        payload = decision.model_dump(mode="json")
        expired = False
        async with self._pool.acquire() as connection, connection.transaction():
            args = await _scope(connection, decision.request_scope)
            request = await _approval_row(connection, args, decision.approval_id, lock=True)
            if request is None or request["target_ref"] != f"binding:{decision.binding_id}":
                raise ValueError("approval request was not found in the authorized scope")
            if request["deadline_at"] <= datetime.now(UTC):
                await connection.execute(
                    f"""
                    UPDATE mission_control.human_task
                    SET lifecycle = 'expired', version = version + 1,
                        updated_at = clock_timestamp()
                    WHERE {_SCOPE} AND human_task_id = $4 AND lifecycle = 'open'
                    """,
                    *args,
                    request["human_task_id"],
                )
                expired = True
            else:
                if request["answer"] is not None:
                    persisted = RuntimeApprovalDecision.model_validate(_json(request["answer"]))
                    comparable = persisted.model_copy(update={"decided_at": decision.decided_at})
                    if comparable != decision:
                        raise IdempotencyConflict("approval already has a different decision")
                    return persisted
                if request["lifecycle"] != "open":
                    raise ValueError("approval request is no longer pending")
                await connection.execute(
                    """
                    INSERT INTO mission_control.human_resolution (
                        installation_id, application_id, tenant_id, resolution_id,
                        human_task_id, actor_ref, answer, answer_ref, answer_digest,
                        expected_task_version, decided_at, created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NULL, $8, $9, $10, $10, $6)
                    """,
                    *args,
                    uuid7(),
                    request["human_task_id"],
                    _decision_actor(payload),
                    json.dumps(payload),
                    sha256_digest(stable_json_dump(decision)),
                    request["version"],
                    decision.decided_at,
                )
                await connection.execute(
                    f"""
                    UPDATE mission_control.human_task
                    SET lifecycle = 'resolved', version = version + 1, updated_at = $5
                    WHERE {_SCOPE} AND human_task_id = $4 AND version = $6
                    """,
                    *args,
                    request["human_task_id"],
                    decision.decided_at,
                    request["version"],
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
            args = await _scope(connection, request_scope)
            row = await _approval_row(connection, args, approval_id)
        if row is None or row["answer"] is None:
            return None
        return RuntimeApprovalDecision.model_validate(_json(row["answer"]))


async def _scope(connection: asyncpg.Connection, request_scope: str) -> tuple[Any, ...]:
    scope = await apply_scope(connection, request_scope)
    return scope.installation_id, scope.application_id, scope.tenant_id


async def _approval_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    approval_id: str,
    *,
    lock: bool = False,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        """
        SELECT task.human_task_id, task.target_ref, task.lifecycle, task.deadline_at,
               task.version, task.request_packet, resolution.answer
        FROM mission_control.human_task task
        LEFT JOIN mission_control.human_resolution resolution
          ON resolution.installation_id = task.installation_id
         AND resolution.application_id = task.application_id
         AND resolution.tenant_id = task.tenant_id
         AND resolution.human_task_id = task.human_task_id
        WHERE task.installation_id = $1 AND task.application_id = $2 AND task.tenant_id = $3
          AND task.task_key = $4 AND task.kind = $5
        """
        + (" FOR UPDATE OF task" if lock else ""),
        *args,
        APPROVAL_PREFIX + approval_id,
        APPROVAL_KIND,
    )


def _decision_actor(payload: dict[str, Any]) -> str:
    return str(payload["actor_id"])


def _approval_channel(request_scope: str, binding_id: str) -> str:
    return f"belllabs:approval:{request_scope}:{binding_id}"


def _decision_key(approval_id: str) -> str:
    return f"belllabs:approval-decision:{approval_id}"


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
