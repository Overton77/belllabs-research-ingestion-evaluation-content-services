from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.async_subagents import AsyncSubagentSpawnRequest
from app.domain.operation_execution.contracts import AsyncSubagentMessage


class PostgresAsyncSubagentAuthority:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def reserve_and_admit(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None:
        now = request.requested_at
        command_id = str(
            uuid5(NAMESPACE_URL, f"async-admit:{request.request_scope}:{child_execution_id}")
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)", request.request_scope
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_authority
                (request_scope, child_execution_id, parent_run_id, parent_operation_id, link_id,
                 contract_id, contract_digest, reservation_id, dependency_class,
                 execution_generation, created_at, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
                ON CONFLICT (request_scope, child_execution_id) DO NOTHING""",
                request.request_scope,
                child_execution_id,
                request.parent_run_id,
                request.parent_operation_id,
                link_id,
                request.contract.contract_id,
                request.contract.contract_digest,
                request.reservation_id,
                request.dependency_class.value,
                request.execution_generation,
                now,
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_commands
                (command_id, request_scope, child_execution_id, command_kind, payload, recorded_at)
                VALUES ($1,$2,$3,'admit',$4::jsonb,$5) ON CONFLICT (command_id) DO NOTHING""",
                command_id,
                request.request_scope,
                child_execution_id,
                json.dumps({"reservation_id": request.reservation_id, "link_id": link_id}),
                now,
            )

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None:
        fact_id = str(
            uuid5(
                NAMESPACE_URL,
                f"async-fact:{request_scope}:{child_execution_id}:{fact_kind}:{fact_ref}",
            )
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)", request_scope
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_facts
                (fact_id, request_scope, child_execution_id, fact_kind, fact_ref, recorded_at)
                VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (fact_id) DO NOTHING""",
                fact_id,
                request_scope,
                child_execution_id,
                fact_kind,
                fact_ref,
                datetime.now(UTC),
            )

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)", request_scope
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_messages
                (message_id, request_scope, child_execution_id, direction,
                 target_sequence, receipt, payload, recorded_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) ON CONFLICT (message_id) DO NOTHING""",
                message.message_id,
                request_scope,
                message.child_execution_id,
                message.direction,
                message.target_sequence,
                message.receipt,
                json.dumps(message.model_dump(mode="json")),
                message.created_at,
            )

    async def request_cancellation(
        self, request_scope: str, child_execution_id: str, reason: str
    ) -> None:
        await self._command(
            request_scope, child_execution_id, "cancel", {"reason": reason}, cancellation=True
        )

    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        manifest_digest: str,
    ) -> None:
        await self._command(
            request_scope,
            child_execution_id,
            "result_decision",
            {"decision": decision, "manifest_digest": manifest_digest},
            decision=decision,
            manifest_digest=manifest_digest,
        )

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str
    ) -> None:
        await self._command(
            request_scope,
            child_execution_id,
            "settle",
            {"settlement_ref": settlement_ref},
            settlement_ref=settlement_ref,
        )

    async def _command(
        self,
        request_scope: str,
        child_execution_id: str,
        kind: str,
        payload: dict[str, object],
        *,
        cancellation: bool = False,
        decision: str | None = None,
        manifest_digest: str | None = None,
        settlement_ref: str | None = None,
    ) -> None:
        command_id = str(
            uuid5(
                NAMESPACE_URL,
                f"async-command:{request_scope}:{child_execution_id}:{kind}:{payload}",
            )
        )
        now = datetime.now(UTC)
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', $1, true)", request_scope
            )
            await connection.execute(
                """UPDATE belllabs_control.async_subagent_authority SET
                cancellation_requested = cancellation_requested OR $3,
                result_decision = COALESCE($4, result_decision),
                result_manifest_digest = COALESCE($5, result_manifest_digest),
                settlement_ref = COALESCE($6, settlement_ref), updated_at = $7
                WHERE request_scope=$1 AND child_execution_id=$2""",
                request_scope,
                child_execution_id,
                cancellation,
                decision,
                manifest_digest,
                settlement_ref,
                now,
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_commands
                (command_id, request_scope, child_execution_id, command_kind, payload, recorded_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT (command_id) DO NOTHING""",
                command_id,
                request_scope,
                child_execution_id,
                kind,
                json.dumps(payload),
                now,
            )
