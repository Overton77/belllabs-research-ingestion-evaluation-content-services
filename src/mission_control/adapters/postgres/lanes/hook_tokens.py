"""PostgreSQL hook task tokens and Operation Intents (migration 0030, G3 section; FT-G3).

`hook_task_token` stores only the token digest with its secret-free context; lookups run
under the claimed scope's forced RLS, so a token presented in another scope is unknown.
`hook_effect_intent` records the Operation Intent a permission Kernel Hook admits, once per
(run, generation, effect ref); a repeat returns the stored intent.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.application.execution.harness.hook_callbacks import (
    HookEffectIntent,
    HookTaskToken,
    HookTokenContext,
)
from mission_control.contracts.identities import uuid7


def _token(row: asyncpg.Record) -> HookTaskToken:
    context = row["context"]
    context = json.loads(context) if isinstance(context, str) else dict(context)
    return HookTaskToken(
        token_hash=row["token_hash"],
        context=HookTokenContext.model_validate(context),
        issued_at=row["issued_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
    )


class PostgresHookTokenStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def put(self, token: HookTaskToken) -> None:
        context = token.context
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, context.request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.hook_task_token (
                    installation_id, application_id, tenant_id, token_hash,
                    harness_execution_id, generation, run_key, lane_profile, context,
                    issued_at, expires_at, revoked_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, NULL)
                ON CONFLICT (token_hash) DO NOTHING
                """,
                *args,
                token.token_hash,
                context.harness_execution_id,
                context.generation,
                context.run_id,
                context.lane_profile,
                context.model_dump_json(),
                token.issued_at,
                token.expires_at,
            )

    async def get(self, request_scope: str, token_hash: str) -> HookTaskToken | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                SELECT token_hash, context, issued_at, expires_at, revoked_at
                FROM mission_control.hook_task_token WHERE {SCOPE} AND token_hash = $4
                """,
                *args,
                token_hash,
            )
        return None if row is None else _token(row)

    async def latest_generation(self, request_scope: str, harness_execution_id: UUID) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            value = await connection.fetchval(
                f"""
                SELECT max(generation) FROM mission_control.hook_task_token
                WHERE {SCOPE} AND harness_execution_id = $4
                """,
                *args,
                harness_execution_id,
            )
        return None if value is None else int(value)

    async def revoke(
        self, request_scope: str, harness_execution_id: UUID, generation: int, at: datetime
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.hook_task_token SET revoked_at = $6
                WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5
                  AND revoked_at IS NULL
                """,
                *args,
                harness_execution_id,
                generation,
                at,
            )


def _intent(row: asyncpg.Record, request_scope: str) -> HookEffectIntent:
    return HookEffectIntent(
        request_scope=request_scope,
        run_id=row["run_key"],
        generation=int(row["generation"]),
        harness_execution_id=row["harness_execution_id"],
        effect_ref=row["effect_ref"],
        effect_kind=row["effect_kind"],
        hook_event=row["hook_event"],
        lane_profile=row["lane_profile"],
        input_digest=row["input_digest"],
        recorded_at=row["recorded_at"],
    )


class PostgresHookIntentLedger:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record(self, intent: HookEffectIntent) -> HookEffectIntent:
        async with self._pool.acquire() as connection, connection.transaction():
            args: tuple[Any, ...] = await begin(connection, intent.request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.hook_effect_intent (
                    installation_id, application_id, tenant_id, intent_id, run_key,
                    generation, harness_execution_id, effect_ref, effect_kind, hook_event,
                    lane_profile, input_digest, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (installation_id, application_id, tenant_id, run_key, generation,
                    effect_ref) DO NOTHING
                """,
                *args,
                uuid7(),
                intent.run_id,
                intent.generation,
                intent.harness_execution_id,
                intent.effect_ref,
                intent.effect_kind,
                intent.hook_event,
                intent.lane_profile,
                intent.input_digest,
                intent.recorded_at,
            )
            row = await connection.fetchrow(
                f"""
                SELECT run_key, generation, harness_execution_id, effect_ref, effect_kind,
                       hook_event, lane_profile, input_digest, recorded_at
                FROM mission_control.hook_effect_intent
                WHERE {SCOPE} AND run_key = $4 AND generation = $5 AND effect_ref = $6
                """,
                *args,
                intent.run_id,
                intent.generation,
                intent.effect_ref,
            )
        if row is None:
            raise LookupError("the Operation Intent was not recorded")
        return _intent(row, intent.request_scope)


__all__ = ["PostgresHookIntentLedger", "PostgresHookTokenStore"]
