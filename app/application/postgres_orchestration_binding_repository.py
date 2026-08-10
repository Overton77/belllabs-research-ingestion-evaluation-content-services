from __future__ import annotations

import json
from typing import Any

import asyncpg

from app.application.orchestration_binding_repository import (
    SemanticInputBindingConflict,
)
from app.domain.orchestration.bindings import RunSemanticInputBinding


class PostgresRunSemanticInputBindingRepository:
    """RLS-scoped durable storage for immutable run semantic bindings."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(
        self,
        binding: RunSemanticInputBinding,
    ) -> RunSemanticInputBinding:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, binding.request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"semantic-binding:{binding.request_scope}:{binding.run_id}",
            )
            prior = await connection.fetchrow(
                """
                SELECT binding_digest, binding_payload
                FROM belllabs_control.workflow_semantic_input_bindings
                WHERE request_scope = $1 AND run_id = $2
                """,
                binding.request_scope,
                binding.run_id,
            )
            if prior is not None:
                persisted = RunSemanticInputBinding.model_validate(_json(prior["binding_payload"]))
                if prior["binding_digest"] != binding.binding_digest or persisted != binding:
                    raise SemanticInputBindingConflict(
                        "Workflow Run already has a different semantic input binding"
                    )
                return persisted
            await connection.execute(
                """
                INSERT INTO belllabs_control.workflow_semantic_input_bindings (
                    binding_id, request_scope, run_id, blueprint_family,
                    effective_configuration_digest, blueprint_digest,
                    binding_digest, binding_payload, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
                """,
                binding.binding_id,
                binding.request_scope,
                binding.run_id,
                binding.blueprint_family,
                binding.effective_configuration_digest,
                binding.blueprint_digest,
                binding.binding_digest,
                _dump(binding),
                binding.created_at,
            )
        return binding

    async def get(
        self,
        binding_id: str,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT binding_payload
                FROM belllabs_control.workflow_semantic_input_bindings
                WHERE binding_id = $1 AND request_scope = $2 AND run_id = $3
                """,
                binding_id,
                request_scope,
                run_id,
            )
        if payload is None:
            return None
        return RunSemanticInputBinding.model_validate(_json(payload))

    async def get_for_run(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT binding_payload
                FROM belllabs_control.workflow_semantic_input_bindings
                WHERE request_scope = $1 AND run_id = $2
                """,
                request_scope,
                run_id,
            )
        if payload is None:
            return None
        return RunSemanticInputBinding.model_validate(_json(payload))


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
