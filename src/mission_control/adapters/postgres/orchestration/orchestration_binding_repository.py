"""Frozen run semantic input bindings as canonical immutable execution bindings."""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import scoped
from mission_control.application.programs.orchestration_binding_repository import (
    SemanticInputBindingConflict,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.programs.bindings import RunSemanticInputBinding

BINDING_CONTRACT = "mc.semantic-input-binding/1"


class PostgresRunSemanticInputBindingRepository:
    """RLS-scoped durable storage for immutable run semantic bindings."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(
        self,
        binding: RunSemanticInputBinding,
    ) -> RunSemanticInputBinding:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, binding.request_scope)
            await mc.advisory_lock(
                connection, f"semantic-binding:{binding.request_scope}:{binding.run_id}"
            )
            prior = await _binding_row(connection, args, binding.run_id)
            if prior is not None:
                persisted = RunSemanticInputBinding.model_validate(_json(prior["manifest"]))
                if prior["manifest_digest"] != binding.binding_digest or persisted != binding:
                    raise SemanticInputBindingConflict(
                        "Workflow Run already has a different semantic input binding"
                    )
                return persisted
            run = await mc.require_run(connection, args, binding.run_id)
            await connection.execute(
                """
                INSERT INTO mission_control.execution_binding (
                    installation_id, application_id, tenant_id, execution_binding_id,
                    binding_key, revision_id, run_id, program_node_id, subordinate_id,
                    binding_contract, manifest, manifest_ref, manifest_digest,
                    admission_decision, admitted_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9::jsonb, NULL, $10,
                        'admitted', $11, $11, $12)
                """,
                *args,
                uuid7(),
                binding.binding_id,
                run["revision_id"],
                run["run_id"],
                BINDING_CONTRACT,
                _dump(binding),
                binding.binding_digest,
                binding.created_at,
                mc.WRITER_REF,
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
            args = await mc.begin(connection, request_scope)
            row = await _binding_row(connection, args, run_id, binding_key=binding_id)
        if row is None:
            return None
        return RunSemanticInputBinding.model_validate(_json(row["manifest"]))

    async def get_for_run(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _binding_row(connection, args, run_id)
        if row is None:
            return None
        return RunSemanticInputBinding.model_validate(_json(row["manifest"]))


async def semantic_binding_row(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str
) -> asyncpg.Record | None:
    """The run's semantic binding manifest and digest (shared with fork source reads)."""

    return await _binding_row(connection, args, run_key)


async def _binding_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run_key: str,
    *,
    binding_key: str | None = None,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT binding.manifest, binding.manifest_digest
        FROM mission_control.execution_binding binding
        JOIN mission_control.mission_run run
          ON run.installation_id = binding.installation_id
         AND run.application_id = binding.application_id
         AND run.tenant_id = binding.tenant_id AND run.run_id = binding.run_id
        WHERE {scoped("binding")} AND run.run_key = $4 AND binding.binding_contract = $5
          AND ($6::text IS NULL OR binding.binding_key = $6)
        """,
        *args,
        run_key,
        BINDING_CONTRACT,
        binding_key,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
