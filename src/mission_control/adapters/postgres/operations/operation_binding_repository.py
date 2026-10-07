"""PostgreSQL immutable operation bindings and settlements for fresh executions."""

from __future__ import annotations

import json

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
)
from mission_control.domain.policies.errors import IdempotencyConflict


class PostgresOperationBindingRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._documents = PostgresDocumentStore(pool)

    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None:
        document = await self._documents.get(
            request_scope=request_scope,
            contract="operation.binding/1",
            identity=semantic_attempt_key,
        )
        if document is None:
            return None
        binding = OperationExecutionBinding.model_validate(document.payload)
        if (
            binding.request_scope != request_scope
            or binding.semantic_attempt_key != semantic_attempt_key
        ):
            raise ValueError("stored operation binding identity does not match scoped key")
        return binding

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None:
        index = await self._documents.get(
            request_scope=request_scope,
            contract="operation.binding-index/1",
            identity=binding_id,
        )
        if index is None:
            return None
        binding = await self.get_binding(
            index.payload["semantic_attempt_key"], request_scope=request_scope
        )
        if binding is None or binding.binding_id != binding_id:
            raise ValueError("stored operation binding index does not match binding")
        return binding

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding:
        if binding.request_scope != request_scope:
            raise ValueError("operation binding write cannot cross request scope")
        async with self._pool.acquire() as connection, connection.transaction():
            await self._documents.set_scope(connection, request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"operation-binding:{request_scope}:{binding.semantic_attempt_key}",
            )
            scope = parse_request_scope(request_scope)
            row = await connection.fetchrow(
                """SELECT payload, digest FROM mission_control.runtime_document
                   WHERE installation_id = $1 AND application_id = $2 AND tenant_id = $3
                     AND contract = 'operation.binding/1' AND identity = $4""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                binding.semantic_attempt_key,
            )
            if row is not None:
                payload = row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if sha256_digest(payload) != row["digest"]:
                    raise ValueError("operation binding payload digest mismatch")
                prior = OperationExecutionBinding.model_validate(payload)
                if prior.request_scope != request_scope:
                    raise ValueError("stored operation binding belongs to another request scope")
                if prior.request_fingerprint != binding.request_fingerprint:
                    raise IdempotencyConflict(
                        "semantic operation binding has a conflicting fingerprint"
                    )
                return prior
            await self._documents.put_on(
                connection,
                request_scope=request_scope,
                contract="operation.binding/1",
                identity=binding.semantic_attempt_key,
                payload=stable_json_dump(binding),
                recorded_at=binding.bound_at,
            )
            await self._documents.put_on(
                connection,
                request_scope=request_scope,
                contract="operation.binding-index/1",
                identity=binding.binding_id,
                payload={"semantic_attempt_key": binding.semantic_attempt_key},
                recorded_at=binding.bound_at,
            )
            return binding

    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationSettlement | None:
        document = await self._documents.get(
            request_scope=request_scope,
            contract="operation.settlement/1",
            identity=binding_id,
        )
        if document is None:
            return None
        return OperationSettlement.model_validate(
            {**document.payload, "settled_at": document.recorded_at},
        )

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            await self._documents.set_scope(connection, binding.request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"operation-claim:{binding.request_scope}:{binding.side_effect_key}",
            )
            scope = parse_request_scope(binding.request_scope)
            exists = await connection.fetchval(
                """SELECT EXISTS (SELECT 1 FROM mission_control.runtime_document
                   WHERE installation_id = $1 AND application_id = $2 AND tenant_id = $3
                     AND contract = 'operation.claim/1' AND identity = $4)""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                binding.side_effect_key,
            )
            await self._documents.put_on(
                connection,
                request_scope=binding.request_scope,
                contract="operation.claim/1",
                identity=binding.side_effect_key,
                payload={"binding_id": binding.binding_id},
                recorded_at=binding.bound_at,
            )
            return not exists

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str,
    ) -> OperationSettlement:
        binding = await self.get_binding_by_id(settlement.binding_id, request_scope=request_scope)
        if binding is None:
            raise ValueError("operation settlement binding is outside request scope")
        document = await self._documents.put(
            request_scope=request_scope,
            contract="operation.settlement/1",
            identity=settlement.binding_id,
            payload=stable_json_dump(settlement, exclude={"settled_at"}),
            recorded_at=settlement.settled_at,
        )
        return OperationSettlement.model_validate(
            {**document.payload, "settled_at": document.recorded_at},
        )
