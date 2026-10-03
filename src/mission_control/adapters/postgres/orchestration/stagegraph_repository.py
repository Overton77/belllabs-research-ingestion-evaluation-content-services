"""PostgreSQL persistence for immutable, scope-bound StageGraph templates."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.execution.contracts import OperationExecutionRequest


def _key(binding: str, operation: str) -> str:
    return json.dumps((binding, operation), separators=(",", ":"))


class PostgresStageGraphOperationTemplateRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._documents = PostgresDocumentStore(pool)

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        templates: Mapping[str, OperationExecutionRequest],
        recorded_at: datetime,
    ) -> None:
        if not templates:
            raise ValueError("StageGraph operation templates cannot be empty")
        async with self._pool.acquire() as connection, connection.transaction():
            await self._documents.set_scope(connection, request_scope)
            for key, template in sorted(templates.items()):
                if not key:
                    raise ValueError("StageGraph operation template keys cannot be empty")
                if template.request_scope != request_scope:
                    raise ValueError(
                        "StageGraph operation template belongs to another request scope"
                    )
                await self._documents.put_on(
                    connection,
                    request_scope=request_scope,
                    contract="stagegraph.template/1",
                    identity=_key(semantic_input_binding_ref, key),
                    payload=stable_json_dump(template),
                    recorded_at=recorded_at,
                )

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_request_key: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        document = await self._documents.get(
            request_scope=request_scope,
            contract="stagegraph.template/1",
            identity=_key(semantic_input_binding_ref, operation_request_key),
        )
        if document is None:
            raise ValueError("StageGraph operation template is unavailable")
        template = OperationExecutionRequest.model_validate(document.payload)
        if template.request_scope != request_scope:
            raise ValueError("StageGraph operation template belongs to another request scope")
        return template

    async def list_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
    ) -> dict[str, OperationExecutionRequest]:
        documents = await self._documents.list(
            request_scope=request_scope,
            contract="stagegraph.template/1",
        )
        result: dict[str, OperationExecutionRequest] = {}
        for document in documents:
            binding, key = json.loads(document.identity)
            if binding != semantic_input_binding_ref:
                continue
            template = OperationExecutionRequest.model_validate(document.payload)
            if template.request_scope != request_scope:
                raise ValueError("StageGraph operation template belongs to another request scope")
            result[key] = template
        return result
