from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from mission_control.adapters.postgres.capability.capability_search_repository import PostgresPool
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.coordinator.launch import WorkflowResultRecord

_OPENAI_KEY = re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{12,}\b")
_BEARER_TOKEN = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}\b", re.IGNORECASE)
_INLINE_SECRET = re.compile(
    r"\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|password|secret)"
    r"\s*[:=]\s*\S+",
    re.IGNORECASE,
)
RESULT_CONTRACT = "mc.coordinator-workflow-result/1"
_SECRET_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "access_token",
        "auth_token",
        "password",
        "secret",
        "openai_api_key",
    }
)


class PostgresWorkflowResultRepository:
    """Append one immutable, secret-free typed result for each terminal run."""

    def __init__(self, pool: PostgresPool) -> None:
        self._pool = pool

    async def save(self, result: WorkflowResultRecord) -> WorkflowResultRecord:
        payload = result.model_dump(mode="json")
        if _contains_secret_material(payload):
            raise ValueError("typed Workflow Result contains secret material")
        payload_json = _dump(payload)
        result_digest = sha256_digest(payload)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await _set_scope(
                connection,
                tenant_scope=result.tenant_scope,
                request_scope=result.request_scope,
            )
            run = await mc.run_row(connection, args, result.run_id)
            if run is None or str(run["phase"]) != "terminal":
                raise ValueError("typed Workflow Result requires its terminal Workflow Run")
            row = await connection.fetchrow(
                """
                INSERT INTO mission_control.coordinator_workflow_result (
                    installation_id, application_id, tenant_id, coordinator_workflow_result_id,
                    run_key, tenant_scope, blueprint_family, terminal_outcome, completed_at,
                    result_digest, result_contract, result_payload, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $9, $13)
                ON CONFLICT (installation_id, application_id, tenant_id, run_key) DO NOTHING
                RETURNING result_digest, result_payload
                """,
                *args,
                uuid7(),
                result.run_id,
                result.tenant_scope,
                result.blueprint_family.value,
                result.terminal_outcome.value,
                result.completed_at,
                result_digest,
                RESULT_CONTRACT,
                payload_json,
                mc.WRITER_REF,
            )
            if row is None:
                row = await _result_row(connection, args, result.tenant_scope, result.run_id)
            if row is None:
                raise ValueError("typed Workflow Result identity conflicts across scopes")
            persisted = _record(
                row,
                tenant_scope=result.tenant_scope,
                request_scope=result.request_scope,
                run_id=result.run_id,
            )
            if persisted != result:
                raise ValueError("typed Workflow Result is immutable")
            return persisted

    async def get(
        self,
        tenant_scope: str,
        request_scope: str,
        run_id: str,
    ) -> WorkflowResultRecord | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await _set_scope(
                connection,
                tenant_scope=tenant_scope,
                request_scope=request_scope,
            )
            row = await _result_row(connection, args, tenant_scope, run_id)
        if row is None:
            return None
        return _record(
            row,
            tenant_scope=tenant_scope,
            request_scope=request_scope,
            run_id=run_id,
        )


async def _result_row(
    connection: Any, args: tuple[Any, ...], tenant_scope: str, run_id: str
) -> Any:
    return await connection.fetchrow(
        f"""
        SELECT result_digest, result_payload FROM mission_control.coordinator_workflow_result
        WHERE {SCOPE} AND run_key = $4 AND tenant_scope = $5
        """,
        *args,
        run_id,
        tenant_scope,
    )


async def _set_scope(
    connection: Any,
    *,
    tenant_scope: str,
    request_scope: str,
) -> tuple[Any, ...]:
    """Both scopes must name the same canonical installation, application and tenant."""

    if parse_request_scope(tenant_scope) != parse_request_scope(request_scope):
        raise ValueError("workflow result tenant scope must match its request scope")
    return await mc.begin(connection, request_scope)


def _record(
    row: Mapping[str, Any],
    *,
    tenant_scope: str,
    request_scope: str,
    run_id: str,
) -> WorkflowResultRecord:
    payload = _json(row["result_payload"])
    result_digest = str(row["result_digest"])
    if sha256_digest(payload) != result_digest:
        raise RuntimeError("persisted typed Workflow Result digest is invalid")
    result = WorkflowResultRecord.model_validate(payload)
    if (
        result.tenant_scope != tenant_scope
        or result.request_scope != request_scope
        or result.run_id != run_id
    ):
        raise RuntimeError("persisted typed Workflow Result scope is invalid")
    if _contains_secret_material(payload):
        raise RuntimeError("persisted typed Workflow Result contains secret material")
    return result


def _contains_secret_material(value: object) -> bool:
    if isinstance(value, str):
        return any(
            pattern.search(value) is not None
            for pattern in (_OPENAI_KEY, _BEARER_TOKEN, _INLINE_SECRET)
        )
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized_key = str(key).casefold().replace("-", "_")
            if normalized_key in _SECRET_KEYS and nested not in (None, "", (), [], {}):
                return True
            if _contains_secret_material(nested):
                return True
        return False
    if isinstance(value, list | tuple | set | frozenset):
        return any(_contains_secret_material(item) for item in value)
    return False


def _dump(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: object) -> object:
    return json.loads(value) if isinstance(value, str) else value


__all__ = ["PostgresWorkflowResultRepository"]
