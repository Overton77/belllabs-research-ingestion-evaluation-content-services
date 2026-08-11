from __future__ import annotations

import json
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from neo4j import READ_ACCESS, AsyncDriver, Query

from app.application.schema.graph_query import (
    compile_query_intent,
    intent_digest,
    validate_query_intent,
)
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    QueryExecutionIntent,
    QueryExecutionResult,
    SchemaOperationProjection,
)
from app.domain.schema_context.errors import QueryIntentRejected


def _sanitize(value: Any, policy: dict[str, int], depth: int = 0) -> Any:
    if depth > 6:
        return "[depth-truncated]"
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key in list(value)[: policy["max_map_keys"]]:
            if "embedding" in str(key).lower():
                continue
            result[str(key)] = _sanitize(value[key], policy, depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        return [
            _sanitize(item, policy, depth + 1) for item in list(value)[: policy["max_list_items"]]
        ]
    if isinstance(value, str):
        return value[: policy["max_string_chars"]]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if hasattr(value, "items"):
        return _sanitize(dict(value), policy, depth + 1)
    return str(value)[: policy["max_string_chars"]]


def _redacted_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    return {
        key: "[redacted]"
        if any(term in key.lower() for term in ("password", "credential", "token", "embedding"))
        else value
        for key, value in parameters.items()
    }


class Neo4jReadExecutor:
    def __init__(self, driver: AsyncDriver, *, database: str = "neo4j") -> None:
        self._driver = driver
        self.database = database
        self.call_count = 0

    async def capability_snapshot(self) -> tuple[tuple[dict, ...], dict[str, Any]]:
        async with self._driver.session(
            database=self.database, default_access_mode=READ_ACCESS
        ) as session:
            indexes_result = await session.run("SHOW INDEXES")
            indexes = tuple(
                _sanitize(item, _snapshot_policy()) for item in await indexes_result.data()
            )
            node_result = await session.run(
                "MATCH (n) UNWIND labels(n) AS label UNWIND keys(n) AS key "
                "WITH label, key, n LIMIT 2000 "
                "RETURN label, key, collect(DISTINCT valueType(n[key]))[..10] AS observed_types"
            )
            relationship_result = await session.run(
                "MATCH (a)-[r]->(b) "
                "RETURN labels(a) AS source_labels, type(r) AS relationship_type, "
                "labels(b) AS target_labels, count(*) AS observed_count LIMIT 500"
            )
            live_schema = {
                "node_properties": [
                    _sanitize(item, _snapshot_policy()) for item in await node_result.data()
                ],
                "relationship_topology": [
                    _sanitize(item, _snapshot_policy()) for item in await relationship_result.data()
                ],
            }
        return indexes, live_schema

    async def execute(
        self,
        intent: QueryExecutionIntent,
        projection: SchemaOperationProjection,
    ) -> QueryExecutionResult:
        started = datetime.now(UTC)
        start = perf_counter()
        digest = intent_digest(intent)
        result_id = str(uuid5(NAMESPACE_URL, f"query-result:{digest}"))
        cypher: str | None = None
        parameters: dict[str, Any] = {}
        status = "failed"
        columns: tuple[str, ...] = ()
        records: tuple[dict[str, Any], ...] = ()
        diagnostics: tuple[str, ...] = ()
        error_type: str | None = None
        truncated = False
        server_info: dict[str, str] = {}
        try:
            validate_query_intent(intent, projection)
            cypher, parameters = compile_query_intent(intent)
            self.call_count += 1
            async with self._driver.session(
                database=self.database, default_access_mode=READ_ACCESS
            ) as session:
                query = Query(cypher, timeout=projection.timeout_seconds)
                cursor = await session.run(query, parameters)
                raw = await cursor.data()
                keys = tuple(cursor.keys())
            policy = projection.result_policy
            bounded: list[dict[str, Any]] = []
            total_bytes = 0
            for item in raw[: policy["max_records"]]:
                clean = _sanitize(item, policy)
                size = len(json.dumps(clean, ensure_ascii=False, default=str).encode())
                if total_bytes + size > policy["max_total_bytes"]:
                    truncated = True
                    break
                bounded.append(clean)
                total_bytes += size
            truncated = truncated or len(raw) > len(bounded)
            columns = keys
            records = tuple(bounded)
            status = "succeeded"
            info = await self._driver.get_server_info()
            server_info = {
                "agent": str(getattr(info, "agent", "unknown")),
                "protocol_version": str(getattr(info, "protocol_version", "unknown")),
            }
        except QueryIntentRejected as error:
            status = "rejected"
            error_type = type(error).__name__
            diagnostics = (str(error)[:500],)
        except Exception as error:  # driver failures must become typed, sanitized evidence
            status = "failed"
            error_type = type(error).__name__
            diagnostics = ("Neo4j read execution failed; connection details were suppressed",)
        finished = datetime.now(UTC)
        logical = {
            "result_id": result_id,
            "intent_id": intent.intent_id,
            "intent_digest": digest,
            "query_kind": intent.query_kind,
            "status": status,
            "compiled_cypher": cypher,
            "redacted_parameters": _redacted_parameters(parameters),
            "columns": columns,
            "records": records,
            "record_count": len(records),
            "truncated": truncated,
            "elapsed_ms": max(0, int((perf_counter() - start) * 1000)),
            "database": self.database,
            "server_info": server_info,
            "diagnostics": diagnostics,
            "error_type": error_type,
            "started_at": started,
            "finished_at": finished,
        }
        return QueryExecutionResult(
            **logical,
            result_digest=sha256_digest(
                {
                    **logical,
                    "started_at": started.isoformat(),
                    "finished_at": finished.isoformat(),
                }
            ),
        )


def _snapshot_policy() -> dict[str, int]:
    return {
        "max_records": 2000,
        "max_total_bytes": 500000,
        "max_string_chars": 1000,
        "max_list_items": 100,
        "max_map_keys": 100,
    }


def rejected_query_result(intent: QueryExecutionIntent, reason: str) -> QueryExecutionResult:
    now = datetime.now(UTC)
    digest = intent_digest(intent)
    logical: dict[str, Any] = {
        "result_id": str(uuid5(NAMESPACE_URL, f"query-result:{digest}:rejected")),
        "intent_id": intent.intent_id,
        "intent_digest": digest,
        "query_kind": intent.query_kind,
        "status": "rejected",
        "compiled_cypher": None,
        "redacted_parameters": {},
        "columns": (),
        "records": (),
        "record_count": 0,
        "truncated": False,
        "elapsed_ms": 0,
        "database": None,
        "server_info": {},
        "diagnostics": (reason[:500],),
        "error_type": "QueryIntentRejected",
        "started_at": now,
        "finished_at": now,
    }
    return QueryExecutionResult(
        **logical,
        result_digest=sha256_digest(
            {**logical, "started_at": now.isoformat(), "finished_at": now.isoformat()}
        ),
    )
