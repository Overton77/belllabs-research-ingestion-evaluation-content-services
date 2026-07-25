from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app.integrations.neo4j_read_executor import Neo4jReadExecutor
from tests.test_graph_query_intents import _intent


class _Cursor:
    def __init__(self, records: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
        self.records = records
        self.columns = columns

    async def data(self) -> list[dict[str, Any]]:
        return self.records

    def keys(self) -> tuple[str, ...]:
        return self.columns


class _Session:
    def __init__(self, driver: _Driver) -> None:
        self.driver = driver

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def run(self, query: Any, parameters: dict[str, Any] | None = None) -> _Cursor:
        text = str(query)
        self.driver.queries.append(text)
        if self.driver.failure is not None:
            raise self.driver.failure
        if text == "SHOW INDEXES":
            return _Cursor([{"name": "OrganizationName", "state": "ONLINE"}], ("name",))
        if "valueType(n[key])" in text:
            return _Cursor(
                [{"label": "Organization", "key": "searchFields", "observed_types": ["LIST"]}],
                ("label", "key", "observed_types"),
            )
        if "relationship_topology" in text or "MATCH (a)-[r]->(b)" in text:
            return _Cursor([], ("source_labels", "relationship_type", "target_labels"))
        return _Cursor(self.driver.records, ("entity",))


class _Driver:
    def __init__(
        self,
        records: list[dict[str, Any]] | None = None,
        *,
        failure: Exception | None = None,
    ) -> None:
        self.records = records or []
        self.failure = failure
        self.queries: list[str] = []

    def session(self, **_kwargs: Any) -> _Session:
        return _Session(self)

    async def get_server_info(self) -> SimpleNamespace:
        return SimpleNamespace(agent="fake-neo4j", protocol_version=(6, 0))


@pytest.mark.asyncio
async def test_executor_bounds_records_lists_and_removes_embeddings() -> None:
    records = [
        {
            "entity": {
                "id": str(index),
                "name": f"Entity {index}",
                "searchEmbedding": [0.1, 0.2],
                "searchFields": [f"field-{item}" for item in range(60)],
            }
        }
        for index in range(101)
    ]
    driver = _Driver(records)
    intent, projection = _intent(limit=100)

    result = await Neo4jReadExecutor(driver).execute(intent, projection)

    assert result.status == "succeeded"
    assert result.record_count == 100
    assert result.truncated
    assert "searchEmbedding" not in result.records[0]["entity"]
    assert len(result.records[0]["entity"]["searchFields"]) == 50


@pytest.mark.asyncio
async def test_capability_snapshot_handles_list_properties_without_scalar_stringification() -> None:
    driver = _Driver()

    indexes, live_schema = await Neo4jReadExecutor(driver).capability_snapshot()

    assert indexes[0]["name"] == "OrganizationName"
    assert live_schema["node_properties"][0]["observed_types"] == ["LIST"]
    assert all("toString(" not in query for query in driver.queries)


@pytest.mark.asyncio
async def test_driver_failure_is_not_reported_as_a_successful_zero_match() -> None:
    intent, projection = _intent()
    driver = _Driver(failure=RuntimeError("database unavailable at secret location"))

    result = await Neo4jReadExecutor(driver).execute(intent, projection)

    assert result.status == "failed"
    assert result.record_count == 0
    assert result.error_type == "RuntimeError"
    assert result.diagnostics == (
        "Neo4j read execution failed; connection details were suppressed",
    )
