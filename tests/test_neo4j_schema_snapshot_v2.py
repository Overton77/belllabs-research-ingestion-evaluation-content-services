from __future__ import annotations

from typing import Any

import pytest
from neo4j import READ_ACCESS

from app.domain.schema_grounding.authority import live_neo4j_schema_snapshot_digest
from app.integrations.neo4j_schema_deployment import (
    Neo4jLiveSchemaDeploymentReader,
)


class _Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def data(self) -> list[dict[str, object]]:
        return self._rows


class _Session:
    def __init__(self, rows_by_query_fragment: dict[str, list[dict[str, object]]]) -> None:
        self._rows_by_query_fragment = rows_by_query_fragment
        self.queries: list[str] = []

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run(
        self,
        query: str,
        _parameters: dict[str, object] | None = None,
    ) -> _Cursor:
        self.queries.append(query)
        for fragment, rows in self._rows_by_query_fragment.items():
            if fragment in query:
                return _Cursor(rows)
        raise AssertionError(f"unexpected query: {query}")


class _ServerInfo:
    agent = "Neo4j/5.27"


class _Driver:
    def __init__(self, session: _Session) -> None:
        self._session = session
        self.session_calls: list[tuple[str, Any]] = []

    def session(self, *, database: str, default_access_mode: Any) -> _Session:
        self.session_calls.append((database, default_access_mode))
        return self._session

    async def get_server_info(self) -> _ServerInfo:
        return _ServerInfo()


@pytest.mark.asyncio
async def test_reader_separates_token_catalog_from_active_usage_and_hashes_descriptors() -> None:
    session = _Session(
        {
            "CALL db.labels()": [
                {"values": ["Organization", "OrganizationState"]}
            ],
            "CALL db.relationshipTypes()": [
                {"values": ["USES_PLATFORM", "LEGACY_REL"]}
            ],
            "MATCH (n) UNWIND labels(n)": [{"values": ["Organization"]}],
            "MATCH ()-[r]->()": [{"values": ["USES_PLATFORM"]}],
            "SHOW INDEXES": [
                {
                    "name": "OrganizationName",
                    "state": "ONLINE",
                    "type": "FULLTEXT",
                    "entityType": "NODE",
                    "labelsOrTypes": ["Organization"],
                    "properties": ["name"],
                    "owningConstraint": None,
                }
            ],
            "SHOW CONSTRAINTS": [
                {
                    "name": "OrganizationId",
                    "type": "UNIQUENESS",
                    "entityType": "NODE",
                    "labelsOrTypes": ["Organization"],
                    "properties": ["id"],
                    "ownedIndex": "OrganizationId",
                }
            ],
        }
    )
    driver = _Driver(session)

    snapshot = await Neo4jLiveSchemaDeploymentReader(driver).read_schema_snapshot(
        database="neo4j"
    )

    assert driver.session_calls == [("neo4j", READ_ACCESS)]
    assert snapshot.token_catalog_node_labels == frozenset(
        {"Organization", "OrganizationState"}
    )
    assert snapshot.active_node_labels == frozenset({"Organization"})
    assert snapshot.token_catalog_relationship_types == frozenset(
        {"USES_PLATFORM", "LEGACY_REL"}
    )
    assert snapshot.active_relationship_types == frozenset({"USES_PLATFORM"})
    assert snapshot.indexes[0].properties == ("name",)
    assert snapshot.constraints[0].owned_index == "OrganizationId"
    assert snapshot.snapshot_digest == live_neo4j_schema_snapshot_digest(
        database=snapshot.database,
        server_agent=snapshot.server_agent,
        token_catalog_node_labels=snapshot.token_catalog_node_labels,
        token_catalog_relationship_types=snapshot.token_catalog_relationship_types,
        active_node_labels=snapshot.active_node_labels,
        active_relationship_types=snapshot.active_relationship_types,
        indexes=snapshot.indexes,
        constraints=snapshot.constraints,
    )
