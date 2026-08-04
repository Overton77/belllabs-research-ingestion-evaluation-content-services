from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from neo4j import READ_ACCESS, WRITE_ACCESS, AsyncDriver

from app.config import Settings
from app.domain.schema_grounding.authority import (
    live_neo4j_schema_snapshot_digest,
    verify_live_schema_evidence_digest,
)
from app.domain.schema_grounding.contracts import (
    LiveNeo4jSchemaSnapshot,
    LiveSchemaDeploymentEvidence,
    Neo4jConstraintDescriptor,
    Neo4jIndexDescriptor,
    SchemaAuthorityIssuerIdentities,
)
from app.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    SchemaDeploymentMismatch,
)

_READ_DEPLOYMENT_EVIDENCE = """
MATCH (e:BellLabsSchemaDeploymentEvidence {
    environment: $environment,
    database: $database,
    deployment_id: $deployment_id
})
RETURN
    e.evidence_id AS evidence_id,
    e.evidence_digest AS evidence_digest,
    e.event_kind AS event_kind,
    e.environment AS environment,
    e.database AS database,
    e.schema_definition_ref AS schema_definition_ref,
    e.deployed_sdl_digest AS deployed_sdl_digest,
    e.live_schema_snapshot_digest AS live_schema_snapshot_digest,
    e.deployment_id AS deployment_id,
    e.issuer_authority_ref AS issuer_authority_ref,
    e.deployment_succeeded AS deployment_succeeded,
    e.active AS active,
    e.revoked AS revoked,
    e.issued_at AS issued_at
ORDER BY e.issued_at DESC
LIMIT 2
""".strip()

_READ_TOKEN_NODE_LABELS = (
    "CALL db.labels() YIELD label RETURN collect(DISTINCT label) AS values"
)
_READ_TOKEN_RELATIONSHIP_TYPES = (
    "CALL db.relationshipTypes() YIELD relationshipType "
    "RETURN collect(DISTINCT relationshipType) AS values"
)
_READ_ACTIVE_NODE_LABELS = (
    "MATCH (n) UNWIND labels(n) AS label "
    "RETURN collect(DISTINCT label) AS values"
)
_READ_ACTIVE_RELATIONSHIP_TYPES = (
    "MATCH ()-[r]->() RETURN collect(DISTINCT type(r)) AS values"
)
_READ_INDEXES = """
SHOW INDEXES
YIELD name, state, type, entityType, labelsOrTypes, properties,
      owningConstraint
RETURN name, state, type, entityType, labelsOrTypes, properties,
       owningConstraint
ORDER BY name
""".strip()
_READ_CONSTRAINTS = """
SHOW CONSTRAINTS
YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex
RETURN name, type, entityType, labelsOrTypes, properties, ownedIndex
ORDER BY name
""".strip()
_WRITE_DEPLOYMENT_EVIDENCE = """
MERGE (e:BellLabsSchemaDeploymentEvidence {
    environment: $environment,
    database: $database,
    deployment_id: $deployment_id
})
ON CREATE SET e = $evidence
RETURN
    e.evidence_id AS evidence_id,
    e.evidence_digest AS evidence_digest,
    e.event_kind AS event_kind,
    e.environment AS environment,
    e.database AS database,
    e.schema_definition_ref AS schema_definition_ref,
    e.deployed_sdl_digest AS deployed_sdl_digest,
    e.live_schema_snapshot_digest AS live_schema_snapshot_digest,
    e.deployment_id AS deployment_id,
    e.issuer_authority_ref AS issuer_authority_ref,
    e.deployment_succeeded AS deployment_succeeded,
    e.active AS active,
    e.revoked AS revoked,
    e.issued_at AS issued_at
""".strip()


class Neo4jLiveSchemaDeploymentReader:
    """Read graph-deployment-process evidence without granting query authority."""

    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    async def read(
        self,
        *,
        environment: str,
        database: str,
        deployment_id: str,
    ) -> LiveSchemaDeploymentEvidence | None:
        async with self._driver.session(
            database=database,
            default_access_mode=READ_ACCESS,
        ) as session:
            cursor = await session.run(
                _READ_DEPLOYMENT_EVIDENCE,
                {
                    "environment": environment,
                    "database": database,
                    "deployment_id": deployment_id,
                },
            )
            rows = await cursor.data()
        if not rows:
            return None
        if len(rows) != 1:
            raise SchemaDeploymentMismatch(
                "multiple live deployment evidence records exist for one deployment identity"
            )
        payload = dict(rows[0])
        issued_at = payload.get("issued_at")
        to_native = getattr(issued_at, "to_native", None)
        if callable(to_native):
            payload["issued_at"] = to_native()
        evidence = LiveSchemaDeploymentEvidence.model_validate(payload)
        verify_live_schema_evidence_digest(evidence)
        return evidence

    async def read_schema_snapshot(
        self,
        *,
        database: str,
    ) -> LiveNeo4jSchemaSnapshot:
        observed_at = datetime.now(UTC)
        async with self._driver.session(
            database=database,
            default_access_mode=READ_ACCESS,
        ) as session:
            token_catalog_node_labels = await _read_collection(
                session,
                _READ_TOKEN_NODE_LABELS,
            )
            token_catalog_relationship_types = await _read_collection(
                session,
                _READ_TOKEN_RELATIONSHIP_TYPES,
            )
            active_node_labels = await _read_collection(
                session,
                _READ_ACTIVE_NODE_LABELS,
            )
            active_relationship_types = await _read_collection(
                session,
                _READ_ACTIVE_RELATIONSHIP_TYPES,
            )
            indexes = await _read_index_descriptors(session)
            constraints = await _read_constraint_descriptors(session)
        server_info = await self._driver.get_server_info()
        server_agent = str(getattr(server_info, "agent", "unknown"))
        snapshot_digest = live_neo4j_schema_snapshot_digest(
            database=database,
            server_agent=server_agent,
            token_catalog_node_labels=token_catalog_node_labels,
            token_catalog_relationship_types=token_catalog_relationship_types,
            active_node_labels=active_node_labels,
            active_relationship_types=active_relationship_types,
            indexes=indexes,
            constraints=constraints,
        )
        return LiveNeo4jSchemaSnapshot(
            database=database,
            server_agent=server_agent,
            token_catalog_node_labels=token_catalog_node_labels,
            token_catalog_relationship_types=token_catalog_relationship_types,
            active_node_labels=active_node_labels,
            active_relationship_types=active_relationship_types,
            indexes=indexes,
            constraints=constraints,
            observed_at=observed_at,
            snapshot_digest=snapshot_digest,
        )

    async def write_deployment_evidence(
        self,
        evidence: LiveSchemaDeploymentEvidence,
    ) -> LiveSchemaDeploymentEvidence:
        verify_live_schema_evidence_digest(evidence)
        payload = evidence.model_dump(mode="python")
        async with self._driver.session(
            database=evidence.database,
            default_access_mode=WRITE_ACCESS,
        ) as session:
            cursor = await session.run(
                _WRITE_DEPLOYMENT_EVIDENCE,
                {
                    "environment": evidence.environment,
                    "database": evidence.database,
                    "deployment_id": evidence.deployment_id,
                    "evidence": payload,
                },
            )
            rows = await cursor.data()
        if len(rows) != 1:
            raise CatalogPublicationConflict(
                "deployment evidence identity resolved to an ambiguous graph record"
            )
        stored_payload = dict(rows[0])
        issued_at = stored_payload.get("issued_at")
        to_native = getattr(issued_at, "to_native", None)
        if callable(to_native):
            stored_payload["issued_at"] = to_native()
        stored = LiveSchemaDeploymentEvidence.model_validate(stored_payload)
        verify_live_schema_evidence_digest(stored)
        if stored != evidence:
            raise CatalogPublicationConflict(
                "deployment evidence identity was reused with conflicting immutable content"
            )
        return stored


def schema_authority_issuer_identities(
    settings: Settings,
) -> SchemaAuthorityIssuerIdentities:
    return SchemaAuthorityIssuerIdentities(
        deployment_issuer_authority_ref=(settings.schema_deployment_issuer_authority_ref),
        workspace_issuer_authority_ref=settings.schema_workspace_issuer_authority_ref,
        graph_capability_authority_ref=settings.graph_capability_authority_ref,
        workspace_materializer_version=settings.schema_workspace_materializer_version,
    )


def deployment_evidence_query() -> str:
    """Expose the fixed read query for deployment-pipeline integration tests."""

    return _READ_DEPLOYMENT_EVIDENCE


async def _read_collection(session: Any, query: str) -> frozenset[str]:
    cursor = await session.run(query)
    rows = await cursor.data()
    if len(rows) != 1:
        raise SchemaDeploymentMismatch("live Neo4j schema snapshot query was ambiguous")
    values = rows[0].get("values")
    if not isinstance(values, list):
        raise SchemaDeploymentMismatch("live Neo4j schema snapshot returned invalid values")
    return frozenset(str(value) for value in values)


async def _read_index_descriptors(
    session: Any,
) -> tuple[Neo4jIndexDescriptor, ...]:
    cursor = await session.run(_READ_INDEXES)
    rows = await cursor.data()
    descriptors = tuple(
        sorted(
            (
                Neo4jIndexDescriptor(
                    name=str(row["name"]),
                    index_type=str(row["type"]),
                    entity_type=str(row["entityType"]),
                    labels_or_types=_sorted_strings(row.get("labelsOrTypes")),
                    properties=_ordered_strings(row.get("properties")),
                    state=str(row["state"]),
                    owning_constraint=_optional_string(
                        row.get("owningConstraint")
                    ),
                )
                for row in rows
            ),
            key=lambda item: item.name,
        )
    )
    if len({item.name for item in descriptors}) != len(descriptors):
        raise SchemaDeploymentMismatch("live Neo4j index names are ambiguous")
    return descriptors


async def _read_constraint_descriptors(
    session: Any,
) -> tuple[Neo4jConstraintDescriptor, ...]:
    cursor = await session.run(_READ_CONSTRAINTS)
    rows = await cursor.data()
    descriptors = tuple(
        sorted(
            (
                Neo4jConstraintDescriptor(
                    name=str(row["name"]),
                    constraint_type=str(row["type"]),
                    entity_type=str(row["entityType"]),
                    labels_or_types=_sorted_strings(row.get("labelsOrTypes")),
                    properties=_ordered_strings(row.get("properties")),
                    owned_index=_optional_string(row.get("ownedIndex")),
                )
                for row in rows
            ),
            key=lambda item: item.name,
        )
    )
    if len({item.name for item in descriptors}) != len(descriptors):
        raise SchemaDeploymentMismatch("live Neo4j constraint names are ambiguous")
    return descriptors


def _sorted_strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SchemaDeploymentMismatch("Neo4j descriptor labels/types are invalid")
    return tuple(sorted({str(item) for item in value}))


def _ordered_strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SchemaDeploymentMismatch("Neo4j descriptor properties are invalid")
    return tuple(str(item) for item in value)


def _optional_string(value: Any) -> str | None:
    return None if value is None else str(value)
