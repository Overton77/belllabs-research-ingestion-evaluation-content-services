from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from neo4j import READ_ACCESS, WRITE_ACCESS

from app.application.schema_authority_issuance import (
    SchemaAuthorityIssuanceService,
    SchemaDeploymentEvidenceProvisioningService,
    deployment_audit_record_id,
)
from app.application.schema_grounding_repository import (
    InMemorySchemaGroundingRecordRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.schema_catalog.parser import parse_physical_schema
from app.domain.schema_context.canonicalization import sha256_digest as content_sha256_digest
from app.domain.schema_grounding.authority import (
    live_neo4j_schema_snapshot_digest,
    live_schema_compatibility_diff,
    live_schema_evidence_digest,
)
from app.domain.schema_grounding.contracts import (
    LiveNeo4jSchemaSnapshot,
    LiveSchemaDeploymentEvidence,
    Neo4jIndexDescriptor,
    SchemaAuthorityIssuanceRequest,
    SchemaAuthorityIssuerIdentities,
    SchemaDeploymentEvidenceProvisioningRequest,
)
from app.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    GraphCapabilityDenied,
    SchemaDeploymentMismatch,
)
from app.integrations.neo4j_schema_deployment import (
    Neo4jLiveSchemaDeploymentReader,
    deployment_evidence_query,
)

NOW = datetime(2026, 7, 26, 18, 0, tzinfo=UTC)
SCHEMA_DIGEST = "sha256:" + "1" * 64
CATALOG_DIGEST = "sha256:" + "2" * 64
RESOURCE_DIGEST = "sha256:" + "3" * 64
SNAPSHOT_DIGEST = "sha256:" + "4" * 64
CANONICAL_SDL = """
type Organization @node @fulltext(
  indexes: [{indexName: "OrganizationName", fields: ["name"]}]
) {
  name: String
  usesPlatforms: [TechnologyPlatform!]!
    @relationship(type: "USES_PLATFORM", direction: OUT)
}

type TechnologyPlatform @node {
  name: String
}
""".strip()
ALIASED_INDEX_SDL = """
type Document @node @fulltext(
  indexes: [{
    indexName: "DocumentSearch",
    fields: ["name", "sourceUrl", "searchText"]
  }]
) @vector(
  indexes: [{
    indexName: "DocumentEmbedding",
    embeddingProperty: "searchEmbedding"
  }]
) {
  name: String! @alias(property: "title")
  sourceUrl: String @alias(property: "url")
  searchText: String
  searchEmbedding: [Float!] @alias(property: "storedEmbedding")
}
""".strip()

IDENTITIES = SchemaAuthorityIssuerIdentities(
    deployment_issuer_authority_ref="issue-12:graph-schema-deployment-service",
    workspace_issuer_authority_ref=("issue-13:schema-workspace-materialization-service"),
    graph_capability_authority_ref="graph-authority:read-capability-service",
    workspace_materializer_version="issue-13-materializer-v1",
)


def _request(**updates: object) -> SchemaAuthorityIssuanceRequest:
    values: dict[str, object] = {
        "request_scope": "tenant-1",
        "run_id": "run-c",
        "environment": "production",
        "database": "neo4j",
        "deployment_id": "deployment-20260726",
        "schema_definition_ref": "schema-definition:neo4j-biotech@2026-07-26",
        "schema_definition_digest": SCHEMA_DIGEST,
        "catalog_build_id": "catalog-build-1",
        "catalog_digest": CATALOG_DIGEST,
        "resource_manifest_digest": RESOURCE_DIGEST,
        "workspace_id": "workspace-run-c",
        "slot_name": "graph_query_runtime",
        "profile": "graph-query-runtime",
        "purpose": "read_query_reconciliation",
        "workspace_read_only": True,
        "requested_graph_access": "read",
        "query_kinds": frozenset({"exact_identity", "entity_details"}),
        "allowed_node_labels": frozenset({"Organization", "TechnologyPlatform"}),
        "allowed_relationship_types": frozenset({"USES_PLATFORM"}),
        "maximum_limit": 25,
        "maximum_traversal_depth": 1,
        "secret_ref": "secret:neo4j-readonly:v1",
        "budget_reservation_id": "reservation:run-c:graph-reads",
        "sensitive_data_policy_ref": "policy:sensitive-data:v1",
        "requested_at": NOW,
    }
    values.update(updates)
    return SchemaAuthorityIssuanceRequest.model_validate(values)


def _evidence(**updates: object) -> LiveSchemaDeploymentEvidence:
    values: dict[str, object] = {
        "evidence_id": "deployment-evidence-20260726",
        "environment": "production",
        "database": "neo4j",
        "schema_definition_ref": "schema-definition:neo4j-biotech@2026-07-26",
        "deployed_sdl_digest": SCHEMA_DIGEST,
        "live_schema_snapshot_digest": SNAPSHOT_DIGEST,
        "deployment_id": "deployment-20260726",
        "issuer_authority_ref": "issue-12:graph-schema-deployment-service",
        "deployment_succeeded": True,
        "active": True,
        "revoked": False,
        "issued_at": NOW,
    }
    values.update(updates)
    values["evidence_digest"] = live_schema_evidence_digest(
        evidence_id=str(values["evidence_id"]),
        environment=str(values["environment"]),
        database=str(values["database"]),
        schema_definition_ref=str(values["schema_definition_ref"]),
        deployed_sdl_digest=str(values["deployed_sdl_digest"]),
        live_schema_snapshot_digest=str(values["live_schema_snapshot_digest"]),
        deployment_id=str(values["deployment_id"]),
        issuer_authority_ref=str(values["issuer_authority_ref"]),
        deployment_succeeded=bool(values["deployment_succeeded"]),
        active=bool(values["active"]),
        revoked=bool(values["revoked"]),
        issued_at=values["issued_at"],
    )
    return LiveSchemaDeploymentEvidence.model_validate(values)


def _snapshot(**updates: object) -> LiveNeo4jSchemaSnapshot:
    values: dict[str, object] = {
        "database": "neo4j",
        "server_agent": "Neo4j/2026.07",
        "token_catalog_node_labels": frozenset(
            {"Organization", "TechnologyPlatform"}
        ),
        "token_catalog_relationship_types": frozenset({"USES_PLATFORM"}),
        "active_node_labels": frozenset({"Organization", "TechnologyPlatform"}),
        "active_relationship_types": frozenset({"USES_PLATFORM"}),
        "indexes": (
            Neo4jIndexDescriptor(
                name="OrganizationName",
                index_type="FULLTEXT",
                entity_type="NODE",
                labels_or_types=("Organization",),
                properties=("name",),
                state="ONLINE",
            ),
        ),
        "constraints": (),
        "observed_at": NOW,
    }
    values.update(updates)
    values["snapshot_digest"] = live_neo4j_schema_snapshot_digest(
        database=str(values["database"]),
        server_agent=str(values["server_agent"]),
        token_catalog_node_labels=frozenset(
            values["token_catalog_node_labels"]  # type: ignore[arg-type]
        ),
        token_catalog_relationship_types=frozenset(
            values["token_catalog_relationship_types"]  # type: ignore[arg-type]
        ),
        active_node_labels=frozenset(
            values["active_node_labels"]  # type: ignore[arg-type]
        ),
        active_relationship_types=frozenset(
            values["active_relationship_types"]  # type: ignore[arg-type]
        ),
        indexes=tuple(values["indexes"]),  # type: ignore[arg-type]
        constraints=tuple(values["constraints"]),  # type: ignore[arg-type]
    )
    return LiveNeo4jSchemaSnapshot.model_validate(values)


def _provision_request(
    **updates: object,
) -> SchemaDeploymentEvidenceProvisioningRequest:
    values: dict[str, object] = {
        "environment": "production",
        "database": "neo4j",
        "deployment_id": "deployment-20260726",
        "schema_definition_ref": "schema-definition:neo4j-biotech@2026-07-26",
        "schema_definition_digest": content_sha256_digest(CANONICAL_SDL.encode()),
        "canonical_sdl": CANONICAL_SDL,
        "issued_at": NOW,
    }
    values.update(updates)
    return SchemaDeploymentEvidenceProvisioningRequest.model_validate(values)


class _Reader:
    def __init__(self, evidence: LiveSchemaDeploymentEvidence | None) -> None:
        self.evidence = evidence
        self.calls = 0

    async def read(
        self,
        *,
        environment: str,
        database: str,
        deployment_id: str,
    ) -> LiveSchemaDeploymentEvidence | None:
        self.calls += 1
        assert (environment, database, deployment_id) == (
            "production",
            "neo4j",
            "deployment-20260726",
        )
        return self.evidence


@pytest.mark.asyncio
async def test_issue_persists_distinct_content_addressed_authorities() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    reader = _Reader(_evidence())
    service = SchemaAuthorityIssuanceService(
        deployment_reader=reader,
        records=records,
        identities=IDENTITIES,
    )

    first = await service.issue(_request())
    second = await service.issue(_request())

    assert first == second
    evidence_record = await records.get(
        "tenant-1",
        "deployment_evidence",
        deployment_audit_record_id("evidence", _evidence().evidence_id),
    )
    assert (
        evidence_record.payload["event_kind"]
        == "current_schema_verification_attestation"
    )
    assert first.deployment_manifest.issuer_authority_ref == (
        IDENTITIES.deployment_issuer_authority_ref
    )
    assert first.workspace_binding.issuer_authority_ref == (
        IDENTITIES.workspace_issuer_authority_ref
    )
    assert first.graph_capability.decided_by_authority_ref == (
        IDENTITIES.graph_capability_authority_ref
    )
    assert first.workspace_binding.read_only is True
    assert first.graph_capability.admitted is True
    manifest_payload = first.deployment_manifest.model_dump(
        mode="python",
        exclude={"manifest_id", "manifest_digest"},
    )
    binding_payload = first.workspace_binding.model_dump(
        mode="python",
        exclude={"binding_id", "binding_digest"},
    )
    grant_payload = first.graph_capability.model_dump(
        mode="python",
        exclude={"grant_id", "grant_digest"},
    )
    assert first.deployment_manifest.manifest_digest == sha256_digest(manifest_payload)
    assert first.workspace_binding.binding_digest == sha256_digest(binding_payload)
    assert first.graph_capability.grant_digest == sha256_digest(grant_payload)
    persisted = await records.list_for_run("tenant-1", "run-c")
    assert {record.record_type for record in persisted} == {
        "workspace_binding",
        "graph_capability",
    }
    assert len(persisted) == 2


@pytest.mark.asyncio
async def test_deployment_audit_is_reused_across_distinct_consumer_runs() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    evidence = _evidence()
    service = SchemaAuthorityIssuanceService(
        deployment_reader=_Reader(evidence),
        records=records,
        identities=IDENTITIES,
    )

    first = await service.issue(_request(run_id="run-c-1"))
    second = await service.issue(_request(run_id="run-c-2"))

    assert first.deployment_manifest == second.deployment_manifest
    assert first.workspace_binding != second.workspace_binding
    assert first.graph_capability != second.graph_capability
    evidence_record = await records.get(
        "tenant-1",
        "deployment_evidence",
        deployment_audit_record_id("evidence", evidence.evidence_id),
    )
    manifest_record = await records.get(
        "tenant-1",
        "deployment_manifest",
        deployment_audit_record_id(
            "manifest",
            first.deployment_manifest.manifest_id,
        ),
    )
    assert evidence_record.run_id is None
    assert manifest_record.run_id is None
    assert evidence_record.payload == evidence.model_dump(mode="json")
    assert {
        record.record_type
        for record in await records.list_for_run("tenant-1", "run-c-1")
    } == {"workspace_binding", "graph_capability"}
    assert {
        record.record_type
        for record in await records.list_for_run("tenant-1", "run-c-2")
    } == {"workspace_binding", "graph_capability"}


@pytest.mark.asyncio
async def test_schema_deployment_digest_mismatch_issues_nothing() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    service = SchemaAuthorityIssuanceService(
        deployment_reader=_Reader(_evidence(deployed_sdl_digest="sha256:" + "9" * 64)),
        records=records,
        identities=IDENTITIES,
    )

    with pytest.raises(SchemaDeploymentMismatch, match="do not match"):
        await service.issue(_request())

    assert await records.list_for_run("tenant-1", "run-c") == ()


@pytest.mark.asyncio
async def test_revoked_deployment_evidence_issues_nothing() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    service = SchemaAuthorityIssuanceService(
        deployment_reader=_Reader(_evidence(active=False, revoked=True)),
        records=records,
        identities=IDENTITIES,
    )

    with pytest.raises(SchemaDeploymentMismatch, match="revoked"):
        await service.issue(_request())

    assert await records.list_for_run("tenant-1", "run-c") == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    (
        {"workspace_read_only": False},
        {"requested_graph_access": "write"},
    ),
)
async def test_write_scoped_workspace_or_grant_is_denied_before_live_read(
    updates: dict[str, object],
) -> None:
    records = InMemorySchemaGroundingRecordRepository()
    reader = _Reader(_evidence())
    service = SchemaAuthorityIssuanceService(
        deployment_reader=reader,
        records=records,
        identities=IDENTITIES,
    )

    with pytest.raises(GraphCapabilityDenied, match="read-only"):
        await service.issue(_request(**updates))

    assert reader.calls == 0
    assert await records.list_for_run("tenant-1", "run-c") == ()


class _ProvisioningGraph:
    def __init__(self, snapshot: LiveNeo4jSchemaSnapshot) -> None:
        self.snapshot = snapshot
        self.stored: LiveSchemaDeploymentEvidence | None = None

    async def read_schema_snapshot(
        self,
        *,
        database: str,
    ) -> LiveNeo4jSchemaSnapshot:
        assert database == self.snapshot.database
        return self.snapshot

    async def write_deployment_evidence(
        self,
        evidence: LiveSchemaDeploymentEvidence,
    ) -> LiveSchemaDeploymentEvidence:
        if self.stored is None:
            self.stored = evidence
        elif self.stored != evidence:
            raise CatalogPublicationConflict(
                "deployment evidence identity was reused with conflicting immutable content"
            )
        return self.stored


@pytest.mark.asyncio
async def test_operator_provisioning_is_idempotent_and_not_coordinator_authority() -> None:
    graph = _ProvisioningGraph(_snapshot())
    service = SchemaDeploymentEvidenceProvisioningService(
        graph=graph,
        identities=IDENTITIES,
    )

    first = await service.provision(_provision_request())
    second = await service.provision(_provision_request())

    assert first == second
    assert first.issuer_authority_ref == IDENTITIES.deployment_issuer_authority_ref
    assert first.live_schema_snapshot_digest == graph.snapshot.snapshot_digest
    assert first.deployed_sdl_digest == content_sha256_digest(CANONICAL_SDL.encode())


@pytest.mark.asyncio
async def test_operator_provisioning_rejects_conflicting_deployment_identity() -> None:
    graph = _ProvisioningGraph(_snapshot())
    service = SchemaDeploymentEvidenceProvisioningService(
        graph=graph,
        identities=IDENTITIES,
    )
    await service.provision(_provision_request())

    with pytest.raises(CatalogPublicationConflict, match="conflicting immutable"):
        await service.provision(
            _provision_request(issued_at=datetime(2026, 7, 26, 18, 1, tzinfo=UTC))
        )


@pytest.mark.asyncio
async def test_operator_provisioning_rejects_incompatible_live_snapshot() -> None:
    graph = _ProvisioningGraph(
        _snapshot(
            token_catalog_node_labels=frozenset(
                {"Organization", "TechnologyPlatform", "LegacyNode"}
            ),
            token_catalog_relationship_types=frozenset(
                {"USES_PLATFORM", "LEGACY_REL"}
            ),
            active_node_labels=frozenset(
                {"Organization", "TechnologyPlatform", "LegacyNode"}
            ),
            active_relationship_types=frozenset({"USES_PLATFORM", "LEGACY_REL"}),
            indexes=(),
        )
    )
    service = SchemaDeploymentEvidenceProvisioningService(
        graph=graph,
        identities=IDENTITIES,
    )

    with pytest.raises(
        SchemaDeploymentMismatch,
        match=(
            r"unexpected_active_node_labels=\['LegacyNode'\].*"
            r"unexpected_active_relationship_types=\['LEGACY_REL'\].*"
            r"missing_index_names=\['OrganizationName'\]"
        ),
    ):
        await service.provision(_provision_request())

    assert graph.stored is None


def test_live_schema_compatibility_diff_reports_exact_expected_and_observed_sets() -> None:
    snapshot = _snapshot(
        token_catalog_node_labels=frozenset(
            {
                "Organization",
                "LegacyNode",
                "BellLabsSchemaDeploymentEvidence",
            }
        ),
        token_catalog_relationship_types=frozenset({"LEGACY_REL"}),
        active_node_labels=frozenset({"Organization", "LegacyNode"}),
        active_relationship_types=frozenset({"LEGACY_REL"}),
        indexes=(
            Neo4jIndexDescriptor(
                name="UnexpectedIndex",
                index_type="RANGE",
                entity_type="NODE",
                labels_or_types=("Organization",),
                properties=("name",),
                state="ONLINE",
            ),
        ),
    )

    diff = live_schema_compatibility_diff(_provision_request(), snapshot)

    assert diff.expected_node_labels == frozenset(
        {"Organization", "TechnologyPlatform"}
    )
    assert diff.observed_node_labels == snapshot.node_labels
    assert diff.expected_but_unobserved_node_labels == frozenset(
        {"TechnologyPlatform"}
    )
    assert diff.unexpected_node_labels == frozenset({"LegacyNode"})
    assert diff.operational_node_labels == frozenset(
        {"BellLabsSchemaDeploymentEvidence"}
    )
    assert diff.expected_relationship_types == frozenset({"USES_PLATFORM"})
    assert diff.expected_but_unobserved_relationship_types == frozenset(
        {"USES_PLATFORM"}
    )
    assert diff.unexpected_relationship_types == frozenset({"LEGACY_REL"})
    assert diff.expected_index_names == frozenset({"OrganizationName"})
    assert diff.missing_index_names == frozenset({"OrganizationName"})
    assert diff.unexpected_index_names == frozenset({"UnexpectedIndex"})
    assert diff.snapshot_digest_matches is True
    assert diff.database_matches is True
    assert diff.compatible is False


def test_zero_count_persistent_tokens_are_informational_without_schema_artifacts() -> None:
    snapshot = _snapshot(
        token_catalog_node_labels=frozenset(
            {"Organization", "TechnologyPlatform", "LegacyNode"}
        ),
        token_catalog_relationship_types=frozenset(
            {"USES_PLATFORM", "LEGACY_REL"}
        ),
    )

    diff = live_schema_compatibility_diff(_provision_request(), snapshot)

    assert diff.unexpected_node_labels == frozenset({"LegacyNode"})
    assert diff.unexpected_relationship_types == frozenset({"LEGACY_REL"})
    assert diff.unexpected_active_node_labels == frozenset()
    assert diff.unexpected_active_relationship_types == frozenset()
    assert diff.compatible is True


def test_exact_index_descriptors_resolve_aliased_physical_properties() -> None:
    request = _provision_request(
        canonical_sdl=ALIASED_INDEX_SDL,
        schema_definition_digest=content_sha256_digest(ALIASED_INDEX_SDL.encode()),
    )
    indexes = (
        Neo4jIndexDescriptor(
            name="DocumentEmbedding",
            index_type="VECTOR",
            entity_type="NODE",
            labels_or_types=("Document",),
            properties=("storedEmbedding",),
            state="ONLINE",
        ),
        Neo4jIndexDescriptor(
            name="DocumentSearch",
            index_type="FULLTEXT",
            entity_type="NODE",
            labels_or_types=("Document",),
            properties=("title", "url", "searchText"),
            state="ONLINE",
        ),
    )
    snapshot = _snapshot(
        token_catalog_node_labels=frozenset({"Document"}),
        token_catalog_relationship_types=frozenset(),
        active_node_labels=frozenset({"Document"}),
        active_relationship_types=frozenset(),
        indexes=indexes,
    )

    diff = live_schema_compatibility_diff(request, snapshot)

    assert diff.expected_canonical_indexes == indexes
    assert diff.compatible is True


def test_canonical_schema_exact_comparison_is_compatible_without_stale_artifacts() -> None:
    schema_path = (
        Path(__file__).resolve().parents[2] / "biotech-kg" / "typedefs.graphql"
    )
    canonical_sdl = schema_path.read_text(encoding="utf-8")
    schema_ref = "schema-definition:canonical-alias-regression"
    physical = parse_physical_schema(canonical_sdl.encode(), schema_ref)
    request = _provision_request(
        schema_definition_ref=schema_ref,
        canonical_sdl=canonical_sdl,
        schema_definition_digest=content_sha256_digest(canonical_sdl.encode()),
    )
    clean_catalog_snapshot = _snapshot(
        token_catalog_node_labels=frozenset(physical.nodes),
        token_catalog_relationship_types=frozenset(physical.relationships),
        active_node_labels=frozenset(),
        active_relationship_types=frozenset(),
        indexes=(),
    )
    expected = live_schema_compatibility_diff(request, clean_catalog_snapshot)
    document_search = next(
        descriptor
        for descriptor in expected.expected_canonical_indexes
        if descriptor.name == "DocumentSearch"
    )
    clean_exact_snapshot = _snapshot(
        token_catalog_node_labels=frozenset(physical.nodes),
        token_catalog_relationship_types=frozenset(physical.relationships),
        active_node_labels=frozenset(),
        active_relationship_types=frozenset(),
        indexes=expected.expected_canonical_indexes,
    )

    assert document_search.properties == ("title", "url", "searchText")
    assert live_schema_compatibility_diff(request, clean_exact_snapshot).compatible is True


class _Cursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def data(self) -> list[dict[str, object]]:
        return self._rows


class _Session:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.query: str | None = None
        self.parameters: dict[str, object] | None = None

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run(
        self,
        query: str,
        parameters: dict[str, object],
    ) -> _Cursor:
        self.query = query
        self.parameters = parameters
        return _Cursor(self._rows)


class _Driver:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.session_instance = _Session(rows)
        self.database: str | None = None
        self.default_access_mode: str | None = None

    def session(
        self,
        *,
        database: str,
        default_access_mode: str,
    ) -> _Session:
        self.database = database
        self.default_access_mode = default_access_mode
        return self.session_instance


class _MergeSession:
    def __init__(self) -> None:
        self.stored: dict[str, object] | None = None

    async def __aenter__(self) -> _MergeSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run(
        self,
        _query: str,
        parameters: dict[str, object],
    ) -> _Cursor:
        candidate = cast(dict[str, object], parameters["evidence"])
        if self.stored is None:
            self.stored = candidate
        return _Cursor([self.stored])


class _MergeDriver:
    def __init__(self) -> None:
        self.session_instance = _MergeSession()
        self.default_access_mode: str | None = None

    def session(
        self,
        *,
        database: str,
        default_access_mode: str,
    ) -> _MergeSession:
        assert database == "neo4j"
        self.default_access_mode = default_access_mode
        return self.session_instance


@pytest.mark.asyncio
async def test_neo4j_adapter_reads_one_digest_verified_attestation() -> None:
    evidence = _evidence()
    driver = _Driver([evidence.model_dump(mode="python")])
    reader = Neo4jLiveSchemaDeploymentReader(driver)  # type: ignore[arg-type]

    actual = await reader.read(
        environment="production",
        database="neo4j",
        deployment_id="deployment-20260726",
    )

    assert actual == evidence
    assert driver.database == "neo4j"
    assert driver.default_access_mode == READ_ACCESS
    assert driver.session_instance.query == deployment_evidence_query()
    assert "CREATE" not in deployment_evidence_query().upper()
    assert "SET " not in deployment_evidence_query().upper()


@pytest.mark.asyncio
async def test_neo4j_evidence_merge_is_idempotent_and_detects_conflict() -> None:
    driver = _MergeDriver()
    graph = Neo4jLiveSchemaDeploymentReader(driver)  # type: ignore[arg-type]
    evidence = _evidence()

    assert await graph.write_deployment_evidence(evidence) == evidence
    assert await graph.write_deployment_evidence(evidence) == evidence
    assert driver.default_access_mode == WRITE_ACCESS

    conflicting = _evidence(issued_at=datetime(2026, 7, 26, 18, 1, tzinfo=UTC))
    with pytest.raises(CatalogPublicationConflict, match="conflicting immutable"):
        await graph.write_deployment_evidence(conflicting)
