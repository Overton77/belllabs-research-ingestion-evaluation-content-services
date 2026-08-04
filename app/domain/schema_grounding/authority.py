from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from app.domain.control_plane.canonical import sha256_digest
from app.domain.schema_catalog.models import PhysicalSchemaCatalog
from app.domain.schema_catalog.parser import parse_physical_schema
from app.domain.schema_context.canonicalization import (
    sha256_digest as content_sha256_digest,
)
from app.domain.schema_grounding.contracts import (
    GraphCapabilityGrant,
    LiveNeo4jSchemaSnapshot,
    LiveSchemaCompatibilityDiff,
    LiveSchemaDeploymentEvidence,
    Neo4jConstraintDescriptor,
    Neo4jIndexDescriptor,
    SchemaAuthorityBundle,
    SchemaAuthorityIssuanceRequest,
    SchemaAuthorityIssuerIdentities,
    SchemaDeploymentEvidenceProvisioningRequest,
    SchemaDeploymentManifestRef,
    SchemaWorkspaceBindingRef,
)
from app.domain.schema_grounding.errors import (
    GraphCapabilityDenied,
    SchemaDeploymentMismatch,
    SchemaSourceDigestMismatch,
)


def issue_schema_authority_bundle(
    request: SchemaAuthorityIssuanceRequest,
    evidence: LiveSchemaDeploymentEvidence,
    identities: SchemaAuthorityIssuerIdentities,
) -> SchemaAuthorityBundle:
    """Apply authority invariants and derive immutable content-addressed records."""

    verify_schema_authority_request_scope(request)
    _verify_deployment(request, evidence, identities)

    manifest_payload = {
        "environment": request.environment,
        "database": request.database,
        "schema_definition_ref": request.schema_definition_ref,
        "deployed_sdl_digest": request.schema_definition_digest,
        "deployment_id": request.deployment_id,
        "issuer_authority_ref": identities.deployment_issuer_authority_ref,
        "active": True,
        "revoked": False,
        "issued_at": evidence.issued_at,
    }
    manifest_digest = sha256_digest(manifest_payload)
    manifest = SchemaDeploymentManifestRef(
        manifest_id=_content_id("schema-deployment-manifest", manifest_digest),
        manifest_digest=manifest_digest,
        environment=request.environment,
        database=request.database,
        schema_definition_ref=request.schema_definition_ref,
        deployed_sdl_digest=request.schema_definition_digest,
        deployment_id=request.deployment_id,
        issuer_authority_ref=identities.deployment_issuer_authority_ref,
        active=True,
        revoked=False,
        issued_at=evidence.issued_at,
    )

    binding_payload = {
        "request_scope": request.request_scope,
        "run_id": request.run_id,
        "workspace_id": request.workspace_id,
        "slot_name": request.slot_name,
        "catalog_build_id": request.catalog_build_id,
        "catalog_digest": request.catalog_digest,
        "resource_manifest_digest": request.resource_manifest_digest,
        "profile": request.profile,
        "purpose": request.purpose,
        "read_only": True,
        "issuer_authority_ref": identities.workspace_issuer_authority_ref,
        "materializer_version": identities.workspace_materializer_version,
        "created_at": request.requested_at,
    }
    binding_digest = sha256_digest(binding_payload)
    binding = SchemaWorkspaceBindingRef(
        binding_id=_content_id("schema-workspace-binding", binding_digest),
        binding_digest=binding_digest,
        request_scope=request.request_scope,
        run_id=request.run_id,
        workspace_id=request.workspace_id,
        slot_name=request.slot_name,
        catalog_build_id=request.catalog_build_id,
        catalog_digest=request.catalog_digest,
        resource_manifest_digest=request.resource_manifest_digest,
        profile=request.profile,
        purpose=request.purpose,
        read_only=True,
        issuer_authority_ref=identities.workspace_issuer_authority_ref,
        materializer_version=identities.workspace_materializer_version,
        created_at=request.requested_at,
    )

    grant_payload = {
        "request_scope": request.request_scope,
        "run_id": request.run_id,
        "environment": request.environment,
        "database": request.database,
        "purpose": request.purpose,
        "admitted": True,
        "query_kinds": request.query_kinds,
        "allowed_node_labels": request.allowed_node_labels,
        "allowed_relationship_types": request.allowed_relationship_types,
        "maximum_limit": request.maximum_limit,
        "maximum_traversal_depth": request.maximum_traversal_depth,
        "secret_ref": request.secret_ref,
        "budget_reservation_id": request.budget_reservation_id,
        "sensitive_data_policy_ref": request.sensitive_data_policy_ref,
        "decided_by_authority_ref": identities.graph_capability_authority_ref,
        "decided_at": request.requested_at,
    }
    grant_digest = sha256_digest(grant_payload)
    grant = GraphCapabilityGrant(
        grant_id=_content_id("graph-capability-grant", grant_digest),
        grant_digest=grant_digest,
        request_scope=request.request_scope,
        run_id=request.run_id,
        environment=request.environment,
        database=request.database,
        purpose=request.purpose,
        admitted=True,
        query_kinds=request.query_kinds,
        allowed_node_labels=request.allowed_node_labels,
        allowed_relationship_types=request.allowed_relationship_types,
        maximum_limit=request.maximum_limit,
        maximum_traversal_depth=request.maximum_traversal_depth,
        secret_ref=request.secret_ref,
        budget_reservation_id=request.budget_reservation_id,
        sensitive_data_policy_ref=request.sensitive_data_policy_ref,
        decided_by_authority_ref=identities.graph_capability_authority_ref,
        decided_at=request.requested_at,
    )
    return SchemaAuthorityBundle(
        deployment_manifest=manifest,
        workspace_binding=binding,
        graph_capability=grant,
    )


def live_schema_evidence_digest(
    *,
    evidence_id: str,
    environment: str,
    database: str,
    schema_definition_ref: str,
    deployed_sdl_digest: str,
    live_schema_snapshot_digest: str,
    deployment_id: str,
    issuer_authority_ref: str,
    deployment_succeeded: bool,
    active: bool,
    revoked: bool,
    issued_at: object,
    event_kind: str = "current_schema_verification_attestation",
) -> str:
    return sha256_digest(
        {
            "evidence_id": evidence_id,
            "event_kind": event_kind,
            "environment": environment,
            "database": database,
            "schema_definition_ref": schema_definition_ref,
            "deployed_sdl_digest": deployed_sdl_digest,
            "live_schema_snapshot_digest": live_schema_snapshot_digest,
            "deployment_id": deployment_id,
            "issuer_authority_ref": issuer_authority_ref,
            "deployment_succeeded": deployment_succeeded,
            "active": active,
            "revoked": revoked,
            "issued_at": issued_at,
        }
    )


def verify_live_schema_evidence_digest(evidence: LiveSchemaDeploymentEvidence) -> None:
    actual = live_schema_evidence_digest(
        evidence_id=evidence.evidence_id,
        event_kind=evidence.event_kind,
        environment=evidence.environment,
        database=evidence.database,
        schema_definition_ref=evidence.schema_definition_ref,
        deployed_sdl_digest=evidence.deployed_sdl_digest,
        live_schema_snapshot_digest=evidence.live_schema_snapshot_digest,
        deployment_id=evidence.deployment_id,
        issuer_authority_ref=evidence.issuer_authority_ref,
        deployment_succeeded=evidence.deployment_succeeded,
        active=evidence.active,
        revoked=evidence.revoked,
        issued_at=evidence.issued_at,
    )
    if actual != evidence.evidence_digest:
        raise SchemaDeploymentMismatch("live deployment evidence content digest mismatch")


def live_neo4j_schema_snapshot_digest(
    *,
    database: str,
    server_agent: str,
    token_catalog_node_labels: frozenset[str],
    token_catalog_relationship_types: frozenset[str],
    active_node_labels: frozenset[str],
    active_relationship_types: frozenset[str],
    indexes: tuple[Neo4jIndexDescriptor, ...],
    constraints: tuple[Neo4jConstraintDescriptor, ...],
) -> str:
    return sha256_digest(
        {
            "snapshot_schema_version": "2",
            "database": database,
            "server_agent": server_agent,
            "token_catalog_node_labels": token_catalog_node_labels,
            "token_catalog_relationship_types": token_catalog_relationship_types,
            "active_node_labels": active_node_labels,
            "active_relationship_types": active_relationship_types,
            "indexes": indexes,
            "constraints": constraints,
        }
    )


def provision_live_schema_deployment_evidence(
    request: SchemaDeploymentEvidenceProvisioningRequest,
    snapshot: LiveNeo4jSchemaSnapshot,
    identities: SchemaAuthorityIssuerIdentities,
) -> LiveSchemaDeploymentEvidence:
    """Verify deployment inputs and produce evidence for the Issue-12 writer port."""

    source = request.canonical_sdl.encode("utf-8")
    actual_digest = content_sha256_digest(source)
    if actual_digest != request.schema_definition_digest:
        raise SchemaSourceDigestMismatch(
            "canonical SDL bytes do not match the declared Schema Definition digest"
        )
    _verify_live_snapshot(request, snapshot)
    evidence_id = _content_id(
        "live-schema-deployment-evidence",
        sha256_digest(
            {
                "environment": request.environment,
                "database": request.database,
                "deployment_id": request.deployment_id,
            }
        ),
    )
    evidence_digest = live_schema_evidence_digest(
        evidence_id=evidence_id,
        event_kind=request.event_kind,
        environment=request.environment,
        database=request.database,
        schema_definition_ref=request.schema_definition_ref,
        deployed_sdl_digest=request.schema_definition_digest,
        live_schema_snapshot_digest=snapshot.snapshot_digest,
        deployment_id=request.deployment_id,
        issuer_authority_ref=identities.deployment_issuer_authority_ref,
        deployment_succeeded=True,
        active=True,
        revoked=False,
        issued_at=request.issued_at,
    )
    return LiveSchemaDeploymentEvidence(
        evidence_id=evidence_id,
        evidence_digest=evidence_digest,
        event_kind=request.event_kind,
        environment=request.environment,
        database=request.database,
        schema_definition_ref=request.schema_definition_ref,
        deployed_sdl_digest=request.schema_definition_digest,
        live_schema_snapshot_digest=snapshot.snapshot_digest,
        deployment_id=request.deployment_id,
        issuer_authority_ref=identities.deployment_issuer_authority_ref,
        deployment_succeeded=True,
        active=True,
        revoked=False,
        issued_at=request.issued_at,
    )


def live_schema_compatibility_diff(
    request: SchemaDeploymentEvidenceProvisioningRequest,
    snapshot: LiveNeo4jSchemaSnapshot,
) -> LiveSchemaCompatibilityDiff:
    """Return the exact canonical-SDL versus live schema/index comparison."""

    recomputed_snapshot_digest = live_neo4j_schema_snapshot_digest(
        database=snapshot.database,
        server_agent=snapshot.server_agent,
        token_catalog_node_labels=snapshot.token_catalog_node_labels,
        token_catalog_relationship_types=snapshot.token_catalog_relationship_types,
        active_node_labels=snapshot.active_node_labels,
        active_relationship_types=snapshot.active_relationship_types,
        indexes=snapshot.indexes,
        constraints=snapshot.constraints,
    )
    physical = parse_physical_schema(
        request.canonical_sdl.encode("utf-8"),
        request.schema_definition_ref,
    )
    expected_nodes = frozenset(physical.nodes)
    expected_relationships = frozenset(physical.relationships)
    operational_labels = frozenset({"BellLabsSchemaDeploymentEvidence"})
    expected_indexes = tuple(
        sorted(
            (
                *(
                    Neo4jIndexDescriptor(
                        name=str(index.arguments["indexName"]),
                        index_type="FULLTEXT",
                        entity_type="NODE",
                        labels_or_types=(index.node_type,),
                        properties=tuple(
                            _physical_property_name(
                                physical,
                                node_type=index.node_type,
                                logical_property=str(value),
                            )
                            for value in index.arguments["fields"]
                        ),
                        state="ONLINE",
                    )
                    for index in physical.fulltext_indexes
                    if index.arguments.get("indexName")
                    and isinstance(index.arguments.get("fields"), list)
                ),
                *(
                    Neo4jIndexDescriptor(
                        name=str(index.arguments["indexName"]),
                        index_type="VECTOR",
                        entity_type="NODE",
                        labels_or_types=(index.node_type,),
                        properties=(
                            _physical_property_name(
                                physical,
                                node_type=index.node_type,
                                logical_property=str(
                                    index.arguments["embeddingProperty"]
                                ),
                            ),
                        ),
                        state="ONLINE",
                    )
                    for index in physical.vector_indexes
                    if index.arguments.get("indexName")
                    and index.arguments.get("embeddingProperty")
                ),
            ),
            key=lambda item: item.name,
        )
    )
    unexpected_nodes = (
        snapshot.token_catalog_node_labels - expected_nodes - operational_labels
    )
    unexpected_active_nodes = (
        snapshot.active_node_labels - expected_nodes - operational_labels
    )
    unexpected_relationships = (
        snapshot.token_catalog_relationship_types - expected_relationships
    )
    unexpected_active_relationships = (
        snapshot.active_relationship_types - expected_relationships
    )
    missing_canonical_indexes = tuple(
        index for index in expected_indexes if index not in snapshot.indexes
    )
    noncanonical_indexes = tuple(
        index
        for index in snapshot.indexes
        if _targets_noncanonical_schema(
            entity_type=index.entity_type,
            labels_or_types=index.labels_or_types,
            expected_nodes=expected_nodes,
            expected_relationships=expected_relationships,
            operational_labels=operational_labels,
        )
    )
    noncanonical_constraints = tuple(
        constraint
        for constraint in snapshot.constraints
        if _targets_noncanonical_schema(
            entity_type=constraint.entity_type,
            labels_or_types=constraint.labels_or_types,
            expected_nodes=expected_nodes,
            expected_relationships=expected_relationships,
            operational_labels=operational_labels,
        )
    )
    expected_index_names = frozenset(index.name for index in expected_indexes)
    database_matches = snapshot.database == request.database
    snapshot_digest_matches = recomputed_snapshot_digest == snapshot.snapshot_digest
    return LiveSchemaCompatibilityDiff(
        schema_definition_ref=request.schema_definition_ref,
        expected_database=request.database,
        observed_database=snapshot.database,
        database_matches=database_matches,
        observed_snapshot_digest=snapshot.snapshot_digest,
        recomputed_snapshot_digest=recomputed_snapshot_digest,
        snapshot_digest_matches=snapshot_digest_matches,
        operational_node_labels=operational_labels,
        expected_node_labels=expected_nodes,
        observed_node_labels=snapshot.token_catalog_node_labels,
        active_node_labels=snapshot.active_node_labels,
        expected_but_unobserved_node_labels=expected_nodes - snapshot.active_node_labels,
        unexpected_node_labels=unexpected_nodes,
        unexpected_active_node_labels=unexpected_active_nodes,
        expected_relationship_types=expected_relationships,
        observed_relationship_types=snapshot.token_catalog_relationship_types,
        active_relationship_types=snapshot.active_relationship_types,
        expected_but_unobserved_relationship_types=(
            expected_relationships - snapshot.active_relationship_types
        ),
        unexpected_relationship_types=unexpected_relationships,
        unexpected_active_relationship_types=unexpected_active_relationships,
        expected_canonical_indexes=expected_indexes,
        observed_indexes=snapshot.indexes,
        missing_canonical_indexes=missing_canonical_indexes,
        noncanonical_indexes=noncanonical_indexes,
        observed_constraints=snapshot.constraints,
        noncanonical_constraints=noncanonical_constraints,
        expected_index_names=expected_index_names,
        observed_index_names=snapshot.index_names,
        missing_index_names=frozenset(
            index.name for index in missing_canonical_indexes
        ),
        unexpected_index_names=snapshot.index_names - expected_index_names,
        compatible=(
            database_matches
            and snapshot_digest_matches
            and not unexpected_active_nodes
            and not unexpected_active_relationships
            and not missing_canonical_indexes
            and not noncanonical_indexes
            and not noncanonical_constraints
        ),
    )


def _physical_property_name(
    physical: PhysicalSchemaCatalog,
    *,
    node_type: str,
    logical_property: str,
) -> str:
    """Resolve a Neo4j GraphQL field name to its stored property name."""

    object_type = physical.nodes.get(node_type)
    if object_type is None:
        return logical_property
    field = next(
        (candidate for candidate in object_type.fields if candidate.name == logical_property),
        None,
    )
    if field is None:
        return logical_property
    alias = next(
        (directive for directive in field.directives if directive.name == "alias"),
        None,
    )
    physical_property = None if alias is None else alias.arguments.get("property")
    if isinstance(physical_property, str) and physical_property:
        return physical_property
    return logical_property


def _targets_noncanonical_schema(
    *,
    entity_type: str,
    labels_or_types: tuple[str, ...],
    expected_nodes: frozenset[str],
    expected_relationships: frozenset[str],
    operational_labels: frozenset[str],
) -> bool:
    if not labels_or_types:
        return False
    if entity_type.upper() == "NODE":
        allowed = expected_nodes | operational_labels
    elif entity_type.upper() == "RELATIONSHIP":
        allowed = expected_relationships
    else:
        return True
    return not set(labels_or_types).issubset(allowed)


def verify_schema_authority_request_scope(
    request: SchemaAuthorityIssuanceRequest,
) -> None:
    if (
        request.requested_graph_access != "read"
        or not request.workspace_read_only
        or request.slot_name != "graph_query_runtime"
        or request.profile != "graph-query-runtime"
        or request.purpose != "read_query_reconciliation"
        or not request.query_kinds
    ):
        raise GraphCapabilityDenied(
            "schema workspace and graph capability issuance is read-only and purpose-bound"
        )


def _verify_deployment(
    request: SchemaAuthorityIssuanceRequest,
    evidence: LiveSchemaDeploymentEvidence,
    identities: SchemaAuthorityIssuerIdentities,
) -> None:
    verify_live_schema_evidence_digest(evidence)
    if evidence.issuer_authority_ref != identities.deployment_issuer_authority_ref:
        raise SchemaDeploymentMismatch(
            "live deployment evidence was not issued by the configured Issue-12 service"
        )
    if not evidence.deployment_succeeded or not evidence.active or evidence.revoked:
        raise SchemaDeploymentMismatch("live deployment evidence is failed, inactive, or revoked")
    if (
        evidence.environment != request.environment
        or evidence.database != request.database
        or evidence.deployment_id != request.deployment_id
        or evidence.schema_definition_ref != request.schema_definition_ref
        or evidence.deployed_sdl_digest != request.schema_definition_digest
    ):
        raise SchemaDeploymentMismatch(
            "live deployment identity and SDL digest do not match the requested schema"
        )


def _verify_live_snapshot(
    request: SchemaDeploymentEvidenceProvisioningRequest,
    snapshot: LiveNeo4jSchemaSnapshot,
) -> None:
    diff = live_schema_compatibility_diff(request, snapshot)
    if not diff.snapshot_digest_matches:
        raise SchemaDeploymentMismatch("live Neo4j schema snapshot digest mismatch")
    if not diff.database_matches:
        raise SchemaDeploymentMismatch("live Neo4j snapshot belongs to another database")
    if not diff.compatible:
        raise SchemaDeploymentMismatch(
            "live Neo4j schema/index snapshot is incompatible with the canonical SDL: "
            "unexpected_active_node_labels="
            f"{sorted(diff.unexpected_active_node_labels)!r}; "
            "unexpected_active_relationship_types="
            f"{sorted(diff.unexpected_active_relationship_types)!r}; "
            f"missing_index_names={sorted(diff.missing_index_names)!r}; "
            "noncanonical_index_names="
            f"{[item.name for item in diff.noncanonical_indexes]!r}; "
            "noncanonical_constraint_names="
            f"{[item.name for item in diff.noncanonical_constraints]!r}"
        )


def _content_id(kind: str, digest: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"{kind}:{digest}"))
