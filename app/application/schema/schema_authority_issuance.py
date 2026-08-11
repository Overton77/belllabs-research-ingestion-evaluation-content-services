from __future__ import annotations

from typing import Protocol

from app.application.schema.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.domain.schema_grounding.authority import (
    issue_schema_authority_bundle,
    provision_live_schema_deployment_evidence,
    verify_schema_authority_request_scope,
)
from app.domain.schema_grounding.contracts import (
    LiveNeo4jSchemaSnapshot,
    LiveSchemaDeploymentEvidence,
    SchemaAuthorityBundle,
    SchemaAuthorityIssuanceRequest,
    SchemaAuthorityIssuerIdentities,
    SchemaDeploymentEvidenceProvisioningRequest,
)
from app.domain.schema_grounding.errors import SchemaDeploymentMismatch

DEPLOYMENT_AUDIT_RECORD_VERSION = "deployment-audit-v2"


def deployment_audit_record_id(kind: str, authority_id: str) -> str:
    """Return a stable envelope identity isolated from legacy run-scoped records."""

    return f"{DEPLOYMENT_AUDIT_RECORD_VERSION}:{kind}:{authority_id}"


class LiveSchemaDeploymentReader(Protocol):
    async def read(
        self,
        *,
        environment: str,
        database: str,
        deployment_id: str,
    ) -> LiveSchemaDeploymentEvidence | None: ...


class SchemaDeploymentEvidenceProvisioningPort(Protocol):
    async def read_schema_snapshot(
        self,
        *,
        database: str,
    ) -> LiveNeo4jSchemaSnapshot: ...

    async def write_deployment_evidence(
        self,
        evidence: LiveSchemaDeploymentEvidence,
    ) -> LiveSchemaDeploymentEvidence: ...


class SchemaDeploymentEvidenceProvisioningService:
    """Operator-only Issue-12 boundary; coordinator composition must not include it."""

    def __init__(
        self,
        *,
        graph: SchemaDeploymentEvidenceProvisioningPort,
        identities: SchemaAuthorityIssuerIdentities,
    ) -> None:
        self._graph = graph
        self._identities = identities

    async def provision(
        self,
        request: SchemaDeploymentEvidenceProvisioningRequest,
    ) -> LiveSchemaDeploymentEvidence:
        snapshot = await self._graph.read_schema_snapshot(database=request.database)
        evidence = provision_live_schema_deployment_evidence(
            request,
            snapshot,
            self._identities,
        )
        return await self._graph.write_deployment_evidence(evidence)


class SchemaAuthorityIssuanceService:
    """Issue and durably audit the three independent Scenario-C authorities."""

    def __init__(
        self,
        *,
        deployment_reader: LiveSchemaDeploymentReader,
        records: SchemaGroundingRecordRepository,
        identities: SchemaAuthorityIssuerIdentities,
    ) -> None:
        self._deployment_reader = deployment_reader
        self._records = records
        self._identities = identities

    async def issue(
        self,
        request: SchemaAuthorityIssuanceRequest,
    ) -> SchemaAuthorityBundle:
        verify_schema_authority_request_scope(request)
        evidence = await self._deployment_reader.read(
            environment=request.environment,
            database=request.database,
            deployment_id=request.deployment_id,
        )
        if evidence is None:
            raise SchemaDeploymentMismatch(
                "no live deployment evidence exists for the requested graph target"
            )
        bundle = issue_schema_authority_bundle(request, evidence, self._identities)
        await self._records.append(
            schema_grounding_record(
                record_type="deployment_evidence",
                record_id=deployment_audit_record_id(
                    "evidence",
                    evidence.evidence_id,
                ),
                request_scope=request.request_scope,
                run_id=None,
                payload=evidence.model_dump(mode="json"),
                created_at=evidence.issued_at,
            )
        )
        await self._records.append(
            schema_grounding_record(
                record_type="deployment_manifest",
                record_id=deployment_audit_record_id(
                    "manifest",
                    bundle.deployment_manifest.manifest_id,
                ),
                request_scope=request.request_scope,
                run_id=None,
                payload=bundle.deployment_manifest.model_dump(mode="json"),
                created_at=bundle.deployment_manifest.issued_at,
            )
        )
        await self._records.append(
            schema_grounding_record(
                record_type="workspace_binding",
                record_id=bundle.workspace_binding.binding_id,
                request_scope=request.request_scope,
                run_id=request.run_id,
                payload=bundle.workspace_binding.model_dump(mode="json"),
                created_at=bundle.workspace_binding.created_at,
            )
        )
        await self._records.append(
            schema_grounding_record(
                record_type="graph_capability",
                record_id=bundle.graph_capability.grant_id,
                request_scope=request.request_scope,
                run_id=request.run_id,
                payload=bundle.graph_capability.model_dump(mode="json"),
                created_at=bundle.graph_capability.decided_at,
            )
        )
        return bundle
