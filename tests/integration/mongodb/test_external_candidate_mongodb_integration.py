from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.web_research.external_candidate_inspection import (
    BeanieExternalCandidateInspectionRepository,
    ExternalCandidateInspectionRequest,
    ExternalCandidateInspectionService,
    InspectionBounds,
    InspectionPrincipal,
    QuarantineInspectionExecution,
    QuarantineInspectionObservations,
)
from app.application.web_research.external_candidate_repository import (
    BeanieExternalCandidateRepository,
)
from app.application.web_research.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
    ExternalDiscoverySource,
)
from app.integrations.mongodb import BEANIE_MODELS
from app.models.external_capability import (
    ExternalCandidateInspectionReportDocument,
    ExternalCandidateInspectionWorkspaceDocument,
    ExternalDiscoveryCandidateDocument,
    ExternalDiscoveryEvidenceDocument,
)

NOW = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)
RAW_DIGEST = f"sha256:{'a' * 64}"


class StaticInspector:
    async def inspect(
        self,
        execution: QuarantineInspectionExecution,
    ) -> QuarantineInspectionObservations:
        return QuarantineInspectionObservations(
            manifest_valid=False,
            provenance_verified=False,
            network_requirement_hosts=execution.workspace.network_host_allowlist,
        )


async def test_real_mongodb_candidate_and_inspection_records_are_append_only(
    test_mongodb_uri: str,
) -> None:
    database_name = f"candidate_test_{uuid4().hex[:16]}"
    client = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    database = client[database_name]
    try:
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        candidates = BeanieExternalCandidateRepository(clock=lambda: NOW)
        evidence = ExternalDiscoveryEvidence(
            source=ExternalDiscoverySource.MCP_REGISTRY,
            source_version="v0.1",
            query="missing capability",
            retrieved_at=NOW,
            raw_response_digest=RAW_DIGEST,
            raw_response_size_bytes=100,
        )
        candidate = ExternalDiscoveryCandidate(
            candidate_id=f"candidate:sha256:{'d' * 64}",
            source=ExternalDiscoverySource.MCP_REGISTRY,
            upstream_identity="example/server",
            upstream_version="1.0.0",
            locator="https://registry.modelcontextprotocol.io/v0.1/servers/example",
            publisher="example",
            discovered_at=NOW,
            query="missing capability",
            raw_response_digest=RAW_DIGEST,
        )
        batch = ExternalDiscoveryBatch(
            source=ExternalDiscoverySource.MCP_REGISTRY,
            candidates=(candidate,),
            evidence=(evidence,),
        )

        first = await candidates.record(batch)
        second = await candidates.record(batch)
        assert first == second
        assert await ExternalDiscoveryEvidenceDocument.count() == 1
        assert await ExternalDiscoveryCandidateDocument.count() == 1

        stored = await candidates.get_candidate(candidate.candidate_id)
        inspection_records = BeanieExternalCandidateInspectionRepository()
        inspection = ExternalCandidateInspectionService(
            candidates=candidates,
            runner=StaticInspector(),
            records=inspection_records,
            bounds=InspectionBounds(),
            service_identity="candidate-inspection-service",
            clock=lambda: NOW,
            id_factory=lambda: "6" * 32,
        )
        report = await inspection.inspect(
            InspectionPrincipal(
                actor_id="planner",
                tenant_scope="tenant-a",
                roles=frozenset({"coordinator_planner"}),
            ),
            ExternalCandidateInspectionRequest(
                candidate_id=stored.candidate.candidate_id,
                correlation_id="correlation",
                requested_at=NOW,
            ),
        )
        assert await inspection_records.get_report(report.inspection_id) == report
        assert await ExternalCandidateInspectionWorkspaceDocument.count() == 1
        assert await ExternalCandidateInspectionReportDocument.count() == 1
    finally:
        await client.drop_database(database_name)
        await client.close()
