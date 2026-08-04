from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.application.external_candidate_inspection import (
    ExternalCandidateInspectionRequest,
    ExternalCandidateInspectionService,
    InMemoryExternalCandidateInspectionRepository,
    InspectedMCPTool,
    InspectionAuthorizationError,
    InspectionBounds,
    InspectionFinding,
    InspectionFindingSeverity,
    InspectionPrincipal,
    InspectionProbeMode,
    InspectionStatus,
    PromotionReadiness,
    QuarantineInspectionExecution,
    QuarantineInspectionObservations,
)
from app.application.external_candidate_repository import (
    ExternalCandidatePersistenceError,
    InMemoryExternalCandidateRepository,
)
from app.application.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
    ExternalDiscoverySource,
)

NOW = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)
RAW_DIGEST = f"sha256:{'a' * 64}"
CONTENT_DIGEST = f"sha256:{'b' * 64}"
TOOL_DIGEST = f"sha256:{'c' * 64}"


class RecordingInspector:
    def __init__(
        self,
        observations: QuarantineInspectionObservations,
    ) -> None:
        self.observations = observations
        self.executions: list[QuarantineInspectionExecution] = []

    async def inspect(
        self,
        execution: QuarantineInspectionExecution,
    ) -> QuarantineInspectionObservations:
        self.executions.append(execution)
        return self.observations


class ExplodingInspector:
    async def inspect(
        self,
        execution: QuarantineInspectionExecution,
    ) -> QuarantineInspectionObservations:
        del execution
        raise RuntimeError("Bearer super-secret-value")


def discovery_batch(
    *,
    source: ExternalDiscoverySource = ExternalDiscoverySource.MCP_REGISTRY,
    locator: str = "https://registry.modelcontextprotocol.io/v0.1/servers/example",
) -> ExternalDiscoveryBatch:
    evidence = ExternalDiscoveryEvidence(
        source=source,
        source_version=(
            "v0.1"
            if source == ExternalDiscoverySource.MCP_REGISTRY
            else "skills@1.5.20"
        ),
        query="missing capability",
        retrieved_at=NOW,
        raw_response_digest=RAW_DIGEST,
        raw_response_size_bytes=512,
    )
    candidate = ExternalDiscoveryCandidate(
        candidate_id=f"candidate:sha256:{'d' * 64}",
        source=source,
        upstream_identity="example/server",
        upstream_version="1.0.0",
        locator=locator,
        publisher="example",
        discovered_at=NOW,
        query="missing capability",
        raw_response_digest=RAW_DIGEST,
    )
    return ExternalDiscoveryBatch(
        source=source,
        candidates=(candidate,),
        evidence=(evidence,),
    )


async def stored_candidates(
    *,
    source: ExternalDiscoverySource = ExternalDiscoverySource.MCP_REGISTRY,
    locator: str = "https://registry.modelcontextprotocol.io/v0.1/servers/example",
) -> tuple[InMemoryExternalCandidateRepository, ExternalDiscoveryCandidate]:
    repository = InMemoryExternalCandidateRepository(clock=lambda: NOW)
    recorded = await repository.record(discovery_batch(source=source, locator=locator))
    return repository, recorded.candidates[0]


def principal(*roles: str) -> InspectionPrincipal:
    return InspectionPrincipal(
        actor_id="coordinator-actor",
        tenant_scope="tenant-a",
        roles=frozenset(roles or ("coordinator_planner",)),
    )


def request(candidate: ExternalDiscoveryCandidate) -> ExternalCandidateInspectionRequest:
    return ExternalCandidateInspectionRequest(
        candidate_id=candidate.candidate_id,
        correlation_id="correlation-1",
        requested_at=NOW,
        requested_capabilities=frozenset({"search"}),
    )


def safe_observations() -> QuarantineInspectionObservations:
    return QuarantineInspectionObservations(
        manifest_valid=True,
        provenance_verified=True,
        immutable_content_digest=CONTENT_DIGEST,
        license_evidence=("Apache-2.0",),
        secret_requirement_names=frozenset({"EXAMPLE_API_KEY"}),
        network_requirement_hosts=frozenset(
            {"registry.modelcontextprotocol.io"}
        ),
        network_hosts_contacted=frozenset({"registry.modelcontextprotocol.io"}),
        requested_capabilities=frozenset({"search"}),
        tools=(
            InspectedMCPTool(
                name="search",
                schema_digest=TOOL_DIGEST,
                side_effect_classification="read_only",
            ),
        ),
        findings=(
            InspectionFinding(
                code="MANIFEST_VALID",
                severity=InspectionFindingSeverity.INFO,
                summary="Manifest structure and immutable metadata were validated.",
            ),
        ),
        downloaded_bytes=1_024,
        files_inspected=2,
        largest_file_bytes=512,
        network_requests=1,
        report_size_bytes=2_048,
        tools_list_probe_used=True,
    )


@pytest.mark.asyncio
async def test_discovery_records_are_immutable_sanitized_observations() -> None:
    repository, candidate = await stored_candidates()
    record = await repository.get_candidate(candidate.candidate_id)
    evidence = await repository.get_evidence(record.evidence_id)

    assert record.content_digest.startswith("sha256:")
    assert record.candidate.raw_response_ref is not None
    assert "sanitized-metadata" in record.candidate.raw_response_ref
    assert evidence.evidence.raw_response_size_bytes == 512
    serialized = evidence.model_dump_json()
    assert "raw_response_body" not in serialized
    assert "super-secret" not in serialized

    repeated = await repository.record(discovery_batch())
    assert repeated.candidates == (record.candidate,)
    assert len(await repository.list_candidate_records(candidate.candidate_id)) == 1


@pytest.mark.asyncio
async def test_candidate_requires_evidence_from_same_discovery_batch() -> None:
    repository = InMemoryExternalCandidateRepository(clock=lambda: NOW)
    batch = discovery_batch().model_copy(update={"evidence": ()})
    with pytest.raises(
        ExternalCandidatePersistenceError,
        match="does not reference evidence",
    ):
        await repository.record(batch)


def test_candidate_rejects_credential_bearing_locator() -> None:
    with pytest.raises(ValidationError, match="cannot embed URI credentials"):
        discovery_batch(locator="https://user:password@example.com/server")
    with pytest.raises(ValidationError, match="credential query values"):
        discovery_batch(locator="https://example.com/server?api_key=secret")


@pytest.mark.asyncio
async def test_inspection_is_bounded_non_installing_and_produces_promotion_request() -> None:
    candidates, candidate = await stored_candidates()
    inspector = RecordingInspector(safe_observations())
    records = InMemoryExternalCandidateInspectionRepository()
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=inspector,
        records=records,
        bounds=InspectionBounds(),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "1" * 32,
    )

    report = await service.inspect(principal(), request(candidate))

    assert report.status == InspectionStatus.PASSED
    assert report.promotion_request.readiness == PromotionReadiness.READY_FOR_HUMAN_REVIEW
    assert report.promotion_request.required_role == "control_plane_publisher"
    assert report.promotion_request.attach_to_current_run is False
    assert report.content_classification == "sanitized_untrusted_inspection_report"
    assert report.report_digest.startswith("sha256:")
    assert await records.get_report(report.inspection_id) == report

    execution = inspector.executions[0]
    assert execution.service_identity == "candidate-inspection-service"
    assert execution.install_allowed is False
    assert execution.execute_candidate_allowed is False
    assert execution.workspace.inputs_read_only is True
    assert execution.workspace.candidate_bundle_mounted is False
    assert execution.workspace.agent_environment_mounted is False
    assert execution.workspace.probe_mode == InspectionProbeMode.STATIC_AND_MCP_TOOLS_LIST
    assert execution.workspace.network_host_allowlist == frozenset(
        {"registry.modelcontextprotocol.io"}
    )


@pytest.mark.asyncio
async def test_skill_candidate_cannot_request_tools_list_or_execution() -> None:
    candidates, candidate = await stored_candidates(
        source=ExternalDiscoverySource.NPX_SKILLS,
        locator="https://skills.sh/example/repository/skill",
    )
    observations = safe_observations().model_copy(
        update={
            "network_requirement_hosts": frozenset({"skills.sh"}),
            "tools_list_probe_used": False,
        }
    )
    inspector = RecordingInspector(observations)
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=inspector,
        records=InMemoryExternalCandidateInspectionRepository(),
        bounds=InspectionBounds(),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "2" * 32,
    )

    await service.inspect(principal(), request(candidate))

    workspace = inspector.executions[0].workspace
    assert workspace.probe_mode == InspectionProbeMode.STATIC_ONLY
    assert workspace.install_allowed is False
    assert workspace.execute_candidate_allowed is False


@pytest.mark.asyncio
async def test_bound_violation_forces_failed_non_promotable_report() -> None:
    candidates, candidate = await stored_candidates()
    inspector = RecordingInspector(
        safe_observations().model_copy(update={"network_requests": 21})
    )
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=inspector,
        records=InMemoryExternalCandidateInspectionRepository(),
        bounds=InspectionBounds(max_network_requests=20),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "3" * 32,
    )

    report = await service.inspect(principal(), request(candidate))

    assert report.status == InspectionStatus.FAILED
    assert report.promotion_request.readiness == PromotionReadiness.NOT_READY
    assert "INSPECTION_BOUND_EXCEEDED" in report.promotion_request.blocking_findings


@pytest.mark.asyncio
async def test_failed_promotion_gates_emit_stable_bounded_unique_blockers() -> None:
    candidates, candidate = await stored_candidates()
    repeated_and_unique_errors = (
        InspectionFinding(
            code="UNTRUSTED_FINDING_000",
            severity=InspectionFindingSeverity.ERROR,
            summary="A repeated untrusted inspection finding.",
        ),
        *(
            InspectionFinding(
                code=f"UNTRUSTED_FINDING_{index:03d}",
                severity=InspectionFindingSeverity.ERROR,
                summary="A bounded untrusted inspection finding.",
            )
            for index in range(110)
        ),
    )
    inspector = RecordingInspector(
        QuarantineInspectionObservations(
            manifest_valid=False,
            provenance_verified=False,
            immutable_content_digest=None,
            license_evidence=(),
            findings=repeated_and_unique_errors,
        )
    )
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=inspector,
        records=InMemoryExternalCandidateInspectionRepository(),
        bounds=InspectionBounds(max_findings=500),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "9" * 32,
    )

    report = await service.inspect(principal(), request(candidate))

    blockers = report.promotion_request.blocking_findings
    assert blockers[:4] == (
        "MANIFEST_INVALID_OR_UNAVAILABLE",
        "PROVENANCE_UNVERIFIED",
        "IMMUTABLE_CONTENT_UNAVAILABLE",
        "LICENSE_EVIDENCE_MISSING",
    )
    assert len(blockers) == 100
    assert len(set(blockers)) == len(blockers)
    assert report.status == InspectionStatus.FAILED
    assert report.promotion_request.readiness == PromotionReadiness.NOT_READY


@pytest.mark.asyncio
async def test_runner_error_is_sanitized_and_persisted() -> None:
    candidates, candidate = await stored_candidates()
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=ExplodingInspector(),
        records=InMemoryExternalCandidateInspectionRepository(),
        bounds=InspectionBounds(),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "4" * 32,
    )

    report = await service.inspect(principal(), request(candidate))

    assert report.status == InspectionStatus.FAILED
    serialized = report.model_dump_json()
    assert "INSPECTION_RUNNER_FAILED" in serialized
    assert "super-secret-value" not in serialized


@pytest.mark.asyncio
async def test_unauthorized_actor_cannot_allocate_or_run_inspection() -> None:
    candidates, candidate = await stored_candidates()
    inspector = RecordingInspector(safe_observations())
    records = InMemoryExternalCandidateInspectionRepository()
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=inspector,
        records=records,
        bounds=InspectionBounds(),
        service_identity="candidate-inspection-service",
        clock=lambda: NOW,
        id_factory=lambda: "5" * 32,
    )

    with pytest.raises(InspectionAuthorizationError):
        await service.inspect(principal("viewer"), request(candidate))
    assert inspector.executions == []
