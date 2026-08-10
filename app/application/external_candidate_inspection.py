from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Literal, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pymongo.errors import DuplicateKeyError

from app.application.external_candidate_repository import (
    ExternalCandidateNotFound,
    PersistedExternalCandidate,
)
from app.domain.control_plane.canonical import sha256_digest
from app.models.external_capability import (
    ExternalCandidateInspectionReportDocument,
    ExternalCandidateInspectionWorkspaceDocument,
)

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
MAX_PROMOTION_BLOCKING_FINDINGS = 100


class InspectionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InspectionAuthorizationError(PermissionError):
    pass


class InspectionPersistenceError(RuntimeError):
    pass


class InspectionStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


class InspectionFindingSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class InspectionProbeMode(StrEnum):
    STATIC_ONLY = "static_only"
    STATIC_AND_MCP_TOOLS_LIST = "static_and_mcp_tools_list"


class PromotionReadiness(StrEnum):
    READY_FOR_HUMAN_REVIEW = "ready_for_human_review"
    NOT_READY = "not_ready"


class InspectionPrincipal(InspectionContract):
    actor_id: str = Field(min_length=1, max_length=256)
    tenant_scope: str = Field(min_length=1, max_length=256)
    roles: frozenset[str] = Field(min_length=1, max_length=32)


class ExternalCandidateInspectionRequest(InspectionContract):
    candidate_id: str = Field(min_length=1, max_length=256)
    correlation_id: str = Field(min_length=1, max_length=256)
    requested_at: AwareDatetime
    requested_capabilities: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=64,
    )

    @field_validator("requested_capabilities")
    @classmethod
    def requested_capability_names_are_bounded(
        cls,
        value: frozenset[str],
    ) -> frozenset[str]:
        normalized = frozenset(item.strip() for item in value)
        if any(not item or len(item) > 256 for item in normalized):
            raise ValueError("requested capability names must contain 1 to 256 characters")
        return normalized


class InspectionBounds(InspectionContract):
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    workspace_ttl_seconds: int = Field(default=300, ge=30, le=900)
    max_download_bytes: int = Field(default=10_000_000, ge=1_024, le=100_000_000)
    max_files: int = Field(default=250, ge=1, le=2_000)
    max_file_bytes: int = Field(default=2_000_000, ge=1_024, le=10_000_000)
    max_network_requests: int = Field(default=20, ge=0, le=100)
    max_network_destinations: int = Field(default=8, ge=0, le=32)
    max_findings: int = Field(default=100, ge=1, le=500)
    max_report_bytes: int = Field(default=250_000, ge=1_024, le=2_000_000)

    @model_validator(mode="after")
    def workspace_outlives_inspection(self) -> InspectionBounds:
        if self.workspace_ttl_seconds <= self.timeout_seconds:
            raise ValueError("inspection workspace TTL must exceed the inspection timeout")
        return self


class ExternalCandidateInspectionWorkspace(InspectionContract):
    workspace_id: str = Field(min_length=1, max_length=256)
    candidate_id: str = Field(min_length=1, max_length=256)
    candidate_record_id: str = Field(min_length=1, max_length=256)
    candidate_content_digest: str = Field(pattern=DIGEST_PATTERN)
    root_ref: str = Field(pattern=r"^quarantine://inspection/[a-f0-9]{32}$")
    allocated_at: AwareDatetime
    expires_at: AwareDatetime
    bounds: InspectionBounds
    network_host_allowlist: frozenset[str] = Field(max_length=32)
    probe_mode: InspectionProbeMode
    inputs_read_only: Literal[True] = True
    candidate_bundle_mounted: Literal[False] = False
    agent_environment_mounted: Literal[False] = False
    install_allowed: Literal[False] = False
    execute_candidate_allowed: Literal[False] = False
    content_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_workspace_envelope(self) -> ExternalCandidateInspectionWorkspace:
        if self.expires_at <= self.allocated_at:
            raise ValueError("inspection workspace must expire after allocation")
        if len(self.network_host_allowlist) > self.bounds.max_network_destinations:
            raise ValueError("inspection network allowlist exceeds its bound")
        if self.content_digest != _workspace_content_digest(self):
            raise ValueError("inspection workspace content digest mismatch")
        return self


class InspectionFinding(InspectionContract):
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{2,63}$")
    severity: InspectionFindingSeverity
    summary: str = Field(min_length=1, max_length=1_000)
    evidence_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    content_classification: Literal["untrusted_data"] = "untrusted_data"


class InspectedMCPTool(InspectionContract):
    name: str = Field(min_length=1, max_length=256)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    side_effect_classification: Literal[
        "read_only",
        "bounded_write",
        "consequential",
        "unknown",
    ] = "unknown"


class QuarantineInspectionObservations(InspectionContract):
    manifest_valid: bool
    provenance_verified: bool
    immutable_content_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    license_evidence: tuple[str, ...] = Field(default_factory=tuple, max_length=32)
    secret_requirement_names: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=32,
    )
    network_requirement_hosts: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=32,
    )
    network_hosts_contacted: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=32,
    )
    requested_capabilities: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=64,
    )
    tools: tuple[InspectedMCPTool, ...] = Field(default_factory=tuple, max_length=256)
    findings: tuple[InspectionFinding, ...] = Field(default_factory=tuple, max_length=500)
    downloaded_bytes: int = Field(default=0, ge=0)
    files_inspected: int = Field(default=0, ge=0)
    largest_file_bytes: int = Field(default=0, ge=0)
    network_requests: int = Field(default=0, ge=0)
    report_size_bytes: int = Field(default=0, ge=0)
    tools_list_probe_used: bool = False

    @field_validator("secret_requirement_names")
    @classmethod
    def secret_requirements_are_names_not_values(
        cls,
        value: frozenset[str],
    ) -> frozenset[str]:
        normalized = frozenset(item.strip() for item in value)
        if any(
            not item or len(item) > 128 or not item.replace("_", "").replace("-", "").isalnum()
            for item in normalized
        ):
            raise ValueError("secret requirements must be bounded reference names")
        return normalized

    @field_validator("network_requirement_hosts", "network_hosts_contacted")
    @classmethod
    def network_requirements_are_hostnames(
        cls,
        value: frozenset[str],
    ) -> frozenset[str]:
        normalized = frozenset(item.strip().casefold() for item in value)
        if any(
            not item or len(item) > 253 or "://" in item or "/" in item or "@" in item
            for item in normalized
        ):
            raise ValueError("network requirements must contain hostnames only")
        return normalized


class QuarantineInspectionExecution(InspectionContract):
    inspection_id: str = Field(min_length=1, max_length=256)
    service_identity: str = Field(min_length=1, max_length=256)
    candidate: PersistedExternalCandidate
    workspace: ExternalCandidateInspectionWorkspace
    requested_capabilities: frozenset[str]
    install_allowed: Literal[False] = False
    execute_candidate_allowed: Literal[False] = False


class CandidatePromotionInspectionRequest(InspectionContract):
    candidate_id: str = Field(min_length=1, max_length=256)
    candidate_record_id: str = Field(min_length=1, max_length=256)
    inspection_id: str = Field(min_length=1, max_length=256)
    readiness: PromotionReadiness
    required_role: Literal["control_plane_publisher"] = "control_plane_publisher"
    attach_to_current_run: Literal[False] = False
    evidence_refs: tuple[str, ...] = Field(min_length=1, max_length=16)
    blocking_findings: tuple[str, ...] = Field(max_length=100)


class ExternalCandidateInspectionReport(InspectionContract):
    inspection_id: str = Field(min_length=1, max_length=256)
    candidate_id: str = Field(min_length=1, max_length=256)
    candidate_record_id: str = Field(min_length=1, max_length=256)
    candidate_content_digest: str = Field(pattern=DIGEST_PATTERN)
    workspace_id: str = Field(min_length=1, max_length=256)
    evidence_id: str = Field(min_length=1, max_length=256)
    requester_actor_id: str = Field(min_length=1, max_length=256)
    tenant_scope: str = Field(min_length=1, max_length=256)
    service_identity: str = Field(min_length=1, max_length=256)
    correlation_id: str = Field(min_length=1, max_length=256)
    requested_at: AwareDatetime
    started_at: AwareDatetime
    completed_at: AwareDatetime
    status: InspectionStatus
    observation_digest: str = Field(pattern=DIGEST_PATTERN)
    manifest_valid: bool
    provenance_verified: bool
    immutable_content_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    license_evidence: tuple[str, ...]
    secret_requirement_names: frozenset[str]
    network_requirement_hosts: frozenset[str]
    network_hosts_contacted: frozenset[str]
    requested_capabilities: frozenset[str]
    tools: tuple[InspectedMCPTool, ...]
    findings: tuple[InspectionFinding, ...]
    promotion_request: CandidatePromotionInspectionRequest
    content_classification: Literal["sanitized_untrusted_inspection_report"] = (
        "sanitized_untrusted_inspection_report"
    )
    report_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_report_envelope(self) -> ExternalCandidateInspectionReport:
        if self.completed_at < self.started_at or self.started_at < self.requested_at:
            raise ValueError("inspection report timestamps are out of order")
        if self.promotion_request.inspection_id != self.inspection_id:
            raise ValueError("promotion request is not bound to this inspection")
        if self.report_digest != _report_content_digest(self):
            raise ValueError("inspection report content digest mismatch")
        return self


class CandidateRecordReader(Protocol):
    async def get_candidate(self, candidate_id: str) -> PersistedExternalCandidate: ...


class QuarantineInspectionRunner(Protocol):
    async def inspect(
        self,
        execution: QuarantineInspectionExecution,
    ) -> QuarantineInspectionObservations: ...


class ExternalCandidateInspectionRepository(Protocol):
    async def append_workspace(
        self,
        workspace: ExternalCandidateInspectionWorkspace,
    ) -> ExternalCandidateInspectionWorkspace: ...

    async def append_report(
        self,
        report: ExternalCandidateInspectionReport,
    ) -> ExternalCandidateInspectionReport: ...

    async def get_report(
        self,
        inspection_id: str,
    ) -> ExternalCandidateInspectionReport: ...


class InMemoryExternalCandidateInspectionRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._workspaces: dict[str, ExternalCandidateInspectionWorkspace] = {}
        self._reports: dict[str, ExternalCandidateInspectionReport] = {}

    async def append_workspace(
        self,
        workspace: ExternalCandidateInspectionWorkspace,
    ) -> ExternalCandidateInspectionWorkspace:
        async with self._lock:
            _append_immutable(
                self._workspaces,
                workspace.workspace_id,
                workspace,
                subject="inspection workspace",
            )
        return workspace.model_copy(deep=True)

    async def append_report(
        self,
        report: ExternalCandidateInspectionReport,
    ) -> ExternalCandidateInspectionReport:
        async with self._lock:
            _append_immutable(
                self._reports,
                report.inspection_id,
                report,
                subject="inspection report",
            )
        return report.model_copy(deep=True)

    async def get_report(
        self,
        inspection_id: str,
    ) -> ExternalCandidateInspectionReport:
        try:
            return self._reports[inspection_id].model_copy(deep=True)
        except KeyError as error:
            raise ExternalCandidateNotFound(
                f"inspection report not found: {inspection_id}"
            ) from error

    async def get_workspace(
        self,
        workspace_id: str,
    ) -> ExternalCandidateInspectionWorkspace:
        try:
            return self._workspaces[workspace_id].model_copy(deep=True)
        except KeyError as error:
            raise ExternalCandidateNotFound(
                f"inspection workspace not found: {workspace_id}"
            ) from error


class BeanieExternalCandidateInspectionRepository:
    async def append_workspace(
        self,
        workspace: ExternalCandidateInspectionWorkspace,
    ) -> ExternalCandidateInspectionWorkspace:
        document = ExternalCandidateInspectionWorkspaceDocument(
            workspace_id=workspace.workspace_id,
            candidate_id=workspace.candidate_id,
            candidate_record_id=workspace.candidate_record_id,
            content_digest=workspace.content_digest,
            payload=workspace.model_dump(mode="json"),
            allocated_at=workspace.allocated_at,
            expires_at=workspace.expires_at,
        )
        try:
            await document.insert()
            return workspace
        except DuplicateKeyError:
            existing = await ExternalCandidateInspectionWorkspaceDocument.find_one(
                ExternalCandidateInspectionWorkspaceDocument.workspace_id == workspace.workspace_id
            )
            if existing is None:
                raise InspectionPersistenceError(
                    "inspection workspace uniqueness conflict"
                ) from None
            prior = _workspace_from_document(existing)
            if prior != workspace:
                raise InspectionPersistenceError(
                    "immutable inspection workspace identity conflict"
                ) from None
            return prior

    async def append_report(
        self,
        report: ExternalCandidateInspectionReport,
    ) -> ExternalCandidateInspectionReport:
        document = ExternalCandidateInspectionReportDocument(
            inspection_id=report.inspection_id,
            candidate_id=report.candidate_id,
            candidate_record_id=report.candidate_record_id,
            workspace_id=report.workspace_id,
            status=report.status.value,
            report_digest=report.report_digest,
            payload=report.model_dump(mode="json"),
            requested_at=report.requested_at,
            completed_at=report.completed_at,
        )
        try:
            await document.insert()
            return report
        except DuplicateKeyError:
            existing = await ExternalCandidateInspectionReportDocument.find_one(
                ExternalCandidateInspectionReportDocument.inspection_id == report.inspection_id
            )
            if existing is None:
                raise InspectionPersistenceError("inspection report uniqueness conflict") from None
            prior = _report_from_document(existing)
            if prior != report:
                raise InspectionPersistenceError(
                    "immutable inspection report identity conflict"
                ) from None
            return prior

    async def get_report(
        self,
        inspection_id: str,
    ) -> ExternalCandidateInspectionReport:
        document = await ExternalCandidateInspectionReportDocument.find_one(
            ExternalCandidateInspectionReportDocument.inspection_id == inspection_id
        )
        if document is None:
            raise ExternalCandidateNotFound(f"inspection report not found: {inspection_id}")
        return _report_from_document(document)

    async def get_workspace(
        self,
        workspace_id: str,
    ) -> ExternalCandidateInspectionWorkspace:
        document = await ExternalCandidateInspectionWorkspaceDocument.find_one(
            ExternalCandidateInspectionWorkspaceDocument.workspace_id == workspace_id
        )
        if document is None:
            raise ExternalCandidateNotFound(f"inspection workspace not found: {workspace_id}")
        return _workspace_from_document(document)


class ExternalCandidateInspectionService:
    """Run one bounded, non-installing inspection under a dedicated identity."""

    def __init__(
        self,
        *,
        candidates: CandidateRecordReader,
        runner: QuarantineInspectionRunner,
        records: ExternalCandidateInspectionRepository,
        bounds: InspectionBounds,
        service_identity: str,
        allowed_requester_roles: frozenset[str] = frozenset(
            {"coordinator_planner", "control_plane_publisher"}
        ),
        allow_mcp_tools_list_probe: bool = True,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        if not service_identity.strip():
            raise ValueError("inspection service identity cannot be blank")
        if not allowed_requester_roles:
            raise ValueError("inspection requester roles cannot be empty")
        self._candidates = candidates
        self._runner = runner
        self._records = records
        self._bounds = bounds
        self._service_identity = service_identity
        self._allowed_requester_roles = allowed_requester_roles
        self._allow_mcp_tools_list_probe = allow_mcp_tools_list_probe
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory or (lambda: uuid4().hex)

    async def inspect(
        self,
        principal: InspectionPrincipal,
        request: ExternalCandidateInspectionRequest,
    ) -> ExternalCandidateInspectionReport:
        if principal.roles.isdisjoint(self._allowed_requester_roles):
            raise InspectionAuthorizationError(
                "caller is not authorized to request candidate inspection"
            )
        candidate = await self._candidates.get_candidate(request.candidate_id)
        started_at = max(self._clock(), request.requested_at)
        nonce = self._id_factory()
        inspection_id = f"inspection:{nonce}"
        workspace = _workspace(
            candidate,
            inspection_id=inspection_id,
            nonce=nonce,
            allocated_at=started_at,
            bounds=self._bounds,
            allow_mcp_tools_list_probe=self._allow_mcp_tools_list_probe,
        )
        await self._records.append_workspace(workspace)
        execution = QuarantineInspectionExecution(
            inspection_id=inspection_id,
            service_identity=self._service_identity,
            candidate=candidate,
            workspace=workspace,
            requested_capabilities=request.requested_capabilities,
        )
        try:
            observations = await asyncio.wait_for(
                self._runner.inspect(execution),
                timeout=self._bounds.timeout_seconds,
            )
        except TimeoutError:
            observations = _failed_observations(
                "INSPECTION_TIMEOUT",
                "The quarantined inspection exceeded its time bound.",
            )
        except Exception:
            observations = _failed_observations(
                "INSPECTION_RUNNER_FAILED",
                "The quarantined inspection failed; dependency details remain server-side.",
            )
        observations = _enforce_bounds(observations, workspace)
        completed_at = max(self._clock(), started_at)
        report = _report(
            principal=principal,
            request=request,
            candidate=candidate,
            workspace=workspace,
            observations=observations,
            inspection_id=inspection_id,
            service_identity=self._service_identity,
            started_at=started_at,
            completed_at=completed_at,
        )
        return await self._records.append_report(report)


def _workspace(
    candidate: PersistedExternalCandidate,
    *,
    inspection_id: str,
    nonce: str,
    allocated_at: datetime,
    bounds: InspectionBounds,
    allow_mcp_tools_list_probe: bool,
) -> ExternalCandidateInspectionWorkspace:
    locator = urlsplit(candidate.candidate.locator)
    network_hosts = (
        frozenset({locator.hostname.casefold()})
        if locator.scheme == "https" and locator.hostname
        else frozenset()
    )
    probe_mode = (
        InspectionProbeMode.STATIC_AND_MCP_TOOLS_LIST
        if allow_mcp_tools_list_probe and candidate.candidate.source.value == "mcp_registry"
        else InspectionProbeMode.STATIC_ONLY
    )
    values = {
        "workspace_id": f"inspection-workspace:{nonce}",
        "candidate_id": candidate.candidate.candidate_id,
        "candidate_record_id": candidate.candidate_record_id,
        "candidate_content_digest": candidate.content_digest,
        "root_ref": f"quarantine://inspection/{nonce}",
        "allocated_at": allocated_at,
        "expires_at": allocated_at + timedelta(seconds=bounds.workspace_ttl_seconds),
        "bounds": bounds,
        "network_host_allowlist": network_hosts,
        "probe_mode": probe_mode,
        "inputs_read_only": True,
        "candidate_bundle_mounted": False,
        "agent_environment_mounted": False,
        "install_allowed": False,
        "execute_candidate_allowed": False,
    }
    return ExternalCandidateInspectionWorkspace(
        **values,
        content_digest=sha256_digest(values),
    )


def _workspace_content_digest(
    workspace: ExternalCandidateInspectionWorkspace,
) -> str:
    return sha256_digest(workspace.model_dump(exclude={"content_digest"}))


def _enforce_bounds(
    observations: QuarantineInspectionObservations,
    workspace: ExternalCandidateInspectionWorkspace,
) -> QuarantineInspectionObservations:
    violations: list[str] = []
    bounds = workspace.bounds
    if observations.downloaded_bytes > bounds.max_download_bytes:
        violations.append("download bytes")
    if observations.files_inspected > bounds.max_files:
        violations.append("file count")
    if observations.largest_file_bytes > bounds.max_file_bytes:
        violations.append("file size")
    if observations.network_requests > bounds.max_network_requests:
        violations.append("network requests")
    if observations.report_size_bytes > bounds.max_report_bytes:
        violations.append("report size")
    if len(observations.model_dump_json().encode()) > bounds.max_report_bytes:
        violations.append("serialized report size")
    if len(observations.findings) > bounds.max_findings:
        violations.append("finding count")
    if not observations.network_hosts_contacted.issubset(workspace.network_host_allowlist):
        violations.append("network host allowlist")
    if (
        observations.tools_list_probe_used
        and workspace.probe_mode != InspectionProbeMode.STATIC_AND_MCP_TOOLS_LIST
    ):
        violations.append("tools/list authority")
    if not violations:
        return observations
    finding = InspectionFinding(
        code="INSPECTION_BOUND_EXCEEDED",
        severity=InspectionFindingSeverity.ERROR,
        summary=(
            "The quarantined inspection exceeded configured bounds: "
            + ", ".join(sorted(violations))
            + "."
        ),
    )
    retained = observations.findings[: max(0, bounds.max_findings - 1)]
    return observations.model_copy(update={"findings": (*retained, finding)})


def _report(
    *,
    principal: InspectionPrincipal,
    request: ExternalCandidateInspectionRequest,
    candidate: PersistedExternalCandidate,
    workspace: ExternalCandidateInspectionWorkspace,
    observations: QuarantineInspectionObservations,
    inspection_id: str,
    service_identity: str,
    started_at: datetime,
    completed_at: datetime,
) -> ExternalCandidateInspectionReport:
    blocking = _promotion_blocking_findings(observations)
    passed = observations.manifest_valid and not blocking
    ready = (
        passed
        and observations.provenance_verified
        and observations.immutable_content_digest is not None
        and bool(observations.license_evidence)
    )
    promotion = CandidatePromotionInspectionRequest(
        candidate_id=candidate.candidate.candidate_id,
        candidate_record_id=candidate.candidate_record_id,
        inspection_id=inspection_id,
        readiness=(
            PromotionReadiness.READY_FOR_HUMAN_REVIEW if ready else PromotionReadiness.NOT_READY
        ),
        evidence_refs=(
            candidate.candidate.raw_response_ref or candidate.evidence_id,
            f"inspection-report:{inspection_id}",
        ),
        blocking_findings=blocking,
    )
    values = {
        "inspection_id": inspection_id,
        "candidate_id": candidate.candidate.candidate_id,
        "candidate_record_id": candidate.candidate_record_id,
        "candidate_content_digest": candidate.content_digest,
        "workspace_id": workspace.workspace_id,
        "evidence_id": candidate.evidence_id,
        "requester_actor_id": principal.actor_id,
        "tenant_scope": principal.tenant_scope,
        "service_identity": service_identity,
        "correlation_id": request.correlation_id,
        "requested_at": request.requested_at,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": InspectionStatus.PASSED if passed else InspectionStatus.FAILED,
        "observation_digest": sha256_digest(observations),
        "manifest_valid": observations.manifest_valid,
        "provenance_verified": observations.provenance_verified,
        "immutable_content_digest": observations.immutable_content_digest,
        "license_evidence": observations.license_evidence,
        "secret_requirement_names": observations.secret_requirement_names,
        "network_requirement_hosts": observations.network_requirement_hosts,
        "network_hosts_contacted": observations.network_hosts_contacted,
        "requested_capabilities": observations.requested_capabilities,
        "tools": observations.tools,
        "findings": observations.findings,
        "promotion_request": promotion,
        "content_classification": "sanitized_untrusted_inspection_report",
    }
    return ExternalCandidateInspectionReport(
        **values,
        report_digest=sha256_digest(values),
    )


def _promotion_blocking_findings(
    observations: QuarantineInspectionObservations,
) -> tuple[str, ...]:
    gate_blockers: list[str] = []
    if not observations.manifest_valid:
        gate_blockers.append("MANIFEST_INVALID_OR_UNAVAILABLE")
    if not observations.provenance_verified:
        gate_blockers.append("PROVENANCE_UNVERIFIED")
    if observations.immutable_content_digest is None:
        gate_blockers.append("IMMUTABLE_CONTENT_UNAVAILABLE")
    if not observations.license_evidence:
        gate_blockers.append("LICENSE_EVIDENCE_MISSING")

    error_blockers = (
        finding.code
        for finding in observations.findings
        if finding.severity == InspectionFindingSeverity.ERROR
    )
    return tuple(dict.fromkeys((*gate_blockers, *error_blockers)))[:MAX_PROMOTION_BLOCKING_FINDINGS]


def _report_content_digest(report: ExternalCandidateInspectionReport) -> str:
    return sha256_digest(report.model_dump(exclude={"report_digest"}))


def _failed_observations(
    code: str,
    summary: str,
) -> QuarantineInspectionObservations:
    return QuarantineInspectionObservations(
        manifest_valid=False,
        provenance_verified=False,
        findings=(
            InspectionFinding(
                code=code,
                severity=InspectionFindingSeverity.ERROR,
                summary=summary,
            ),
        ),
    )


def _append_immutable[T](
    records: dict[str, T],
    identity: str,
    value: T,
    *,
    subject: str,
) -> None:
    prior = records.get(identity)
    if prior is None:
        records[identity] = value
    elif prior != value:
        raise InspectionPersistenceError(f"immutable {subject} identity conflict")


def _workspace_from_document(
    document: ExternalCandidateInspectionWorkspaceDocument,
) -> ExternalCandidateInspectionWorkspace:
    workspace = ExternalCandidateInspectionWorkspace.model_validate(document.payload)
    if workspace.content_digest != document.content_digest:
        raise InspectionPersistenceError("inspection workspace digest mismatch")
    return workspace


def _report_from_document(
    document: ExternalCandidateInspectionReportDocument,
) -> ExternalCandidateInspectionReport:
    report = ExternalCandidateInspectionReport.model_validate(document.payload)
    if report.report_digest != document.report_digest:
        raise InspectionPersistenceError("inspection report digest mismatch")
    return report
