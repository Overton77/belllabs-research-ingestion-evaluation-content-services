from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from string import Formatter
from typing import Protocol, TypeVar
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.application.capability_search import (
    CapabilitySearchResponse,
    CapabilitySearchService,
    MCPToolSearchGroup,
    TokenUseMeasurement,
    search_token_use,
    token_measurement,
)
from app.application.capability_search_repository import CatalogSearchRepository
from app.application.control_plane_repository import DefinitionRepository
from app.application.coordinator_launch import (
    CoordinatorLaunchPreparationService,
    CoordinatorWorkflowLaunchService,
)
from app.application.coordinator_results import CoordinatorResultService
from app.application.external_candidate_inspection import (
    ExternalCandidateInspectionReport,
    ExternalCandidateInspectionRequest,
    ExternalCandidateInspectionService,
    InspectionAuthorizationError,
    InspectionPrincipal,
)
from app.application.external_candidate_repository import ExternalCandidateNotFound
from app.application.external_capability_discovery import (
    ExternalCapabilityDiscoveryDisabled,
    ExternalCapabilityDiscoveryService,
)
from app.application.orchestration_binding_repository import (
    SemanticInputBindingNotFound,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    DefinitionSelector,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PromptDefinition,
    PublishedDefinition,
    SkillDefinition,
    WorkflowTypeDefinition,
)
from app.domain.control_plane.errors import (
    CompilationRejected,
    DefinitionConflict,
    DefinitionNotFound,
    ReferenceMismatch,
    RetiredDefinition,
)
from app.domain.coordinator.contracts import (
    CapabilitySearchHit,
    CapabilitySearchRequest,
    WorkflowDesignDraft,
)
from app.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from app.domain.coordinator.launch import (
    BlueprintFamily,
    LaunchAuthorizationError,
    LaunchIdempotencyConflict,
    LaunchRequestContext,
    LaunchTicketNotFound,
    LaunchTicketUnavailable,
    PublicPreparedLaunchTicket,
    WorkflowLaunchHandle,
    WorkflowLaunchProposal,
    WorkflowResultView,
)

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
_SAFE_TEMPLATE_VARIABLE = re.compile(r"^[a-z][a-z0-9_]*$")
COORDINATOR_CORRELATION_ID: ContextVar[str | None] = ContextVar(
    "coordinator_correlation_id",
    default=None,
)


class FacadeContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CoordinatorPrincipalLike(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def tenant_scope(self) -> str: ...

    @property
    def request_scope(self) -> str: ...

    @property
    def roles(self) -> frozenset[str]: ...

    @property
    def permissions(self) -> frozenset[str]: ...


class BlueprintRuntimeStatus(FacadeContract):
    family: BlueprintFamily
    executable: bool
    reason: str = Field(min_length=1)
    evidence_ref: str | None = Field(default=None, min_length=1)


class CoordinatorBootstrap(FacadeContract):
    schema_version: str = "1"
    supported_blueprint_families: tuple[BlueprintFamily, ...]
    executable_blueprint_families: tuple[BlueprintFamily, ...]
    runtime_status: tuple[BlueprintRuntimeStatus, ...]
    resource_templates: tuple[str, ...]
    root_tools: tuple[str, ...]
    prompts: tuple[str, ...]
    recommended_tool_sequence: tuple[str, ...]
    coordinator_skill_ref: ExactDefinitionRef


class CapabilityDetail(FacadeContract):
    exact_ref: ExactDefinitionRef
    lifecycle_status: str
    published_at: datetime
    definition: dict[str, object]
    token_use: tuple[TokenUseMeasurement, ...]


class WorkflowDesignValidation(FacadeContract):
    draft_id: str
    structurally_valid: bool
    requires_publication: bool
    launchable: bool
    resolved_asset_refs: tuple[ExactDefinitionRef, ...]
    missing_asset_refs: tuple[ExactDefinitionRef, ...]
    candidate_ids_requiring_promotion: tuple[str, ...]
    findings: tuple[str, ...]


class CoordinatorFeatureFlags(FacadeContract):
    capability_search_enabled: bool
    external_discovery_enabled: bool
    coordinator_launch_enabled: bool


class EffectiveCoordinatorSurface(FacadeContract):
    """Provider-backed MCP surface fixed when the application is composed."""

    tools: tuple[str, ...]
    resource_templates: tuple[str, ...]
    prompts: tuple[str, ...]


class CoordinatorLimits(FacadeContract):
    request_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    max_request_bytes: int = Field(default=131_072, ge=1_024, le=1_000_000)
    max_response_bytes: int = Field(default=1_000_000, ge=1_024, le=4_000_000)
    max_prompt_argument_bytes: int = Field(default=32_768, ge=256, le=250_000)
    max_concurrency: int = Field(default=16, ge=1, le=256)
    requests_per_minute: int = Field(default=120, ge=1, le=10_000)


class CoordinatorAuditEvent(FacadeContract):
    event_id: str
    occurred_at: datetime
    operation: str
    actor_id: str
    tenant_scope: str
    outcome: str
    correlation_id: str
    request_digest: str
    response_digest: str | None = None
    error_code: str | None = None


class CoordinatorAuditSink(Protocol):
    async def emit(self, event: CoordinatorAuditEvent) -> None: ...


class RuntimeReadinessPort(Protocol):
    async def snapshot(self) -> tuple[BlueprintRuntimeStatus, ...]: ...


class CatalogAuthorizationPort(Protocol):
    async def allowed_kinds(
        self,
        principal: CoordinatorPrincipalLike,
        requested: frozenset[DefinitionKind],
    ) -> frozenset[DefinitionKind]: ...

    async def can_read(
        self,
        principal: CoordinatorPrincipalLike,
        published: PublishedDefinition,
    ) -> bool: ...


class LaunchContextProvider(Protocol):
    async def current(
        self,
        principal: CoordinatorPrincipalLike,
        request_scope: str,
    ) -> LaunchRequestContext: ...


class CatalogPayloadReader(Protocol):
    async def read_text(self, uri: str, digest: str, max_bytes: int) -> str: ...


class InspectionReportReader(Protocol):
    async def get_report(
        self,
        inspection_id: str,
    ) -> ExternalCandidateInspectionReport: ...


class RunResourceReader(Protocol):
    async def launch(
        self,
        request_scope: str,
        run_id: str,
    ) -> object: ...

    async def bindings(
        self,
        request_scope: str,
        run_id: str,
    ) -> object: ...


class LoggingCoordinatorAuditSink:
    """Production-safe sink: only identities and digests, never request bodies."""

    async def emit(self, event: CoordinatorAuditEvent) -> None:
        LOGGER.info("coordinator_audit %s", event.model_dump_json())


class PermissionCatalogAuthorization:
    """Default global-catalog policy with optional per-kind scope narrowing."""

    async def allowed_kinds(
        self,
        principal: CoordinatorPrincipalLike,
        requested: frozenset[DefinitionKind],
    ) -> frozenset[DefinitionKind]:
        if "catalog.read" not in principal.permissions:
            return frozenset()
        kind_scopes = {
            permission.removeprefix("catalog.read.")
            for permission in principal.permissions
            if permission.startswith("catalog.read.")
        }
        universe = frozenset(DefinitionKind)
        allowed = (
            universe
            if not kind_scopes or "all" in kind_scopes
            else frozenset(
                kind for kind in universe if kind.value in kind_scopes
            )
        )
        return allowed & requested if requested else allowed

    async def can_read(
        self,
        principal: CoordinatorPrincipalLike,
        published: PublishedDefinition,
    ) -> bool:
        allowed = await self.allowed_kinds(
            principal,
            frozenset({published.ref.kind}),
        )
        return published.ref.kind in allowed


class InMemoryCoordinatorAuditSink:
    def __init__(self) -> None:
        self.events: list[CoordinatorAuditEvent] = []

    async def emit(self, event: CoordinatorAuditEvent) -> None:
        self.events.append(event)


class CoordinatorRateLimiter:
    """Bounded in-process guard; a shared gateway limiter remains deployment authority."""

    def __init__(self, requests_per_minute: int) -> None:
        self._limit = requests_per_minute
        self._entries: dict[str, deque[datetime]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def acquire(self, actor_id: str, now: datetime) -> None:
        cutoff = now - timedelta(minutes=1)
        async with self._lock:
            entries = self._entries[actor_id]
            while entries and entries[0] <= cutoff:
                entries.popleft()
            if len(entries) >= self._limit:
                raise CoordinatorDomainError(
                    CoordinatorErrorCode.RATE_LIMITED,
                    "coordinator request rate limit exceeded",
                    retryable=True,
                )
            entries.append(now)


class ProductionCoordinatorFacade:
    """Application-owned coordinator surface shared by MCP, HTTP, and tests."""

    RESOURCE_TEMPLATES = (
        "belllabs://workflow-types/{logical_id}/{revision}/contract",
        "belllabs://workflow-types/{logical_id}/{revision}/input-schema",
        "belllabs://workflow-types/{logical_id}/{revision}/output-contracts",
        "belllabs://catalog/{kind}/{logical_id}/{revision}",
        "belllabs://catalog/{kind}/{logical_id}/{revision}/manifest",
        "belllabs://runs/{run_id}/launch",
        "belllabs://runs/{run_id}/result",
        "belllabs://runs/{run_id}/bindings",
    )
    ROOT_TOOLS = (
        "coordinator_bootstrap",
        "search_capabilities",
        "get_capability",
        "discover_mcp_servers",
        "discover_agent_skills",
        "inspect_external_candidate",
        "validate_workflow_design",
        "prepare_workflow_launch",
        "launch_workflow",
        "get_workflow_result",
    )
    RECOMMENDED_SEQUENCE = (
        "coordinator_bootstrap",
        "search_capabilities:workflow_type",
        "get_capability:exact_workflow_type",
        "search_capabilities:required_assets",
        "discover_only_if_internal_capability_is_missing",
        "validate_workflow_design_if_novel",
        "prepare_workflow_launch",
        "launch_workflow",
        "get_workflow_result",
    )
    _CATALOG_RESOURCE_TEMPLATES = RESOURCE_TEMPLATES[:5]
    _RUN_RESOURCE_TEMPLATES = RESOURCE_TEMPLATES[5:]

    def __init__(
        self,
        *,
        definitions: DefinitionRepository,
        catalog_index: CatalogSearchRepository,
        search: CapabilitySearchService | None,
        readiness: RuntimeReadinessPort,
        coordinator_skill: DefinitionSelector,
        prompt_bindings: Mapping[str, ExactDefinitionRef],
        flags: CoordinatorFeatureFlags,
        audit: CoordinatorAuditSink,
        catalog_authorization: CatalogAuthorizationPort | None = None,
        discovery: ExternalCapabilityDiscoveryService | None = None,
        inspections: ExternalCandidateInspectionService | None = None,
        inspection_reports: InspectionReportReader | None = None,
        preparation: CoordinatorLaunchPreparationService | None = None,
        launcher: CoordinatorWorkflowLaunchService | None = None,
        results: CoordinatorResultService | None = None,
        launch_contexts: LaunchContextProvider | None = None,
        run_resources: RunResourceReader | None = None,
        payloads: CatalogPayloadReader | None = None,
        limits: CoordinatorLimits | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._definitions = definitions
        self._catalog_index = catalog_index
        self._search = search
        self._readiness = readiness
        self._coordinator_skill = coordinator_skill
        self._prompt_bindings = dict(prompt_bindings)
        self._flags = flags
        self._audit = audit
        self._catalog_authorization = (
            catalog_authorization or PermissionCatalogAuthorization()
        )
        self._discovery = discovery
        self._inspections = inspections
        self._inspection_reports = inspection_reports
        self._preparation = preparation
        self._launcher = launcher
        self._results = results
        self._launch_contexts = launch_contexts
        self._run_resources = run_resources
        self._payloads = payloads
        self._limits = limits or CoordinatorLimits()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._semaphore = asyncio.Semaphore(self._limits.max_concurrency)
        self._rate_limiter = CoordinatorRateLimiter(self._limits.requests_per_minute)
        self._effective_surface = self._derive_effective_surface()

    @property
    def effective_surface(self) -> EffectiveCoordinatorSurface:
        return self._effective_surface

    async def bootstrap(self, principal: CoordinatorPrincipalLike) -> object:
        async def operation() -> CoordinatorBootstrap:
            skill_ref = await self._resolve_selector(self._coordinator_skill)
            skill = await self._published(skill_ref)
            await self._require_catalog_read(principal, skill)
            if not isinstance(skill.definition, SkillDefinition):
                raise self._internal("coordinator skill selector resolved to the wrong kind")
            status = await self._readiness.snapshot()
            by_family = {item.family: item for item in status}
            required = (BlueprintFamily.STAGE_GRAPH, BlueprintFamily.GOAL_DIRECTED)
            if set(by_family) != set(required):
                raise self._dependency("runtime readiness snapshot is incomplete")
            ordered = tuple(by_family[family] for family in required)
            return CoordinatorBootstrap(
                supported_blueprint_families=required,
                executable_blueprint_families=tuple(
                    item.family for item in ordered if item.executable
                ),
                runtime_status=ordered,
                resource_templates=self._effective_surface.resource_templates,
                root_tools=self._effective_surface.tools,
                prompts=self._effective_surface.prompts,
                recommended_tool_sequence=tuple(
                    item
                    for item in self.RECOMMENDED_SEQUENCE
                    if (
                        item.split(":", 1)[0] in self._effective_surface.tools
                        or (
                            item == "discover_only_if_internal_capability_is_missing"
                            and "discover_mcp_servers"
                            in self._effective_surface.tools
                        )
                    )
                ),
                coordinator_skill_ref=skill.ref,
            )

        return await self._run("coordinator_bootstrap", principal, {}, operation)

    async def search(
        self,
        principal: CoordinatorPrincipalLike,
        request: dict[str, object],
    ) -> object:
        async def operation() -> CapabilitySearchResponse:
            self._require_permission(principal, "catalog.read")
            if not self._flags.capability_search_enabled or self._search is None:
                raise self._dependency("internal capability search is disabled")
            parsed = CapabilitySearchRequest.model_validate(request)
            if parsed.tenant_scope != principal.tenant_scope:
                raise self._forbidden("catalog search scope differs from authenticated tenant")
            allowed_kinds = await self._catalog_authorization.allowed_kinds(
                principal,
                parsed.kinds,
            )
            if not allowed_kinds:
                raise self._forbidden(
                    "authenticated principal cannot read the requested catalog kinds"
                )
            response = await self._search.search(
                parsed.model_copy(update={"kinds": allowed_kinds})
            )
            visible = []
            for hit in response.hits:
                assert hit.exact_ref is not None
                published = await self._published(hit.exact_ref)
                if await self._catalog_authorization.can_read(principal, published):
                    visible.append(hit)
            visible_hits = tuple(visible)
            groups: dict[ExactDefinitionRef, list[CapabilitySearchHit]] = {}
            for hit in visible_hits:
                if hit.parent_ref is not None:
                    groups.setdefault(hit.parent_ref, []).append(hit)
            return CapabilitySearchResponse(
                hits=visible_hits,
                tool_groups=tuple(
                    MCPToolSearchGroup(parent_ref=parent, tools=tuple(tools))
                    for parent, tools in sorted(
                        groups.items(),
                        key=lambda item: (
                            item[0].logical_id,
                            item[0].revision,
                            item[0].digest,
                        ),
                    )
                ),
                token_use=search_token_use(parsed.query, visible_hits),
            )

        return await self._run("search_capabilities", principal, request, operation)

    async def get_capability(
        self,
        principal: CoordinatorPrincipalLike,
        exact_ref: dict[str, object],
    ) -> object:
        async def operation() -> object:
            self._require_permission(principal, "catalog.read")
            if "inspection_id" in exact_ref:
                self._require_permission(principal, "capability.inspect")
                if self._inspection_reports is None:
                    raise self._dependency("candidate inspection records are unavailable")
                report = await self._inspection_reports.get_report(
                    str(exact_ref["inspection_id"])
                )
                if report.tenant_scope != principal.tenant_scope:
                    raise self._not_found("candidate inspection was not found")
                return report
            ref = ExactDefinitionRef.model_validate(exact_ref)
            published = await self._published(ref)
            await self._require_catalog_read(principal, published)
            definition_payload = published.definition.model_dump(mode="json")
            return CapabilityDetail(
                exact_ref=published.ref,
                lifecycle_status="published",
                published_at=published.published_at,
                definition=definition_payload,
                token_use=_capability_token_use(
                    published.definition,
                    definition_payload,
                ),
            )

        return await self._run("get_capability", principal, exact_ref, operation)

    async def discover_mcp_servers(
        self,
        principal: CoordinatorPrincipalLike,
        query: str,
    ) -> object:
        async def operation() -> object:
            self._require_permission(principal, "capability.discover")
            service = self._required_discovery()
            return await service.discover_mcp_servers(query)

        return await self._run(
            "discover_mcp_servers",
            principal,
            {"query": query},
            operation,
        )

    async def discover_agent_skills(
        self,
        principal: CoordinatorPrincipalLike,
        query: str,
    ) -> object:
        async def operation() -> object:
            self._require_permission(principal, "capability.discover")
            service = self._required_discovery()
            return await service.discover_agent_skills(query)

        return await self._run(
            "discover_agent_skills",
            principal,
            {"query": query},
            operation,
        )

    async def inspect_external_candidate(
        self,
        principal: CoordinatorPrincipalLike,
        candidate_id: str,
    ) -> object:
        async def operation() -> object:
            self._require_permission(principal, "capability.inspect")
            if not self._flags.external_discovery_enabled or self._inspections is None:
                raise self._dependency("external candidate inspection is disabled")
            request = ExternalCandidateInspectionRequest(
                candidate_id=candidate_id,
                correlation_id=str(uuid4()),
                requested_at=self._clock(),
            )
            return await self._inspections.inspect(
                InspectionPrincipal(
                    actor_id=principal.actor_id,
                    tenant_scope=principal.tenant_scope,
                    roles=principal.roles,
                ),
                request,
            )

        return await self._run(
            "inspect_external_candidate",
            principal,
            {"candidate_id": candidate_id},
            operation,
        )

    async def validate_workflow_design(
        self,
        principal: CoordinatorPrincipalLike,
        draft: dict[str, object],
    ) -> object:
        async def operation() -> WorkflowDesignValidation:
            self._require_permission(principal, "workflow.design.validate")
            parsed = WorkflowDesignDraft.model_validate(draft)
            resolved: list[ExactDefinitionRef] = []
            missing: list[ExactDefinitionRef] = []
            candidates: list[str] = []
            findings = list(parsed.validation_findings)
            for requested in parsed.requested_assets:
                if requested.candidate_id is not None:
                    candidates.append(requested.candidate_id)
                    findings.append(
                        f"candidate {requested.candidate_id} requires inspection and publication"
                    )
                    continue
                assert requested.exact_ref is not None
                try:
                    await self._published(requested.exact_ref)
                except CoordinatorDomainError as error:
                    if error.code != CoordinatorErrorCode.NOT_FOUND:
                        raise
                    missing.append(requested.exact_ref)
                    findings.append(
                        "missing exact catalog asset "
                        f"{requested.exact_ref.kind.value}:"
                        f"{requested.exact_ref.logical_id}@"
                        f"{requested.exact_ref.revision}"
                    )
                else:
                    resolved.append(requested.exact_ref)
            requires_publication = True
            findings.append(
                "validated designs are drafts and require authorized control-plane publication"
            )
            return WorkflowDesignValidation(
                draft_id=parsed.draft_id,
                structurally_valid=not missing,
                requires_publication=requires_publication,
                launchable=False,
                resolved_asset_refs=tuple(resolved),
                missing_asset_refs=tuple(missing),
                candidate_ids_requiring_promotion=tuple(candidates),
                findings=tuple(findings),
            )

        return await self._run("validate_workflow_design", principal, draft, operation)

    async def prepare_workflow_launch(
        self,
        principal: CoordinatorPrincipalLike,
        proposal: dict[str, object],
    ) -> object:
        async def operation() -> PublicPreparedLaunchTicket:
            self._require_permission(principal, "workflow.prepare")
            preparation, contexts = self._required_preparation()
            parsed = WorkflowLaunchProposal.model_validate(proposal)
            self._enforce_launch_identity(principal, parsed)
            context = await contexts.current(principal, parsed.request_scope)
            return await preparation.prepare(parsed, context)

        return await self._run(
            "prepare_workflow_launch",
            principal,
            proposal,
            operation,
        )

    async def launch_workflow(
        self,
        principal: CoordinatorPrincipalLike,
        ticket_id: str,
        idempotency_issuer: str,
        idempotency_key: str,
    ) -> object:
        async def operation() -> WorkflowLaunchHandle:
            self._require_permission(principal, "workflow.launch")
            if idempotency_issuer != principal.actor_id:
                raise self._forbidden(
                    "launch idempotency issuer differs from authenticated actor"
                )
            if not idempotency_key.strip() or len(idempotency_key) > 256:
                raise self._invalid("launch idempotency key is invalid")
            launcher, contexts = self._required_launcher()
            context = await contexts.current(
                principal,
                _principal_request_scope(principal),
            )
            return await launcher.launch(ticket_id, context)

        return await self._run(
            "launch_workflow",
            principal,
            {
                "ticket_id": ticket_id,
                "idempotency_issuer": idempotency_issuer,
                "idempotency_key": idempotency_key,
            },
            operation,
        )

    async def get_workflow_result(
        self,
        principal: CoordinatorPrincipalLike,
        run_id: str,
    ) -> object:
        async def operation() -> WorkflowResultView:
            self._require_permission(principal, "workflow.result.read")
            if self._results is None or self._launch_contexts is None:
                raise self._dependency("workflow result retrieval is unavailable")
            context = await self._launch_contexts.current(
                principal,
                _principal_request_scope(principal),
            )
            return await self._results.get_workflow_result(run_id, context)

        return await self._run(
            "get_workflow_result",
            principal,
            {"run_id": run_id},
            operation,
        )

    async def resource(
        self,
        principal: CoordinatorPrincipalLike,
        uri: str,
    ) -> str | dict[str, object]:
        async def operation() -> dict[str, object]:
            self._require_permission(principal, "catalog.read")
            return await self._read_resource(principal, uri)

        value = await self._run("read_resource", principal, {"uri": uri}, operation)
        assert isinstance(value, dict)
        return value

    async def prompt(
        self,
        principal: CoordinatorPrincipalLike,
        name: str,
        arguments: dict[str, str],
    ) -> str:
        async def operation() -> str:
            self._require_permission(principal, "catalog.read")
            return await self._render_prompt(principal, name, arguments)

        value = await self._run(
            "render_prompt",
            principal,
            {"name": name, "arguments": arguments},
            operation,
        )
        assert isinstance(value, str)
        return value

    async def _read_resource(
        self,
        principal: CoordinatorPrincipalLike,
        uri: str,
    ) -> dict[str, object]:
        parsed = urlsplit(uri)
        if parsed.scheme != "belllabs" or parsed.query or parsed.fragment:
            raise self._invalid("resource URI is invalid")
        segments = tuple(unquote(item) for item in parsed.path.strip("/").split("/") if item)
        if parsed.netloc == "workflow-types" and len(segments) == 3:
            logical_id, raw_revision, view = segments
            published = await self._resource_definition(
                principal.tenant_scope,
                DefinitionKind.WORKFLOW_TYPE,
                logical_id,
                self._revision(raw_revision),
            )
            await self._require_catalog_read(principal, published)
            definition = published.definition
            if not isinstance(definition, WorkflowTypeDefinition):
                raise self._internal("Workflow Type resource resolved to the wrong kind")
            if view == "contract":
                return {
                    "exact_ref": published.ref.model_dump(mode="json"),
                    "purpose": definition.purpose,
                    "non_goals": sorted(definition.non_goals),
                    "invariants": sorted(definition.invariants),
                    "obligations": sorted(definition.obligations),
                    "authority_ceiling": definition.authority_ceiling.model_dump(
                        mode="json"
                    ),
                    "workspace_contract": definition.workspace_contract.model_dump(
                        mode="json"
                    ),
                    "linked_run_slots": [
                        item.model_dump(mode="json")
                        for item in definition.linked_run_slots
                    ],
                }
            if view == "input-schema":
                return {
                    "exact_ref": published.ref.model_dump(mode="json"),
                    "input_admission_contract": definition.input_admission_contract,
                }
            if view == "output-contracts":
                return {
                    "exact_ref": published.ref.model_dump(mode="json"),
                    "output_contracts": sorted(definition.output_contracts),
                }
            raise self._not_found("resource view was not found")
        if parsed.netloc == "catalog" and len(segments) in {3, 4}:
            raw_kind, logical_id, raw_revision, *view_parts = segments
            try:
                kind = DefinitionKind(raw_kind)
            except ValueError as error:
                raise self._invalid("catalog resource kind is invalid") from error
            published = await self._resource_definition(
                principal.tenant_scope,
                kind,
                logical_id,
                self._revision(raw_revision),
            )
            await self._require_catalog_read(principal, published)
            if not view_parts:
                definition_payload = published.definition.model_dump(mode="json")
                return CapabilityDetail(
                    exact_ref=published.ref,
                    lifecycle_status="published",
                    published_at=published.published_at,
                    definition=definition_payload,
                    token_use=_capability_token_use(
                        published.definition,
                        definition_payload,
                    ),
                ).model_dump(mode="json")
            if view_parts == ["manifest"]:
                return self._manifest(published)
            raise self._not_found("resource view was not found")
        if parsed.netloc == "runs" and len(segments) == 2:
            self._require_permission(principal, "workflow.result.read")
            run_id, view = segments
            if view == "result":
                if self._results is None or self._launch_contexts is None:
                    raise self._dependency("workflow result retrieval is unavailable")
                context = await self._launch_contexts.current(
                    principal,
                    _principal_request_scope(principal),
                )
                result = await self._results.get_workflow_result(run_id, context)
                return result.model_dump(mode="json")
            if self._run_resources is None:
                raise self._dependency("Workflow Run resources are unavailable")
            if view == "launch":
                return _as_mapping(
                    await self._run_resources.launch(
                        _principal_request_scope(principal),
                        run_id,
                    )
                )
            if view == "bindings":
                return _as_mapping(
                    await self._run_resources.bindings(
                        _principal_request_scope(principal),
                        run_id,
                    )
                )
            raise self._not_found("resource view was not found")
        raise self._not_found("resource URI was not found")

    async def _render_prompt(
        self,
        principal: CoordinatorPrincipalLike,
        name: str,
        arguments: dict[str, str],
    ) -> str:
        ref = self._prompt_bindings.get(name)
        if ref is None:
            raise self._not_found("prompt view was not found")
        published = await self._published(ref)
        await self._require_catalog_read(principal, published)
        definition = published.definition
        if not isinstance(definition, PromptDefinition):
            raise self._internal("prompt binding resolved to the wrong definition kind")
        encoded_arguments = _encoded(arguments)
        if len(encoded_arguments) > self._limits.max_prompt_argument_bytes:
            raise self._invalid("prompt arguments exceed the configured size limit")
        expected = {variable.name for variable in definition.variables}
        supplied = set(arguments)
        required = {variable.name for variable in definition.variables if variable.required}
        if missing := required - supplied:
            raise self._invalid(
                "prompt is missing required variables: " + ", ".join(sorted(missing))
            )
        if unknown := supplied - expected:
            raise self._invalid(
                "prompt received unknown variables: " + ", ".join(sorted(unknown))
            )
        if any(not _SAFE_TEMPLATE_VARIABLE.fullmatch(name) for name in supplied):
            raise self._invalid("prompt variable name is invalid")
        body = definition.body
        if body is None:
            if definition.payload_ref is None or self._payloads is None:
                raise self._dependency("immutable prompt payload is unavailable")
            body = await self._payloads.read_text(
                definition.payload_ref.uri,
                definition.payload_ref.digest,
                self._limits.max_response_bytes,
            )
        rendered = _safe_render(definition, body, arguments)
        view = {
            "prompt_ref": published.ref.model_dump(mode="json"),
            "rendered_digest": sha256_digest(rendered),
            "content": rendered,
        }
        return json.dumps(view, sort_keys=True, separators=(",", ":"))

    async def _resource_definition(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
        logical_id: str,
        revision: int,
    ) -> PublishedDefinition:
        document = await self._catalog_index.get(
            tenant_scope,
            kind,
            logical_id,
            revision,
        )
        if document is None and tenant_scope != "global":
            document = await self._catalog_index.get(
                "global",
                kind,
                logical_id,
                revision,
            )
        if document is None:
            raise CoordinatorDomainError(
                CoordinatorErrorCode.PROJECTION_STALE,
                "resource identity is absent from the capability projection",
                retryable=True,
            )
        return await self._published(document.exact_ref)

    async def _published(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        try:
            published = await self._definitions.get(ref)
        except DefinitionNotFound as error:
            raise self._not_found("exact catalog capability was not found") from error
        except ReferenceMismatch as error:
            raise CoordinatorDomainError(
                CoordinatorErrorCode.PROJECTION_STALE,
                "exact catalog capability digest changed",
                retryable=True,
            ) from error
        if published.retired_at is not None:
            raise CoordinatorDomainError(
                CoordinatorErrorCode.CAPABILITY_NOT_SELECTABLE,
                "exact catalog capability is retired",
            )
        return published

    async def _require_catalog_read(
        self,
        principal: CoordinatorPrincipalLike,
        published: PublishedDefinition,
    ) -> None:
        if not await self._catalog_authorization.can_read(principal, published):
            # Avoid confirming an exact hidden identity to an unauthorized caller.
            raise self._not_found("exact catalog capability was not found")

    async def _resolve_selector(self, selector: DefinitionSelector) -> ExactDefinitionRef:
        if selector.exact is not None:
            return selector.exact
        assert selector.alias is not None
        try:
            return (await self._definitions.resolve(selector.alias)).target
        except (DefinitionNotFound, RetiredDefinition) as error:
            raise self._not_found("coordinator skill selector did not resolve") from error

    def _required_discovery(self) -> ExternalCapabilityDiscoveryService:
        if not self._flags.external_discovery_enabled or self._discovery is None:
            raise self._dependency("external capability discovery is disabled")
        return self._discovery

    def _required_preparation(
        self,
    ) -> tuple[CoordinatorLaunchPreparationService, LaunchContextProvider]:
        if not self._flags.coordinator_launch_enabled or self._launch_contexts is None:
            raise self._dependency("coordinator launch is disabled")
        if self._preparation is None:
            raise self._dependency("coordinator launch preparation is unavailable")
        return self._preparation, self._launch_contexts

    def _required_launcher(
        self,
    ) -> tuple[CoordinatorWorkflowLaunchService, LaunchContextProvider]:
        if not self._flags.coordinator_launch_enabled or self._launch_contexts is None:
            raise self._dependency("coordinator launch is disabled")
        if self._launcher is None:
            raise self._dependency("coordinator workflow launcher is unavailable")
        return self._launcher, self._launch_contexts

    def _derive_effective_surface(self) -> EffectiveCoordinatorSurface:
        if self._flags.capability_search_enabled and self._search is None:
            raise ValueError(
                "capability search is enabled without a composed search provider"
            )
        if self._flags.external_discovery_enabled and self._discovery is None:
            raise ValueError(
                "external discovery is enabled without a composed discovery provider"
            )
        if self._flags.coordinator_launch_enabled:
            required_launch = {
                "preparation": self._preparation,
                "launcher": self._launcher,
                "results": self._results,
                "launch_contexts": self._launch_contexts,
                "run_resources": self._run_resources,
            }
            missing = tuple(
                name for name, provider in required_launch.items() if provider is None
            )
            if missing:
                raise ValueError(
                    "coordinator launch is enabled without providers: "
                    + ", ".join(missing)
                )

        tools = ["coordinator_bootstrap", "get_capability", "validate_workflow_design"]
        if self._flags.capability_search_enabled:
            tools.insert(1, "search_capabilities")
        if self._flags.external_discovery_enabled:
            tools.extend(("discover_mcp_servers", "discover_agent_skills"))
        if self._inspections is not None:
            tools.append("inspect_external_candidate")
        if self._flags.coordinator_launch_enabled:
            tools.extend(
                (
                    "prepare_workflow_launch",
                    "launch_workflow",
                    "get_workflow_result",
                )
            )

        resources = list(self._CATALOG_RESOURCE_TEMPLATES)
        if self._results is not None and self._launch_contexts is not None:
            resources.append("belllabs://runs/{run_id}/result")
        if self._run_resources is not None:
            resources.extend(
                (
                    "belllabs://runs/{run_id}/launch",
                    "belllabs://runs/{run_id}/bindings",
                )
            )
        return EffectiveCoordinatorSurface(
            tools=tuple(tools),
            resource_templates=tuple(resources),
            prompts=tuple(sorted(self._prompt_bindings)),
        )

    @staticmethod
    def _enforce_launch_identity(
        principal: CoordinatorPrincipalLike,
        proposal: WorkflowLaunchProposal,
    ) -> None:
        if proposal.tenant_scope != principal.tenant_scope:
            raise ProductionCoordinatorFacade._forbidden(
                "launch tenant differs from authenticated tenant"
            )
        if proposal.request_scope != _principal_request_scope(principal):
            raise ProductionCoordinatorFacade._forbidden(
                "launch request scope differs from authenticated request scope"
            )
        if proposal.admission.actor.actor_id != principal.actor_id:
            raise ProductionCoordinatorFacade._forbidden(
                "launch actor differs from authenticated actor"
            )
        if proposal.idempotency_issuer != principal.actor_id:
            raise ProductionCoordinatorFacade._forbidden(
                "launch idempotency issuer differs from authenticated actor"
            )

    async def _run(
        self,
        operation_name: str,
        principal: CoordinatorPrincipalLike,
        request: object,
        operation: Callable[[], Awaitable[T]],
    ) -> T:
        correlation_id = COORDINATOR_CORRELATION_ID.get() or str(uuid4())
        request_bytes = _encoded(request)
        request_digest = sha256_digest(request)
        if len(request_bytes) > self._limits.max_request_bytes:
            error = self._invalid("coordinator request exceeds the configured size limit")
            await self._audit_failure(
                operation_name,
                principal,
                correlation_id,
                request_digest,
                error,
            )
            raise error
        try:
            await self._rate_limiter.acquire(principal.actor_id, self._clock())
            async with self._semaphore, asyncio.timeout(
                self._limits.request_timeout_seconds
            ):
                response = await operation()
            response_bytes = _encoded(response)
            if len(response_bytes) > self._limits.max_response_bytes:
                raise self._dependency(
                    "coordinator response exceeds the configured size limit"
                )
        except CoordinatorDomainError as error:
            await self._audit_failure(
                operation_name,
                principal,
                correlation_id,
                request_digest,
                error,
            )
            raise
        except ValidationError as error:
            translated = self._invalid("coordinator request failed schema validation")
            await self._audit_failure(
                operation_name,
                principal,
                correlation_id,
                request_digest,
                translated,
            )
            raise translated from error
        except TimeoutError as error:
            translated = self._dependency("coordinator request timed out", retryable=True)
            await self._audit_failure(
                operation_name,
                principal,
                correlation_id,
                request_digest,
                translated,
            )
            raise translated from error
        except Exception as error:
            translated = _translate_dependency_error(error)
            await self._audit_failure(
                operation_name,
                principal,
                correlation_id,
                request_digest,
                translated,
            )
            if translated.code == CoordinatorErrorCode.INTERNAL_ERROR:
                LOGGER.error(
                    "coordinator operation failed correlation_id=%s operation=%s "
                    "error_type=%s",
                    correlation_id,
                    operation_name,
                    type(error).__name__,
                )
            raise translated from error
        await self._audit.emit(
            CoordinatorAuditEvent(
                event_id=str(uuid4()),
                occurred_at=self._clock(),
                operation=operation_name,
                actor_id=principal.actor_id,
                tenant_scope=principal.tenant_scope,
                outcome="succeeded",
                correlation_id=correlation_id,
                request_digest=request_digest,
                response_digest=sha256_digest(response),
            )
        )
        return response

    async def _audit_failure(
        self,
        operation: str,
        principal: CoordinatorPrincipalLike,
        correlation_id: str,
        request_digest: str,
        error: CoordinatorDomainError,
    ) -> None:
        await self._audit.emit(
            CoordinatorAuditEvent(
                event_id=str(uuid4()),
                occurred_at=self._clock(),
                operation=operation,
                actor_id=principal.actor_id,
                tenant_scope=principal.tenant_scope,
                outcome="failed",
                correlation_id=correlation_id,
                request_digest=request_digest,
                error_code=error.code.value,
            )
        )

    @staticmethod
    def _require_permission(
        principal: CoordinatorPrincipalLike,
        permission: str,
    ) -> None:
        if permission not in principal.permissions:
            raise ProductionCoordinatorFacade._forbidden(
                f"authenticated principal lacks {permission}"
            )

    @staticmethod
    def _manifest(published: PublishedDefinition) -> dict[str, object]:
        definition = published.definition
        base: dict[str, object] = {
            "exact_ref": published.ref.model_dump(mode="json"),
        }
        if isinstance(definition, SkillDefinition):
            return base | {
                "bundle_ref": definition.bundle_ref.model_dump(mode="json"),
                "manifest_digest": definition.manifest_digest,
                "file_manifest": [
                    item.model_dump(mode="json") for item in definition.file_manifest
                ],
            }
        if isinstance(definition, MCPServerDefinition):
            return base | {
                "schema_snapshot_ref": definition.schema_snapshot_ref.model_dump(
                    mode="json"
                ),
                "schema_digest": definition.schema_digest,
                "allowed_tools": sorted(definition.allowed_tools),
            }
        if isinstance(definition, MCPToolDefinition):
            return base | {
                "server_ref": definition.server_ref.model_dump(mode="json"),
                "tool_name": definition.tool_name,
                "input_schema": definition.input_schema,
                "output_schema": definition.output_schema,
                "schema_digest": definition.schema_digest,
            }
        if isinstance(definition, PromptDefinition):
            return base | {
                "payload_ref": (
                    definition.payload_ref.model_dump(mode="json")
                    if definition.payload_ref is not None
                    else None
                ),
                "inline_body_digest": (
                    sha256_digest(definition.body)
                    if definition.body is not None
                    else None
                ),
            }
        raise ProductionCoordinatorFacade._not_found(
            "this catalog capability has no manifest resource"
        )

    @staticmethod
    def _revision(value: str) -> int:
        try:
            revision = int(value)
        except ValueError as error:
            raise ProductionCoordinatorFacade._invalid(
                "resource revision is invalid"
            ) from error
        if revision < 1:
            raise ProductionCoordinatorFacade._invalid(
                "resource revision is invalid"
            )
        return revision

    @staticmethod
    def _invalid(message: str) -> CoordinatorDomainError:
        return CoordinatorDomainError(CoordinatorErrorCode.INVALID_ARGUMENT, message)

    @staticmethod
    def _forbidden(message: str) -> CoordinatorDomainError:
        return CoordinatorDomainError(CoordinatorErrorCode.FORBIDDEN, message)

    @staticmethod
    def _not_found(message: str) -> CoordinatorDomainError:
        return CoordinatorDomainError(CoordinatorErrorCode.NOT_FOUND, message)

    @staticmethod
    def _dependency(
        message: str,
        *,
        retryable: bool = False,
    ) -> CoordinatorDomainError:
        return CoordinatorDomainError(
            CoordinatorErrorCode.DEPENDENCY_UNAVAILABLE,
            message,
            retryable=retryable,
        )

    @staticmethod
    def _internal(message: str) -> CoordinatorDomainError:
        LOGGER.error(message)
        return CoordinatorDomainError(
            CoordinatorErrorCode.INTERNAL_ERROR,
            "coordinator service configuration is invalid",
        )


def _safe_render(
    definition: PromptDefinition,
    body: str,
    arguments: Mapping[str, str],
) -> str:
    if definition.template_engine == "none":
        if arguments:
            raise ProductionCoordinatorFacade._invalid(
                "non-templated prompt does not accept arguments"
            )
        return body
    rendered = body
    values = {
        variable.name: arguments.get(variable.name, "")
        for variable in definition.variables
    }
    if definition.template_engine == "format":
        chunks: list[str] = []
        try:
            parsed = Formatter().parse(body)
            for literal, field_name, format_spec, conversion in parsed:
                chunks.append(literal)
                if field_name is None:
                    continue
                if (
                    field_name not in values
                    or format_spec
                    or conversion is not None
                    or not _SAFE_TEMPLATE_VARIABLE.fullmatch(field_name)
                ):
                    raise ProductionCoordinatorFacade._invalid(
                        "prompt format template contains an unsupported field"
                    )
                chunks.append(values[field_name])
        except ValueError as error:
            raise ProductionCoordinatorFacade._invalid(
                "prompt format template is invalid"
            ) from error
        return "".join(chunks)
    if "{%" in body or "{#" in body:
        raise ProductionCoordinatorFacade._invalid(
            "prompt jinja template contains unsupported control syntax"
        )
    for name, value in values.items():
        if definition.template_engine == "jinja2":
            rendered = rendered.replace("{{ " + name + " }}", value)
            rendered = rendered.replace("{{" + name + "}}", value)
    if "{{" in rendered or "}}" in rendered:
        raise ProductionCoordinatorFacade._invalid(
            "prompt jinja template contains an unsupported variable"
        )
    return rendered


def _translate_dependency_error(error: Exception) -> CoordinatorDomainError:
    if isinstance(error, CoordinatorDomainError):
        return error
    if isinstance(
        error,
        (DefinitionNotFound, ExternalCandidateNotFound, SemanticInputBindingNotFound),
    ):
        return ProductionCoordinatorFacade._not_found("requested record was not found")
    if isinstance(error, (DefinitionConflict, ReferenceMismatch)):
        return CoordinatorDomainError(
            CoordinatorErrorCode.CONFLICT,
            "authoritative state changed during the coordinator request",
        )
    if isinstance(error, RetiredDefinition):
        return CoordinatorDomainError(
            CoordinatorErrorCode.CAPABILITY_NOT_SELECTABLE,
            "selected exact capability is retired",
        )
    if isinstance(error, CompilationRejected):
        return CoordinatorDomainError(
            CoordinatorErrorCode.ADMISSION_REJECTED,
            "workflow configuration compilation was rejected",
        )
    if isinstance(error, InspectionAuthorizationError | LaunchAuthorizationError):
        return ProductionCoordinatorFacade._forbidden(
            "authenticated principal is not authorized for this operation"
        )
    if isinstance(error, LaunchTicketNotFound):
        return ProductionCoordinatorFacade._not_found("launch ticket was not found")
    if isinstance(error, LaunchIdempotencyConflict):
        return CoordinatorDomainError(
            CoordinatorErrorCode.IDEMPOTENCY_CONFLICT,
            "launch idempotency identity conflicts with prior state",
        )
    if isinstance(error, LaunchTicketUnavailable):
        message = str(error).casefold()
        if "expired" in message:
            code = CoordinatorErrorCode.LAUNCH_TICKET_EXPIRED
        elif "invalidated" in message:
            code = CoordinatorErrorCode.LAUNCH_TICKET_INVALIDATED
        elif "admission" in message:
            code = CoordinatorErrorCode.ADMISSION_REJECTED
        else:
            code = CoordinatorErrorCode.CONFLICT
        return CoordinatorDomainError(code, "launch ticket is unavailable")
    if isinstance(error, ExternalCapabilityDiscoveryDisabled):
        return ProductionCoordinatorFacade._dependency(
            "external capability discovery is disabled"
        )
    if isinstance(error, (TypeError, ValueError)):
        return ProductionCoordinatorFacade._invalid(
            "coordinator request is invalid"
        )
    return CoordinatorDomainError(
        CoordinatorErrorCode.INTERNAL_ERROR,
        "coordinator operation failed",
    )


def _capability_token_use(
    definition: object,
    definition_payload: dict[str, object],
) -> tuple[TokenUseMeasurement, ...]:
    measurements = [
        token_measurement("catalog_definition", definition_payload),
    ]
    if isinstance(definition, MCPToolDefinition):
        measurements.append(
            token_measurement(
                "tool_schema",
                {
                    "input_schema": definition.input_schema,
                    "output_schema": definition.output_schema,
                    "annotations": definition.annotations,
                },
            )
        )
    return tuple(measurements)


def _encoded(value: object) -> bytes:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


def _json_value(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list | set | frozenset):
        return [_json_value(item) for item in value]
    return value


def _as_mapping(value: object) -> dict[str, object]:
    serialized = _json_value(value)
    if not isinstance(serialized, dict):
        raise ProductionCoordinatorFacade._internal(
            "Workflow Run resource adapter returned a non-object"
        )
    return serialized


def _principal_request_scope(principal: CoordinatorPrincipalLike) -> str:
    return getattr(principal, "request_scope", "") or principal.tenant_scope
