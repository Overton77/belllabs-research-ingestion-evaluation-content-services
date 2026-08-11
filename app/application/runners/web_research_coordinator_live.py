from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Protocol, cast
from uuid import uuid4

from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.application.capability.capability_search import (
    CapabilitySearchResponse,
    CapabilitySearchService,
)
from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.coordinator.coordinator_composition import CoordinatorLaunchProductionInputs
from app.application.coordinator.coordinator_facade import (
    BlueprintRuntimeStatus,
    CapabilityDetail,
    CoordinatorBootstrap,
    CoordinatorFeatureFlags,
    CoordinatorLimits,
    CoordinatorPrincipalLike,
    ProductionCoordinatorFacade,
    WorkflowDesignValidation,
)
from app.application.coordinator.coordinator_launch import UnavailableRuntimePlanPreparer
from app.application.coordinator.coordinator_results import (
    CoordinatorResultService,
    TerminalWorkflowCompletionService,
)
from app.application.coordinator.coordinator_run_resources import CoordinatorRunResourceService
from app.application.web_research.external_candidate_repository import (
    BeanieExternalCandidateRepository,
)
from app.application.web_research.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
    ExternalDiscoveryBatch,
)
from app.application.operations.mongo_operation_execution_repository import (
    create_semantic_operation_binding_repository,
)
from app.application.orchestration.service import (
    F1OrchestrationBindingVerifier,
    RunControlLifecycleGateway,
    StageGraphDecisionService,
    StageGraphLaunchService,
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
    WorkflowLaunchDispatcher,
    orchestration_lifecycle_actor,
)
from app.application.orchestration.orchestration_binding_repository import (
    RunSemanticInputBindingService,
)
from app.application.capability.postgres_capability_search_generation_repository import (
    PostgresProjectionGenerationRepository,
)
from app.application.capability.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.application.coordinator.postgres_coordinator_audit_repository import (
    PostgresCoordinatorAuditSink,
)
from app.application.coordinator.postgres_launch_ticket_repository import (
    PostgresLaunchTicketRepository,
)
from app.application.orchestration.postgres_orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from app.application.run_control.postgres_run_control_repository import (
    PostgresRunControlRepository,
)
from app.application.coordinator.postgres_workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from app.application.run_control.service import (
    REQUIRED_SHARED_BUDGET_DIMENSIONS,
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    RunControlService,
)
from app.application.operations.semantic_operation_bindings import (
    SemanticOperationBindingRepository,
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.application.run_control.web_research_admission import (
    register_web_research_admission_policies,
)
from app.application.web_research.web_research_semantic_binding import (
    REQUIRED_SELECTED_IDENTITIES,
    SemanticServiceWebResearchOperationBindingAuthor,
    WebResearchSemanticBindingProvider,
)
from app.config import PROJECT_ROOT, Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    CompilationContext,
    CompileInvocation,
    DefinitionKind,
    DefinitionSelector,
    EnvironmentAvailability,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    SkillDefinition,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchHit,
    CapabilitySearchRequest,
)
from app.domain.coordinator.launch import (
    AdmissionPreviewDecision,
    BlueprintFamily,
    LaunchRequestContext,
    PublicPreparedLaunchTicket,
    RunAdmissionSpec,
    WorkflowLaunchHandle,
    WorkflowLaunchProposal,
    WorkflowResultView,
)
from app.domain.coordinator.web_research_runtime import WebResearchGoal
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    ImmutableAssetBinding,
    MCPServerBinding,
    ModelPolicy,
    NativeOperationExecutionPlacement,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    WorkspaceContract,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
)
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
)
from app.integrations.artifact_payloads import S3ArtifactPayloadStore
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.control_plane_payloads import UnavailablePayloadStore
from app.integrations.mcp_registry import HttpxMCPRegistryRunner, MCPRegistryAdapter
from app.integrations.mongodb import create_mongodb
from app.integrations.npx_skills_discovery import (
    AsyncioSkillDiscoverySubprocessRunner,
    NpxSkillsDiscoveryAdapter,
)
from app.integrations.postgres import (
    create_application_postgres_pool,
    create_postgres_pool,
)
from app.integrations.temporal import create_temporal_client
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.integrations.web_research_runtime import (
    ArtifactPayloadBrowserScreenshotAdapter,
    BrowserScreenshotArtifactPort,
    attest_reviewed_web_research_runtime,
    build_live_web_research_handler_dependencies,
)
from app.temporal.web_research_smoke import create_web_research_stagegraph_worker
from app.temporal.workflows.stagegraph import StageGraphWorkflow

SEARCH_PLAN: tuple[tuple[str, frozenset[DefinitionKind]], ...] = (
    (
        "public company technology research with cited independent browser verification",
        frozenset({DefinitionKind.WORKFLOW_TYPE}),
    ),
    (
        "read-only public web discovery servers with structured source evidence",
        frozenset({DefinitionKind.MCP_SERVER}),
    ),
    (
        "read-only public web search tools returning source URLs and page evidence",
        frozenset({DefinitionKind.MCP_TOOL}),
    ),
    (
        "reviewed public web research and browser navigation verification procedures",
        frozenset({DefinitionKind.SKILL}),
    ),
    (
        "browser-capable two-source public research agent profile",
        frozenset({DefinitionKind.AGENT_PROFILE}),
    ),
)
EXTERNAL_MCP_DISCOVERY_QUERY = "search"
EXTERNAL_SKILL_DISCOVERY_QUERY = "public web research and browser verification"


class VerifiedS3ScreenshotStore(BrowserScreenshotArtifactPort):
    """Upload through the governed adapter, retrieve, hash-check, and mirror for QA."""

    def __init__(
        self,
        payloads: S3ArtifactPayloadStore,
        *,
        mirror_root: Path,
    ) -> None:
        self._payloads = payloads
        self._delegate = ArtifactPayloadBrowserScreenshotAdapter(payloads)
        self._mirror_root = mirror_root
        self.refs: dict[str, dict[str, object]] = {}

    async def store(
        self,
        *,
        request_scope: str,
        run_id: str,
        idempotency_key: str,
        source_url: str,
        content: bytes,
        media_type: str,
    ) -> str:
        if media_type != "image/png":
            raise ValueError("browser verification artifacts must be PNG")
        digest = f"sha256:{sha256(content).hexdigest()}"
        ref = await self._delegate.store(
            request_scope=request_scope,
            run_id=run_id,
            idempotency_key=idempotency_key,
            source_url=source_url,
            content=content,
            media_type="image/png",
        )
        if not ref.startswith("s3://"):
            raise RuntimeError("authoritative browser screenshot is not stored in S3")
        retrieved = await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=ref,
                content_digest=digest,
                size_bytes=len(content),
            )
        )
        if retrieved != content:
            raise RuntimeError("retrieved S3 browser screenshot differs from uploaded bytes")
        run_root = self._mirror_root / _safe_segment(run_id)
        await asyncio.to_thread(run_root.mkdir, parents=True, exist_ok=True)
        path = run_root / f"{digest.removeprefix('sha256:')}.png"
        if not path.exists():
            await asyncio.to_thread(path.write_bytes, content)
        self.refs[ref] = {
            "content_digest": digest,
            "size_bytes": len(content),
            "retrieval_verified": True,
            "local_qa_mirror": str(path),
        }
        return ref


class ReadOnlyAdmissionPreview:
    """Run executable F1/policy checks without writing a Run Control decision."""

    def __init__(
        self,
        verifier: F1RunConfigurationVerifier,
        policies: AdmissionPolicyRegistry,
    ) -> None:
        self._verifier = verifier
        self._policies = policies

    async def preview(self, request: Any) -> AdmissionPreviewDecision:
        try:
            configuration = await self._verifier.verify(request)
            await self._policies.validate(request, configuration)
        except Exception as error:
            return AdmissionPreviewDecision(
                accepted=False,
                reason_code=type(error).__name__,
                reason="read-only admission preview rejected the frozen request",
            )
        return AdmissionPreviewDecision(
            accepted=True,
            reason_code="accepted",
            reason="read-only admission preview accepted",
        )


@dataclass(frozen=True)
class ProfileDerivedCapabilitySelection:
    workflow_hit: CapabilitySearchHit
    profile_hit: CapabilitySearchHit
    profile_record: PublishedDefinition
    selected_hits: tuple[CapabilitySearchHit, ...]
    requests: tuple[CapabilitySearchRequest, ...]


@dataclass(frozen=True)
class MCPPlanningProof:
    bootstrap: CoordinatorBootstrap
    progressive_skill: dict[str, object]
    selection: ProfileDerivedCapabilitySelection
    workflow_detail: CapabilityDetail
    workflow_contract: dict[str, object]
    external_discovery: dict[str, object] | None
    candidate_design_validation: WorkflowDesignValidation | None
    transport: dict[str, object]


@dataclass(frozen=True)
class LiveCoordinatorPrincipal:
    actor_id: str
    tenant_scope: str
    roles: frozenset[str]
    permissions: frozenset[str]
    request_scope: str = ""


class LiveRuntimeReadiness:
    def __init__(self, *, stagegraph_task_queue: str) -> None:
        self._stagegraph_task_queue = stagegraph_task_queue

    async def snapshot(self) -> tuple[BlueprintRuntimeStatus, ...]:
        return (
            BlueprintRuntimeStatus(
                family=BlueprintFamily.STAGE_GRAPH,
                executable=True,
                reason="Scenario D coordinator owns a bounded worker on the selected queue",
                evidence_ref=f"temporal-task-queue:{self._stagegraph_task_queue}",
            ),
            BlueprintRuntimeStatus(
                family=BlueprintFamily.GOAL_DIRECTED,
                executable=False,
                reason="Scenario D does not start a GoalDirected worker",
                evidence_ref="scenario-d:goal-directed:not-started",
            ),
        )


class FrozenLaunchContextProvider:
    def __init__(
        self,
        context: LaunchRequestContext,
        *,
        actor_id: str,
        tenant_scope: str,
    ) -> None:
        self._context = context
        self._actor_id = actor_id
        self._tenant_scope = tenant_scope

    async def current(
        self,
        principal: CoordinatorPrincipalLike,
        request_scope: str,
    ) -> LaunchRequestContext:
        if (
            principal.actor_id != self._actor_id
            or principal.tenant_scope != self._tenant_scope
            or request_scope != self._context.request_scope
        ):
            raise RuntimeError("live launch context identity does not match the principal")
        return self._context


class InMemoryMCPCapabilitySearchAdapter:
    def __init__(self, client: Any) -> None:
        self._client = client
        self.token_use: list[dict[str, object]] = []
        self.timings_ms: list[float] = []

    async def search(
        self,
        request: CapabilitySearchRequest,
    ) -> CapabilitySearchResponse:
        payload: dict[str, object] = {
            "query": request.query,
            "kinds": [kind.value for kind in sorted(request.kinds, key=str)],
            "required_capabilities": sorted(request.required_capabilities),
            "limit": request.limit,
        }
        if request.workflow_type_ref is not None:
            payload["workflow_type_ref"] = request.workflow_type_ref.model_dump(mode="json")
        if request.operation_class is not None:
            payload["operation_class"] = request.operation_class
        if request.runtime is not None:
            payload["runtime"] = request.runtime
        started = perf_counter()
        response = await _call_mcp_tool_data(
            self._client,
            "search_capabilities",
            payload,
        )
        self.timings_ms.append(round((perf_counter() - started) * 1_000, 3))
        parsed = CapabilitySearchResponse.model_validate(response)
        self.token_use.extend(
            {
                "request_index": len(self.timings_ms),
                **measurement.model_dump(mode="json"),
            }
            for measurement in parsed.token_use
        )
        return parsed


class CapabilitySearchCaller(Protocol):
    async def search(
        self,
        request: CapabilitySearchRequest,
    ) -> CapabilitySearchResponse: ...


async def run_live_coordinator(
    args: Any,
    *,
    artifact_root: Path,
) -> dict[str, Any]:
    settings = _live_settings()
    operator_correction_count = 0
    mongo_client, _ = await create_mongodb(settings)
    capability_pool = await create_postgres_pool(settings)
    application_pool = await create_application_postgres_pool(settings)
    try:
        definitions = BeanieDefinitionRepository()
        control_plane = _control_plane(definitions)
        task_queue = args.task_queue or f"{settings.temporal_task_queue}-web-research-live"
        audit_started_at = datetime.now(UTC)
        audit = PostgresCoordinatorAuditSink(application_pool)
        principal = LiveCoordinatorPrincipal(
            actor_id=args.actor_id,
            tenant_scope=args.tenant_scope,
            roles=frozenset({"coordinator_planner"}),
            permissions=frozenset(
                {
                    "catalog.read",
                    "capability.discover",
                    "workflow.design.validate",
                    "workflow.prepare",
                    "workflow.launch",
                    "workflow.result.read",
                }
            ),
        )
        catalog_index = PostgresCatalogSearchRepository(capability_pool)
        search = CapabilitySearchService(
            search=catalog_index,
            definitions=definitions,
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
        refs = await list_published_definition_refs()
        records: tuple[PublishedDefinition, ...] = tuple(
            [await definitions.get(ref) for ref in refs]
        )
        current = _current_records(records)
        coordinator_skill_record = _required_current(
            current,
            DefinitionKind.SKILL,
            "skill.belllabs-workflow-coordinator",
        )
        candidates = BeanieExternalCandidateRepository()
        discovery_service = (
            None
            if args.skip_external_discovery
            else _external_discovery_service(settings, candidates)
        )
        prompt_record = current.get(
            (
                DefinitionKind.PROMPT,
                "prompt.coordinator.propose-workflow",
            )
        )
        read_facade = ProductionCoordinatorFacade(
            definitions=definitions,
            catalog_index=catalog_index,
            search=search,
            readiness=LiveRuntimeReadiness(stagegraph_task_queue=task_queue),
            coordinator_skill=DefinitionSelector(exact=coordinator_skill_record.ref),
            prompt_bindings=(
                {"propose_workflow": prompt_record.ref} if prompt_record is not None else {}
            ),
            flags=CoordinatorFeatureFlags(
                capability_search_enabled=True,
                external_discovery_enabled=discovery_service is not None,
                coordinator_launch_enabled=False,
            ),
            audit=audit,
            discovery=discovery_service,
            limits=CoordinatorLimits(request_timeout_seconds=120),
        )
        planning = await _run_mounted_mcp_planning(
            read_facade,
            settings=settings,
            principal=principal,
            current=current,
            tenant_scope=args.tenant_scope,
            candidates=candidates,
            npx_package_version=settings.npx_skills_package_version,
            skip_external_discovery=args.skip_external_discovery,
        )
        bootstrap = planning.bootstrap
        progressive_skill = planning.progressive_skill
        selection = planning.selection
        workflow_hit = selection.workflow_hit
        selected_hits = selection.selected_hits
        requests = selection.requests
        projection_generation_proof = await _projection_generation_proof(
            PostgresProjectionGenerationRepository(capability_pool),
            tenant_scope=args.tenant_scope,
            hits=(workflow_hit, *selected_hits),
        )
        selected_identities = {
            (ref.kind, ref.logical_id) for ref in (_required_hit_ref(hit) for hit in selected_hits)
        }
        if selected_identities != REQUIRED_SELECTED_IDENTITIES:
            raise RuntimeError(
                "profile-derived selection does not satisfy the Scenario D "
                "post-retrieval acceptance contract"
            )
        workflow_ref = _required_hit_ref(workflow_hit)
        workflow_record = _required_current(
            current, DefinitionKind.WORKFLOW_TYPE, workflow_ref.logical_id
        )
        if workflow_record.ref != workflow_ref:
            raise RuntimeError("Workflow Type search evidence is not the current Mongo head")
        workflow_detail = planning.workflow_detail
        workflow_contract = planning.workflow_contract
        implementation = _implementation_for(current, workflow_ref)
        assert isinstance(
            implementation.definition,
            WorkflowImplementationBindingDefinition,
        )
        implementation_definition = implementation.definition
        runtime_record = _required_current(
            current,
            DefinitionKind.RUNTIME_PROFILE,
            implementation_definition.runtime_profile_ref.logical_id,
        )
        workspace_record = _required_current(
            current,
            DefinitionKind.WORKSPACE_TEMPLATE,
            implementation_definition.workspace_template_ref.logical_id,
        )
        assert isinstance(workflow_record.definition, WorkflowTypeDefinition)
        assert isinstance(runtime_record.definition, RuntimeProfileDefinition)
        assert isinstance(workspace_record.definition, WorkspaceTemplateDefinition)
        blueprint_record = _record_for_exact_ref(
            current,
            implementation_definition.blueprint_ref,
        )
        if not isinstance(blueprint_record.definition, StageGraphBlueprint):
            raise RuntimeError("Scenario D selected implementation is not a StageGraph")
        external_discovery = planning.external_discovery
        candidate_design_validation = planning.candidate_design_validation
        if args.plan_only:
            audit_events = await audit.list_events(
                tenant_scope=principal.tenant_scope,
                actor_id=principal.actor_id,
                occurred_since=audit_started_at,
            )
            return {
                "mode": "live-coordinator-plan-only",
                "mission_digest": sha256_digest(args.goal),
                "coordinator_transport": planning.transport,
                "coordinator_bootstrap": bootstrap.model_dump(mode="json"),
                "progressive_coordinator_skill": progressive_skill,
                "workflow_type_search_ref": workflow_ref.model_dump(mode="json"),
                "workflow_type_exact_contract": {
                    "detail": workflow_detail.model_dump(mode="json"),
                    "resource": workflow_contract,
                },
                "internal_search_requests": [
                    request.model_dump(mode="json") for request in requests
                ],
                "selected_capability_hits": [hit.model_dump(mode="json") for hit in selected_hits],
                "selection_basis": {
                    "strategy": "top-current-profile-derived-exact-dependencies",
                    "agent_profile_ref": selection.profile_record.ref.model_dump(mode="json"),
                    "required_identity_assertion_phase": "post-retrieval-only",
                    "external_candidates_authorize_selection": False,
                    "query_identity_hints_absent": _selection_queries_are_name_free(requests),
                    "current_head_and_digest_verified": True,
                },
                "projection_generation": projection_generation_proof,
                "external_discovery": external_discovery,
                "candidate_design_validation": (
                    candidate_design_validation.model_dump(mode="json")
                    if candidate_design_validation is not None
                    else None
                ),
                "measured_metrics": {
                    "operator_correction_count": operator_correction_count,
                    "planning_elapsed_ms": planning.transport["planning_elapsed_ms"],
                    "search_estimated_tokens_total": planning.transport[
                        "search_estimated_tokens_total"
                    ],
                    "audit_event_count": len(audit_events),
                },
                "coordinator_audit": {
                    "event_count": len(audit_events),
                    "events": [event.model_dump(mode="json") for event in audit_events],
                },
            }

        now = datetime.now(UTC)
        actor = ActorContext(
            actor_id=args.actor_id,
            authority_refs=frozenset({"authority:coordinator-live"}),
            permissions=frozenset({"workflow_run.admit"}),
        )
        invocation = _compile_invocation(
            workflow_record,
            implementation,
            runtime_record,
            goal=args.goal,
            actor=actor,
            request_scope=args.request_scope,
            compiled_at=now,
        )
        goal = WebResearchGoal(question=args.goal)
        artifacts = attest_reviewed_web_research_runtime(settings)
        operation_repository = create_semantic_operation_binding_repository(settings)
        operation_templates = _operation_templates(
            selected_hits,
            records=current,
            profile_record=selection.profile_record,
            workflow=workflow_record.definition,
            runtime=runtime_record.definition,
            workspace_ref=workspace_record.ref,
            goal=goal,
            created_at=now,
            operation_task_queue=f"{settings.temporal_task_queue}-agent-cognitive",
        )
        operation_author = SemanticServiceWebResearchOperationBindingAuthor(
            operation_templates,
            SemanticOperationExecutionBindingService(operation_repository),
            operation_repository,
        )
        retrieval_request = CapabilitySearchRequest(
            query=(
                "public company technology research using two source searches and "
                "independent browser verification"
            ),
            kinds=frozenset(
                {
                    DefinitionKind.MCP_SERVER,
                    DefinitionKind.MCP_TOOL,
                    DefinitionKind.SKILL,
                    DefinitionKind.AGENT_PROFILE,
                }
            ),
            tenant_scope=args.tenant_scope,
            workflow_type_ref=workflow_ref,
            limit=20,
        )
        provider = WebResearchSemanticBindingProvider(
            catalog_records=records,
            retrieval_request=retrieval_request,
            retrieval_hits=selected_hits,
            goal=goal,
            firecrawl_runtime=artifacts.firecrawl,
            tavily_runtime=artifacts.tavily,
            browser_runtime=artifacts.browser,
            operation_bindings=operation_author,
            maximum_results=args.maximum_results,
            browser_verification_limit=args.browser_verification_limit,
        )
        policies = AdmissionPolicyRegistry()
        register_web_research_admission_policies(policies)
        verifier = F1RunConfigurationVerifier(control_plane)
        run_repository = PostgresRunControlRepository(application_pool)
        run_control = RunControlService(
            run_repository,
            verifier,
            policies,
        )
        tickets = PostgresLaunchTicketRepository(application_pool)
        semantic_repository = PostgresRunSemanticInputBindingRepository(application_pool)
        dispatcher = WorkflowLaunchDispatcher(
            stagegraph=StageGraphLaunchService(run_control, control_plane),
            goal_directed=None,
            run_control=run_control,
            control_plane=control_plane,
        )
        temporal = await create_temporal_client(settings)
        launch_inputs = CoordinatorLaunchProductionInputs(
            compiler=control_plane,
            admission_preview=ReadOnlyAdmissionPreview(verifier, policies),
            admission=run_control,
            tickets=tickets,
            dispatcher=dispatcher,
            submissions=TemporalWorkflowSubmitter(
                temporal,
                stagegraph_task_queue=task_queue,
                goal_directed_task_queue=f"{task_queue}-unused-goal-directed",
            ),
            semantic_bindings=provider,
            runtime_plans=UnavailableRuntimePlanPreparer(),
            binding_service=RunSemanticInputBindingService(semantic_repository),
        )
        preparation, launcher = launch_inputs.build()
        proposal, context = _launch_proposal(
            invocation,
            workflow=workflow_record.definition,
            selected_hits=selected_hits,
            goal=goal,
            actor=actor,
            tenant_scope=args.tenant_scope,
            request_scope=args.request_scope,
            now=now,
        )
        if args.request_scope != principal.tenant_scope:
            raise RuntimeError(
                "production coordinator requires request scope to match authenticated tenant"
            )
        context_provider = FrozenLaunchContextProvider(
            context,
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
        )
        results = PostgresWorkflowResultRepository(application_pool)
        result_service = CoordinatorResultService(
            runs=run_control,
            results=results,
        )
        run_resources = CoordinatorRunResourceService(
            runs=run_control,
            bindings=semantic_repository,
        )
        live_facade = ProductionCoordinatorFacade(
            definitions=definitions,
            catalog_index=catalog_index,
            search=search,
            readiness=LiveRuntimeReadiness(stagegraph_task_queue=task_queue),
            coordinator_skill=DefinitionSelector(exact=coordinator_skill_record.ref),
            prompt_bindings=(
                {"propose_workflow": prompt_record.ref} if prompt_record is not None else {}
            ),
            flags=CoordinatorFeatureFlags(
                capability_search_enabled=True,
                external_discovery_enabled=discovery_service is not None,
                coordinator_launch_enabled=True,
            ),
            audit=audit,
            discovery=discovery_service,
            preparation=preparation,
            launcher=launcher,
            results=result_service,
            launch_contexts=context_provider,
            run_resources=run_resources,
            limits=CoordinatorLimits(request_timeout_seconds=120),
        )
        prepare_started = perf_counter()
        public_ticket = await _prepare_through_mounted_mcp(
            live_facade,
            principal,
            settings=settings,
            proposal=proposal,
        )
        prepare_latency_ms = round((perf_counter() - prepare_started) * 1_000, 3)
        artifact_bucket = args.artifact_bucket or settings.s3_bucket
        if not artifact_bucket:
            raise RuntimeError(
                "live acceptance requires --artifact-bucket or the S3_BUCKET setting"
            )
        screenshots = VerifiedS3ScreenshotStore(
            S3ArtifactPayloadStore(
                settings,
                artifact_bucket,
                prefix="web-research/screenshots",
            ),
            mirror_root=artifact_root,
        )
        _ = build_live_web_research_handler_dependencies(
            settings=settings,
            firecrawl_tool_ref=_selected_ref(
                selected_hits,
                DefinitionKind.MCP_TOOL,
                "mcp.firecrawl:firecrawl_search",
            ),
            tavily_tool_ref=_selected_ref(
                selected_hits,
                DefinitionKind.MCP_TOOL,
                "mcp.tavily:tavily_search",
            ),
            screenshot_artifacts=screenshots,
        )
        lifecycle = RunControlLifecycleGateway(
            run_control,
            F1OrchestrationBindingVerifier(control_plane),
            orchestration_lifecycle_actor(),
        )
        worker = create_web_research_stagegraph_worker(
            temporal,
            task_queue=task_queue,
            lifecycle=lifecycle,
            decision_service=StageGraphDecisionService(
                run_control,
                run_repository,
            ),
            operation_materializer=StageGraphOperationPreparationService(
                templates=StaticStageGraphOperationTemplateProvider(
                    {
                        f"{stage_id}/execute/default": template
                        for stage_id, template in operation_templates.operations.items()
                    }
                ),
                operation_bindings=operation_repository,
            ),
            completion=TerminalWorkflowCompletionService(
                runs=run_control,
                results=results,
                bindings=semantic_repository,
            ),
        )
        async with worker:
            launch_started = perf_counter()
            launch_handle = await _launch_through_mounted_mcp(
                live_facade,
                principal,
                settings=settings,
                ticket=public_ticket,
                proposal=proposal,
            )
            launch_submission_latency_ms = round(
                (perf_counter() - launch_started) * 1_000,
                3,
            )
            temporal_handle = temporal.get_workflow_handle_for(
                StageGraphWorkflow.run,
                launch_handle.workflow_id,
            )
            workflow_wait_started = perf_counter()
            run_result = await temporal_handle.result()
            workflow_execution_latency_ms = round(
                (perf_counter() - workflow_wait_started) * 1_000,
                3,
            )

        final_refs = run_result.output_refs.get("promote_verified_result", ())
        if len(final_refs) != 1:
            raise RuntimeError(
                "Scenario D coordinator run did not produce one verified terminal result"
            )
        persisted = await results.get(
            args.tenant_scope,
            args.request_scope,
            launch_handle.run_id,
        )
        if persisted is None:
            raise RuntimeError("Temporal completion did not materialize a durable typed result")
        result_read_started = perf_counter()
        retrieved = await _result_through_mounted_mcp(
            live_facade,
            principal,
            settings=settings,
            run_id=launch_handle.run_id,
        )
        returned_resource_reads = await _read_run_resources_through_mounted_mcp(
            live_facade,
            principal,
            settings=settings,
            uris={
                "launch": f"belllabs://runs/{launch_handle.run_id}/launch",
                "bindings": f"belllabs://runs/{launch_handle.run_id}/bindings",
                "result": launch_handle.result_resource_uri,
            },
        )
        result_read_latency_ms = round(
            (perf_counter() - result_read_started) * 1_000,
            3,
        )
        audit_events = await audit.list_events(
            tenant_scope=principal.tenant_scope,
            actor_id=principal.actor_id,
            occurred_since=audit_started_at,
        )
        required_audit_operations = {
            "coordinator_bootstrap",
            "search_capabilities",
            "get_capability",
            "read_resource",
            "prepare_workflow_launch",
            "launch_workflow",
            "get_workflow_result",
        }
        if external_discovery is not None:
            required_audit_operations |= {
                "discover_mcp_servers",
                "discover_agent_skills",
                "validate_workflow_design",
            }
        observed_audit_operations = {event.operation for event in audit_events}
        if missing_audits := required_audit_operations - observed_audit_operations:
            raise RuntimeError(
                "durable coordinator audit is incomplete: " + ", ".join(sorted(missing_audits))
            )
        provider_stage_ids = ("search_firecrawl", "search_tavily")
        provider_success_count = sum(
            bool(run_result.output_refs.get(stage_id)) for stage_id in provider_stage_ids
        )
        browser_evidence_success_count = int(
            bool(run_result.output_refs.get("browser_verify")) and bool(screenshots.refs)
        )
        if provider_success_count != len(provider_stage_ids):
            raise RuntimeError("terminal run lacks successful evidence from both providers")
        if browser_evidence_success_count != 1:
            raise RuntimeError("terminal run lacks verified browser screenshot evidence")
        audit_event_type_counts = {
            operation: sum(event.operation == operation for event in audit_events)
            for operation in sorted(observed_audit_operations)
        }
        return {
            "mode": "live-coordinator",
            "mission_digest": sha256_digest(args.goal),
            "coordinator_transport": planning.transport,
            "coordinator_bootstrap": bootstrap.model_dump(mode="json"),
            "progressive_coordinator_skill": progressive_skill,
            "workflow_type_search_ref": workflow_ref.model_dump(mode="json"),
            "workflow_type_exact_contract": {
                "detail": workflow_detail.model_dump(mode="json"),
                "resource": workflow_contract,
            },
            "internal_search_requests": [request.model_dump(mode="json") for request in requests],
            "selected_capability_refs": [
                _required_hit_ref(hit).model_dump(mode="json") for hit in selected_hits
            ],
            "selected_capability_hits": [hit.model_dump(mode="json") for hit in selected_hits],
            "selection_basis": {
                "strategy": "top-current-profile-derived-exact-dependencies",
                "workflow_type_ref": workflow_ref.model_dump(mode="json"),
                "agent_profile_ref": selection.profile_record.ref.model_dump(mode="json"),
                "required_identity_assertion_phase": "post-retrieval-only",
                "external_candidates_authorize_selection": False,
                "query_identity_hints_absent": _selection_queries_are_name_free(requests),
                "current_head_and_digest_verified": True,
            },
            "projection_generation": projection_generation_proof,
            "external_discovery": external_discovery,
            "candidate_design_validation": (
                candidate_design_validation.model_dump(mode="json")
                if candidate_design_validation is not None
                else None
            ),
            "ticket_id": public_ticket.ticket_id,
            "run_id": launch_handle.run_id,
            "operation_binding_refs": list(persisted.operation_binding_refs),
            "semantic_binding_plan_ref": public_ticket.semantic_binding_plan_ref,
            "workflow_id": launch_handle.workflow_id,
            "temporal_run_id": launch_handle.temporal_run_id,
            "typed_result": retrieved.model_dump(mode="json"),
            "returned_resource_reads": returned_resource_reads,
            "screenshots": screenshots.refs,
            "measured_metrics": {
                "prepare_latency_ms": prepare_latency_ms,
                "launch_submission_latency_ms": launch_submission_latency_ms,
                "workflow_execution_latency_ms": workflow_execution_latency_ms,
                "result_read_latency_ms": result_read_latency_ms,
                "operator_correction_count": operator_correction_count,
                "provider_success": {
                    "numerator": provider_success_count,
                    "denominator": len(provider_stage_ids),
                },
                "browser_evidence_success": {
                    "numerator": browser_evidence_success_count,
                    "denominator": 1,
                },
                "successful_run_failure_count": 0,
                "search_estimated_tokens_total": planning.transport[
                    "search_estimated_tokens_total"
                ],
                "audit_event_count": len(audit_events),
                "audit_event_type_counts": audit_event_type_counts,
            },
            "coordinator_audit": {
                "event_count": len(audit_events),
                "event_refs": [
                    (f"postgres://belllabs_control/coordinator_audit_events/{event.event_id}")
                    for event in audit_events
                ],
                "events": [event.model_dump(mode="json") for event in audit_events],
                "required_operations": sorted(required_audit_operations),
            },
        }
    finally:
        await application_pool.close()
        await capability_pool.close()
        await mongo_client.close()


def _live_settings() -> Settings:
    """Bind reviewed workspace Node/npx artifacts without relying on host PATH."""

    node = (Path(sys.base_prefix).resolve().parent / "node" / "bin" / "node.exe").resolve(
        strict=True
    )
    npx = (PROJECT_ROOT.parent / ".tools" / "node_modules" / ".bin" / "npx.CMD").resolve(
        strict=True
    )
    node_path = str(node.parent)
    current_path = os.environ.get("PATH", "")
    if node_path.casefold() not in {
        part.casefold() for part in current_path.split(os.pathsep) if part
    }:
        os.environ["PATH"] = node_path + os.pathsep + current_path
    return Settings().model_copy(
        update={
            "npx_skills_executable": str(npx),
            "web_research_agent_browser_node": node,
            # The preview Registry routinely responds just beyond the ten-second
            # application default. Keep the live proof bounded, but align it with
            # the already-proven Scenario B discovery envelope.
            "external_discovery_request_timeout_seconds": 30.0,
            "external_discovery_command_timeout_seconds": 60.0,
            "external_discovery_max_retries": 4,
            "web_research_browser_command_timeout_seconds": 60.0,
            "web_research_browser_timeout_seconds": 240.0,
        }
    )


def _external_discovery_service(
    settings: Settings,
    candidates: BeanieExternalCandidateRepository,
) -> ExternalCapabilityDiscoveryService:
    return ExternalCapabilityDiscoveryService(
        enabled=True,
        mcp_registry=MCPRegistryAdapter(
            HttpxMCPRegistryRunner(),
            base_url=settings.mcp_registry_base_url,
            api_version=settings.mcp_registry_api_version,
            timeout_seconds=settings.external_discovery_request_timeout_seconds,
            max_response_bytes=settings.external_discovery_max_output_bytes,
            max_pages=settings.external_discovery_max_pages,
            max_retries=settings.external_discovery_max_retries,
        ),
        skills=NpxSkillsDiscoveryAdapter(
            AsyncioSkillDiscoverySubprocessRunner(),
            executable=settings.npx_skills_executable,
            package_version=settings.npx_skills_package_version,
            timeout_seconds=settings.external_discovery_command_timeout_seconds,
            max_output_bytes=settings.external_discovery_max_output_bytes,
        ),
        candidates=candidates,
        max_results=settings.external_discovery_max_results,
    )


async def _run_mounted_mcp_planning(
    facade: ProductionCoordinatorFacade,
    *,
    settings: Settings,
    principal: LiveCoordinatorPrincipal,
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
    tenant_scope: str,
    candidates: BeanieExternalCandidateRepository,
    npx_package_version: str,
    skip_external_discovery: bool,
) -> MCPPlanningProof:
    """Exercise the real agent-facing FastMCP schemas before any launch mutation."""

    from app.mcp.coordinator_http_client import mounted_coordinator_client
    from app.mcp.coordinator_server import CoordinatorPrincipal

    planning_started = perf_counter()
    async with mounted_coordinator_client(
        settings=settings,
        facade=cast(Any, facade),
        principal=CoordinatorPrincipal(
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
            request_scope=principal.request_scope or principal.tenant_scope,
            roles=principal.roles,
            permissions=principal.permissions,
        ),
    ) as client:
        tools = await client.list_tools()
        tool_snapshot = {
            "schema_version": "fastmcp-tools-list/1",
            "tools": [
                tool.model_dump(mode="json", exclude_none=True)
                for tool in sorted(tools, key=lambda item: item.name)
            ],
        }
        bootstrap = CoordinatorBootstrap.model_validate(
            await _call_mcp_tool_data(client, "coordinator_bootstrap", {})
        )
        progressive_skill = await _progressive_coordinator_skill_proof(
            client,
            bootstrap=bootstrap,
            current=current,
        )
        search_adapter = InMemoryMCPCapabilitySearchAdapter(client)
        selection = await _retrieve_exact_capabilities(
            search_adapter,
            tenant_scope=tenant_scope,
            current=current,
        )
        workflow_ref = _required_hit_ref(selection.workflow_hit)
        workflow_detail = CapabilityDetail.model_validate(
            await _call_mcp_tool_data(
                client,
                "get_capability",
                {"exact_ref": workflow_ref.model_dump(mode="json")},
            )
        )
        workflow_contract = await _mcp_read_json_resource(
            client,
            (
                "belllabs://workflow-types/"
                f"{workflow_ref.logical_id}/{workflow_ref.revision}/contract"
            ),
        )
        external_discovery = (
            None
            if skip_external_discovery
            else await _external_discovery_proof(
                client,
                candidates=candidates,
                npx_package_version=npx_package_version,
            )
        )
        candidate_design_validation: WorkflowDesignValidation | None = None
        if external_discovery is not None:
            workflow_record = _record_for_exact_ref(current, workflow_ref)
            if not isinstance(workflow_record.definition, WorkflowTypeDefinition):
                raise RuntimeError("selected workflow exact contract has the wrong kind")
            implementation = _implementation_for(current, workflow_ref)
            assert isinstance(
                implementation.definition,
                WorkflowImplementationBindingDefinition,
            )
            blueprint_record = _record_for_exact_ref(
                current,
                implementation.definition.blueprint_ref,
            )
            if not isinstance(blueprint_record.definition, StageGraphBlueprint):
                raise RuntimeError("candidate validation fixture is not a StageGraph")
            candidate_id = _first_discovered_candidate_id(external_discovery)
            candidate_design_validation = WorkflowDesignValidation.model_validate(
                await _call_mcp_tool_data(
                    client,
                    "validate_workflow_design",
                    {
                        "draft": _candidate_only_design_draft(
                            workflow=workflow_record.definition,
                            blueprint=blueprint_record.definition,
                            candidate_id=candidate_id,
                        )
                    },
                )
            )
            if (
                candidate_design_validation.launchable
                or not candidate_design_validation.requires_publication
                or candidate_id not in candidate_design_validation.candidate_ids_requiring_promotion
            ):
                raise RuntimeError("external candidate design validation granted launch authority")
    planning_calls = [
        "coordinator_bootstrap",
        "get_capability:coordinator-skill-metadata",
        "read_resource:coordinator-skill-manifest",
        *["search_capabilities" for _request in selection.requests],
        "get_capability:workflow-type-exact",
        "read_resource:workflow-type-contract",
    ]
    if external_discovery is not None:
        planning_calls.extend(
            [
                "discover_mcp_servers",
                "discover_agent_skills",
                "validate_workflow_design:candidate-only",
            ]
        )
    return MCPPlanningProof(
        bootstrap=bootstrap,
        progressive_skill=progressive_skill,
        selection=selection,
        workflow_detail=workflow_detail,
        workflow_contract=workflow_contract,
        external_discovery=external_discovery,
        candidate_design_validation=candidate_design_validation,
        transport={
            "kind": "mounted-fastapi-streamable-http",
            "server": "BellLabs Coordinator",
            "schema_version": "1",
            "tool_schema_count": len(tools),
            "tool_schema_digest": sha256_digest(tool_snapshot),
            "tool_names": [tool.name for tool in sorted(tools, key=lambda item: item.name)],
            "planning_calls": planning_calls,
            "planning_elapsed_ms": round(
                (perf_counter() - planning_started) * 1_000,
                3,
            ),
            "search_call_timings_ms": search_adapter.timings_ms,
            "search_token_use": search_adapter.token_use,
            "search_estimated_tokens_total": sum(
                cast(int, item["estimated_tokens"]) for item in search_adapter.token_use
            ),
            "mutation_calls": [
                "prepare_workflow_launch",
                "launch_workflow",
                "get_workflow_result",
            ],
            "stable_envelope": True,
        },
    )


async def _progressive_coordinator_skill_proof(
    client: Any,
    *,
    bootstrap: CoordinatorBootstrap,
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
) -> dict[str, object]:
    selected_ref = bootstrap.coordinator_skill_ref
    record = _record_for_exact_ref(current, selected_ref)
    if not isinstance(record.definition, SkillDefinition):
        raise RuntimeError("bootstrap coordinator skill ref has the wrong catalog kind")
    metadata = CapabilityDetail.model_validate(
        await _call_mcp_tool_data(
            client,
            "get_capability",
            {"exact_ref": selected_ref.model_dump(mode="json")},
        )
    )
    manifest = await _mcp_read_json_resource(
        client,
        (f"belllabs://catalog/skill/{selected_ref.logical_id}/{selected_ref.revision}/manifest"),
    )
    skill_entry = next(
        (entry for entry in record.definition.file_manifest if entry.path == "SKILL.md"),
        None,
    )
    if skill_entry is None:
        raise RuntimeError("selected coordinator skill manifest has no SKILL.md")
    skill_path = (
        PROJECT_ROOT / ".agents" / "skills" / record.definition.skill_name / "SKILL.md"
    ).resolve(strict=True)
    skill_bytes = await asyncio.to_thread(skill_path.read_bytes)
    actual_digest = f"sha256:{sha256(skill_bytes).hexdigest()}"
    if actual_digest != skill_entry.digest or len(skill_bytes) != skill_entry.size_bytes:
        raise RuntimeError("exact coordinator SKILL.md differs from its Mongo manifest")
    return {
        "bootstrap_selected_ref": selected_ref.model_dump(mode="json"),
        "metadata_read_before_file": metadata.model_dump(mode="json"),
        "manifest_resource": manifest,
        "exact_skill_file": {
            "path": str(skill_path),
            "digest": actual_digest,
            "size_bytes": len(skill_bytes),
            "loaded_after_bootstrap_selection": True,
            "content_omitted_from_result": True,
        },
    }


async def _external_discovery_proof(
    client: Any,
    *,
    candidates: BeanieExternalCandidateRepository,
    npx_package_version: str,
) -> dict[str, object]:
    """Query both discovery tools through FastMCP and prove Mongo persistence."""

    mcp, skills = await asyncio.gather(
        _call_mcp_tool_data(
            client,
            "discover_mcp_servers",
            {"query": EXTERNAL_MCP_DISCOVERY_QUERY},
        ),
        _call_mcp_tool_data(
            client,
            "discover_agent_skills",
            {"query": EXTERNAL_SKILL_DISCOVERY_QUERY},
        ),
    )
    mcp_batch = ExternalDiscoveryBatch.model_validate(mcp)
    skills_batch = ExternalDiscoveryBatch.model_validate(skills)
    persisted: list[dict[str, object]] = []
    for candidate in (*mcp_batch.candidates, *skills_batch.candidates):
        record = await candidates.get_candidate(candidate.candidate_id)
        if record.candidate != candidate:
            raise RuntimeError("external candidate readback does not match this discovery batch")
        evidence = await candidates.get_evidence(record.evidence_id)
        if evidence.evidence.raw_response_digest != candidate.raw_response_digest:
            raise RuntimeError("external discovery evidence digest does not match candidate")
        persisted.append(
            {
                "candidate_id": candidate.candidate_id,
                "candidate_record_id": record.candidate_record_id,
                "candidate_content_digest": record.content_digest,
                "candidate_recorded_at": record.recorded_at.isoformat(),
                "evidence_id": evidence.evidence_id,
                "evidence_record_digest": evidence.record_digest,
                "evidence_recorded_at": evidence.recorded_at.isoformat(),
                "source": candidate.source.value,
                "source_version": evidence.evidence.source_version,
                "query": candidate.query,
                "retrieved_at": evidence.evidence.retrieved_at.isoformat(),
                "raw_response_digest": candidate.raw_response_digest,
                "raw_response_size_bytes": (evidence.evidence.raw_response_size_bytes),
                "trust_tier": candidate.trust_tier,
                "inspection_status": candidate.inspection_status,
                "promoted_ref": candidate.promoted_ref,
            }
        )
    return {
        "mcp_registry": mcp_batch.model_dump(mode="json"),
        "npx_skills": skills_batch.model_dump(mode="json"),
        "npx_package_version": npx_package_version,
        "persisted_candidate_records": persisted,
        "selection_policy": (
            "candidate-only; external discoveries never authorize exact selection"
        ),
    }


async def _prepare_through_mounted_mcp(
    facade: ProductionCoordinatorFacade,
    principal: LiveCoordinatorPrincipal,
    *,
    settings: Settings,
    proposal: WorkflowLaunchProposal,
) -> PublicPreparedLaunchTicket:
    from app.mcp.coordinator_http_client import mounted_coordinator_client
    from app.mcp.coordinator_server import CoordinatorPrincipal

    async with mounted_coordinator_client(
        settings=settings,
        facade=cast(Any, facade),
        principal=CoordinatorPrincipal(
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
            request_scope=principal.request_scope or principal.tenant_scope,
            roles=principal.roles,
            permissions=principal.permissions,
        ),
    ) as client:
        return PublicPreparedLaunchTicket.model_validate(
            await _call_mcp_tool_data(
                client,
                "prepare_workflow_launch",
                {"proposal": proposal.model_dump(mode="json")},
            )
        )


async def _launch_through_mounted_mcp(
    facade: ProductionCoordinatorFacade,
    principal: LiveCoordinatorPrincipal,
    *,
    settings: Settings,
    ticket: PublicPreparedLaunchTicket,
    proposal: WorkflowLaunchProposal,
) -> WorkflowLaunchHandle:
    from app.mcp.coordinator_http_client import mounted_coordinator_client
    from app.mcp.coordinator_server import CoordinatorPrincipal

    async with mounted_coordinator_client(
        settings=settings,
        facade=cast(Any, facade),
        principal=CoordinatorPrincipal(
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
            request_scope=principal.request_scope or principal.tenant_scope,
            roles=principal.roles,
            permissions=principal.permissions,
        ),
    ) as client:
        return WorkflowLaunchHandle.model_validate(
            await _call_mcp_tool_data(
                client,
                "launch_workflow",
                {
                    "ticket_id": ticket.ticket_id,
                    "idempotency_issuer": proposal.idempotency_issuer,
                    "idempotency_key": proposal.idempotency_key,
                },
            )
        )


async def _result_through_mounted_mcp(
    facade: ProductionCoordinatorFacade,
    principal: LiveCoordinatorPrincipal,
    *,
    settings: Settings,
    run_id: str,
) -> WorkflowResultView:
    from app.mcp.coordinator_http_client import mounted_coordinator_client
    from app.mcp.coordinator_server import CoordinatorPrincipal

    async with mounted_coordinator_client(
        settings=settings,
        facade=cast(Any, facade),
        principal=CoordinatorPrincipal(
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
            request_scope=principal.request_scope or principal.tenant_scope,
            roles=principal.roles,
            permissions=principal.permissions,
        ),
    ) as client:
        return WorkflowResultView.model_validate(
            await _call_mcp_tool_data(
                client,
                "get_workflow_result",
                {"run_id": run_id},
            )
        )


async def _read_run_resources_through_mounted_mcp(
    facade: ProductionCoordinatorFacade,
    principal: LiveCoordinatorPrincipal,
    *,
    settings: Settings,
    uris: dict[str, str],
) -> dict[str, object]:
    from app.mcp.coordinator_http_client import mounted_coordinator_client
    from app.mcp.coordinator_server import CoordinatorPrincipal

    async with mounted_coordinator_client(
        settings=settings,
        facade=cast(Any, facade),
        principal=CoordinatorPrincipal(
            actor_id=principal.actor_id,
            tenant_scope=principal.tenant_scope,
            request_scope=principal.request_scope or principal.tenant_scope,
            roles=principal.roles,
            permissions=principal.permissions,
        ),
    ) as client:
        return {
            name: {
                "uri": uri,
                "content": await _mcp_read_json_resource(client, uri),
            }
            for name, uri in uris.items()
        }


async def _call_mcp_tool_data(
    client: Any,
    tool_name: str,
    arguments: dict[str, object],
) -> object:
    result = await client.call_tool(tool_name, arguments, timeout=120)
    envelope = result.data
    if not isinstance(envelope, dict):
        raise RuntimeError(f"coordinator MCP tool returned no stable envelope: {tool_name}")
    if envelope.get("ok") is not True:
        error = envelope.get("error")
        raise RuntimeError(f"coordinator MCP tool failed: {tool_name}: {error}")
    if "data" not in envelope:
        raise RuntimeError(f"coordinator MCP tool returned no data: {tool_name}")
    return envelope["data"]


async def _mcp_read_json_resource(client: Any, uri: str) -> dict[str, object]:
    resources = await client.read_resource(uri)
    if len(resources) != 1 or not isinstance(resources[0].text, str):
        raise RuntimeError("coordinator MCP resource returned the wrong content shape")
    try:
        payload = json.loads(resources[0].text)
    except json.JSONDecodeError as error:
        raise RuntimeError("coordinator MCP resource returned non-JSON content") from error
    if not isinstance(payload, dict):
        raise RuntimeError("coordinator MCP resource returned a non-object contract")
    return payload


def _first_discovered_candidate_id(discovery: dict[str, object]) -> str:
    for key in ("mcp_registry", "npx_skills"):
        batch = discovery.get(key)
        if isinstance(batch, dict):
            values = batch.get("candidates")
            if isinstance(values, list) and values:
                candidate = values[0]
                if isinstance(candidate, dict):
                    candidate_id = candidate.get("candidate_id")
                    if isinstance(candidate_id, str) and candidate_id:
                        return candidate_id
    raise RuntimeError("external discovery returned no candidate for quarantine proof")


def _candidate_only_design_draft(
    *,
    workflow: WorkflowTypeDefinition,
    blueprint: StageGraphBlueprint,
    candidate_id: str,
) -> dict[str, object]:
    return {
        "draft_id": f"candidate-quarantine-proof:{uuid4()}",
        "purpose": workflow.purpose,
        "proposed_workflow_type": workflow.model_dump(mode="json"),
        "blueprint_family": "StageGraph",
        "proposed_stage_graph": blueprint.model_dump(mode="json"),
        "input_contract": workflow.input_admission_contract,
        "invariants": sorted(workflow.invariants),
        "obligations": sorted(workflow.obligations),
        "output_contracts": sorted(workflow.output_contracts),
        "linked_run_slots": [],
        "requested_assets": [
            {
                "candidate_id": candidate_id,
                "purpose": "candidate-only discovery cannot authorize launch",
            }
        ],
        "requested_authority": workflow.authority_ceiling.model_dump(mode="json"),
        "workspace_requirements": workflow.workspace_contract.model_dump(mode="json"),
        "budgets": workflow.authority_ceiling.budgets.model_dump(mode="json"),
        "rationale": (
            "Prove that an external discovery remains quarantined until inspection "
            "and authorized publication."
        ),
    }


def _selection_queries_are_name_free(
    requests: tuple[CapabilitySearchRequest, ...],
) -> bool:
    queries = "\n".join(request.query.casefold() for request in requests)
    forbidden_hints = (
        "firecrawl",
        "tavily",
        "agent-browser",
        "mcp.firecrawl",
        "mcp.tavily",
        "firecrawl_search",
        "tavily_search",
    )
    return not any(hint in queries for hint in forbidden_hints)


async def _projection_generation_proof(
    repository: PostgresProjectionGenerationRepository,
    *,
    tenant_scope: str,
    hits: tuple[CapabilitySearchHit, ...],
) -> dict[str, object]:
    generations = {
        hit.projection_generation for hit in hits if hit.projection_generation is not None
    }
    if len(generations) != 1:
        raise RuntimeError("selected search evidence spans multiple projection generations")
    projection_generation = next(iter(generations))
    record = await repository.get(tenant_scope, projection_generation)
    if (
        record is None
        or record.state != "active"
        or record.actual_count != record.expected_count
        or record.actual_source_set_digest != record.expected_source_set_digest
    ):
        raise RuntimeError("selected search generation lacks active source-set verification")
    active_by_kind: dict[str, str] = {}
    for kind in sorted({hit.kind for hit in hits}, key=lambda item: item.value):
        active = await repository.active_for_kind(tenant_scope, kind)
        if active != projection_generation:
            raise RuntimeError("selected search evidence is not from the active kind generation")
        active_by_kind[kind.value] = active
    return {
        "projection_generation": projection_generation,
        "state": record.state,
        "expected_count": record.expected_count,
        "actual_count": record.actual_count,
        "expected_source_set_digest": record.expected_source_set_digest,
        "actual_source_set_digest": record.actual_source_set_digest,
        "embedding_model_id": record.embedding_model_id,
        "embedding_dimensions": record.embedding_dimensions,
        "search_document_format_version": record.search_document_format_version,
        "verified_at": (record.verified_at.isoformat() if record.verified_at is not None else None),
        "activated_at": (
            record.activated_at.isoformat() if record.activated_at is not None else None
        ),
        "active_generation_by_selected_kind": active_by_kind,
    }


def _control_plane(
    definitions: BeanieDefinitionRepository,
) -> ControlPlaneService:
    extensions = ExtensionRegistry()
    register_schema_grounding_extensions(extensions)
    return ControlPlaneService(
        definitions,
        extensions,
        UnavailablePayloadStore(),
        externalize_above_bytes=15_000_000,
    )


async def _retrieve_exact_capabilities(
    search: CapabilitySearchCaller,
    *,
    tenant_scope: str,
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
) -> ProfileDerivedCapabilitySelection:
    requests: list[CapabilitySearchRequest] = []
    hits: list[CapabilitySearchHit] = []
    workflow_hit: CapabilitySearchHit | None = None
    workflow_ref: ExactDefinitionRef | None = None
    for query, kinds in SEARCH_PLAN:
        request = CapabilitySearchRequest(
            query=query,
            kinds=kinds,
            tenant_scope=tenant_scope,
            workflow_type_ref=workflow_ref,
            limit=30,
        )
        response = await search.search(request)
        requests.append(request)
        if kinds == frozenset({DefinitionKind.WORKFLOW_TYPE}):
            workflow_hit = _top_current_selectable_hit(
                response.hits,
                current=current,
                kind=DefinitionKind.WORKFLOW_TYPE,
            )
            workflow_ref = _required_hit_ref(workflow_hit)
            continue
        hits.extend(response.hits)
    if workflow_hit is None:
        raise RuntimeError("internal search did not return a current selectable Workflow Type")
    profile_hit = _top_current_selectable_hit(
        tuple(hit for hit in hits if hit.kind == DefinitionKind.AGENT_PROFILE),
        current=current,
        kind=DefinitionKind.AGENT_PROFILE,
    )
    profile_ref = _required_hit_ref(profile_hit)
    profile_record = current[(profile_ref.kind, profile_ref.logical_id)]
    if not isinstance(profile_record.definition, AgentProfileDefinition):
        raise RuntimeError("selected Agent Profile rehydrated to the wrong definition kind")
    selected_hits = _profile_derived_selection(
        profile_hit,
        tuple(hits),
        current=current,
    )
    return ProfileDerivedCapabilitySelection(
        workflow_hit=workflow_hit,
        profile_hit=profile_hit,
        profile_record=profile_record,
        selected_hits=selected_hits,
        requests=tuple(requests),
    )


def _top_current_selectable_hit(
    hits: tuple[CapabilitySearchHit, ...],
    *,
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
    kind: DefinitionKind,
) -> CapabilitySearchHit:
    for hit in hits:
        ref = hit.exact_ref
        if (
            ref is None
            or hit.candidate_id is not None
            or hit.kind != kind
            or hit.authorization_state != AuthorizationState.SELECTABLE
        ):
            continue
        record = current.get((kind, ref.logical_id))
        if (
            record is not None
            and record.ref == ref
            and sha256_digest(record.definition) == ref.digest
        ):
            return hit
    raise RuntimeError(
        f"internal search did not return a current digest-verified selectable {kind.value}"
    )


def _profile_derived_selection(
    profile_hit: CapabilitySearchHit,
    hits: tuple[CapabilitySearchHit, ...],
    *,
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
) -> tuple[CapabilitySearchHit, ...]:
    profile_ref = _required_hit_ref(profile_hit)
    profile_record = current.get((profile_ref.kind, profile_ref.logical_id))
    if (
        profile_record is None
        or profile_record.ref != profile_ref
        or sha256_digest(profile_record.definition) != profile_ref.digest
        or not isinstance(profile_record.definition, AgentProfileDefinition)
    ):
        raise RuntimeError("selected Agent Profile is not the current digest-verified head")
    profile = profile_record.definition
    derived_refs = frozenset(
        {
            profile_ref,
            *profile.mcp_server_refs,
            *profile.tool_refs,
            *profile.skill_refs,
        }
    )
    selectable_by_ref = {
        ref: hit
        for hit in hits
        if (
            (ref := hit.exact_ref) is not None
            and hit.candidate_id is None
            and hit.authorization_state == AuthorizationState.SELECTABLE
            and current.get((ref.kind, ref.logical_id)) is not None
            and current[(ref.kind, ref.logical_id)].ref == ref
            and sha256_digest(current[(ref.kind, ref.logical_id)].definition) == ref.digest
        )
    }
    selectable_by_ref[profile_ref] = profile_hit
    missing = derived_refs - selectable_by_ref.keys()
    if missing:
        raise RuntimeError(
            "internal bounded search did not retrieve every exact dependency derived "
            "from the selected Agent Profile: "
            + ", ".join(
                sorted(f"{ref.kind.value}:{ref.logical_id}@{ref.revision}" for ref in missing)
            )
        )
    return tuple(
        selectable_by_ref[ref]
        for ref in sorted(
            derived_refs,
            key=lambda item: (
                item.kind.value,
                item.logical_id,
                item.revision,
                item.digest,
            ),
        )
    )


def _current_records(
    records: tuple[PublishedDefinition, ...],
) -> dict[tuple[DefinitionKind, str], PublishedDefinition]:
    current: dict[tuple[DefinitionKind, str], PublishedDefinition] = {}
    for record in records:
        if record.retired_at is not None:
            continue
        key = (record.ref.kind, record.ref.logical_id)
        prior = current.get(key)
        if prior is None or record.ref.revision > prior.ref.revision:
            current[key] = record
    return current


def _required_current(
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
    kind: DefinitionKind,
    logical_id: str,
) -> PublishedDefinition:
    record = current.get((kind, logical_id))
    if record is None:
        raise RuntimeError(f"published catalog head is unavailable: {kind.value}:{logical_id}")
    return record


def _implementation_for(
    current: dict[tuple[DefinitionKind, str], PublishedDefinition],
    workflow_ref: ExactDefinitionRef,
) -> PublishedDefinition:
    matches = [
        record
        for (kind, _logical_id), record in current.items()
        if kind == DefinitionKind.WORKFLOW_IMPLEMENTATION
        and isinstance(record.definition, WorkflowImplementationBindingDefinition)
        and record.definition.workflow_type_ref == workflow_ref
    ]
    if len(matches) != 1:
        raise RuntimeError("Scenario D requires exactly one current implementation binding")
    return matches[0]


def _compile_invocation(
    workflow: PublishedDefinition,
    implementation: PublishedDefinition,
    runtime: PublishedDefinition,
    *,
    goal: str,
    actor: ActorContext,
    request_scope: str,
    compiled_at: datetime,
) -> CompileInvocation:
    assert isinstance(workflow.definition, WorkflowTypeDefinition)
    assert isinstance(runtime.definition, RuntimeProfileDefinition)
    authority = workflow.definition.authority_ceiling
    return CompileInvocation(
        workflow_type=DefinitionSelector(exact=workflow.ref),
        implementation=DefinitionSelector(exact=implementation.ref),
        input_manifest=RunInputManifestRef(
            manifest_id=f"web-research-live:{uuid4()}",
            revision=1,
            digest=sha256_digest({"goal": goal, "request_scope": request_scope}),
        ),
        caller_authority=authority,
        parent_authority=authority,
        environment=EnvironmentAvailability(
            capabilities=authority.capabilities,
            runtime_bindings=frozenset({runtime.definition.binding}),
            secret_refs=runtime.definition.required_secrets,
        ),
        context=CompilationContext(
            compilation_id=f"web-research-live:{uuid4()}",
            compiled_at=compiled_at,
            actor_id=actor.actor_id,
            authority_subject_id=actor.actor_id,
            authority_scope=request_scope,
        ),
    )


def _launch_proposal(
    invocation: CompileInvocation,
    *,
    workflow: WorkflowTypeDefinition,
    selected_hits: tuple[CapabilitySearchHit, ...],
    goal: WebResearchGoal,
    actor: ActorContext,
    tenant_scope: str,
    request_scope: str,
    now: datetime,
) -> tuple[WorkflowLaunchProposal, LaunchRequestContext]:
    selected_refs = tuple(_required_hit_ref(hit) for hit in selected_hits)
    by_identity = {(ref.kind, ref.logical_id): ref for ref in selected_refs}
    evidence = (
        f"public-goal:{sha256_digest(goal)}",
        "capability-selection:"
        + sha256_digest([ref.model_dump(mode="json") for ref in selected_refs]),
        *(
            f"catalog://mcp_server/{logical_id}/{ref.revision}/{ref.digest}"
            for (kind, logical_id), ref in by_identity.items()
            if kind == DefinitionKind.MCP_SERVER
        ),
        "tool-allowlist:firecrawl_search:exact",
        "tool-allowlist:tavily_search:exact",
        (
            "catalog://skill/skill.agent-browser/"
            f"{by_identity[(DefinitionKind.SKILL, 'skill.agent-browser')].revision}/"
            f"{by_identity[(DefinitionKind.SKILL, 'skill.agent-browser')].digest}"
        ),
        f"browser-authority:{sha256_digest('bounded-public-browser')}",
        "policy:untrusted-web-content-is-data:v1",
    )
    dimensions = workflow.authority_ceiling.budgets.dimensions
    budget = BudgetEnvelope(
        dimensions=tuple(
            BudgetDimensionLimit(
                dimension=dimension,
                applicability=(
                    BudgetApplicability.BOUNDED
                    if dimension in dimensions
                    else BudgetApplicability.NOT_APPLICABLE
                ),
                hard_cap=dimensions.get(dimension),
            )
            for dimension in sorted(REQUIRED_SHARED_BUDGET_DIMENSIONS)
        )
    )
    idempotency_key = f"web-research-live:{uuid4()}"
    proposal = WorkflowLaunchProposal(
        request_scope=request_scope,
        tenant_scope=tenant_scope,
        compilation=invocation,
        admission=RunAdmissionSpec(
            actor=actor,
            budget_envelope=budget,
            requested_at=now,
            correlation_id=idempotency_key,
            sponsorship_ref="sponsorship:live-coordinator",
            approval_refs=("approval:user-live-web-research",),
            delegation_authority_refs=actor.authority_refs,
            admission_evidence_refs=evidence,
        ),
        selected_asset_refs=selected_refs,
        policy_snapshot_digest=sha256_digest("live-web-research-policy-v1"),
        environment_snapshot_digest=sha256_digest("live-web-research-environment-v1"),
        idempotency_issuer=actor.actor_id,
        idempotency_key=idempotency_key,
    )
    context = LaunchRequestContext(
        caller_id=actor.actor_id,
        tenant_scope=tenant_scope,
        request_scope=request_scope,
        approval_refs=("approval:user-live-web-research",),
        policy_snapshot_digest=proposal.policy_snapshot_digest,
        environment_snapshot_digest=proposal.environment_snapshot_digest,
        observed_at=now,
    )
    return proposal, context


def _operation_templates(
    selected_hits: tuple[CapabilitySearchHit, ...],
    *,
    records: dict[tuple[DefinitionKind, str], PublishedDefinition],
    profile_record: PublishedDefinition,
    workflow: WorkflowTypeDefinition,
    runtime: RuntimeProfileDefinition,
    workspace_ref: ExactDefinitionRef,
    goal: WebResearchGoal,
    created_at: datetime,
    operation_task_queue: str,
) -> SemanticOperationBindingTemplates:
    refs = tuple(_required_hit_ref(hit) for hit in selected_hits)
    assert isinstance(profile_record.definition, AgentProfileDefinition)
    profile = profile_record.definition
    if profile_record.ref not in refs:
        raise RuntimeError("selected Agent Profile is absent from exact retrieval evidence")
    servers: list[MCPServerBinding] = []
    for server_ref in sorted(
        profile.mcp_server_refs,
        key=lambda item: (item.logical_id, item.revision, item.digest),
    ):
        record = _record_for_exact_ref(records, server_ref)
        assert isinstance(record.definition, MCPServerDefinition)
        definition = record.definition
        allowed_tools = frozenset(
            tool.tool_name
            for tool_ref in profile.tool_refs
            for tool_record in (_record_for_exact_ref(records, tool_ref),)
            for tool in (tool_record.definition,)
            if (isinstance(tool, MCPToolDefinition) and tool.server_ref == server_ref)
        )
        if not allowed_tools:
            raise RuntimeError(
                "selected Agent Profile has an MCP server without an exact tool binding"
            )
        servers.append(
            MCPServerBinding(
                server_id=server_ref.logical_id,
                revision=record.ref.revision,
                transport=definition.transport,
                endpoint_ref=(
                    definition.endpoint or f"catalog://mcp-server/{server_ref.logical_id}/stdio"
                ),
                allowed_tools=allowed_tools,
                schema_digest=definition.schema_digest,
                approval_policy="never",
            )
        )
    skills: list[ImmutableAssetBinding] = []
    for skill_ref in sorted(
        profile.skill_refs,
        key=lambda item: (item.logical_id, item.revision, item.digest),
    ):
        record = _record_for_exact_ref(records, skill_ref)
        assert isinstance(record.definition, SkillDefinition)
        skills.append(
            ImmutableAssetBinding(
                ref=skill_ref,
                manifest_digest=record.definition.manifest_digest,
                mount_path=f"/skills/{record.definition.skill_name}/SKILL.md",
            )
        )
    prompt = goal.question
    model_settings = profile.model_policy.settings
    reasoning = model_settings.get("reasoning_effort", "medium")
    if reasoning not in {"minimal", "low", "medium", "high"}:
        raise RuntimeError("Agent Profile has an unsupported reasoning effort")
    max_turns = model_settings.get("max_turns", 12)
    if not isinstance(max_turns, int):
        raise RuntimeError("Agent Profile max_turns must be an integer")
    model = ModelPolicy(
        provider=profile.model_policy.provider,
        model=profile.model_policy.model,
        reasoning_effort=cast(
            Literal["minimal", "low", "medium", "high"],
            reasoning,
        ),
        max_turns=max_turns,
    )
    hosts = {
        requirement.host
        for server_ref in profile.mcp_server_refs
        for requirement in (
            _record_for_exact_ref(records, server_ref).definition.network_requirements  # type: ignore[union-attr]
        )
        if requirement.protocol != "stdio"
    } | {"upgradelabs.com", "daveasprey.com"}
    runtime_digest = sha256_digest(
        {"binding": runtime.binding, "required_capabilities": sorted(runtime.required_capabilities)}
    )
    operations: dict[str, OperationExecutionRequest] = {}
    for stage_id in ("search_firecrawl", "search_tavily", "browser_verify"):
        operations[stage_id] = OperationExecutionRequest(
            identity=OperationAttemptIdentity(
                run_id="{run_id}",
                operation_id=stage_id,
                operation_attempt=1,
            ),
            request_scope="{request_scope}",
            effective_configuration_digest="sha256:" + "0" * 64,
            run_control_revision=1,
            operation_contract_ref=f"operation:web-research:{stage_id}@1",
            prompt_segments=(
                PromptSegment(
                    source_ref="input:web-research-goal@1",
                    source_revision=1,
                    trust_class=PromptTrustClass.ADMITTED_INPUT,
                    content=prompt,
                    rendered_digest=sha256_digest(prompt),
                ),
            ),
            model_policy=model,
            mcp_servers=tuple(servers),
            skills=tuple(skills),
            agent_profile_ref=profile_record.ref,
            capability_grant=CapabilityGrant(
                capabilities=profile.maximum_capability_request.capabilities,
                mcp_server_ids=frozenset(ref.logical_id for ref in profile.mcp_server_refs),
                network_hosts=frozenset(hosts),
            ),
            workspace=WorkspaceContract(
                namespace_id="run/{run_id}/web-research",
                workspace_id="workspace:{run_id}:web-research",
                provider=runtime.binding,
                template_ref=workspace_ref,
                exclusive_write_paths=(
                    "/workspace/browser",
                    "/artifacts/browser-evidence",
                    "/outputs/web-research",
                ),
                network_policy="allowlisted",
                runtime_digest=runtime_digest,
                image_digest=runtime_digest,
                package_digest=runtime_digest,
                environment_digest=runtime_digest,
            ),
            secret_refs=tuple(
                ref
                for server_ref in sorted(
                    profile.mcp_server_refs,
                    key=lambda item: (item.logical_id, item.revision, item.digest),
                )
                for ref in (
                    _record_for_exact_ref(records, server_ref).definition.credential_refs  # type: ignore[union-attr]
                )
            ),
            budget_reservation_id=f"reservation:{{run_id}}:{stage_id}",
            budget_limits={"tool.calls.total": 10, "operation.attempts": 1},
            tracing_policy_ref="tracing:web-research-live@1",
            sensitive_data_policy_ref="policy:public-web-no-secrets@1",
            native_placement=NativeOperationExecutionPlacement.create(
                placement_id="native.web-research",
                revision=1,
                task_queue=operation_task_queue,
                qualification_refs=("QUAL-CP-TEMPORAL-REPLAY-RECOVERY",),
            ),
            snapshot_policy_ref="snapshot:browser-evidence@1",
            requested_at=created_at,
            idempotency_key=f"web-research:{{run_id}}:{stage_id}:1",
        )
    return SemanticOperationBindingTemplates(operations=operations)


def _record_for_exact_ref(
    records: dict[tuple[DefinitionKind, str], PublishedDefinition],
    ref: ExactDefinitionRef,
) -> PublishedDefinition:
    record = records.get((ref.kind, ref.logical_id))
    if record is None or record.ref != ref or sha256_digest(record.definition) != ref.digest:
        raise RuntimeError(
            "profile-derived exact catalog dependency is not the current "
            f"digest-verified head: {ref.kind.value}:{ref.logical_id}@{ref.revision}"
        )
    return record


async def _operation_binding_ids(
    repository: SemanticOperationBindingRepository,
    *,
    request_scope: str,
    run_id: str,
) -> tuple[str, ...]:
    refs = []
    for stage_id in ("search_firecrawl", "search_tavily", "browser_verify"):
        binding = await repository.get_binding_by_id(
            f"{run_id}:operation:{stage_id}:attempt:1",
            request_scope=request_scope,
        )
        if binding is None:
            raise RuntimeError(f"Scenario D OEB is unavailable after launch: {stage_id}")
        refs.append(binding.binding_id)
    return tuple(refs)


def _usage_summary(result: Any) -> dict[str, int]:
    usage: dict[str, int] = {}
    for item in result.lineage:
        for dimension, amount in item.actual_usage.items():
            usage[dimension] = usage.get(dimension, 0) + amount
    return usage


def _required_hit_ref(hit: CapabilitySearchHit) -> ExactDefinitionRef:
    if hit.exact_ref is None:
        raise RuntimeError("external candidates cannot authorize a live coordinator run")
    return hit.exact_ref


def _selected_ref(
    hits: tuple[CapabilitySearchHit, ...],
    kind: DefinitionKind,
    logical_id: str,
) -> ExactDefinitionRef:
    matches = [
        _required_hit_ref(hit)
        for hit in hits
        if hit.kind == kind and _required_hit_ref(hit).logical_id == logical_id
    ]
    if len(matches) != 1:
        raise RuntimeError(f"selected search evidence is unavailable: {kind.value}:{logical_id}")
    return matches[0]


def _safe_segment(value: str) -> str:
    normalized = "".join(
        character if character.isalnum() or character in "-_" else "-" for character in value
    ).strip("-")
    if not normalized:
        raise ValueError("artifact path identity is empty after normalization")
    return normalized[:120]
