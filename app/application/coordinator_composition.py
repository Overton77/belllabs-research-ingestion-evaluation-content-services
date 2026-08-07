from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from app.application.capability_search import CapabilitySearchService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.coordinator_facade import (
    BlueprintRuntimeStatus,
    CatalogAuthorizationPort,
    CatalogPayloadReader,
    CoordinatorAuditSink,
    CoordinatorFeatureFlags,
    CoordinatorLimits,
    InspectionReportReader,
    LaunchContextProvider,
    ProductionCoordinatorFacade,
    RunResourceReader,
    RuntimeReadinessPort,
)
from app.application.coordinator_launch import (
    AdmissionPort,
    AdmissionPreviewPort,
    BoundLaunchDispatcherPort,
    CompilerPort,
    CoordinatorLaunchPreparationService,
    CoordinatorWorkflowLaunchService,
    LaunchTicketRepository,
    RuntimePlanPreparer,
    RuntimePlanRequirement,
    SemanticBindingProvider,
    WorkflowSubmissionPort,
)
from app.application.coordinator_results import (
    CoordinatorResultService,
    RunProjectionPort,
)
from app.application.coordinator_run_resources import CoordinatorRunResourceService
from app.application.external_candidate_inspection import (
    ExternalCandidateInspectionService,
)
from app.application.external_candidate_repository import (
    BeanieExternalCandidateRepository,
)
from app.application.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
)
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
    RunSemanticInputBindingService,
)
from app.application.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
    PostgresPool,
)
from app.application.postgres_coordinator_audit_repository import (
    PostgresCoordinatorAuditSink,
)
from app.application.postgres_workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from app.config import Settings
from app.domain.control_plane.contracts import (
    DefinitionKind,
    DefinitionSelector,
    ExactDefinitionRef,
    PromptDefinition,
    SkillDefinition,
)
from app.domain.coordinator.launch import BlueprintFamily
from app.integrations.capability_embeddings import (
    OpenAICapabilityEmbeddingAdapter,
)
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.mcp_registry import (
    HttpxMCPRegistryRunner,
    MCPRegistryAdapter,
)
from app.integrations.npx_skills_discovery import (
    AsyncioSkillDiscoverySubprocessRunner,
    NpxSkillsDiscoveryAdapter,
)

_COORDINATOR_SKILL_ID = "skill.belllabs-workflow-coordinator"
_PROMPT_LOGICAL_IDS = {
    "propose_workflow": "prompt.coordinator.propose-workflow",
    "review_workflow_design": "prompt.coordinator.review-workflow-design",
    "explain_launch_blocker": "prompt.coordinator.explain-launch-blocker",
    "summarize_workflow_result": "prompt.coordinator.summarize-workflow-result",
}


class ReadOnlyCoordinatorRuntimeReadiness:
    async def snapshot(self) -> tuple[BlueprintRuntimeStatus, ...]:
        return tuple(
            BlueprintRuntimeStatus(
                family=family,
                executable=False,
                reason="coordinator is composed in read-only mode",
            )
            for family in (
                BlueprintFamily.STAGE_GRAPH,
                BlueprintFamily.GOAL_DIRECTED,
            )
        )


@dataclass(frozen=True)
class CoordinatorLaunchProductionInputs:
    """One exact provider graph shared by preparation and post-admission launch."""

    compiler: CompilerPort
    admission_preview: AdmissionPreviewPort
    admission: AdmissionPort
    tickets: LaunchTicketRepository
    dispatcher: BoundLaunchDispatcherPort
    submissions: WorkflowSubmissionPort
    semantic_bindings: SemanticBindingProvider
    runtime_plans: RuntimePlanPreparer
    binding_service: RunSemanticInputBindingService

    def build(
        self,
    ) -> tuple[CoordinatorLaunchPreparationService, CoordinatorWorkflowLaunchService]:
        preparation = CoordinatorLaunchPreparationService(
            compiler=self.compiler,
            admission=self.admission_preview,
            tickets=self.tickets,
            semantic_bindings=self.semantic_bindings,
            runtime_plans=self.runtime_plans,
            runtime_plan_requirement=RuntimePlanRequirement.REQUIRE_RUN_PLAN_V3,
        )
        launcher = CoordinatorWorkflowLaunchService(
            tickets=self.tickets,
            admission=self.admission,
            dispatcher=self.dispatcher,
            submissions=self.submissions,
            semantic_bindings=self.semantic_bindings,
            binding_service=self.binding_service,
            runtime_plan_requirement=RuntimePlanRequirement.REQUIRE_RUN_PLAN_V3,
        )
        return preparation, launcher


@dataclass(frozen=True)
class CoordinatorProductionDependencies:
    """Deployment-owned dependencies that cannot be safely inferred."""

    readiness: RuntimeReadinessPort
    coordinator_skill: DefinitionSelector
    prompt_bindings: Mapping[str, ExactDefinitionRef]
    inspections: ExternalCandidateInspectionService | None = None
    inspection_reports: InspectionReportReader | None = None
    launch: CoordinatorLaunchProductionInputs | None = None
    preparation: CoordinatorLaunchPreparationService | None = None
    launcher: CoordinatorWorkflowLaunchService | None = None
    results: CoordinatorResultService | None = None
    run_projections: RunProjectionPort | None = None
    launch_contexts: LaunchContextProvider | None = None
    run_resources: RunResourceReader | None = None
    run_bindings: RunSemanticInputBindingRepository | None = None
    payloads: CatalogPayloadReader | None = None
    audit: CoordinatorAuditSink | None = None
    catalog_authorization: CatalogAuthorizationPort | None = None


def build_production_coordinator_facade(
    *,
    settings: Settings,
    capability_postgres_pool: PostgresPool,
    application_postgres_pool: PostgresPool,
    dependencies: CoordinatorProductionDependencies,
    limits: CoordinatorLimits | None = None,
) -> ProductionCoordinatorFacade:
    """Wire real adapters after Mongo/Beanie and Postgres lifespans are active."""
    if dependencies.launch is not None and (
        dependencies.preparation is not None or dependencies.launcher is not None
    ):
        raise ValueError(
            "provide either coordinator launch production inputs or prebuilt launch "
            "services, not both"
        )
    if dependencies.results is not None and dependencies.run_projections is not None:
        raise ValueError(
            "provide either a prebuilt coordinator result service or a Run "
            "Projection port for production result composition, not both"
        )
    preparation = dependencies.preparation
    launcher = dependencies.launcher
    if dependencies.launch is not None:
        preparation, launcher = dependencies.launch.build()
    results = dependencies.results
    if results is None and dependencies.run_projections is not None:
        results = CoordinatorResultService(
            runs=dependencies.run_projections,
            results=PostgresWorkflowResultRepository(application_postgres_pool),
        )
    run_resources = dependencies.run_resources
    if (
        run_resources is None
        and dependencies.run_projections is not None
        and dependencies.run_bindings is not None
    ):
        run_resources = CoordinatorRunResourceService(
            runs=dependencies.run_projections,
            bindings=dependencies.run_bindings,
        )
    definitions = BeanieDefinitionRepository()
    catalog_index = PostgresCatalogSearchRepository(capability_postgres_pool)
    search = None
    if settings.capability_search_enabled:
        embeddings = OpenAICapabilityEmbeddingAdapter(settings)
        search = CapabilitySearchService(
            search=catalog_index,
            definitions=definitions,
            embeddings=embeddings,
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
    discovery = _discovery(settings)
    return ProductionCoordinatorFacade(
        definitions=definitions,
        catalog_index=catalog_index,
        search=search,
        readiness=dependencies.readiness,
        coordinator_skill=dependencies.coordinator_skill,
        prompt_bindings=dependencies.prompt_bindings,
        flags=CoordinatorFeatureFlags(
            capability_search_enabled=settings.capability_search_enabled,
            external_discovery_enabled=settings.external_capability_discovery_enabled,
            coordinator_launch_enabled=settings.coordinator_launch_enabled,
        ),
        audit=dependencies.audit or PostgresCoordinatorAuditSink(
            application_postgres_pool
        ),
        catalog_authorization=dependencies.catalog_authorization,
        discovery=discovery,
        inspections=dependencies.inspections,
        inspection_reports=dependencies.inspection_reports,
        preparation=preparation,
        launcher=launcher,
        results=results,
        launch_contexts=dependencies.launch_contexts,
        run_resources=run_resources,
        payloads=dependencies.payloads,
        limits=limits,
    )


async def load_coordinator_catalog_bindings(
    *,
    payloads_available: bool = False,
) -> tuple[DefinitionSelector, dict[str, ExactDefinitionRef]]:
    """Resolve exact current skill/prompt bindings before MCP registration."""

    refs = await list_published_definition_refs()
    current: dict[tuple[object, str], ExactDefinitionRef] = {}
    for ref in refs:
        key = (ref.kind, ref.logical_id)
        prior = current.get(key)
        if prior is None or ref.revision > prior.revision:
            current[key] = ref
    skill_ref = current.get((DefinitionKind.SKILL, _COORDINATOR_SKILL_ID))
    if skill_ref is None:
        raise RuntimeError("published BellLabs coordinator skill is unavailable")
    repository = BeanieDefinitionRepository()
    skill = await repository.get(skill_ref)
    if not isinstance(skill.definition, SkillDefinition):
        raise RuntimeError("coordinator skill binding resolved to an invalid definition")
    prompt_bindings: dict[str, ExactDefinitionRef] = {}
    for name, logical_id in _PROMPT_LOGICAL_IDS.items():
        prompt_ref = current.get((DefinitionKind.PROMPT, logical_id))
        if prompt_ref is not None:
            prompt = await repository.get(prompt_ref)
            definition = prompt.definition
            if not isinstance(definition, PromptDefinition):
                raise RuntimeError("coordinator prompt binding has an invalid definition")
            if definition.body is not None or (
                definition.payload_ref is not None and payloads_available
            ):
                prompt_bindings[name] = prompt_ref
    return DefinitionSelector(exact=skill_ref), prompt_bindings


def _discovery(
    settings: Settings,
) -> ExternalCapabilityDiscoveryService | None:
    if not settings.external_capability_discovery_enabled:
        return None
    candidates = BeanieExternalCandidateRepository()
    registry = MCPRegistryAdapter(
        HttpxMCPRegistryRunner(),
        base_url=settings.mcp_registry_base_url,
        api_version=settings.mcp_registry_api_version,
        timeout_seconds=settings.external_discovery_request_timeout_seconds,
        max_response_bytes=settings.external_discovery_max_output_bytes,
        max_pages=settings.external_discovery_max_pages,
        max_retries=settings.external_discovery_max_retries,
    )
    skills = NpxSkillsDiscoveryAdapter(
        AsyncioSkillDiscoverySubprocessRunner(),
        executable=settings.npx_skills_executable,
        package_version=settings.npx_skills_package_version,
        timeout_seconds=settings.external_discovery_command_timeout_seconds,
        max_output_bytes=settings.external_discovery_max_output_bytes,
    )
    return ExternalCapabilityDiscoveryService(
        enabled=True,
        mcp_registry=registry,
        skills=skills,
        candidates=candidates,
        max_results=settings.external_discovery_max_results,
    )
