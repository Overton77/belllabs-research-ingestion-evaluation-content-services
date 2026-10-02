"""The deployment `WorkerActivityCompositionFactory` (RRM-009, CP-050 prerequisite).

`python -m app.temporal.worker` composes this factory when `COORDINATOR_LAUNCH_ENABLED` is
set. It wires, through the existing application ports and nothing else:

* the application PostgreSQL authority (run control, operation journal, checkpoint lineage
  and recovery, fork materialization, typed results) on the worker's runtime pool;
* MongoDB immutable definitions, operation bindings, family documents and templates,
  workspace manifests and async-child detail;
* the object artifact store (S3 when a bucket is configured, otherwise the shared
  content-addressed directory) for result payloads, artifact promotion and candidates;
* the persistent, registered LangGraph saver and store (REQ-CP-DA-004, DA-016), opened once
  per worker process and registered under the deployment's exact checkpointer and store
  digests;
* the exact capability registry from the deployment pins (models, Skills, MCP servers,
  host tools, sandboxes), the governed async-subagent middleware (RRM-013), the recovery
  composition (RRM-004), the fork reuse resolver (RRM-006), the boundary application
  services (RRM-007) and the journaled GoalDirected settlement (RRM-016);
* the canonical workflow and activity registries and task queues.

Nothing here is company- or fixture-specific. Deterministic qualification models are
registered through `additional_components`, exactly like any other exact model revision.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import asyncpg
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from temporalio.client import Client

from app.application.async_subagents.mongo_async_subagent_repository import (
    MongoAsyncSubagentDetailRepository,
)
from app.application.async_subagents.parent_effects import RunControlAsyncChildEffects
from app.application.async_subagents.postgres_async_subagents import PostgresAsyncSubagentAuthority
from app.application.async_subagents.service import AsyncSubagentService
from app.application.control_plane.service import ControlPlaneService
from app.application.coordinator.coordinator_results import TerminalWorkflowCompletionService
from app.application.coordinator.postgres_workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from app.application.operations.checkpoint_lineage import DEFAULT_CLAIM_LEASE
from app.application.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from app.application.operations.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operations.operation_execution import (
    OperationExecutionService,
    RunControlOperationAuthority,
    RunControlOperationBudgetAuthority,
)
from app.application.operations.operation_journal import OperationJournalService
from app.application.operations.operation_recovery_composition import (
    OperationRecoveryComposition,
    compose_postgres_operation_recovery,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.application.orchestration.mongo_stagegraph_repository import (
    MongoStageGraphOperationTemplateRepository,
)
from app.application.orchestration.orchestration_routing import SemanticHandlerRegistry
from app.application.orchestration.postgres_orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from app.application.orchestration.service import (
    F1OrchestrationBindingVerifier,
    RunControlLifecycleGateway,
    orchestration_lifecycle_actor,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.service import RunControlService
from app.application.runtime.postgres_run_forks import PostgresForkMaterializationStore
from app.application.runtime.run_forks import ForkReuseResolver
from app.application.workspaces.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
    ArtifactPromotionService,
    ArtifactValidationAuthorityPort,
    StaticArtifactValidationAuthority,
)
from app.application.workspaces.mongo_artifact_repository import MongoArtifactMetadataRepository
from app.application.workspaces.mongo_workspace_repository import MongoWorkspaceManifestRepository
from app.application.workspaces.postgres_artifact_repository import (
    PostgresArtifactDurableReferenceRepository,
)
from app.application.workspaces.workspace_candidates import WorkspaceCandidateCaptureService
from app.application.workspaces.workspace_materialization import (
    BindingWorkspaceMaterializer,
    WorkspaceMaterializationService,
)
from app.config import PROJECT_ROOT, Settings
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    OperationExecutionBinding,
)
from app.domain.run_control.contracts import ActorContext
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    DeepAgentsAsyncSubagentAdapter,
    DockerSandboxFactory,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    LangSmithSandboxFactory,
    OpenAIExactModelFactory,
    StateSandboxFactory,
)
from app.integrations.agents.deep_agents.async_subagents import BellLabsAsyncSubagentMiddleware
from app.integrations.agents.deep_agents.browser_tool import AgentBrowserPageTool
from app.integrations.agents.deep_agents.checkpoint_verifier import (
    LangGraphCheckpointDescendantVerifier,
)
from app.integrations.agents.deep_agents.materializer import (
    ModelFactory,
    ResolvedSkillBundle,
    SandboxFactory,
)
from app.integrations.artifact_payloads import S3ArtifactPayloadStore
from app.integrations.capability_pins import CapabilityPins
from app.integrations.filesystem_workspace import FilesystemWorkspaceProvisioner
from app.integrations.langgraph_persistence import StandalonePersistenceLifespan
from app.integrations.operation_runtime_ports import (
    EnvironmentSecretResolver,
    FilesystemArtifactPayloadStore,
    PinnedCapabilityAssetVerifier,
    RecordedOperationEventSink,
)
from app.integrations.temporal_unit_reconciliation import TemporalUnitReconciliationNudge
from app.integrations.workspace_candidate_contents import ObjectStoreWorkspaceCandidateContents
from app.temporal.artifact_activities import ArtifactPromotionActivities
from app.temporal.coordinator_runtime import (
    GoalDirectedCoordinatorDependencies,
    StageGraphCoordinatorDependencies,
    create_routed_coordinator_activities,
)
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.worker import WorkerActivityComposition

DEFAULT_ARTIFACT_PAYLOAD_ROOT = PROJECT_ROOT / ".artifact-payloads"
DEFAULT_WORKSPACE_ROOT = PROJECT_ROOT / ".workspaces"
GOAL_DIRECTED_WORKER_ACTOR = "goal-directed-worker"


@dataclass(frozen=True)
class DeploymentCapabilityComponents:
    """Exact components a deployment registers beside the pinned catalog.

    Keys are immutable definition digests (`ExactComponentRegistry` semantics). A
    qualification registers its deterministic model under the model digest it binds; a
    deployment registers nothing here unless it serves a component the pin file cannot
    describe (a structured output schema, a middleware instance).
    """

    model_factories: Mapping[str, ModelFactory] = field(default_factory=dict)
    prompts: Mapping[str, str] = field(default_factory=dict)
    tools: Mapping[str, BaseTool] = field(default_factory=dict)
    skill_bundles: Mapping[str, ResolvedSkillBundle] = field(default_factory=dict)
    sandbox_factories: Mapping[str, SandboxFactory] = field(default_factory=dict)
    structured_output_schemas: Mapping[str, type[Any] | dict[str, Any]] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class DeploymentCapabilityRegistry:
    registry: ExactComponentRegistry
    pins: CapabilityPins
    checkpointers: Mapping[str, BaseCheckpointSaver[Any]]

    def disclosure(self) -> dict[str, object]:
        return {
            **self.pins.disclosure(),
            "checkpointer_digests": sorted(self.checkpointers),
            "store_digests": sorted(self.registry.stores),
            "model_digests": sorted(self.registry.model_factories),
            "sandbox_digests": sorted(self.registry.sandbox_factories),
        }


def build_deployment_capability_registry(
    settings: Settings,
    pins: CapabilityPins,
    *,
    saver: BaseCheckpointSaver[Any],
    store: BaseStore,
    additional: DeploymentCapabilityComponents | None = None,
) -> DeploymentCapabilityRegistry:
    """The exact component registry: pinned catalog plus the persistent saver and store."""

    extra = additional or DeploymentCapabilityComponents()
    model_factories: dict[str, ModelFactory] = {
        model.ref.digest: OpenAIExactModelFactory() for model in pins.models
    }
    model_factories.update(extra.model_factories)
    sandbox_factories: dict[str, SandboxFactory] = {}
    for sandbox in pins.sandboxes:
        if sandbox.backend == "state":
            sandbox_factories[sandbox.ref.digest] = StateSandboxFactory()
        elif sandbox.backend == "docker":
            sandbox_factories[sandbox.ref.digest] = DockerSandboxFactory(
                workspace_root=settings.deep_agent_sandbox_workspace_root
                or DEFAULT_WORKSPACE_ROOT / "sandboxes"
            )
        else:
            sandbox_factories[sandbox.ref.digest] = LangSmithSandboxFactory()
    sandbox_factories.update(extra.sandbox_factories)
    tools: dict[str, BaseTool] = {}
    for tool in pins.tools:
        if settings.web_research_agent_browser_node is None:
            raise ValueError("pinned agent-browser tool requires WEB_RESEARCH_AGENT_BROWSER_NODE")
        tools[tool.ref.digest] = AgentBrowserPageTool(
            node_executable=settings.web_research_agent_browser_node,
            entrypoint=tool.verify_entrypoint(),
            command_timeout_seconds=settings.web_research_browser_command_timeout_seconds,
            max_output_bytes=settings.web_research_max_browser_output_bytes,
        )
    tools.update(extra.tools)
    skill_bundles: dict[str, ResolvedSkillBundle] = {
        skill.bundle_digest: skill.bundle() for skill in pins.skills
    }
    skill_bundles.update(extra.skill_bundles)
    checkpointers = {item.ref.digest: saver for item in pins.checkpointers}
    stores = {item.ref.digest: store for item in pins.stores}
    registry = ExactComponentRegistry(
        model_factories=model_factories,
        prompts=dict(extra.prompts),
        tools=tools,
        skill_bundles=skill_bundles,
        sandbox_factories=sandbox_factories,
        checkpointers=checkpointers,
        stores=stores,
        structured_output_schemas=dict(extra.structured_output_schemas),
    )
    return DeploymentCapabilityRegistry(registry=registry, pins=pins, checkpointers=checkpointers)


def artifact_payload_store(settings: Settings) -> ArtifactPayloadPort:
    if settings.s3_bucket:
        return S3ArtifactPayloadStore(settings, settings.s3_bucket)
    return FilesystemArtifactPayloadStore(
        settings.artifact_payload_root or DEFAULT_ARTIFACT_PAYLOAD_ROOT
    )


class ProductionAsyncSubagentMiddlewareFactory:
    """RRM-013 in production: one governed `AsyncSubagentService` per parent operation.

    The provider adapter is built from the operation's resolved secrets (the deployment
    credential reference the parent binding declares) and its request scope; reservations,
    links and the parent's effect claim go through the worker's run control before any
    provider call. New spawns are allowed only when the deployment opts in.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        run_control: RunControlService,
        actor: ActorContext,
        allow_new_spawns: bool,
        submitter_identity: str,
        dependency_class: AsyncSubagentDependencyClass = (
            AsyncSubagentDependencyClass.REQUIRED_BLOCKING
        ),
    ) -> None:
        self._pool = pool
        self._run_control = run_control
        self._actor = actor
        self._allow_new_spawns = allow_new_spawns
        self._submitter_identity = submitter_identity
        self._dependency_class = dependency_class

    def middleware(
        self,
        binding: OperationExecutionBinding,
        contracts: tuple[AsyncSubagentContract, ...],
        resolved_secrets: Mapping[str, str],
    ) -> BellLabsAsyncSubagentMiddleware:
        adapter = DeepAgentsAsyncSubagentAdapter(
            secrets=resolved_secrets, request_scope=binding.request_scope
        )
        service = AsyncSubagentService(
            MongoAsyncSubagentDetailRepository(),
            PostgresAsyncSubagentAuthority(self._pool),
            adapter,
            parent_effects=RunControlAsyncChildEffects(self._run_control, actor=self._actor),
            allow_new_spawns=self._allow_new_spawns,
            submitter_identity=self._submitter_identity,
        )
        return BellLabsAsyncSubagentMiddleware(
            service=service,
            adapter=adapter,
            binding=binding,
            contracts=contracts,
            dependency_class=self._dependency_class,
        )


@dataclass(frozen=True)
class ProductionOperationComposition:
    """What the factory built for the operation boundary, kept for the worker's readiness."""

    service: OperationExecutionService
    recovery: OperationRecoveryComposition
    capabilities: DeploymentCapabilityRegistry
    saver: BaseCheckpointSaver[Any]
    payloads: ArtifactPayloadPort
    promotion: ArtifactPromotionService
    candidates: WorkspaceCandidateCaptureService


class ProductionWorkerActivityCompositionFactory:
    def __init__(
        self,
        client: Client,
        *,
        pins: CapabilityPins | None = None,
        additional_components: DeploymentCapabilityComponents | None = None,
        artifact_validation: ArtifactValidationAuthorityPort | None = None,
        worker_identity: str | None = None,
        claim_lease: timedelta | None = None,
    ) -> None:
        self._client = client
        self._pins = pins
        self._additional = additional_components
        self._artifact_validation = artifact_validation
        self._worker_identity = worker_identity
        self._claim_lease = claim_lease
        self.operation: ProductionOperationComposition | None = None

    async def build(
        self,
        *,
        settings: Settings,
        control_plane: ControlPlaneService,
        run_control: RunControlService,
        postgres_pool: asyncpg.Pool,
    ) -> WorkerActivityComposition:
        resources = AsyncExitStack()
        try:
            persistence = await resources.enter_async_context(
                StandalonePersistenceLifespan(
                    settings.langgraph_checkpoint_dsn,
                    run_setup=settings.langgraph_checkpoint_setup,
                )
            )
        except BaseException:
            await resources.aclose()
            raise
        pins = self._pins if self._pins is not None else CapabilityPins.from_settings(settings)
        capabilities = build_deployment_capability_registry(
            settings,
            pins,
            saver=persistence.saver,
            store=persistence.store,
            additional=self._additional,
        )
        actor = orchestration_lifecycle_actor()
        payloads = artifact_payload_store(settings)
        bindings = MongoOperationBindingRepository()
        workspaces = WorkspaceMaterializationService(
            manifests=MongoWorkspaceManifestRepository(),
            provisioner=FilesystemWorkspaceProvisioner(
                settings.deep_agent_sandbox_workspace_root or DEFAULT_WORKSPACE_ROOT
            ),
            durable_inputs=_DurableInputsFromPayloads(payloads),
        )
        candidates = WorkspaceCandidateCaptureService(
            materializer=workspaces, contents=ObjectStoreWorkspaceCandidateContents(payloads)
        )
        promotion = ArtifactPromotionService(
            bindings=bindings,
            metadata=MongoArtifactMetadataRepository(),
            payloads=payloads,
            workspaces=workspaces,
            durable_references=PostgresArtifactDurableReferenceRepository(postgres_pool),
            validation_authority=self._artifact_validation
            or StaticArtifactValidationAuthority(
                permission_outcomes={}, check_outcomes={}, required_check_ids={}
            ),
        )
        recovery = compose_postgres_operation_recovery(
            postgres_pool,
            run_control=run_control,
            nudge=TemporalUnitReconciliationNudge(self._client),
            verifier=LangGraphCheckpointDescendantVerifier(capabilities.checkpointers),
            default_lease=self._claim_lease or DEFAULT_CLAIM_LEASE,
        )
        adapter = DeepAgentRuntimeAdapter(
            ExactDeepAgentMaterializer(capabilities.registry),
            async_subagents=ProductionAsyncSubagentMiddlewareFactory(
                pool=postgres_pool,
                run_control=run_control,
                actor=actor,
                allow_new_spawns=settings.async_subagent_spawning_enabled,
                submitter_identity=settings.async_subagent_submitter_identity,
            ),
            workspace_outputs=candidates,
        )
        verifier = PinnedCapabilityAssetVerifier(pins)
        service = OperationExecutionService(
            authority=RunControlOperationAuthority(run_control, control_plane),
            bindings=bindings,
            runtime=adapter,
            sandbox=BindingWorkspaceMaterializer(workspaces),
            assets=verifier,
            mcp=verifier,
            secrets=EnvironmentSecretResolver(),
            events=RecordedOperationEventSink(),
            budget=RunControlOperationBudgetAuthority(run_control, actor=actor),
            journal=JournaledOperationExecutionCoordinator(
                journal=OperationJournalService(
                    PostgresAtomicOperationJournalRepository(postgres_pool)
                ),
                run_control=run_control,
                results=payloads,
                actor=actor,
            ),
            journal_claimed_by=settings.operation_journal_claimed_by,
            lineage=recovery.lineage,
            fork_reuse=ForkReuseResolver(
                PostgresForkMaterializationStore(postgres_pool), results=payloads, bindings=bindings
            ),
        )
        self.operation = ProductionOperationComposition(
            service=service,
            recovery=recovery,
            capabilities=capabilities,
            saver=persistence.saver,
            payloads=payloads,
            promotion=promotion,
            candidates=candidates,
        )
        semantic_bindings = PostgresRunSemanticInputBindingRepository(postgres_pool)
        completion = TerminalWorkflowCompletionService(
            runs=run_control,
            results=PostgresWorkflowResultRepository(postgres_pool),
        )
        goal_documents = MongoGoalDirectedDocumentRepository()
        coordinator = create_routed_coordinator_activities(
            bindings=semantic_bindings,
            handlers=SemanticHandlerRegistry(),
            lifecycle=RunControlLifecycleGateway(
                run_control, F1OrchestrationBindingVerifier(control_plane), actor
            ),
            goal_directed=GoalDirectedCoordinatorDependencies(
                run_control=run_control,
                operation_bindings=bindings,
                templates=goal_documents,
                documents=goal_documents,
                actor=ActorContext(
                    actor_id=GOAL_DIRECTED_WORKER_ACTOR,
                    permissions=frozenset(
                        {"workflow_run.goal_directed", "workflow_run.reserve_budget"}
                    ),
                    authority_refs=frozenset({f"authority:{GOAL_DIRECTED_WORKER_ACTOR}"}),
                ),
            ),
            stagegraph=StageGraphCoordinatorDependencies(
                run_control=run_control,
                repository=PostgresRunControlRepository(postgres_pool),
                operation_bindings=bindings,
                templates=MongoStageGraphOperationTemplateRepository(),
            ),
            completion=completion,
        )
        return WorkerActivityComposition(
            coordinator=coordinator,
            operation=OperationExecutionActivities(service, worker_identity=self._worker_identity),
            artifacts=ArtifactPromotionActivities(service=promotion, candidates=candidates),
            resources=resources,
        )


class _DurableInputsFromPayloads:
    """Governed read-only workspace inputs come from the content-addressed payload store."""

    def __init__(self, payloads: ArtifactPayloadPort) -> None:
        self._payloads = payloads

    async def retrieve(self, durable_ref: str) -> bytes:
        object_ref, _, rest = durable_ref.partition("#")
        digest, _, size = rest.partition(":")
        if not digest or not size.isdigit():
            raise ValueError(
                "durable workspace input must be addressed as <object_ref>#<sha256>:<size>"
            )
        return await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=object_ref, content_digest=digest, size_bytes=int(size)
            )
        )


__all__ = [
    "DeploymentCapabilityComponents",
    "DeploymentCapabilityRegistry",
    "ProductionAsyncSubagentMiddlewareFactory",
    "ProductionOperationComposition",
    "ProductionWorkerActivityCompositionFactory",
    "artifact_payload_store",
    "build_deployment_capability_registry",
]
