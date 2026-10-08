"""The deployment `WorkerActivityCompositionFactory` (RRM-009, CP-050 prerequisite).

`python -m mission_control.adapters.temporal.worker` composes this factory when
`COORDINATOR_LAUNCH_ENABLED` is
set. It wires, through the existing application ports and nothing else:

* the application PostgreSQL authority (run control, operation journal, checkpoint lineage
  and recovery, fork materialization, typed results) on the worker's runtime pool;
* PostgreSQL operation bindings, family documents and templates, scoped workspace
  manifests, artifact metadata and async-child detail;
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
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any

import asyncpg
import httpx
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from temporalio.client import Client

from mission_control.adapters.capabilities.capability_bundles import (
    BundleReader,
    DirectoryBundleStore,
)
from mission_control.adapters.capabilities.capability_pins import CapabilityPins
from mission_control.adapters.cursor import cursor_lane_stubs
from mission_control.adapters.cursor.bridge import SdkBridgeLauncher
from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.cloud_api import CloudAgentsClient
from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.cursor.local import CursorLocalHarness, CursorLocalSettings
from mission_control.adapters.cursor.projection import (
    CatalogRows,
    RenderedProjectionSource,
    hook_context_index,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.adapters.deep_agents import (
    DeepAgentRuntimeAdapter,
    DeepAgentsAsyncSubagentAdapter,
    DockerSandboxFactory,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    LangSmithSandboxFactory,
    OpenAIExactModelFactory,
    StateSandboxFactory,
)
from mission_control.adapters.deep_agents.async_subagents import BellLabsAsyncSubagentMiddleware
from mission_control.adapters.deep_agents.browser_tool import (
    AgentBrowserPageTool,
    granted_network_hosts,
)
from mission_control.adapters.deep_agents.checkpoint_verifier import (
    LangGraphCheckpointDescendantVerifier,
)
from mission_control.adapters.deep_agents.materializer import (
    ModelFactory,
    ResolvedSkillBundle,
    SandboxFactory,
)
from mission_control.adapters.deep_agents.persistence import StandalonePersistenceLifespan
from mission_control.adapters.operations.runtime_ports import (
    EnvironmentSecretResolver,
    FilesystemArtifactPayloadStore,
    PinnedCapabilityAssetVerifier,
    RecordedOperationEventSink,
)
from mission_control.adapters.postgres.async_subagents.async_subagent_detail_repository import (
    PostgresAsyncSubagentDetailRepository,
)
from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.capability_bundles import PostgresCapabilityBundleAdmissions
from mission_control.adapters.postgres.context.artifact_bytes import PostgresArtifactBytes
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.coordinator.workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import (
    PostgresLaneExecutionStateStore,
)
from mission_control.adapters.postgres.lanes.hook_tokens import (
    PostgresHookIntentLedger,
    PostgresHookTokenStore,
)
from mission_control.adapters.postgres.lanes.workspace_leases import PostgresWorkspaceLeaseStore
from mission_control.adapters.postgres.operations.operation_binding_repository import (
    PostgresOperationBindingRepository,
)
from mission_control.adapters.postgres.operations.operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.postgres.runtime.run_forks import PostgresForkMaterializationStore
from mission_control.adapters.postgres.workspace_candidate_contents import (
    PostgresWorkspaceCandidateContents,
)
from mission_control.adapters.postgres.workspaces.artifact_metadata_repository import (
    PostgresArtifactMetadataRepository,
)
from mission_control.adapters.postgres.workspaces.artifact_repository import (
    PostgresArtifactDurableReferenceRepository,
)
from mission_control.adapters.postgres.workspaces.workspace_manifest_repository import (
    PostgresWorkspaceManifestRepository,
)
from mission_control.adapters.storage.artifact_payloads import S3ArtifactPayloadStore
from mission_control.adapters.storage.context_files import PayloadContextFiles
from mission_control.adapters.storage.filesystem_workspace import FilesystemWorkspaceProvisioner
from mission_control.adapters.supabase_storage.bundles import configured_supabase_bundle_reader
from mission_control.adapters.temporal.artifact_activities import ArtifactPromotionActivities
from mission_control.adapters.temporal.coordinator_runtime import (
    GoalDirectedCoordinatorDependencies,
    StageGraphCoordinatorDependencies,
    create_routed_coordinator_activities,
)
from mission_control.adapters.temporal.operation_activities import OperationExecutionActivities
from mission_control.adapters.temporal.unit_reconciliation import TemporalUnitReconciliationNudge
from mission_control.adapters.temporal.worker import (
    WorkerActivityComposition,
    operation_heartbeat_policy,
)
from mission_control.application.artifacts.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
    ArtifactPromotionService,
    ArtifactValidationAuthorityPort,
    ScopedArtifactPromotionService,
    StaticArtifactValidationAuthority,
)
from mission_control.application.artifacts.workspace_candidates import (
    WorkspaceCandidateCaptureService,
)
from mission_control.application.artifacts.workspace_materialization import (
    BindingWorkspaceMaterializer,
    WorkspaceMaterializationService,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.coordinator.coordinator_results import (
    TerminalWorkflowCompletionService,
)
from mission_control.application.execution.harness.deep_agents_harness import DeepAgentsHarness
from mission_control.application.execution.harness.hook_callbacks import HookCallbackService
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.protocol import AgentHarness
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.operations.checkpoint_lineage import DEFAULT_CLAIM_LEASE
from mission_control.application.execution.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationExecutionService,
    RunControlOperationAuthority,
    RunControlOperationBudgetAuthority,
    SecretResolutionPort,
)
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalService,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.application.frames.reducer import FrameFactProjector
from mission_control.application.programs.orchestration_routing import SemanticHandlerRegistry
from mission_control.application.programs.service import (
    F1OrchestrationBindingVerifier,
    RunControlLifecycleGateway,
    orchestration_lifecycle_actor,
)
from mission_control.application.recovery.run_forks import ForkReuseResolver
from mission_control.application.subordinates.parent_completion import (
    AdmissionRule,
    AsyncChildCompletion,
    admit_typed_manifest,
)
from mission_control.application.subordinates.parent_effects import RunControlAsyncChildEffects
from mission_control.application.subordinates.service import AsyncSubagentService
from mission_control.bootstrap.operation_recovery_composition import (
    OperationRecoveryComposition,
    compose_postgres_operation_recovery,
)
from mission_control.bootstrap.settings import PROJECT_ROOT, Settings
from mission_control.domain.execution.contracts import (
    AsyncChildCancellationRecord,
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    DeepAgentMCPServerComponent,
    OperationExecutionBinding,
    RuntimeInvocation,
    RuntimeResult,
)
from mission_control.domain.policies.contracts import ActorContext

DEFAULT_ARTIFACT_PAYLOAD_ROOT = PROJECT_ROOT / ".artifact-payloads"
ASYNC_CHILD_COMPLETION_KIND = "async_child_completion.v1"
# The result admission policies this deployment decides at the parent boundary, by the
# contract's `result_admission_policy_ref`; a child of any other policy is left undecided.
DEFAULT_ASYNC_RESULT_POLICIES: Mapping[str, AdmissionRule] = {
    "policy:async-result:technical-child@1": admit_typed_manifest,
}
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
    # Exact MCP server components (by ref digest) the worker may launch besides the pinned
    # ones; compared by full equality before every launch (qualification harnesses only).
    mcp_servers: Mapping[str, DeepAgentMCPServerComponent] = field(default_factory=dict)


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
    bundle_reader: BundleReader | None = None,
) -> DeploymentCapabilityRegistry:
    """The exact component registry: pinned catalog plus the persistent saver and store."""

    if settings.capability_bundle_backend == "supabase" and bundle_reader is None:
        raise ValueError("Supabase skill pins require a PostgreSQL-admitted bundle reader")
    if bundle_reader is None and settings.capability_bundle_local_root is not None:
        bundle_reader = DirectoryBundleStore(settings.capability_bundle_local_root)
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
        skill.bundle_digest: skill.bundle(store=bundle_reader) for skill in pins.skills
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

    def service(
        self, binding: OperationExecutionBinding, resolved_secrets: Mapping[str, str]
    ) -> tuple[AsyncSubagentService, DeepAgentsAsyncSubagentAdapter]:
        """The governed service of one parent operation, over its scope-bound credential."""

        adapter = DeepAgentsAsyncSubagentAdapter(
            secrets=resolved_secrets, request_scope=binding.request_scope
        )
        service = AsyncSubagentService(
            PostgresAsyncSubagentDetailRepository(self._pool),
            PostgresAsyncSubagentAuthority(self._pool),
            adapter,
            parent_effects=RunControlAsyncChildEffects(self._run_control, actor=self._actor),
            allow_new_spawns=self._allow_new_spawns,
            submitter_identity=self._submitter_identity,
        )
        return service, adapter

    def middleware(
        self,
        binding: OperationExecutionBinding,
        contracts: tuple[AsyncSubagentContract, ...],
        resolved_secrets: Mapping[str, str],
    ) -> BellLabsAsyncSubagentMiddleware:
        service, adapter = self.service(binding, resolved_secrets)
        return BellLabsAsyncSubagentMiddleware(
            service=service,
            adapter=adapter,
            binding=binding,
            contracts=contracts,
            dependency_class=self._dependency_class,
        )


class ProductionAsyncChildCancellation:
    """RRM-008 step 4 in production: the operation boundary's `AsyncChildCancellationPort`.

    `OperationExecutionService.cancel_children` names only the parent binding. The provider
    adapter of that parent's children needs the operation's own scope-bound credential, so
    this port resolves the binding's `secret_refs` (the same resolver cognition uses), builds
    the parent's governed `AsyncSubagentService` exactly as the middleware does, and cancels
    every active child under its link policy. A parent that spawned no child resolves no
    secret and calls no provider.
    """

    def __init__(
        self,
        children: ProductionAsyncSubagentMiddlewareFactory,
        authority: PostgresAsyncSubagentAuthority,
        secrets: SecretResolutionPort,
    ) -> None:
        self._children = children
        self._authority = authority
        self._secrets = secrets

    async def cancel_children(
        self,
        binding: OperationExecutionBinding,
        *,
        reason: str,
        requested_at: datetime,
    ) -> tuple[AsyncChildCancellationRecord, ...]:
        if not await self._authority.list_child_ids(binding.request_scope, binding.binding_id):
            return ()
        resolved = await self._secrets.resolve(binding.secret_refs)
        service, _adapter = self._children.service(binding, resolved)
        return await service.cancel_children(binding, reason=reason, requested_at=requested_at)


def compose_lane_registry(
    settings: Settings,
    deep_agents: DeepAgentsHarness,
    *,
    cursor_local: AgentHarness | None = None,
    cursor_cloud: AgentHarness | None = None,
) -> LaneRegistry:
    """`deep_agents` always; the Cursor profiles when a Cursor credential is bound: the real
    `cursor_local` (FT-G3) and `cursor_cloud` (FT-G5) harnesses when composed, otherwise
    unqualified stubs. Unqualified lanes are admitted only under the application's local-proof
    policy (`MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES`)."""

    harnesses: list[AgentHarness] = [deep_agents]
    if settings.cursor_api_key is not None:
        local_stub, cloud_stub = cursor_lane_stubs()
        harnesses.append(cursor_local if cursor_local is not None else local_stub)
        harnesses.append(cursor_cloud if cursor_cloud is not None else cloud_stub)
    return LaneRegistry(harnesses, allow_unqualified=settings.allow_unqualified_lanes)


def compose_cursor_cloud(
    settings: Settings, pool: asyncpg.Pool, payloads: ArtifactPayloadPort
) -> tuple[CursorCloudHarness, CloudAgentsClient] | None:
    """The `cursor_cloud` lane (FT-G5) over the Cloud Agents API v1 when a Cursor credential
    is bound. Branches are published through the worker's git configuration; the cloud
    projection carries catalog command hooks only (the VM cannot reach the worker)."""

    if settings.cursor_api_key is None:
        return None
    files = PayloadContextFiles(payloads)
    root = settings.cursor_lease_root or (
        (settings.deep_agent_sandbox_workspace_root or DEFAULT_WORKSPACE_ROOT) / "cursor-leases"
    )
    client = CloudAgentsClient(settings.cursor_api_key)
    harness = CursorCloudHarness(
        client=client,
        publisher=GitBranchPublisher(root / "cloud-mirrors"),
        projections=RenderedProjectionSource(
            CatalogRows(
                PostgresDefinitionRepository(
                    pool, catalog_scope=settings.mission_control_catalog_scope or ""
                )
            ),
            kernel_hooks=(),
        ),
        artifacts=files,
        inputs=files,
    )
    return harness, client


def compose_cursor_local(
    settings: Settings, pool: asyncpg.Pool, payloads: ArtifactPayloadPort
) -> tuple[CursorLocalHarness, HookCallbackService] | None:
    """The `cursor_local` lane (FT-G3) and its Kernel Hook callback service, when a Cursor
    credential is bound. Leases, tokens, intents and frames persist in the common component;
    the packet and the patch artifacts go through the content-addressed payload store; the
    projection resolves the binding's exact catalog pins."""

    if settings.cursor_api_key is None:
        return None
    files = PayloadContextFiles(payloads)
    hooks = HookCallbackService(
        tokens=PostgresHookTokenStore(pool),
        intents=PostgresHookIntentLedger(pool),
        mapper=CursorHookMapper(),
        fences=KernelHookFenceGate(PostgresStopFenceRepository(pool)),
        frames=PostgresFrameRepository(pool),
        context_reader=hook_context_index,
    )
    lease_root = settings.cursor_lease_root or (
        (settings.deep_agent_sandbox_workspace_root or DEFAULT_WORKSPACE_ROOT) / "cursor-leases"
    )
    harness = CursorLocalHarness(
        launcher=SdkBridgeLauncher(settings.cursor_api_key),
        leaser=GitWorktreeLeaser(PostgresWorkspaceLeaseStore(pool), lease_root=lease_root),
        projections=RenderedProjectionSource(
            CatalogRows(
                PostgresDefinitionRepository(
                    pool, catalog_scope=settings.mission_control_catalog_scope or ""
                )
            )
        ),
        hooks=hooks,
        artifacts=files,
        inputs=files,
        settings=CursorLocalSettings(
            lease_root=lease_root,
            callback_base_url=f"http://127.0.0.1:{settings.mission_control_hook_callback_port}",
            default_repository=settings.cursor_local_repository,
        ),
    )
    return harness, hooks


class DeploymentOperationRuntime:
    """The deployment's operation runtime around the canonical Deep Agent adapter.

    * Constrained egress: the operation's granted `network_hosts` bound every governed
      browser page its cognition opens (`granted_network_hosts`).
    * After cognition, the parent-boundary completion of the async children it spawned
      (`AsyncChildCompletion`); the records travel with the runtime's event payloads into the
      settlement's digest-bound output payload.

    Every other runtime capability is the wrapped adapter's. RRM-008: a requested cancel
    reaches `execute` as `asyncio.CancelledError` (during cognition or during the children's
    completion wait) and is never caught here; the operation boundary decides whether it is a
    journaled cancel of the unit (`observe_latest` then reports the latest durable
    checkpoint, still inside the granted egress) or anything else (re-raised).
    """

    def __init__(
        self,
        runtime: DeepAgentRuntimeAdapter,
        children: ProductionAsyncSubagentMiddlewareFactory,
        *,
        pool: asyncpg.Pool,
        policies: Mapping[str, AdmissionRule],
        wait_seconds: float,
        launch_verifier: PinnedCapabilityAssetVerifier | None = None,
    ) -> None:
        self._runtime = runtime
        self._children = children
        self._pool = pool
        self._policies = dict(policies)
        self._wait_seconds = wait_seconds
        self._launch_verifier = launch_verifier

    def _verify_launch(self, invocation: RuntimeInvocation) -> None:
        # RRM-009 review: every path that materializes the agent (and so starts its stdio
        # MCP servers) verifies the launch against the pins first, including the
        # cancellation path's `observe_latest`.
        if self._launch_verifier is not None:
            self._launch_verifier.verify_launch(invocation.binding)

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        self._verify_launch(invocation)
        with granted_network_hosts(invocation.binding.capability_grant.network_hosts):
            result = await self._runtime.execute(invocation, resolved_secrets)
        deep = invocation.binding.deep_agent_binding
        if deep is None or not deep.async_subagents:
            return result
        service, _adapter = self._children.service(invocation.binding, resolved_secrets)
        records = await AsyncChildCompletion(
            service,
            PostgresAsyncSubagentAuthority(self._pool),
            policies=self._policies,
            wait_seconds=self._wait_seconds,
        ).complete(invocation.binding, execution_generation=deep.execution_generation)
        return result.model_copy(
            update={
                "event_payloads": (
                    *result.event_payloads,
                    {
                        "kind": ASYNC_CHILD_COMPLETION_KIND,
                        "binding_id": invocation.binding.binding_id,
                        "children": [record.as_payload() for record in records],
                    },
                )
            }
        )

    async def observe_latest(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        self._verify_launch(invocation)
        with granted_network_hosts(invocation.binding.capability_grant.network_hosts):
            return await self._runtime.observe_latest(invocation, resolved_secrets)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._runtime, name)


@dataclass(frozen=True)
class ProductionOperationComposition:
    """What the factory built for the operation boundary, kept for the worker's readiness."""

    service: OperationExecutionService
    recovery: OperationRecoveryComposition
    capabilities: DeploymentCapabilityRegistry
    saver: BaseCheckpointSaver[Any]
    payloads: ArtifactPayloadPort
    promotion: ScopedArtifactPromotionService
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
        async_result_policies: Mapping[str, AdmissionRule] | None = None,
    ) -> None:
        self._client = client
        self._async_result_policies = dict(
            async_result_policies
            if async_result_policies is not None
            else DEFAULT_ASYNC_RESULT_POLICIES
        )
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
            return await self._build(
                resources,
                settings=settings,
                control_plane=control_plane,
                run_control=run_control,
                postgres_pool=postgres_pool,
            )
        except BaseException:
            # RRM-009 review: a composition that fails partway closes what it opened.
            await resources.aclose()
            raise

    async def _build(
        self,
        resources: AsyncExitStack,
        *,
        settings: Settings,
        control_plane: ControlPlaneService,
        run_control: RunControlService,
        postgres_pool: asyncpg.Pool,
    ) -> WorkerActivityComposition:
        persistence = await resources.enter_async_context(
            StandalonePersistenceLifespan(
                settings.langgraph_checkpoint_dsn,
                run_setup=settings.langgraph_checkpoint_setup,
            )
        )
        pins = self._pins if self._pins is not None else CapabilityPins.from_settings(settings)
        bundle_reader = None
        if settings.capability_bundle_backend == "supabase":
            if (
                not settings.capability_bundle_namespace
                or not settings.mission_control_catalog_scope
            ):
                raise ValueError(
                    "Supabase skills require trusted storage namespace and catalog scope"
                )
            # Deployment-owned origin/credential only; never provided by a mission or skill.
            # Redirects are disabled so bearer credentials cannot leave the configured origin.
            storage_http = resources.enter_context(httpx.Client(timeout=30, follow_redirects=False))
            storage_reader = configured_supabase_bundle_reader(settings, http_client=storage_http)
            bundle_reader = await PostgresCapabilityBundleAdmissions(
                postgres_pool,
                catalog_scope=settings.mission_control_catalog_scope,
                storage_namespace=settings.capability_bundle_namespace,
            ).resolve_pins(pins, reader=storage_reader)
        capabilities = build_deployment_capability_registry(
            settings,
            pins,
            saver=persistence.saver,
            store=persistence.store,
            additional=self._additional,
            bundle_reader=bundle_reader,
        )
        actor = orchestration_lifecycle_actor()
        payloads = artifact_payload_store(settings)
        bindings = PostgresOperationBindingRepository(postgres_pool)

        def workspaces_for_scope(request_scope: str) -> WorkspaceMaterializationService:
            if not request_scope.strip():
                raise ValueError("workspace materialization requires request scope")
            # Scope is authority from the exact binding, never a caller-controlled path.
            scope_directory = sha256(request_scope.encode("utf-8")).hexdigest()
            return WorkspaceMaterializationService(
                manifests=PostgresWorkspaceManifestRepository(
                    postgres_pool, request_scope=request_scope
                ),
                provisioner=FilesystemWorkspaceProvisioner(
                    (settings.deep_agent_sandbox_workspace_root or DEFAULT_WORKSPACE_ROOT)
                    / scope_directory
                ),
                durable_inputs=_DurableInputsFromPayloads(payloads),
            )

        candidates = WorkspaceCandidateCaptureService(
            materializer_for_scope=workspaces_for_scope,
            contents_for_scope=lambda scope: PostgresWorkspaceCandidateContents(
                postgres_pool, payloads, request_scope=scope
            ),
        )

        def promotion_for_scope(request_scope: str) -> ArtifactPromotionService:
            return ArtifactPromotionService(
                bindings=bindings,
                metadata=PostgresArtifactMetadataRepository(
                    postgres_pool, request_scope=request_scope
                ),
                payloads=payloads,
                workspaces=workspaces_for_scope(request_scope),
                durable_references=PostgresArtifactDurableReferenceRepository(postgres_pool),
                validation_authority=self._artifact_validation
                or StaticArtifactValidationAuthority(
                    permission_outcomes={}, check_outcomes={}, required_check_ids={}
                ),
            )

        promotion = ScopedArtifactPromotionService(promotion_for_scope)
        recovery = compose_postgres_operation_recovery(
            postgres_pool,
            run_control=run_control,
            nudge=TemporalUnitReconciliationNudge(self._client),
            verifier=LangGraphCheckpointDescendantVerifier(capabilities.checkpointers),
            default_lease=self._claim_lease or DEFAULT_CLAIM_LEASE,
        )
        children = ProductionAsyncSubagentMiddlewareFactory(
            pool=postgres_pool,
            run_control=run_control,
            actor=actor,
            allow_new_spawns=settings.async_subagent_spawning_enabled,
            submitter_identity=settings.async_subagent_submitter_identity,
        )
        verifier = PinnedCapabilityAssetVerifier(
            pins,
            node_executable=settings.web_research_agent_browser_node,
            registered_mcp_servers=(
                self._additional.mcp_servers if self._additional is not None else None
            ),
        )
        # FT-B2 (ADR-0027): Context Packet files are content-addressed in the payload store;
        # the packer captures producer outputs from custody records and seals into PostgreSQL.
        context_files = PayloadContextFiles(payloads)
        context_packs = ContextPackService(
            artifacts=PostgresArtifactBytes(postgres_pool, payloads),
            selections=PostgresContextSelectionRepository(postgres_pool),
            staging=context_files,
        )
        adapter = DeploymentOperationRuntime(
            DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(capabilities.registry),
                async_subagents=children,
                workspace_outputs=candidates,
                # SPEC-03 (C1): provider frames persist before any derivation; (C2) their
                # closing facts become mission events through run control.
                frames=PostgresFrameRepository(postgres_pool),
                frame_facts=FrameFactProjector(
                    PostgresFrameRepository(postgres_pool), run_control, actor=actor
                ),
                context_inputs=context_files,
            ),
            children,
            pool=postgres_pool,
            policies=self._async_result_policies,
            wait_seconds=settings.async_subagent_completion_wait_seconds,
            launch_verifier=verifier,
        )
        secrets = EnvironmentSecretResolver()
        # FT-G3: the real `cursor_local` lane and its hook callbacks when Cursor is bound.
        cursor = compose_cursor_local(settings, postgres_pool, payloads)
        # FT-G5: the real `cursor_cloud` lane over the Cloud Agents API v1.
        cloud = compose_cursor_cloud(settings, postgres_pool, payloads)
        if cloud is not None:
            resources.push_async_callback(cloud[1].aclose)
        # FT-G1: the lane registry is built once per worker (SPEC-07 section 3).
        lanes = compose_lane_registry(
            settings,
            DeepAgentsHarness(adapter, secrets),
            cursor_local=cursor[0] if cursor is not None else None,
            cursor_cloud=cloud[0] if cloud is not None else None,
        )
        service = OperationExecutionService(
            lanes=lanes,
            # FT-F3: immediate-cancel Delivery Report milestones on the run's Stop Fence.
            stop_fences=PostgresStopFenceRepository(postgres_pool),
            authority=RunControlOperationAuthority(run_control, control_plane),
            bindings=bindings,
            runtime=adapter,
            sandbox=BindingWorkspaceMaterializer(service_for_scope=workspaces_for_scope),
            assets=verifier,
            mcp=verifier,
            secrets=secrets,
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
            # RRM-008 step 4: the cancellation saga cancels the unit's active async children
            # with the operation's own scope-bound credential.
            children=ProductionAsyncChildCancellation(
                children, PostgresAsyncSubagentAuthority(postgres_pool), secrets
            ),
        )
        # FT-G2: `lane.turn`, `lane.status`, `lane.cancel` on the cognitive queues. Session
        # Lanes persist frames through the FrameSink and lane state on harness_execution;
        # the Deep Agents lane runs its governed body through `lane.turn` unchanged.
        frame_store = PostgresFrameRepository(postgres_pool)
        lane_turns = LaneTurnService(
            lanes=lanes,
            boundary=service,
            frames=frame_store,
            states=PostgresLaneExecutionStateStore(postgres_pool),
            secrets=secrets,
            frame_facts=FrameFactProjector(frame_store, run_control, actor=actor),
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
        goal_documents = PostgresGoalDirectedDocumentRepository(postgres_pool)
        # RRM-008 composed: heartbeat timeout per operation class, and a worker drain shorter
        # than every one of them (refused before any worker is created).
        heartbeats = operation_heartbeat_policy(settings)
        heartbeats.verify_graceful_shutdown(settings.worker_graceful_shutdown_seconds)
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
                operation_heartbeats=heartbeats,
                context_packs=context_packs,
            ),
            stagegraph=StageGraphCoordinatorDependencies(
                run_control=run_control,
                repository=PostgresRunControlRepository(postgres_pool),
                operation_bindings=bindings,
                templates=PostgresStageGraphOperationTemplateRepository(postgres_pool),
                operation_heartbeats=heartbeats,
                context_packs=context_packs,
            ),
            completion=completion,
        )
        return WorkerActivityComposition(
            coordinator=coordinator,
            operation=OperationExecutionActivities(
                service, worker_identity=self._worker_identity, lane_turns=lane_turns
            ),
            artifacts=ArtifactPromotionActivities(service=promotion, candidates=candidates),
            resources=resources,
            hook_callbacks=cursor[1] if cursor is not None else None,
        )


class _DurableInputsFromPayloads:
    """Governed read-only workspace inputs come from the content-addressed payload store."""

    def __init__(self, payloads: ArtifactPayloadPort) -> None:
        self._payloads = payloads

    async def retrieve(self, durable_ref: str) -> bytes:
        object_ref, _, rest = durable_ref.partition("#")
        # FT-B2: the digest itself contains ':' (`sha256:<hex>`); the size is the last field.
        digest, _, size = rest.rpartition(":")
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
    "ASYNC_CHILD_COMPLETION_KIND",
    "DEFAULT_ASYNC_RESULT_POLICIES",
    "DeploymentCapabilityComponents",
    "DeploymentCapabilityRegistry",
    "DeploymentOperationRuntime",
    "ProductionAsyncChildCancellation",
    "ProductionAsyncSubagentMiddlewareFactory",
    "ProductionOperationComposition",
    "ProductionWorkerActivityCompositionFactory",
    "artifact_payload_store",
    "build_deployment_capability_registry",
]
