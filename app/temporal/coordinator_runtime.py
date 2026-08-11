from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import asyncpg
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client
from temporalio.worker import Worker

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.goal_directed import (
    GoalDirectedDocumentRepository,
    GoalOperationTemplateProvider,
)
from app.application.orchestration import (
    RunControlLifecycleGateway,
    StageGraphDecisionService,
    StageGraphOperationPreparationService,
    StageGraphOperationTemplateProvider,
)
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
    RunSemanticInputBindingService,
)
from app.application.orchestration_routing import (
    OperationExecutionBindingReader,
    SemanticHandlerRegistry,
)
from app.application.postgres_orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from app.application.run_control import RunControlService
from app.application.run_control_repository import RunControlRepository
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_selection import ReviewAgentPort, SelectionAgentPort
from app.application.schema_context_stage_handlers import (
    register_schema_context_stage_handlers,
)
from app.application.schema_grounding_repository import SchemaGroundingRecordRepository
from app.application.semantic_operation_bindings import SemanticOperationBindingRepository
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.application.web_research_semantic_handlers import (
    WebResearchHandlerDependencies,
    register_web_research_stagegraph_handlers,
)
from app.domain.run_control.contracts import ActorContext
from app.integrations.control_plane_payloads import ContentAddressedPayloadStore
from app.temporal.activities.goal_directed import (
    GoalDirectedActivities,
    compose_goal_directed_activities,
    create_goal_directed_worker,
)
from app.temporal.orchestration_activities import (
    StageGraphActivities,
    create_stagegraph_worker,
)
from app.temporal.registration.task_queues import BellLabsTaskQueues


@dataclass(frozen=True)
class CoordinatorTaskQueues:
    stagegraph: str
    goal_directed: str

    def __post_init__(self) -> None:
        if not self.stagegraph or not self.goal_directed:
            raise ValueError("coordinator Temporal task queues must be non-empty")
        if self.stagegraph == self.goal_directed:
            raise ValueError(
                "StageGraph and GoalDirected require distinct task queues for readiness"
            )


@dataclass(frozen=True)
class CoordinatorWorkerActivities:
    stagegraph: StageGraphActivities
    goal_directed: GoalDirectedActivities

    @property
    def completion_configured(self) -> bool:
        return self.stagegraph.completion_configured and self.goal_directed.completion_configured


@dataclass(frozen=True)
class GoalDirectedCoordinatorDependencies:
    """Exact ports required to compose the canonical GoalDirected activity surface."""

    run_control: RunControlService
    operation_bindings: SemanticOperationBindingRepository
    templates: GoalOperationTemplateProvider
    documents: GoalDirectedDocumentRepository
    actor: ActorContext


@dataclass(frozen=True)
class StageGraphCoordinatorDependencies:
    """Exact authority and operation-template ports for canonical StageGraph activities."""

    run_control: RunControlService
    repository: RunControlRepository
    operation_bindings: SemanticOperationBindingRepository
    templates: StageGraphOperationTemplateProvider


@dataclass(frozen=True)
class SchemaGroundingCoordinatorRuntimeDependencies:
    """Concrete semantic services registered by the dual-family coordinator worker."""

    lifecycle: RunControlLifecycleGateway
    records: SchemaGroundingRecordRepository
    catalog_builds: SchemaCatalogBuildService
    sources: ContentAddressedPayloadStore
    catalog_payloads: ContentAddressedPayloadStore
    selector: SelectionAgentPort
    reviewer: ReviewAgentPort
    reconciliations: SupportingGraphReconciliationWorkflow
    goal_directed: GoalDirectedCoordinatorDependencies
    stagegraph: StageGraphCoordinatorDependencies
    operation_bindings: OperationExecutionBindingReader | None = None


@dataclass(frozen=True)
class SchemaGroundingCoordinatorRuntime:
    """Shared launch/worker composition for one authoritative PostgreSQL binding store."""

    activities: CoordinatorWorkerActivities
    bindings: PostgresRunSemanticInputBindingRepository
    binding_service: RunSemanticInputBindingService


def create_routed_coordinator_activities(
    *,
    bindings: RunSemanticInputBindingRepository,
    handlers: SemanticHandlerRegistry,
    lifecycle: RunControlLifecycleGateway,
    goal_directed: GoalDirectedCoordinatorDependencies,
    stagegraph: StageGraphCoordinatorDependencies,
    web_research: WebResearchHandlerDependencies | None = None,
    operation_bindings: OperationExecutionBindingReader | None = None,
    completion: TerminalWorkflowCompletionPort | None = None,
) -> CoordinatorWorkerActivities:
    """Compose production activity ports from durable bindings and exact handlers."""

    if web_research is not None:
        register_web_research_stagegraph_handlers(handlers, web_research)
    return CoordinatorWorkerActivities(
        stagegraph=StageGraphActivities(
            lifecycle_gateway=lifecycle,
            completion=completion,
            decision_service=StageGraphDecisionService(
                stagegraph.run_control,
                stagegraph.repository,
            ),
            operation_materializer=StageGraphOperationPreparationService(
                templates=stagegraph.templates,
                operation_bindings=stagegraph.operation_bindings,
            ),
        ),
        goal_directed=compose_goal_directed_activities(
            run_control=goal_directed.run_control,
            operation_bindings=goal_directed.operation_bindings,
            templates=goal_directed.templates,
            documents=goal_directed.documents,
            lifecycle=lifecycle,
            actor=goal_directed.actor,
            completion=completion,
        ),
    )


def create_schema_grounding_coordinator_activities(
    *,
    application_postgres_pool: asyncpg.Pool,
    dependencies: SchemaGroundingCoordinatorRuntimeDependencies,
) -> CoordinatorWorkerActivities:
    """Compose PostgreSQL routing with both production schema workflow families."""

    return create_schema_grounding_coordinator_runtime(
        application_postgres_pool=application_postgres_pool,
        dependencies=dependencies,
    ).activities


def create_schema_grounding_coordinator_runtime(
    *,
    application_postgres_pool: asyncpg.Pool,
    dependencies: SchemaGroundingCoordinatorRuntimeDependencies,
) -> SchemaGroundingCoordinatorRuntime:
    """Expose one shared durable binding authority to launch and worker paths."""

    bindings = PostgresRunSemanticInputBindingRepository(application_postgres_pool)
    binding_service = RunSemanticInputBindingService(bindings)
    handlers = SemanticHandlerRegistry()
    register_schema_context_stage_handlers(
        handlers,
        catalog_builds=dependencies.catalog_builds,
        sources=dependencies.sources,
        catalog_payloads=dependencies.catalog_payloads,
        records=dependencies.records,
        selector=dependencies.selector,
        reviewer=dependencies.reviewer,
    )
    return SchemaGroundingCoordinatorRuntime(
        activities=create_routed_coordinator_activities(
            bindings=bindings,
            handlers=handlers,
            lifecycle=dependencies.lifecycle,
            goal_directed=dependencies.goal_directed,
            stagegraph=dependencies.stagegraph,
            operation_bindings=dependencies.operation_bindings,
        ),
        bindings=bindings,
        binding_service=binding_service,
    )


@dataclass(frozen=True)
class CoordinatorWorkerSet:
    stagegraph: Worker
    goal_directed: Worker
    task_queues: CoordinatorTaskQueues

    @property
    def workers(self) -> tuple[Worker, Worker]:
        return (self.stagegraph, self.goal_directed)


@dataclass(frozen=True)
class TemporalFamilyReadiness:
    family: str
    task_queue: str
    workflow_registered: bool
    workflow_pollers: int

    @property
    def available(self) -> bool:
        return self.workflow_registered and self.workflow_pollers > 0


def coordinator_task_queues(base_task_queue: str) -> CoordinatorTaskQueues:
    if not base_task_queue:
        raise ValueError("base Temporal task queue must be non-empty")
    logical = BellLabsTaskQueues.from_base(base_task_queue)
    return CoordinatorTaskQueues(
        stagegraph=f"{logical.coordinator_family}-stagegraph",
        goal_directed=f"{logical.coordinator_family}-goal-directed",
    )


def create_coordinator_workers(
    client: Client,
    *,
    task_queues: CoordinatorTaskQueues,
    activities: CoordinatorWorkerActivities,
) -> CoordinatorWorkerSet:
    """Register both accepted coordinator workflow families with real activities."""

    if not activities.completion_configured:
        raise ValueError("coordinator workers require durable typed-result completion providers")
    return CoordinatorWorkerSet(
        stagegraph=create_stagegraph_worker(
            client,
            task_queue=task_queues.stagegraph,
            activities=activities.stagegraph,
        ),
        goal_directed=create_goal_directed_worker(
            client,
            task_queue=task_queues.goal_directed,
            activities=activities.goal_directed,
        ),
        task_queues=task_queues,
    )


async def coordinator_worker_readiness(
    client: Client,
    *,
    task_queues: CoordinatorTaskQueues,
    rpc_timeout: timedelta = timedelta(seconds=5),
) -> tuple[TemporalFamilyReadiness, TemporalFamilyReadiness]:
    """Report actual workflow pollers; configured queue names alone are not availability."""

    stagegraph_pollers = await _workflow_poller_count(
        client,
        task_queues.stagegraph,
        rpc_timeout=rpc_timeout,
    )
    goal_directed_pollers = await _workflow_poller_count(
        client,
        task_queues.goal_directed,
        rpc_timeout=rpc_timeout,
    )
    return (
        TemporalFamilyReadiness(
            family="StageGraph",
            task_queue=task_queues.stagegraph,
            workflow_registered=True,
            workflow_pollers=stagegraph_pollers,
        ),
        TemporalFamilyReadiness(
            family="GoalDirected",
            task_queue=task_queues.goal_directed,
            workflow_registered=True,
            workflow_pollers=goal_directed_pollers,
        ),
    )


async def _workflow_poller_count(
    client: Client,
    task_queue: str,
    *,
    rpc_timeout: timedelta,
) -> int:
    response = await client.workflow_service.describe_task_queue(
        DescribeTaskQueueRequest(
            namespace=client.namespace,
            task_queue=TaskQueue(name=task_queue),
            task_queue_type=TaskQueueType.Value("TASK_QUEUE_TYPE_WORKFLOW"),
        ),
        timeout=rpc_timeout,
    )
    return len(response.pollers)
