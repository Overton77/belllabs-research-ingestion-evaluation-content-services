from __future__ import annotations

from dataclasses import dataclass

import asyncpg

from biotech_mission_adapters.application.schema.schema_catalog_build import (
    SchemaCatalogBuildService,
)
from biotech_mission_adapters.application.schema.schema_context_selection import (
    ReviewAgentPort,
    SelectionAgentPort,
)
from biotech_mission_adapters.application.schema.schema_context_stage_handlers import (
    register_schema_context_stage_handlers,
)
from biotech_mission_adapters.application.schema.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
)
from biotech_mission_adapters.application.schema.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from mission_control.adapters.postgres.orchestration.orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from mission_control.adapters.temporal.coordinator_runtime import (
    CoordinatorWorkerActivities,
    GoalDirectedCoordinatorDependencies,
    StageGraphCoordinatorDependencies,
    create_routed_coordinator_activities,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.application.programs.orchestration_binding_repository import (
    RunSemanticInputBindingService,
)
from mission_control.application.programs.orchestration_routing import (
    OperationExecutionBindingReader,
    SemanticHandlerRegistry,
)
from mission_control.application.programs.service import RunControlLifecycleGateway


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
