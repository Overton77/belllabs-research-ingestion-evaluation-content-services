from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Protocol

import asyncpg

from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.control_plane.service import ControlPlaneService
from app.application.orchestration.goal_directed import configure_goal_directed_family_admissions
from app.application.orchestration.linked_runs import LinkedRunService
from app.application.orchestration.postgres_linked_run_repository import PostgresLinkedRunRepository
from app.application.orchestration.service import register_stagegraph_family_mutations
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.run_control_repository import RunControlRepository
from app.application.run_control.schema_grounding_admission import (
    register_schema_grounding_admission_policies,
)
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    FamilyAdmissionRegistry,
    RunConfigurationVerifier,
    RunControlService,
)
from app.application.run_control.web_research_admission import (
    register_web_research_admission_policies,
)
from app.application.schema.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema.schema_context_derivation import SchemaContextDerivationService
from app.application.schema.schema_grounding_repository import (
    BeanieSchemaGroundingRecordRepository,
)
from app.application.schema.schema_workspace_binding import SchemaGraphAdmissionService
from app.application.schema.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.config import Settings, get_settings
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.run_control.contracts import ActorContext
from app.domain.schema_grounding.definitions import register_schema_grounding_extensions
from app.integrations.control_plane_payloads import (
    S3PayloadStore,
    UnavailablePayloadStore,
)
from app.integrations.langsmith_tracing import configure_langsmith_tracing
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import (
    create_application_family_writer_pool,
    create_application_postgres_pool,
)
from app.integrations.schema_catalog_payloads import schema_catalog_payload_store
from app.integrations.schema_neo4j_executor import (
    Neo4jBoundedReadExecutorFactory,
)
from app.integrations.temporal import create_temporal_client
from app.temporal.artifact_activities import (
    ArtifactPromotionActivities,
    create_generic_artifact_worker,
)
from app.temporal.coordinator_runtime import (
    CoordinatorWorkerActivities,
    coordinator_task_queues,
    create_coordinator_workers,
)
from app.temporal.linked_run_activities import (
    DeferredLinkedResultAssessor,
    LinkedRunActivities,
    LinkedRunDecisionGateway,
    create_linked_run_worker,
)
from app.temporal.operation_activities import (
    OperationExecutionActivities,
    create_agent_cognitive_worker,
)
from app.temporal.registration.task_queues import (
    BellLabsTaskQueues,
    generic_artifact_task_queue,
)
from app.temporal.schema_grounding_activities import (
    SchemaGroundingActivities,
    create_schema_grounding_activity_worker,
)
from app.temporal.search_attributes import verify_belllabs_search_attributes


@dataclass(frozen=True)
class WorkerActivityComposition:
    """Deployment-supplied, fully wired activity adapters.

    `artifacts` serves `GenericArtifactWorkflow` (candidate capture and governed promotion)
    on its own queue; `resources` holds provider lifespans the composition opened (the
    persistent LangGraph saver and store) and is closed when the workers stop.
    """

    coordinator: CoordinatorWorkerActivities
    operation: OperationExecutionActivities
    artifacts: ArtifactPromotionActivities | None = None
    resources: AsyncExitStack | None = None


class WorkerActivityCompositionFactory(Protocol):
    async def build(
        self,
        *,
        settings: Settings,
        control_plane: ControlPlaneService,
        run_control: RunControlService,
        postgres_pool: asyncpg.Pool,
    ) -> WorkerActivityComposition: ...


def compose_worker_run_control_service(
    repository: RunControlRepository,
    configuration_verifier: RunConfigurationVerifier,
    policies: AdmissionPolicyRegistry,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
) -> RunControlService:
    """Build worker run control with an optional exact family-policy registry."""

    if family_admission_registry is None:
        registry = FamilyAdmissionRegistry()
        configure_goal_directed_family_admissions(registry)
        register_stagegraph_family_mutations(registry)
    else:
        registry = family_admission_registry
    return RunControlService(
        repository,
        configuration_verifier,
        policies,
        registry,
    )


async def main(
    composition_factory: WorkerActivityCompositionFactory | None = None,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
) -> None:
    settings = get_settings()
    configure_langsmith_tracing(settings)
    client = await create_temporal_client(settings)
    if settings.coordinator_launch_enabled:
        if composition_factory is None:
            # RRM-009: the deployment composition is the repository's own; an explicit
            # factory is still accepted so a deployment can register extra exact components.
            from app.temporal.deployment_composition import (
                ProductionWorkerActivityCompositionFactory,
            )

            composition_factory = ProductionWorkerActivityCompositionFactory(client)
        # REQ-CP-EXEC-015: readiness verifies the namespace's Search Attributes and never
        # mutates it; `scripts/register_belllabs_search_attributes.py` is the admin step.
        await verify_belllabs_search_attributes(client, settings.temporal_namespace)
    mongo_client, _database = await create_mongodb(settings)
    postgres_pool = await create_application_postgres_pool(settings)
    family_writer_pool = None
    try:
        if settings.has_application_family_writer_postgres:
            family_writer_pool = await create_application_family_writer_pool(settings)
        control_plane_payload_store = (
            S3PayloadStore(settings, settings.s3_bucket)
            if settings.s3_bucket
            else UnavailablePayloadStore()
        )
        catalog_payload_store = schema_catalog_payload_store(settings)
        extensions = ExtensionRegistry()
        register_schema_grounding_extensions(extensions)
        control_plane = ControlPlaneService(
            BeanieDefinitionRepository(),
            extensions,
            control_plane_payload_store,
            externalize_above_bytes=(256_000 if settings.s3_bucket else 15_000_000),
        )
        policies = AdmissionPolicyRegistry()
        register_schema_grounding_admission_policies(policies)
        register_web_research_admission_policies(policies)
        run_control = compose_worker_run_control_service(
            PostgresRunControlRepository(
                postgres_pool,
                family_writer_pool=family_writer_pool,
            ),
            F1RunConfigurationVerifier(control_plane),
            policies,
            family_admission_registry,
        )
        coordinator_workers = None
        operation_worker = None
        artifact_worker = None
        composition: WorkerActivityComposition | None = None
        if settings.coordinator_launch_enabled:
            assert composition_factory is not None
            composition = await composition_factory.build(
                settings=settings,
                control_plane=control_plane,
                run_control=run_control,
                postgres_pool=postgres_pool,
            )
            coordinator_workers = create_coordinator_workers(
                client,
                task_queues=coordinator_task_queues(settings.temporal_task_queue),
                activities=composition.coordinator,
            )
            operation_worker = create_agent_cognitive_worker(
                client,
                task_queue=BellLabsTaskQueues.from_base(
                    settings.temporal_task_queue
                ).agent_cognitive,
                activities=composition.operation,
            )
            if composition.artifacts is not None:
                artifact_worker = create_generic_artifact_worker(
                    client,
                    task_queue=generic_artifact_task_queue(settings.temporal_task_queue),
                    operations=composition.operation,
                    artifacts=composition.artifacts,
                )
        linked_service = LinkedRunService(
            control_plane,
            run_control,
            PostgresLinkedRunRepository(postgres_pool),
        )
        linked_gateway = LinkedRunDecisionGateway(
            linked_service,
            DeferredLinkedResultAssessor(),
            actor=ActorContext(
                actor_id="linked-run-worker",
                permissions=frozenset({"workflow_run.admit_linked_result"}),
                authority_refs=frozenset({"authority:linked-run-worker"}),
            ),
            authority_ref="authority:linked-run-worker",
        )
        linked_worker = create_linked_run_worker(
            client,
            task_queue=f"{settings.temporal_task_queue}-linked-runs",
            activities=LinkedRunActivities(linked_gateway),
        )
        schema_records = BeanieSchemaGroundingRecordRepository()
        schema_admission = SchemaGraphAdmissionService(schema_records)
        schema_activities = SchemaGroundingActivities(
            catalog_builds=SchemaCatalogBuildService(
                schema_records,
                catalog_payload_store,
            ),
            derivations=SchemaContextDerivationService(schema_records),
            reconciliations=SupportingGraphReconciliationWorkflow(
                admission=schema_admission,
                executor_factory=Neo4jBoundedReadExecutorFactory(settings),
                records=schema_records,
            ),
        )
        schema_worker = create_schema_grounding_activity_worker(
            client,
            task_queue=f"{settings.temporal_task_queue}-schema-grounding",
            activities=schema_activities,
        )
        workers = [
            linked_worker,
            schema_worker,
        ]
        if coordinator_workers is not None:
            workers.extend(coordinator_workers.workers)
        if operation_worker is not None:
            workers.append(operation_worker)
        if artifact_worker is not None:
            workers.append(artifact_worker)
        try:
            await asyncio.gather(*(worker.run() for worker in workers))
        finally:
            if composition is not None and composition.resources is not None:
                await composition.resources.aclose()
    finally:
        if family_writer_pool is not None:
            await family_writer_pool.close()
        await postgres_pool.close()
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
