from __future__ import annotations

import asyncio

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.linked_runs import LinkedRunService
from app.application.postgres_linked_run_repository import PostgresLinkedRunRepository
from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    RunControlService,
)
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_derivation import SchemaContextDerivationService
from app.application.schema_grounding_admission import (
    register_schema_grounding_admission_policies,
)
from app.application.schema_grounding_repository import (
    BeanieSchemaGroundingRecordRepository,
)
from app.application.schema_workspace_binding import SchemaGraphAdmissionService
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.application.web_research_admission import (
    register_web_research_admission_policies,
)
from app.config import get_settings
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.run_control.contracts import ActorContext
from app.domain.schema_grounding.definitions import register_schema_grounding_extensions
from app.integrations.control_plane_payloads import (
    S3PayloadStore,
    UnavailablePayloadStore,
)
from app.integrations.langsmith_tracing import configure_langsmith_tracing
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import create_application_postgres_pool
from app.integrations.schema_catalog_payloads import schema_catalog_payload_store
from app.integrations.schema_neo4j_executor import (
    Neo4jBoundedReadExecutorFactory,
)
from app.integrations.temporal import create_temporal_client
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
from app.temporal.schema_grounding_activities import (
    SchemaGroundingActivities,
    create_schema_grounding_activity_worker,
)


async def main(
    coordinator_activities: CoordinatorWorkerActivities | None = None,
) -> None:
    settings = get_settings()
    if settings.coordinator_launch_enabled and coordinator_activities is None:
        raise RuntimeError(
            "COORDINATOR_LAUNCH_ENABLED requires concrete StageGraph and "
            "GoalDirected activity adapters; refusing to advertise unusable workers"
        )
    configure_langsmith_tracing(settings)
    client = await create_temporal_client(settings)
    coordinator_workers = None
    if settings.coordinator_launch_enabled:
        assert coordinator_activities is not None
        coordinator_workers = create_coordinator_workers(
            client,
            task_queues=coordinator_task_queues(settings.temporal_task_queue),
            activities=coordinator_activities,
        )
    mongo_client, _database = await create_mongodb(settings)
    postgres_pool = await create_application_postgres_pool(settings)
    try:
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
        run_control = RunControlService(
            PostgresRunControlRepository(postgres_pool),
            F1RunConfigurationVerifier(control_plane),
            policies,
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
        await asyncio.gather(*(worker.run() for worker in workers))
    finally:
        await postgres_pool.close()
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
