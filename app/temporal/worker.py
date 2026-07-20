from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import docker
from agents.sandbox.sandboxes import DockerSandboxClient
from temporalio.contrib.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
    SandboxClientProvider,
)
from temporalio.worker import Worker

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
from app.config import get_settings
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.run_control.contracts import ActorContext
from app.integrations.control_plane_payloads import (
    S3PayloadStore,
    UnavailablePayloadStore,
)
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import create_application_postgres_pool
from app.integrations.temporal import create_temporal_client
from app.temporal.linked_run_activities import (
    DeferredLinkedResultAssessor,
    LinkedRunActivities,
    LinkedRunDecisionGateway,
    create_linked_run_worker,
)
from app.temporal.workflows import SandboxAgentProbeWorkflow


async def main() -> None:
    settings = get_settings()
    # PRE-EMPTIVE SETUP: Pydantic reads .env without mutating process env, while
    # the Agents SDK reads OPENAI_API_KEY from the process environment.
    os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key.get_secret_value())
    docker_client = DockerSandboxClient(docker.from_env())
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(start_to_close_timeout=timedelta(seconds=60)),
        sandbox_clients=[SandboxClientProvider("docker", docker_client)],
    )
    client = await create_temporal_client(settings, plugins=[plugin])
    probe_worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[SandboxAgentProbeWorkflow],
    )
    mongo_client, _database = await create_mongodb(settings)
    postgres_pool = await create_application_postgres_pool(settings)
    try:
        payload_store = (
            S3PayloadStore(settings, settings.s3_bucket)
            if settings.s3_bucket
            else UnavailablePayloadStore()
        )
        control_plane = ControlPlaneService(
            BeanieDefinitionRepository(),
            ExtensionRegistry(),
            payload_store,
            externalize_above_bytes=(
                256_000 if settings.s3_bucket else 15_000_000
            ),
        )
        run_control = RunControlService(
            PostgresRunControlRepository(postgres_pool),
            F1RunConfigurationVerifier(control_plane),
            AdmissionPolicyRegistry(),
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
        await asyncio.gather(probe_worker.run(), linked_worker.run())
    finally:
        await postgres_pool.close()
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
