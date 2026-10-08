"""One installation-bound PostgreSQL/Temporal worker; startup never writes schema."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

import asyncpg
import uvicorn
from pydantic import SecretStr
from temporalio.worker import Worker

from mission_control.adapters.deep_agents.persistence import RuntimeConninfoError
from mission_control.adapters.deep_agents.runtime_persistence_verifier import (
    RuntimePersistenceUnavailable,
    verify_runtime_persistence,
)
from mission_control.adapters.langsmith.tracing import configure_langsmith_tracing
from mission_control.adapters.postgres.chains.store import install_chain_release_hook
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.orchestration.linked_run_repository import (
    PostgresLinkedRunRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.storage.control_plane_payloads import (
    S3PayloadStore,
    UnavailablePayloadStore,
)
from mission_control.adapters.temporal.client import connect_temporal, resolve_temporal_connection
from mission_control.adapters.temporal.coordinator_runtime import coordinator_task_queues
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.linked_run_activities import (
    DeferredLinkedResultAssessor,
    LinkedResultAssessmentPort,
    LinkedRunActivities,
    LinkedRunDecisionGateway,
    create_linked_run_worker,
)
from mission_control.adapters.temporal.search_attributes import verify_belllabs_search_attributes
from mission_control.adapters.temporal.versioning import (
    promote_worker_deployment_version,
    worker_deployment_config,
)
from mission_control.adapters.temporal.worker import (
    WorkerActivityCompositionFactory,
    compose_worker_run_control_service,
    production_workers_or_close,
)
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.execution.harness.hook_callbacks import HookCallbackService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    FamilyAdmissionRegistry,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
)
from mission_control.application.programs.linked_runs import LinkedRunService
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    RuntimeOptions,
    application_pool,
    load_deployment,
    load_runtime_options,
)
from mission_control.bootstrap.common_installation import (
    CommonReadiness,
    inspect_common_installation,
)
from mission_control.bootstrap.composition import inspect_family_writer_installation
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.hook_callback import create_hook_callback_app


@dataclass(frozen=True)
class PreparedWorker:
    application: ApplicationDeployment
    settings: Settings
    runtime_pool: asyncpg.Pool
    family_pool: asyncpg.Pool
    readiness: CommonReadiness


def select_application(
    deployment: MissionDeployment,
    application_id: str,
    binding_digest: str,
) -> ApplicationDeployment:
    deployment = MissionDeployment.model_validate(deployment.model_dump(mode="python"))
    if deployment.storage_mode != "production_common":
        raise InstallationUnavailable("only the common Mission Control component is supported")
    matches = [
        item
        for item in deployment.applications
        if item.authentication.binding.application_id == application_id
    ]
    if len(matches) != 1:
        raise InstallationUnavailable("worker requires one explicitly selected application")
    item = matches[0]
    binding = ApplicationBinding.model_validate(
        item.authentication.binding.model_dump(mode="python")
    )
    if binding.binding_digest != binding_digest:
        raise InstallationUnavailable("worker application binding pin mismatch")
    if item.temporal is None or item.family_writer_secret_ref is None:
        raise InstallationUnavailable(
            "worker requires explicit Temporal and family writer bindings"
        )
    selected_queues = {
        item.temporal.root_task_queue,
        item.temporal.stagegraph_task_queue,
        item.temporal.goal_directed_task_queue,
    }
    for other in deployment.applications:
        if other is item or other.temporal is None:
            continue
        if (
            other.temporal.address == item.temporal.address
            and other.temporal.namespace == item.temporal.namespace
            and selected_queues
            & {
                other.temporal.root_task_queue,
                other.temporal.stagegraph_task_queue,
                other.temporal.goal_directed_task_queue,
            }
        ):
            raise InstallationUnavailable("application worker queues overlap another installation")
    return item


def _settings_for_application(settings: Settings, item: ApplicationDeployment) -> Settings:
    binding = item.authentication.binding
    assert item.temporal is not None
    if not settings.coordinator_launch_enabled:
        raise InstallationUnavailable("COORDINATOR_LAUNCH_ENABLED must explicitly enable execution")
    suffix = "-coordinator-family-stagegraph"
    if not item.temporal.stagegraph_task_queue.endswith(suffix):
        raise InstallationUnavailable(
            "stagegraph queue must use the qualified family queue convention"
        )
    base = item.temporal.stagegraph_task_queue.removesuffix(suffix)
    queues = coordinator_task_queues(base)
    if item.temporal.goal_directed_task_queue != queues.goal_directed:
        raise InstallationUnavailable(
            "worker family task queues do not share the configured application base"
        )
    checkpoint = settings.langgraph_checkpoint_database_direct
    if checkpoint is None:
        raise InstallationUnavailable(
            "an explicit LangGraph checkpoint database binding is required"
        )
    if settings.langgraph_checkpoint_setup:
        raise InstallationUnavailable(
            "checkpoint schema setup is an operator step, never worker startup"
        )
    runtime_dsn = os.environ.get(binding.database_secret_ref)
    if not runtime_dsn:
        raise InstallationUnavailable("configured runtime database reference is unavailable")
    source, target = urlsplit(runtime_dsn), urlsplit(checkpoint.get_secret_value())
    if (source.hostname, source.port or 5432, source.path) != (
        target.hostname,
        target.port or 5432,
        target.path,
    ):
        raise InstallationUnavailable(
            "checkpoint persistence must use the selected installation database"
        )
    return settings.model_copy(
        update={
            "application_database_direct": SecretStr(runtime_dsn),
            "application_database_url": None,
            "mission_control_catalog_scope": (
                f"mc/{binding.installation_id}/{binding.application_id}/catalog"
            ),
            "temporal_address": item.temporal.address,
            "temporal_namespace": item.temporal.namespace,
            "temporal_task_queue": base,
        }
    )


async def inspect_checkpoint_binding(settings: Settings, database_name: str) -> None:
    """The checkpoint login serves only the provisioned private saver/store schema.

    Read-only: verifies the restricted `mission_control_checkpointer` identity, schema
    privileges, pinned tables/columns/ledgers, valid indexes and search_path shadowing
    through the exact saver conninfo. Missing or invalid persistence fails closed.
    """
    try:
        conninfo = settings.langgraph_checkpoint_dsn
    except RuntimeConninfoError as error:
        raise InstallationUnavailable(f"checkpoint binding rejected: {error}") from None
    try:
        await verify_runtime_persistence(
            conninfo,
            expected_database=database_name,
            schema=settings.langgraph_checkpoint_schema,
        )
    except RuntimePersistenceUnavailable as error:
        raise InstallationUnavailable(
            f"checkpoint runtime persistence is not ready: {error}"
        ) from None


@asynccontextmanager
async def prepare_worker(
    deployment: MissionDeployment,
    *,
    application_id: str,
    binding_digest: str,
    settings: Settings,
) -> AsyncIterator[PreparedWorker]:
    """Read actual installation evidence before any Temporal/provider activity starts."""
    item = select_application(deployment, application_id, binding_digest)
    configured = _settings_for_application(settings, item)
    binding = item.authentication.binding
    assert item.family_writer_secret_ref is not None
    async with AsyncExitStack() as stack:
        pool = await application_pool(binding.database_secret_ref)
        stack.push_async_callback(pool.close)
        readiness = await inspect_common_installation(pool, binding)
        family = await application_pool(item.family_writer_secret_ref)
        stack.push_async_callback(family.close)
        await inspect_family_writer_installation(family, readiness.database_name, binding)
        await inspect_checkpoint_binding(configured, readiness.database_name)
        yield PreparedWorker(item, configured, pool, family, readiness)


async def run_worker(
    deployment: MissionDeployment,
    *,
    application_id: str,
    binding_digest: str,
    settings: Settings,
    composition_factory: WorkerActivityCompositionFactory | None = None,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
    linked_assessor: LinkedResultAssessmentPort | None = None,
    runtime_options: RuntimeOptions | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    async with (
        prepare_worker(
            deployment,
            application_id=application_id,
            binding_digest=binding_digest,
            settings=settings,
        ) as prepared,
        AsyncExitStack() as stack,
    ):
        settings = prepared.settings
        configure_langsmith_tracing(settings)
        temporal = prepared.application.temporal
        assert temporal is not None
        connection = resolve_temporal_connection(
            settings, address=temporal.address, namespace=temporal.namespace
        )
        client = await connect_temporal(connection)
        await verify_belllabs_search_attributes(client, connection.namespace)
        # FT-G7: every worker of this release polls as one Worker Deployment Version.
        deployment_config = worker_deployment_config(settings)
        configured_options = load_runtime_options(deployment) if runtime_options is None else None
        options = runtime_options or (configured_options or {}).get(application_id)
        payloads = (
            options.payload_store
            if options is not None
            else S3PayloadStore(settings, settings.s3_bucket)
            if settings.s3_bucket
            else UnavailablePayloadStore()
        )
        catalog = ControlPlaneService(
            PostgresDefinitionRepository(
                prepared.runtime_pool, catalog_scope=settings.mission_control_catalog_scope or ""
            ),
            options.extensions if options is not None else ExtensionRegistry(),
            payloads,
            externalize_above_bytes=256_000 if settings.s3_bucket else 15_000_000,
        )
        # FT-D2: family commits (terminalization, evidence) release chain links in-transaction.
        install_chain_release_hook()
        runs = compose_worker_run_control_service(
            PostgresRunControlRepository(
                prepared.runtime_pool, family_writer_pool=prepared.family_pool
            ),
            F1RunConfigurationVerifier(catalog),
            options.admission_policies if options is not None else AdmissionPolicyRegistry(),
            family_admission_registry
            if family_admission_registry is not None
            else options.family_admissions
            if options is not None
            else None,
        )
        factory = composition_factory or ProductionWorkerActivityCompositionFactory(client)
        composition = await factory.build(
            settings=settings,
            control_plane=catalog,
            run_control=runs,
            postgres_pool=prepared.runtime_pool,
        )
        if composition.resources is not None:
            stack.push_async_callback(composition.resources.aclose)
        if composition.hook_callbacks is not None:
            # FT-G3: the Kernel Hook callback listener, loopback only, lives with the worker.
            await start_hook_callback_listener(
                stack, composition.hook_callbacks, port=settings.mission_control_hook_callback_port
            )
        production = await production_workers_or_close(
            client, settings, composition, deployment_config=deployment_config
        )
        workers = list(production.workers)
        if temporal.root_task_queue not in {
            temporal.stagegraph_task_queue,
            temporal.goal_directed_task_queue,
        }:
            workers.append(
                Worker(
                    client,
                    task_queue=temporal.root_task_queue,
                    workflows=[MissionRunWorkflow],
                    workflow_runner=coordinator_workflow_runner(),
                    deployment_config=deployment_config,
                )
            )
        linked = LinkedRunDecisionGateway(
            LinkedRunService(catalog, runs, PostgresLinkedRunRepository(prepared.runtime_pool)),
            linked_assessor or DeferredLinkedResultAssessor(),
            actor=ActorContext(
                actor_id="linked-run-worker",
                permissions=frozenset({"workflow_run.admit_linked_result"}),
                authority_refs=frozenset({"authority:linked-run-worker"}),
            ),
            authority_ref="authority:linked-run-worker",
        )
        workers.append(
            create_linked_run_worker(
                client,
                task_queue=f"{settings.temporal_task_queue}-linked-runs",
                activities=LinkedRunActivities(linked),
                deployment_config=deployment_config,
            )
        )
        for worker in workers:
            await stack.enter_async_context(worker)
        if deployment_config is not None and settings.temporal_promote_on_start:
            # A versioned worker receives new executions only once its version is current.
            await promote_worker_deployment_version(
                client, connection.namespace, deployment_config.version
            )
        await (stop or asyncio.Event()).wait()


async def start_hook_callback_listener(
    stack: AsyncExitStack, service: HookCallbackService, *, port: int
) -> uvicorn.Server:
    """Serve `POST /v1/applications/{app}/internal/hook-callback` on 127.0.0.1 only; the
    server stops with the worker (SPEC-07 section 5.5)."""

    server = uvicorn.Server(
        uvicorn.Config(
            create_hook_callback_app(service),
            host="127.0.0.1",
            port=port,
            log_level="warning",
            lifespan="off",
        )
    )
    serving = asyncio.create_task(server.serve())

    async def stop() -> None:
        server.should_exit = True
        await serving

    stack.push_async_callback(stop)
    for _ in range(200):
        if server.started or serving.done():
            break
        await asyncio.sleep(0.05)
    if serving.done():
        serving.result()
    return server


async def main(
    composition_factory: WorkerActivityCompositionFactory | None = None,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
) -> None:
    application_id = os.environ.get("MISSION_CONTROL_WORKER_APPLICATION_ID", "")
    binding_digest = os.environ.get("MISSION_CONTROL_WORKER_BINDING_DIGEST", "")
    if not application_id or not binding_digest:
        raise InstallationUnavailable(
            "worker application selection and binding digest must be explicit"
        )
    await run_worker(
        load_deployment(),
        application_id=application_id,
        binding_digest=binding_digest,
        settings=get_settings(),
        composition_factory=composition_factory,
        family_admission_registry=family_admission_registry,
    )


def run_cli() -> None:
    """Psycopg's asynchronous adapter requires a selector loop on Windows."""
    asyncio.run(
        main(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )


if __name__ == "__main__":
    run_cli()
