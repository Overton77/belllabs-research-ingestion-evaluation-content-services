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
from langgraph.checkpoint.postgres.base import BasePostgresSaver
from langgraph.store.postgres.base import BasePostgresStore
from pydantic import SecretStr
from temporalio.client import Client
from temporalio.worker import Worker

from mission_control.adapters.langsmith.tracing import configure_langsmith_tracing
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
from mission_control.adapters.temporal.worker import (
    WorkerActivityCompositionFactory,
    compose_worker_run_control_service,
    production_workers_or_close,
)
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.application.authoring.service import ControlPlaneService
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
    load_deployment,
    load_runtime_options,
    local_pool,
)
from mission_control.bootstrap.composition import (
    TRANSITIONAL_COMPONENT_VERSION,
    CompositionReadiness,
    inspect_family_writer_installation,
    inspect_transitional_installation,
)
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.policies.contracts import ActorContext

WORKER_MIGRATIONS = frozenset(
    {
        "0028_workspace_artifact_documents.sql",
        "0029_async_subagent_detail_documents.sql",
        "0031_workspace_payload_hardening.sql",
        "0032_async_subagent_detail_payload_identity.sql",
        "0035_workspace_candidate_descriptors.sql",
    }
)


@dataclass(frozen=True)
class PreparedWorker:
    application: ApplicationDeployment
    settings: Settings
    runtime_pool: asyncpg.Pool
    family_pool: asyncpg.Pool
    readiness: CompositionReadiness


def select_application(
    deployment: MissionDeployment,
    application_id: str,
    binding_digest: str,
) -> ApplicationDeployment:
    deployment = MissionDeployment.model_validate(deployment.model_dump(mode="python"))
    if deployment.storage_mode != "transitional_local":
        raise InstallationUnavailable("the common Mission Control component is not released")
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
    if binding.required_component_version != TRANSITIONAL_COMPONENT_VERSION:
        raise InstallationUnavailable("worker component compatibility mismatch")
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
    """Runtime recovery credentials cannot also mutate business authority tables."""
    secret = settings.langgraph_checkpoint_database_direct
    assert secret is not None
    try:
        connection = await asyncpg.connect(secret.get_secret_value(), timeout=10)
    except Exception:
        raise InstallationUnavailable("configured checkpoint connection is unavailable") from None
    try:
        row = await connection.fetchrow(
            """SELECT current_database() AS database_name, role.rolsuper, role.rolbypassrls,
               pg_has_role(current_user,'belllabs_control_runtime','member') AS runtime_member,
               pg_has_role(current_user,'belllabs_family_repository_writer','member')
                   AS family_member,
               has_table_privilege(current_user,'belllabs_control.workflow_runs',
                   'INSERT,UPDATE,DELETE') AS authority_writer,
               has_schema_privilege(current_user,namespace.oid,'USAGE') AS schema_usage
               FROM pg_roles role CROSS JOIN pg_namespace namespace
               WHERE role.rolname=current_user AND namespace.nspname=$1""",
            settings.langgraph_checkpoint_schema,
        )
        if (
            row is None
            or row["database_name"] != database_name
            or row["rolsuper"]
            or row["rolbypassrls"]
            or row["runtime_member"]
            or row["family_member"]
            or row["authority_writer"]
            or not row["schema_usage"]
        ):
            raise InstallationUnavailable(
                "checkpoint role must be restricted to its provisioned recovery schema"
            )
        try:
            async with connection.transaction():
                await connection.execute(
                    "SELECT set_config('search_path', $1, true)",
                    settings.langgraph_checkpoint_schema,
                )
                saver_versions = {
                    record["v"]
                    for record in await connection.fetch("SELECT v FROM checkpoint_migrations")
                }
                store_versions = {
                    record["v"]
                    for record in await connection.fetch("SELECT v FROM store_migrations")
                }
                if saver_versions != set(
                    range(len(BasePostgresSaver.MIGRATIONS))
                ) or store_versions != set(range(len(BasePostgresStore.MIGRATIONS))):
                    raise InstallationUnavailable(
                        "checkpoint schema differs from the pinned SDK release"
                    )
        except (asyncpg.UndefinedTableError, asyncpg.InsufficientPrivilegeError) as error:
            raise InstallationUnavailable(
                "checkpoint schema is not provisioned for this runtime"
            ) from error
    finally:
        await connection.close()


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
        pool = await local_pool(binding.database_secret_ref)
        stack.push_async_callback(pool.close)
        readiness = await inspect_transitional_installation(pool, binding)
        if not WORKER_MIGRATIONS <= readiness.applied_migrations:
            raise InstallationUnavailable("worker persistence migrations are incomplete")
        family = await local_pool(item.family_writer_secret_ref)
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
        client = await Client.connect(temporal.address, namespace=temporal.namespace)
        await verify_belllabs_search_attributes(client, temporal.namespace)
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
        production = await production_workers_or_close(client, settings, composition)
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
            )
        )
        for worker in workers:
            await stack.enter_async_context(worker)
        await (stop or asyncio.Event()).wait()


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
