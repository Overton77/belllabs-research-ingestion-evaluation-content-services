"""One installation-bound PostgreSQL/Temporal worker; startup never writes schema."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import platform
import socket
import sys
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

import asyncpg
import uvicorn
from pydantic import SecretStr, ValidationError
from temporalio.client import Client
from temporalio.service import RPCError
from temporalio.worker import Worker

from mission_control.adapters.capabilities.capability_pins import (
    CapabilityPinError,
    CapabilityPins,
    workspace_root,
)
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
from mission_control.adapters.postgres.frames.retention import PostgresFrameRetention
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
from mission_control.adapters.temporal.activities.frames_expire import (
    FramesExpireActivities,
    ensure_frames_expire_schedule,
)
from mission_control.adapters.temporal.client import connect_temporal, resolve_temporal_connection
from mission_control.adapters.temporal.coordinator_runtime import coordinator_task_queues
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
    registered_lane_profiles,
)
from mission_control.adapters.temporal.linked_run_activities import (
    DeferredLinkedResultAssessor,
    LinkedResultAssessmentPort,
    LinkedRunActivities,
    LinkedRunDecisionGateway,
    create_linked_run_worker,
)
from mission_control.adapters.temporal.search_attributes import verify_belllabs_search_attributes
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
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
from mission_control.adapters.temporal.workflows.frames_expire import FramesExpireWorkflow
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.chains.relay import ChainRelayPump
from mission_control.application.execution.harness.hook_callbacks import HookCallbackService
from mission_control.application.execution.run_launch import TemporalClusterIdentity
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    FamilyAdmissionRegistry,
    RunControlService,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
    VerifiedApplicationIdentity,
    request_scope,
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
from mission_control.bootstrap.composition import (
    inspect_family_writer_installation,
    temporal_cluster_identity,
)
from mission_control.bootstrap.manifests import (
    compose_chain_relay_pump,
    compose_manifest_launch_inputs,
)
from mission_control.bootstrap.preflight import (
    PreflightIssue,
    check_lane_hosts,
    check_pins,
    check_registered_lane_hosts,
    load_profile,
)
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.hook_callback import create_hook_callback_app

_LOGGER = logging.getLogger(__name__)


def maintenance_task_queue(base: str) -> str:
    """The worker's maintenance queue (FT-C1 `mc.frames_expire.v1` retention runs)."""

    return f"{base}-maintenance"


def tenant_request_scopes(item: ApplicationDeployment) -> list[str]:
    """Every tenant scope the deployment grants for this application, in a stable order.

    Retention runs per tenant scope because row-level security binds one tenant per
    transaction (SPEC-03 retention)."""

    config = item.authentication
    binding = config.binding
    tenants = sorted({tenant for grant in config.grants for tenant in grant.tenant_ids}, key=str)
    return [
        request_scope(
            VerifiedApplicationIdentity(
                issuer=config.issuer,
                audiences=frozenset({config.audience}),
                application_id=binding.application_id,
                installation_id=binding.installation_id,
                tenant_id=tenant,
            )
        )
        for tenant in tenants
    ]


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


def verify_startup_readiness(settings: Settings) -> list[PreflightIssue]:
    """MP-22 delta 3: refuse to poll when a capability pin drifted or the local run profile
    names a lane this host cannot run (`LANE_UNSUPPORTED_OS`, `PIN_DRIFT`, ...).

    Without a profile the lanes the settings register are only reported (advisory): a Windows
    worker with a Cursor credential still serves `deep_agents` and `cursor_cloud`; the local
    CLI lanes run under WSL/Linux (OWNER-FIXTURE-RUNBOOK 2.8). Returns the advisory issues.
    """

    pins_label = str(settings.capability_pins_path)
    try:
        pins = CapabilityPins.load(settings.capability_pins_path)
    except (CapabilityPinError, ValidationError, OSError) as error:
        raise InstallationUnavailable(
            f"capability pins are unavailable at {pins_label}: {type(error).__name__}"
        ) from None
    root = settings.mission_control_preflight_workspace_root or workspace_root()
    issues: list[PreflightIssue] = list(check_pins(pins, root, pins_label))
    system = platform.system()
    profile_path = settings.mission_control_local_run_profile
    if profile_path is not None:
        profile, found = load_profile(profile_path)
        issues.extend(found)
        if profile is not None:
            issues.extend(check_lane_hosts(profile, system, profile_label=str(profile_path)))
    advisory = [issue for issue in issues if issue.severity != "blocking"]
    advisory.extend(check_registered_lane_hosts(registered_lane_profiles(settings), system))
    blocking = [issue for issue in issues if issue.severity == "blocking"]
    if blocking:
        raise InstallationUnavailable(
            "worker startup refused by readiness: "
            + "; ".join(f"{issue.code} at {issue.pointer}" for issue in blocking)
        )
    for issue in advisory:
        _LOGGER.warning("readiness %s at %s: %s", issue.code, issue.pointer, issue.message)
    return advisory


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
        # MP-22: pins and lane hosts are verified before any Temporal poller or provider
        # process exists; drift is a typed refusal, never a bare CapabilityPinError later.
        verify_startup_readiness(settings)
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
        # FT-C1: Native Event Store retention (`frames.expire`) on the maintenance queue.
        maintenance_queue = maintenance_task_queue(settings.temporal_task_queue)
        workers.append(
            Worker(
                client,
                task_queue=maintenance_queue,
                workflows=[FramesExpireWorkflow],
                activities=[
                    FramesExpireActivities(PostgresFrameRetention(prepared.runtime_pool)).expire
                ],
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
        if settings.mission_control_frames_expire_schedule:
            await ensure_retention_schedule(
                client,
                application_id=application_id,
                request_scopes=tenant_request_scopes(prepared.application),
                task_queue=maintenance_queue,
            )
        if settings.chain_relay_enabled:
            start_chain_relay(
                stack,
                prepared,
                settings=settings,
                client=client,
                run_control=runs,
                control_plane=catalog,
                cluster=temporal_cluster_identity(
                    target=connection.target,
                    address=temporal.address,
                    namespace=connection.namespace,
                    task_queue=temporal.root_task_queue,
                ),
            )
        await (stop or asyncio.Event()).wait()


def start_chain_relay(
    stack: AsyncExitStack,
    prepared: PreparedWorker,
    *,
    settings: Settings,
    client: Client,
    run_control: RunControlService,
    control_plane: ControlPlaneService,
    cluster: TemporalClusterIdentity | None = None,
) -> ChainRelayPump:
    """MP-02: deliver `mc.chain.start_run` intents through the governed launch while the
    worker runs; the pump stops with the worker (bounded by the graceful shutdown)."""

    temporal = prepared.application.temporal
    assert temporal is not None
    author = compose_manifest_launch_inputs(
        prepared.runtime_pool,
        settings=settings,
        run_control=run_control,
        control_plane=control_plane,
    )
    if author is None:
        raise InstallationUnavailable(
            "CHAIN_RELAY_ENABLED requires MANIFEST_LAUNCH_BINDINGS_PATH (the launch input author)"
        )
    binding = prepared.application.authentication.binding
    pump = compose_chain_relay_pump(
        prepared.runtime_pool,
        settings=settings,
        author=author,
        run_control=run_control,
        submitter=TemporalWorkflowSubmitter.for_production(
            client,
            root_task_queue=temporal.root_task_queue,
            stagegraph_task_queue=temporal.stagegraph_task_queue,
            goal_directed_task_queue=temporal.goal_directed_task_queue,
            search_attribute_policy="required",
            mission_installation_id=binding.installation_id,
            mission_application_id=binding.application_id,
        ),
        request_scopes=tenant_request_scopes(prepared.application),
        lease_owner=f"chain-relay:{socket.gethostname()}:{os.getpid()}",
        cluster=cluster,
    )
    halt = asyncio.Event()
    task = asyncio.create_task(pump.run(halt))

    async def stop_pump() -> None:
        halt.set()
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=max(settings.worker_graceful_shutdown_seconds, 1))

    stack.push_async_callback(stop_pump)
    return pump


async def ensure_retention_schedule(
    client: Client, *, application_id: str, request_scopes: list[str], task_queue: str
) -> str | None:
    """Create or update the per-application `frames.expire` Schedule (daily).

    Retention is housekeeping: a namespace that refuses Schedules leaves frames unexpired
    and is reported, but does not stop the worker from executing missions."""

    if not request_scopes:
        _LOGGER.warning("frames.expire schedule skipped: the deployment grants no tenant")
        return None
    try:
        return await ensure_frames_expire_schedule(
            client,
            application_id=application_id,
            request_scopes=request_scopes,
            task_queue=task_queue,
        )
    except RPCError as error:
        _LOGGER.warning(
            "frames.expire schedule was not created (%s); provider frames will not expire",
            error.status.name if error.status is not None else "unknown",
        )
        return None


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
