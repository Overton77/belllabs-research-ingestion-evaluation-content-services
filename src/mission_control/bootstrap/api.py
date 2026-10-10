"""Configured scoped API: ``uvicorn mission_control.bootstrap.api:create_app --factory``.

This composition reads deployed identity and migrations; it never creates or upgrades
schemas. The transitional local mode is explicit and does not attest production parity.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Protocol
from urllib.parse import parse_qsl, urlsplit

import asyncpg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.adapters.auth.jwt import (
    ApplicationAuthentication,
    MissionAuthenticationRejected,
    MissionTokenVerifier,
)
from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
from mission_control.adapters.postgres.approvals.intents import PostgresGovernedIntentRepository
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.chains.store import PostgresChainReader
from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresCheckpointRepository,
    PostgresContinuationRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_projection import (
    PostgresTranscriptDocuments,
)
from mission_control.adapters.postgres.frames.transcript_reads import PostgresMissionEventReader
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.postgres.workspaces.artifact_metadata_repository import (
    PostgresArtifactMetadataRepository,
)
from mission_control.adapters.realtime.stream_source import PostgresStreamSource
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.boundary_commands import TemporalBoundaryCommandTransport
from mission_control.adapters.temporal.client import connect_temporal, resolve_temporal_connection
from mission_control.adapters.temporal.human_gate_wake import TemporalHumanGateWake
from mission_control.adapters.temporal.search_attributes import verify_belllabs_search_attributes
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.unit_reconciliation import TemporalUnitReconciliationNudge
from mission_control.adapters.temporal.visibility import TemporalRunVisibility
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.chains.service import ChainInspectionService
from mission_control.application.context.continuation import CheckpointReadService
from mission_control.application.execution.approvals_governed import (
    GovernedEffectService,
    GovernedToolRegistry,
)
from mission_control.application.execution.harness.registry import describe_only_registry
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
)
from mission_control.application.frames.artifact_bodies import (
    ArtifactPayloadReader,
    GrantedArtifactBodyReader,
)
from mission_control.application.frames.search import RunListService, TranscriptSearchService
from mission_control.application.frames.transcript import TranscriptService
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.application.installations.registry import (
    ApplicationRegistry,
    VerifiedApplicationIdentity,
    request_scope,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.application.streams.service import MissionStreamService
from mission_control.bootstrap.catalog import (
    compose_catalog_service,
    configured_catalog_embeddings,
)
from mission_control.bootstrap.composition import (
    MissionApplicationServices,
    compose_application_services,
    temporal_cluster_identity,
)
from mission_control.bootstrap.manifests import (
    compose_manifest_launch_inputs,
    compose_manifest_service,
)
from mission_control.bootstrap.settings import get_settings
from mission_control.bootstrap.subscriptions import (
    InboxComposition,
    compose_coordinator_inbox_service,
    compose_subscription_service,
    relay_enabled,
    run_subscription_relays,
)
from mission_control.contracts.json import parse_json_object
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.http.catalog import router as catalog_router
from mission_control.interfaces.http.chains import router as chains_router
from mission_control.interfaces.http.continuation import router as continuation_router
from mission_control.interfaces.http.coordinator_inbox import router as coordinator_inbox_router
from mission_control.interfaces.http.human_tasks import router as human_tasks_router
from mission_control.interfaces.http.lanes import router as lanes_router
from mission_control.interfaces.http.middleware.body_limit import BodySizeLimitMiddleware
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)
from mission_control.interfaces.http.missions import router as missions_router
from mission_control.interfaces.http.stop_fence import router as stop_fence_router
from mission_control.interfaces.http.subscriptions import router as subscriptions_router
from mission_control.interfaces.http.transcript import router as transcript_router


class TemporalDeployment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    address: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    root_task_queue: str = Field(min_length=1)
    stagegraph_task_queue: str = Field(min_length=1)
    goal_directed_task_queue: str = Field(min_length=1)


class ApplicationDeployment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    authentication: ApplicationAuthentication
    family_writer_secret_ref: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]*$")
    temporal: TemporalDeployment | None = None


class MissionDeployment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    storage_mode: Literal["production_common"]
    applications: tuple[ApplicationDeployment, ...] = Field(min_length=1)
    max_request_bytes: int = Field(ge=1, le=16_000_000)

    @model_validator(mode="after")
    def unique_applications(self) -> MissionDeployment:
        ids = [item.authentication.binding.application_id for item in self.applications]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate application deployment")
        return self


class CatalogFactory(Protocol):
    def __call__(
        self,
        pool: asyncpg.Pool,
        *,
        request_scope: str,
        catalog_scope: str,
    ) -> CatalogService: ...


@dataclass(frozen=True)
class RuntimeOptions:
    """Trusted Python composition extensions; never populated from request bodies."""

    admission_policies: AdmissionPolicyRegistry
    extensions: ExtensionRegistry
    payload_store: ContentAddressedPayloadStore
    family_admissions: FamilyAdmissionRegistry | None = None
    fork_policies: ForkPatchPolicyRegistry | None = None
    catalog_factory: CatalogFactory | None = None
    # MP-13: the artifact payload store the transcript's `full=true` reads bodies from. The API
    # has no payload store composed by default; unset keeps `501 full_body_unavailable`.
    artifact_payloads: ArtifactPayloadReader | None = None
    # MP-11: the Mission-Control-owned effect tools the governed gateway may run (trusted
    # Python composition; none by default, so the gateway governs nothing).
    governed_tools: GovernedToolRegistry | None = None


def install_authentication(application: FastAPI, verifier: MissionTokenVerifier) -> None:
    def authenticate(
        application_id: str,
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> MissionPrincipal:
        scheme, _, token = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise HTTPException(status_code=401, detail={"code": "bearer_token_required"})
        try:
            authenticated = verifier.authenticate(application_id, token)
        except MissionAuthenticationRejected as error:
            raise HTTPException(
                status_code=error.status_code, detail={"code": error.code}
            ) from None
        identity = authenticated.identity
        return MissionPrincipal(
            installation_id=identity.installation_id,
            application_id=identity.application_id,
            tenant_id=identity.tenant_id,
            issuer=identity.issuer,
            audiences=identity.audiences,
            actor=authenticated.actor,
            sponsorship_refs=authenticated.sponsorship_refs,
            approval_refs=authenticated.approval_refs,
        )

    application.dependency_overrides[get_mission_principal] = authenticate


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
TLS_MODES = frozenset({"require", "verify-ca", "verify-full"})


async def application_pool(secret_ref: str) -> asyncpg.Pool:
    """Open an app-bound restricted pool; remote endpoints must require TLS."""
    dsn = os.environ.get(secret_ref)
    if not dsn:
        raise RuntimeError("configured database credential reference is unavailable")
    parsed = urlsplit(dsn)
    query = dict(parse_qsl(parsed.query))
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise RuntimeError("configured database credential is not a PostgreSQL endpoint")
    if parsed.hostname not in LOOPBACK_HOSTS and query.get("sslmode") not in TLS_MODES:
        raise RuntimeError("remote PostgreSQL endpoints must require TLS")
    if "options" in query:
        raise RuntimeError("application pools cannot override server options")
    try:
        return await asyncpg.create_pool(
            dsn=dsn,
            min_size=1,
            max_size=8,
            command_timeout=30,
            server_settings={"statement_timeout": "30000", "lock_timeout": "10000"},
        )
    except Exception:
        raise RuntimeError("configured PostgreSQL connection is unavailable") from None


async def coordinator_inbox_installed(pool: asyncpg.Pool) -> bool:
    """Whether the application database has the MP-15 inbox tables (release 1.2.0, 0033)."""

    async with pool.acquire() as connection:
        return bool(
            await connection.fetchval(
                "SELECT to_regclass('mission_control.coordinator_inbox') IS NOT NULL"
            )
        )


async def _stop_relay(stop: asyncio.Event, task: asyncio.Task[None]) -> None:
    stop.set()
    await task


def create_application(
    deployment: MissionDeployment,
    *,
    runtime_options: Mapping[str, RuntimeOptions] | None = None,
) -> FastAPI:
    deployment = MissionDeployment.model_validate(deployment.model_dump(mode="python"))
    if deployment.storage_mode != "production_common":
        raise RuntimeError("only the common Mission Control component is supported")
    configs = tuple(item.authentication for item in deployment.applications)
    verifier = MissionTokenVerifier(configs)
    registry = ApplicationRegistry(tuple(config.binding for config in configs))
    options_by_app = dict(runtime_options or {})
    allowed_applications = {item.binding.application_id for item in configs}
    if set(options_by_app) - allowed_applications or any(
        not isinstance(value, RuntimeOptions) for value in options_by_app.values()
    ):
        raise ValueError(
            "runtime options must name configured applications and typed RuntimeOptions"
        )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        catalog_embeddings = configured_catalog_embeddings(get_settings())
        async with AsyncExitStack() as stack:
            subscription_stores: list[PostgresSubscriptionStore] = []
            inbox_compositions: list[InboxComposition] = []
            try:
                for item in deployment.applications:
                    config = item.authentication
                    binding = config.binding
                    pool = await application_pool(binding.database_secret_ref)
                    stack.push_async_callback(pool.close)
                    # MP-15: the coordinator inbox needs release 1.2.0 (migration 0033); an
                    # application database on an older release keeps every other service.
                    inbox_installed = await coordinator_inbox_installed(pool)
                    family_pool = None
                    if item.family_writer_secret_ref is not None:
                        family_pool = await application_pool(item.family_writer_secret_ref)
                        stack.push_async_callback(family_pool.close)
                    options = options_by_app.get(binding.application_id) or RuntimeOptions(
                        AdmissionPolicyRegistry(),
                        ExtensionRegistry(),
                        UnavailablePayloadStore(),
                    )
                    transport = submitter = nudge = None
                    run_visibility: TemporalRunVisibility | None = None
                    gate_wake: TemporalHumanGateWake | None = None
                    cluster = None
                    if item.temporal is not None:
                        temporal = item.temporal
                        # FT-G7: local server by default; Temporal Cloud only when
                        # TEMPORAL_TARGET=cloud (api key + TLS).
                        connection = resolve_temporal_connection(
                            get_settings(), address=temporal.address, namespace=temporal.namespace
                        )
                        client = await connect_temporal(connection)
                        await verify_belllabs_search_attributes(client, connection.namespace)
                        transport = TemporalBoundaryCommandTransport(client)
                        # FT-C4: run list over the typed Search Attributes.
                        run_visibility = TemporalRunVisibility(client)
                        nudge = TemporalUnitReconciliationNudge(client)
                        # MP-10: a committed resolution nudges its waiting Human Gate.
                        gate_wake = TemporalHumanGateWake(client)
                        submitter = TemporalWorkflowSubmitter.for_production(
                            client,
                            root_task_queue=temporal.root_task_queue,
                            stagegraph_task_queue=temporal.stagegraph_task_queue,
                            goal_directed_task_queue=temporal.goal_directed_task_queue,
                            search_attribute_policy="required",
                            mission_installation_id=binding.installation_id,
                            mission_application_id=binding.application_id,
                        )
                        # MP-22: every launch binds the run to this cluster first.
                        cluster = temporal_cluster_identity(
                            target=connection.target,
                            address=temporal.address,
                            namespace=connection.namespace,
                            task_queue=temporal.root_task_queue,
                        )
                    tenants = {tenant for grant in config.grants for tenant in grant.tenant_ids}
                    if not tenants:
                        raise RuntimeError("deployment must grant at least one application tenant")
                    for tenant in tenants:
                        identity = VerifiedApplicationIdentity(
                            issuer=config.issuer,
                            audiences=frozenset({config.audience}),
                            application_id=binding.application_id,
                            installation_id=binding.installation_id,
                            tenant_id=tenant,
                        )
                        services = await compose_application_services(
                            binding,
                            identity,
                            runtime_pool=pool,
                            admission_policies=options.admission_policies,
                            extensions=options.extensions,
                            payload_store=options.payload_store,
                            storage_mode="production_common",
                            registry=registry,
                            family_writer_pool=family_pool,
                            family_admissions=options.family_admissions,
                            fork_policies=options.fork_policies,
                            boundary_transport=transport,
                            submitter=submitter,
                            nudge=nudge,
                            externalize_above_bytes=15_000_000,
                            cluster=cluster,
                        )
                        key = (binding.installation_id, binding.application_id, tenant)
                        application.state.mission_control_services[key] = services.lifecycle
                        application.state.mission_control_runtime_services[key] = services.runtime
                        application.state.mission_control_admission_services[key] = (
                            services.admission
                        )
                        application.state.mission_control_compositions[key] = services
                        catalog_scope = (
                            f"mc/{binding.installation_id}/{binding.application_id}/catalog"
                        )
                        if options.catalog_factory is not None:
                            catalog = options.catalog_factory(
                                pool,
                                request_scope=request_scope(identity),
                                catalog_scope=catalog_scope,
                            )
                        else:
                            # FT-A3: lexical search always; hybrid when the embedding
                            # Model Profile and its credential are configured.
                            catalog = compose_catalog_service(
                                pool,
                                request_scope=request_scope(identity),
                                catalog_scope=catalog_scope,
                                embeddings=catalog_embeddings,
                            )
                        if (
                            catalog.request_scope != request_scope(identity)
                            or catalog.catalog_scope
                            != f"mc/{binding.installation_id}/{binding.application_id}/catalog"
                        ):
                            raise RuntimeError(
                                "catalog composition differs from authenticated scope"
                            )
                        application.state.mission_control_catalog_services[key] = catalog
                        # SPEC-03 (C3): the run transcript, read under the tenant scope.
                        transcripts = TranscriptService(
                            PostgresMissionEventReader(pool),
                            PostgresFrameRepository(pool),
                            request_scope=request_scope(identity),
                        )
                        application.state.mission_control_transcript_services[key] = transcripts
                        # MP-13: full artifact bodies, read under the same scope and the
                        # `workflow.result.read` grant; only when a payload store is composed.
                        if options.artifact_payloads is not None:
                            application.state.mission_control_artifact_body_readers[key] = (
                                GrantedArtifactBodyReader(
                                    PostgresArtifactMetadataRepository(
                                        pool, request_scope=request_scope(identity)
                                    ),
                                    options.artifact_payloads,
                                    request_scope=request_scope(identity),
                                )
                            )
                        # SPEC-03 (C4): run list (Temporal Visibility + ledger) and search.
                        application.state.mission_control_run_list_services[key] = RunListService(
                            run_visibility,
                            services.run_control,
                            request_scope=request_scope(identity),
                        )
                        application.state.mission_control_transcript_search_services[key] = (
                            TranscriptSearchService(transcripts, PostgresTranscriptDocuments(pool))
                        )
                        # SPEC-02 (B4): sealed continuation checkpoints, read under scope.
                        application.state.mission_control_checkpoint_services[key] = (
                            CheckpointReadService(
                                PostgresCheckpointRepository(pool),
                                PostgresContinuationRepository(pool),
                                request_scope=request_scope(identity),
                            )
                        )
                        subscriptions = compose_subscription_service(pool, request_scope(identity))
                        application.state.mission_control_subscription_services[key] = subscriptions
                        mailbox_semantics = (
                            services.mailbox.semantics if services.mailbox is not None else None
                        )
                        if inbox_installed:
                            # MP-15 (SPEC-04): the durable coordinator inbox; prompts go through
                            # this tenant's MissionControlService and its mailbox semantics.
                            application.state.mission_control_coordinator_inbox_services[key] = (
                                compose_coordinator_inbox_service(
                                    pool,
                                    request_scope(identity),
                                    commands=services.lifecycle,
                                    mailbox_semantics=mailbox_semantics,
                                )
                            )
                            inbox_compositions.append(
                                (
                                    pool,
                                    request_scope(identity),
                                    services.lifecycle,
                                    mailbox_semantics,
                                )
                            )
                        # MP-10 (SPEC-03): Human Gate tasks under the tenant scope; HTTP, MCP
                        # and the socket resolve through this one service. MP-11: approval-
                        # origin tasks (provider permission/question, MCP elicitation,
                        # governed effect) share it and its one resolution path; their waiters
                        # live in the worker's broker, which re-reads the task (no wake here).
                        approval_tasks = PostgresApprovalTaskRepository(pool)
                        application.state.mission_control_human_task_services[key] = (
                            HumanTaskService(
                                PostgresHumanTaskRepository(pool),
                                request_scope=request_scope(identity),
                                wake=gate_wake,
                                approvals=approval_tasks,
                            )
                        )
                        # MP-11: prepare/review/execute for Mission-Control-owned tools.
                        application.state.mission_control_governed_effect_services[key] = (
                            GovernedEffectService(
                                PostgresGovernedIntentRepository(pool),
                                approval_tasks,
                                options.governed_tools or GovernedToolRegistry(),
                                request_scope=request_scope(identity),
                                probe=PostgresApprovalContextProbe(pool),
                                fences=PostgresStopFenceRepository(pool),
                            )
                        )
                        # MP-14 (SPEC-04): scoped mission/frame streams for the /missions
                        # socket; replay reads PostgreSQL only (`bootstrap/realtime.py`).
                        application.state.mission_control_stream_services[key] = (
                            MissionStreamService(
                                PostgresStreamSource(pool, request_scope(identity)),
                                request_scope=request_scope(identity),
                            )
                        )
                        # FT-E2/E3: Mission Manifest compile, submit and start for the tenant;
                        # MP-02: start authors the launch inputs from the deployment bindings.
                        launch_inputs = (
                            compose_manifest_launch_inputs(
                                pool,
                                settings=get_settings(),
                                run_control=services.run_control,
                                control_plane=services.control_plane,
                            )
                            if services.launch is not None
                            else None
                        )
                        application.state.mission_control_manifest_services[key] = (
                            compose_manifest_service(
                                pool,
                                request_scope=request_scope(identity),
                                catalog=catalog,
                                extensions=options.extensions,
                                payload_store=options.payload_store,
                                run_control=services.run_control,
                                admission_policies=options.admission_policies,
                                launches=services.launch,
                                subscriptions=subscriptions,
                                launch_inputs=launch_inputs,
                            )
                        )
                        # FT-D2: the Mission Chain projection, read under the tenant scope.
                        application.state.mission_control_chain_services[key] = (
                            ChainInspectionService(
                                PostgresChainReader(pool), request_scope=request_scope(identity)
                            )
                        )
                        subscription_stores.append(
                            PostgresSubscriptionStore(pool, request_scope(identity))
                        )
                if subscription_stores and relay_enabled():
                    relay_stop = asyncio.Event()
                    relay_task = asyncio.create_task(
                        run_subscription_relays(
                            subscription_stores, relay_stop, inboxes=inbox_compositions
                        )
                    )
                    stack.push_async_callback(_stop_relay, relay_stop, relay_task)
                application.state.mission_control_ready = True
                yield
            finally:
                application.state.mission_control_ready = False
                for config in configs:
                    registry.disable(config.binding.application_id)

    application = FastAPI(title="Mission Control", lifespan=lifespan)
    application.add_middleware(BodySizeLimitMiddleware, max_bytes=deployment.max_request_bytes)
    application.state.mission_control_registry = registry
    application.state.mission_control_services = {}
    application.state.mission_control_runtime_services = {}
    application.state.mission_control_admission_services = {}
    application.state.mission_control_catalog_services = {}
    application.state.mission_control_transcript_services = {}
    application.state.mission_control_artifact_body_readers = {}
    application.state.mission_control_checkpoint_services = {}
    application.state.mission_control_run_list_services = {}
    application.state.mission_control_transcript_search_services = {}
    application.state.mission_control_subscription_services = {}
    application.state.mission_control_coordinator_inbox_services = {}
    application.state.mission_control_human_task_services = {}
    application.state.mission_control_governed_effect_services = {}
    application.state.mission_control_stream_services = {}
    application.state.mission_control_chain_services = {}
    application.state.mission_control_manifest_services = {}
    application.state.mission_control_compositions = {}
    application.state.mission_control_ready = False
    # FT-G1: the API lists and describes lanes; workers execute them.
    lane_settings = get_settings()
    application.state.mission_control_lanes = describe_only_registry(
        cursor_bound=lane_settings.cursor_api_key is not None,
        claude_bound=lane_settings.mission_control_claude_lane,
        codex_bound=lane_settings.mission_control_codex_lane,
        allow_unqualified=lane_settings.allow_unqualified_lanes,
    )
    install_authentication(application, verifier)
    application.include_router(router)
    application.include_router(catalog_router)
    application.include_router(lanes_router)
    application.include_router(stop_fence_router)
    application.include_router(transcript_router)
    application.include_router(continuation_router)
    application.include_router(subscriptions_router)
    application.include_router(chains_router)
    application.include_router(missions_router)
    application.include_router(human_tasks_router)
    application.include_router(coordinator_inbox_router)

    @application.get("/health/live")
    def live() -> dict[str, bool]:
        return {"live": True}

    @application.get("/health/ready")
    def ready() -> dict[str, object]:
        if not application.state.mission_control_ready:
            raise HTTPException(status_code=503, detail={"code": "installation_unavailable"})
        services: list[MissionApplicationServices] = list(
            application.state.mission_control_compositions.values()
        )
        return {
            "ready": True,
            "storage_mode": "production_common",
            "production_ready": all(item.readiness.production_ready for item in services),
            "component_versions": sorted({item.readiness.component_version for item in services}),
            "launch_configured": all(item.launch is not None for item in services),
        }

    return application


def load_deployment(filename: str | Path | None = None) -> MissionDeployment:
    filename = filename or os.environ.get("MISSION_CONTROL_DEPLOYMENT_FILE")
    if not filename:
        raise RuntimeError("MISSION_CONTROL_DEPLOYMENT_FILE must name an operator deployment file")
    config_path = Path(filename).resolve()
    document = parse_json_object(config_path.read_bytes())
    deployment = MissionDeployment.model_validate(document)
    # Relative public-key paths are relative to the operator configuration, not cwd.
    resolved = []
    for item in deployment.applications:
        auth = item.authentication
        path = auth.public_jwks_file
        if not path.is_absolute():
            auth = auth.model_copy(update={"public_jwks_file": config_path.parent / path})
        resolved.append(item.model_copy(update={"authentication": auth}))
    return deployment.model_copy(update={"applications": tuple(resolved)})


def create_app() -> FastAPI:
    deployment = load_deployment()
    return create_application(deployment, runtime_options=load_runtime_options(deployment))


def load_runtime_options(
    deployment: MissionDeployment,
    specification: str | None = None,
) -> Mapping[str, RuntimeOptions] | None:
    """Load an explicitly installed, operator-reviewed Python composition factory.

    Only process configuration selects this callable. No request, token, catalog
    document, downloaded skill, or model output can select code to import.
    """
    specification = specification or os.environ.get("MISSION_CONTROL_RUNTIME_OPTIONS_FACTORY")
    if not specification:
        return None
    if not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", specification):
        raise ValueError("runtime options factory must be an installed module:callable")
    module_name, function_name = specification.split(":")
    factory = getattr(importlib.import_module(module_name), function_name)
    if not callable(factory):
        raise ValueError("runtime options factory is not callable")
    result = factory(deployment)
    expected = {item.authentication.binding.application_id for item in deployment.applications}
    if (
        not isinstance(result, Mapping)
        or set(result) != expected
        or any(not isinstance(value, RuntimeOptions) for value in result.values())
    ):
        raise ValueError(
            "runtime options factory must return exactly the configured typed app mapping"
        )
    return dict(result)
