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
from temporalio.client import Client

from mission_control.adapters.auth.jwt import (
    ApplicationAuthentication,
    MissionAuthenticationRejected,
    MissionTokenVerifier,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.boundary_commands import TemporalBoundaryCommandTransport
from mission_control.adapters.temporal.search_attributes import verify_belllabs_search_attributes
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.unit_reconciliation import TemporalUnitReconciliationNudge
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
)
from mission_control.application.installations.registry import (
    ApplicationRegistry,
    VerifiedApplicationIdentity,
    request_scope,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.bootstrap.catalog import compose_catalog_service
from mission_control.bootstrap.composition import (
    MissionApplicationServices,
    compose_application_services,
)
from mission_control.bootstrap.subscriptions import (
    compose_subscription_service,
    relay_enabled,
    run_subscription_relays,
)
from mission_control.contracts.json import parse_json_object
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.http.catalog import router as catalog_router
from mission_control.interfaces.http.middleware.body_limit import BodySizeLimitMiddleware
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)
from mission_control.interfaces.http.subscriptions import router as subscriptions_router


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
        async with AsyncExitStack() as stack:
            subscription_stores: list[PostgresSubscriptionStore] = []
            try:
                for item in deployment.applications:
                    config = item.authentication
                    binding = config.binding
                    pool = await application_pool(binding.database_secret_ref)
                    stack.push_async_callback(pool.close)
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
                    if item.temporal is not None:
                        temporal = item.temporal
                        client = await Client.connect(
                            temporal.address, namespace=temporal.namespace
                        )
                        await verify_belllabs_search_attributes(client, temporal.namespace)
                        transport = TemporalBoundaryCommandTransport(client)
                        nudge = TemporalUnitReconciliationNudge(client)
                        submitter = TemporalWorkflowSubmitter.for_production(
                            client,
                            root_task_queue=temporal.root_task_queue,
                            stagegraph_task_queue=temporal.stagegraph_task_queue,
                            goal_directed_task_queue=temporal.goal_directed_task_queue,
                            search_attribute_policy="required",
                            mission_installation_id=binding.installation_id,
                            mission_application_id=binding.application_id,
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
                        )
                        key = (binding.installation_id, binding.application_id, tenant)
                        application.state.mission_control_services[key] = services.lifecycle
                        application.state.mission_control_runtime_services[key] = services.runtime
                        application.state.mission_control_admission_services[key] = (
                            services.admission
                        )
                        application.state.mission_control_compositions[key] = services
                        catalog = (options.catalog_factory or compose_catalog_service)(
                            pool,
                            request_scope=request_scope(identity),
                            catalog_scope=(
                                f"mc/{binding.installation_id}/{binding.application_id}/catalog"
                            ),
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
                        subscriptions = compose_subscription_service(pool, request_scope(identity))
                        application.state.mission_control_subscription_services[key] = subscriptions
                        subscription_stores.append(
                            PostgresSubscriptionStore(pool, request_scope(identity))
                        )
                if subscription_stores and relay_enabled():
                    relay_stop = asyncio.Event()
                    relay_task = asyncio.create_task(
                        run_subscription_relays(subscription_stores, relay_stop)
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
    application.state.mission_control_subscription_services = {}
    application.state.mission_control_compositions = {}
    application.state.mission_control_ready = False
    install_authentication(application, verifier)
    application.include_router(router)
    application.include_router(catalog_router)
    application.include_router(subscriptions_router)

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
