from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, TypeAdapter

from mission_control.adapters.postgres.connections import create_application_postgres_pool
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.storage.control_plane_payloads import (
    S3PayloadStore,
    UnavailablePayloadStore,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.bootstrap.settings import get_settings
from mission_control.domain.authoring.contracts import (
    AliasBinding,
    AliasRef,
    AuthoringHead,
    AuthorityCeiling,
    CompileInvocation,
    Definition,
    DefinitionKind,
    EffectiveRunConfiguration,
    MoveAliasRequest,
    PublishDraftRequest,
    PublishedDefinition,
    PublishRequest,
    RetireRequest,
    SaveDraftRequest,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry

router = APIRouter(prefix="/control-plane/v1", tags=["control-plane"])

_service_initialization_lock = asyncio.Lock()


async def get_control_plane_service(request: Request) -> ControlPlaneService:
    """Lazily compose the installation-scoped PostgreSQL catalog service."""
    service = getattr(request.app.state, "control_plane_service", None)
    if service is not None:
        return service
    async with _service_initialization_lock:
        service = getattr(request.app.state, "control_plane_service", None)
        if service is not None:
            return service
        settings = get_settings()
        if not settings.mission_control_catalog_scope:
            raise HTTPException(
                status_code=503, detail="installation catalog scope is not configured"
            )
        pool = getattr(request.app.state, "run_control_postgres_pool", None)
        if pool is None:
            pool = await create_application_postgres_pool(settings)
            request.app.state.control_plane_postgres_pool = pool
        payload_store: ContentAddressedPayloadStore
        if settings.s3_bucket:
            payload_store = S3PayloadStore(settings, settings.s3_bucket)
            externalize_above_bytes = 256_000
        else:
            payload_store = UnavailablePayloadStore()
            # Bound inline payload size; large payloads require an admitted object store.
            externalize_above_bytes = 15_000_000
        extensions = ExtensionRegistry()
        service = ControlPlaneService(
            PostgresDefinitionRepository(
                pool, catalog_scope=settings.mission_control_catalog_scope
            ),
            extensions,
            payload_store,
            externalize_above_bytes=externalize_above_bytes,
        )
        request.app.state.control_plane_service = service
        return service


async def close_control_plane_resources(application: object) -> None:
    state = getattr(application, "state", None)
    pool = getattr(state, "control_plane_postgres_pool", None)
    if pool is not None:
        await pool.close()


# What do these exactly control when creating the ERCs.
class ControlPlanePrincipal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    actor_id: str
    roles: frozenset[str]
    tenant_scopes: frozenset[str] = frozenset()
    authority_refs: frozenset[str] = frozenset()
    sponsorship_refs: frozenset[str] = frozenset()
    approval_refs: frozenset[str] = frozenset()
    compilation_authority: AuthorityCeiling | None = None


async def get_control_plane_principal() -> ControlPlanePrincipal:
    # The deployment must supply authenticated identity and role mapping.
    raise HTTPException(
        status_code=503,
        detail="control-plane authorization dependency is not configured",
    )


def require_role(
    principal: ControlPlanePrincipal, actor_id: str, allowed_roles: frozenset[str]
) -> None:
    if principal.actor_id != actor_id:
        raise HTTPException(status_code=403, detail="actor identity mismatch")
    if not principal.roles & allowed_roles:
        raise HTTPException(status_code=403, detail="insufficient control-plane role")


Service = Annotated[ControlPlaneService, Depends(get_control_plane_service)]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]


@router.post("/definitions", response_model=PublishedDefinition, status_code=201)
async def publish_definition(
    request: PublishRequest, principal: Principal, service: Service
) -> PublishedDefinition:
    require_role(principal, request.actor_id, frozenset({"publisher"}))
    return await service.publish(request.model_copy(update={"published_at": datetime.now(UTC)}))


@router.put("/drafts", response_model=AuthoringHead)
async def save_draft(
    request: SaveDraftRequest, principal: Principal, service: Service
) -> AuthoringHead:
    require_role(principal, request.actor_id, frozenset({"author", "publisher"}))
    return await service.save_draft(request.model_copy(update={"updated_at": datetime.now(UTC)}))


@router.get("/drafts/{kind}/{logical_id}", response_model=AuthoringHead)
async def get_draft(
    kind: DefinitionKind,
    logical_id: str,
    principal: Principal,
    service: Service,
) -> AuthoringHead:
    require_role(principal, principal.actor_id, frozenset({"author", "publisher"}))
    return await service.get_draft(kind.value, logical_id)


@router.post("/drafts/publish", response_model=PublishedDefinition, status_code=201)
async def publish_draft(
    request: PublishDraftRequest, principal: Principal, service: Service
) -> PublishedDefinition:
    require_role(principal, request.actor_id, frozenset({"publisher"}))
    return await service.publish_draft(
        request.model_copy(update={"published_at": datetime.now(UTC)})
    )


@router.post("/aliases", response_model=AliasBinding)
async def move_alias(
    request: MoveAliasRequest, principal: Principal, service: Service
) -> AliasBinding:
    require_role(principal, request.actor_id, frozenset({"publisher", "operator"}))
    return await service.move_alias(request.model_copy(update={"moved_at": datetime.now(UTC)}))


@router.post("/aliases/resolve", response_model=AliasBinding)
async def resolve_alias(alias: AliasRef, service: Service) -> AliasBinding:
    return await service.resolve_alias(alias)


# Question region BellLabs Owner.
# invocation is intent as input ? principal is what ? Service is control plane service
@router.post("/compile", response_model=EffectiveRunConfiguration, status_code=201)
async def compile_configuration(
    invocation: CompileInvocation, principal: Principal, service: Service
) -> EffectiveRunConfiguration:
    # Turns catalog refs into an immutable EffectiveRunConfiguration
    require_role(
        principal,
        invocation.context.actor_id,
        frozenset({"compiler", "operator"}),
    )
    if invocation.context.authority_scope not in principal.tenant_scopes:
        raise HTTPException(status_code=403, detail="compilation authority scope was not granted")
    if invocation.context.authority_subject_id != principal.actor_id:
        raise HTTPException(status_code=403, detail="compilation authority subject mismatch")
    if principal.compilation_authority is None:
        raise HTTPException(status_code=403, detail="compilation authority is not configured")
    invocation = invocation.model_copy(
        update={
            "caller_authority": principal.compilation_authority,
            "context": invocation.context.model_copy(update={"compiled_at": datetime.now(UTC)}),
        }
    )
    return await service.compile(invocation)


@router.get("/effective-run-configurations/{digest}", response_model=EffectiveRunConfiguration)
async def retrieve_configuration(digest: str, service: Service) -> EffectiveRunConfiguration:
    return await service.retrieve(digest)


@router.post("/definitions/retire", response_model=PublishedDefinition)
async def retire_definition(
    request: RetireRequest, principal: Principal, service: Service
) -> PublishedDefinition:
    require_role(principal, request.actor_id, frozenset({"publisher", "operator"}))
    return await service.retire(request.model_copy(update={"retired_at": datetime.now(UTC)}))


@router.get("/schemas")
async def control_plane_schemas() -> dict[str, object]:
    """Export schemas from the same contracts used by publication and compilation."""
    return {
        "definition": TypeAdapter(Definition).json_schema(),
        "save_draft": SaveDraftRequest.model_json_schema(),
        "compile_invocation": CompileInvocation.model_json_schema(),
        "effective_run_configuration": EffectiveRunConfiguration.model_json_schema(),
    }
