from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, TypeAdapter

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.config import get_settings
from app.domain.control_plane.contracts import (
    AliasBinding,
    AliasRef,
    AuthoringHead,
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
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import (
    ContentAddressedPayloadStore,
    S3PayloadStore,
    UnavailablePayloadStore,
)
from app.integrations.mongodb import create_mongodb

router = APIRouter(prefix="/control-plane/v1", tags=["control-plane"])

_service_initialization_lock = asyncio.Lock()


async def get_control_plane_service(request: Request) -> ControlPlaneService:
    """Lazily compose the authoritative Mongo-backed service."""
    service = getattr(request.app.state, "control_plane_service", None)
    if service is not None:
        return service
    async with _service_initialization_lock:
        service = getattr(request.app.state, "control_plane_service", None)
        if service is not None:
            return service
        settings = get_settings()
        client, _database = await create_mongodb(settings)
        payload_store: ContentAddressedPayloadStore
        if settings.s3_bucket:
            payload_store = S3PayloadStore(settings, settings.s3_bucket)
            externalize_above_bytes = 256_000
        else:
            payload_store = UnavailablePayloadStore()
            # Stay below MongoDB's 16 MiB document limit and fail explicitly above it.
            externalize_above_bytes = 15_000_000
        service = ControlPlaneService(
            BeanieDefinitionRepository(),
            ExtensionRegistry(),
            payload_store,
            externalize_above_bytes=externalize_above_bytes,
        )
        request.app.state.control_plane_service = service
        request.app.state.control_plane_mongodb_client = client
        return service


async def close_control_plane_resources(application: object) -> None:
    state = getattr(application, "state", None)
    client = getattr(state, "control_plane_mongodb_client", None)
    if client is not None:
        await client.close()


class ControlPlanePrincipal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    actor_id: str
    roles: frozenset[str]


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


@router.post("/compile", response_model=EffectiveRunConfiguration, status_code=201)
async def compile_configuration(
    invocation: CompileInvocation, principal: Principal, service: Service
) -> EffectiveRunConfiguration:
    require_role(
        principal,
        invocation.context.actor_id,
        frozenset({"compiler", "operator"}),
    )
    invocation = invocation.model_copy(
        update={"context": invocation.context.model_copy(update={"compiled_at": datetime.now(UTC)})}
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
