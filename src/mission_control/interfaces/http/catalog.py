"""Authenticated operational catalog routes sharing the mission installation binding."""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.capabilities.bundle_custody import (
    BundleCustodyConflict,
    BundleCustodyService,
    PublishPlan,
    PublishPrepareRequest,
    PublishResult,
)
from mission_control.application.capabilities.capability_search import CapabilitySearchResponse
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionReport,
    ExternalCandidateInspectionRequest,
    InspectionPrincipal,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalDiscoveryBatch,
)
from mission_control.contracts.json import parse_json_object
from mission_control.domain.agentic_components.contracts import (
    AgenticComponentRelease,
    ComponentQuery,
)
from mission_control.domain.authoring.contracts import ExactDefinitionRef, PublishedDefinition
from mission_control.domain.authoring.errors import DefinitionNotFound
from mission_control.domain.capabilities.bundles import CapabilityDrift
from mission_control.domain.capabilities.catalog_entry import CatalogEntrySummary, summarize
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
)


async def strict_catalog_json(request: Request) -> None:
    if request.method == "POST":
        try:
            parse_json_object(await request.body())
        except ValueError:
            raise HTTPException(400, detail={"code": "invalid_json_object"}) from None


router = APIRouter(
    prefix="/v1/applications/{application_id}/catalog",
    tags=["capability-catalog"],
    dependencies=[Depends(strict_catalog_json)],
)
Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]


def catalog_service(application_id: str, request: Request, principal: Principal) -> CatalogService:
    authorize_application(application_id, request, principal)
    service = getattr(request.app.state, "mission_control_catalog_services", {}).get(
        (principal.installation_id, application_id, principal.tenant_id)
    )
    if not isinstance(service, CatalogService) or service.request_scope != principal_request_scope(
        principal
    ):
        raise HTTPException(503, detail={"code": "catalog_unavailable"})
    return service


Service = Annotated[CatalogService, Depends(catalog_service)]


def permitted(principal: MissionPrincipal, permission: str) -> None:
    if permission not in principal.actor.permissions:
        raise HTTPException(403, detail={"code": "catalog_permission_denied"})


class DiscoveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source: Literal["mcp", "skills"]
    query: str = Field(min_length=1, max_length=500)
    limit: int = Field(default=10, ge=1, le=100)
    owner: str | None = None


class DefinitionsResponse(BaseModel):
    catalog_scope: str
    definitions: tuple[ExactDefinitionRef, ...]
    # Per row: kind, agent-composition kind and lane-profile support (SPEC-01, FT-A1).
    capabilities: tuple[CatalogEntrySummary, ...] = ()


class ComponentsResponse(BaseModel):
    components: tuple[AgenticComponentRelease, ...]


@router.get("/definitions")
async def definitions(principal: Principal, service: Service) -> DefinitionsResponse:
    permitted(principal, "catalog:read")
    published = await service.definitions.list_published_definitions()
    return DefinitionsResponse(
        catalog_scope=service.catalog_scope,
        definitions=tuple(item.ref for item in published),
        capabilities=tuple(summarize(item) for item in published),
    )


@router.post("/resolve")
async def resolve(
    ref: ExactDefinitionRef, principal: Principal, service: Service
) -> PublishedDefinition:
    permitted(principal, "catalog:read")
    try:
        return await service.definitions.get(ref)
    except DefinitionNotFound:
        raise HTTPException(404, detail={"code": "definition_not_found"}) from None


@router.post("/search")
async def search(
    body: CapabilitySearchRequest, principal: Principal, service: Service
) -> CapabilitySearchResponse:
    permitted(principal, "catalog:read")
    if body.tenant_scope != service.catalog_scope:
        raise HTTPException(403, detail={"code": "catalog_scope_denied"})
    if service.search is None:
        raise HTTPException(503, detail={"code": "catalog_search_unavailable"})
    return await service.search.search(body)


@router.post("/discover")
async def discover(
    body: DiscoveryRequest, principal: Principal, service: Service
) -> ExternalDiscoveryBatch:
    permitted(principal, "catalog:discover")
    if service.discovery is None:
        raise HTTPException(503, detail={"code": "catalog_discovery_unavailable"})
    if body.source == "mcp":
        return await service.discovery.discover_mcp_servers(body.query, limit=body.limit)
    return await service.discovery.discover_agent_skills(
        body.query, limit=body.limit, owner=body.owner
    )


@router.post("/inspect")
async def inspect(
    body: ExternalCandidateInspectionRequest, principal: Principal, service: Service
) -> ExternalCandidateInspectionReport:
    permitted(principal, "catalog:inspect")
    if service.inspection is None:
        raise HTTPException(503, detail={"code": "catalog_inspection_unavailable"})
    return await service.inspection.inspect(
        InspectionPrincipal(
            actor_id=principal.actor.actor_id,
            tenant_scope=service.request_scope,
            roles=frozenset({"coordinator_planner"}),
        ),
        body,
    )


@router.post("/components/search")
async def component_search(
    body: ComponentQuery, principal: Principal, service: Service
) -> ComponentsResponse:
    permitted(principal, "catalog:read")
    if service.components is None:
        raise HTTPException(503, detail={"code": "component_catalog_unavailable"})
    return ComponentsResponse(components=await service.components.search(body))


def _custody(
    principal: MissionPrincipal, service: CatalogService, body: PublishPrepareRequest
) -> BundleCustodyService:
    permitted(principal, "catalog:publish")
    if service.custody is None:
        raise HTTPException(503, detail={"code": "catalog_publish_unavailable"})
    if body.manifest.application_id != principal.application_id:
        raise HTTPException(403, detail={"code": "catalog_scope_denied"})
    return service.custody


@router.post("/publish:prepare")
async def publish_prepare(
    body: PublishPrepareRequest, principal: Principal, service: Service
) -> PublishPlan:
    """Signed-URL handshake (FT-A2): a signed upload URL per object not yet stored."""
    return await _custody(principal, service, body).prepare(body.manifest)


@router.post("/publish:complete")
async def publish_complete(
    body: PublishPrepareRequest, principal: Principal, service: Service
) -> PublishResult:
    """Verify every stored object against the manifest, then register a proposed row."""
    custody = _custody(principal, service, body)
    try:
        return await custody.complete(body.manifest, body.definition)
    except CapabilityDrift as error:
        raise HTTPException(
            409, detail={"code": CapabilityDrift.code, "message": str(error)}
        ) from None
    except BundleCustodyConflict as error:
        raise HTTPException(
            409, detail={"code": "bundle_conflict", "message": str(error)}
        ) from None
    except ValueError as error:
        raise HTTPException(
            422, detail={"code": "invalid_bundle_definition", "message": str(error)}
        ) from None
