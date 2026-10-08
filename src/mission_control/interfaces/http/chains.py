"""Mission Chains over HTTP (SPEC-04 "Interfaces", FT-D2).

- `GET /v1/applications/{app}/chains/{chain_id}`: the `mc.chain.v1` projection with every
  member's current run (`mc.chain_inspection.v1`).
- `GET /v1/applications/{app}/chains?mission_id=`: the chains a mission belongs to.

Authentication and the application/tenant scope are verified exactly as the Mission Control
router does; `workflow_run.read` is required. Read-only.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request

from mission_control.application.chains.service import ChainInspectionService, ChainNotFound
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["mission-chains"])


def get_chain_service(
    application_id: str,
    request: Request,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
) -> ChainInspectionService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_chain_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, ChainInspectionService):
        raise HTTPException(status_code=503, detail={"code": "chains_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]
Service = Annotated[ChainInspectionService, Depends(get_chain_service)]


@router.get("/chains/{chain_id}")
async def inspect_chain(chain_id: UUID, principal: Principal, service: Service) -> dict[str, Any]:
    try:
        inspection = await service.inspect(chain_id, principal.actor)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    except ChainNotFound:
        raise HTTPException(status_code=404, detail={"code": "resource_not_found"}) from None
    return {"application_id": principal.application_id, **inspection.model_dump(mode="json")}


@router.get("/chains")
async def list_chains(mission_id: UUID, principal: Principal, service: Service) -> dict[str, Any]:
    try:
        found = await service.chains_for_mission(mission_id, principal.actor)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    return {
        "application_id": principal.application_id,
        "chains": [item.model_dump(mode="json") for item in found],
    }
