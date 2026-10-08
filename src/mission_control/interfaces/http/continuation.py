"""Continuation checkpoints over HTTP (SPEC-02 Interfaces, FT-B4).

- ``GET /v1/applications/{app}/runs/{run_id}/checkpoints`` lists a run's sealed
  ``mc.continuation_checkpoint.v1`` checkpoints (valid and invalid: an invalid one is
  evidence, never a hydration source).
- ``GET .../runs/{run_id}/checkpoints/{checkpoint_id}[?full=true]`` returns one.

Bodies above 4 KiB are replaced by their digest and size unless ``full=true``.
Authentication and the application/tenant scope are verified exactly as the Mission
Control router does; ``workflow_run.read`` is required. ``request_continuation`` itself is
a command on ``POST .../runs/{run_id}/commands``.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from mission_control.application.context.continuation import (
    CheckpointNotFound,
    CheckpointReadDenied,
    CheckpointReadService,
)
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["mission-continuation"])


def get_checkpoint_service(
    application_id: str,
    request: Request,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
) -> CheckpointReadService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_checkpoint_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, CheckpointReadService):
        raise HTTPException(status_code=503, detail={"code": "checkpoints_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]
Service = Annotated[CheckpointReadService, Depends(get_checkpoint_service)]


@router.get("/runs/{run_id}/checkpoints")
async def list_checkpoints(run_id: str, principal: Principal, service: Service) -> dict[str, Any]:
    try:
        views = await service.list(run_id, actor=principal.actor)
    except CheckpointReadDenied:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    return {
        "application_id": principal.application_id,
        "run_id": run_id,
        "checkpoints": [view.model_dump(mode="json") for view in views],
    }


@router.get("/runs/{run_id}/checkpoints/{checkpoint_id}")
async def get_checkpoint(
    run_id: str,
    checkpoint_id: str,
    principal: Principal,
    service: Service,
    full: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    try:
        view = await service.get(run_id, checkpoint_id, actor=principal.actor, full=full)
    except CheckpointReadDenied:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    except CheckpointNotFound:
        raise HTTPException(status_code=404, detail={"code": "resource_not_found"}) from None
    return {"application_id": principal.application_id, **view.model_dump(mode="json")}
