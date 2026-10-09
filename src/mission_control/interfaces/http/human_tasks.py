"""Human Tasks over HTTP (SPEC-03 "Transport and authorization", MP-10).

``GET /human-tasks`` lists, ``GET /human-tasks/{id}`` reads and ``POST
/human-tasks/{id}/resolutions`` is the one mutation path, all under the application prefix and
the same authenticated principal as the rest of the public API. Reviewer authorization is
the service's (``HumanTaskService``); MCP calls the same service. A retry of the same
``request_id`` returns the stored resolution (200, ``duplicate``); a stale version, another
packet digest, a non-reviewer or a second different answer is refused with a typed code.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from mission_control.application.human_tasks.service import (
    HumanTaskRejected,
    HumanTaskService,
)
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
    require_strict_json_body,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["human-tasks"])
Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]

REJECTION_STATUS: dict[str, int] = {
    "not_found": 404,
    "not_reviewer": 403,
    "stale_version": 409,
    "packet_digest_mismatch": 409,
    "already_resolved": 409,
    "task_expired": 409,
    "task_cancelled": 409,
    "deadline_passed": 409,
    "decision_not_admitted": 422,
    "feedback_required": 422,
    "invalid_filter": 422,
}


def get_human_task_service(
    application_id: str, request: Request, principal: Principal
) -> HumanTaskService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_human_task_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, HumanTaskService):
        raise HTTPException(status_code=503, detail={"code": "human_tasks_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Service = Annotated[HumanTaskService, Depends(get_human_task_service)]


def _error(exc: HumanTaskRejected) -> HTTPException:
    return HTTPException(
        status_code=REJECTION_STATUS.get(exc.code, 422),
        detail={"code": exc.code, "message": str(exc)},
    )


@router.get("/human-tasks")
async def list_human_tasks(
    principal: Principal,
    service: Service,
    run_id: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
    lifecycle: Annotated[str | None, Query(max_length=32)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> dict[str, Any]:
    try:
        tasks = await service.list(principal.actor, run_id=run_id, lifecycle=lifecycle, limit=limit)
    except HumanTaskRejected as exc:
        raise _error(exc) from None
    return {
        "application_id": principal.application_id,
        "human_tasks": [task.public() for task in tasks],
    }


@router.get("/human-tasks/{human_task_id}")
async def get_human_task(
    human_task_id: str, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        task = await service.get(human_task_id, principal.actor)
    except HumanTaskRejected as exc:
        raise _error(exc) from None
    return {"application_id": principal.application_id, **task.public()}


@router.post(
    "/human-tasks/{human_task_id}/resolutions",
    dependencies=[Depends(require_strict_json_body)],
)
async def resolve_human_task(
    human_task_id: str,
    body: HumanResolutionRequest,
    principal: Principal,
    service: Service,
) -> dict[str, Any]:
    try:
        receipt = await service.resolve(human_task_id, body, principal.actor)
    except HumanTaskRejected as exc:
        raise _error(exc) from None
    return {"application_id": principal.application_id, **receipt.public()}


__all__ = ["REJECTION_STATUS", "get_human_task_service", "router"]
