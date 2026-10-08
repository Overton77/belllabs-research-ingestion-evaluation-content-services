"""Mission Manifest verbs over HTTP (SPEC-05 "Interfaces").

- `POST /v1/applications/{app}/missions:compile` body `{manifest_yaml}`: 200 with the
  Validation Report and the `mc.manifest_resolution.v1` document, 422 with the report when it
  has blockers. Nothing is persisted.
- `POST .../missions:submit` body `{manifest_yaml, request_id}`: 201 with the mission, revision
  and run ids (or the chain id and members); 200 on an exact replay or an unchanged head; 409
  `IDEMPOTENCY_CONFLICT`; 422 with the blockers. Nothing is started.
- `POST .../missions:start` body `{run_id, family_input?}` and `POST .../missions/{id}/runs`
  (the admitted run of the head revision): the governed launch; registers the manifest's
  `controls.subscriptions` first. 202.

Authentication and the application/tenant scope are verified exactly as the Mission Control
router does; the file's `application` is checked against that scope by the compile itself.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.authoring.manifest_service import (
    ManifestCompilation,
    ManifestPermissionDenied,
    MissionManifestService,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestBlocked,
    ManifestIdempotencyConflict,
    ManifestStartUnavailable,
    SubmitRequest,
)
from mission_control.application.execution.run_launch import RunLaunchRejected
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["mission-manifests"])
MAX_MANIFEST_CHARS = 512_000


class ManifestBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_yaml: str = Field(min_length=1, max_length=MAX_MANIFEST_CHARS)


def get_manifest_service(
    application_id: str,
    request: Request,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
) -> MissionManifestService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_manifest_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, MissionManifestService):
        raise HTTPException(status_code=503, detail={"code": "manifests_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]
Service = Annotated[MissionManifestService, Depends(get_manifest_service)]


def compilation_body(application_id: str, compilation: ManifestCompilation) -> dict[str, Any]:
    return {
        "application_id": application_id,
        "report": compilation.report.model_dump(mode="json", by_alias=True),
    }


@router.post("/missions:compile")
async def compile_manifest(
    body: ManifestBody, response: Response, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        compilation = await service.compile(
            body.manifest_yaml,
            actor_id=principal.actor.actor_id,
            permissions=principal.actor.permissions,
        )
    except ManifestPermissionDenied:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    response.status_code = 200 if compilation.ok else 422
    return compilation_body(principal.application_id, compilation)


class SubmitBody(ManifestBody):
    request_id: UUID


class StartBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str | None = Field(default=None, min_length=1, max_length=512)
    family_input: dict[str, Any] | None = None


def _lifecycle(service: MissionManifestService) -> Any:
    if service.lifecycle is None:
        raise HTTPException(status_code=503, detail={"code": "manifest_submit_unavailable"})
    return service.lifecycle


def _blocked(error: ManifestBlocked, application_id: str) -> HTTPException:
    issues = [issue.model_dump(mode="json") for issue in error.issues]
    report = (
        error.compilation.report.model_dump(mode="json", by_alias=True)
        if error.compilation is not None
        else None
    )
    return HTTPException(
        status_code=422,
        detail={
            "code": "manifest_blocked",
            "application_id": application_id,
            "blockers": issues,
            "report": report,
        },
    )


@router.post("/missions:submit")
async def submit_manifest(
    body: SubmitBody,
    response: Response,
    principal: Principal,
    service: Service,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict[str, Any]:
    """Compile, commit the revision(s) and admit the run (or chain) without starting it."""

    if idempotency_key is not None and idempotency_key != str(body.request_id):
        raise HTTPException(status_code=409, detail={"code": "idempotency_key_mismatch"})
    lifecycle = _lifecycle(service)
    try:
        receipt, replayed = await lifecycle.submit(
            SubmitRequest(
                manifest_yaml=body.manifest_yaml,
                request_id=body.request_id,
                actor=principal.actor,
                sponsorship_refs=principal.sponsorship_refs,
                approval_refs=principal.approval_refs,
            )
        )
    except ManifestPermissionDenied:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    except ManifestIdempotencyConflict:
        raise HTTPException(status_code=409, detail={"code": "IDEMPOTENCY_CONFLICT"}) from None
    except ManifestBlocked as error:
        raise _blocked(error, principal.application_id) from None
    response.status_code = 200 if replayed or receipt.unchanged else 201
    return {"application_id": principal.application_id, **receipt.model_dump(mode="json")}


async def _start(
    service: MissionManifestService, principal: MissionPrincipal, run_id: str, body: StartBody
) -> dict[str, Any]:
    lifecycle = _lifecycle(service)
    try:
        receipt = await lifecycle.start(run_id, principal.actor, family_input=body.family_input)
    except ManifestPermissionDenied:
        raise HTTPException(status_code=403, detail={"code": "unauthorized"}) from None
    except ManifestStartUnavailable as error:
        raise HTTPException(
            status_code=409, detail={"code": "start_unavailable", "message": str(error)}
        ) from None
    except RunLaunchRejected as error:
        raise HTTPException(
            status_code=409, detail={"code": error.code, "message": error.message}
        ) from None
    return {"application_id": principal.application_id, **receipt.model_dump(mode="json")}


@router.post("/missions:start", status_code=202)
async def start_run(body: StartBody, principal: Principal, service: Service) -> dict[str, Any]:
    """Start an admitted manifest run by run id (``missionctl mission start RUN_ID``)."""

    if body.run_id is None:
        raise HTTPException(status_code=422, detail={"code": "run_id_required"})
    return await _start(service, principal, body.run_id, body)


@router.post("/missions/{mission_id}/runs", status_code=202)
async def start_mission_run(
    mission_id: UUID, body: StartBody, principal: Principal, service: Service
) -> dict[str, Any]:
    """Alias of launch for the admitted run of the mission's head revision."""

    run_id = body.run_id or await _lifecycle(service).head_run(mission_id)
    if run_id is None:
        raise HTTPException(status_code=404, detail={"code": "resource_not_found"})
    return await _start(service, principal, run_id, body)
