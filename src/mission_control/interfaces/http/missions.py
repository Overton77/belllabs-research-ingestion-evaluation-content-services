"""Mission Manifest verbs over HTTP (SPEC-05 "Interfaces").

- `POST /v1/applications/{app}/missions:compile` body `{manifest_yaml}`: 200 with the
  Validation Report and the `mc.manifest_resolution.v1` document, 422 with the report when it
  has blockers. Nothing is persisted.

Authentication and the application/tenant scope are verified exactly as the Mission Control
router does; the file's `application` is checked against that scope by the compile itself.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.authoring.manifest_service import (
    ManifestCompilation,
    ManifestPermissionDenied,
    MissionManifestService,
)
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
