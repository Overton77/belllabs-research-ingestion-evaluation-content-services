"""Lane listing and describe routes (SPEC-07 Interfaces; FT-G1).

`GET /v1/applications/{app}/lanes` lists the registered Lane Profiles with their
`mc.lane_describe.v1` matrices; `GET .../lanes/{profile}` returns one. Both are read-only,
authenticated like every public route, and never reveal whether a credential is bound beyond
the fact that a profile is registered.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from mission_control.application.execution.harness.registry import (
    LaneRegistry,
    UnknownLaneProfile,
)
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
)

router = APIRouter(prefix="/v1/applications/{application_id}/lanes", tags=["lanes"])
Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]


class LaneSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    lane_profile: str
    lane: str
    placement: str
    qualified: bool
    describe_digest: str


class LaneList(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    lanes: tuple[LaneSummary, ...]
    allow_unqualified: bool


def lane_registry(application_id: str, request: Request, principal: Principal) -> LaneRegistry:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_lanes", None)
    if not isinstance(registry, LaneRegistry):
        raise HTTPException(503, detail={"code": "lanes_unavailable"})
    return registry


Registry = Annotated[LaneRegistry, Depends(lane_registry)]


@router.get("", response_model=LaneList)
def list_lanes(registry: Registry) -> LaneList:
    return LaneList(
        lanes=tuple(
            LaneSummary(
                lane_profile=describe.lane_profile,
                lane=describe.lane,
                placement=describe.placement,
                qualified=describe.qualified,
                describe_digest=describe.digest,
            )
            for describe in registry.describe_all()
        ),
        allow_unqualified=registry.allow_unqualified,
    )


@router.get("/{lane_profile}")
def describe_lane(lane_profile: str, registry: Registry) -> dict[str, Any]:
    try:
        describe = registry.describe(lane_profile)
    except UnknownLaneProfile:
        raise HTTPException(404, detail={"code": "lane_profile_not_registered"}) from None
    return describe.model_dump(mode="json")
