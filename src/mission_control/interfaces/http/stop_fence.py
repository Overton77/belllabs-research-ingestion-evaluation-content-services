"""Immediate-cancel Delivery Report route (FT-F3).

`GET /v1/applications/{app}/runs/{run_id}/stop-fence` returns the run's Stop Fence report:
the immediate cancel that wrote it and the requested, fence-persisted, provider-acknowledged
and settled timestamps (SPEC-06). FT-F6 folds the same report into run inspection.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from mission_control.interfaces.http.mission_control import (
    _EXPECTED_ERRORS,
    Principal,
    Service,
    _error,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["mission-control"])


@router.get("/runs/{run_id}/stop-fence")
async def stop_fence_report(run_id: str, principal: Principal, service: Service) -> dict[str, Any]:
    try:
        report = await service.stop_fence_report(run_id, principal.actor)
    except _EXPECTED_ERRORS as exc:
        raise _error(exc) from None
    if report is None:
        raise HTTPException(status_code=404, detail={"code": "stop_fence_not_found"})
    return {
        "application_id": principal.application_id,
        "state": report.state,
        **report.model_dump(mode="json"),
    }
