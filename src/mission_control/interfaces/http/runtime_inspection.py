"""`/run-control/v1/inspection`: scoped, non-mutating runtime inspection (REQ-CP-RUN-011/012).

Every route is a GET, requires `workflow_run.read`, and is bound to one request scope
the principal holds; an unknown run and a run outside the caller's scope are the same
404. The redacted checkpoint state summary additionally requires the distinct
`workflow_run.read_checkpoint_summary` permission. Responses follow
`CON-CP-INSPECTION-READ-V1`. Reconciliation of `in_doubt` units is not offered here: it
is the separate, privileged `reconcile_unit` lifecycle command.

The schema-export route `/v2/graph-runtime/schemas` is not an inspection endpoint.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from mission_control.adapters.postgres.run_control.inspection_repository import (
    PostgresInspectionReadRepository,
)
from mission_control.application.execution.inspection import (
    AsyncChildDetailReader,
    CheckpointHistoryReader,
    RuntimeInspectionService,
    TemporalVisibilityReader,
)
from mission_control.application.execution.inspection_cursor import InspectionCursorCodec
from mission_control.domain.policies.contracts import RunPhase
from mission_control.domain.policies.inspection import (
    CHECKPOINT_SUMMARY_PERMISSION,
    INSPECTION_READ_PERMISSION,
    CheckpointHistoryPage,
    InspectionRead,
    RedactedCheckpointStateSummary,
    RunInspection,
    RunListPage,
    UnitInspection,
)
from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from mission_control.interfaces.http.run_control import (
    initialize_run_control_resources,
    principal_permissions,
)

router = APIRouter(prefix="/run-control/v1/inspection", tags=["run-inspection"])
_composition_lock = asyncio.Lock()


async def get_runtime_inspection_service(request: Request) -> RuntimeInspectionService:
    """Compose inspection over the application pool and the optional qualified sources.

    Deployments attach the qualified runtime sources on `app.state` before first use:
    `temporal_visibility_reader`, `inspection_checkpoint_reader`,
    `inspection_async_child_details`, and a shared `inspection_cursor_key` for
    multi-replica pagination. Each one that is absent is reported `unavailable`.
    """

    state = request.app.state
    service = getattr(state, "runtime_inspection_service", None)
    if service is not None:
        return service
    await initialize_run_control_resources(request.app)
    pool: asyncpg.Pool | None = getattr(state, "run_control_postgres_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=503, detail="application PostgreSQL authority is not configured"
        )
    async with _composition_lock:
        service = getattr(state, "runtime_inspection_service", None)
        if service is not None:
            return service
        visibility: TemporalVisibilityReader | None = getattr(
            state, "temporal_visibility_reader", None
        )
        checkpoints: CheckpointHistoryReader | None = getattr(
            state, "inspection_checkpoint_reader", None
        )
        details: AsyncChildDetailReader | None = getattr(
            state, "inspection_async_child_details", None
        )
        service = RuntimeInspectionService(
            PostgresInspectionReadRepository(pool),
            cursors=InspectionCursorCodec(getattr(state, "inspection_cursor_key", None)),
            visibility=visibility,
            checkpoints=checkpoints,
            async_details=details,
        )
        state.runtime_inspection_service = service
        return service


Inspection = Annotated[RuntimeInspectionService, Depends(get_runtime_inspection_service)]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]


def _authorize(principal: ControlPlanePrincipal, request_scope: str, *, permission: str) -> None:
    if request_scope not in principal.tenant_scopes:
        raise HTTPException(status_code=404, detail="workflow run not found")
    if permission not in principal_permissions(principal):
        raise HTTPException(status_code=403, detail=f"{permission} permission required")


@router.get("/runs", response_model=InspectionRead[RunListPage])
async def list_runs(
    request_scope: str,
    principal: Principal,
    inspection: Inspection,
    phase: Annotated[list[RunPhase] | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    cursor: str | None = None,
) -> InspectionRead[RunListPage]:
    _authorize(principal, request_scope, permission=INSPECTION_READ_PERMISSION)
    return await inspection.list_runs(
        request_scope, phases=frozenset(phase or ()), limit=limit, cursor=cursor
    )


@router.get("/runs/{run_id}", response_model=InspectionRead[RunInspection])
async def get_run(
    run_id: str, request_scope: str, principal: Principal, inspection: Inspection
) -> InspectionRead[RunInspection]:
    _authorize(principal, request_scope, permission=INSPECTION_READ_PERMISSION)
    return await inspection.get_run(request_scope, run_id)


@router.get("/runs/{run_id}/units/{unit_key}", response_model=InspectionRead[UnitInspection])
async def get_unit(
    run_id: str,
    unit_key: str,
    request_scope: str,
    principal: Principal,
    inspection: Inspection,
) -> InspectionRead[UnitInspection]:
    _authorize(principal, request_scope, permission=INSPECTION_READ_PERMISSION)
    return await inspection.get_unit(request_scope, run_id, unit_key)


@router.get(
    "/runs/{run_id}/units/{unit_key}/checkpoints",
    response_model=InspectionRead[CheckpointHistoryPage],
)
async def get_checkpoint_history(
    run_id: str,
    unit_key: str,
    request_scope: str,
    principal: Principal,
    inspection: Inspection,
    execution_generation: Annotated[int | None, Query(ge=1)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    cursor: str | None = None,
) -> InspectionRead[CheckpointHistoryPage]:
    _authorize(principal, request_scope, permission=INSPECTION_READ_PERMISSION)
    return await inspection.get_checkpoint_history(
        request_scope,
        run_id,
        unit_key,
        execution_generation=execution_generation,
        limit=limit,
        cursor=cursor,
    )


@router.get(
    "/runs/{run_id}/units/{unit_key}/checkpoints/{checkpoint_id}/summary",
    response_model=InspectionRead[RedactedCheckpointStateSummary],
)
async def get_checkpoint_summary(
    run_id: str,
    unit_key: str,
    checkpoint_id: str,
    request_scope: str,
    principal: Principal,
    inspection: Inspection,
    execution_generation: Annotated[int | None, Query(ge=1)] = None,
) -> InspectionRead[RedactedCheckpointStateSummary]:
    _authorize(principal, request_scope, permission=INSPECTION_READ_PERMISSION)
    if CHECKPOINT_SUMMARY_PERMISSION not in principal_permissions(principal):
        raise HTTPException(
            status_code=403,
            detail=f"{CHECKPOINT_SUMMARY_PERMISSION} permission required",
        )
    return await inspection.get_checkpoint_summary(
        request_scope,
        run_id,
        unit_key,
        checkpoint_id,
        execution_generation=execution_generation,
    )
