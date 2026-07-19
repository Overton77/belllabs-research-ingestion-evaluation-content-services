from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from pydantic import TypeAdapter

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
    get_control_plane_service,
)
from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control import (
    F1RunConfigurationVerifier,
    RunControlService,
)
from app.config import get_settings
from app.domain.run_control.contracts import (
    ActorContext,
    AdmissionDecision,
    BudgetState,
    CommandResult,
    LifecycleCommand,
    LifecycleTransitionRecord,
    OutboxRecord,
    RunProjection,
    RunRequest,
)
from app.integrations.postgres import (
    apply_application_migrations,
    create_application_migration_pool,
    create_application_postgres_pool,
)

router = APIRouter(prefix="/run-control/v1", tags=["run-control"])
_initialization_lock = asyncio.Lock()

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "operator": frozenset(
        {
            "workflow_run.admit",
            "workflow_run.start",
            "workflow_run.observe_wait",
            "workflow_run.pause",
            "workflow_run.resume",
            "workflow_run.cancel",
            "workflow_run.reserve_budget",
            "workflow_run.report_usage",
            "workflow_run.settle_usage",
            "workflow_run.propose_continuation",
            "workflow_run.decide_continuation",
            "workflow_run.accept_finalization",
            "workflow_run.record_finalization",
            "workflow_run.accept_obligation_evidence",
            "workflow_run.accept_output_evidence",
            "workflow_run.terminalize",
            "workflow_run.decide_readiness",
            "workflow_run.read",
        }
    ),
    "scheduler": frozenset(
        {
            "workflow_run.start",
            "workflow_run.observe_wait",
            "workflow_run.reserve_budget",
            "workflow_run.report_usage",
            "workflow_run.settle_usage",
            "workflow_run.propose_continuation",
            "workflow_run.accept_finalization",
            "workflow_run.record_finalization",
            "workflow_run.terminalize",
            "workflow_run.read",
        }
    ),
    "auditor": frozenset({"workflow_run.read"}),
    "relay": frozenset({"workflow_run.relay"}),
}


async def initialize_run_control_resources(application: FastAPI) -> None:
    state = application.state
    if getattr(state, "run_control_postgres_pool", None) is not None:
        return
    settings = get_settings()
    if not settings.has_application_postgres:
        return
    async with _initialization_lock:
        if getattr(state, "run_control_postgres_pool", None) is not None:
            return
        migration_pool = await create_application_migration_pool(settings)
        try:
            await apply_application_migrations(migration_pool)
        finally:
            await migration_pool.close()
        pool = await create_application_postgres_pool(settings)
        state.run_control_postgres_pool = pool


async def get_run_control_service(request: Request) -> RunControlService:
    service = getattr(request.app.state, "run_control_service", None)
    if service is not None:
        return service
    await initialize_run_control_resources(request.app)
    pool: asyncpg.Pool | None = getattr(request.app.state, "run_control_postgres_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="application PostgreSQL authority is not configured",
        )
    async with _initialization_lock:
        service = getattr(request.app.state, "run_control_service", None)
        if service is not None:
            return service
        control_plane = await get_control_plane_service(request)
        policies = getattr(request.app.state, "admission_policy_registry", None)
        if policies is None:
            raise HTTPException(
                status_code=503,
                detail="executable admission policy registry is not configured",
            )
        service = RunControlService(
            PostgresRunControlRepository(pool),
            F1RunConfigurationVerifier(control_plane),
            policies,
        )
        request.app.state.run_control_service = service
        return service


async def close_run_control_resources(application: FastAPI) -> None:
    state = application.state
    pool = getattr(state, "run_control_postgres_pool", None)
    if pool is not None:
        await pool.close()
        state.run_control_postgres_pool = None


def _principal_permissions(principal: ControlPlanePrincipal) -> frozenset[str]:
    return frozenset(
        permission for role in principal.roles for permission in ROLE_PERMISSIONS.get(role, ())
    )


def _authorize_actor(
    principal: ControlPlanePrincipal,
    actor_id: str,
    asserted_permissions: frozenset[str],
    asserted_authority_refs: frozenset[str],
) -> ActorContext:
    if principal.actor_id != actor_id:
        raise HTTPException(status_code=403, detail="actor identity mismatch")
    granted = _principal_permissions(principal)
    if not asserted_permissions <= granted:
        raise HTTPException(status_code=403, detail="asserted permission was not granted")
    if not asserted_authority_refs <= principal.authority_refs:
        raise HTTPException(status_code=403, detail="asserted authority was not granted")
    return ActorContext(
        actor_id=principal.actor_id,
        permissions=granted,
        authority_refs=principal.authority_refs,
    )


def _authorize_scope(principal: ControlPlanePrincipal, request_scope: str) -> None:
    if request_scope not in principal.tenant_scopes:
        raise HTTPException(status_code=404, detail="workflow run not found")


def _authorize_read(principal: ControlPlanePrincipal) -> None:
    if "workflow_run.read" not in _principal_permissions(principal):
        raise HTTPException(status_code=403, detail="workflow run read permission required")


Service = Annotated[RunControlService, Depends(get_run_control_service)]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]


@router.post("/run-requests", response_model=AdmissionDecision, status_code=201)
async def admit_run(
    run_request: RunRequest, principal: Principal, service: Service
) -> AdmissionDecision:
    _authorize_scope(principal, run_request.request_scope)
    if run_request.idempotency_issuer != principal.actor_id:
        raise HTTPException(status_code=403, detail="idempotency issuer mismatch")
    trusted_actor = _authorize_actor(
        principal,
        run_request.actor.actor_id,
        run_request.actor.permissions,
        run_request.actor.authority_refs,
    )
    if run_request.sponsorship_ref not in principal.sponsorship_refs:
        raise HTTPException(status_code=403, detail="sponsorship was not granted")
    if not set(run_request.approval_refs) <= principal.approval_refs:
        raise HTTPException(status_code=403, detail="approval was not granted")
    return await service.admit(
        run_request.model_copy(update={"actor": trusted_actor, "requested_at": datetime.now(UTC)})
    )


@router.post("/runs/{run_id}/commands", response_model=CommandResult)
async def execute_command(
    run_id: str,
    command: LifecycleCommand,
    principal: Principal,
    service: Service,
) -> CommandResult:
    if command.run_id != run_id:
        raise HTTPException(status_code=422, detail="path and command run ids differ")
    _authorize_scope(principal, command.request_scope)
    if command.idempotency_issuer != principal.actor_id:
        raise HTTPException(status_code=403, detail="idempotency issuer mismatch")
    trusted_actor = _authorize_actor(
        principal,
        command.actor.actor_id,
        command.actor.permissions,
        command.actor.authority_refs,
    )
    return await service.execute(
        command.model_copy(update={"actor": trusted_actor, "occurred_at": datetime.now(UTC)})
    )


@router.get("/runs/{run_id}", response_model=RunProjection)
async def get_run(
    run_id: str, request_scope: str, principal: Principal, service: Service
) -> RunProjection:
    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await service.get_run(request_scope, run_id)


@router.get("/runs/{run_id}/budget", response_model=BudgetState)
async def get_budget(
    run_id: str, request_scope: str, principal: Principal, service: Service
) -> BudgetState:
    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await service.get_budget(request_scope, run_id)


@router.get(
    "/runs/{run_id}/transitions",
    response_model=tuple[LifecycleTransitionRecord, ...],
)
async def get_transitions(
    run_id: str, request_scope: str, principal: Principal, service: Service
) -> tuple[LifecycleTransitionRecord, ...]:
    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await service.list_transitions(request_scope, run_id)


@router.get("/outbox", response_model=tuple[OutboxRecord, ...])
async def get_outbox(
    request_scope: str, principal: Principal, service: Service, limit: int = 100
) -> tuple[OutboxRecord, ...]:
    _authorize_scope(principal, request_scope)
    if "workflow_run.relay" not in _principal_permissions(principal):
        raise HTTPException(status_code=403, detail="outbox relay permission required")
    return await service.pending_outbox(request_scope, limit=min(max(limit, 1), 1000))


@router.get("/schemas")
async def run_control_schemas() -> dict[str, object]:
    return {
        "run_request": RunRequest.model_json_schema(),
        "admission_decision": AdmissionDecision.model_json_schema(),
        "lifecycle_command": LifecycleCommand.model_json_schema(),
        "command_result": CommandResult.model_json_schema(),
        "run_projection": RunProjection.model_json_schema(),
        "budget_state": BudgetState.model_json_schema(),
        "transition": LifecycleTransitionRecord.model_json_schema(),
        "outbox_record": OutboxRecord.model_json_schema(),
        "lifecycle_action": TypeAdapter(LifecycleCommand).json_schema(),
    }
