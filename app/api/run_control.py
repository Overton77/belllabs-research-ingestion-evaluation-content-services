from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
    get_control_plane_service,
)
from app.application.async_subagents.service import (
    AsyncSubagentDecisionRejected,
    AsyncSubagentError,
)
from app.application.async_subagents.usage_reconciliation import AsyncChildUsageReconciliation
from app.application.operations.operation_recovery_composition import (
    compose_postgres_operation_recovery,
)
from app.application.operations.operation_submission import GenericArtifactSubmissionPort
from app.application.operations.unit_reconciliation import (
    UnitReconciliationRejected,
    UnitReconciliationService,
)
from app.application.run_control.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryCommandTransport,
    BoundaryInterventionService,
)
from app.application.run_control.liability_hints import (
    LIABILITY_DECISION_KINDS,
    FamilyLiabilityHints,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.run_control_repository import RunControlRepository
from app.application.run_control.run_launch import (
    LAUNCH_PERMISSION,
    RunLaunchReceipt,
    RunLaunchRejected,
    RunLaunchRequest,
    RunLaunchService,
)
from app.application.run_control.schema_grounding_admission import (
    register_schema_grounding_admission_policies,
)
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    FamilyAdmissionRegistry,
    RunConfigurationVerifier,
    RunControlService,
)
from app.application.run_control.web_research_admission import (
    register_web_research_admission_policies,
)
from app.config import get_settings
from app.domain.operation_execution.async_subagent_reconciliation import (
    ASYNC_CHILD_RECONCILE_PERMISSION,
)
from app.domain.operation_execution.contracts import (
    AsyncSubagentUsage,
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
    ParentAsyncSubagentLink,
)
from app.domain.run_control.contracts import (
    ActorContext,
    AdmissionDecision,
    BoundaryCommandStatus,
    BudgetState,
    CommandResult,
    CommandStatus,
    EffectLedgerEntry,
    EffectLedgerState,
    LifecycleCommand,
    LifecycleTransitionRecord,
    OutboxRecord,
    ReconcileUnitAction,
    RunPhase,
    RunProjection,
    RunRequest,
)
from app.integrations.postgres import (
    apply_application_migrations,
    create_application_family_writer_pool,
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
            "workflow_run.claim_effect",
            "workflow_run.observe_effect",
            "workflow_run.settle_effect",
            "workflow_run.register_async_child",
            "workflow_run.observe_async_child",
            "workflow_run.decide_async_child",
            "workflow_run.propose_continuation",
            "workflow_run.decide_continuation",
            "workflow_run.accept_finalization",
            "workflow_run.record_finalization",
            "workflow_run.accept_obligation_evidence",
            "workflow_run.accept_output_evidence",
            "workflow_run.terminalize",
            "workflow_run.decide_readiness",
            "workflow_run.read",
            "workflow_run.execute_operation",
        }
    ),
    "scheduler": frozenset(
        {
            "workflow_run.start",
            "workflow_run.observe_wait",
            "workflow_run.reserve_budget",
            "workflow_run.report_usage",
            "workflow_run.settle_usage",
            "workflow_run.claim_effect",
            "workflow_run.observe_effect",
            "workflow_run.settle_effect",
            "workflow_run.register_async_child",
            "workflow_run.observe_async_child",
            "workflow_run.propose_continuation",
            "workflow_run.accept_finalization",
            "workflow_run.record_finalization",
            "workflow_run.terminalize",
            "workflow_run.read",
            "workflow_run.execute_operation",
        }
    ),
    "auditor": frozenset({"workflow_run.read"}),
    # REQ-CP-RUN-012: redacted checkpoint state summaries are separately authorized; no
    # other role holds the summary permission by default.
    "state_inspector": frozenset(
        {"workflow_run.read", "workflow_run.read_checkpoint_summary"}
    ),
    # RRM-007 (RRM-004 review): `reconcile_unit` is privileged; the plain operator role does
    # not hold it. This role decides `in_doubt` units through the governed route only.
    # RRM-008 composed by RRM-009: the privileged usage reconciliation of a cancelled async
    # child (`workflow_run.reconcile_async_child`) belongs to the same privileged role.
    "reconciliation_operator": frozenset(
        {
            "workflow_run.read",
            "workflow_run.reconcile_unit",
            "workflow_run.reconcile_async_child",
        }
    ),
    "relay": frozenset({"workflow_run.relay"}),
    # REQ-CP-EXEC-012/016 (RRM-006): snapshots and forks are separately authorized; no
    # existing role holds them. A fork also admits a run, so it needs `workflow_run.admit`.
    "fork_operator": frozenset(
        {"workflow_run.read", "workflow_run.snapshot", "workflow_run.fork"}
    ),
}


def configure_family_admission_registry(
    application: FastAPI,
    registry: FamilyAdmissionRegistry,
) -> None:
    """Inject exact family mutation policies before API service composition."""

    state = application.state
    if getattr(state, "run_control_service", None) is not None:
        raise RuntimeError("family admission registry must be configured before run control")
    prior = getattr(state, "run_control_family_admission_registry", None)
    if prior is not None and prior is not registry:
        raise RuntimeError("family admission registry is already configured")
    state.run_control_family_admission_registry = registry


def compose_api_run_control_service(
    application: FastAPI,
    repository: RunControlRepository,
    configuration_verifier: RunConfigurationVerifier,
    policies: AdmissionPolicyRegistry,
) -> RunControlService:
    """Build API run control with the public, optional family registry hook."""

    family_admissions = getattr(
        application.state,
        "run_control_family_admission_registry",
        None,
    )
    return RunControlService(
        repository,
        configuration_verifier,
        policies,
        family_admissions,
    )


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
        family_writer_pool = None
        if settings.has_application_family_writer_postgres:
            try:
                family_writer_pool = await create_application_family_writer_pool(settings)
            except Exception:
                await pool.close()
                raise
        state.run_control_postgres_pool = pool
        state.run_control_family_writer_pool = family_writer_pool


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
            policies = AdmissionPolicyRegistry()
            register_schema_grounding_admission_policies(policies)
            register_web_research_admission_policies(policies)
            request.app.state.admission_policy_registry = policies
        service = compose_api_run_control_service(
            request.app,
            PostgresRunControlRepository(
                pool,
                family_writer_pool=getattr(
                    request.app.state, "run_control_family_writer_pool", None
                ),
            ),
            F1RunConfigurationVerifier(control_plane),
            policies,
        )
        request.app.state.run_control_service = service
        return service


async def get_boundary_intervention_service(request: Request) -> BoundaryInterventionService:
    """The governed intervention facade over run control (RRM-007).

    Deployments attach `boundary_command_transport` (a `TemporalBoundaryCommandTransport`)
    on `app.state`; without it, accepted commands stay pending until a transport delivers
    them, and `redeliver` returns nothing.
    """

    state = request.app.state
    service = getattr(state, "boundary_intervention_service", None)
    if service is not None:
        return service
    run_control = await get_run_control_service(request)
    async with _initialization_lock:
        service = getattr(state, "boundary_intervention_service", None)
        if service is not None:
            return service
        transport: BoundaryCommandTransport | None = getattr(
            state, "boundary_command_transport", None
        )
        delivery = (
            BoundaryCommandDeliveryService(run_control, transport)
            if transport is not None
            else None
        )
        service = BoundaryInterventionService(run_control, delivery)
        state.boundary_intervention_service = service
        return service


async def get_unit_reconciliation_service(request: Request) -> UnitReconciliationService:
    """`reconcile_unit` over PostgreSQL lineage, run control and the optional Temporal hint.

    Deployments attach `unit_reconciliation_nudge` (a `TemporalUnitReconciliationNudge`)
    and `unit_reconciliation_verifier` on `app.state`; the signal stays the hint transport.
    """

    state = request.app.state
    service = getattr(state, "unit_reconciliation_service", None)
    if service is not None:
        return service
    run_control = await get_run_control_service(request)
    pool: asyncpg.Pool | None = getattr(state, "run_control_postgres_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="application PostgreSQL authority is not configured",
        )
    async with _initialization_lock:
        service = getattr(state, "unit_reconciliation_service", None)
        if service is not None:
            return service
        service = compose_postgres_operation_recovery(
            pool,
            run_control=run_control,
            nudge=getattr(state, "unit_reconciliation_nudge", None),
            verifier=getattr(state, "unit_reconciliation_verifier", None),
        ).reconciliation
        state.unit_reconciliation_service = service
        return service


async def get_run_launch_service(request: Request) -> RunLaunchService:
    """The governed API-to-Temporal launch (RRM-009); composed by the deployment
    (`compose_runtime_control`) on `app.state.run_launch_service`."""

    launcher = getattr(request.app.state, "run_launch_service", None)
    if launcher is None:
        raise HTTPException(
            status_code=503,
            detail="governed Temporal launch is not composed (RUN_CONTROL_TEMPORAL_ENABLED)",
        )
    return launcher


def get_family_liability_hints(request: Request) -> FamilyLiabilityHints | None:
    """RRM-008 composed by RRM-009: the `liability_reconciled` hint to a cancelling family,
    attached by the deployment (`compose_runtime_control`); absent, the family's timer is
    the only retry."""

    return getattr(request.app.state, "family_liability_hints", None)


async def get_async_child_usage_reconciliation(
    request: Request,
) -> AsyncChildUsageReconciliation:
    service = getattr(request.app.state, "async_child_usage_reconciliation", None)
    if service is None:
        raise HTTPException(
            status_code=503,
            detail="async child usage reconciliation is not composed",
        )
    return service


async def get_generic_artifact_submitter(
    request: Request,
) -> GenericArtifactSubmissionPort:
    submitter = getattr(request.app.state, "generic_artifact_submitter", None)
    if submitter is None:
        raise HTTPException(
            status_code=503,
            detail="generic artifact Temporal submission is not configured",
        )
    return submitter


async def close_run_control_resources(application: FastAPI) -> None:
    state = application.state
    pool = getattr(state, "run_control_postgres_pool", None)
    if pool is not None:
        await pool.close()
        state.run_control_postgres_pool = None
    family_writer_pool = getattr(state, "run_control_family_writer_pool", None)
    if family_writer_pool is not None:
        await family_writer_pool.close()
        state.run_control_family_writer_pool = None


def principal_permissions(principal: ControlPlanePrincipal) -> frozenset[str]:
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
    granted = principal_permissions(principal)
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
    if "workflow_run.read" not in principal_permissions(principal):
        raise HTTPException(status_code=403, detail="workflow run read permission required")


Service = Annotated[RunControlService, Depends(get_run_control_service)]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]
Interventions = Annotated[
    BoundaryInterventionService, Depends(get_boundary_intervention_service)
]
UnitReconciliation = Annotated[
    UnitReconciliationService, Depends(get_unit_reconciliation_service)
]
ArtifactSubmitter = Annotated[
    GenericArtifactSubmissionPort,
    Depends(get_generic_artifact_submitter),
]
Launcher = Annotated[RunLaunchService, Depends(get_run_launch_service)]
LiabilityHints = Annotated[FamilyLiabilityHints | None, Depends(get_family_liability_hints)]
ChildUsageReconciliation = Annotated[
    AsyncChildUsageReconciliation, Depends(get_async_child_usage_reconciliation)
]


class AsyncChildUsageReconciliationRequest(BaseModel):
    """A privileged statement of what the provider attributes to each provider run of a
    cancelled child (zero is a recorded decision, never a default)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    actor: ActorContext
    run_usage: dict[str, AsyncSubagentUsage] = Field(min_length=1)
    settlement_ref: str = Field(min_length=1, max_length=512)


class AsyncChildUsageReconciliationReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    link: ParentAsyncSubagentLink
    liability_hint_sent: bool


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


@router.post("/runs/{run_id}/launch", response_model=RunLaunchReceipt, status_code=202)
async def launch_run(
    run_id: str, launch: RunLaunchRequest, principal: Principal, launcher: Launcher
) -> RunLaunchReceipt:
    """RRM-009: start the admitted run's `BellLabsRunWorkflow` from the facade. The family
    input is verified against the admitted authority; a fork-derived run starts with its
    parent and patched templates; a repeated launch of a pending run is idempotent."""

    if launch.run_id != run_id:
        raise HTTPException(status_code=422, detail="path and launch run ids differ")
    _authorize_scope(principal, launch.request_scope)
    granted = principal_permissions(principal)
    if LAUNCH_PERMISSION not in granted:
        raise HTTPException(status_code=403, detail=f"{LAUNCH_PERMISSION} permission required")
    actor = ActorContext(
        actor_id=principal.actor_id,
        permissions=granted,
        authority_refs=principal.authority_refs,
    )
    try:
        return await launcher.launch(launch, actor)
    except RunLaunchRejected as error:
        status = 409 if error.retryable or error.code == "run_not_pending" else 422
        raise HTTPException(
            status_code=status, detail={"code": error.code, "message": error.message}
        ) from error


@router.post("/runs/{run_id}/commands", response_model=CommandResult)
async def execute_command(
    run_id: str,
    command: LifecycleCommand,
    principal: Principal,
    interventions: Interventions,
    hints: LiabilityHints,
) -> CommandResult:
    """Every public command enters here; a boundary command (pause, resume, wait release,
    cancel) is accepted by run control and reaches Temporal only as a recorded delivery
    (REQ-CP-EXEC-007). `reconcile_unit` has its own privileged route."""

    if command.run_id != run_id:
        raise HTTPException(status_code=422, detail="path and command run ids differ")
    _authorize_scope(principal, command.request_scope)
    if command.idempotency_issuer != principal.actor_id:
        raise HTTPException(status_code=403, detail="idempotency issuer mismatch")
    if isinstance(command.action, ReconcileUnitAction):
        raise HTTPException(
            status_code=422,
            detail="reconcile_unit is delivered through /runs/{run_id}/reconcile-unit",
        )
    trusted_actor = _authorize_actor(
        principal,
        command.actor.actor_id,
        command.actor.permissions,
        command.actor.authority_refs,
    )
    result = await interventions.execute(
        command.model_copy(update={"actor": trusted_actor, "occurred_at": datetime.now(UTC)})
    )
    if (
        hints is not None
        and result.status == CommandStatus.ACCEPTED
        and command.action.kind in LIABILITY_DECISION_KINDS
    ):
        # RRM-008: an operator decision may have resolved a liability the cancelling family
        # waits on; wake its terminalization retry (a hint only; no-op unless cancelling).
        await hints.notify(command.request_scope, run_id, f"command:{command.command_id}")
    return result


@router.post("/runs/{run_id}/reconcile-unit", response_model=CommandResult)
async def reconcile_unit(
    run_id: str,
    command: LifecycleCommand,
    principal: Principal,
    reconciliation: UnitReconciliation,
    interventions: Interventions,
    hints: LiabilityHints,
) -> CommandResult:
    """RRM-007 (RRM-004 review): governed delivery of the privileged `reconcile_unit`
    decision. Run control records `accepted`; the lineage applies the decision; the parked
    operation receives the wake-up hint (`delivered`) and records `applied` when it acts."""

    if command.run_id != run_id:
        raise HTTPException(status_code=422, detail="path and command run ids differ")
    if not isinstance(command.action, ReconcileUnitAction):
        raise HTTPException(status_code=422, detail="the command must carry reconcile_unit")
    _authorize_scope(principal, command.request_scope)
    if command.idempotency_issuer != principal.actor_id:
        raise HTTPException(status_code=403, detail="idempotency issuer mismatch")
    trusted_actor = _authorize_actor(
        principal,
        command.actor.actor_id,
        command.actor.permissions,
        command.actor.authority_refs,
    )
    if "workflow_run.reconcile_unit" not in trusted_actor.permissions:
        raise HTTPException(status_code=403, detail="workflow_run.reconcile_unit required")
    try:
        result = await reconciliation.reconcile_unit(
            command.model_copy(
                update={"actor": trusted_actor, "occurred_at": datetime.now(UTC)}
            )
        )
    except UnitReconciliationRejected as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if hints is not None and result.status == CommandStatus.ACCEPTED:
        # RRM-008: the decided unit may have been the cancelling family's last liability.
        await hints.notify(command.request_scope, run_id, f"reconcile-unit:{command.command_id}")
    return result


@router.post(
    "/runs/{run_id}/async-children/{child_execution_id}/reconcile-usage",
    response_model=AsyncChildUsageReconciliationReceipt,
)
async def reconcile_async_child_usage(
    run_id: str,
    child_execution_id: str,
    body: AsyncChildUsageReconciliationRequest,
    principal: Principal,
    reconciliation: ChildUsageReconciliation,
) -> AsyncChildUsageReconciliationReceipt:
    """RRM-008 composed by RRM-009: the privileged reconciliation of a cancelled async
    child's pending usage (REQ-CP-RUN-009), then the `liability_reconciled` hint."""

    _authorize_scope(principal, body.request_scope)
    trusted_actor = _authorize_actor(
        principal, body.actor.actor_id, body.actor.permissions, body.actor.authority_refs
    )
    if ASYNC_CHILD_RECONCILE_PERMISSION not in trusted_actor.permissions:
        raise HTTPException(
            status_code=403, detail=f"{ASYNC_CHILD_RECONCILE_PERMISSION} required"
        )
    try:
        link, hinted = await reconciliation.reconcile_usage(
            body.request_scope,
            run_id,
            child_execution_id,
            actor=trusted_actor,
            run_usage=body.run_usage,
            settlement_ref=body.settlement_ref,
            reconciled_at=datetime.now(UTC),
        )
    except AsyncSubagentDecisionRejected as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except AsyncSubagentError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return AsyncChildUsageReconciliationReceipt(link=link, liability_hint_sent=hinted)


@router.get(
    "/runs/{run_id}/boundary-commands",
    response_model=tuple[BoundaryCommandStatus, ...],
)
async def get_boundary_commands(
    run_id: str, request_scope: str, principal: Principal, interventions: Interventions
) -> tuple[BoundaryCommandStatus, ...]:
    """The durable command and receipt ledger of a run (REQ-CP-RUN-004 pending commands)."""

    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await interventions.list_commands(request_scope, run_id)


@router.post(
    "/runs/{run_id}/boundary-commands/redeliver",
    response_model=tuple[BoundaryCommandStatus, ...],
)
async def redeliver_boundary_commands(
    run_id: str, request_scope: str, principal: Principal, interventions: Interventions
) -> tuple[BoundaryCommandStatus, ...]:
    """Re-drive delivery of accepted, undelivered commands in order (at-least-once; the
    target de-duplicates and receipts never transition twice)."""

    _authorize_scope(principal, request_scope)
    if "workflow_run.relay" not in principal_permissions(principal):
        raise HTTPException(status_code=403, detail="outbox relay permission required")
    return await interventions.redeliver(request_scope, run_id)


@router.post(
    "/runs/{run_id}/operations",
    response_model=GenericArtifactWorkflowResult,
    status_code=201,
)
async def submit_generic_artifact_operation(
    run_id: str,
    submission: GenericArtifactWorkflowRequest,
    principal: Principal,
    service: Service,
    submitter: ArtifactSubmitter,
) -> GenericArtifactWorkflowResult:
    if submission.run_id != run_id:
        raise HTTPException(status_code=422, detail="path and operation run ids differ")
    _authorize_scope(principal, submission.request_scope)
    if "workflow_run.execute_operation" not in principal_permissions(principal):
        raise HTTPException(status_code=403, detail="operation execution permission required")
    run = await service.get_run(submission.request_scope, run_id)
    if run.phase != RunPhase.ACTIVE:
        raise HTTPException(status_code=409, detail="operation requires an active workflow run")
    if (
        submission.operation.run_control_revision != run.version
        or submission.operation.effective_configuration_digest != run.effective_configuration_digest
    ):
        raise HTTPException(
            status_code=409,
            detail="operation is not bound to the current accepted run revision",
        )
    budget = await service.get_budget(submission.request_scope, run_id)
    if submission.operation.budget_reservation_id not in budget.reservations:
        raise HTTPException(status_code=409, detail="operation budget reservation is unavailable")
    return await submitter.submit(submission)


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


@router.get("/runs/{run_id}/effects", response_model=EffectLedgerState)
async def get_effects(
    run_id: str, request_scope: str, principal: Principal, service: Service
) -> EffectLedgerState:
    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await service.get_effects(request_scope, run_id)


@router.get(
    "/runs/{run_id}/effect-ledger",
    response_model=tuple[EffectLedgerEntry, ...],
)
async def get_effect_ledger(
    run_id: str, request_scope: str, principal: Principal, service: Service
) -> tuple[EffectLedgerEntry, ...]:
    _authorize_read(principal)
    _authorize_scope(principal, request_scope)
    return await service.list_effect_ledger(request_scope, run_id)


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
    if "workflow_run.relay" not in principal_permissions(principal):
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
        "effect_ledger_state": EffectLedgerState.model_json_schema(),
        "transition": LifecycleTransitionRecord.model_json_schema(),
        "outbox_record": OutboxRecord.model_json_schema(),
        "boundary_command_status": BoundaryCommandStatus.model_json_schema(),
        "generic_artifact_workflow": GenericArtifactWorkflowRequest.model_json_schema(),
        "lifecycle_action": TypeAdapter(LifecycleCommand).json_schema(),
    }
