"""`/run-control/v1`: safe macro snapshots and semantic forks (REQ-CP-EXEC-012, 016).

`POST /runs/{run_id}/snapshots` takes an immutable `RunSnapshotManifest` at a declared safe
boundary (or rejects `snapshot_not_quiescent`); `POST /runs/{run_id}/forks` validates a typed
patch against that snapshot and admits the derived run independently at epoch 1 through the
audited fork saga, returning its one durable receipt. A scope the principal does not hold is
the same 404 as an unknown run. The routes are separate from `graph_runtime_schemas.py`.

The fork admits the derived run; launching it on Temporal (with `BellLabsParentRunId` set to
the source run) is the governed launch path's job (RRM-009).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.run_control.inspection_repository import (
    PostgresInspectionReadRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.runtime.run_forks import (
    PostgresForkMaterializationStore,
    PostgresForkSourceReader,
    PostgresRunSnapshotRepository,
)
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    RETENTION_DAYS,
    PostgresForkRepository,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.recovery.run_forks import (
    FORK_PERMISSION,
    SNAPSHOT_PERMISSION,
    AsyncChildForkClassifier,
    ForkCommand,
    ForkMaterializationStore,
    ForkPatchPolicyRegistry,
    ForkSnapshotNotFound,
    LedgerPendingCommands,
    LineageAsyncChildForkClassifier,
    PendingCommandReader,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    RunSnapshotService,
    SemanticForkService,
)
from mission_control.application.recovery.runtime_recovery import ForkRepository, RuntimeForkService
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    CognitiveSeed,
    ForkPatchChange,
    ForkRejected,
    ForkRejectionCode,
    ForkReuseDecision,
    RunForkReceipt,
    RunSnapshotManifest,
)
from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from mission_control.interfaces.http.run_control import (
    get_run_control_service,
    initialize_run_control_resources,
    principal_permissions,
)

router = APIRouter(prefix="/run-control/v1", tags=["run-forks"])
_composition_lock = asyncio.Lock()

_STATUS: dict[ForkRejectionCode, int] = {
    "snapshot_not_quiescent": 409,
    "unsupported_boundary": 409,
    "stale_expected_version": 409,
    "snapshot_source_moving": 409,
    "stale_snapshot": 409,
    "protected_field": 422,
    "field_not_patchable": 422,
    "invalid_patch": 422,
    "cognitive_seed_not_supported": 422,
    "incompatible_restore": 409,
    "cross_scope_fork": 404,
    "fork_admission_rejected": 409,
    "fork_materialization_ambiguous": 503,
    "fork_not_materialized": 409,
    "unauthorized": 403,
    "snapshot_digest_conflict": 409,
    "fork_lineage_missing": 409,
}


@dataclass(frozen=True)
class RunForkServices:
    snapshots: RunSnapshotService
    forks: SemanticForkService
    receipts: ForkRepository
    materializations: ForkMaterializationStore


def compose_run_fork_services(
    pool: asyncpg.Pool,
    run_control: RunControlService,
    *,
    policies: ForkPatchPolicyRegistry | None = None,
    async_children: AsyncChildForkClassifier | None = None,
    commands: PendingCommandReader | None = None,
) -> RunForkServices:
    """Production composition over application PostgreSQL authority.

    Async children are classified by RRM-013's classifier over the authority lineage
    (`PostgresAsyncSubagentAuthority.list_children`) unless another classifier is supplied.
    Unapplied command receipts come from RRM-007's ledger (`LedgerPendingCommands` over
    `RunControlService.list_boundary_commands`) unless another reader is supplied.
    """

    snapshots = PostgresRunSnapshotRepository(pool)
    store = PostgresForkMaterializationStore(pool)
    repository = PostgresForkRepository(pool)
    saga = RuntimeForkService(
        repository=repository,
        authority=RunControlForkAuthority(run_control, PostgresRunControlRepository(pool)),
        materializer=RecordingForkMaterializer(
            store, retention=lambda at: at + timedelta(days=RETENTION_DAYS)
        ),
    )
    return RunForkServices(
        snapshots=RunSnapshotService(
            reads=PostgresInspectionReadRepository(pool),
            sources=PostgresForkSourceReader(pool),
            snapshots=snapshots,
            async_children=async_children
            or LineageAsyncChildForkClassifier(PostgresAsyncSubagentAuthority(pool)),
            commands=commands or LedgerPendingCommands(run_control),
        ),
        forks=SemanticForkService(snapshots=snapshots, saga=saga, policies=policies),
        receipts=repository,
        materializations=store,
    )


async def get_run_fork_services(request: Request) -> RunForkServices:
    """Compose once per application. Deployments may attach `fork_patch_policies` (a
    `ForkPatchPolicyRegistry`), and may override `async_child_fork_classifier` and
    `pending_command_reader` on `app.state` first."""

    state = request.app.state
    services = getattr(state, "run_fork_services", None)
    if services is not None:
        return services
    await initialize_run_control_resources(request.app)
    pool: asyncpg.Pool | None = getattr(state, "run_control_postgres_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=503, detail="application PostgreSQL authority is not configured"
        )
    run_control = await get_run_control_service(request)
    async with _composition_lock:
        services = getattr(state, "run_fork_services", None)
        if services is None:
            services = compose_run_fork_services(
                pool,
                run_control,
                policies=getattr(state, "fork_patch_policies", None),
                async_children=getattr(state, "async_child_fork_classifier", None),
                commands=getattr(state, "pending_command_reader", None),
            )
            state.run_fork_services = services
        return services


Services = Annotated[RunForkServices, Depends(get_run_fork_services)]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]


class SnapshotRequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1, max_length=256)
    expected_run_version: int | None = Field(default=None, ge=1)


class ForkRequestBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1, max_length=256)
    request_id: str = Field(min_length=1, max_length=256)
    idempotency_key: str = Field(min_length=1, max_length=512)
    snapshot_id: str = Field(min_length=1, max_length=512)
    snapshot_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    changes: tuple[ForkPatchChange, ...] = ()
    invalidation_frontier: tuple[str, ...] = ()
    cognitive_seed: CognitiveSeed | None = None
    baseline_reservations: dict[str, int] = Field(default_factory=dict)
    sponsorship_ref: str = Field(min_length=1)
    approval_refs: tuple[str, ...] = ()
    reason: str = Field(min_length=1, max_length=2_000)


class ForkReceiptView(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt: RunForkReceipt
    reuse_decisions: tuple[ForkReuseDecision, ...]


def _authorize(principal: ControlPlanePrincipal, request_scope: str, *, permission: str) -> None:
    if request_scope not in principal.tenant_scopes:
        raise HTTPException(status_code=404, detail="workflow run not found")
    if permission not in principal_permissions(principal):
        raise HTTPException(status_code=403, detail=f"{permission} permission required")


def _rejected(error: ForkRejected) -> HTTPException:
    return HTTPException(
        status_code=_STATUS.get(error.code, 409),
        detail={"code": error.code, "message": error.message, "reasons": list(error.reasons)},
    )


@router.post("/runs/{run_id}/snapshots", response_model=RunSnapshotManifest, status_code=201)
async def take_run_snapshot(
    run_id: str, body: SnapshotRequestBody, principal: Principal, services: Services
) -> RunSnapshotManifest:
    _authorize(principal, body.request_scope, permission=SNAPSHOT_PERMISSION)
    try:
        return await services.snapshots.take(
            body.request_scope, run_id, expected_run_version=body.expected_run_version
        )
    except ForkSnapshotNotFound as error:
        raise HTTPException(status_code=404, detail="workflow run not found") from error
    except ForkRejected as error:
        raise _rejected(error) from error


@router.get("/runs/{run_id}/snapshots/{snapshot_id}", response_model=RunSnapshotManifest)
async def get_run_snapshot(
    run_id: str,
    snapshot_id: str,
    request_scope: str,
    principal: Principal,
    services: Services,
) -> RunSnapshotManifest:
    _authorize(principal, request_scope, permission=SNAPSHOT_PERMISSION)
    try:
        snapshot = await services.snapshots.get(request_scope, snapshot_id)
    except ForkSnapshotNotFound as error:
        raise HTTPException(status_code=404, detail="run snapshot not found") from error
    if snapshot.source_run_id != run_id:
        raise HTTPException(status_code=404, detail="run snapshot not found")
    return snapshot


@router.post("/runs/{run_id}/forks", response_model=RunForkReceipt, status_code=201)
async def fork_run(
    run_id: str, body: ForkRequestBody, principal: Principal, services: Services
) -> RunForkReceipt:
    _authorize(principal, body.request_scope, permission=FORK_PERMISSION)
    if body.sponsorship_ref not in principal.sponsorship_refs:
        raise HTTPException(status_code=403, detail="sponsorship was not granted")
    if not set(body.approval_refs) <= principal.approval_refs:
        raise HTTPException(status_code=403, detail="approval was not granted")
    command = ForkCommand(
        request_scope=body.request_scope,
        source_run_id=run_id,
        request_id=body.request_id,
        idempotency_key=body.idempotency_key,
        snapshot_id=body.snapshot_id,
        snapshot_digest=body.snapshot_digest,
        changes=body.changes,
        invalidation_frontier=body.invalidation_frontier,
        cognitive_seed=body.cognitive_seed,
        baseline_reservations=body.baseline_reservations,
        sponsorship_ref=body.sponsorship_ref,
        approval_refs=body.approval_refs,
        actor=ActorContext(
            actor_id=principal.actor_id,
            permissions=principal_permissions(principal),
            authority_refs=principal.authority_refs,
        ),
        reason=body.reason,
        requested_at=datetime.now(UTC),
    )
    try:
        return await services.forks.fork(command)
    except ForkSnapshotNotFound as error:
        raise HTTPException(status_code=404, detail="run snapshot not found") from error
    except ForkRejected as error:
        raise _rejected(error) from error
    except IdempotencyConflict as error:
        raise HTTPException(
            status_code=409, detail={"code": "idempotency_conflict", "message": str(error)}
        ) from error


@router.get("/forks/{request_id}", response_model=ForkReceiptView)
async def get_fork(
    request_id: str, request_scope: str, principal: Principal, services: Services
) -> ForkReceiptView:
    _authorize(principal, request_scope, permission=FORK_PERMISSION)
    receipt = await services.receipts.get(request_scope, request_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail="fork not found")
    return ForkReceiptView(
        receipt=receipt,
        reuse_decisions=await services.materializations.list_reuse_decisions(
            request_scope, request_id
        ),
    )
