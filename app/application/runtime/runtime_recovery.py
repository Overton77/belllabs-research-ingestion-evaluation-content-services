from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.domain.graph_runtime.contracts import (
    CancelRunIntervention,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.identities import DIGEST_PATTERN, ExecutionEpochKey
from app.domain.graph_runtime.kernel import CancellationContext
from app.domain.run_control.errors import IdempotencyConflict
from app.domain.run_control.forks import (
    DERIVED_EXECUTION_EPOCH,
    ForkLineageManifest,
    ForkRejected,
    RunForkReceipt,
    RunForkRequest,
    fork_request_fingerprint,
    lineage_for,
)


class RecoveryMode(StrEnum):
    INSPECT = "inspect"
    DIAGNOSTIC_REPLAY = "diagnostic_replay"
    TECHNICAL_RETRY = "technical_retry"
    FORK = "fork"
    EPOCH_ROLLOVER = "epoch_rollover"
    ROLLBACK = "rollback"


class RecoveryPolicyDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: RecoveryMode
    allowed: bool
    effect_claims_allowed: bool
    creates_new_belllabs_run: bool
    creates_new_thread: bool
    reason_code: str = Field(min_length=1)


def decide_recovery_mode(mode: RecoveryMode) -> RecoveryPolicyDecision:
    if mode == RecoveryMode.DIAGNOSTIC_REPLAY:
        return RecoveryPolicyDecision(
            mode=mode,
            allowed=True,
            effect_claims_allowed=False,
            creates_new_belllabs_run=False,
            creates_new_thread=True,
            reason_code="diagnostic_replay_isolated_no_effects",
        )
    if mode == RecoveryMode.TECHNICAL_RETRY:
        return RecoveryPolicyDecision(
            mode=mode,
            allowed=True,
            effect_claims_allowed=True,
            creates_new_belllabs_run=False,
            creates_new_thread=False,
            reason_code="technical_retry_retains_semantic_identity",
        )
    if mode == RecoveryMode.FORK:
        return RecoveryPolicyDecision(
            mode=mode,
            allowed=True,
            effect_claims_allowed=True,
            creates_new_belllabs_run=True,
            creates_new_thread=True,
            reason_code="fork_requires_new_run_thread_budget_and_lineage",
        )
    if mode == RecoveryMode.EPOCH_ROLLOVER:
        return RecoveryPolicyDecision(
            mode=mode,
            allowed=False,
            effect_claims_allowed=False,
            creates_new_belllabs_run=False,
            creates_new_thread=True,
            reason_code="epoch_rollover_policy_not_published",
        )
    if mode == RecoveryMode.ROLLBACK:
        return RecoveryPolicyDecision(
            mode=mode,
            allowed=False,
            effect_claims_allowed=False,
            creates_new_belllabs_run=False,
            creates_new_thread=False,
            reason_code="authoritative_history_cannot_be_rewritten",
        )
    return RecoveryPolicyDecision(
        mode=mode,
        allowed=True,
        effect_claims_allowed=False,
        creates_new_belllabs_run=False,
        creates_new_thread=False,
        reason_code="read_only_inspection",
    )


class CancellationPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    cancellation: CancellationContext
    binding_id: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    cascade_runtime_resources: tuple[str, ...] = ()
    linked_runs_requiring_commands: tuple[str, ...] = ()
    accepted_at: AwareDatetime


def build_cancellation_plan(
    intervention: CancelRunIntervention,
    binding: RuntimeExecutionBinding,
    *,
    runtime_resource_refs: tuple[str, ...],
    linked_run_refs: tuple[str, ...],
) -> CancellationPlan:
    if intervention.epoch != binding.epoch:
        raise ValueError("cancellation command does not target the runtime binding")
    return CancellationPlan(
        cancellation=CancellationContext(
            cancellation_id=intervention.command_id,
            requested=True,
            requested_at=intervention.requested_at,
            cascade_policy_ref="policy:cancellation:cooperative-cascade.v1",
        ),
        binding_id=binding.binding_id,
        command_id=intervention.command_id,
        cascade_runtime_resources=runtime_resource_refs,
        linked_runs_requiring_commands=linked_run_refs,
        accepted_at=intervention.requested_at,
    )


def apply_terminal_runtime_observation(
    binding: RuntimeExecutionBinding,
    *,
    observed_status: RuntimeExecutionStatus,
    cancellation_settled: bool,
    observed_at: datetime,
) -> RuntimeExecutionBinding:
    if binding.status in {RuntimeExecutionStatus.CANCELLING, RuntimeExecutionStatus.CANCELLED}:
        if not cancellation_settled:
            return binding.model_copy(
                update={
                    "status": RuntimeExecutionStatus.CANCELLING,
                    "version": binding.version + 1,
                    "updated_at": observed_at,
                }
            )
        return binding.model_copy(
            update={
                "status": RuntimeExecutionStatus.CANCELLED,
                "active": False,
                "version": binding.version + 1,
                "updated_at": observed_at,
            }
        )
    return binding.model_copy(
        update={
            "status": observed_status,
            "active": observed_status
            not in {
                RuntimeExecutionStatus.COMPLETED,
                RuntimeExecutionStatus.FAILED,
                RuntimeExecutionStatus.CANCELLED,
            },
            "version": binding.version + 1,
            "updated_at": observed_at,
        }
    )


class ForkAdmission(BaseModel):
    """The derived run's independent admission (REQ-CP-EXEC-012, REQ-CP-RUN-001)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: str
    target_epoch: ExecutionEpochKey
    admission_ref: str = Field(min_length=1)
    budget_reservation_ref: str = Field(min_length=1)
    admitted_effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)


class ForkAdmissionObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["admitted", "definitively_missing", "ambiguous"]
    admission: ForkAdmission | None = None

    def model_post_init(self, _context: object) -> None:
        if (self.status == "admitted") != (self.admission is not None):
            raise ValueError("only admitted fork observations contain an admission")


class ForkMaterialization(BaseModel):
    """The recorded fork lineage and reuse decisions of an admitted derived run.

    It replaces the Agent Server-shaped provider `copy_checkpoint` (RRM-001 disposition row
    33): nothing is copied from the source run; the fork records its lineage and the reuse
    decisions the derived run's units consult, by immutable ref.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: str
    target_run_id: str
    lineage: ForkLineageManifest


class ForkMaterializationObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["materialized", "definitively_missing", "ambiguous"]
    materialization: ForkMaterialization | None = None

    def model_post_init(self, _context: object) -> None:
        if (self.status == "materialized") != (self.materialization is not None):
            raise ValueError("only materialized fork observations contain a materialization")


class ForkAuthority(Protocol):
    async def admit_fork(self, request: RunForkRequest) -> ForkAdmission: ...

    async def reconcile_fork_admission(
        self,
        request: RunForkRequest,
    ) -> ForkAdmissionObservation: ...


class ForkMaterializer(Protocol):
    async def materialize(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
    ) -> ForkMaterialization: ...

    async def reconcile_materialization(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
    ) -> ForkMaterializationObservation: ...


class ForkRepository(Protocol):
    def guard(self, request: RunForkRequest) -> AbstractAsyncContextManager[None]: ...

    async def reserve(self, request: RunForkRequest) -> bool: ...

    async def get_request(self, request_scope: str, request_id: str) -> RunForkRequest | None: ...

    async def get(self, request_scope: str, request_id: str) -> RunForkReceipt | None: ...

    async def claim_admission(self, request: RunForkRequest) -> bool: ...

    async def release_admission_claim(self, request: RunForkRequest) -> None: ...

    async def claim_copy(self, request: RunForkRequest) -> bool: ...

    async def release_copy_claim(self, request: RunForkRequest) -> None: ...

    async def get_admission(
        self,
        request_scope: str,
        request_id: str,
    ) -> ForkAdmission | None: ...

    async def record_admission(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission: ...

    async def record(self, request: RunForkRequest, receipt: RunForkReceipt) -> RunForkReceipt: ...


class InMemoryForkRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._requests: dict[tuple[str, str], RunForkRequest] = {}
        self._admissions: dict[tuple[str, str], ForkAdmission] = {}
        self._receipts: dict[tuple[str, str], RunForkReceipt] = {}
        self._admission_claims: set[tuple[str, str]] = set()
        self._copy_claims: set[tuple[str, str]] = set()
        self._guards: dict[tuple[str, str], asyncio.Lock] = {}

    @asynccontextmanager
    async def guard(self, request: RunForkRequest) -> AsyncIterator[None]:
        key = (request.request_scope, request.request_id)
        lock = self._guards.setdefault(key, asyncio.Lock())
        async with lock:
            yield

    async def reserve(self, request: RunForkRequest) -> bool:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            prior = self._requests.get(key) or next(
                (
                    item
                    for (scope, _), item in self._requests.items()
                    if scope == request.request_scope
                    and item.idempotency_key == request.idempotency_key
                ),
                None,
            )
            if prior is not None:
                if fork_request_fingerprint(prior) != fork_request_fingerprint(request):
                    raise IdempotencyConflict("fork identity has conflicting intent")
                return False
            self._requests[key] = deepcopy(request)
            return True

    async def get_request(self, request_scope: str, request_id: str) -> RunForkRequest | None:
        return deepcopy(self._requests.get((request_scope, request_id)))

    async def get(self, request_scope: str, request_id: str) -> RunForkReceipt | None:
        return deepcopy(self._receipts.get((request_scope, request_id)))

    async def claim_admission(self, request: RunForkRequest) -> bool:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            if key in self._admission_claims or key in self._admissions:
                return False
            self._admission_claims.add(key)
            return True

    async def release_admission_claim(self, request: RunForkRequest) -> None:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            self._admission_claims.discard(key)

    async def claim_copy(self, request: RunForkRequest) -> bool:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            if key in self._copy_claims or key in self._receipts:
                return False
            self._copy_claims.add(key)
            return True

    async def release_copy_claim(self, request: RunForkRequest) -> None:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            self._copy_claims.discard(key)

    async def get_admission(
        self,
        request_scope: str,
        request_id: str,
    ) -> ForkAdmission | None:
        return deepcopy(self._admissions.get((request_scope, request_id)))

    async def record_admission(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            prior = self._admissions.get(key)
            if prior is not None:
                if prior != admission:
                    raise IdempotencyConflict("fork admission has conflicting identities")
                return deepcopy(prior)
            self._admissions[key] = deepcopy(admission)
            self._admission_claims.discard(key)
            return deepcopy(admission)

    async def record(self, request: RunForkRequest, receipt: RunForkReceipt) -> RunForkReceipt:
        key = (request.request_scope, request.request_id)
        async with self._lock:
            prior = self._receipts.get(key)
            if prior is not None:
                if prior != receipt:
                    raise IdempotencyConflict("fork receipt has conflicting identities")
                return deepcopy(prior)
            self._receipts[key] = deepcopy(receipt)
            self._copy_claims.discard(key)
            return deepcopy(receipt)


class RuntimeForkService:
    """The audited fork saga, versioned for semantic forks (RRM-001 disposition row 33).

    reserve -> admit the derived run independently at epoch 1 -> record the admission ->
    materialize the fork (lineage and reuse decisions; nothing is copied from the source)
    -> one durable receipt. Every step is idempotent, and a step whose outcome was lost is
    reconciled from durable state before it is retried; an ambiguous outcome fails closed.
    """

    def __init__(
        self,
        *,
        repository: ForkRepository,
        authority: ForkAuthority,
        materializer: ForkMaterializer,
    ) -> None:
        self._repository = repository
        self._authority = authority
        self._materializer = materializer

    async def fork(self, request: RunForkRequest) -> RunForkReceipt:
        async with self._repository.guard(request):
            return await self._fork_guarded(request)

    async def _fork_guarded(self, request: RunForkRequest) -> RunForkReceipt:
        created = await self._repository.reserve(request)
        if not created:
            prior = await self._repository.get(request.request_scope, request.request_id)
            if prior is not None:
                return prior
            # A replay continues the persisted intent (its original request times).
            persisted = await self._repository.get_request(
                request.request_scope, request.request_id
            )
            if persisted is None:
                raise IdempotencyConflict("fork idempotency key belongs to another request")
            request = persisted
        admission = await self._repository.get_admission(
            request.request_scope,
            request.request_id,
        )
        admission_was_persisted = admission is not None
        if admission is None:
            if not await self._repository.claim_admission(request):
                admission_observation = await self._authority.reconcile_fork_admission(request)
                if admission_observation.status == "ambiguous":
                    raise RuntimeError("fork admission remains ambiguous")
                if admission_observation.status == "admitted":
                    assert admission_observation.admission is not None
                    self._validate_admission(request, admission_observation.admission)
                    admission = await self._repository.record_admission(
                        request,
                        admission_observation.admission,
                    )
                else:
                    await self._repository.release_admission_claim(request)
                    if not await self._repository.claim_admission(request):
                        raise RuntimeError("fork admission claim could not be recovered")
            if admission is None:
                admission = await self._authority.admit_fork(request)
                self._validate_admission(request, admission)
                admission = await self._repository.record_admission(request, admission)
        if not created and admission_was_persisted:
            observation = await self._materializer.reconcile_materialization(
                request,
                admission,
            )
            if observation.status == "ambiguous":
                raise ForkRejected(
                    "fork_materialization_ambiguous",
                    "fork materialization remains ambiguous; no receipt is recorded",
                )
            if observation.status == "materialized":
                assert observation.materialization is not None
                return await self._record_receipt(request, admission, observation.materialization)
            await self._repository.release_copy_claim(request)
        if not await self._repository.claim_copy(request):
            raise RuntimeError("fork materialization is already in progress")
        materialization = await self._materializer.materialize(request, admission)
        return await self._record_receipt(request, admission, materialization)

    @staticmethod
    def _validate_admission(request: RunForkRequest, admission: ForkAdmission) -> None:
        if (
            admission.request_id != request.request_id
            or admission.target_epoch.request_scope != request.request_scope
            or admission.target_epoch.belllabs_run_id != request.derived_run_id
            or admission.target_epoch.belllabs_run_id == request.source_run_id
        ):
            raise ValueError("fork admission does not match the immutable request")
        # REQ-CP-EXEC-012 (RRM-001 §7 #12): the derived run is a new run at epoch 1.
        if admission.target_epoch.execution_epoch != DERIVED_EXECUTION_EPOCH:
            raise ValueError("fork admission must target execution epoch 1")
        # The admitted configuration is the compiled (possibly patched) target's; a fork
        # never requires the source's run plan or ERC digest to be unchanged.
        if (
            admission.admitted_effective_configuration_digest
            != request.target.effective_configuration_digest
        ):
            raise ValueError("fork admission bound another effective configuration")

    async def _record_receipt(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
        materialization: ForkMaterialization,
    ) -> RunForkReceipt:
        lineage = materialization.lineage
        if (
            materialization.request_id != request.request_id
            or materialization.target_run_id != admission.target_epoch.belllabs_run_id
            or lineage != lineage_for(request, admission_ref=admission.admission_ref)
        ):
            raise ValueError("fork materialization does not match the admitted request")
        receipt = RunForkReceipt(
            request_id=request.request_id,
            request_scope=request.request_scope,
            source_run_id=request.source_run_id,
            snapshot_id=request.snapshot_id,
            snapshot_digest=request.snapshot_digest,
            patch_digest=request.patch.patch_digest,
            target_run_id=admission.target_epoch.belllabs_run_id,
            admission_ref=admission.admission_ref,
            admitted_effective_configuration_digest=(
                admission.admitted_effective_configuration_digest
            ),
            lineage=lineage,
            recorded_at=request.requested_at,
        )
        return await self._repository.record(request, receipt)
