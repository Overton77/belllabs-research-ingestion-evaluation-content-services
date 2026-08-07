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
    ForkReceipt,
    ForkRequest,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.identities import AgentThreadKey, ExecutionEpochKey
from app.domain.graph_runtime.kernel import CancellationContext
from app.domain.run_control.errors import IdempotencyConflict


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


class CancellationSettlement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    cancellation_id: str
    binding_id: str
    status: Literal["cancelled"] = "cancelled"
    usage_settlement_refs: tuple[str, ...] = ()
    effect_settlement_refs: tuple[str, ...] = ()
    settled_at: AwareDatetime


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
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: str
    target_epoch: ExecutionEpochKey
    budget_reservation_ref: str = Field(min_length=1)
    admitted_run_plan_digest: str = Field(min_length=1)


class ForkAdmissionObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["admitted", "definitively_missing", "ambiguous"]
    admission: ForkAdmission | None = None

    def model_post_init(self, _context: object) -> None:
        if (self.status == "admitted") != (self.admission is not None):
            raise ValueError("only admitted fork observations contain an admission")


class ForkRuntimeObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["copied", "definitively_missing", "ambiguous"]
    target_thread: AgentThreadKey | None = None

    def model_post_init(self, _context: object) -> None:
        if (self.status == "copied") != (self.target_thread is not None):
            raise ValueError("only copied fork observations contain a target thread")


class ForkAuthority(Protocol):
    async def admit_fork(self, request: ForkRequest) -> ForkAdmission: ...

    async def reconcile_fork_admission(
        self,
        request: ForkRequest,
    ) -> ForkAdmissionObservation: ...


class ForkRuntimeClient(Protocol):
    async def copy_checkpoint(
        self,
        request: ForkRequest,
        source_binding: RuntimeExecutionBinding,
        admission: ForkAdmission,
    ) -> AgentThreadKey: ...

    async def reconcile_checkpoint_copy(
        self,
        request: ForkRequest,
        source_binding: RuntimeExecutionBinding,
        admission: ForkAdmission,
    ) -> ForkRuntimeObservation: ...


class ForkRepository(Protocol):
    def guard(self, request: ForkRequest) -> AbstractAsyncContextManager[None]: ...

    async def reserve(self, request: ForkRequest) -> bool: ...

    async def get(self, request_scope: str, request_id: str) -> ForkReceipt | None: ...

    async def claim_admission(self, request: ForkRequest) -> bool: ...

    async def release_admission_claim(self, request: ForkRequest) -> None: ...

    async def claim_copy(self, request: ForkRequest) -> bool: ...

    async def release_copy_claim(self, request: ForkRequest) -> None: ...

    async def get_admission(
        self,
        request_scope: str,
        request_id: str,
    ) -> ForkAdmission | None: ...

    async def record_admission(
        self,
        request: ForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission: ...

    async def record(self, request: ForkRequest, receipt: ForkReceipt) -> ForkReceipt: ...


class InMemoryForkRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._requests: dict[tuple[str, str], ForkRequest] = {}
        self._admissions: dict[tuple[str, str], ForkAdmission] = {}
        self._receipts: dict[tuple[str, str], ForkReceipt] = {}
        self._admission_claims: set[tuple[str, str]] = set()
        self._copy_claims: set[tuple[str, str]] = set()
        self._guards: dict[tuple[str, str], asyncio.Lock] = {}

    @asynccontextmanager
    async def guard(self, request: ForkRequest) -> AsyncIterator[None]:
        key = (request.source_epoch.request_scope, request.request_id)
        lock = self._guards.setdefault(key, asyncio.Lock())
        async with lock:
            yield

    async def reserve(self, request: ForkRequest) -> bool:
        key = (request.source_epoch.request_scope, request.request_id)
        async with self._lock:
            prior = self._requests.get(key)
            if prior is not None:
                if prior != request:
                    raise IdempotencyConflict("fork identity has conflicting intent")
                return False
            self._requests[key] = deepcopy(request)
            return True

    async def get(self, request_scope: str, request_id: str) -> ForkReceipt | None:
        return deepcopy(self._receipts.get((request_scope, request_id)))

    async def claim_admission(self, request: ForkRequest) -> bool:
        key = (request.source_epoch.request_scope, request.request_id)
        async with self._lock:
            if key in self._admission_claims or key in self._admissions:
                return False
            self._admission_claims.add(key)
            return True

    async def release_admission_claim(self, request: ForkRequest) -> None:
        key = (request.source_epoch.request_scope, request.request_id)
        async with self._lock:
            self._admission_claims.discard(key)

    async def claim_copy(self, request: ForkRequest) -> bool:
        key = (request.source_epoch.request_scope, request.request_id)
        async with self._lock:
            if key in self._copy_claims or key in self._receipts:
                return False
            self._copy_claims.add(key)
            return True

    async def release_copy_claim(self, request: ForkRequest) -> None:
        key = (request.source_epoch.request_scope, request.request_id)
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
        request: ForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission:
        key = (request.source_epoch.request_scope, request.request_id)
        async with self._lock:
            prior = self._admissions.get(key)
            if prior is not None:
                if prior != admission:
                    raise IdempotencyConflict("fork admission has conflicting identities")
                return deepcopy(prior)
            self._admissions[key] = deepcopy(admission)
            self._admission_claims.discard(key)
            return deepcopy(admission)

    async def record(self, request: ForkRequest, receipt: ForkReceipt) -> ForkReceipt:
        key = (request.source_epoch.request_scope, request.request_id)
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
    """Admits a new BellLabs run/budget before copying provider checkpoint state."""

    def __init__(
        self,
        *,
        repository: ForkRepository,
        authority: ForkAuthority,
        runtime: ForkRuntimeClient,
    ) -> None:
        self._repository = repository
        self._authority = authority
        self._runtime = runtime

    async def fork(
        self,
        request: ForkRequest,
        source_binding: RuntimeExecutionBinding,
    ) -> ForkReceipt:
        if request.source_epoch != source_binding.epoch:
            raise ValueError("fork source does not match its persisted runtime binding")
        if request.target_run.request_scope != request.source_epoch.request_scope:
            raise ValueError("fork cannot cross request scopes")
        if request.target_run.belllabs_run_id == request.source_epoch.belllabs_run_id:
            raise ValueError("fork must create a new BellLabs run")
        if request.run_plan_digest != source_binding.run_plan_digest:
            raise ValueError("fork RunPlan differs from the source epoch")
        async with self._repository.guard(request):
            return await self._fork_guarded(request, source_binding)

    async def _fork_guarded(
        self,
        request: ForkRequest,
        source_binding: RuntimeExecutionBinding,
    ) -> ForkReceipt:
        created = await self._repository.reserve(request)
        if not created:
            prior = await self._repository.get(
                request.source_epoch.request_scope,
                request.request_id,
            )
            if prior is not None:
                return prior
        admission = await self._repository.get_admission(
            request.source_epoch.request_scope,
            request.request_id,
        )
        admission_was_persisted = admission is not None
        if admission is None:
            if not await self._repository.claim_admission(request):
                admission_observation = (
                    await self._authority.reconcile_fork_admission(request)
                )
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
            runtime_observation = await self._runtime.reconcile_checkpoint_copy(
                request,
                source_binding,
                admission,
            )
            if runtime_observation.status == "ambiguous":
                raise RuntimeError("fork provider application remains ambiguous")
            if runtime_observation.status == "copied":
                assert runtime_observation.target_thread is not None
                return await self._record_receipt(
                    request,
                    admission,
                    runtime_observation.target_thread,
                )
            await self._repository.release_copy_claim(request)
        if not await self._repository.claim_copy(request):
            raise RuntimeError("fork checkpoint copy is already in progress")
        target_thread = await self._runtime.copy_checkpoint(
            request,
            source_binding,
            admission,
        )
        return await self._record_receipt(request, admission, target_thread)

    @staticmethod
    def _validate_admission(request: ForkRequest, admission: ForkAdmission) -> None:
        if (
            admission.request_id != request.request_id
            or admission.target_epoch.request_scope != request.source_epoch.request_scope
            or admission.target_epoch.belllabs_run_id != request.target_run.belllabs_run_id
            or admission.admitted_run_plan_digest != request.run_plan_digest
        ):
            raise ValueError("fork admission does not match the immutable request")

    async def _record_receipt(
        self,
        request: ForkRequest,
        admission: ForkAdmission,
        target_thread: AgentThreadKey,
    ) -> ForkReceipt:
        if (
            target_thread.belllabs_run_id != admission.target_epoch.belllabs_run_id
            or target_thread.execution_epoch != admission.target_epoch.execution_epoch
            or target_thread.relationship != "fork"
            or target_thread.parent_belllabs_run_id != request.source_epoch.belllabs_run_id
        ):
            raise ValueError("provider fork returned an invalid child thread identity")
        receipt = ForkReceipt(
            request_id=request.request_id,
            source_epoch=request.source_epoch,
            target_epoch=admission.target_epoch,
            target_thread=target_thread,
            status="accepted",
            recorded_at=request.requested_at,
        )
        return await self._repository.record(request, receipt)
