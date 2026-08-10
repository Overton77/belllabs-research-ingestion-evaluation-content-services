from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
    AsyncSubagentResultManifest,
    ParentAsyncSubagentLink,
)


class AsyncSubagentError(RuntimeError):
    pass


class AsyncSubagentSpawnRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    parent_operation_id: str = Field(min_length=1)
    parent_binding_id: str = Field(min_length=1)
    execution_generation: int = Field(ge=1)
    contract: AsyncSubagentContract
    dependency_class: AsyncSubagentDependencyClass
    objective_ref: str = Field(min_length=1)
    objective: str = Field(min_length=1, max_length=100_000)
    context_slice_ref: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    requested_at: datetime


class ProviderAsyncObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["running", "waiting", "success", "error", "cancelled", "orphaned"]
    thread_id: str
    run_id: str
    output_ref: str | None = None
    evidence_refs: tuple[str, ...] = ()
    usage_ref: str | None = None
    checkpoint_ref: str | None = None
    effect_refs: tuple[str, ...] = ()
    observed_at: datetime


class AsyncSubagentDetailRepository(Protocol):
    async def create_before_submit(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        link: ParentAsyncSubagentLink,
    ) -> AsyncSubagentExecution: ...

    async def get_execution(self, request_scope: str, child_execution_id: str) -> AsyncSubagentExecution: ...
    async def get_link(self, request_scope: str, child_execution_id: str) -> ParentAsyncSubagentLink: ...
    async def save_execution(self, request_scope: str, execution: AsyncSubagentExecution) -> None: ...
    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None: ...


class AsyncSubagentAuthorityPort(Protocol):
    async def reserve_and_admit(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None: ...

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None: ...

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None: ...
    async def request_cancellation(self, request_scope: str, child_execution_id: str, reason: str) -> None: ...
    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        manifest_digest: str,
    ) -> None: ...

    async def settle(self, request_scope: str, child_execution_id: str, settlement_ref: str) -> None: ...


class AsyncSubagentProviderPort(Protocol):
    async def start(
        self, contract: AsyncSubagentContract, objective: str
    ) -> ProviderAsyncObservation: ...
    async def check(self, execution: AsyncSubagentExecution) -> ProviderAsyncObservation: ...
    async def update(
        self, execution: AsyncSubagentExecution, message: AsyncSubagentMessage
    ) -> ProviderAsyncObservation: ...
    async def cancel(self, execution: AsyncSubagentExecution) -> ProviderAsyncObservation: ...
    async def list(self, executions: tuple[AsyncSubagentExecution, ...]) -> tuple[ProviderAsyncObservation, ...]: ...


class AsyncSubagentService:
    """Coordinates subordinate detail, PostgreSQL authority, and provider observations."""

    def __init__(
        self,
        details: AsyncSubagentDetailRepository,
        authority: AsyncSubagentAuthorityPort,
        provider: AsyncSubagentProviderPort,
    ) -> None:
        self._details = details
        self._authority = authority
        self._provider = provider

    async def spawn(self, request: AsyncSubagentSpawnRequest) -> AsyncSubagentExecution:
        if request.dependency_class not in request.contract.dependency_classes:
            raise AsyncSubagentError("dependency class exceeds the immutable contract ceiling")
        child_id = str(uuid5(NAMESPACE_URL, f"async-child:{request.parent_binding_id}:{request.idempotency_key}"))
        link_id = str(uuid5(NAMESPACE_URL, f"async-link:{child_id}"))
        execution = AsyncSubagentExecution(
            child_execution_id=child_id,
            contract_id=request.contract.contract_id,
            contract_digest=request.contract.contract_digest,
            parent_run_id=request.parent_run_id,
            parent_operation_id=request.parent_operation_id,
            parent_binding_id=request.parent_binding_id,
            execution_generation=request.execution_generation,
            objective_ref=request.objective_ref,
            context_slice_ref=request.context_slice_ref,
            reservation_id=request.reservation_id,
            lifecycle=AsyncSubagentLifecycle.PROPOSED,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )
        link = ParentAsyncSubagentLink(
            link_id=link_id,
            child_execution_id=child_id,
            parent_run_id=request.parent_run_id,
            parent_operation_id=request.parent_operation_id,
            dependency_class=request.dependency_class,
            timeout_at=request.requested_at + timedelta(seconds=request.contract.timeout_seconds),
            cancellation_propagation=request.contract.cancellation_propagation,
            late_result_policy=request.contract.late_result_policy,
            fallback_policy=request.contract.fallback_policy,
            result_admission_policy_ref=request.contract.result_admission_policy_ref,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )
        prior = await self._details.create_before_submit(request.request_scope, request.contract, execution, link)
        if prior.lifecycle != AsyncSubagentLifecycle.PROPOSED:
            return prior
        await self._authority.reserve_and_admit(request, child_id, link_id)
        admitted = execution.model_copy(
            update={"lifecycle": AsyncSubagentLifecycle.ADMITTED, "updated_at": request.requested_at}
        )
        await self._details.save_execution(request.request_scope, admitted)
        try:
            observation = await self._provider.start(request.contract, request.objective)
        except Exception:
            orphaned = admitted.model_copy(
                update={"lifecycle": AsyncSubagentLifecycle.ORPHANED, "updated_at": request.requested_at}
            )
            await self._details.save_execution(request.request_scope, orphaned)
            await self._authority.record_fact(request.request_scope, child_id, "lifecycle", "orphaned")
            raise
        return await self._apply_observation(request.request_scope, admitted, observation)

    async def reconcile(self, request_scope: str, child_execution_id: str) -> AsyncSubagentExecution:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        observation = await self._provider.check(execution)
        return await self._apply_observation(request_scope, execution, observation)

    async def send_message(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        payload_ref: str,
        correlation_id: str,
        created_at: datetime,
        context_authority: Literal["instruction", "admitted_context", "untrusted_observation"] = "instruction",
    ) -> AsyncSubagentMessage:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        sequence = 1 + max(
            (item.target_sequence for item in link.messages if item.direction == "parent_to_child"),
            default=0,
        )
        message = AsyncSubagentMessage(
            message_id=str(uuid5(NAMESPACE_URL, f"async-message:{child_execution_id}:parent:{sequence}")),
            child_execution_id=child_execution_id,
            direction="parent_to_child",
            target_sequence=sequence,
            correlation_id=correlation_id,
            payload_ref=payload_ref,
            context_authority=context_authority,
            created_at=created_at,
        )
        await self._authority.append_message(request_scope, message)
        link = link.model_copy(update={"messages": (*link.messages, message), "updated_at": created_at})
        await self._details.save_link(request_scope, link)
        observation = await self._provider.update(execution, message)
        applied = message.model_copy(update={"receipt": "provider_applied"})
        link = link.model_copy(
            update={"messages": (*link.messages[:-1], applied), "updated_at": observation.observed_at}
        )
        await self._details.save_link(request_scope, link)
        await self._apply_observation(request_scope, execution, observation)
        return applied

    async def cancel(
        self, request_scope: str, child_execution_id: str, reason: str, requested_at: datetime
    ) -> AsyncSubagentExecution:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        await self._authority.request_cancellation(request_scope, child_execution_id, reason)
        await self._details.save_link(
            request_scope,
            link.model_copy(update={"cancellation_requested": True, "cancellation_reason": reason, "updated_at": requested_at}),
        )
        observation = await self._provider.cancel(execution)
        return await self._apply_observation(request_scope, execution, observation)

    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        *,
        parent_open: bool,
        current_generation: int,
        decided_at: datetime,
    ) -> ParentAsyncSubagentLink:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        manifest = execution.result_manifest
        if manifest is None:
            raise AsyncSubagentError("result admission requires a typed result manifest")
        late = not parent_open or execution.execution_generation != current_generation
        if late and decision in {"admit", "conditionally_admit"}:
            raise AsyncSubagentError("late or superseded child result cannot mutate the parent")
        await self._authority.decide_result(
            request_scope, child_execution_id, decision, manifest.manifest_digest
        )
        updated = link.model_copy(
            update={
                "result_decision": decision,
                "admitted_manifest_digest": manifest.manifest_digest if decision in {"admit", "conditionally_admit"} else None,
                "updated_at": decided_at,
            }
        )
        await self._details.save_link(request_scope, updated)
        return updated

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str, settled_at: datetime
    ) -> ParentAsyncSubagentLink:
        link = await self._details.get_link(request_scope, child_execution_id)
        if link.result_decision is None and link.dependency_class in {
            AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
            AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING,
        }:
            raise AsyncSubagentError("blocking child cannot settle before a result decision")
        await self._authority.settle(request_scope, child_execution_id, settlement_ref)
        updated = link.model_copy(update={"settled": True, "updated_at": settled_at})
        await self._details.save_link(request_scope, updated)
        return updated

    @staticmethod
    def parent_dependency(link: ParentAsyncSubagentLink) -> Literal["wait", "proceed", "degrade"]:
        if link.dependency_class == AsyncSubagentDependencyClass.REQUIRED_BLOCKING:
            return "proceed" if link.result_decision in {"admit", "conditionally_admit"} else "wait"
        if link.dependency_class == AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING:
            if link.result_decision in {"admit", "conditionally_admit"}:
                return "proceed"
            return "degrade" if link.result_decision == "reject" else "wait"
        return "proceed"

    async def _apply_observation(
        self,
        request_scope: str,
        execution: AsyncSubagentExecution,
        observation: ProviderAsyncObservation,
    ) -> AsyncSubagentExecution:
        statuses = {
            "running": AsyncSubagentLifecycle.RUNNING,
            "waiting": AsyncSubagentLifecycle.WAITING,
            "success": AsyncSubagentLifecycle.COMPLETED,
            "error": AsyncSubagentLifecycle.FAILED,
            "cancelled": AsyncSubagentLifecycle.CANCELLED,
            "orphaned": AsyncSubagentLifecycle.ORPHANED,
        }
        manifest = None
        if observation.status == "success":
            if not observation.output_ref or not observation.usage_ref or not observation.checkpoint_ref:
                raise AsyncSubagentError("provider success lacks canonical result references")
            manifest = AsyncSubagentResultManifest.create(
                manifest_id=str(uuid5(NAMESPACE_URL, f"async-result:{execution.child_execution_id}:{execution.execution_generation}")),
                child_execution_id=execution.child_execution_id,
                execution_generation=execution.execution_generation,
                output_refs=(observation.output_ref,),
                evidence_refs=observation.evidence_refs,
                usage_ref=observation.usage_ref,
                checkpoint_ref=observation.checkpoint_ref,
                effect_refs=observation.effect_refs,
                completed_at=observation.observed_at,
            )
        updated = execution.model_copy(
            update={
                "lifecycle": statuses[observation.status],
                "provider_thread_id": observation.thread_id,
                "provider_run_id": observation.run_id,
                "result_manifest": manifest,
                "updated_at": observation.observed_at,
            }
        )
        await self._details.save_execution(request_scope, updated)
        fact_ref = manifest.manifest_digest if manifest is not None else observation.status
        await self._authority.record_fact(request_scope, execution.child_execution_id, "result" if manifest else "lifecycle", fact_ref)
        return updated


class InMemoryAsyncSubagentDetailRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.contracts: dict[tuple[str, str], AsyncSubagentContract] = {}
        self.executions: dict[tuple[str, str], AsyncSubagentExecution] = {}
        self.links: dict[tuple[str, str], ParentAsyncSubagentLink] = {}

    async def create_before_submit(self, request_scope: str, contract: AsyncSubagentContract, execution: AsyncSubagentExecution, link: ParentAsyncSubagentLink) -> AsyncSubagentExecution:
        key = (request_scope, execution.child_execution_id)
        async with self._lock:
            prior = self.executions.get(key)
            if prior is not None:
                if prior.contract_digest != execution.contract_digest:
                    raise AsyncSubagentError("async child identity conflicts with prior contract")
                return deepcopy(prior)
            self.contracts[(request_scope, contract.contract_id)] = deepcopy(contract)
            self.executions[key] = deepcopy(execution)
            self.links[key] = deepcopy(link)
            return deepcopy(execution)

    async def get_execution(self, request_scope: str, child_execution_id: str) -> AsyncSubagentExecution:
        try:
            return deepcopy(self.executions[(request_scope, child_execution_id)])
        except KeyError as error:
            raise AsyncSubagentError("async child execution not found") from error

    async def get_link(self, request_scope: str, child_execution_id: str) -> ParentAsyncSubagentLink:
        try:
            return deepcopy(self.links[(request_scope, child_execution_id)])
        except KeyError as error:
            raise AsyncSubagentError("async child link not found") from error

    async def save_execution(self, request_scope: str, execution: AsyncSubagentExecution) -> None:
        self.executions[(request_scope, execution.child_execution_id)] = deepcopy(execution)

    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None:
        self.links[(request_scope, link.child_execution_id)] = deepcopy(link)


class InMemoryAsyncSubagentAuthority:
    """Test authority mirroring the dedicated PostgreSQL command/fact ledger."""

    def __init__(self) -> None:
        self.reservations: dict[tuple[str, str], dict[str, object]] = {}
        self.facts: list[tuple[str, str, str, str]] = []
        self.messages: list[tuple[str, AsyncSubagentMessage]] = []
        self.cancellations: dict[tuple[str, str], str] = {}
        self.decisions: dict[tuple[str, str], tuple[str, str]] = {}
        self.settlements: dict[tuple[str, str], str] = {}

    async def reserve_and_admit(self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str) -> None:
        self.reservations.setdefault((request.request_scope, child_execution_id), {"reservation_id": request.reservation_id, "link_id": link_id, "dependency_class": request.dependency_class.value})

    async def record_fact(self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str) -> None:
        item = (request_scope, child_execution_id, fact_kind, fact_ref)
        if item not in self.facts:
            self.facts.append(item)

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None:
        expected = 1 + max((item.target_sequence for scope, item in self.messages if scope == request_scope and item.child_execution_id == message.child_execution_id and item.direction == message.direction), default=0)
        if message.target_sequence != expected:
            raise AsyncSubagentError("message target sequence is not monotonic")
        self.messages.append((request_scope, deepcopy(message)))

    async def request_cancellation(self, request_scope: str, child_execution_id: str, reason: str) -> None:
        self.cancellations.setdefault((request_scope, child_execution_id), reason)

    async def decide_result(self, request_scope: str, child_execution_id: str, decision: Literal["admit", "conditionally_admit", "reject", "defer"], manifest_digest: str) -> None:
        key = (request_scope, child_execution_id)
        prior = self.decisions.get(key)
        if prior is not None and prior != (decision, manifest_digest):
            raise AsyncSubagentError("result has a conflicting authority decision")
        self.decisions[key] = (decision, manifest_digest)

    async def settle(self, request_scope: str, child_execution_id: str, settlement_ref: str) -> None:
        self.settlements.setdefault((request_scope, child_execution_id), settlement_ref)
