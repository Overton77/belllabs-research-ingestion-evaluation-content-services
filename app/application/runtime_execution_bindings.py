from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from app.domain.graph_runtime.contracts import (
    GraphExecutionSubmission,
    InterventionReceipt,
    RuntimeAsyncTaskProjection,
    RuntimeExecutionAttempt,
    RuntimeExecutionBinding,
    RuntimeExecutionProjection,
    RuntimeIntervention,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.run_control.errors import IdempotencyConflict


class RuntimeBindingConflict(IdempotencyConflict):
    """A runtime identity or submission was reused with different immutable intent."""


@dataclass(frozen=True)
class RuntimeBindingReservation:
    binding: RuntimeExecutionBinding
    created: bool


class RuntimeExecutionBindingRepository(Protocol):
    async def create_binding(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> RuntimeBindingReservation: ...

    async def get_binding(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionBinding | None: ...

    async def get_by_submission(
        self,
        request_scope: str,
        submission_id: str,
    ) -> RuntimeExecutionBinding | None: ...

    async def append_attempt(
        self,
        attempt: RuntimeExecutionAttempt,
    ) -> RuntimeExecutionAttempt: ...

    async def update_binding(
        self,
        binding: RuntimeExecutionBinding,
        *,
        expected_version: int,
    ) -> RuntimeExecutionBinding: ...

    async def projection(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionProjection | None: ...


class RuntimeInterventionRepository(Protocol):
    async def reserve(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> bool: ...

    async def record(
        self,
        intervention: RuntimeIntervention,
        receipt: InterventionReceipt,
    ) -> InterventionReceipt: ...

    async def get_intervention(
        self,
        request_scope: str,
        command_id: str,
    ) -> tuple[RuntimeIntervention, InterventionReceipt] | None: ...


class RuntimeAsyncTaskRepository(Protocol):
    async def put(
        self,
        task: RuntimeAsyncTaskProjection,
        *,
        expected_version: int | None,
    ) -> RuntimeAsyncTaskProjection: ...

    async def get_task(
        self,
        request_scope: str,
        async_task_id: str,
    ) -> RuntimeAsyncTaskProjection | None: ...


class InMemoryRuntimeCoordinationRepository(
    RuntimeExecutionBindingRepository,
    RuntimeInterventionRepository,
    RuntimeAsyncTaskRepository,
):
    """Concurrency-safe behavioral adapter matching PostgreSQL uniqueness semantics."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._bindings: dict[tuple[str, str, int], RuntimeExecutionBinding] = {}
        self._submissions: dict[tuple[str, str], tuple[str, RuntimeExecutionBinding]] = {}
        self._attempts: dict[str, list[RuntimeExecutionAttempt]] = {}
        self._interventions: dict[
            tuple[str, str], tuple[RuntimeIntervention, InterventionReceipt]
        ] = {}
        self._tasks: dict[tuple[str, str], RuntimeAsyncTaskProjection] = {}

    async def create_binding(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> RuntimeBindingReservation:
        key = _epoch_tuple(binding.epoch)
        submission_key = (binding.epoch.request_scope, submission.submission_id)
        idempotency_key = (
            binding.epoch.request_scope,
            f"idempotency:{submission.idempotency_key}",
        )
        async with self._lock:
            prior_submission = self._submissions.get(submission_key) or self._submissions.get(
                idempotency_key
            )
            if prior_submission is not None:
                digest, prior = prior_submission
                if digest != submission.request_digest or prior != binding:
                    raise RuntimeBindingConflict(
                        "runtime submission identity was reused with conflicting intent"
                    )
                return RuntimeBindingReservation(binding=deepcopy(prior), created=False)
            existing_binding = self._bindings.get(key)
            if existing_binding is not None:
                if existing_binding != binding:
                    raise RuntimeBindingConflict(
                        "execution epoch already has a different runtime binding"
                    )
                return RuntimeBindingReservation(
                    binding=deepcopy(existing_binding),
                    created=False,
                )
            if binding.active and any(
                item.active
                and item.epoch.request_scope == binding.epoch.request_scope
                and item.epoch.belllabs_run_id == binding.epoch.belllabs_run_id
                and item.epoch.execution_epoch == binding.epoch.execution_epoch
                for item in self._bindings.values()
            ):
                raise RuntimeBindingConflict("execution epoch already has an active binding")
            self._bindings[key] = deepcopy(binding)
            self._submissions[submission_key] = (
                submission.request_digest,
                deepcopy(binding),
            )
            self._submissions[idempotency_key] = (
                submission.request_digest,
                deepcopy(binding),
            )
            return RuntimeBindingReservation(binding=deepcopy(binding), created=True)

    async def get_binding(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionBinding | None:
        return deepcopy(self._bindings.get(_epoch_tuple(epoch)))

    async def get_by_submission(
        self,
        request_scope: str,
        submission_id: str,
    ) -> RuntimeExecutionBinding | None:
        prior = self._submissions.get((request_scope, submission_id))
        return deepcopy(prior[1]) if prior is not None else None

    async def append_attempt(
        self,
        attempt: RuntimeExecutionAttempt,
    ) -> RuntimeExecutionAttempt:
        async with self._lock:
            attempts = self._attempts.setdefault(attempt.binding_id, [])
            prior = next(
                (
                    item
                    for item in attempts
                    if item.attempt_key.runtime_attempt == attempt.attempt_key.runtime_attempt
                ),
                None,
            )
            if prior is not None:
                if prior != attempt:
                    raise RuntimeBindingConflict(
                        "runtime attempt identity was reused with conflicting facts"
                    )
                return deepcopy(prior)
            attempts.append(deepcopy(attempt))
            return deepcopy(attempt)

    async def update_binding(
        self,
        binding: RuntimeExecutionBinding,
        *,
        expected_version: int,
    ) -> RuntimeExecutionBinding:
        key = _epoch_tuple(binding.epoch)
        async with self._lock:
            prior = self._bindings.get(key)
            if prior is None:
                raise LookupError("runtime binding not found")
            if prior.version != expected_version:
                raise RuntimeBindingConflict("runtime binding version is stale")
            if binding.version != expected_version + 1:
                raise ValueError("runtime binding updates must advance one version")
            immutable = (
                "binding_id",
                "epoch",
                "submission_id",
                "submission_idempotency_key",
                "submission_digest",
                "run_plan_digest",
                "graph_assembly_digest",
                "state_schema_digest",
                "runtime_provider",
                "deployment",
                "agent_thread",
                "graph_id",
                "created_at",
            )
            if any(getattr(prior, field) != getattr(binding, field) for field in immutable):
                raise RuntimeBindingConflict("runtime binding immutable identity changed")
            self._bindings[key] = deepcopy(binding)
            for submission_key, (digest, submitted_binding) in tuple(self._submissions.items()):
                if submitted_binding.binding_id == binding.binding_id:
                    self._submissions[submission_key] = (digest, deepcopy(binding))
            return deepcopy(binding)

    async def projection(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionProjection | None:
        binding = await self.get_binding(epoch)
        if binding is None:
            return None
        return RuntimeExecutionProjection(
            binding=binding,
            attempts=tuple(deepcopy(self._attempts.get(binding.binding_id, []))),
        )

    async def record(
        self,
        intervention: RuntimeIntervention,
        receipt: InterventionReceipt,
    ) -> InterventionReceipt:
        key = (intervention.epoch.request_scope, intervention.command_id)
        async with self._lock:
            prior = self._interventions.get(key)
            if prior is not None:
                if (
                    prior[0].request_digest != intervention.request_digest
                    or prior[0] != intervention
                ):
                    raise RuntimeBindingConflict(
                        "intervention command identity was reused with conflicting intent"
                    )
                if prior[1] == receipt:
                    return deepcopy(prior[1])
                if prior[1].reason_code != "provider_application_pending":
                    raise RuntimeBindingConflict(
                        "intervention receipt conflicts with its prior completion"
                    )
                completed = (deepcopy(intervention), deepcopy(receipt))
                self._interventions[key] = completed
                self._interventions[
                    (
                        intervention.epoch.request_scope,
                        f"idempotency:{intervention.idempotency_key}",
                    )
                ] = completed
                return deepcopy(receipt)
            self._interventions[key] = (deepcopy(intervention), deepcopy(receipt))
            return deepcopy(receipt)

    async def reserve(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> bool:
        command_key = (intervention.epoch.request_scope, intervention.command_id)
        idempotency_key = (
            intervention.epoch.request_scope,
            f"idempotency:{intervention.idempotency_key}",
        )
        async with self._lock:
            prior = self._interventions.get(command_key) or self._interventions.get(idempotency_key)
            if prior is not None:
                if prior[0] != intervention:
                    raise RuntimeBindingConflict(
                        "intervention idempotency identity has conflicting intent"
                    )
                return False
            pending = InterventionReceipt(
                command_id=intervention.command_id,
                status="reconciliation_required",
                binding_id=binding_id,
                resulting_belllabs_version=intervention.expected_belllabs_version,
                reason_code="provider_application_pending",
                recorded_at=intervention.requested_at,
            )
            value = (deepcopy(intervention), pending)
            self._interventions[command_key] = value
            self._interventions[idempotency_key] = value
            return True

    async def get_intervention(
        self,
        request_scope: str,
        command_id: str,
    ) -> tuple[RuntimeIntervention, InterventionReceipt] | None:
        return deepcopy(self._interventions.get((request_scope, command_id)))

    async def put(
        self,
        task: RuntimeAsyncTaskProjection,
        *,
        expected_version: int | None,
    ) -> RuntimeAsyncTaskProjection:
        key = (
            task.parent_epoch.request_scope,
            task.task.async_task_id,
        )
        async with self._lock:
            prior = self._tasks.get(key)
            if prior is None:
                if expected_version is not None:
                    raise RuntimeBindingConflict("async task does not exist at expected version")
            else:
                if expected_version != prior.version or task.version != prior.version + 1:
                    raise RuntimeBindingConflict("async task version is stale")
                if (
                    prior.task != task.task
                    or prior.binding_id != task.binding_id
                    or prior.parent_epoch != task.parent_epoch
                    or prior.request_digest != task.request_digest
                    or prior.created_at != task.created_at
                ):
                    raise RuntimeBindingConflict("async task immutable identity changed")
            self._tasks[key] = deepcopy(task)
            return deepcopy(task)

    async def get_task(
        self,
        request_scope: str,
        async_task_id: str,
    ) -> RuntimeAsyncTaskProjection | None:
        return deepcopy(self._tasks.get((request_scope, async_task_id)))


def _epoch_tuple(epoch: ExecutionEpochKey) -> tuple[str, str, int]:
    return (epoch.request_scope, epoch.belllabs_run_id, epoch.execution_epoch)


def touch_binding(
    binding: RuntimeExecutionBinding,
    *,
    observed_at: datetime,
    **changes: object,
) -> RuntimeExecutionBinding:
    return binding.model_copy(
        update={
            **changes,
            "version": binding.version + 1,
            "updated_at": observed_at,
        }
    )
