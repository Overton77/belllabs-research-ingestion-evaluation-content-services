"""Short, non-cognitive activities of the Human Gate control activation (MP-10).

They run on the family's coordinator queue: open the task (task row, event and outbox in
one transaction), observe or time out the persisted task, cancel it, and release a gate
stage's admitted reservation. None of them waits for a human.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.exceptions import ApplicationError

from mission_control.application.human_tasks.service import HumanTaskRepository
from mission_control.application.programs.human_gates import GateReservationSettlement
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.programs.human_gate import (
    GateReservationSettlementRequest,
    GateReservationSettlementResult,
    HumanGateActivation,
    HumanGateCancelRequest,
    HumanGateObservation,
    HumanGateObserveRequest,
    HumanTaskView,
    observation_of,
)

OPEN_ACTOR = "mission-control-runtime/human-gate"


class HumanGateActivities:
    def __init__(
        self,
        repository: HumanTaskRepository,
        *,
        settlement: GateReservationSettlement | None = None,
    ) -> None:
        self._repository = repository
        self._settlement = settlement

    @activity.defn(name="human_gate.open")
    async def open(self, activation: HumanGateActivation) -> HumanGateObservation:
        try:
            task = await self._repository.open(activation, actor_ref=OPEN_ACTOR)
        except IdempotencyConflict as error:
            raise ApplicationError(
                str(error), type="human_gate_identity_conflict", non_retryable=True
            ) from error
        return observation_of(task)

    @activity.defn(name="human_gate.observe")
    async def observe(self, request: HumanGateObserveRequest) -> HumanGateObservation:
        task = (
            await self._repository.apply_timeout(
                request.request_scope, request.human_task_id, now=request.now
            )
            if request.apply_timeout
            else await self._repository.get(request.request_scope, request.human_task_id)
        )
        return observation_of(_present(task, request.human_task_id))

    @activity.defn(name="human_gate.cancel")
    async def cancel(self, request: HumanGateCancelRequest) -> HumanGateObservation:
        task = await self._repository.cancel(
            request.request_scope,
            request.human_task_id,
            now=request.now,
            actor_ref=request.actor_ref,
        )
        return observation_of(_present(task, request.human_task_id))

    @activity.defn(name="human_gate.settle_stage_reservation")
    async def settle_stage_reservation(
        self, request: GateReservationSettlementRequest
    ) -> GateReservationSettlementResult:
        if self._settlement is None:
            raise ApplicationError(
                "gate reservation settlement is not composed on this worker",
                type="human_gate_settlement_unavailable",
                non_retryable=True,
            )
        return await self._settlement.settle(request)

    @property
    def functions(self) -> list[object]:
        return [self.open, self.observe, self.cancel, self.settle_stage_reservation]


def _present(task: HumanTaskView | None, human_task_id: str) -> HumanTaskView:
    if task is None:
        raise ApplicationError(
            f"human task {human_task_id} is not persisted",
            type="human_task_missing",
            non_retryable=True,
        )
    return task


__all__ = ["HumanGateActivities"]
