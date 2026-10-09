"""`mc.human_gate.v1`: the Human Gate control activation (SPEC-03, ADR-0038, MP-10).

A family (StageGraph gate stage, GoalDirected review) starts this workflow as a child with
one immutable ``HumanGateActivation``. It commits task-open plus its event and outbox row
through a short activity, then waits durably: ``workflow.wait_condition`` on a resolution
hint signal or a bounded poll/deadline timer, holding no activity and no cognition slot.
On every wake it re-reads the persisted task (the only authority); the signal is a hint, so
a hint lost between commit and delivery delays nothing beyond the poll interval. At the
deadline the gate's timeout policy is applied by the repository (never implicit approval;
``keep_waiting`` keeps waiting). Cancelling the activation cancels the open task. The
result is the outcome bound to this activation's exact task and packet digest.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from mission_control.domain.programs.human_gate import (
        HumanGateActivation,
        HumanGateBindingError,
        HumanGateCancelRequest,
        HumanGateObservation,
        HumanGateObserveRequest,
        HumanGateOutcome,
        bind_outcome,
    )

HUMAN_GATE_WORKFLOW = "mc.human_gate.v1"
CANCEL_ACTOR = "mission-control-runtime/human-gate-cancel"


@dataclass(frozen=True)
class HumanGateWorkflowInput:
    activation: HumanGateActivation
    poll_seconds: int = 300
    activity_timeout_seconds: int = 30


@workflow.defn(name=HUMAN_GATE_WORKFLOW)
class HumanGateWorkflow:
    def __init__(self) -> None:
        self._hints = 0
        self._observation: HumanGateObservation | None = None

    @workflow.signal
    def resolution_committed(self, human_task_id: str) -> None:
        del human_task_id  # a wake-up hint only; the persisted task decides
        self._hints += 1

    @workflow.query
    def state(self) -> dict[str, Any]:
        observation = self._observation
        return {
            "hints": self._hints,
            "lifecycle": observation.lifecycle if observation else None,
            "version": observation.version if observation else None,
            "escalated": observation.escalated if observation else False,
        }

    @workflow.run
    async def run(self, run_input: HumanGateWorkflowInput) -> HumanGateOutcome:
        activation = run_input.activation
        if not isinstance(activation, HumanGateActivation):
            activation = HumanGateActivation.model_validate(activation)
        timeout = timedelta(seconds=run_input.activity_timeout_seconds)
        retry = RetryPolicy(maximum_attempts=5)
        task_id = str(activation.human_task_id)
        self._observation = await workflow.execute_activity(
            "human_gate.open",
            activation,
            result_type=HumanGateObservation,
            start_to_close_timeout=timeout,
            retry_policy=retry,
        )
        deadline = activation.deadline_at
        timeout_applied = False
        try:
            while self._observation.outcome is None:
                seen = self._hints
                wait_seconds = float(run_input.poll_seconds)
                if deadline is not None and not timeout_applied:
                    wait_seconds = min(wait_seconds, (deadline - workflow.now()).total_seconds())
                if wait_seconds > 0:

                    def hinted(bound: int = seen) -> bool:
                        return self._hints > bound

                    with contextlib.suppress(TimeoutError):
                        await workflow.wait_condition(
                            hinted, timeout=timedelta(seconds=max(wait_seconds, 1.0))
                        )
                apply_timeout = (
                    deadline is not None and not timeout_applied and workflow.now() >= deadline
                )
                self._observation = await workflow.execute_activity(
                    "human_gate.observe",
                    HumanGateObserveRequest(
                        request_scope=activation.request_scope,
                        human_task_id=task_id,
                        now=workflow.now(),
                        apply_timeout=apply_timeout,
                    ),
                    result_type=HumanGateObservation,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
                timeout_applied = timeout_applied or apply_timeout
        except asyncio.CancelledError:
            # The run stops or cancels: an outstanding task is cancelled consistently.
            await workflow.execute_activity(
                "human_gate.cancel",
                HumanGateCancelRequest(
                    request_scope=activation.request_scope,
                    human_task_id=task_id,
                    now=workflow.now(),
                    actor_ref=CANCEL_ACTOR,
                ),
                result_type=HumanGateObservation,
                start_to_close_timeout=timeout,
                retry_policy=retry,
            )
            raise
        try:
            return bind_outcome(activation, self._observation.outcome)
        except HumanGateBindingError as error:
            raise ApplicationError(
                str(error), type="human_gate_binding", non_retryable=True
            ) from error


__all__ = ["HUMAN_GATE_WORKFLOW", "HumanGateWorkflow", "HumanGateWorkflowInput"]
