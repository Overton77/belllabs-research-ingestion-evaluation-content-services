from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker, WorkerDeploymentConfig

from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.operations.operation_execution import (
    ForkMaterializationPending,
    OperationExecutionInProgress,
    OperationExecutionService,
)
from mission_control.application.execution.operations.operation_progress import (
    CURRENT_CANCEL_PROBE,
    CURRENT_PROGRESS,
    CognitionProgress,
)
from mission_control.domain.execution.checkpoint_lineage import (
    CheckpointLineageConflict,
    CheckpointLineageError,
    CheckpointNamespaceBusy,
    OperationActivityAttempt,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from mission_control.domain.policies.errors import IdempotencyConflict

# RRM-008 (REQ-CP-EXEC-008 step 3): a cognitive Activity heartbeats compact progress at a
# fraction of its heartbeat timeout, so a Temporal cancel reaches running cognition and a
# lost worker is detected by the heartbeat timeout, not the start-to-close timeout.
DEFAULT_HEARTBEAT_INTERVAL = timedelta(seconds=15)
HEARTBEAT_FRACTION = 3


def default_worker_identity() -> str:
    return f"{os.getpid()}@{socket.gethostname()}"


def heartbeat_interval(heartbeat_timeout: timedelta | None) -> timedelta:
    """One third of the declared heartbeat timeout (at least one second), else the default."""

    if heartbeat_timeout is None:
        return DEFAULT_HEARTBEAT_INTERVAL
    return max(heartbeat_timeout / HEARTBEAT_FRACTION, timedelta(seconds=1))


class OperationExecutionActivities:
    def __init__(
        self,
        service: OperationExecutionService,
        *,
        worker_identity: str | None = None,
        lane_turns: LaneTurnService | None = None,
    ) -> None:
        self._service = service
        self._worker_identity = worker_identity or default_worker_identity()
        # FT-G2: `lane.turn`, `lane.status`, `lane.cancel` beside the operation pair; every
        # worker that serves `operation.execute` serves them on the same queue.
        self._lanes: Any = None
        if lane_turns is not None:
            from mission_control.adapters.temporal.activities.lane_turn import (
                LaneTurnActivities,
            )

            self._lanes = LaneTurnActivities(lane_turns, self)

    def lane_activities(self) -> tuple[Any, ...]:
        """The lane activity surface (empty when no lane turn service is composed)."""

        if self._lanes is None:
            return ()
        return (self._lanes.turn, self._lanes.status, self._lanes.cancel)

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._run(payload, self._service.execute)

    @activity.defn(name="operation.cancel")
    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        """RRM-008: the cancellation saga's settlement of one unit; never dispatches cognition.

        It stands down (retryably) behind a live holder that is settling the unit itself and
        takes over a lost holder's lease when it expires, so cancellation survives worker loss.
        """

        return await self._run(payload, self._service.cancel)

    async def run_governed(
        self, payload: dict[str, Any], *, cancel: bool = False
    ) -> dict[str, Any]:
        """FT-G2: the governed `execute` (or `cancel`) body inside another activity.

        `lane.turn` and `lane.cancel` run the Deep Agents lane through exactly this path,
        with the same heartbeats, cancel probe, claim lease and error classification as
        `operation.execute` / `operation.cancel` (which stay registered until FT-G6).
        """

        return await self._run(payload, self._service.cancel if cancel else self._service.execute)

    @property
    def service(self) -> OperationExecutionService:
        return self._service

    @property
    def worker_identity(self) -> str:
        return self._worker_identity

    async def _run(
        self,
        payload: dict[str, Any],
        body: Callable[
            [OperationExecutionRequest, OperationActivityAttempt],
            Awaitable[OperationExecutionResult],
        ],
    ) -> dict[str, Any]:
        info = activity.info()
        # REQ-CP-EXEC-014: the real Temporal delivery is observed, never part of identity.
        # Its claim lease ends with the scheduler's own deadline for this attempt, so the
        # retry Temporal schedules after a lost worker finds the lease expired.
        attempt = OperationActivityAttempt(
            workflow_id=info.workflow_id,
            workflow_run_id=info.workflow_run_id,
            activity_id=info.activity_id,
            attempt=info.attempt,
            worker_identity=self._worker_identity,
            lease_expires_at=(
                info.started_time + info.start_to_close_timeout
                if info.start_to_close_timeout is not None
                else None
            ),
        )
        try:
            request = OperationExecutionRequest.model_validate(payload)
        except ValueError as error:
            raise ApplicationError(
                str(error), type="operation_execution_rejected", non_retryable=True
            ) from error
        unit = request.runtime_unit
        progress = CognitionProgress(
            unit_key=unit.unit_key if unit is not None else None,
            execution_generation=(
                request.deep_agent_binding.execution_generation
                if request.deep_agent_binding is not None
                else 1
            ),
            activity_attempt=info.attempt,
        )
        token = CURRENT_PROGRESS.set(progress)
        probe_token = CURRENT_CANCEL_PROBE.set(_temporal_cancel_requested)
        heartbeats = asyncio.create_task(
            _heartbeat_loop(progress, heartbeat_interval(info.heartbeat_timeout))
        )
        try:
            result = await body(request, attempt)
        except ForkMaterializationPending as error:
            # RRM-006: transient; retried until the fork's materialization is visible.
            raise ApplicationError(str(error), type="fork_not_materialized") from error
        except OperationExecutionInProgress as error:
            raise ApplicationError(str(error), type="operation_execution_in_progress") from error
        except CheckpointNamespaceBusy as error:
            raise ApplicationError(str(error), type="checkpoint_namespace_busy") from error
        except CheckpointLineageError as error:
            raise ApplicationError(
                str(error),
                type=(
                    "checkpoint_lineage_conflict"
                    if isinstance(error, CheckpointLineageConflict)
                    else "checkpoint_lineage_in_doubt"
                ),
                non_retryable=True,
            ) from error
        except (IdempotencyConflict, ValueError) as error:
            raise ApplicationError(
                str(error),
                type="operation_execution_rejected",
                non_retryable=True,
            ) from error
        finally:
            heartbeats.cancel()
            CURRENT_CANCEL_PROBE.reset(probe_token)
            CURRENT_PROGRESS.reset(token)
        # The final heartbeat carries the settled phase and the result checkpoint key.
        activity.heartbeat(progress.payload())
        return result.model_dump(mode="json")


def _temporal_cancel_requested() -> bool:
    """True only for a requested cancellation of this Activity (RRM-008 review F1).

    `worker_shutdown`, `timed_out`, `paused`, `reset` and `not_found` also cancel the task;
    none of them is a cancellation of the unit.
    """

    details = activity.cancellation_details()
    return details is not None and details.cancel_requested


async def _heartbeat_loop(progress: CognitionProgress, interval: timedelta) -> None:
    """Heartbeat compact progress until the attempt ends; cancellation is observed here."""

    while True:
        activity.heartbeat(await progress.observe())
        if activity.is_cancelled():
            progress.cancel_observed = True
        await asyncio.sleep(interval.total_seconds())


def create_agent_cognitive_worker(
    client: Client,
    *,
    task_queue: str,
    activities: OperationExecutionActivities,
    graceful_shutdown_timeout: timedelta = timedelta(),
    deployment_config: WorkerDeploymentConfig | None = None,
) -> Worker:
    """`operation.execute` and `operation.cancel` (RRM-008) on the cognitive queue.

    A deployment drains with a `graceful_shutdown_timeout` shorter than every heartbeat
    timeout it serves (`OperationHeartbeatPolicy.verify_graceful_shutdown`): a worker
    shutdown is never a cancel of the unit, so the drained attempt settles nothing and
    Temporal's retry lands on a live worker.
    """

    return Worker(
        client,
        task_queue=task_queue,
        activities=agent_cognitive_activities(activities),
        graceful_shutdown_timeout=graceful_shutdown_timeout,
        deployment_config=deployment_config,
    )


def parse_operation_result(payload: dict[str, Any]) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(payload)
