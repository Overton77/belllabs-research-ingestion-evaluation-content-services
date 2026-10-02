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
from temporalio.worker import Worker

from app.application.operations.operation_execution import (
    ForkMaterializationPending,
    OperationExecutionInProgress,
    OperationExecutionService,
)
from app.application.operations.operation_progress import (
    CURRENT_PROGRESS,
    CognitionProgress,
)
from app.domain.operation_execution.checkpoint_lineage import (
    CheckpointLineageConflict,
    CheckpointLineageError,
    CheckpointNamespaceBusy,
    OperationActivityAttempt,
)
from app.domain.operation_execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.temporal.registration.activities import agent_cognitive_activities

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
    ) -> None:
        self._service = service
        self._worker_identity = worker_identity or default_worker_identity()

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
            CURRENT_PROGRESS.reset(token)
        # The final heartbeat carries the settled phase and the result checkpoint key.
        activity.heartbeat(progress.payload())
        return result.model_dump(mode="json")


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
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        activities=agent_cognitive_activities(activities),
    )


def parse_operation_result(payload: dict[str, Any]) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(payload)
