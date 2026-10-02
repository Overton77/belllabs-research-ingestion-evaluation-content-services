from __future__ import annotations

import os
import socket
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.operations.operation_execution import (
    OperationExecutionInProgress,
    OperationExecutionService,
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


def default_worker_identity() -> str:
    return f"{os.getpid()}@{socket.gethostname()}"


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
            result = await self._service.execute(request, attempt)
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
        return result.model_dump(mode="json")


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
