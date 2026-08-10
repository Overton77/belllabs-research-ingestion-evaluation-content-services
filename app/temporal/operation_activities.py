from __future__ import annotations

from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.operation_execution import (
    OperationExecutionInProgress,
    OperationExecutionService,
)
from app.domain.operation_execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.temporal.registration.activities import agent_cognitive_activities


class OperationExecutionActivities:
    def __init__(self, service: OperationExecutionService) -> None:
        self._service = service

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            request = OperationExecutionRequest.model_validate(payload)
            result = await self._service.execute(request)
        except OperationExecutionInProgress as error:
            raise ApplicationError(str(error), type="operation_execution_in_progress") from error
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
