from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from app.application.orchestration import (
    RunControlLifecycleGateway,
    StageOperationExecutor,
    WorkflowEvaluator,
)
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.temporal.stagegraph_workflow import StageGraphWorkflow


class StageGraphActivities:
    """Nondeterministic StageGraph boundaries registered on a Temporal worker."""

    def __init__(
        self,
        operation_executor: StageOperationExecutor,
        workflow_evaluator: WorkflowEvaluator,
        lifecycle_gateway: RunControlLifecycleGateway,
    ) -> None:
        self._operation_executor = operation_executor
        self._workflow_evaluator = workflow_evaluator
        self._lifecycle_gateway = lifecycle_gateway

    @activity.defn(name="stagegraph.execute_operation")
    async def execute_operation(self, request: StageOperationRequest) -> StageOperationResult:
        result = await self._operation_executor.execute(request)
        return replace(result, temporal_activity_attempt=activity.info().attempt)

    @activity.defn(name="stagegraph.evaluate_workflow")
    async def evaluate_workflow(
        self, request: WorkflowEvaluationRequest
    ) -> WorkflowEvaluationResult:
        return await self._workflow_evaluator.evaluate(request)

    @activity.defn(name="stagegraph.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle_gateway.execute(request)


def create_stagegraph_worker(
    client: Client,
    *,
    task_queue: str,
    activities: StageGraphActivities,
) -> Worker:
    """Compose the F3 worker after F4 supplies concrete operation/evaluator ports."""

    return Worker(
        client,
        task_queue=task_queue,
        workflows=[StageGraphWorkflow],
        activities=[
            activities.execute_operation,
            activities.evaluate_workflow,
            activities.apply_lifecycle_command,
        ],
    )
