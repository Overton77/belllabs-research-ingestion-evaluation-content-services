from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.orchestration import (
    RunControlLifecycleGateway,
    StageOperationExecutor,
    WorkflowEvaluator,
)
from app.application.orchestration_binding_repository import SemanticInputBindingNotFound
from app.application.orchestration_routing import SemanticRoutingError
from app.domain.coordinator.launch import (
    LaunchAuthorizationError,
    TerminalWorkflowCompletion,
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
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class StageGraphActivities:
    """Nondeterministic StageGraph boundaries registered on a Temporal worker."""

    def __init__(
        self,
        operation_executor: StageOperationExecutor,
        workflow_evaluator: WorkflowEvaluator,
        lifecycle_gateway: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
    ) -> None:
        self._operation_executor = operation_executor
        self._workflow_evaluator = workflow_evaluator
        self._lifecycle_gateway = lifecycle_gateway
        self._completion = completion

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    @activity.defn(name="stagegraph.execute_operation")
    async def execute_operation(self, request: StageOperationRequest) -> StageOperationResult:
        try:
            result = await self._operation_executor.execute(request)
        except (SemanticRoutingError, SemanticInputBindingNotFound) as error:
            raise ApplicationError(
                str(error),
                type="semantic_input_binding_rejected",
                non_retryable=True,
            ) from error
        return replace(result, temporal_activity_attempt=activity.info().attempt)

    @activity.defn(name="stagegraph.evaluate_workflow")
    async def evaluate_workflow(
        self, request: WorkflowEvaluationRequest
    ) -> WorkflowEvaluationResult:
        try:
            return await self._workflow_evaluator.evaluate(request)
        except (SemanticRoutingError, SemanticInputBindingNotFound) as error:
            raise ApplicationError(
                str(error),
                type="semantic_input_binding_rejected",
                non_retryable=True,
            ) from error

    @activity.defn(name="stagegraph.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle_gateway.execute(request)

    @activity.defn(name="coordinator.materialize_workflow_result")
    async def materialize_workflow_result(
        self,
        completion: TerminalWorkflowCompletion,
    ) -> object:
        if self._completion is None:
            raise ApplicationError(
                "typed Workflow Result materializer is unavailable",
                type="workflow_result_materializer_unavailable",
                non_retryable=True,
            )
        try:
            return await self._completion.complete(completion)
        except (LaunchAuthorizationError, ValueError) as error:
            raise ApplicationError(
                str(error),
                type="workflow_result_completion_conflict",
                non_retryable=True,
            ) from error


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
        workflow_runner=coordinator_workflow_runner(),
        activities=[
            activities.execute_operation,
            activities.evaluate_workflow,
            activities.apply_lifecycle_command,
            activities.materialize_workflow_result,
        ],
    )
