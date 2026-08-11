from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.orchestration import (
    RunControlLifecycleGateway,
    StageGraphDecisionService,
    StageGraphOperationMaterializer,
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
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
    StageGraphInitializeRequest,
    StageGraphInitializeResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.temporal.registration.activities import coordinator_activities
from app.temporal.registration.workflows import coordinator_workflows
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class StageGraphActivities:
    """Nondeterministic StageGraph boundaries registered on a Temporal worker."""

    def __init__(
        self,
        operation_executor: StageOperationExecutor,
        workflow_evaluator: WorkflowEvaluator,
        lifecycle_gateway: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
        decision_service: StageGraphDecisionService | None = None,
        operation_materializer: StageGraphOperationMaterializer | None = None,
    ) -> None:
        self._operation_executor = operation_executor
        self._workflow_evaluator = workflow_evaluator
        self._lifecycle_gateway = lifecycle_gateway
        self._completion = completion
        self._decision_service = decision_service
        self._operation_materializer = operation_materializer

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    def _canonical_decisions(self) -> StageGraphDecisionService:
        if self._decision_service is None:
            raise ApplicationError(
                "canonical StageGraph decision service is unavailable",
                type="stagegraph_decision_service_unavailable",
                non_retryable=True,
            )
        return self._decision_service

    @activity.defn(name="stagegraph.initialize")
    async def initialize(
        self, request: StageGraphInitializeRequest
    ) -> StageGraphInitializeResult:
        return await self._canonical_decisions().initialize(request)

    @activity.defn(name="stagegraph.admit_operation")
    async def admit_operation(
        self, request: StageGraphAdmissionActivityRequest
    ) -> StageGraphAdmissionActivityResult:
        if request.operation is None:
            if self._operation_materializer is None:
                raise ApplicationError(
                    "canonical StageGraph operation materializer is unavailable",
                    type="stagegraph_operation_materializer_unavailable",
                    non_retryable=True,
                )
            request = replace(
                request,
                operation=await self._operation_materializer.materialize(request),
            )
        return await self._canonical_decisions().admit_operation(request)

    @activity.defn(name="stagegraph.decide_result")
    async def decide_result(
        self, request: StageGraphResultActivityRequest
    ) -> StageGraphResultActivityResult:
        return await self._canonical_decisions().decide_result(request)

    @activity.defn(name="stagegraph.complete")
    async def complete_stagegraph(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        return await self._canonical_decisions().complete(request)

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
        return result

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
        workflows=coordinator_workflows("StageGraph"),
        workflow_runner=coordinator_workflow_runner(),
        activities=coordinator_activities("StageGraph", activities),
    )
