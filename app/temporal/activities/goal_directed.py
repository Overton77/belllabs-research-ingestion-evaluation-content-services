from __future__ import annotations

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.coordinator.coordinator_results import TerminalWorkflowCompletionPort
from app.application.orchestration.goal_directed import (
    GoalDirectedDocumentRepository,
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    GoalOperationTemplateProvider,
)
from app.application.orchestration.service import RunControlLifecycleGateway
from app.application.run_control.service import RunControlService
from app.application.operations.semantic_operation_bindings import SemanticOperationBindingRepository
from app.domain.coordinator.launch import LaunchAuthorizationError, TerminalWorkflowCompletion
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.domain.orchestration.goal_directed_runtime import (
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import ActorContext
from app.temporal.registration.activities import coordinator_activities
from app.temporal.registration.workflows import coordinator_workflows
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class GoalDirectedActivities:
    """GoalDirected I/O adapters; cognition runs only through OperationWorkflow."""

    def __init__(
        self,
        *,
        operations: GoalDirectedOperationPreparationService,
        results: GoalDirectedOperationResultService,
        lifecycle: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
    ) -> None:
        self._operations = operations
        self._results = results
        self._lifecycle = lifecycle
        self._completion = completion

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    @activity.defn(name="goaldirected.prepare_executor")
    async def execute_iteration(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        if request.operation_role != "executor":
            raise ApplicationError(
                "executor preparation received another operation role",
                type="goal_operation_role_mismatch",
                non_retryable=True,
            )
        return await self._operations.prepare(request)

    @activity.defn(name="goaldirected.prepare_verifier")
    async def verify_iteration(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        if request.operation_role != "verifier":
            raise ApplicationError(
                "verifier preparation received another operation role",
                type="goal_operation_role_mismatch",
                non_retryable=True,
            )
        return await self._operations.prepare(request)

    @activity.defn(name="goaldirected.reconcile_operation")
    async def prepare_handoff(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        return await self._results.reconcile(request)

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle.execute(request)

    @activity.defn(name="coordinator.materialize_workflow_result")
    async def materialize_workflow_result(
        self, completion: TerminalWorkflowCompletion
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


def compose_goal_directed_activities(
    *,
    run_control: RunControlService,
    operation_bindings: SemanticOperationBindingRepository,
    templates: GoalOperationTemplateProvider,
    documents: GoalDirectedDocumentRepository,
    lifecycle: RunControlLifecycleGateway,
    actor: ActorContext,
    completion: TerminalWorkflowCompletionPort | None = None,
) -> GoalDirectedActivities:
    """Wire production GoalDirected activities on the OperationWorkflow path."""

    return GoalDirectedActivities(
        operations=GoalDirectedOperationPreparationService(
            templates=templates,
            operation_bindings=operation_bindings,
            run_control=run_control,
            documents=documents,
            actor=actor,
        ),
        results=GoalDirectedOperationResultService(documents),
        lifecycle=lifecycle,
        completion=completion,
    )


def create_goal_directed_worker(
    client: Client,
    *,
    task_queue: str,
    activities: GoalDirectedActivities,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=coordinator_workflows("GoalDirected"),
        workflow_runner=coordinator_workflow_runner(),
        activities=coordinator_activities("GoalDirected", activities),
    )


__all__ = [
    "GoalDirectedActivities",
    "compose_goal_directed_activities",
    "create_goal_directed_worker",
]
