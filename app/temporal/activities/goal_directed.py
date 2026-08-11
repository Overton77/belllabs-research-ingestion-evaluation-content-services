from __future__ import annotations

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
)
from app.application.orchestration import RunControlLifecycleGateway
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


__all__ = ["GoalDirectedActivities"]
