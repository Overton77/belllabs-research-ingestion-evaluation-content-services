from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.orchestration import (
    GoalHandoffPreparer,
    GoalIndependentVerifier,
    GoalIterationExecutor,
    RunControlLifecycleGateway,
)
from app.application.orchestration_binding_repository import SemanticInputBindingNotFound
from app.application.orchestration_routing import SemanticRoutingError
from app.domain.coordinator.launch import (
    LaunchAuthorizationError,
    TerminalWorkflowCompletion,
)
from app.domain.orchestration.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalVerificationRequest,
    GoalVerificationResult,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.temporal.registration.activities import coordinator_activities
from app.temporal.registration.workflows import coordinator_workflows
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class GoalDirectedActivities:
    """All nondeterministic GoalDirected work lives behind these activities."""

    def __init__(
        self,
        *,
        executor: GoalIterationExecutor,
        verifier: GoalIndependentVerifier,
        handoffs: GoalHandoffPreparer,
        lifecycle: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
    ) -> None:
        self._executor = executor
        self._verifier = verifier
        self._handoffs = handoffs
        self._lifecycle = lifecycle
        self._completion = completion

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    @activity.defn(name="goaldirected.execute_iteration")
    async def execute_iteration(self, claim: GoalExecutionClaim) -> GoalExecutionResult:
        try:
            result = await self._executor.execute(claim)
        except (SemanticRoutingError, SemanticInputBindingNotFound) as error:
            raise ApplicationError(
                str(error),
                type="semantic_input_binding_rejected",
                non_retryable=True,
            ) from error
        return replace(result, temporal_activity_attempt=activity.info().attempt)

    @activity.defn(name="goaldirected.prepare_handoff")
    async def prepare_handoff(self, request: GoalHandoffRequest) -> GoalHandoffResult:
        try:
            return await self._handoffs.prepare(request)
        except (SemanticRoutingError, SemanticInputBindingNotFound) as error:
            raise ApplicationError(
                str(error),
                type="semantic_input_binding_rejected",
                non_retryable=True,
            ) from error

    @activity.defn(name="goaldirected.verify_iteration")
    async def verify_iteration(
        self,
        request: GoalVerificationRequest,
    ) -> GoalVerificationResult:
        try:
            return await self._verifier.verify(request)
        except (SemanticRoutingError, SemanticInputBindingNotFound) as error:
            raise ApplicationError(
                str(error),
                type="semantic_input_binding_rejected",
                non_retryable=True,
            ) from error

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self,
        request: LifecycleCommandRequest,
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle.execute(request)

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
