from __future__ import annotations

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from mission_control.adapters.temporal.boundary_activities import apply_boundary_fact
from mission_control.adapters.temporal.registration.activities import coordinator_activities
from mission_control.adapters.temporal.registration.workflows import coordinator_workflows
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.application.coordinator.coordinator_results import (
    TerminalWorkflowCompletionPort,
)
from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandApplicationService,
)
from mission_control.application.execution.operations.semantic_operation_bindings import (
    SemanticOperationBindingRepository,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.programs.goal_directed import (
    GoalAdmissionStale,
    GoalDirectedDocumentRepository,
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    GoalOperationTemplateProvider,
    RunControlGoalOperationSettlements,
)
from mission_control.application.programs.service import RunControlLifecycleGateway
from mission_control.domain.coordinator.launch import (
    LaunchAuthorizationError,
    TerminalWorkflowCompletion,
)
from mission_control.domain.execution.heartbeats import (
    DEFAULT_OPERATION_HEARTBEATS,
    OperationHeartbeatPolicy,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.contracts import (
    BoundaryLifecycleOutcome,
    BoundaryLifecycleRequest,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from mission_control.domain.programs.goal_directed_runtime import (
    GOAL_ADMISSION_STALE,
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
        boundary: BoundaryCommandApplicationService | None = None,
    ) -> None:
        self._operations = operations
        self._results = results
        self._lifecycle = lifecycle
        self._completion = completion
        self._boundary = boundary

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
        return await self._prepare(request)

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
        return await self._prepare(request)

    async def _prepare(self, request: GoalOperationPreparationRequest) -> GoalOperationDispatch:
        try:
            return await self._operations.prepare(request)
        except GoalAdmissionStale as error:
            # RRM-016 review fix 2: reported to the family, which re-admits once at the
            # current version or enters cancellation; never retried by Temporal as-is.
            raise ApplicationError(
                str(error),
                error.current_run_version,
                error.phase,
                type=GOAL_ADMISSION_STALE,
                non_retryable=True,
            ) from error

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

    @activity.defn(name="goaldirected.apply_boundary_command")
    async def apply_boundary_command(
        self, request: BoundaryLifecycleRequest
    ) -> BoundaryLifecycleOutcome:
        """RRM-007: the family boundary's run-control facts, bound to the current version."""

        if self._boundary is None:
            raise ApplicationError(
                "boundary command application is not composed for GoalDirected",
                type="boundary_application_unavailable",
                non_retryable=True,
            )
        return await apply_boundary_fact(self._boundary, request)

    @activity.defn(name="coordinator.materialize_workflow_result")
    async def materialize_workflow_result(self, completion: TerminalWorkflowCompletion) -> object:
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
    boundary: BoundaryCommandApplicationService | None = None,
    heartbeats: OperationHeartbeatPolicy = DEFAULT_OPERATION_HEARTBEATS,
) -> GoalDirectedActivities:
    """Wire production GoalDirected activities on the OperationWorkflow path."""

    return GoalDirectedActivities(
        operations=GoalDirectedOperationPreparationService(
            templates=templates,
            operation_bindings=operation_bindings,
            run_control=run_control,
            documents=documents,
            actor=actor,
            heartbeats=heartbeats,
        ),
        # RRM-016: the family consumes each operation's journaled run-control settlement.
        results=GoalDirectedOperationResultService(
            documents, RunControlGoalOperationSettlements(run_control, operation_bindings)
        ),
        lifecycle=lifecycle,
        completion=completion,
        boundary=boundary,
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
