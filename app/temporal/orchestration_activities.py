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
)
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
    StageGraphCycleActivityRequest,
    StageGraphCycleActivityResult,
    StageGraphInitializeRequest,
    StageGraphInitializeResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
)
from app.temporal.registration.activities import coordinator_activities
from app.temporal.registration.workflows import coordinator_workflows
from app.temporal.workflow_sandbox import coordinator_workflow_runner


class StageGraphActivities:
    """Nondeterministic StageGraph boundaries registered on a Temporal worker."""

    def __init__(
        self,
        *,
        decision_service: StageGraphDecisionService,
        operation_materializer: StageGraphOperationMaterializer,
        lifecycle_gateway: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
    ) -> None:
        self._lifecycle_gateway = lifecycle_gateway
        self._completion = completion
        self._decision_service = decision_service
        self._operation_materializer = operation_materializer

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    def _canonical_decisions(self) -> StageGraphDecisionService:
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

    @activity.defn(name="stagegraph.apply_cycle")
    async def apply_cycle(
        self, request: StageGraphCycleActivityRequest
    ) -> StageGraphCycleActivityResult:
        return await self._canonical_decisions().apply_cycle(request)

    @activity.defn(name="stagegraph.complete")
    async def complete_stagegraph(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        return await self._canonical_decisions().complete(request)

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
