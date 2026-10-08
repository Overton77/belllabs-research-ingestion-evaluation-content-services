from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker, WorkerDeploymentConfig

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
from mission_control.application.programs.service import (
    RunControlLifecycleGateway,
    StageGraphDecisionService,
    StageGraphOperationMaterializer,
)
from mission_control.domain.coordinator.launch import (
    LaunchAuthorizationError,
    TerminalWorkflowCompletion,
)
from mission_control.domain.programs.contracts import (
    BoundaryLifecycleOutcome,
    BoundaryLifecycleRequest,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
    StageGraphBaselineSettlementRequest,
    StageGraphBaselineSettlementResult,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
    StageGraphCycleActivityRequest,
    StageGraphCycleActivityResult,
    StageGraphInitializeRequest,
    StageGraphInitializeResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
)


class StageGraphActivities:
    """Nondeterministic StageGraph boundaries registered on a Temporal worker."""

    def __init__(
        self,
        *,
        decision_service: StageGraphDecisionService,
        operation_materializer: StageGraphOperationMaterializer,
        lifecycle_gateway: RunControlLifecycleGateway,
        completion: TerminalWorkflowCompletionPort | None = None,
        boundary: BoundaryCommandApplicationService | None = None,
    ) -> None:
        self._lifecycle_gateway = lifecycle_gateway
        self._completion = completion
        self._decision_service = decision_service
        self._operation_materializer = operation_materializer
        self._boundary = boundary

    @property
    def completion_configured(self) -> bool:
        return self._completion is not None

    def _canonical_decisions(self) -> StageGraphDecisionService:
        return self._decision_service

    @activity.defn(name="stagegraph.initialize")
    async def initialize(self, request: StageGraphInitializeRequest) -> StageGraphInitializeResult:
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

    @activity.defn(name="stagegraph.settle_baseline")
    async def settle_baseline(
        self, request: StageGraphBaselineSettlementRequest
    ) -> StageGraphBaselineSettlementResult:
        """RRM-021: release the admitted baseline reservation before terminalization."""

        return await self._canonical_decisions().settle_baseline(request)

    @activity.defn(name="stagegraph.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle_gateway.execute(request)

    @activity.defn(name="stagegraph.apply_boundary_command")
    async def apply_boundary_command(
        self, request: BoundaryLifecycleRequest
    ) -> BoundaryLifecycleOutcome:
        """RRM-007: the family boundary's run-control facts, bound to the current version."""

        if self._boundary is None:
            raise ApplicationError(
                "boundary command application is not composed for StageGraph",
                type="boundary_application_unavailable",
                non_retryable=True,
            )
        return await apply_boundary_fact(self._boundary, request)

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
    deployment_config: WorkerDeploymentConfig | None = None,
) -> Worker:
    """Compose the F3 worker after F4 supplies concrete operation/evaluator ports."""

    return Worker(
        client,
        task_queue=task_queue,
        workflows=coordinator_workflows("StageGraph"),
        workflow_runner=coordinator_workflow_runner(),
        activities=coordinator_activities("StageGraph", activities),
        deployment_config=deployment_config,
    )
