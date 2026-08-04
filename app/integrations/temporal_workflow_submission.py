from __future__ import annotations

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.domain.coordinator.launch import (
    BlueprintFamily,
    WorkflowSubmission,
)
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    StageGraphRunInput,
)
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow
from app.temporal.stagegraph_workflow import StageGraphWorkflow


class TemporalWorkflowSubmitter:
    """Start exactly the Temporal workflow family frozen by launch preparation."""

    def __init__(
        self,
        client: Client,
        *,
        stagegraph_task_queue: str,
        goal_directed_task_queue: str,
    ) -> None:
        if not stagegraph_task_queue or not goal_directed_task_queue:
            raise ValueError("coordinator Temporal task queues must be non-empty")
        if stagegraph_task_queue == goal_directed_task_queue:
            raise ValueError(
                "StageGraph and GoalDirected require distinct task queues for readiness"
            )
        self._client = client
        self._stagegraph_task_queue = stagegraph_task_queue
        self._goal_directed_task_queue = goal_directed_task_queue

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
    ) -> WorkflowSubmission:
        if not workflow_id:
            raise ValueError("Temporal workflow identity must be non-empty")

        if blueprint_family == BlueprintFamily.STAGE_GRAPH:
            if not isinstance(workflow_input, StageGraphRunInput):
                raise ValueError(
                    "StageGraph submission requires an immutable StageGraphRunInput"
                )
            try:
                stage_handle = await self._client.start_workflow(
                    StageGraphWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=self._stagegraph_task_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                )
            except WorkflowAlreadyStartedError as error:
                return WorkflowSubmission(
                    workflow_id=workflow_id,
                    temporal_run_id=error.run_id or None,
                )
            return WorkflowSubmission(
                workflow_id=stage_handle.id,
                temporal_run_id=stage_handle.first_execution_run_id or None,
            )
        elif blueprint_family == BlueprintFamily.GOAL_DIRECTED:
            if not isinstance(workflow_input, GoalDirectedRunInput):
                raise ValueError(
                    "GoalDirected submission requires an immutable GoalDirectedRunInput"
                )
            try:
                goal_handle = await self._client.start_workflow(
                    GoalDirectedWorkflow.run,
                    workflow_input,
                    id=workflow_id,
                    task_queue=self._goal_directed_task_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                )
            except WorkflowAlreadyStartedError as error:
                return WorkflowSubmission(
                    workflow_id=workflow_id,
                    temporal_run_id=error.run_id or None,
                )
            return WorkflowSubmission(
                workflow_id=goal_handle.id,
                temporal_run_id=goal_handle.first_execution_run_id or None,
            )
        else:
            raise ValueError(f"unsupported Temporal blueprint family: {blueprint_family}")
