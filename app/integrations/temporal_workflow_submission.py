from __future__ import annotations

from dataclasses import asdict, replace

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.domain.coordinator.launch import BlueprintFamily, WorkflowSubmission
from app.domain.orchestration.contracts import (
    BellLabsRunInput,
    GoalDirectedRunInput,
    StageGraphRunInput,
)
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow


class TemporalWorkflowSubmitter:
    """Root-only Temporal submission adapter with stable duplicate-start identity."""

    def __init__(
        self,
        client: Client,
        *,
        stagegraph_task_queue: str,
        goal_directed_task_queue: str,
        root_task_queue: str | None = None,
    ) -> None:
        if not stagegraph_task_queue or not goal_directed_task_queue:
            raise ValueError("coordinator Temporal task queues must be non-empty")
        if stagegraph_task_queue == goal_directed_task_queue:
            raise ValueError("StageGraph and GoalDirected require distinct task queues")
        self._client = client
        self._stagegraph_task_queue = stagegraph_task_queue
        self._goal_directed_task_queue = goal_directed_task_queue
        self._root_task_queue = root_task_queue

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
    ) -> WorkflowSubmission:
        del workflow_id  # Callers cannot override the admitted BellLabs root identity.
        if blueprint_family == BlueprintFamily.STAGE_GRAPH:
            if not isinstance(workflow_input, StageGraphRunInput):
                raise ValueError("StageGraph submission requires an immutable StageGraphRunInput")
            family_queue = self._stagegraph_task_queue
        elif blueprint_family == BlueprintFamily.GOAL_DIRECTED:
            if not isinstance(workflow_input, GoalDirectedRunInput):
                raise ValueError(
                    "GoalDirected submission requires an immutable GoalDirectedRunInput"
                )
            family_queue = self._goal_directed_task_queue
        else:
            raise ValueError(f"unsupported Temporal blueprint family: {blueprint_family}")

        root_input = BellLabsRunInput(
            schema_version="belllabs.temporal-root.v1",
            run_id=workflow_input.run_id,
            request_scope=workflow_input.request_scope,
            effective_configuration_digest=workflow_input.effective_configuration_digest,
            workflow_type_digest=workflow_input.blueprint_digest,
            family=blueprint_family.value,
            family_input=asdict(replace(workflow_input, durable_operation_children=True)),
            family_task_queue=family_queue,
        )
        root_queue = self._root_task_queue or family_queue
        try:
            handle = await self._client.start_workflow(
                BellLabsRunWorkflow.run,
                root_input,
                id=root_input.workflow_id,
                task_queue=root_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
        except WorkflowAlreadyStartedError as error:
            return WorkflowSubmission(
                workflow_id=root_input.workflow_id,
                temporal_run_id=error.run_id or None,
            )
        return WorkflowSubmission(
            workflow_id=handle.id,
            temporal_run_id=handle.first_execution_run_id or None,
        )
