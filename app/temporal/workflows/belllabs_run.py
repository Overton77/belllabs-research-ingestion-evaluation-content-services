from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from app.domain.orchestration.contracts import (
        BellLabsRunInput,
        GoalDirectedRunInput,
        RunContinuityState,
        StageGraphRunInput,
        WorkflowMessage,
        WorkflowMessageReceipt,
    )
    from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
    from app.temporal.workflows.stagegraph import StageGraphWorkflow


@workflow.defn(name="belllabs.run.v1")
class BellLabsRunWorkflow:
    """Stable root for one admitted BellLabs run across all family implementations."""

    def __init__(self) -> None:
        self._continuity = RunContinuityState()
        self._receipts: list[WorkflowMessageReceipt] = []
        self._cancel_requested = False
        self._family_handle: Any | None = None

    def _accept_message(self, message: WorkflowMessage) -> WorkflowMessageReceipt:
        duplicate = next(
            (item for item in self._receipts if item.message_id == message.message_id),
            None,
        )
        if duplicate is not None:
            return replace(duplicate, status="duplicate")
        status: Literal["accepted", "duplicate", "stale_generation", "gap"]
        if message.execution_generation != self._continuity.execution_generation:
            status = "stale_generation"
        elif message.sequence != self._continuity.last_message_sequence + 1:
            status = "gap"
        else:
            status = "accepted"
            self._continuity = replace(
                self._continuity,
                last_message_sequence=message.sequence,
            )
            if message.kind == "cancel":
                self._cancel_requested = True
        receipt = WorkflowMessageReceipt(
            message_id=message.message_id,
            sequence=message.sequence,
            status=status,
            technical_segment=self._continuity.technical_segment,
        )
        self._receipts.append(receipt)
        return receipt

    @workflow.signal
    def signal_message(self, message: WorkflowMessage) -> None:
        self._accept_message(message)

    @workflow.update
    def deliver_message(self, message: WorkflowMessage) -> WorkflowMessageReceipt:
        return self._accept_message(message)

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True
        handle = self._family_handle
        if handle is not None:
            handle.cancel()

    @workflow.query
    def continuity(self) -> RunContinuityState:
        return replace(self._continuity, message_receipts=tuple(self._receipts))

    @workflow.query
    def message_receipts(self) -> tuple[WorkflowMessageReceipt, ...]:
        return tuple(self._receipts)

    @workflow.run
    async def run(self, run_input: BellLabsRunInput) -> Any:
        self._continuity = run_input.continuity
        self._receipts = list(run_input.continuity.message_receipts)
        if run_input.force_continue_as_new:
            workflow.continue_as_new(
                replace(
                    run_input,
                    continuity=run_input.continuity.next_technical_segment(),
                    force_continue_as_new=False,
                )
            )

        family_id = run_input.family_workflow_id
        handle: Any
        if run_input.family == "StageGraph":
            stage_input = StageGraphRunInput(**run_input.family_input)
            handle = await workflow.start_child_workflow(
                StageGraphWorkflow.run,
                stage_input,
                id=family_id,
                task_queue=run_input.family_task_queue,
                parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
            )
        else:
            goal_input = GoalDirectedRunInput(**run_input.family_input)
            handle = cast(
                Any,
                await workflow.start_child_workflow(
                    GoalDirectedWorkflow.run,
                    goal_input,
                    id=family_id,
                    task_queue=run_input.family_task_queue,
                    parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                ),
            )
        self._family_handle = handle
        if self._cancel_requested:
            handle.cancel()
        return await handle
