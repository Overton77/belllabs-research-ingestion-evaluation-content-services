from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal, cast

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from app.domain.orchestration.contracts import (
        BellLabsRunInput,
        CancelAck,
        CancelDelivery,
        GoalDirectedRunInput,
        RunContinuityState,
        StageGraphRunInput,
        WorkflowMessage,
        WorkflowMessageReceipt,
    )
    from app.domain.orchestration.search_attributes import (
        BellLabsSearchAttributeValues,
        run_search_attributes,
    )
    from app.temporal.search_attributes import (
        child_search_attributes,
        ensure_workflow_search_attributes,
    )
    from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
    from app.temporal.workflows.stagegraph import StageGraphWorkflow


# RRM-008 (REQ-CP-EXEC-008 steps 1-2, RRM-001 section 7 #7): a cancel reaches the root only
# as the delivery of a journaled, accepted command (`deliver_cancel`); the raw
# `request_cancel` signal is a hint that cancels nothing. Histories recorded before the
# patch keep the earlier behaviour of the signal.
GOVERNED_CANCEL_PATCH = "rrm-008-governed-root-cancel"


@workflow.defn(name="belllabs.run.v1")
class BellLabsRunWorkflow:
    """Stable root for one admitted BellLabs run across all family implementations."""

    def __init__(self) -> None:
        self._continuity = RunContinuityState()
        self._receipts: list[WorkflowMessageReceipt] = []
        self._cancel_requested = False
        self._family_handle: Any | None = None
        self._cancel_acks: dict[str, CancelAck] = {}

    def _accept_message(self, message: WorkflowMessage) -> WorkflowMessageReceipt:
        duplicate = next(
            (item for item in self._receipts if item.message_id == message.message_id),
            None,
        )
        if duplicate is not None:
            return replace(duplicate, status="duplicate", cached_status=duplicate.status)
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
            cached_status=status,
        )
        if status != "gap":
            # A gap is transient (the earlier message may still arrive): it is not cached,
            # so a redelivery is decided again (F7).
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
        if workflow.patched(GOVERNED_CANCEL_PATCH):
            # REQ-CP-EXEC-007: a raw signal is not a governed command path; it is recorded
            # as a hint only. Cancellation enters through run control and `deliver_cancel`.
            return
        self._cancel_requested = True
        handle = self._family_handle
        if handle is not None:
            handle.cancel()

    @workflow.update
    def deliver_cancel(self, delivery: CancelDelivery) -> CancelAck:
        """Record the journaled cancellation intent at the root (sequenced in the `cancel`
        space, never in the `execution` message sequence). Evidence of delivery to the root
        only; the family acknowledges its own delivery and runs the saga."""

        prior = self._cancel_acks.get(delivery.command_id)
        if prior is not None:
            return replace(prior, status="duplicate")
        status: Literal["delivered", "stale_generation"]
        if delivery.execution_generation != self._continuity.execution_generation:
            status = "stale_generation"
        else:
            status = "delivered"
            self._cancel_requested = True
        ack = CancelAck(
            command_id=delivery.command_id,
            status=status,
            technical_segment=self._continuity.technical_segment,
        )
        self._cancel_acks[delivery.command_id] = ack
        return ack

    @workflow.query
    def cancel_receipts(self) -> tuple[CancelAck, ...]:
        """Diagnostic only (REQ-CP-EXEC-007); the receipt ledger is the authority."""

        return tuple(self._cancel_acks.values())

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

        # REQ-CP-EXEC-015: under `required` the root carries its attributes (upserting
        # only those it was not started with) and starts the family with its own.
        policy = run_input.search_attribute_policy
        ensure_workflow_search_attributes(policy, self._attributes(run_input, "root"))
        family_attributes = child_search_attributes(
            policy, self._attributes(run_input, "family")
        )
        family_input = (
            {**run_input.family_input, "search_attribute_policy": policy}
            if family_attributes is not None
            else run_input.family_input
        )
        family_id = run_input.family_workflow_id
        handle: Any
        if run_input.family == "StageGraph":
            stage_input = StageGraphRunInput(**family_input)
            handle = await workflow.start_child_workflow(
                StageGraphWorkflow.run,
                stage_input,
                id=family_id,
                task_queue=run_input.family_task_queue,
                parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                search_attributes=family_attributes,
            )
        else:
            goal_input = GoalDirectedRunInput(**family_input)
            handle = cast(
                Any,
                await workflow.start_child_workflow(
                    GoalDirectedWorkflow.run,
                    goal_input,
                    id=family_id,
                    task_queue=run_input.family_task_queue,
                    parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                    search_attributes=family_attributes,
                ),
            )
        self._family_handle = handle
        if self._cancel_requested and not workflow.patched(GOVERNED_CANCEL_PATCH):
            handle.cancel()
        return await handle

    @staticmethod
    def _attributes(
        run_input: BellLabsRunInput, kind: Literal["root", "family"]
    ) -> BellLabsSearchAttributeValues:
        return run_search_attributes(
            workflow_kind=kind,
            run_id=run_input.run_id,
            request_scope=run_input.request_scope,
            family=run_input.family,
            execution_epoch=run_input.continuity.execution_epoch,
            # `BellLabsParentRunId` is set on fork and linked roots only (EXEC-015).
            parent_run_id=run_input.parent_run_id if kind == "root" else None,
        )
