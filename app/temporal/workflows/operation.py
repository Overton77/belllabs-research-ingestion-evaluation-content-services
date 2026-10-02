from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from app.domain.operation_execution.contracts import (
        MAX_ACTIVE_ASYNC_CHILDREN,
        MAX_ASYNC_CHILD_ID_LENGTH,
        OperationWorkflowRequest,
        OperationWorkflowResult,
    )
    from app.temporal.search_attributes import (
        ensure_workflow_search_attributes,
        operation_workflow_search_attributes,
    )

PARK_IN_DOUBT_PATCH = "rrm-004-park-in-doubt-units"


def _parks(result: dict[str, object]) -> bool:
    return (
        result.get("status") == "in_doubt"
        and result.get("failure_code") != "generation_superseded"
    )


@workflow.defn(name="belllabs.operation.v2")
class OperationWorkflow:
    """Durable technical wrapper for one stable semantic operation attempt."""

    def __init__(self) -> None:
        self._cancel_requested = False
        self._execution_generation = 1
        self._active_async_child_ids: tuple[str, ...] = ()
        self._reconciliation_nudges = 0
        self._nudges_seen = 0

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.signal
    def unit_reconciliation_recorded(self, decision_ref: str) -> None:
        """A compact wake-up hint after run control accepted `reconcile_unit`.

        It releases nothing by itself (REQ-CP-EXEC-007): the next `operation.execute`
        attempt reads the accepted decision from PostgreSQL authority, and a hint without
        an accepted decision simply parks the unit again.
        """

        del decision_ref
        self._reconciliation_nudges += 1

    @workflow.query
    def execution_generation(self) -> int:
        return self._execution_generation

    @workflow.signal
    def record_async_child(self, child_execution_id: str) -> None:
        if not child_execution_id or len(child_execution_id) > MAX_ASYNC_CHILD_ID_LENGTH:
            raise ApplicationError(
                "async child identity is outside the exact operation workflow bound",
                type="invalid_async_child_identity",
                non_retryable=True,
            )
        if child_execution_id in self._active_async_child_ids:
            return
        if len(self._active_async_child_ids) >= MAX_ACTIVE_ASYNC_CHILDREN:
            raise ApplicationError(
                "operation workflow active async child ceiling exceeded",
                type="active_async_child_ceiling_exceeded",
                non_retryable=True,
            )
        self._active_async_child_ids = (*self._active_async_child_ids, child_execution_id)

    @workflow.query
    def active_async_children(self) -> tuple[str, ...]:
        return self._active_async_child_ids

    @workflow.run
    async def run(self, request: OperationWorkflowRequest) -> OperationWorkflowResult:
        self._execution_generation = request.execution_generation
        # REQ-CP-EXEC-015: a `required` operation carries its run, unit and generation
        # attributes (the family normally starts it with them; nothing is upserted then).
        ensure_workflow_search_attributes(
            request.search_attribute_policy,
            operation_workflow_search_attributes(request),
        )
        pre_start_signal_ids = self._active_async_child_ids
        merged_ids = list(request.active_async_child_ids)
        seen_ids = set(merged_ids)
        for child_execution_id in pre_start_signal_ids:
            if child_execution_id in seen_ids:
                continue
            if len(merged_ids) >= MAX_ACTIVE_ASYNC_CHILDREN:
                raise ApplicationError(
                    "operation workflow active async child ceiling exceeded during initialization",
                    type="active_async_child_ceiling_exceeded",
                    non_retryable=True,
                )
            merged_ids.append(child_execution_id)
            seen_ids.add(child_execution_id)
        self._active_async_child_ids = tuple(merged_ids)
        if self._cancel_requested:
            return OperationWorkflowResult(
                semantic_attempt_id=request.semantic_attempt_id,
                execution_generation=request.execution_generation,
                disposition="cancelled",
                message_cursor=request.message_cursor,
                effect_frontier=request.effect_frontier,
                active_async_child_ids=self._active_async_child_ids,
            )
        self._nudges_seen = self._reconciliation_nudges
        result = await self._execute_operation(request)
        if _parks(result) and workflow.patched(PARK_IN_DOUBT_PATCH):
            # REQ-CP-RUN-007 / REQ-CP-DA-018: an `in_doubt` unit keeps its claim unsettled
            # and waits durably for operator reconciliation; it is never re-executed
            # speculatively. Each wake-up re-runs classification, which applies only an
            # accepted decision.
            while _parks(result):
                await workflow.wait_condition(
                    lambda: self._reconciliation_nudges > self._nudges_seen
                )
                self._nudges_seen = self._reconciliation_nudges
                result = await self._execute_operation(request)
        status = result.get("status", "completed")
        disposition = (
            status
            if status in {"completed", "cancelled", "failed", "in_doubt"}
            else "failed"
        )
        return OperationWorkflowResult(
            semantic_attempt_id=request.semantic_attempt_id,
            execution_generation=request.execution_generation,
            disposition=disposition,
            result=result,
            message_cursor=request.message_cursor,
            effect_frontier=request.effect_frontier,
            active_async_child_ids=self._active_async_child_ids,
        )

    async def _execute_operation(self, request: OperationWorkflowRequest) -> dict[str, object]:
        return await workflow.execute_activity(
            "operation.execute",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
