from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.domain.operation_execution.contracts import (
        OperationWorkflowRequest,
        OperationWorkflowResult,
    )

_ACTIVITY_NAMES = {
    "stage_operation": "stagegraph.execute_operation",
    "goal_iteration": "goaldirected.execute_iteration",
    "goal_verification": "goaldirected.verify_iteration",
    "bound_operation": "operation.execute",
}


@workflow.defn(name="belllabs.operation.v1")
class OperationWorkflow:
    """Durable technical wrapper for one stable semantic operation attempt."""

    def __init__(self) -> None:
        self._cancel_requested = False
        self._execution_generation = 1
        self._active_async_child_ids: tuple[str, ...] = ()

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.query
    def execution_generation(self) -> int:
        return self._execution_generation

    @workflow.signal
    def record_async_child(self, child_execution_id: str) -> None:
        if child_execution_id not in self._active_async_child_ids:
            self._active_async_child_ids = (*self._active_async_child_ids, child_execution_id)

    @workflow.query
    def active_async_children(self) -> tuple[str, ...]:
        return self._active_async_child_ids

    @workflow.run
    async def run(self, request: OperationWorkflowRequest) -> OperationWorkflowResult:
        self._execution_generation = request.execution_generation
        self._active_async_child_ids = request.active_async_child_ids
        if self._cancel_requested:
            return OperationWorkflowResult(
                semantic_attempt_id=request.semantic_attempt_id,
                execution_generation=request.execution_generation,
                disposition="cancelled",
                message_cursor=request.message_cursor,
                effect_frontier=request.effect_frontier,
                active_async_child_ids=self._active_async_child_ids,
            )
        result = await workflow.execute_activity(
            _ACTIVITY_NAMES[request.operation_kind],
            request.payload,
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return OperationWorkflowResult(
            semantic_attempt_id=request.semantic_attempt_id,
            execution_generation=request.execution_generation,
            disposition="completed",
            result=result,
            message_cursor=request.message_cursor,
            effect_frontier=request.effect_frontier,
            active_async_child_ids=self._active_async_child_ids,
        )
