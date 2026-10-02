from __future__ import annotations

import asyncio
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
# RRM-008 (REQ-CP-EXEC-008): the cognitive Activity declares a heartbeat timeout, a cancel
# reaches it through the heartbeat (TRY_CANCEL), and every reconciliation after a cancel runs
# `operation.cancel`, which never dispatches or resumes cognition. Histories recorded before
# the patch replay the exact earlier command sequence.
CANCELLATION_SAGA_PATCH = "rrm-008-operation-cancellation-saga"
NUDGE_SNAPSHOT_PATCH = "rrm-008-nudge-snapshot-before-activity"
TERMINAL_DISPOSITIONS = frozenset({"completed", "cancelled", "failed", "in_doubt"})


def _parks(result: dict[str, object]) -> bool:
    return (
        result.get("status") == "in_doubt" and result.get("failure_code") != "generation_superseded"
    )


class _CancellationRequested(Exception):
    """The unit's cancellation was requested while its attempt was in flight."""


def _uncancel() -> None:
    # The workflow's own task was cancelled once (a Temporal cancel). Clearing the counter
    # lets the saga keep awaiting its reconciliation activities (as the SDK does itself).
    task = asyncio.current_task()
    if task is not None:
        task.uncancel()


@workflow.defn(name="belllabs.operation.v2")
class OperationWorkflow:
    """Durable technical wrapper for one stable semantic operation attempt.

    Cancellation (RRM-008, REQ-CP-EXEC-008): a cancel reaches the workflow as a Temporal
    cancellation from its family or as the `request_cancel` signal. An in-flight
    `operation.execute` Activity is cancelled (the cancel reaches cognition through its
    heartbeat and the holder settles the unit `cancelled` with its latest durable
    checkpoint); then `operation.cancel` reconciles the unit from durable facts, standing
    down behind a live holder and taking over a lost one, so cancellation survives worker
    loss. A unit parked `in_doubt` is reached by the cancel too: it keeps its incident and
    is settled only by the operator's `reconcile_unit` decision, never speculatively. The
    workflow then completes normally with the unit's disposition; it never fails in place
    of reconciliation.
    """

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

    @workflow.query
    def cancellation_requested(self) -> bool:
        """Diagnostic only (REQ-CP-EXEC-007); the settlement is the authority."""

        return self._cancel_requested

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
        if workflow.patched(CANCELLATION_SAGA_PATCH):
            result = await self._run_governed(request)
        else:
            legacy = await self._run_legacy(request)
            if legacy is None:
                return self._result(request, "cancelled", None)
            result = legacy
        status = result.get("status", "completed")
        disposition = status if status in TERMINAL_DISPOSITIONS else "failed"
        return self._result(request, str(disposition), result)

    def _result(
        self, request: OperationWorkflowRequest, disposition: str, result: dict[str, object] | None
    ) -> OperationWorkflowResult:
        return OperationWorkflowResult(
            semantic_attempt_id=request.semantic_attempt_id,
            execution_generation=request.execution_generation,
            disposition=disposition,
            result=result or {},
            message_cursor=request.message_cursor,
            effect_frontier=request.effect_frontier,
            active_async_child_ids=self._active_async_child_ids,
        )

    # --- RRM-008 governed path ----------------------------------------------------------------

    async def _run_governed(self, request: OperationWorkflowRequest) -> dict[str, object]:
        cancel_mode = self._cancel_requested
        while True:
            # Review F2: snapshot the hint counter before the Activity, as the legacy path
            # does, so a `reconcile_unit` hint that lands while it runs is not lost.
            snapshot_first = workflow.patched(NUDGE_SNAPSHOT_PATCH)
            if snapshot_first:
                self._nudges_seen = self._reconciliation_nudges
            try:
                if cancel_mode:
                    result = await self._cancel_operation(request)
                else:
                    result = await self._execute_cancellable(request)
            except _CancellationRequested:
                cancel_mode = True
                continue
            if not _parks(result):
                return result
            # REQ-CP-RUN-007 / REQ-CP-DA-018: an `in_doubt` unit keeps its claim unsettled
            # and waits durably for operator reconciliation. A cancel reaches it here; it
            # is never re-executed speculatively, and after a cancel every wake-up runs the
            # cancellation settlement, which applies only an accepted decision.
            if not snapshot_first:
                self._nudges_seen = self._reconciliation_nudges

            def woken(cancelling: bool = cancel_mode) -> bool:
                return self._reconciliation_nudges > self._nudges_seen or (
                    self._cancel_requested and not cancelling
                )

            try:
                await workflow.wait_condition(woken)
            except asyncio.CancelledError:
                self._cancel_requested = True
                _uncancel()
            if self._cancel_requested:
                cancel_mode = True

    async def _execute_cancellable(self, request: OperationWorkflowRequest) -> dict[str, object]:
        """One `operation.execute` attempt that a cancel can interrupt in flight."""

        handle = workflow.start_activity(
            "operation.execute",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            # Timeout meanings: start-to-close is the holder's maximum (and its claim lease
            # deadline); heartbeat detects a lost worker and carries the cancel; retries
            # recover the unit (REQ-CP-DA-018) as long as no cancel was requested.
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            heartbeat_timeout=timedelta(seconds=request.heartbeat_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
            cancellation_type=workflow.ActivityCancellationType.TRY_CANCEL,
        )
        try:
            await workflow.wait_condition(lambda: handle.done() or self._cancel_requested)
        except asyncio.CancelledError:
            self._cancel_requested = True
            _uncancel()
        if self._cancel_requested and not handle.done():
            # The cancel reaches the Activity through its heartbeat; the holder settles the
            # unit `cancelled` itself when it can. Whatever the attempt ends with,
            # `operation.cancel` reconciles the unit from durable facts.
            handle.cancel()
            try:
                await handle
            except (Exception, asyncio.CancelledError):
                _uncancel()
            raise _CancellationRequested
        return handle.result()

    async def _cancel_operation(self, request: OperationWorkflowRequest) -> dict[str, object]:
        """`operation.cancel`: retried until a live holder's lease is released or expires."""

        handle = workflow.start_activity(
            "operation.cancel",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            schedule_to_close_timeout=timedelta(
                seconds=2 * request.timeout_seconds + request.heartbeat_timeout_seconds
            ),
            heartbeat_timeout=timedelta(seconds=request.heartbeat_timeout_seconds),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=15),
                backoff_coefficient=2.0,
                maximum_attempts=0,
            ),
        )
        while True:
            try:
                return await asyncio.shield(handle)
            except asyncio.CancelledError:
                # A repeated cancel of this workflow must not cancel the reconciliation.
                self._cancel_requested = True
                _uncancel()
                if handle.done():
                    return handle.result()

    # --- pre-RRM-008 path (replay of recorded histories only) ---------------------------------

    async def _run_legacy(self, request: OperationWorkflowRequest) -> dict[str, object] | None:
        if self._cancel_requested:
            return None
        self._nudges_seen = self._reconciliation_nudges
        result = await self._execute_operation(request)
        if _parks(result) and workflow.patched(PARK_IN_DOUBT_PATCH):
            while _parks(result):
                await workflow.wait_condition(
                    lambda: self._reconciliation_nudges > self._nudges_seen
                )
                self._nudges_seen = self._reconciliation_nudges
                result = await self._execute_operation(request)
        return result

    async def _execute_operation(self, request: OperationWorkflowRequest) -> dict[str, object]:
        result: dict[str, object] = await workflow.execute_activity(
            "operation.execute",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return result


__all__: tuple[str, ...] = ("CANCELLATION_SAGA_PATCH", "OperationWorkflow")
