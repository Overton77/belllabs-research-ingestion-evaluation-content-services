from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import CancelledError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from app.domain.composition.contracts import (
        LinkedChildResolution,
        LinkedRunContinuationState,
        LinkedRunExecutionBinding,
        RunCompositionLink,
        RunDependencyClass,
    )
    from app.domain.orchestration.contracts import StageGraphRunInput, StageGraphRunResult
    from app.temporal.stagegraph_workflow import StageGraphWorkflow


@workflow.defn(name="belllabs.linked-run")
class LinkedRunWorkflow:
    """Starts an observer that may outlive the parent-facing linked workflow."""

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_binding = await workflow.execute_activity(
            "linked_run.resolve_execution_binding",
            {
                "request_scope": payload["request_scope"],
                "link_id": payload["link_id"],
                "dependency_revision_id": payload.get("dependency_revision_id"),
            },
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        execution_binding = LinkedRunExecutionBinding.model_validate(raw_binding)
        link = execution_binding.link
        effective_dependency = execution_binding.effective_dependency_class
        current_epoch = int(payload.get("current_execution_epoch", 1))
        continuation_payload = payload.get("continuation_state")
        continuation = (
            LinkedRunContinuationState.model_validate(continuation_payload)
            if continuation_payload is not None
            else None
        )
        if continuation is not None:
            if link.link_id not in continuation.link_ids:
                raise ValueError("Continue-As-New state omitted the active composition link")
            if payload.get("force_continue_as_new", False):
                if continuation.next_execution_epoch != current_epoch + 1:
                    raise ValueError("continuation epoch does not follow the active execution")
                workflow.continue_as_new(
                    {
                        **payload,
                        "current_execution_epoch": continuation.next_execution_epoch,
                        "force_continue_as_new": False,
                    }
                )
        timeout = timedelta(seconds=int(payload.get("execution_timeout_seconds", 3600)))
        child_task_queue = str(payload.get("child_task_queue", "")).strip()
        if not child_task_queue:
            raise ValueError("linked-run execution requires an explicit child_task_queue")
        blocking = effective_dependency in {
            RunDependencyClass.REQUIRED_BLOCKING,
            RunDependencyClass.DEGRADABLE_BLOCKING,
        }
        observer_id = f"linked-observer:{link.link_id}"
        handle = await workflow.start_child_workflow(
            LinkedRunObserverWorkflow.run,
            {
                "execution_binding": execution_binding.model_dump(mode="json"),
                "child_input": payload["child_input"],
                "execution_timeout_seconds": int(timeout.total_seconds()),
                "child_task_queue": child_task_queue,
                "current_execution_epoch": current_epoch,
                "continuation_state": (
                    continuation.model_dump(mode="json")
                    if continuation is not None
                    else None
                ),
            },
            id=observer_id,
            task_queue=workflow.info().task_queue,
            execution_timeout=timeout,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )
        if not blocking:
            if link.cancellation_policy == "request_cancel":
                await handle.signal(LinkedRunObserverWorkflow.request_cancel)
            return {
                "link_id": link.link_id,
                "child_run_id": link.child_run_id,
                "observer_workflow_id": observer_id,
                "disposition": "launched_nonblocking",
                "execution_epoch": current_epoch,
                "continuation_state": (
                    continuation.model_dump(mode="json")
                    if continuation is not None
                    else None
                ),
            }
        try:
            return await handle
        except asyncio.CancelledError:
            if link.cancellation_policy == "request_cancel":
                await handle.signal(LinkedRunObserverWorkflow.request_cancel)
            raise


@workflow.defn(name="belllabs.linked-run-observer")
class LinkedRunObserverWorkflow:
    """Durably observes a linked child and submits every terminal fact."""

    def __init__(self) -> None:
        self._child_handle: Any | None = None
        self._cancel_requested = False

    @workflow.signal
    async def request_cancel(self) -> None:
        self._cancel_requested = True
        if self._child_handle is not None:
            self._child_handle.cancel()

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        execution_binding = LinkedRunExecutionBinding.model_validate(
            payload["execution_binding"]
        )
        link = execution_binding.link
        current_epoch = int(payload.get("current_execution_epoch", 1))
        continuation_payload = payload.get("continuation_state")
        continuation = (
            LinkedRunContinuationState.model_validate(continuation_payload)
            if continuation_payload is not None
            else None
        )
        child_input = StageGraphRunInput(**payload["child_input"])
        timeout = timedelta(seconds=int(payload.get("execution_timeout_seconds", 3600)))
        handle = await workflow.start_child_workflow(
            StageGraphWorkflow.run,
            child_input,
            id=f"linked-child:{link.child_run_id}",
            task_queue=str(payload["child_task_queue"]),
            execution_timeout=timeout,
            parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
        )
        self._child_handle = handle
        if self._cancel_requested:
            handle.cancel()
        try:
            result: StageGraphRunResult = await handle
        except (asyncio.CancelledError, CancelledError) as error:
            resolution = await self._resolve_observation(
                {
                    "link": link.model_dump(mode="json"),
                    "status": "cancelled",
                    "exact_output_refs": [],
                    "failure_ref": f"temporal-child:{type(error).__name__}",
                    "observed_at": workflow.now().isoformat(),
                }
            )
            return self._result_payload(
                link,
                resolution,
                current_epoch=current_epoch,
                continuation=continuation,
            )
        except ChildWorkflowError as error:
            cause_names: list[str] = []
            cause: BaseException | None = error
            while cause is not None:
                cause_names.append(type(cause).__name__)
                nested = getattr(cause, "cause", None)
                cause = nested if isinstance(nested, BaseException) else cause.__cause__
            failure_name = ":".join(cause_names)
            if any("Cancel" in name for name in cause_names):
                status = "cancelled"
            elif any("Timeout" in name for name in cause_names):
                status = "timed_out"
            else:
                status = "failed"
            resolution = await self._resolve_observation(
                {
                    "link": link.model_dump(mode="json"),
                    "status": status,
                    "exact_output_refs": [],
                    "failure_ref": f"temporal-child:{failure_name}",
                    "observed_at": workflow.now().isoformat(),
                }
            )
            return self._result_payload(
                link,
                resolution,
                current_epoch=current_epoch,
                continuation=continuation,
            )
        exact_output_refs = tuple(
            output_ref
            for stage_outputs in result.output_refs.values()
            for output_ref in stage_outputs
        )
        resolution = await self._resolve_observation(
            {
                "link": link.model_dump(mode="json"),
                "status": "completed",
                "exact_output_refs": exact_output_refs,
                "observed_at": workflow.now().isoformat(),
            }
        )
        return self._result_payload(
            link,
            resolution,
            current_epoch=current_epoch,
            continuation=continuation,
        )

    @staticmethod
    def _result_payload(
        link: RunCompositionLink,
        resolution: LinkedChildResolution,
        *,
        current_epoch: int,
        continuation: LinkedRunContinuationState | None,
    ) -> dict[str, Any]:
        return {
            "link_id": link.link_id,
            "child_run_id": link.child_run_id,
            "child_status": resolution.child_status,
            "failure_ref": resolution.failure_ref,
            "disposition": resolution.disposition,
            "decision_ids": list(resolution.decision_ids),
            "admitted_output_refs": list(resolution.admitted_output_refs),
            "execution_epoch": current_epoch,
            "continuation_state": (
                continuation.model_dump(mode="json") if continuation is not None else None
            ),
        }

    async def _resolve_observation(self, payload: dict[str, Any]) -> LinkedChildResolution:
        raw = await workflow.execute_activity(
            "linked_run.resolve_child_observation",
            payload,
            result_type=dict,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        return LinkedChildResolution.model_validate(raw)
