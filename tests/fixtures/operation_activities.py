"""Shared fixture pieces for fake `operation.execute` / `operation.cancel` worker sets.

Since RRM-008 every `OperationWorkflow` reconciles a cancelled unit through
`operation.cancel`, and a cancel reaches a running `operation.execute` only through its
heartbeat (`TRY_CANCEL`). A fake worker set therefore registers both Activities, and a fake
`execute` that waits heartbeats while it waits, so a cancel reaches it under the
time-skipping server instead of after a heartbeat or schedule-to-close timeout.
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import activity

from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)

HEARTBEAT_POLL_SECONDS = 0.25
CANCEL_REASON = "operation cancelled by the governed cancellation saga"


async def wait_heartbeating(event: asyncio.Event) -> None:
    """Wait for `event`, heartbeating so a requested cancel is delivered promptly."""

    while not event.is_set():
        activity.heartbeat()
        try:
            await asyncio.wait_for(event.wait(), timeout=HEARTBEAT_POLL_SECONDS)
        except TimeoutError:
            continue


async def sleep_heartbeating(seconds: float) -> None:
    """`asyncio.sleep(seconds)` that heartbeats while it sleeps."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while (remaining := deadline - loop.time()) > 0:
        activity.heartbeat()
        await asyncio.sleep(min(HEARTBEAT_POLL_SECONDS, remaining))


def cancelled_operation_result(request: dict[str, Any]) -> dict[str, Any]:
    """The operation boundary's public result of a unit settled `cancelled` before dispatch
    (`OperationExecutionService.cancel`): exact binding, no usage, no checkpoint."""

    binding = bind_operation_execution_request(OperationExecutionRequest.model_validate(request))
    return OperationExecutionResult(
        binding_id=binding.binding_id,
        semantic_attempt_key=binding.semantic_attempt_key,
        status="cancelled",
        failure_code="cancelled",
        failure_message=CANCEL_REASON,
        unit_key=binding.runtime_unit.unit_key if binding.runtime_unit is not None else None,
    ).model_dump(mode="json")


class RecordingOperationCancel:
    """A faithful fake `operation.cancel`: records each call and settles the unit
    `cancelled` with the operation boundary's result shape."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    @activity.defn(name="operation.cancel")
    async def cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(str(request["identity"]["operation_id"]))
        return cancelled_operation_result(request)
