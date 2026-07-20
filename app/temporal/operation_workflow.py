from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn(name="belllabs.operation-execution")
class OperationExecutionWorkflow:
    """Durably invokes one stable semantic operation activity."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        result = await workflow.execute_activity(
            "operation.execute",
            request,
            result_type=dict,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        return result
