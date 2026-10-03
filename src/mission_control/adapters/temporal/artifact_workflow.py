from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError


@workflow.defn(name="belllabs.generic-artifact")
class GenericArtifactWorkflow:
    """Generic traced path: governed operation, candidate capture, explicit promotion."""

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        operation = await workflow.execute_activity(
            "operation.execute",
            payload["operation"],
            result_type=dict,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        if operation.get("status") != "completed":
            raise ApplicationError(
                "generic artifact operation did not complete",
                type="generic_artifact_operation_failed",
                non_retryable=True,
            )
        artifact = await workflow.execute_activity(
            "artifact.promote",
            {
                "request_scope": payload["request_scope"],
                "binding_id": operation["binding_id"],
                "promotion": payload["promotion"],
            },
            result_type=dict,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        return {
            "workflow_id": workflow.info().workflow_id,
            "operation": operation,
            "artifact": artifact,
        }
