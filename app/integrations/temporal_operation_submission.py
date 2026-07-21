from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from temporalio.client import Client

from app.domain.operation_execution.contracts import (
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
)
from app.temporal.artifact_workflow import GenericArtifactWorkflow


class TemporalGenericArtifactSubmitter:
    def __init__(self, client: Client, *, task_queue: str) -> None:
        self._client = client
        self._task_queue = task_queue

    async def submit(
        self, request: GenericArtifactWorkflowRequest
    ) -> GenericArtifactWorkflowResult:
        workflow_id = str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "generic-artifact-workflow",
                        request.run_id,
                        request.operation.identity.semantic_key,
                        request.operation.idempotency_key,
                    )
                ),
            )
        )
        payload = await self._client.execute_workflow(
            GenericArtifactWorkflow.run,
            request.model_dump(mode="json"),
            id=workflow_id,
            task_queue=self._task_queue,
        )
        return GenericArtifactWorkflowResult.model_validate(payload)
