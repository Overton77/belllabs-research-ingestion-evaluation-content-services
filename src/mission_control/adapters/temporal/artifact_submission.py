from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from temporalio.client import Client

from mission_control.adapters.temporal.artifact_workflow import GenericArtifactWorkflow
from mission_control.contracts.identities import mission_root_id
from mission_control.domain.execution.contracts import (
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
)


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
        if request.request_scope.startswith("mc/"):
            root_id = mission_root_id(request.request_scope, request.run_id)
            workflow_id = f"{root_id}/artifact/{workflow_id}"
        payload = await self._client.execute_workflow(
            GenericArtifactWorkflow.run,
            request.model_dump(mode="json"),
            id=workflow_id,
            task_queue=self._task_queue,
        )
        return GenericArtifactWorkflowResult.model_validate(payload)
