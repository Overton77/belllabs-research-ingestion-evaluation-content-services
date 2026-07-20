from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.application.artifact_promotion import ArtifactPromotionService
from app.application.workspace_candidates import WorkspaceCandidateCaptureService
from app.domain.operation_execution.contracts import (
    ArtifactPromotionPlan,
    ArtifactPromotionRequest,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.temporal.artifact_workflow import GenericArtifactWorkflow
from app.temporal.operation_activities import OperationExecutionActivities


class ArtifactPromotionActivities:
    def __init__(
        self,
        *,
        service: ArtifactPromotionService,
        candidates: WorkspaceCandidateCaptureService,
    ) -> None:
        self._service = service
        self._candidates = candidates

    @activity.defn(name="artifact.promote")
    async def promote(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            plan = ArtifactPromotionPlan.model_validate(payload["promotion"])
            candidate, content = await self._candidates.get_for_path(
                plan.namespace_id,
                plan.workspace_id,
                plan.logical_path,
            )
            if candidate.output_slot != plan.output_slot or candidate.owner != plan.owner:
                raise ValueError("captured candidate does not match promotion plan")
            request = ArtifactPromotionRequest(
                request_scope=str(payload["request_scope"]),
                binding_id=str(payload["binding_id"]),
                namespace_id=plan.namespace_id,
                workspace_id=plan.workspace_id,
                output_slot=plan.output_slot,
                logical_path=plan.logical_path,
                owner=plan.owner,
                candidate_id=candidate.candidate_id,
                content_digest=candidate.content_digest,
                media_type=candidate.media_type,
                size_bytes=candidate.size_bytes,
                permission_ref=plan.permission_ref,
                permission_outcome=plan.permission_outcome,
                output_contract_ref=plan.output_contract_ref,
                checks=plan.checks,
                requested_at=datetime.now(UTC),
            )
            promoted = await self._service.promote(request, content)
        except (IdempotencyConflict, ValueError) as error:
            raise ApplicationError(
                str(error),
                type="artifact_promotion_rejected",
                non_retryable=True,
            ) from error
        return promoted.model_dump(mode="json")


def create_generic_artifact_worker(
    client: Client,
    *,
    task_queue: str,
    operations: OperationExecutionActivities,
    artifacts: ArtifactPromotionActivities,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[GenericArtifactWorkflow],
        activities=[operations.execute, artifacts.promote],
    )
