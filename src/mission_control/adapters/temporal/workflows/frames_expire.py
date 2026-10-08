"""`mc.frames_expire.v1`: the scheduled retention run of the Native Event Store."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from mission_control.adapters.temporal.activities.frames_expire import (
        FRAMES_EXPIRE_ACTIVITY,
        FRAMES_EXPIRE_WORKFLOW,
        FramesExpireInput,
        FramesExpireResult,
    )


@workflow.defn(name=FRAMES_EXPIRE_WORKFLOW)
class FramesExpireWorkflow:
    @workflow.run
    async def run(self, request: FramesExpireInput) -> FramesExpireResult:
        now = request.now or workflow.now().isoformat()
        return await workflow.execute_activity(
            FRAMES_EXPIRE_ACTIVITY,
            FramesExpireInput(request_scopes=list(request.request_scopes), now=now),
            result_type=FramesExpireResult,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
