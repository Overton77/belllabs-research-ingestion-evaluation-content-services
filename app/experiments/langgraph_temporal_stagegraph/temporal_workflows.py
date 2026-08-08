from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import CancelledError

with workflow.unsafe.imports_passed_through():
    from .contracts import CompletionRecord, TemporalStageInput
    from .temporal_activities import execute_deep_agent_stage, record_stage_completion


@workflow.defn
class TemporalStageWorkflow:
    @workflow.run
    async def run(self, request: TemporalStageInput) -> None:
        info = workflow.info()
        try:
            result = await workflow.execute_activity(
                execute_deep_agent_stage,
                request,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )
            completion = CompletionRecord(
                run_id=request.run_id,
                thread_id=request.thread_id,
                attempt_id=request.attempt_id,
                stage_id=request.stage_id,
                temporal_workflow_id=info.workflow_id,
                temporal_run_id=info.run_id,
                disposition="succeeded",
                output_text=result.output_text,
                output_digest=result.output_digest,
                error_type=None,
            )
        except CancelledError:
            raise
        except Exception as exc:
            completion = CompletionRecord(
                run_id=request.run_id,
                thread_id=request.thread_id,
                attempt_id=request.attempt_id,
                stage_id=request.stage_id,
                temporal_workflow_id=info.workflow_id,
                temporal_run_id=info.run_id,
                disposition="failed",
                output_text=None,
                output_digest=None,
                error_type=type(exc).__name__,
            )
        await workflow.execute_activity(
            record_stage_completion,
            completion,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=0,
            ),
        )
