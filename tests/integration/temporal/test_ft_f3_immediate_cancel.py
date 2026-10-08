"""FT-F3 immediate cancel on the operation workflow (time-skipping Temporal server).

The run's Stop Fence is persisted first; the `request_immediate_cancel` signal then stops the
in-flight cognitive Activity through the RRM-008 saga (the cancel path until FT-G2's
`lane.turn` lands): the in-flight model call is interrupted, `operation.cancel` reconciles,
and the Delivery Report collects the provider-acknowledged and settled milestones. The
workflow issues exactly the commands a normal cancel issues, and its history replays.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from temporalio.testing import WorkflowEnvironment

from mission_control.adapters.temporal.operation_activities import parse_operation_result
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.application.execution.stop_fence import (
    InMemoryStopFenceRepository,
    KernelHookFenceGate,
)
from mission_control.domain.policies.stop_fence import EffectAdmission, StopFence
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.integration.temporal.test_rrm_008_operation_workflow_cancellation import (
    _replays,
    _scheduled,
    _Stack,
)


@pytest.mark.asyncio
async def test_immediate_cancel_fences_then_cancels_and_reports_four_timestamps() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        fences = InMemoryStopFenceRepository()
        harness.service._stop_fences = fences  # the worker composition passes the store
        stack = _Stack(harness, environment)
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        entered, _gate = harness.model.gate_on(1)
        workflow_worker, activity_worker = stack.workers()
        async with workflow_worker, activity_worker:
            handle = await stack.start(request, "ft-f3-immediate-cancel")
            await asyncio.wait_for(entered.wait(), timeout=60)
            # 1. The fence is persisted before any cancel reaches the provider.
            fence = await fences.persist(
                StopFence(
                    request_scope="tenant-1",
                    run_id=harness.run_id,
                    generation=1,
                    command_id="cancel-immediate-1",
                    reason="wrong repo",
                    requested_at=datetime.now(UTC),
                )
            )
            # 2. From now on a Kernel Hook denies any new effect of this run.
            late = await KernelHookFenceGate(fences).before_effect(
                EffectAdmission(
                    request_scope="tenant-1",
                    run_id=harness.run_id,
                    generation=1,
                    effect_ref="tool_use:after-fence",
                    effect_kind="shell",
                )
            )
            assert not late.allowed and late.frame is not None
            # 3. The immediate branch cancels the in-flight attempt.
            await handle.signal(OperationWorkflow.request_immediate_cancel, fence.command_id)
            result = await asyncio.wait_for(handle.result(), timeout=120)
            assert await handle.query(OperationWorkflow.stop_fence_command) == fence.command_id
            history = await _replays(handle)

        assert result.disposition == "cancelled"
        assert parse_operation_result(result.result).status == "cancelled"
        assert len(harness.model.calls) == 1, "the interrupted call never resumed"
        scheduled = _scheduled(history)
        assert scheduled[0] == "operation.execute" and "operation.cancel" in scheduled
        report = await fences.report("tenant-1", harness.run_id)
        assert report is not None and report.state == "settled"
        assert report.requested_at <= report.fence_persisted_at
        assert report.provider_acknowledged_at is not None
        assert report.settled_at is not None
        assert report.provider_acknowledged_at <= report.settled_at
        print("FT-F3 EVIDENCE immediate cancel report:", report.model_dump(mode="json"))
