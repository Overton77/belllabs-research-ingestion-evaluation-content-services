from __future__ import annotations

import asyncio
import sys
from datetime import timedelta

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow
from tests.integration.temporal.test_wp_bp_010_temporal import (
    QUEUE,
    FakeStageGraphActivities,
    _blueprint,
    _run_input,
)


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform == "win32", reason="qualified in the WSL Linux worker runtime")
async def test_declared_wait_survives_worker_loss_and_resumes_from_signal() -> None:
    activities = FakeStageGraphActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    workflow_id = "family/run-wp-bp-010-wait/1"
    # The fixture's exact native placement binds operation activities to QUEUE,
    # so the recovery workers must poll that same queue for the child workflows
    # and their activities.
    wait_queue = QUEUE
    async with environment:
        first_worker = Worker(
            environment.client,
            task_queue=wait_queue,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
            identity="wp-bp-010-worker-before-loss",
            sticky_queue_schedule_to_start_timeout=timedelta(seconds=1),
            max_cached_workflows=0,
        )
        first_worker_task = asyncio.create_task(first_worker.run())
        second_worker: Worker | None = None
        second_worker_task: asyncio.Task[None] | None = None
        try:
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _run_input(_blueprint(workflow_wait=True)),
                id=workflow_id,
                task_queue=wait_queue,
            )
            await asyncio.wait_for(activities.initialized.wait(), timeout=20)
            assert activities.admission_order == []
            target_host = environment.client.service_client.config.target_host
            replacement_client = await Client.connect(target_host)
            second_worker = Worker(
                replacement_client,
                task_queue=wait_queue,
                workflows=[StageGraphWorkflow, OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
                activities=activities.functions,
                identity="wp-bp-010-worker-after-loss",
                sticky_queue_schedule_to_start_timeout=timedelta(seconds=1),
                max_cached_workflows=0,
            )
            second_worker_task = asyncio.create_task(second_worker.run())
            # Establish the replacement's non-sticky poller before terminating
            # the worker that owns the cached workflow task.
            await asyncio.sleep(1)
        finally:
            await first_worker.shutdown()
            await first_worker_task
        assert second_worker is not None and second_worker_task is not None
        try:
            if second_worker_task.done():
                second_worker_task.result()
            # Allow the one-second sticky schedule-to-start timeout to expire
            # after the owning worker has actually stopped.
            await asyncio.sleep(3)
            handle = environment.client.get_workflow_handle(workflow_id)
            await handle.signal(StageGraphWorkflow.satisfy_wait, "release-workflow")
            assert "release-workflow" in await handle.query(
                StageGraphWorkflow.satisfied_waits
            )
            try:
                await asyncio.wait_for(activities.downstream_started.wait(), timeout=30)
            except TimeoutError:
                pytest.fail(
                    f"replacement runtime state: "
                    f"{await handle.query(StageGraphWorkflow.runtime_state)}"
                )
            activities.slow_release.set()
            result = await handle.result()
        finally:
            await second_worker.shutdown()
            await second_worker_task

    completion = result["completion_proposal"]
    assert completion["required_obligations_accepted"] is True
    assert completion["pending_dependency_ids"] == []
    assert completion["open_producer_liability_ids"] == []
    assert activities.admission_order
