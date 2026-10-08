"""FT-G7 Worker Deployment versioning against the local Temporal server (1.31, :7233).

A rolled worker drains the old version: a probe execution starts on version `v1`, version
`v2` (whose code adds a `workflow.patched` branch) is started and promoted, the `v1`
worker stops, and the running execution finishes on `v2` (`AUTO_UPGRADE`, the Mission
Control default). Its history then replays without nondeterminism on the `v2` code, and
Visibility shows no running execution left on `v1`.

The test needs the local development server from `make temporal-up` (127.0.0.1:7233 by
default, `MC_TEMPORAL_TEST_ADDRESS` overrides). It uses a unique deployment name and task
queue per run, so it never touches a real deployment's routing. Temporal Cloud is not used.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.client import Client, WorkflowHistory
from temporalio.common import WorkerDeploymentVersion
from temporalio.worker import Replayer, Worker, WorkerDeploymentConfig

with workflow.unsafe.imports_passed_through():
    from mission_control.adapters.temporal.client import (
        TemporalConnection,
        connect_temporal,
    )
    from mission_control.adapters.temporal.versioning import (
        DEFAULT_VERSIONING_BEHAVIOR,
        current_deployment_build_id,
        promote_worker_deployment_version,
    )

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
ROLL_PATCH = "ft-g7-versioning-probe-roll"


@workflow.defn(name="mc.ft_g7.versioning_probe", sandboxed=False)
class ProbeV1:
    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._released)
        return "v1"


@workflow.defn(name="mc.ft_g7.versioning_probe", sandboxed=False)
class ProbeV2:
    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._released)
        if workflow.patched(ROLL_PATCH):
            return "v2"
        return "v1"


async def _client() -> Client:
    try:
        async with asyncio.timeout(5):
            return await connect_temporal(
                TemporalConnection(target="local", address=ADDRESS, namespace=NAMESPACE)
            )
    except (TimeoutError, RuntimeError) as error:
        pytest.skip(f"local Temporal server at {ADDRESS} is unavailable: {error}")


def _config(deployment: str, build_id: str) -> WorkerDeploymentConfig:
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment_name=deployment, build_id=build_id),
        use_worker_versioning=True,
        default_versioning_behavior=DEFAULT_VERSIONING_BEHAVIOR,
    )


async def _eventually(predicate, *, attempts: int = 120) -> None:  # type: ignore[no-untyped-def]
    for _ in range(attempts):
        if await predicate():
            return
        await asyncio.sleep(0.5)
    raise AssertionError("condition not reached")


@pytest.mark.asyncio
async def test_rolled_worker_drains_the_old_version_and_its_history_replays() -> None:
    client = await _client()
    suffix = uuid.uuid4().hex[:10]
    deployment = f"mc-ft-g7-{suffix}"
    queue = f"mc-ft-g7-versioning-{suffix}"
    v1 = WorkerDeploymentVersion(deployment_name=deployment, build_id="v1")
    v2 = WorkerDeploymentVersion(deployment_name=deployment, build_id="v2")

    old = Worker(
        client, task_queue=queue, workflows=[ProbeV1], deployment_config=_config(deployment, "v1")
    )
    old_run = asyncio.create_task(old.run())
    try:
        assert await promote_worker_deployment_version(client, NAMESPACE, v1) is True
        assert await promote_worker_deployment_version(client, NAMESPACE, v1) is False
        handle = await client.start_workflow(
            ProbeV1.run,
            id=f"mc-ft-g7-probe-{suffix}",
            task_queue=queue,
            execution_timeout=timedelta(minutes=5),
        )

        async def started_on_v1() -> bool:
            execution = (await handle.describe()).raw_description.workflow_execution_info
            return execution.versioning_info.deployment_version.build_id == "v1"

        await _eventually(started_on_v1)

        new = Worker(
            client,
            task_queue=queue,
            workflows=[ProbeV2],
            deployment_config=_config(deployment, "v2"),
        )
        new_run = asyncio.create_task(new.run())
        try:
            assert await promote_worker_deployment_version(client, NAMESPACE, v2) is True
            assert await current_deployment_build_id(client, NAMESPACE, deployment) == "v2"
            await old.shutdown()
            await old_run
            await handle.signal(ProbeV1.release)
            # AUTO_UPGRADE: the running execution moved to v2 and took the new branch.
            assert await handle.result() == "v2"
            history: WorkflowHistory = await handle.fetch_history()

            async def v1_drained() -> bool:
                query = (
                    f"TemporalWorkerDeploymentVersion = '{deployment}:v1' "
                    "AND ExecutionStatus = 'Running'"
                )
                return (await client.count_workflows(query)).count == 0

            await _eventually(v1_drained)
        finally:
            await new.shutdown()
            await new_run
    finally:
        if not old_run.done():
            await old.shutdown()
            await old_run

    await Replayer(workflows=[ProbeV2]).replay_workflow(history)
