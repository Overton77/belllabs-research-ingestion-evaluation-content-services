"""FT-G7 fast-track listing attributes on a real namespace (`WorkflowEnvironment.start_local`).

A StageGraph run submitted under the `required` policy starts its root with `mc_run_id`,
`mc_mission_id` and (for a fork) `ForkedFromRunId`; the root passes `mc_run_id` and its
`mc_mission_id` to the family; each operation writes `mc_run_id`, `mc_lane` and
`mc_phase` at its segment boundaries only (start `executing`, close `completed`). The
fast-track queries (`mc_run_id = ...`, `mc_phase = ...`, `ForkedFromRunId = ...`) list
them, and the new histories (which carry the `ft-g7-mc-visibility` marker) replay.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest
from temporalio.client import Client, WorkflowHistory
from temporalio.common import SearchAttributeKey
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    MC_VISIBILITY_PATCH,
)
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow
from mission_control.domain.coordinator.launch import BlueprintFamily
from tests.fixtures.temporal_history import patch_ids
from tests.integration.temporal.test_rrm_005_search_attributes import (
    UnitBoundStageGraphActivities,
    _start_local,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_run_input
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service


def _key(name: str) -> SearchAttributeKey[Any]:
    return next(key for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS if key.name == name)


def _mc_upserts(history: WorkflowHistory) -> list[dict[str, str]]:
    """The fast-track upserts of a history, decoded as `{name: raw json}`."""

    upserts: list[dict[str, str]] = []
    for event in history.events:
        if not event.HasField("upsert_workflow_search_attributes_event_attributes"):
            continue
        fields = event.upsert_workflow_search_attributes_event_attributes.search_attributes
        mc = {
            name: payload.data.decode("utf-8")
            for name, payload in fields.indexed_fields.items()
            if name.startswith("mc_")
        }
        if mc:
            upserts.append(mc)
    return upserts


async def _count(client: Client, query: str, expected: int) -> int:
    async with asyncio.timeout(30):
        while True:
            count = (await client.count_workflows(query)).count
            if count == expected:
                return count
            await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_runs_forks_and_operations_carry_the_fast_track_attributes() -> None:
    run_service, _runs = run_control_service()
    admitted = await run_service.admit(run_request(request_id="ft-g7-visibility"))
    fork = await run_service.admit(run_request(request_id="ft-g7-visibility-fork"))
    assert admitted.run_id is not None and fork.run_id is not None
    run_id, fork_id = admitted.run_id, fork.run_id
    activities = UnitBoundStageGraphActivities()
    activities.slow_release.set()
    env = await _start_local()
    async with env:
        client = env.client
        async with Worker(
            client,
            task_queue=QUEUE,
            workflows=[BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitter = TemporalWorkflowSubmitter.for_production(
                client,
                stagegraph_task_queue=QUEUE,
                goal_directed_task_queue="ft-g7-goal-unused",
                search_attribute_policy="required",
            )
            submission = await submitter.submit(
                replace(stage_run_input(_blueprint()), run_id=run_id),
                workflow_id="ignored",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
                mission_id="mission-ft-g7",
            )
            root = client.get_workflow_handle(submission.workflow_id)
            await asyncio.wait_for(root.result(), timeout=120)
            forked = await submitter.submit(
                replace(stage_run_input(_blueprint()), run_id=fork_id),
                workflow_id="ignored",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
                parent_run_id=run_id,
                mission_id="mission-ft-g7",
            )
            fork_root = client.get_workflow_handle(forked.workflow_id)
            await asyncio.wait_for(fork_root.result(), timeout=120)

        # Root, family and three operations of the source run.
        assert await _count(client, f"mc_run_id = '{run_id}'", 5) == 5
        # Root and family carry the mission; operations carry lane and phase instead.
        assert await _count(client, "mc_mission_id = 'mission-ft-g7'", 4) == 4
        assert await _count(client, f"mc_run_id = '{run_id}' AND mc_phase = 'completed'", 3) == 3
        assert await _count(client, f"mc_run_id = '{run_id}' AND mc_phase = 'executing'", 0) == 0
        assert await _count(client, f"ForkedFromRunId = '{run_id}'", 1) == 1
        listed = [item async for item in client.list_workflows(f"ForkedFromRunId = '{run_id}'")]
        assert [item.id for item in listed] == [forked.workflow_id]
        assert listed[0].typed_search_attributes.get(_key("ForkedFromRunId")) == [run_id]
        assert listed[0].typed_search_attributes.get(_key("mc_run_id")) == [fork_id]

        lanes = {
            "deep_agents" if bound.operation.execution_runtime == "deep_agent" else None
            for _unit, bound in activities.operations[:3]
        }
        for lane in lanes - {None}:
            assert await _count(client, f"mc_run_id = '{run_id}' AND mc_lane = '{lane}'", 3) == 3

        family = client.get_workflow_handle(f"family/{run_id}/1")
        family_attributes = (await family.describe()).typed_search_attributes
        assert family_attributes.get(_key("mc_mission_id")) == "mission-ft-g7"
        assert family_attributes.get(_key("mc_phase")) is None

        histories = [await root.fetch_history(), await family.fetch_history()]
        for _unit, bound in activities.operations[:3]:
            history = await client.get_workflow_handle(bound.workflow_id).fetch_history()
            assert MC_VISIBILITY_PATCH in patch_ids(history)
            # Segment boundaries only: the first (executing) and the closing (completed).
            upserts = _mc_upserts(history)
            assert [item.get("mc_phase") for item in upserts] == ['"executing"', '"completed"']
            assert '"mc_run_id"' not in str(upserts[1:]), "unchanged keys are not re-upserted"
            histories.append(history)

    replayer = Replayer(
        workflows=[BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    )
    for history in histories:
        await replayer.replay_workflow(history)
