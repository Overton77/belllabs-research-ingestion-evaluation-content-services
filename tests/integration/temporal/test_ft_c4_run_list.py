"""FT-C4 on a real namespace (`WorkflowEnvironment.start_local`): run list over Visibility.

A mission-scoped StageGraph run (``mc.mission_run.v1`` root, family, ``mc.operation.v1``
operations; every workflow id under ``mc/<installation>/<application>/``) is submitted
under the `required` policy with its ledger mission. The family now upserts ``mc_phase``
(``executing`` at start, ``completed`` at its close, behind ``ft-c4-family-phase``); the
root carries ``mc_mission_id`` and ``mc_run_id``; operations carry ``mc_lane`` and
``mc_phase``. :class:`RunListService` over :class:`TemporalRunVisibility` lists the run for
``lane``, ``phase``, ``mission_id`` and ``status`` filters, enriched from the ledger, and
another application's scope lists nothing. The new histories replay.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest
from temporalio.common import SearchAttributeKey
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    FAMILY_PHASE_PATCH,
)
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.visibility import TemporalRunVisibility
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.adapters.temporal.workflows.operation import MissionOperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow
from mission_control.application.frames.search import RunListService
from mission_control.domain.coordinator.launch import BlueprintFamily
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.temporal_history import patch_ids
from tests.integration.temporal.test_ft_g7_visibility import _mc_upserts
from tests.integration.temporal.test_rrm_005_search_attributes import (
    UnitBoundStageGraphActivities,
    _start_local,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_run_input
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

INSTALLATION = UUID("0192a4f0-0000-7000-8000-00000000c404")
SCOPE = f"mc/{INSTALLATION}/biotech/1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a01"
OTHER_APP_SCOPE = f"mc/{INSTALLATION}/ai-engineer/1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a01"
READER = ActorContext(actor_id="reader", permissions=frozenset({"workflow_run.read"}))


def _key(name: str) -> SearchAttributeKey[Any]:
    return next(key for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS if key.name == name)


class _Ledger:
    """The in-memory ledger admitted under `tenant-1`; served under the mission scope."""

    def __init__(self, authority: Any) -> None:
        self._authority = authority

    async def get_run(self, request_scope: str, run_id: str) -> Any:
        return await self._authority.get_run("tenant-1", run_id)


async def _eventually(service: RunListService, query: str, expected: set[str]) -> Any:
    async with asyncio.timeout(30):
        while True:
            page = await service.list(query, actor=READER)
            if {row.run_id for row in page.rows} == expected:
                return page
            await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_run_list_queries_typed_search_attributes_and_family_phase() -> None:
    authority, _runs = run_control_service()
    admitted = await authority.admit(run_request(request_id="ft-c4-run-list"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    activities = UnitBoundStageGraphActivities()
    activities.slow_release.set()
    env = await _start_local()
    async with env:
        client = env.client
        async with Worker(
            client,
            task_queue=QUEUE,
            workflows=[MissionRunWorkflow, StageGraphWorkflow, MissionOperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitter = TemporalWorkflowSubmitter.for_production(
                client,
                stagegraph_task_queue=QUEUE,
                goal_directed_task_queue="ft-c4-goal-unused",
                search_attribute_policy="required",
                mission_installation_id=INSTALLATION,
                mission_application_id="biotech",
            )
            submission = await submitter.submit(
                replace(stage_run_input(_blueprint()), run_id=run_id, request_scope=SCOPE),
                workflow_id="ignored",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
                mission_id="mission-ft-c4",
            )
            assert submission.workflow_id.startswith(f"mc/{INSTALLATION}/biotech/run/")
            root = client.get_workflow_handle(submission.workflow_id)
            await asyncio.wait_for(root.result(), timeout=120)

        family_id = f"{submission.workflow_id}/family/1"
        family = client.get_workflow_handle(family_id)
        family_attributes = (await family.describe()).typed_search_attributes
        assert family_attributes.get(_key("mc_phase")) == "completed"
        family_history = await family.fetch_history()
        assert FAMILY_PHASE_PATCH in patch_ids(family_history)
        assert [item.get("mc_phase") for item in _mc_upserts(family_history)] == [
            '"executing"',
            '"completed"',
        ]

        service = RunListService(
            TemporalRunVisibility(client), _Ledger(authority), request_scope=SCOPE
        )
        by_phase = await _eventually(service, "phase='completed'", {run_id})
        row = by_phase.rows[0]
        assert row.mission_id == "mission-ft-c4"
        assert row.phases == ("completed",)
        assert row.temporal_status == "COMPLETED"
        assert row.lifecycle == "pending"  # the ledger is the authority (fake activities)
        await _eventually(service, "phase='completed' AND mission_id='mission-ft-c4'", {run_id})
        # The fixture's operations are native (no lane), so no execution carries `mc_lane`.
        assert (await service.list("lane='deep_agents'", actor=READER)).rows == ()
        await _eventually(service, "status='completed'", {run_id})
        assert (await service.list("phase='executing'", actor=READER)).rows == ()

        other = RunListService(
            TemporalRunVisibility(client), _Ledger(authority), request_scope=OTHER_APP_SCOPE
        )
        assert (await other.list(None, actor=READER)).rows == ()

        histories = [await root.fetch_history(), family_history]
    replayer = Replayer(
        workflows=[MissionRunWorkflow, StageGraphWorkflow, MissionOperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    )
    for history in histories:
        await replayer.replay_workflow(history)
