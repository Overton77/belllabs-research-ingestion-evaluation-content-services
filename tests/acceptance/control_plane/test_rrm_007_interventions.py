"""RRM-007 demonstrations: governed interventions on real Temporal with PostgreSQL authority.

A real Temporal dev server (`WorkflowEnvironment.start_local`) runs the stable root
(`BellLabsRunWorkflow`, started through `TemporalWorkflowSubmitter`) and the family child.
Run control, the boundary command ledger and the receipts are application PostgreSQL
(migration 0023). Every command enters through the public facade
(`POST /run-control/v1/runs/{run_id}/commands`), is delivered root-first (`deliver_message`
Update) and then to the family (`deliver_boundary_command` Update), and is applied by the
family's own boundary activity, which records `applied` atomically with the phase effect.

1. StageGraph: the declared wait is inspectable through authority while held, is released
   through the facade, and stays satisfied across the family's Continue-As-New.
2. GoalDirected: a pause requested while the executor runs is delivered at once and applied
   at the iteration boundary; the worker is stopped while the run is paused and a new worker
   takes over; the resume continues from the recorded frontier. Receipts, the root's message
   receipt cache and every captured history (root and family) are checked.

Cognition is a technical fixture (no model, no company research). Opt-in through
`TEST_APPLICATION_POSTGRES_DSN` (disposable stack only).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import Any

import asyncpg
import httpx
import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.temporal.boundary_commands import TemporalBoundaryCommandTransport
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import (
    StageGraphWorkflow,
    wait_condition_id,
)
from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from mission_control.bootstrap.technical_api import api
from mission_control.domain.coordinator.launch import BlueprintFamily
from mission_control.domain.policies.contracts import RunPhase, SatisfyWaitAction
from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from mission_control.interfaces.http.run_control import (
    get_boundary_intervention_service,
    get_run_control_service,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    GOAL_QUEUE,
    SCOPE,
    Authority,
    GovernedGoalActivities,
    GovernedStageGraphActivities,
    _goal_blueprint,
    _has_wait,
    _phase_is,
    _state_is,
    pause,
    replay,
    resume,
    until,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_input
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as goal_input
from tests.unit.run_control.test_run_control import actor, command
from tests.unit.run_control.test_run_control import service as run_control_service

WORKFLOWS = [BellLabsRunWorkflow, StageGraphWorkflow, GoalDirectedWorkflow, OperationWorkflow]


class Facade:
    """The public facade over the real Postgres run control, driven through the ASGI app."""

    def __init__(self, authority: Authority) -> None:
        self.authority = authority

    async def __aenter__(self) -> Facade:
        api.dependency_overrides[get_run_control_service] = lambda: self.authority.run_control
        api.dependency_overrides[get_boundary_intervention_service] = lambda: self.authority.facade
        api.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
            actor_id="operator",
            roles=frozenset({"operator", "relay"}),
            tenant_scopes=frozenset({SCOPE}),
            authority_refs=frozenset({"authority:lifecycle"}),
        )
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://run-control"
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()
        api.dependency_overrides.clear()

    async def command(self, run_id: str, command_id: str, action: Any) -> dict[str, Any]:
        run = await self.authority.run(run_id)
        body = command(run_id, run.version, command_id, action).model_copy(
            update={"actor": actor().model_copy(update={"permissions": frozenset()})}
        )
        response = await self._client.post(
            f"/run-control/v1/runs/{run_id}/commands", json=body.model_dump(mode="json")
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == "accepted", result
        return result

    async def projection(self, run_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/run-control/v1/runs/{run_id}", params={"request_scope": SCOPE}
        )
        assert response.status_code == 200, response.text
        return response.json()

    async def ledger(self, run_id: str) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"/run-control/v1/runs/{run_id}/boundary-commands", params={"request_scope": SCOPE}
        )
        assert response.status_code == 200, response.text
        return response.json()


async def _receipt_rows(pool: asyncpg.Pool, run_id: str) -> list[tuple[str, int, str, str]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT c.command_id, c.target_sequence, r.state, r.recorded_by
            FROM belllabs_control.boundary_commands c
            JOIN belllabs_control.boundary_command_receipts r
              ON r.request_scope = c.request_scope AND r.run_id = c.run_id
             AND r.command_id = c.command_id
            WHERE c.run_id = $1
            ORDER BY c.target_sequence, r.ordinal
            """,
            run_id,
        )
    return [
        (row["command_id"], row["target_sequence"], row["state"], row["recorded_by"])
        for row in rows
    ]


def _submitter(client: Client) -> TemporalWorkflowSubmitter:
    return TemporalWorkflowSubmitter(
        client, stagegraph_task_queue=QUEUE, goal_directed_task_queue=GOAL_QUEUE
    )


@pytest.mark.asyncio
async def test_interventions_on_real_temporal_with_postgres_authority(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
    finally:
        await owner.close()
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=8)
    evidence: dict[str, Any] = {}
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        async with await WorkflowEnvironment.start_local(dev_server_log_level="error") as env:
            authority = Authority(env.client, run_control)
            authority.facade = BoundaryInterventionService(
                run_control,
                BoundaryCommandDeliveryService(
                    run_control, TemporalBoundaryCommandTransport(env.client)
                ),
            )
            async with Facade(authority) as facade:
                evidence["stagegraph"] = await _stagegraph_demo(env, pool, authority, facade)
                evidence["goal_directed"] = await _goal_directed_demo(env, pool, authority, facade)
    finally:
        await pool.close()
    print("RRM-007 EVIDENCE interventions:", json.dumps(evidence, sort_keys=True))


async def _stagegraph_demo(
    env: WorkflowEnvironment, pool: asyncpg.Pool, authority: Authority, facade: Facade
) -> dict[str, Any]:
    activities = GovernedStageGraphActivities(authority)
    run_id = await authority.admit("rrm-007-demo-stagegraph")
    run_input = replace(
        stage_input(_blueprint(workflow_wait=True)), run_id=run_id, force_continue_as_new=True
    )
    condition_id = wait_condition_id("release-workflow")
    async with Worker(
        env.client,
        task_queue=QUEUE,
        workflows=WORKFLOWS,
        workflow_runner=coordinator_workflow_runner(),
        activities=activities.functions,
    ):
        submitted = await _submitter(env.client).submit(
            run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
        )
        root = env.client.get_workflow_handle(submitted.workflow_id)
        family_id = f"family/{run_id}/1"
        await until(lambda: _has_wait(authority, run_id, condition_id), seconds=60)
        projection = await facade.projection(run_id)
        assert projection["phase"] == "waiting"
        assert projection["execution_target"]["root_workflow_id"] == submitted.workflow_id
        assert projection["execution_target"]["family_workflow_id"] == family_id
        assert [item["condition_id"] for item in projection["active_waits"]] == [condition_id]

        accepted = await facade.command(
            run_id,
            "release",
            SatisfyWaitAction(
                condition_id=condition_id, verification_evidence_ref="evidence:operator"
            ),
        )
        assert accepted["reason_code"] == "accepted_pending_application"
        await until(lambda: _state_is(authority, run_id, "release", "applied"), seconds=60)
        projection = await facade.projection(run_id)
        assert projection["phase"] == "active" and projection["active_waits"] == []
        continuity = await root.query(BellLabsRunWorkflow.continuity)
        assert [
            (item.message_id, item.sequence, item.status) for item in continuity.message_receipts
        ] == [("release", 1, "accepted")]
        await asyncio.wait_for(activities.downstream_started.wait(), timeout=60)
        activities.slow_release.set()
        root_result = await asyncio.wait_for(root.result(), timeout=120)
        family = env.client.get_workflow_handle(family_id)
        family_state = await family.query(StageGraphWorkflow.boundary_state)
        root_runs = await replay(root, [BellLabsRunWorkflow])
        family_runs = await replay(family, [StageGraphWorkflow, OperationWorkflow])

    rows = await _receipt_rows(pool, run_id)
    assert rows == [
        ("release", 1, "accepted", "run_control"),
        ("release", 1, "delivered", family_id),
        ("release", 1, "applied", family_id),
    ] or rows == [
        ("release", 1, "accepted", "run_control"),
        ("release", 1, "delivered", "boundary-delivery"),
        ("release", 1, "applied", family_id),
    ]
    ledger = await facade.ledger(run_id)
    assert [item["state"] for item in ledger[0]["receipts"]] == [
        "accepted",
        "delivered",
        "applied",
    ]
    assert (root_runs, family_runs) == (1, 2), "root and both family segments replayed"
    assert family_state["technical_segment"] == 2
    assert family_state["satisfied_wait_ids"] == ["release-workflow"]
    assert sorted(activities.admission_order) == ["downstream", "fast", "slow"]
    # The root returns the family result payload (a dict) as its own result.
    assert root_result["output_refs"]["downstream"] == ["artifact:downstream"]
    return {
        "run_id": run_id,
        "root_workflow_id": submitted.workflow_id,
        "family_workflow_id": family_id,
        "receipts": rows,
        "family_runs_replayed": family_runs,
        "root_runs_replayed": root_runs,
        "admission_order": activities.admission_order,
    }


async def _family_recorded_pause(client: Any, family_id: str) -> bool:
    state = await client.get_workflow_handle(family_id).query(GoalDirectedWorkflow.boundary_state)
    return state["paused"] is not None


async def _goal_directed_demo(
    env: WorkflowEnvironment, pool: asyncpg.Pool, authority: Authority, facade: Facade
) -> dict[str, Any]:
    activities = GovernedGoalActivities(authority, complete_at_iteration=2)
    activities.release_executor.clear()
    run_id = await authority.admit("rrm-007-demo-goal", bounded={"goal.iterations": 10})
    run_input = goal_input(blueprint=_goal_blueprint(max_iterations=3), run_id=run_id)
    family_id = f"family/{run_id}/1"

    def worker(identity: str) -> Worker:
        return Worker(
            env.client,
            task_queue=GOAL_QUEUE,
            workflows=WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
            identity=identity,
        )

    async with worker("rrm007-worker-1"):
        submitted = await _submitter(env.client).submit(
            run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.GOAL_DIRECTED
        )
        await asyncio.wait_for(activities.executor_started.wait(), timeout=60)
        await facade.command(run_id, "pause", pause("hold-run"))
        await until(lambda: _state_is(authority, run_id, "pause", "delivered"), seconds=60)
        assert (await facade.projection(run_id))["phase"] == "active", "delivered is not applied"
        activities.release_executor.set()
        await until(lambda: _phase_is(authority, run_id, RunPhase.PAUSED), seconds=90)
        # The `applied` receipt and the PAUSED projection are written by the boundary
        # activity, before the family's workflow task has consumed the activity result. Stop
        # worker 1 only once the family itself has recorded the pause, or worker 2 would
        # query a family that is still waiting to re-run that activity.
        await until(lambda: _family_recorded_pause(env.client, family_id), seconds=90)
    # Worker 1 is gone while the run is paused: nothing is in memory any more.
    paused_rows = await _receipt_rows(pool, run_id)
    assert [row[2] for row in paused_rows if row[0] == "pause"] == [
        "accepted",
        "delivered",
        "applied",
    ]
    projection = await facade.projection(run_id)
    assert projection["phase"] == "paused"
    assert [item["decision_id"] for item in projection["active_pauses"]] == ["hold-run"]
    assert activities.prepared_roles == ["executor", "verifier"]

    async with worker("rrm007-worker-2"):
        root = env.client.get_workflow_handle(submitted.workflow_id)
        family = env.client.get_workflow_handle(family_id)
        state = await family.query(GoalDirectedWorkflow.boundary_state)
        assert state["paused"]["next_goal_iteration"] == 2
        assert state["paused"]["active_revision_id"] == "goal-revision:1"
        await facade.command(run_id, "resume", resume("hold-run", "release-run"))
        await until(lambda: _state_is(authority, run_id, "resume", "applied"), seconds=90)
        root_result = await asyncio.wait_for(root.result(), timeout=180)
        final_state = await family.query(GoalDirectedWorkflow.boundary_state)
        continuity = await root.query(BellLabsRunWorkflow.continuity)
        root_runs = await replay(root, [BellLabsRunWorkflow])
        family_runs = await replay(family, [GoalDirectedWorkflow, OperationWorkflow])

    rows = await _receipt_rows(pool, run_id)
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("pause", 1, "accepted"),
        ("pause", 1, "delivered"),
        ("pause", 1, "applied"),
        ("resume", 2, "accepted"),
        ("resume", 2, "delivered"),
        ("resume", 2, "applied"),
    ]
    assert [
        (item.message_id, item.sequence, item.status) for item in continuity.message_receipts
    ] == [("pause", 1, "accepted"), ("resume", 2, "accepted")]
    assert root_result["convergence_proposal"]["action"] == "complete"
    assert root_result["goal_iterations"] == 2
    assert activities.prepared_roles == ["executor", "verifier", "executor", "verifier"]
    assert final_state["paused"] is None
    assert final_state["applied_command_ids"] == ["pause", "resume"]
    assert (root_runs, family_runs) == (1, 1)
    return {
        "run_id": run_id,
        "root_workflow_id": submitted.workflow_id,
        "family_workflow_id": family_id,
        "receipts": rows,
        "worker_restart_while_paused": True,
        "family_runs_replayed": family_runs,
        "root_runs_replayed": root_runs,
    }
