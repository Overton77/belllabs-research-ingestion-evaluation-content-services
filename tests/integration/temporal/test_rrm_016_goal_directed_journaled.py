"""RRM-016: the journaled GoalDirected family on Temporal (time-skipping server).

The production GoalDirected activities (`compose_goal_directed_activities`) and the
production `operation.execute` activity (`OperationExecutionActivities`) run a two-iteration
GoalDirected run over in-memory run control, journal and lineage, with a real
`create_deep_agent` graph and a deterministic model. A pause is requested while the first
executor runs and applied at the iteration boundary (RRM-007); the resume continues the
frontier. Every executor and verifier operation is verified by the real run-control
authority, journaled, fenced and settled exactly once; the family consumes each settlement
and records no operation usage itself. Captured histories replay, carry the RRM-016 patch,
and pre-change histories keep their own `record_usage` commands.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from temporalio import activity
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.application.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    operation_settlement_id,
)
from app.domain.operation_execution.contracts import OperationExecutionResult
from app.domain.orchestration.goal_directed_runtime import (
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import EffectDisposition, RunOutcome, RunPhase
from app.integrations.artifact_payloads import InMemoryArtifactPayloadStore
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.goal_directed import (
    JOURNALED_SETTLEMENT_PATCH,
    GoalDirectedWorkflow,
)
from app.temporal.workflows.operation import OperationWorkflow
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    TOKENS_PER_OPERATION,
    GoalComposition,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
    turns_by_operation,
)
from tests.fixtures.temporal_history import patch_ids, scheduled_activity_inputs
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    Authority,
    _phase_is,
    _state_is,
    pause,
    replay,
    resume,
    until,
)
from tests.integration.temporal.test_wp_bp_020_temporal import (
    FakeGoalDirectedActivities,
)
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as fake_goal_input

QUEUE = "rrm016-goal-directed"
# Captured from `test_journaled_goal_directed_run_settles_each_operation_once_through_a_pause`
# with `RRM016_CAPTURE_HISTORY_DIR` set: the journaled family through a pause and resume.
POST_CHANGE = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm016_post_change"
POST_CHANGE_HISTORY = "goal_directed_journaled_pause_resume.run1.json"
PRE_CHANGE = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm007_pre_change"
BASELINE = {"tokens.total": 20}


# --- History inspection ----------------------------------------------------------------------


def _write_history(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _read_history(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def lifecycle_command_ids(history: WorkflowHistory) -> list[str]:
    return [
        str(item["command_id"])
        for item in scheduled_activity_inputs(history, "goaldirected.apply_lifecycle_command")
    ]


# --- The journaled family --------------------------------------------------------------------


async def _composition(run_control: Any) -> GoalComposition:
    return await compose_goal_directed(
        run_control=run_control,
        journal=MemoryOperationJournal(),
        lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
        results=InMemoryArtifactPayloadStore(),
        bindings=InMemoryOperationBindingRepository(),
        saver=InMemorySaver(),
        model=GoalScriptedModel(),
        blueprint=goal_blueprint(),
    )


def _workers(environment: WorkflowEnvironment, composition: GoalComposition) -> tuple[Worker, ...]:
    return (
        Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=coordinator_activities("GoalDirected", composition.family),
        ),
        Worker(
            environment.client,
            task_queue=composition.binding.task_queue,
            activities=agent_cognitive_activities(
                OperationExecutionActivities(composition.service, worker_identity="rrm016")
            ),
        ),
    )


@pytest.mark.asyncio
async def test_journaled_goal_directed_run_settles_each_operation_once_through_a_pause() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        run_control = goal_run_control()
        authority = Authority(environment.client, run_control)
        composition = await _composition(run_control)
        run_id = await admit_goal_run(run_control, "rrm-016-temporal")
        workflow_id = f"family/{run_id}/1"
        entered, gate = composition.model.gate_on(1)
        family_worker, cognitive_worker = _workers(environment, composition)
        async with family_worker, cognitive_worker:
            handle = await environment.client.start_workflow(
                GoalDirectedWorkflow.run,
                goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
                id=workflow_id,
                task_queue=QUEUE,
            )
            # REQ-BP-GD-011: a pause requested while the first executor runs is delivered at
            # once and applied only at the iteration boundary.
            await asyncio.wait_for(entered.wait(), timeout=60)
            await authority.intervene(run_id, "pause", pause("hold-run"))
            await until(lambda: _state_is(authority, run_id, "pause", "delivered"), seconds=60)
            assert (await authority.run(run_id)).phase == RunPhase.ACTIVE
            gate.set()
            await until(lambda: _state_is(authority, run_id, "pause", "applied"), seconds=60)
            assert await _phase_is(authority, run_id, RunPhase.PAUSED)
            paused = (await handle.query(GoalDirectedWorkflow.boundary_state))["paused"]
            assert paused["next_goal_iteration"] == 2
            # The iteration-1 operations settled before the boundary: nothing is reserved
            # for them while the run is paused.
            budget = await run_control.get_budget(SCOPE, run_id)
            assert set(budget.reservations) == {"baseline"}
            assert len(budget.usage_records) == 2
            await authority.intervene(run_id, "resume", resume("hold-run", "release-run"))
            result = await handle.result()
            history = await handle.fetch_history()

        assert result.convergence_proposal is not None
        assert result.convergence_proposal.action == "complete"
        assert result.goal_iterations == 2
        assert await authority.states(run_id, "pause") == ["accepted", "delivered", "applied"]
        assert await authority.states(run_id, "resume") == ["accepted", "delivered", "applied"]

        # Every executor and verifier operation: one claim, one settlement, one usage record.
        bindings = [
            item.operation_binding_ref for item in composition.documents.iterations
        ] + [item.verifier_binding_ref for item in composition.documents.verifications]
        assert len(set(bindings)) == 4
        budget = await run_control.get_budget(SCOPE, run_id)
        effects = await run_control.get_effects(SCOPE, run_id)
        run = await run_control.get_run(SCOPE, run_id)
        settlements = {operation_settlement_id(binding): binding for binding in bindings}
        assert set(budget.usage_records) == {*settlements, "goal-usage:baseline"}
        for settlement_id, binding in settlements.items():
            usage = budget.usage_records[settlement_id]
            assert usage.authority_ref == binding
            assert usage.actual_amounts == {"tokens.total": TOKENS_PER_OPERATION}
        assert budget.consumed.get("tokens.total") == 4 * TOKENS_PER_OPERATION
        assert not any(budget.reserved.values()) and not any(budget.pending_settlement.values())
        assert budget.reservations == {}
        assert sorted(item.operation_ref for item in effects.claims.values()) == sorted(bindings)
        assert {item.disposition for item in effects.claims.values()} == {
            EffectDisposition.SUCCEEDED
        }
        assert {
            (item.settlement_id, item.accepted_by_authority_ref)
            for item in run.accepted_operation_settlement_evidence
        } == set(settlements.items())
        assert run.terminal_outcome == RunOutcome.COMPLETED

        # REQ-BP-GD-012 / isolation: the executor session is reused in iteration order (one
        # new input per iteration), the verifier never shares it, and the goal context never
        # reached the system prompt.
        assert turns_by_operation(composition.model) == [
            ("executor", 1, 1),
            ("executor", 1, 1),
            ("verifier", 1, 1),
            ("verifier", 1, 1),
            ("executor", 2, 2),
            ("executor", 2, 2),
            ("verifier", 2, 2),
            ("verifier", 2, 2),
        ]
        [first, second] = composition.documents.iterations
        assert first.session_id == second.session_id
        assert {item.verifier_session_id for item in composition.documents.verifications} != {
            first.session_id
        }
        assert not any(item["system_has_goal_context"] for item in composition.model.turns)

        # The family recorded no operation usage: only the run-level baseline release.
        assert [
            item for item in lifecycle_command_ids(history) if item.startswith("goal:usage:")
        ] == ["goal:usage:baseline"]
        assert JOURNALED_SETTLEMENT_PATCH in patch_ids(history)
        assert await replay(handle, [GoalDirectedWorkflow, OperationWorkflow]) == 1
        capture = os.getenv("RRM016_CAPTURE_HISTORY_DIR")
        if capture:  # re-capture the committed post-change fixture (see POST_CHANGE)
            _write_history(Path(capture) / POST_CHANGE_HISTORY, history.to_json())
        print(
            "RRM-016 EVIDENCE temporal "
            + json.dumps(
                {
                    "run_id": run_id,
                    "usage_records": sorted(budget.usage_records),
                    "consumed": budget.consumed,
                    "settled_effects": len(effects.claims),
                    "accepted_settlements": len(run.accepted_operation_settlement_evidence),
                    "pause": await authority.states(run_id, "pause"),
                    "patches": sorted(patch_ids(history)),
                },
                sort_keys=True,
            )
        )


class _UngovernedGoalActivities(FakeGoalDirectedActivities):
    """A composition whose reconciliation carries no run-control settlement."""

    @activity.defn(name="goaldirected.reconcile_operation")
    async def reconcile(  # type: ignore[override]
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        result = await super().reconcile(request)
        return result.model_copy(update={"settlement": None})


@pytest.mark.asyncio
async def test_family_fails_closed_without_a_run_control_settlement() -> None:
    activities = _UngovernedGoalActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        async with Worker(
            environment.client,
            task_queue="wp-bp-020-temporal",
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            with pytest.raises(WorkflowFailureError) as failure:
                await environment.client.execute_workflow(
                    GoalDirectedWorkflow.run,
                    replace(fake_goal_input(), run_id="run-rrm-016-ungoverned"),
                    id="family/run-rrm-016-ungoverned/1",
                    task_queue="wp-bp-020-temporal",
                )
    assert "goal_operation_settlement_missing" in str(failure.value.cause)
    # No usage was recorded by the family in place of the missing settlement.
    assert "record_usage" not in activities.lifecycle_kinds


@pytest.mark.asyncio
async def test_pre_change_goal_directed_history_keeps_its_own_usage_commands() -> None:
    """The pre-change history has no RRM-016 patch and replays its `record_usage` path."""

    history = WorkflowHistory.from_json(
        "family/pre-goal/1",
        (PRE_CHANGE / "goal_directed_two_iterations.run1.json").read_text("utf-8"),
    )
    assert JOURNALED_SETTLEMENT_PATCH not in patch_ids(history)
    usage_commands = [
        item for item in lifecycle_command_ids(history) if item.startswith("goal:usage:")
    ]
    assert len(usage_commands) == 4  # executor and verifier of two iterations
    await Replayer(
        workflows=[GoalDirectedWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    ).replay_workflow(history)


def test_operation_result_contract_is_unchanged() -> None:
    """The family consumes settlements through reconciliation; the operation result payload
    (a Temporal payload in every captured history) gains no field."""

    assert "settlement" not in OperationExecutionResult.model_fields


@pytest.mark.asyncio
async def test_post_change_goal_directed_history_replays_on_the_journaled_path() -> None:
    """The captured RRM-016 history (pause, resume, four journaled settlements) replays; later
    tickets that change the family must keep it replaying."""

    history = WorkflowHistory.from_json(
        "family/rrm-016-post-change/1", _read_history(POST_CHANGE / POST_CHANGE_HISTORY)
    )
    assert JOURNALED_SETTLEMENT_PATCH in patch_ids(history)
    assert [
        item for item in lifecycle_command_ids(history) if item.startswith("goal:usage:")
    ] == ["goal:usage:baseline"]
    assert [
        item["operation_role"]
        for name in ("goaldirected.prepare_executor", "goaldirected.prepare_verifier")
        for item in scheduled_activity_inputs(history, name)
    ] == ["executor", "executor", "verifier", "verifier"]
    await Replayer(
        workflows=[GoalDirectedWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    ).replay_workflow(history)


@pytest.mark.xfail(
    strict=True,
    raises=WorkflowFailureError,
    reason=(
        "RRM-019: the family promotes every iteration's output refs, the terminal proposal "
        "names only the last executor's, so the reducer rejects terminal_output_mismatch"
    ),
)
@pytest.mark.asyncio
async def test_iterations_with_distinct_output_refs_terminalize() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        run_control = goal_run_control()
        composition = await _composition(run_control)
        composition.model.stable_output_ref = False
        run_id = await admit_goal_run(run_control, "rrm-019-distinct-output-refs")
        family_worker, cognitive_worker = _workers(environment, composition)
        async with family_worker, cognitive_worker:
            try:
                result = await environment.client.execute_workflow(
                    GoalDirectedWorkflow.run,
                    goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
                    id=f"family/{run_id}/1",
                    task_queue=QUEUE,
                )
            except WorkflowFailureError as failure:
                # Only the RRM-019 rejection is the expected failure; anything else fails.
                assert "terminal_output_mismatch" in str(failure.cause), failure.cause
                raise
        assert result.goal_iterations == 2
