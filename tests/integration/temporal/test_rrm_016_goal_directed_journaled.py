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
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from temporalio import activity
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.exceptions import ApplicationError
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
from app.domain.run_control.contracts import (
    CancelAction,
    CommandStatus,
    EffectDisposition,
    RecordUsageAction,
    ReserveBudgetAction,
    RunOutcome,
    RunPhase,
)
from app.integrations.artifact_payloads import InMemoryArtifactPayloadStore
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.goal_directed import (
    JOURNALED_SETTLEMENT_PATCH,
    STALE_VERSION_RETRY_PATCH,
    VERIFIED_TERMINAL_OUTPUTS_PATCH,
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
from tests.unit.run_control.test_run_control import command

QUEUE = "rrm016-goal-directed"
# Captured from `test_journaled_goal_directed_run_settles_each_operation_once_through_a_pause`
# with `RRM016_CAPTURE_HISTORY_DIR` set: the journaled family through a pause and resume.
POST_CHANGE = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm016_post_change"
POST_CHANGE_HISTORY = "goal_directed_journaled_pause_resume.run1.json"
# Captured from `test_outside_commands_between_settlement_and_next_command_do_not_fail_the_run`
# with `RRM016_CAPTURE_HISTORY_DIR` set: two re-admissions and one lifecycle retry.
STALE_RETRY_HISTORY = "goal_directed_stale_version_retry.run1.json"
PRE_CHANGE = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm007_pre_change"
# Captured from `test_iterations_with_distinct_output_refs_terminalize` with
# `RRM019_CAPTURE_HISTORY_DIR` set: distinct output refs, the verified final output promoted.
RRM019_POST_CHANGE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm019_post_change"
)
RRM019_HISTORY = "goal_directed_distinct_output_refs.run1.json"
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


@pytest.mark.asyncio
async def test_iterations_with_distinct_output_refs_terminalize() -> None:
    """RRM-019: iterations produce different output refs. The run completes and promotes
    exactly the outputs its terminalization proposal names: the final executor's outputs that
    the accepting verifier admitted, not iteration 1's (rejected) output."""

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
            handle = await environment.client.start_workflow(
                GoalDirectedWorkflow.run,
                goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )
            result = await handle.result()
            history = await handle.fetch_history()
        assert result.goal_iterations == 2
        # Every iteration's immutable outputs remain in the family result (lineage) ...
        assert result.output_refs == ("artifact:rrm016:1", "artifact:rrm016:2")
        [first_verification, final_verification] = result.verification_results
        assert first_verification.decision != "accepted"
        assert final_verification.decision == "accepted"
        assert final_verification.admitted_executor_output_refs == ("artifact:rrm016:2",)
        # ... but the proposal and the promoted evidence name the verified final outputs only.
        assert result.terminalization_proposal is not None
        assert result.terminalization_proposal.output_refs == ("artifact:rrm016:2",)
        run = await run_control.get_run(SCOPE, run_id)
        assert run.terminal_outcome == RunOutcome.COMPLETED
        assert [item.output_ref for item in run.accepted_output_evidence] == [
            "artifact:rrm016:2"
        ]
        assert [item for item in lifecycle_command_ids(history) if item.startswith("goal:output:")]
        assert all(
            item.startswith("goal:output:artifact:rrm016:2:")
            for item in lifecycle_command_ids(history)
            if item.startswith("goal:output:")
        )
        assert VERIFIED_TERMINAL_OUTPUTS_PATCH in patch_ids(history)
        assert await replay(handle, [GoalDirectedWorkflow, OperationWorkflow]) == 1
        capture = os.getenv("RRM019_CAPTURE_HISTORY_DIR")
        if capture:  # re-capture the committed post-change fixture (see RRM019_POST_CHANGE)
            _write_history(Path(capture) / RRM019_HISTORY, history.to_json())


@pytest.mark.asyncio
async def test_rrm_019_distinct_outputs_history_replays() -> None:
    """The captured RRM-019 history (two iterations, distinct output refs, the verified final
    output promoted) replays; later tickets that change the family must keep it replaying."""

    history = WorkflowHistory.from_json(
        "family/rrm-019-post-change/1", _read_history(RRM019_POST_CHANGE / RRM019_HISTORY)
    )
    assert VERIFIED_TERMINAL_OUTPUTS_PATCH in patch_ids(history)
    assert [
        item.split(":")[2:5]
        for item in lifecycle_command_ids(history)
        if item.startswith("goal:output:")
    ] == [["artifact", "rrm016", "2"]]
    await Replayer(
        workflows=[GoalDirectedWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    ).replay_workflow(history)


# --- Review fix 2: an outside command between a settlement read and the next command ------


class _OutsideCommandAfterReconciliation:
    """The production family activities, with an outside run-control command issued right
    after each reconciliation: the version the family read is stale for its next command."""

    def __init__(
        self,
        family: Any,
        act: Callable[[GoalOperationReconciliationRequest], Awaitable[None]],
    ) -> None:
        self._family = family
        self._act = act

    @activity.defn(name="goaldirected.reconcile_operation")
    async def prepare_handoff(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        result = await self._family.prepare_handoff(request)
        await self._act(request)
        return cast(GoalOperationReconciliationResult, result)

    @property
    def functions(self) -> list[Any]:
        family = self._family
        return [
            family.execute_iteration,
            family.verify_iteration,
            self.prepare_handoff,
            family.apply_lifecycle_command,
            family.materialize_workflow_result,
            family.apply_boundary_command,
        ]


async def _bump(run_control: Any, run_id: str, tag: str) -> None:
    """Two harmless outside facts: reserve one token, then release it (no usage)."""

    run = await run_control.get_run(SCOPE, run_id)
    reserved = await run_control.execute(
        command(
            run_id,
            run.version,
            f"outside-reserve:{tag}",
            ReserveBudgetAction(reservation_id=f"outside:{tag}", amounts={"tokens.total": 1}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED, reserved.reason_code
    released = await run_control.execute(
        command(
            run_id,
            reserved.resulting_run_version,
            f"outside-release:{tag}",
            RecordUsageAction(
                usage_id=f"outside:{tag}",
                actual_amounts={},
                reservation_id=f"outside:{tag}",
                release_amounts={"tokens.total": 1},
            ),
        )
    )
    assert released.status == CommandStatus.ACCEPTED, released.reason_code


async def _run_with_outside_commands(
    act: Callable[[Any, str, GoalOperationReconciliationRequest], Awaitable[None]],
    request_id: str,
) -> tuple[Any, Any, str, WorkflowHistory, BaseException | None]:
    environment = await WorkflowEnvironment.start_time_skipping()
    async with environment:
        run_control = goal_run_control()
        composition = await _composition(run_control)
        run_id = await admit_goal_run(run_control, request_id)

        async def outside(request: GoalOperationReconciliationRequest) -> None:
            await act(run_control, run_id, request)

        family = _OutsideCommandAfterReconciliation(composition.family, outside)
        failure: BaseException | None = None
        async with (
            Worker(
                environment.client,
                task_queue=QUEUE,
                workflows=[GoalDirectedWorkflow, OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
                activities=family.functions,
            ),
            Worker(
                environment.client,
                task_queue=composition.binding.task_queue,
                activities=agent_cognitive_activities(
                    OperationExecutionActivities(composition.service, worker_identity="rrm016")
                ),
            ),
        ):
            handle = await environment.client.start_workflow(
                GoalDirectedWorkflow.run,
                goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )
            try:
                await handle.result()
            except WorkflowFailureError as error:
                failure = error
            history = await handle.fetch_history()
        await Replayer(
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)
        return run_control, composition, run_id, history, failure


def _admission_attempts(history: WorkflowHistory) -> list[tuple[str, int, int]]:
    return sorted(
        (str(item["operation_role"]), int(item["goal_iteration"]), int(item["admission_attempt"]))
        for name in ("goaldirected.prepare_executor", "goaldirected.prepare_verifier")
        for item in scheduled_activity_inputs(history, name)
    )


@pytest.mark.asyncio
async def test_outside_commands_between_settlement_and_next_command_do_not_fail_the_run() -> None:
    """Every settlement read but one is followed by an outside command, so each next family
    command is stale once: two operation admissions and the first terminal lifecycle fact.
    Each is retried once at the version the stale result reported; the run completes."""

    async def bump(run_control: Any, run_id: str, request: Any) -> None:
        iteration = request.claim.identity.iteration.goal_iteration
        if (iteration, request.operation_role) != (2, "executor"):
            await _bump(run_control, run_id, f"{iteration}:{request.operation_role}")

    run_control, composition, run_id, history, failure = await _run_with_outside_commands(
        bump, "rrm-016-outside-commands"
    )
    assert failure is None, failure
    capture = os.getenv("RRM016_CAPTURE_HISTORY_DIR")
    if capture:  # re-capture the committed stale-retry fixture (see STALE_RETRY_HISTORY)
        _write_history(Path(capture) / STALE_RETRY_HISTORY, history.to_json())
    run = await run_control.get_run(SCOPE, run_id)
    budget = await run_control.get_budget(SCOPE, run_id)
    assert run.terminal_outcome == RunOutcome.COMPLETED
    assert STALE_VERSION_RETRY_PATCH in patch_ids(history)
    # The iteration-1 verifier and the iteration-2 executor were re-admitted (attempt 2).
    assert _admission_attempts(history) == sorted(
        [
            ("executor", 1, 1),
            ("executor", 2, 1),
            ("executor", 2, 2),
            ("verifier", 1, 1),
            ("verifier", 1, 2),
            ("verifier", 2, 1),
        ]
    )
    # The first terminal fact after the iteration-2 verifier was retried at the new version.
    retried = [item for item in lifecycle_command_ids(history) if ":at-version:" in item]
    assert len(retried) == 1 and retried[0].startswith("goal:obligation:"), retried
    # Each operation still settled once; the outside facts consumed nothing.
    operation_usage = [item for item in budget.usage_records.values() if item.authority_ref]
    assert len(operation_usage) == 4
    assert budget.consumed.get("tokens.total") == 4 * TOKENS_PER_OPERATION
    assert not any(budget.reserved.values())
    assert len(composition.model.turns) == 8


@pytest.mark.asyncio
async def test_outside_cancel_enters_the_cancellation_boundary_not_a_lifecycle_failure() -> None:
    """An API cancel accepted between the executor's settlement and the verifier admission:
    the stale admission reports `cancelling`; the family does not retry, does not issue a
    second cancel, and stops at its cancellation boundary (`goal_cancelling`, RRM-008's seam)."""

    async def cancel(run_control: Any, run_id: str, request: Any) -> None:
        if request.operation_role != "executor":
            return
        run = await run_control.get_run(SCOPE, run_id)
        cancelled = await run_control.execute(
            command(run_id, run.version, "operator-cancel", CancelAction())
        )
        assert cancelled.status == CommandStatus.ACCEPTED, cancelled.reason_code

    run_control, _unused, run_id, history, failure = await _run_with_outside_commands(
        cancel, "rrm-016-outside-cancel"
    )
    assert isinstance(failure, WorkflowFailureError)
    assert isinstance(failure.cause, ApplicationError)
    assert failure.cause.type == "goal_cancelling", failure.cause
    assert (await run_control.get_run(SCOPE, run_id)).phase == RunPhase.CANCELLING
    assert STALE_VERSION_RETRY_PATCH in patch_ids(history)
    # No re-admission, and no second cancel from the family.
    assert _admission_attempts(history) == [("executor", 1, 1), ("verifier", 1, 1)]
    assert not [item for item in lifecycle_command_ids(history) if item.endswith(":cancel")]


@pytest.mark.asyncio
async def test_stale_version_retry_history_replays() -> None:
    """The captured stale-retry history replays; RRM-008 changes this path and must keep it
    replaying."""

    history = WorkflowHistory.from_json(
        "family/rrm-016-stale-retry/1", _read_history(POST_CHANGE / STALE_RETRY_HISTORY)
    )
    assert {JOURNALED_SETTLEMENT_PATCH, STALE_VERSION_RETRY_PATCH} <= patch_ids(history)
    assert [item[2] for item in _admission_attempts(history)].count(2) == 2
    await Replayer(
        workflows=[GoalDirectedWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    ).replay_workflow(history)
