"""RRM-016: crash windows of a GoalDirected unit converge to one settlement (RRM-004 harness).

The RRM-004 crash-window harness (real `create_deep_agent` graph, journaled coordinator,
checkpoint lineage, the real run-control authority) runs a GoalDirected executor unit that
the real GoalDirected preparer admitted. A worker is lost before the authority settlement
(after the fenced result and the effect observation path began), or the settled unit is
redelivered. Each window converges to exactly one run-control settlement, one usage record
and two model calls; the family then consumes that one settlement.
"""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.execution.operations.operation_execution import (
    operation_settlement_id,
)
from mission_control.domain.policies.contracts import CommandStatus, EffectDisposition
from tests.fixtures.checkpoint_recovery import (
    SimulatedWorkerCrash,
    governed_workspace,
    recovery_harness,
    result_digest,
)
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    TOKENS_PER_OPERATION,
    GoalScriptedModel,
    RecordingGoalDocuments,
    admit_goal_run,
    goal_run_control,
    goal_start_action,
    goal_templates,
    governed_result_service,
    preparer,
)
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import (
    _claim,
    _preparation,
    _reconciliation,
)
from tests.unit.run_control.test_run_control import command


class _Templates:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    async def get_template(self, *, operation_role: str, **_: object) -> Any:
        return self._values[operation_role]


async def _goal_unit(window: str) -> tuple[Any, Any, Any, str]:
    """A started GoalDirected run on the harness, and its prepared executor operation."""

    run_control = goal_run_control()
    model = GoalScriptedModel()
    harness = await recovery_harness(model=model, real_authority=True, run_control=run_control)
    # The harness admitted and started its own run; the GoalDirected unit uses a run whose
    # budget bounds the iteration reservation.
    run_id = await admit_goal_run(run_control, f"rrm-016-recovery-{window}")
    started = await run_control.execute(
        command(run_id, 1, f"start-{window}", goal_start_action(run_id))
    )
    assert started.status == CommandStatus.ACCEPTED
    documents = RecordingGoalDocuments()
    claim = await _claim(run_id)
    dispatch = await preparer(
        run_control=run_control,
        # The harness authority's compiled contract is the `/workspace/output` slot.
        templates=_Templates(goal_templates(harness.binding, workspace=governed_workspace)),
        bindings=harness.service._bindings,
        documents=documents,
    ).prepare(_preparation(run_id, claim, "executor", 2, 0))
    return harness, dispatch, claim, run_id


async def _consume(harness: Any, dispatch: Any, claim: Any, result: Any) -> Any:
    reconciled = await governed_result_service(
        RecordingGoalDocuments(), harness.run_control, harness.service._bindings
    ).reconcile(_reconciliation(claim, "executor", dispatch, result))
    assert reconciled.settlement is not None
    return reconciled.settlement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("window", "crash_after_checkpoint"),
    [
        ("after_intermediate_checkpoint", 6),
        ("before_settlement", None),
        ("after_settlement", None),
    ],
)
async def test_goal_directed_unit_crash_windows_converge_to_one_settlement(
    window: str, crash_after_checkpoint: int | None
) -> None:
    harness, dispatch, claim, run_id = await _goal_unit(window)
    operation = dispatch.workflow_request.operation
    assert operation.runtime_unit is not None
    assert operation.runtime_unit.unit_kind == "goal_executor"
    assert operation.workspace.exclusive_write_paths == ("/goal/1/executor/workspace/output",)

    if crash_after_checkpoint is not None:
        harness.saver.crash_after(crash_after_checkpoint)
        await harness.crash(operation)
        result = await harness.run(operation)
    elif window == "before_settlement":
        harness.crashable.crash_before_settlement = True
        await harness.crash(operation)
        result = await harness.run(operation)
    else:
        first = await harness.run(operation)
        result = await harness.run(operation)  # redelivery after settlement
        assert result == first
    assert result.status == "completed"
    assert result.usage.amounts == {"tokens.total": TOKENS_PER_OPERATION}
    assert result.structured_output is not None
    assert result.structured_output["schema_version"] == ("belllabs.goal-executor-observation.v1")

    # Two model calls in total, each seeing the operation's input exactly once.
    assert harness.model.calls == [(1, 0), (1, 1)]
    binding_id = dispatch.operation_binding_ref
    settlement_id = operation_settlement_id(binding_id)
    budget = await harness.run_control.get_budget(SCOPE, run_id)
    effects = await harness.run_control.get_effects(SCOPE, run_id)
    run = await harness.run_control.get_run(SCOPE, run_id)
    assert list(budget.usage_records) == [settlement_id]
    assert budget.consumed.get("tokens.total") == TOKENS_PER_OPERATION
    assert claim.reservation_id not in budget.reservations
    [effect] = effects.claims.values()
    assert (effect.operation_ref, effect.disposition) == (binding_id, EffectDisposition.SUCCEEDED)
    assert [item.settlement_id for item in run.accepted_operation_settlement_evidence] == [
        settlement_id
    ]
    [journal_settlement] = harness.journal.settlements.values()
    assert journal_settlement.settlement_id == settlement_id

    # The family consumes exactly that settlement, and a repeated consumption is the same.
    consumed = await _consume(harness, dispatch, claim, result)
    again = await _consume(harness, dispatch, claim, result)
    assert consumed == again
    assert consumed.settlement_id == settlement_id
    assert consumed.usage == {"tokens.total": TOKENS_PER_OPERATION}
    assert consumed.settled_run_version == run.version
    assert list((await harness.run_control.get_budget(SCOPE, run_id)).usage_records) == [
        settlement_id
    ]
    print(
        f"RRM-016 EVIDENCE crash window {window}: model_calls={harness.model.calls} "
        f"technical_attempts={list(harness.journal.technical_attempts.values())} "
        f"settlements=1 usage_records=1 digest={result_digest(result)[:19]}"
    )


@pytest.mark.asyncio
async def test_crash_injection_is_real_for_the_before_settlement_window() -> None:
    """Guard: the before-settlement crash happens after the fenced result is recorded."""

    harness, dispatch, _claim, _run_id = await _goal_unit("guard")
    harness.crashable.crash_before_settlement = True
    with pytest.raises(SimulatedWorkerCrash):
        await harness.service.execute(
            dispatch.workflow_request.operation,
            harness.attempt(dispatch.workflow_request.operation),
        )
    assert harness.journal.settlements == {}
    assert len(harness.model.calls) == 2
