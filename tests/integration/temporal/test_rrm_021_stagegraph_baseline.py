"""RRM-021: a StageGraph run releases its admitted baseline reservation before it terminalizes.

REQ-CP-RUN-006: reservations are released or settled before terminalization, and the reducer
rejects a terminal proposal while one remains (`budget_not_settled`). The family calls the
run-control `stagegraph.settle_baseline` activity (the real `StageGraphDecisionService`
over the real run control) before it proposes terminalization, gated by
`rrm-021-settle-stagegraph-baseline`; the terminal proposal here is decided by the real
reducer, and the fixture refuses to terminalize while `baseline` is still reserved, so only
the workflow's own settlement can make the run terminal.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import (
    RELEASE_BASELINE_ON_BLOCKED_PATCH,
    SETTLE_BASELINE_PATCH,
    StageGraphWorkflow,
)
from mission_control.application.programs.service import StageGraphDecisionService
from mission_control.domain.coordinator.launch import BlueprintFamily
from mission_control.domain.policies.contracts import RunOutcome, RunPhase
from mission_control.domain.programs.contracts import (
    DependencyDisposition,
    StageGraphBaselineSettlementRequest,
    StageGraphBaselineSettlementResult,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
)
from tests.fixtures.temporal_history import patch_ids
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    ROOT_WORKFLOWS,
    SCOPE,
    Authority,
    _submitter,
    replay,
    until,
)
from tests.integration.temporal.test_rrm_008_family_cancellation import (
    BASELINE,
    CancellableStageGraphActivities,
    _await_completion,
    _cancel,
    terminalize_through_run_control,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_input


def _environment() -> Any:
    try:
        return WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:  # pragma: no cover - environment-dependent
        pytest.skip(f"Temporal test server is unavailable: {error}")


class BaselineSettlingActivities(CancellableStageGraphActivities):
    """The RRM-008 harness whose baseline is released by the production settlement service."""

    def __init__(self, authority: Authority) -> None:
        super().__init__(authority)
        self._decisions = StageGraphDecisionService(authority.run_control, authority.repository)
        self.settlements: list[StageGraphBaselineSettlementResult] = []
        self.baseline_at_complete: list[bool] = []
        self.block_after_downstream = False

    @activity.defn(name="stagegraph.decide_result")
    async def decide(
        self, request: StageGraphResultActivityRequest
    ) -> StageGraphResultActivityResult:
        result = await super().decide(request)
        if self.block_after_downstream and request.observation.identity.stage_id == "downstream":
            # Every required dependency stays unresolved after the last stage: the family has
            # no admissible work and no terminal completion proposal.
            projection = replace(
                result.projection,
                dependencies={
                    key: replace(item, disposition=DependencyDisposition.UNRESOLVED)
                    for key, item in result.projection.dependencies.items()
                },
            )
            return replace(result, projection=projection)
        return result

    @activity.defn(name="stagegraph.settle_baseline")
    async def settle_baseline(
        self, request: StageGraphBaselineSettlementRequest
    ) -> StageGraphBaselineSettlementResult:
        result = await self._decisions.settle_baseline(request)
        self.settlements.append(result)
        return result

    @activity.defn(name="stagegraph.complete")
    async def complete(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        budget = await self.authority.run_control.get_budget(SCOPE, request.run_id)
        self.baseline_at_complete.append("baseline" in budget.reservations)
        assert "baseline" not in budget.reservations, "the family settles before proposing"
        # The fixture operations record no output evidence in run control, so the reducer's
        # authoritative output set is empty (`terminal_output_mismatch` otherwise).
        request = replace(request, proposal=replace(request.proposal, valid_output_refs=()))
        return await terminalize_through_run_control(self.authority.run_control, request)

    @property
    def functions(self) -> list[object]:
        return [*super().functions, self.settle_baseline]


async def _admitted_with_baseline(authority: Authority, request_id: str) -> str:
    run_id = await authority.admit(request_id)
    budget = await authority.run_control.get_budget(SCOPE, run_id)
    assert budget.reservations == {"baseline": BASELINE}
    assert budget.reserved == BASELINE
    return run_id


def _stage_input(run_id: str, **overrides: Any) -> Any:
    return replace(
        stage_input(_blueprint()), run_id=run_id, baseline_reservation=dict(BASELINE), **overrides
    )


@pytest.mark.asyncio
async def test_stagegraph_with_a_baseline_completes_with_the_budget_settled() -> None:
    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = BaselineSettlingActivities(authority)
        run_id = await _admitted_with_baseline(authority, "rrm-021-stagegraph-complete")
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=cast(Any, activities.functions),
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _stage_input(run_id),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )
            await asyncio.wait_for(activities.slow_started.wait(), timeout=60)
            activities.slow_release.set()
            activities.fast_release.set()
            await handle.result()
            history = await handle.fetch_history()
            runs = await replay(handle, [StageGraphWorkflow, OperationWorkflow])

        run = await authority.run(run_id)
        budget = await authority.run_control.get_budget(SCOPE, run_id)
        assert run.phase == RunPhase.TERMINAL and run.terminal_outcome == RunOutcome.COMPLETED
        assert not any(budget.reserved.values()), budget.reserved
        assert not any(budget.pending_settlement.values())
        assert "baseline" not in budget.reservations
        assert budget.consumed.get("tokens.total", 0) == 0, "released, never double counted"
        assert activities.baseline_at_complete == [False]
        assert [item.accepted for item in activities.settlements] == [True]
        assert SETTLE_BASELINE_PATCH in patch_ids(history)
        assert runs == 1

        # Idempotent: the same settlement again changes nothing (an Activity retry, a
        # continued segment) and reports the current run version.
        decisions = StageGraphDecisionService(authority.run_control, authority.repository)
        repeated = await decisions.settle_baseline(
            StageGraphBaselineSettlementRequest(
                run_id=run_id,
                request_scope=SCOPE,
                occurred_at=datetime.now(UTC),
                idempotency_issuer="rrm-021",
                correlation_id="rrm-021:repeat",
                baseline_reservation=dict(BASELINE),
            )
        )
        assert repeated.accepted and repeated.resulting_run_version == run.version
        assert await authority.run_control.get_budget(SCOPE, run_id) == budget


@pytest.mark.asyncio
async def test_cancelled_stagegraph_with_a_baseline_releases_it_and_terminalizes_cancelled() -> (
    None
):
    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = BaselineSettlingActivities(authority)
        run_id = await _admitted_with_baseline(authority, "rrm-021-stagegraph-cancel")
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=cast(Any, activities.functions),
        ):
            submitted = await _submitter(environment.client).submit(
                _stage_input(run_id),
                workflow_id="ignored",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            family = environment.client.get_workflow_handle(f"family/{run_id}/1")
            await asyncio.wait_for(activities.slow_started.wait(), timeout=60)
            await _cancel(authority, run_id)
            await until(lambda: _terminal(authority, run_id), seconds=120)
            result = await _await_completion(root, seconds=120)
            family_history = await family.fetch_history()
            family_runs = await replay(family, [StageGraphWorkflow, OperationWorkflow])

        run = await authority.run(run_id)
        budget = await authority.run_control.get_budget(SCOPE, run_id)
        assert result["completion_proposal"]["cancelled"] is True
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert not any(budget.reserved.values()), budget.reserved
        assert "baseline" not in budget.reservations
        assert budget.consumed.get("tokens.total", 0) == 0
        assert [item.accepted for item in activities.settlements] == [True]
        assert SETTLE_BASELINE_PATCH in patch_ids(family_history)
        assert family_runs == 1


async def _terminal(authority: Authority, run_id: str) -> bool:
    return (await authority.run(run_id)).phase == RunPhase.TERMINAL


@pytest.mark.asyncio
async def test_a_baseline_that_differs_from_the_admitted_reservation_is_refused() -> None:
    async with await _environment() as environment:
        authority = Authority(environment.client)
        run_id = await _admitted_with_baseline(authority, "rrm-021-stagegraph-mismatch")
        decisions = StageGraphDecisionService(authority.run_control, authority.repository)
        with pytest.raises(ValueError, match="differs from the admitted"):
            await decisions.settle_baseline(
                StageGraphBaselineSettlementRequest(
                    run_id=run_id,
                    request_scope=SCOPE,
                    occurred_at=datetime.now(UTC),
                    idempotency_issuer="rrm-021",
                    correlation_id="rrm-021:mismatch",
                    baseline_reservation={"tokens.total": 21},
                )
            )
        budget = await authority.run_control.get_budget(SCOPE, run_id)
        assert budget.reservations == {"baseline": BASELINE}


@pytest.mark.asyncio
async def test_a_blocked_stagegraph_releases_its_baseline_before_it_fails() -> None:
    """The family fails `stagegraph_blocked` (a required dependency never resolves) without
    proposing terminalization: the baseline is released first, so nothing stays reserved."""

    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = BaselineSettlingActivities(authority)
        activities.block_after_downstream = True
        run_id = await _admitted_with_baseline(authority, "rrm-021-stagegraph-blocked")
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=cast(Any, activities.functions),
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _stage_input(run_id),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )
            activities.fast_release.set()
            activities.slow_release.set()
            with pytest.raises(WorkflowFailureError) as failure:
                await asyncio.wait_for(handle.result(), timeout=120)
            history = await handle.fetch_history()
            runs = await replay(handle, [StageGraphWorkflow, OperationWorkflow])

        cause = failure.value.cause
        assert isinstance(cause, ApplicationError) and cause.type == "stagegraph_blocked"
        budget = await authority.run_control.get_budget(SCOPE, run_id)
        assert not any(budget.reserved.values()), budget.reserved
        assert "baseline" not in budget.reservations
        assert budget.consumed.get("tokens.total", 0) == 0
        assert [item.accepted for item in activities.settlements] == [True]
        assert activities.baseline_at_complete == [], "never proposed terminalization"
        assert RELEASE_BASELINE_ON_BLOCKED_PATCH in patch_ids(history)
        assert SETTLE_BASELINE_PATCH not in patch_ids(history)
        assert runs == 1
