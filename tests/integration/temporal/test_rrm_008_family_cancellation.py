"""RRM-008: the cancellation saga at both family boundaries (time-skipping Temporal).

Every cancel enters through the run-control facade (journal first), is delivered root-first
through the boundary-command ledger in the `cancel` space (`accepted -> delivered`, the
root's `deliver_cancel` then the family's), reaches the active `OperationWorkflow` and its
cognitive Activity, and the family completes the saga as workflow logic: it consumes the
cancelled unit's settlement, releases its reservations, rejects superseded commands and
proposes terminal `cancelled`, which the reducer records (`applied`). Both families, the
root result, the receipts and every captured history (root and family) are checked.

GoalDirected runs on the governed composition (real `create_deep_agent` cognition, the real
run-control authority, journal and lineage) so budgets and effects are real liabilities;
StageGraph runs on the RRM-007 governed harness (real run control, fixture operations).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from temporalio import activity
from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    operation_settlement_id,
)
from app.application.orchestration.service import (
    RunControlLifecycleGateway,
    orchestration_lifecycle_actor,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import GoalDirectedBlueprint
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
)
from app.domain.orchestration.goal_directed_runtime import (
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import (
    CancelAction,
    CommandStatus,
    EffectDisposition,
    LifecycleCommand,
    RecordUsageAction,
    RunOutcome,
    RunPhase,
    SatisfyWaitAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from app.integrations.artifact_payloads import InMemoryArtifactPayloadStore
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import GOVERNED_CANCEL_PATCH, BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import (
    CANCELLATION_SAGA_PATCH as GOAL_CANCELLATION_PATCH,
)
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import (
    CANCELLATION_SAGA_PATCH as STAGEGRAPH_CANCELLATION_PATCH,
)
from app.temporal.workflows.stagegraph import StageGraphWorkflow, wait_condition_id
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    ExactLifecycleBinding,
    GoalComposition,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
)
from tests.fixtures.temporal_history import patch_ids, scheduled_activity_inputs
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    GOAL_QUEUE,
    ROOT_WORKFLOWS,
    Authority,
    GovernedGoalActivities,
    GovernedStageGraphActivities,
    _goal_blueprint,
    _has_wait,
    _phase_is,
    _state_is,
    _submitter,
    pause,
    replay,
    until,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_input
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as goal_input
from tests.integration.temporal.test_wp_bp_020_temporal import fake_settlement

BASELINE = {"tokens.total": 20}
GOAL_COMPOSITION_QUEUE = "rrm008-goal-directed"


def _environment() -> Any:
    try:
        return WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:  # pragma: no cover - environment-dependent
        pytest.skip(f"Temporal test server is unavailable: {error}")


# --- fixture operation boundaries that honour the saga -----------------------------------------


class CancellableStageGraphActivities(GovernedStageGraphActivities):
    """The RRM-007 StageGraph harness plus the fixture `operation.cancel` of a cancelled unit."""

    def __init__(self, authority: Authority) -> None:
        super().__init__(authority, gate_fast_on_slow=False)
        self.cancel_settlements: list[str] = []
        self.fast_release = asyncio.Event()
        self.fast_cancelled = asyncio.Event()

    @activity.defn(name="operation.execute")
    async def execute_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        if ":stage:fast:" in operation_id:
            # Held by the test so that the join never releases before the cancel lands.
            try:
                await self.fast_release.wait()
            except asyncio.CancelledError:
                self.fast_cancelled.set()
                raise
            return {"output_refs": ["artifact:fast"]}
        if ":stage:slow:" in operation_id:
            self.slow_started.set()
            try:
                await self.slow_release.wait()
            except asyncio.CancelledError:
                # The cancel reached the running Activity (heartbeat or worker shutdown).
                self.slow_cancelled.set()
                raise
            self.slow_completed.set()
            return {"output_refs": ["artifact:slow"]}
        return await super().execute_operation(request)

    @activity.defn(name="operation.cancel")
    async def cancel_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        self.cancel_settlements.append(operation_id)
        return {
            "binding_id": f"binding:{operation_id}",
            "semantic_attempt_key": str(request["identity"]),
            "status": "cancelled",
            "failure_code": "cancelled",
        }

    @activity.defn(name="stagegraph.complete")
    async def complete(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        """The terminal proposal through the real run control (the fixture operations hold
        no reservations; the fixture admission's baseline is released here first)."""

        return await terminalize_through_run_control(self.authority.run_control, request)

    @property
    def functions(self) -> list[object]:
        return [*super().functions, self.cancel_operation]


async def terminalize_through_run_control(
    run_control: Any, request: StageGraphCompletionActivityRequest
) -> StageGraphCompletionActivityResult:
    scope, run_id = request.request_scope, request.run_id
    run = await run_control.get_run(scope, run_id)
    budget = await run_control.get_budget(scope, run_id)
    if "baseline" in budget.reservations:
        released = await run_control.execute(
            LifecycleCommand(
                command_id=f"stagegraph:{run_id}:release-baseline",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=scope,
                run_id=run_id,
                expected_run_version=run.version,
                actor=orchestration_lifecycle_actor(),
                action=RecordUsageAction(
                    usage_id="stagegraph-usage:baseline",
                    reservation_id="baseline",
                    actual_amounts={},
                    release_amounts=dict(budget.reservations["baseline"]),
                ),
                reason="Release the fixture baseline before the terminal proposal",
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            )
        )
        assert released.status == CommandStatus.ACCEPTED, released.reason_code
        run = await run_control.get_run(scope, run_id)
        budget = await run_control.get_budget(scope, run_id)
    effects = await run_control.get_effects(scope, run_id)
    evidence_payload = [
        item.model_dump(mode="json")
        for item in sorted(run.accepted_obligation_evidence, key=lambda item: item.obligation_ref)
    ]
    accepted_refs = {item.obligation_ref for item in run.accepted_obligation_evidence}
    proposal = TerminalizationProposal(
        proposal_id=f"stagegraph-fixture:{run_id}:v{run.version}",
        expected_run_version=run.version,
        workflow_type_digest=run.workflow_type_ref.digest,
        obligation_revision=run.obligation_revision,
        evidence_frontier_digest=run.evidence_frontier_digest,
        accepted_obligation_evidence_digest=sha256_digest(evidence_payload),
        proposing_execution_binding_ref="stagegraph-fixture",
        required_obligations_accepted=run.required_obligation_refs <= accepted_refs,
        valid_output_refs=request.proposal.valid_output_refs,
        cancellation_settled=request.proposal.cancelled,
        budget_settled=(
            not any(budget.reserved.values()) and not any(budget.pending_settlement.values())
        ),
        effects_settled=all(claim.settlement is not None for claim in effects.claims.values()),
        pending_wait_or_link_ids=(),
        proposed_at=request.occurred_at,
        finalization_plan=run.finalization_plan,
        output_omission_reason=run.finalization_omission_reason,
    )
    result = await run_control.execute(
        LifecycleCommand(
            command_id=f"stagegraph:{run_id}:terminalize:v{run.version}",
            idempotency_issuer=request.idempotency_issuer,
            request_scope=scope,
            run_id=run_id,
            expected_run_version=run.version,
            actor=orchestration_lifecycle_actor(),
            action=TerminalizeAction(proposal=proposal),
            reason="Fixture StageGraph terminal proposal through run control",
            occurred_at=request.occurred_at,
            correlation_id=request.correlation_id,
        )
    )
    return StageGraphCompletionActivityResult(
        accepted=result.status == CommandStatus.ACCEPTED,
        terminal_outcome=result.terminal_outcome,
        resulting_run_version=result.resulting_run_version,
        reason_code=result.reason_code,
    )


class CancellableGoalActivities(GovernedGoalActivities):
    """The RRM-007 GoalDirected harness with a cancellable fixture operation boundary and
    every lifecycle command routed through the real run control (the saga's terminal
    proposal is decided by the reducer, not by a fixture)."""

    def __init__(
        self, authority: Authority, blueprint: GoalDirectedBlueprint, **kwargs: Any
    ) -> None:
        super().__init__(authority, **kwargs)
        self.executor_cancelled = asyncio.Event()
        self.cancel_settlements: list[str] = []
        self._gateway = RunControlLifecycleGateway(
            authority.run_control,
            ExactLifecycleBinding(blueprint),
            orchestration_lifecycle_actor(),
        )

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def lifecycle(self, request: LifecycleCommandRequest) -> LifecycleCommandOutcome:
        self.lifecycle_kinds.append(str(request.action["kind"]))
        return await self._gateway.execute(request)

    @activity.defn(name="operation.execute")
    async def execute_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        if operation_id.endswith("/1/executor"):
            self.executor_started.set()
            try:
                await self.release_executor.wait()
            except asyncio.CancelledError:
                self.executor_cancelled.set()
                raise
        self.operation_started.set()
        return {"operation_id": str(request["identity"])}

    @activity.defn(name="operation.cancel")
    async def cancel_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        self.cancel_settlements.append(operation_id)
        return {
            "binding_id": f"binding:{request['identity']['operation_id'].split('/')[-1]}",
            "semantic_attempt_key": str(request["identity"]),
            "status": "cancelled",
            "failure_code": "cancelled",
        }

    @activity.defn(name="goaldirected.reconcile_operation")
    async def reconcile(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        observed = request.operation_result
        if observed.disposition == "cancelled":
            return GoalOperationReconciliationResult(
                operation_role=request.operation_role,
                detail_ref=f"goal-operation:cancelled:{request.operation_binding_ref}",
                settlement=fake_settlement(request, {}),
                operation_disposition="cancelled",
            )
        return await super().reconcile(request)

    @property
    def functions(self) -> list[object]:
        return [*super().functions, self.cancel_operation]


async def _cancel(authority: Authority, run_id: str, command_id: str = "cancel") -> Any:
    accepted = await authority.intervene(run_id, command_id, CancelAction())
    assert accepted.phase == RunPhase.CANCELLING
    status = await authority.run_control.get_boundary_command(SCOPE, run_id, "operator", command_id)
    assert status is not None
    if status.command.target.kind != "run_control":
        assert status.command.target.sequence_space == "cancel"
    return status


async def _await_completion(handle: WorkflowHandle[Any, Any], *, seconds: float) -> Any:
    """Wait for the execution to close, then read its result (the time-skipping server's
    long poll can return without a close event while a workflow is still running)."""

    async def closed() -> bool:
        description = await handle.describe()
        return description.status is not None and description.status.name != "RUNNING"

    await until(closed, seconds=seconds)
    return await handle.result()


async def _ledger(authority: Authority, run_id: str, command_id: str) -> list[tuple[str, str]]:
    status = await authority.run_control.get_boundary_command(SCOPE, run_id, "operator", command_id)
    assert status is not None
    return [(item.state.value, item.recorded_by) for item in status.receipts]


# --- StageGraph ----------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stagegraph_cancel_reaches_the_running_sibling_and_terminalizes_cancelled() -> None:
    """Window: during tool/model work of a StageGraph unit. The cancel is journaled, delivered
    root-first, interrupts the running `slow` Activity (CancelledError), settles that unit
    through `operation.cancel`, cancels the never-admitted `downstream` dependency, and the
    reducer records `cancelled` (`applied`). The root returns the cancelled family result."""

    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = CancellableStageGraphActivities(authority)
        run_id = await authority.admit("rrm-008-stagegraph-cancel")
        run_input = replace(stage_input(_blueprint()), run_id=run_id)
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitted = await _submitter(environment.client).submit(
                run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            family = environment.client.get_workflow_handle(f"family/{run_id}/1")
            await asyncio.wait_for(activities.slow_started.wait(), timeout=60)
            status = await _cancel(authority, run_id)
            assert status.command.target.root_workflow_id == submitted.workflow_id
            await until(lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60)
            assert await _ledger(authority, run_id, "cancel") == [
                ("accepted", "run_control"),
                ("delivered", "boundary-delivery"),
            ]
            result = await _await_completion(root, seconds=120)
            root_runs = await replay(root, [BellLabsRunWorkflow])
            family_runs = await replay(family, [StageGraphWorkflow, OperationWorkflow])
            family_history = await family.fetch_history()
            root_receipts = await root.query(BellLabsRunWorkflow.cancel_receipts)

        assert activities.slow_cancelled.is_set() and not activities.slow_completed.is_set()
        assert activities.fast_cancelled.is_set()
        assert len(activities.cancel_settlements) == 2, "both active units settled by the saga"
        assert result["completion_proposal"]["cancelled"] is True
        assert result["completion_proposal"]["open_producer_liability_ids"] == []
        assert "downstream" not in activities.admission_order, "nothing is admitted after a cancel"
        run = await authority.run(run_id)
        assert run.phase == RunPhase.TERMINAL and run.terminal_outcome == RunOutcome.CANCELLED
        assert await _ledger(authority, run_id, "cancel") == [
            ("accepted", "run_control"),
            ("delivered", "boundary-delivery"),
            ("applied", "run_control"),
        ]
        assert [(item.command_id, item.status) for item in root_receipts] == [
            ("cancel", "delivered")
        ]
        assert (root_runs, family_runs) == (1, 1)
        assert STAGEGRAPH_CANCELLATION_PATCH in patch_ids(family_history)
        assert GOVERNED_CANCEL_PATCH not in patch_ids(await root.fetch_history()), (
            "the raw root signal was never used"
        )
        print(
            "RRM-008 EVIDENCE stagegraph cancel:",
            {
                "run_id": run_id,
                "receipts": await _ledger(authority, run_id, "cancel"),
                "admission_order": activities.admission_order,
                "cancelled_units": activities.cancel_settlements,
                "result_decisions": activities.result_decisions,
            },
        )


@pytest.mark.asyncio
async def test_stagegraph_cancel_supersedes_a_delivered_release_and_a_held_wait() -> None:
    """A cancel delivered while the family holds a declared wait: the wait is cancelled by the
    outcome (never satisfied), a release delivered afterwards is rejected `superseded`, and
    the run terminalizes `cancelled` with nothing admitted."""

    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = CancellableStageGraphActivities(authority)
        run_id = await authority.admit("rrm-008-stagegraph-cancel-wait")
        run_input = replace(stage_input(_blueprint(workflow_wait=True)), run_id=run_id)
        condition_id = wait_condition_id("release-workflow")
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitted = await _submitter(environment.client).submit(
                run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            await until(lambda: _has_wait(authority, run_id, condition_id), seconds=60)
            await _cancel(authority, run_id)
            await until(lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60)
            result = await _await_completion(root, seconds=120)
            terminal_run = await authority.run(run_id)
            released = await authority.run_control.execute(
                authority_command(
                    terminal_run.version,
                    run_id,
                    "release",
                    SatisfyWaitAction(
                        condition_id=condition_id, verification_evidence_ref="evidence:late"
                    ),
                )
            )
        assert result["completion_proposal"]["cancelled"] is True
        assert activities.admission_order == []
        run = await authority.run(run_id)
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert run.active_waits == (), "the declared wait was cancelled by the outcome"
        assert released.status.value == "rejected" and released.reason_code == "run_is_terminal"
        assert await _ledger(authority, run_id, "cancel") == [
            ("accepted", "run_control"),
            ("delivered", "boundary-delivery"),
            ("applied", "run_control"),
        ]


def authority_command(version: int, run_id: str, command_id: str, action: Any) -> Any:
    from tests.unit.run_control.test_run_control import command

    return command(run_id, version, command_id, action)


@pytest.mark.asyncio
async def test_cancel_accepted_before_the_family_starts_terminalizes_through_the_saga() -> None:
    """Window: before dispatch at the run level. The cancel is accepted while run control is
    the boundary (`accepted`, `delivered`); the family's `start` binds the target under
    `cancelling`, nothing is admitted, and the saga terminalizes `cancelled` (`applied`)."""

    async with await _environment() as environment:
        authority = Authority(environment.client)
        activities = CancellableStageGraphActivities(authority)
        run_id = await authority.admit("rrm-008-cancel-before-start")
        run_input = replace(stage_input(_blueprint()), run_id=run_id)
        await _cancel(authority, run_id)
        assert await _ledger(authority, run_id, "cancel") == [
            ("accepted", "run_control"),
            ("delivered", "run_control"),
        ]
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitted = await _submitter(environment.client).submit(
                run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            result = await _await_completion(root, seconds=120)
        assert result["completion_proposal"]["cancelled"] is True
        assert activities.admission_order == [] and activities.cancel_settlements == []
        run = await authority.run(run_id)
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert run.execution_target is not None, "the family bound its target under cancelling"
        assert await _ledger(authority, run_id, "cancel") == [
            ("accepted", "run_control"),
            ("delivered", "run_control"),
            ("applied", "run_control"),
        ]


# --- GoalDirected ---------------------------------------------------------------------------------


async def _goal_composition(run_control: Any, model: GoalScriptedModel) -> GoalComposition:
    return await compose_goal_directed(
        run_control=run_control,
        journal=MemoryOperationJournal(),
        lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
        results=InMemoryArtifactPayloadStore(),
        bindings=InMemoryOperationBindingRepository(),
        saver=InMemorySaver(),
        model=model,
        blueprint=goal_blueprint(),
    )


def _goal_workers(
    environment: WorkflowEnvironment, composition: GoalComposition
) -> tuple[Worker, Worker]:
    return (
        Worker(
            environment.client,
            task_queue=GOAL_COMPOSITION_QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=coordinator_activities("GoalDirected", composition.family),
        ),
        Worker(
            environment.client,
            task_queue=composition.binding.task_queue,
            activities=agent_cognitive_activities(
                OperationExecutionActivities(composition.service, worker_identity="rrm008")
            ),
        ),
    )


@pytest.mark.asyncio
async def test_goal_directed_cancel_during_executor_cognition_finishes_the_saga() -> None:
    """RRM-001 section 7 #5: the family no longer fails `goal_cancelling`. The cancel is
    delivered root-first while the executor's real model call is in flight; the heartbeat
    cancel interrupts it; the unit settles `cancelled` once (its reservation released, its
    effect `cancelled`, its latest checkpoint recorded); the family consumes that settlement
    through the terminal path, releases the baseline and proposes terminal `cancelled`,
    which the reducer records. No liability remains."""

    async with await _environment() as environment:
        run_control = goal_run_control()
        authority = Authority(environment.client, run_control)
        model = GoalScriptedModel()
        composition = await _goal_composition(run_control, model)
        run_id = await admit_goal_run(run_control, "rrm-008-goal-cancel")
        entered, _gate = model.gate_on(1)
        run_input = replace(
            goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
            cancellation_retry_seconds=1,
        )
        family_worker, cognitive_worker = _goal_workers(environment, composition)
        async with family_worker, cognitive_worker:
            submitter = _submitter(environment.client)
            submitter._goal_directed_task_queue = GOAL_COMPOSITION_QUEUE
            submitted = await submitter.submit(
                run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.GOAL_DIRECTED
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            family: WorkflowHandle[Any, Any] = environment.client.get_workflow_handle(
                f"family/{run_id}/1"
            )
            await asyncio.wait_for(entered.wait(), timeout=60)
            await _cancel(authority, run_id)
            await until(lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60)
            result = await _await_completion(root, seconds=180)
            root_runs = await replay(root, [BellLabsRunWorkflow])
            family_runs = await replay(family, [GoalDirectedWorkflow, OperationWorkflow])
            family_history = await family.fetch_history()

        assert result["status"] == "cancelled" and result["convergence_proposal"] is None
        assert result["goal_iterations"] == 0
        run = await run_control.get_run(SCOPE, run_id)
        budget = await run_control.get_budget(SCOPE, run_id)
        effects = await run_control.get_effects(SCOPE, run_id)
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert budget.reservations == {} and not any(budget.pending_settlement.values())
        [claim] = effects.claims.values()
        assert claim.disposition == EffectDisposition.CANCELLED and claim.settlement is not None
        [settlement] = composition.service._journal._journal._repository.settlements.values()  # type: ignore[union-attr]
        assert settlement.status == "cancelled"
        [usage_id] = [key for key in budget.usage_records if key != "goal-usage:baseline"]
        assert usage_id == operation_settlement_id(claim.operation_ref)
        assert budget.usage_records["goal-usage:baseline"].actual_amounts == {}
        assert len(model.calls) == 1, "the interrupted call never resumed"
        assert composition.documents.iterations == [], "no iteration document for a cancelled unit"
        assert await _ledger(authority, run_id, "cancel") == [
            ("accepted", "run_control"),
            ("delivered", "boundary-delivery"),
            ("applied", "run_control"),
        ]
        assert (root_runs, family_runs) == (1, 1)
        assert GOAL_CANCELLATION_PATCH in patch_ids(family_history)
        print(
            "RRM-008 EVIDENCE goal-directed cancel:",
            {
                "run_id": run_id,
                "receipts": await _ledger(authority, run_id, "cancel"),
                "model_calls": model.calls,
                "usage_records": sorted(budget.usage_records),
                "consumed": budget.consumed,
                "effects": {claim.effect_id: claim.disposition.value},
                "terminal_outcome": run.terminal_outcome.value,
            },
        )


@pytest.mark.asyncio
async def test_goal_directed_cancel_while_paused_and_across_continue_as_new() -> None:
    """REQ-CP-EXEC-011: a cancel delivered while the family is paused wakes it; the saga
    rejects the pending commands `superseded`, releases the baseline and terminalizes
    `cancelled` (with the pause still active, which the reducer allows under cancellation)."""

    async with await _environment() as environment:
        authority = Authority(environment.client)
        blueprint = _goal_blueprint(max_iterations=3)
        activities = CancellableGoalActivities(authority, blueprint, complete_at_iteration=3)
        activities.release_executor.clear()
        run_id = await authority.admit(
            "rrm-008-goal-paused-cancel", bounded={"goal.iterations": 10}
        )
        run_input = replace(
            goal_input(blueprint=blueprint, run_id=run_id),
            baseline_reservation=BASELINE,
            cancellation_retry_seconds=1,
        )
        async with Worker(
            environment.client,
            task_queue=GOAL_QUEUE,
            workflows=ROOT_WORKFLOWS,
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitted = await _submitter(environment.client).submit(
                run_input, workflow_id="ignored", blueprint_family=BlueprintFamily.GOAL_DIRECTED
            )
            root = environment.client.get_workflow_handle(submitted.workflow_id)
            await asyncio.wait_for(activities.executor_started.wait(), timeout=60)
            await authority.intervene(run_id, "pause", pause("hold-run"))
            activities.release_executor.set()
            await until(lambda: _phase_is(authority, run_id, RunPhase.PAUSED), seconds=90)
            await _cancel(authority, run_id)
            await until(lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60)
            try:
                result = await _await_completion(root, seconds=120)
            except Exception:
                family = environment.client.get_workflow_handle(f"family/{run_id}/1")
                history = await family.fetch_history()
                print("lifecycle", [
                    (item["command_id"], item["action"]["kind"])
                    for item in scheduled_activity_inputs(
                        history, "goaldirected.apply_lifecycle_command"
                    )
                ])
                raise
        assert result["status"] == "cancelled"
        assert result["goal_iterations"] == 1, "iteration 1 settled before the pause"
        run = await authority.run(run_id)
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert [item.decision_id for item in run.active_pauses] == ["hold-run"]
        assert (await _ledger(authority, run_id, "cancel"))[-1] == ("applied", "run_control")
        assert "terminalize" in activities.lifecycle_kinds
        assert "cancel" not in activities.lifecycle_kinds, "the family issues no cancel of its own"
        assert await _ledger(authority, run_id, "pause") == [
            ("accepted", "run_control"),
            ("delivered", "boundary-delivery"),
            ("applied", f"family/{run_id}/1"),
        ]
