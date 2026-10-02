"""RRM-008: cancellation at the operation boundary (REQ-CP-EXEC-008 steps 3-6).

A real Deep Agent graph runs through the whole operation boundary (the RRM-004 harness:
journaled coordinator, checkpoint lineage, in-memory run control). Cancellation is injected
before dispatch, during a model call (the Temporal heartbeat cancel arrives as
`asyncio.CancelledError` inside the in-flight step), during the tool step's checkpoint
write, while a unit is classified `interrupted`, after an ambiguous effect, with an unsettled
consequential tool effect, and during async child work. Every window converges to exactly
one settlement, never resumes cognition, records the latest durable checkpoint as the result
checkpoint and leaves no unexplained liability. A lost cancelling holder is taken over.

RRM-004 review finding 4: a shared GoalDirected session namespace advances its head over a
unit settled without completing (cancelled while interrupted, provider failure with a partial
lineage, budget violation after a terminal leaf), so the next iteration proceeds.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.application.async_subagents.parent_effects import (
    RunControlAsyncChildEffects,
    async_child_effect_id,
    async_child_usage_id,
)
from app.application.async_subagents.service import (
    AsyncSubagentService,
    AsyncSubagentSpawnRequest,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
)
from app.application.operations.journaled_operation_execution import _effect_claim_id
from app.application.operations.operation_execution import (
    bind_operation_execution_request,
)
from app.application.operations.operation_progress import CURRENT_CANCEL_PROBE
from app.domain.operation_execution.async_subagent_reconciliation import (
    ASYNC_CHILD_RECONCILE_PERMISSION,
)
from app.domain.operation_execution.checkpoint_lineage import (
    STAMP_INVOCATION_ID,
    CheckpointClassification,
    CheckpointLineageConflict,
    submission_invocation_id,
)
from app.domain.operation_execution.contracts import (
    AsyncSubagentDependencyClass,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
    OperationExecutionRequest,
)
from app.domain.orchestration.goal_directed import GoalDirectedInterpreter
from app.domain.run_control.contracts import (
    CancelAction,
    ClaimEffectAction,
    CommandStatus,
    ReconcileUnitAction,
)
from tests.acceptance.control_plane.test_wp_cp_045 import DeterministicProvider
from tests.fixtures.checkpoint_recovery import (
    CrashingSaver,
    RecoveryHarness,
    SimulatedWorkerCrash,
    governed_workspace,
    recovery_harness,
    stage_recovery_unit,
)
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    GoalScriptedModel,
    RecordingGoalDocuments,
    admit_goal_run,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
    goal_start_action,
    goal_templates,
    preparer,
)
from tests.unit.operations.test_checkpoint_recovery_classification import (
    AFTER_TOOL_CHECKPOINT,
    _chain,
    _leaf_id,
    _namespace,
    _root_checkpoints,
    _write_foreign_root_checkpoint,
)
from tests.unit.operations.test_rrm_016_goal_directed_recovery import _Templates
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import _preparation
from tests.unit.run_control.test_run_control import actor, command

CANCEL_REASON = "operation cancelled by the governed cancellation saga"


# --- helpers ---------------------------------------------------------------------------------


class GatingSaver(CrashingSaver):
    """A checkpointer that blocks inside the N-th checkpoint write until released."""

    def __init__(self) -> None:
        super().__init__()
        self.gate_on_put: int | None = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def aput(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        if self.gate_on_put is not None and self.puts + 1 == self.gate_on_put:
            self.entered.set()
            await self.release.wait()
        return await super().aput(config, checkpoint, metadata, new_versions)


async def _cancel(harness: RecoveryHarness, request: OperationExecutionRequest) -> Any:
    """The `operation.cancel` Activity's body: the saga's settlement of one unit."""

    return await harness.service.cancel(request, harness.attempt(request))


async def _cancel_mid_cognition(
    harness: RecoveryHarness,
    request: OperationExecutionRequest,
    entered: asyncio.Event,
    *,
    cancel_requested: bool = True,
) -> Any:
    """Run an attempt and cancel it like Temporal does through the heartbeat: the running
    Activity task receives `asyncio.CancelledError` inside its in-flight step. The Activity
    runner's probe says whether that cancellation was requested (review F1); any other cause
    (worker shutdown, heartbeat timeout) is modelled by `cancel_requested=False`."""

    token = CURRENT_CANCEL_PROBE.set(lambda: cancel_requested)
    try:
        task = asyncio.create_task(harness.service.execute(request, harness.attempt(request)))
    finally:
        CURRENT_CANCEL_PROBE.reset(token)
    gate = asyncio.ensure_future(entered.wait())
    done, _pending = await asyncio.wait({task, gate}, timeout=30, return_when="FIRST_COMPLETED")
    if task in done:
        gate.cancel()
        raise AssertionError(f"the attempt ended before the window: {task.result()!r}")
    assert gate in done, "the injection window was not reached"
    task.cancel()
    return await task


def _settlements(harness: RecoveryHarness, request: OperationExecutionRequest) -> list[Any]:
    claim_id = _effect_claim_id(bind_operation_execution_request(request))
    return [item for key, item in harness.journal.settlements.items() if key == claim_id]


async def _liability(
    harness: RecoveryHarness, request: OperationExecutionRequest
) -> dict[str, Any]:
    """The unit's run-control liability after settlement: reservation, effect, evidence."""

    binding = bind_operation_execution_request(request)
    budget = await harness.run_control.get_budget("tenant-1", harness.run_id)
    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    claim = effects.claims.get(_effect_claim_id(binding))
    return {
        "reservation_open": request.budget_reservation_id in budget.reservations,
        "pending_settlement": dict(budget.pending_settlement),
        "effect": claim.disposition.value if claim is not None else None,
        "effect_settled": claim is not None and claim.settlement is not None,
        "evidence": [
            item.settlement_id
            for item in run.accepted_operation_settlement_evidence
            if item.accepted_by_authority_ref == binding.binding_id
        ],
        "consumed": dict(budget.consumed),
    }


async def _assert_cancelled_once(
    harness: RecoveryHarness,
    request: OperationExecutionRequest,
    result: Any,
    *,
    model_calls: int,
    with_checkpoint: bool,
) -> None:
    unit = request.runtime_unit
    assert unit is not None
    namespace = _namespace(request)
    assert result.status == "cancelled", result
    assert result.failure_code == "cancelled"
    assert len(harness.model.calls) == model_calls
    [settlement] = _settlements(harness, request)
    assert settlement.status == "cancelled"
    liability = await _liability(harness, request)
    assert liability["reservation_open"] is False
    assert liability["effect"] == "cancelled" and liability["effect_settled"]
    assert liability["evidence"] == [settlement.settlement_id]
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    if with_checkpoint:
        assert transition is not None
        assert result.result_checkpoint == transition.result_key
        assert result.checkpoint_transition_id == transition.transition_id
        assert await harness.lineage.get_namespace_head("tenant-1", namespace) == (
            transition.result_key
        )
        chain = await _chain(harness, namespace, transition.result_key.checkpoint_id)
        assert {item.metadata[STAMP_INVOCATION_ID] for item in chain} == {
            submission_invocation_id(unit.unit_key, 1)
        }
    else:
        assert transition is None and result.result_checkpoint is None
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None
    # Terminal cancellation is immutable: neither path re-dispatches or changes the result.
    assert await _cancel(harness, request) == result
    assert await harness.run(request) == result
    assert len(harness.model.calls) == model_calls
    assert len(_settlements(harness, request)) == 1


# --- injection windows -----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_before_dispatch_settles_cancelled_without_cognition() -> None:
    """Window: before dispatch. One binding, one claim, zero invocations, zero checkpoints;
    the reservation is released and the effect settles `cancelled` (steps 5-7)."""

    harness = await recovery_harness()
    request = await harness.request(stage_recovery_unit(harness.run_id))
    result = await _cancel(harness, request)
    await _assert_cancelled_once(harness, request, result, model_calls=0, with_checkpoint=False)
    assert harness.runtime.invocations == 0
    assert await _root_checkpoints(harness, _namespace(request)) == []
    assert result.usage.amounts == {}
    assert (await _liability(harness, request))["consumed"].get("tokens.total", 0) == 0


@pytest.mark.asyncio
async def test_cancel_during_model_work_settles_with_the_latest_checkpoint() -> None:
    """Window: during controlled model work. The heartbeat cancel interrupts the in-flight
    model call; the holder records the latest durable checkpoint (classified `interrupted`),
    settles `cancelled` once and nothing ever resumes (step 3)."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, _gate = harness.model.gate_on(1)
    result = await _cancel_mid_cognition(harness, request, entered)
    await _assert_cancelled_once(harness, request, result, model_calls=1, with_checkpoint=True)
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    assert result.result_checkpoint is not None
    assert result.result_checkpoint.checkpoint_id == await _leaf_id(harness, _namespace(request))
    assert result.usage.amounts == {"tokens.total": 0}, "the interrupted call is unobservable"
    assert harness.runtime.invocations == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["worker_shutdown", "heartbeat_timeout"])
async def test_task_cancellation_that_is_not_a_requested_cancel_recovers_on_the_next_attempt(
    cause: str,
) -> None:
    """Review F1 (REQ-CP-EXEC-011): Temporal also cancels an Activity's task for a worker
    shutdown or a heartbeat/start-to-close timeout. Neither is a cancel of the unit: the
    holder re-raises (releasing its lease), nothing is settled, and the next attempt
    classifies the interrupted lineage and completes without re-appending the prompt."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, _gate = harness.model.gate_on(1)
    with pytest.raises(asyncio.CancelledError):
        await _cancel_mid_cognition(harness, request, entered, cancel_requested=False)
    assert _settlements(harness, request) == [], f"{cause}: nothing is settled"
    assert await harness.lineage.get_transition("tenant-1", unit.unit_key, 1) is None

    result = await harness.run(request)
    assert result.status == "completed", result
    assert await harness.lineage.get_namespace_in_flight("tenant-1", _namespace(request)) is None
    [settlement] = _settlements(harness, request)
    assert settlement.status == "completed"
    assert all(human == 1 for human, _tools in harness.model.calls), "no re-appended prompt"
    attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
    assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [(1, 1), (2, 2)]
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None and transition.seedable


@pytest.mark.asyncio
async def test_requested_cancel_without_journaled_intent_stands_down() -> None:
    """Review F1: the journal is the authority. A requested Temporal cancellation of an
    attempt whose run control holds no accepted `cancel` is not applied: the holder stands
    down (nothing is settled). Once the operator's cancel is journaled, the saga's
    `operation.cancel` settles the unit `cancelled` with its interrupted lineage."""

    harness, prepare, _templates, run_id = await _goal_harness("journal")
    first_claim, _second = await _claims(run_id)
    run = await harness.run_control.get_run(SCOPE, run_id)
    first = await prepare.prepare(_preparation(run_id, first_claim, "executor", run.version, 0))
    operation = first.workflow_request.operation
    assert operation.runtime_unit is not None
    entered, _gate = harness.model.gate_on(1)
    with pytest.raises(asyncio.CancelledError):
        await _cancel_mid_cognition(harness, operation, entered, cancel_requested=True)
    assert _settlements(harness, operation) == []
    assert (await harness.run_control.get_run(SCOPE, run_id)).phase.value == "active"

    run = await harness.run_control.get_run(SCOPE, run_id)
    cancelled = await harness.run_control.execute(
        command(run_id, run.version, "operator-cancel", CancelAction())
    )
    assert cancelled.status == CommandStatus.ACCEPTED and cancelled.phase.value == "cancelling"
    settled = await _cancel(harness, operation)
    assert settled.status == "cancelled" and settled.failure_code == "cancelled"
    transition = await harness.lineage.get_transition(SCOPE, operation.runtime_unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    assert settled.result_checkpoint == transition.result_key
    assert len(harness.model.calls) == 1, "the interrupted call never resumed"


@pytest.mark.asyncio
async def test_cancel_during_the_tool_step_checkpoint_write() -> None:
    """Window: during controlled tool work (the tool ran; its result checkpoint is being
    written). The latest durable checkpoint is the model's tool call; the tool result is not
    durable and is never re-run, because cancellation never resumes."""

    saver = GatingSaver()
    saver.gate_on_put = AFTER_TOOL_CHECKPOINT
    harness = await recovery_harness(saver=saver)
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    result = await _cancel_mid_cognition(harness, request, saver.entered)
    await _assert_cancelled_once(harness, request, result, model_calls=1, with_checkpoint=True)
    assert len(await _root_checkpoints(harness, _namespace(request))) == AFTER_TOOL_CHECKPOINT - 1
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    assert result.usage.amounts == {"tokens.total": 5}, "one completed model call is recorded"


@pytest.mark.asyncio
async def test_cancel_of_an_interrupted_unit_does_not_resume_cognition() -> None:
    """EXEC-008: a cancel delivered while the unit is `interrupted` (a lost worker left a
    durable intermediate checkpoint) settles `cancelled` with that checkpoint as the result
    checkpoint and never resumes; the next attempt of a cancelled unit returns unchanged."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    assert len(harness.model.calls) == 1
    crash_leaf = await _leaf_id(harness, _namespace(request))

    result = await _cancel(harness, request)
    await _assert_cancelled_once(harness, request, result, model_calls=1, with_checkpoint=True)
    assert result.result_checkpoint is not None
    assert result.result_checkpoint.checkpoint_id == crash_leaf
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    assert transition.claim_fence == 2, "the cancelling attempt took the lost lease over"
    attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
    assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [(1, 1), (2, 2)]


@pytest.mark.asyncio
async def test_cancel_after_an_ambiguous_effect_keeps_the_incident_for_the_operator() -> None:
    """Window: after an ambiguous effect. The unit is `in_doubt`; the cancel neither
    re-executes nor settles it speculatively (step 5). `abandon_unit` under cancellation
    settles `cancelled` (`in_doubt_abandoned`) and releases the namespace."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    await _write_foreign_root_checkpoint(harness, namespace)
    parked = await harness.run(request)
    assert parked.status == "in_doubt" and parked.failure_code == "foreign_descendant"

    cancelled = await _cancel(harness, request)
    assert cancelled.status == "in_doubt" and cancelled.failure_code == "foreign_descendant"
    assert cancelled.reconciliation_incident_id == parked.reconciliation_incident_id
    # The parking attempt entered the runtime (classification only); the cancel never does.
    assert harness.model.calls == [] and harness.runtime.invocations == 1
    assert _settlements(harness, request) == []
    assert (await _liability(harness, request))["effect"] == "ambiguous"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None and incident.status == "operator_required"

    decided = await harness.reconcile(
        request,
        "reconcile-abandon-cancel",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="abandon_unit",
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    settled = await _cancel(harness, request)
    assert settled.status == "cancelled" and settled.failure_code == "in_doubt_abandoned"
    assert harness.model.calls == []
    [settlement] = _settlements(harness, request)
    assert settlement.status == "cancelled"
    liability = await _liability(harness, request)
    assert liability["effect"] == "cancelled" and liability["effect_settled"]
    assert liability["reservation_open"] is False
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None
    assert await _cancel(harness, request) == settled


@pytest.mark.asyncio
@pytest.mark.parametrize("superseded_first", [True, False])
async def test_cancel_settles_a_generation_superseded_before_the_cancel(
    superseded_first: bool,
) -> None:
    """Re-review edge case (REQ-CP-EXEC-005/008): `start_new_generation` was accepted for an
    in_doubt unit before the run was cancelled. A cancelling run admits no new generation,
    so nothing else would ever settle the unit; `operation.cancel` settles its claim,
    reservation and effect `cancelled` (`generation_superseded`) exactly once, without
    cognition and without any lineage write by the fenced generation. Both orders: the
    superseded generation's own retry already returned `generation_superseded` (its
    OperationWorkflow ended), or the cancel is the first attempt after the decision."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    await _write_foreign_root_checkpoint(harness, namespace)
    parked = await harness.run(request)
    assert parked.status == "in_doubt" and parked.failure_code == "foreign_descendant"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    decided = await harness.reconcile(
        request,
        "reconcile-new-generation",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="start_new_generation",
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    if superseded_first:
        superseded = await harness.run(request)
        assert (superseded.status, superseded.failure_code) == (
            "in_doubt",
            "generation_superseded",
        )
    assert _settlements(harness, request) == []
    assert (await _liability(harness, request))["reservation_open"] is True

    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    cancelling = await harness.run_control.execute(
        command(harness.run_id, run.version, "operator-cancel", CancelAction())
    )
    assert cancelling.status == CommandStatus.ACCEPTED
    settled = await _cancel(harness, request)
    assert (settled.status, settled.failure_code) == ("cancelled", "generation_superseded")
    assert settled.result_checkpoint is None and settled.checkpoint_transition_id is None
    # The parking attempt entered the runtime (classification only); nothing else did.
    assert harness.model.calls == [] and harness.runtime.invocations == 1
    [settlement] = _settlements(harness, request)
    assert settlement.status == "cancelled"
    liability = await _liability(harness, request)
    assert liability["reservation_open"] is False
    assert liability["effect"] == "cancelled" and liability["effect_settled"]
    assert liability["evidence"] == [settlement.settlement_id]
    # The fenced generation wrote nothing to its lineage.
    assert await harness.lineage.get_result("tenant-1", unit.unit_key, 1) is None
    assert await harness.lineage.get_transition("tenant-1", unit.unit_key, 1) is None
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None
    # Settled once and immutable on every path.
    assert await _cancel(harness, request) == settled
    assert await harness.run(request) == settled
    assert len(_settlements(harness, request)) == 1
    assert harness.model.calls == []


@pytest.mark.asyncio
async def test_cancel_with_an_unsettled_consequential_tool_effect_is_in_doubt() -> None:
    """A consequential tool effect claimed during cognition whose outcome is unknown when the
    cancel lands is ambiguous: the unit parks `in_doubt` (`unsettled_effect_claims`) with a
    governed incident instead of settling `cancelled` over it."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    binding = bind_operation_execution_request(request)

    async def claim_tool_effect() -> None:
        run = await harness.run_control.get_run("tenant-1", harness.run_id)
        claimed = await harness.run_control.execute(
            command(
                harness.run_id,
                run.version,
                "claim-tool-effect",
                ClaimEffectAction(
                    effect_id="tool-effect:send-email",
                    effect_kind="tool.consequential",
                    operation_ref=binding.binding_id,
                    provider_idempotency_key="tool-effect:send-email",
                    reservation_id=request.budget_reservation_id,
                ),
            )
        )
        assert claimed.status == CommandStatus.ACCEPTED

    harness.model.before_call(2, claim_tool_effect)
    entered, _gate = harness.model.gate_on(2)
    result = await _cancel_mid_cognition(harness, request, entered)
    assert result.status == "in_doubt" and result.failure_code == "unsettled_effect_claims"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    assert incident.unsettled_effect_ids == ("tool-effect:send-email",)
    assert _settlements(harness, request) == [], "never settled while an effect is ambiguous"
    assert len(harness.model.calls) == 2
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert [item.kind for item in run.active_waits] == ["operator_reconciliation"]


@pytest.mark.asyncio
async def test_lost_cancelling_holder_is_taken_over_and_the_unit_settles_once() -> None:
    """Cancellation survives worker loss: the holder that was settling the unit `cancelled`
    is lost after the fenced result observation; the next `operation.cancel` attempt takes
    the lease over, settles exactly the recorded manifest and nothing is re-run."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    harness.crashable.crash_before_settlement = True
    with pytest.raises(SimulatedWorkerCrash):
        await _cancel(harness, request)
    harness.clock.advance(timedelta(minutes=10))
    assert _settlements(harness, request) == []
    recorded = await harness.lineage.get_result("tenant-1", unit.unit_key, 1)
    assert recorded is not None and recorded.status == "cancelled"

    result = await _cancel(harness, request)
    await _assert_cancelled_once(harness, request, result, model_calls=1, with_checkpoint=True)
    attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
    assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [
        (1, 1),
        (2, 2),
        (3, 3),
    ]
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None and transition.claim_fence == 2
    assert harness.journal.technical_attempts[
        _effect_claim_id(bind_operation_execution_request(request))
    ] == [3]


# --- async children ---------------------------------------------------------------------------


async def _async_children(
    run_control: Any,
) -> tuple[AsyncSubagentService, DeterministicProvider, InMemoryAsyncSubagentAuthority]:
    events: list[str] = []
    provider = DeterministicProvider(events)
    authority = InMemoryAsyncSubagentAuthority()
    service = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        authority,
        provider,
        parent_effects=RunControlAsyncChildEffects(run_control, actor=actor()),
        allow_new_spawns=True,
    )
    return service, provider, authority


@pytest.mark.asyncio
async def test_cancel_during_async_work_cancels_the_child_and_leaves_its_usage_pending() -> None:
    """Window: during async child work. The child is cancelled under its link policy with
    the provider's acknowledgement recorded (step 4); its unattributed usage stays pending on
    its own effect (REQ-CP-RUN-009), which blocks the run's terminal settlement until a
    privileged `reconcile_usage`; the parent unit settles `cancelled` once."""

    from app.domain.operation_execution.contracts import AsyncSubagentContract as Contract
    from tests.acceptance.control_plane.test_wp_cp_045 import contract as base_contract

    harness = await recovery_harness()
    children, provider, _authority = await _async_children(harness.run_control)
    harness.service._children = children  # the composition seam; see goal_directed_journaled
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    binding = bind_operation_execution_request(request)
    child_contract = Contract.create(
        **{
            **base_contract().model_dump(mode="python", exclude={"contract_digest"}),
            "budget_limits": {"tokens.total": 5},
        }
    )
    spawn = AsyncSubagentSpawnRequest(
        request_scope="tenant-1",
        parent_run_id=harness.run_id,
        parent_operation_id=unit.semantic_operation_id,
        parent_binding_id=binding.binding_id,
        execution_generation=1,
        contract=child_contract,
        dependency_class=AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
        objective_ref="ref:objective:cancel",
        objective="Wait, then report.",
        context_slice_ref="ref:context-slice:cancel",
        reservation_id="reservation:rrm-008-child",
        idempotency_key="rrm-008-child",
        requested_at=datetime.now(UTC),
        parent_reservation_id=request.budget_reservation_id,
    )
    spawned: list[Any] = []

    async def spawn_child() -> None:
        # The child is spawned from inside the parent's cognition (after the tool call), as
        # the governed middleware does; its reservation and claim move the run version
        # after the parent's claim, never before it.
        spawned.append(await children.spawn(spawn))

    harness.model.before_call(2, spawn_child)
    entered, _gate = harness.model.gate_on(2)
    result = await _cancel_mid_cognition(harness, request, entered)
    [child] = spawned
    assert child.lifecycle == AsyncSubagentLifecycle.RUNNING
    await _assert_cancelled_once(harness, request, result, model_calls=2, with_checkpoint=True)
    assert provider.events.count("provider.cancel") == 1
    records = await children.cancel_children(
        binding, reason=CANCEL_REASON, requested_at=datetime.now(UTC)
    )
    [record] = records  # a second pass finds the child already cancelled and settled once
    assert record.child_execution_id == child.child_execution_id
    assert record.effect_id == async_child_effect_id(child.child_execution_id)
    assert record.cancellation_receipt == "not_requested", "cancelled once, by the saga"
    assert record.lifecycle == AsyncSubagentLifecycle.CANCELLED
    assert record.result_decision == "reject"
    assert record.usage_disposition == "pending_usage"
    assert provider.events.count("provider.cancel") == 1
    cancelled = await children.execution("tenant-1", child.child_execution_id)
    assert cancelled.lifecycle == AsyncSubagentLifecycle.CANCELLED
    link = await children.link("tenant-1", child.child_execution_id)
    assert link.cancellation_requested and link.cancellation_receipt == "provider_acknowledged"
    assert link.settled is False and link.usage_disposition == "pending_usage"

    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    child_claim = effects.claims[async_child_effect_id(child.child_execution_id)]
    assert child_claim.settlement is None, "pending usage keeps the child effect unsettled"
    # The rejection is the run's authoritative decision on the child's terminal fact, so the
    # cancelled run is not held by `unresolved_async_children` once its usage settles.
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    [child_state] = [
        item for item in run.async_children if item.child_execution_id == child.child_execution_id
    ]
    assert [(item.fact_id, item.outcome.value) for item in child_state.decisions] == [
        (f"async-child-fact:async-terminal:{child.child_execution_id}", "rejected")
    ]
    budget = await harness.run_control.get_budget("tenant-1", harness.run_id)
    child_usage = budget.usage_records[async_child_usage_id(child.child_execution_id)]
    assert child_usage.pending_external_amounts == {"tokens.total": 5}
    assert budget.pending_settlement.get("tokens.total") == 5

    # A late child result can never mutate the cancelled parent (step 6): a cancelled child
    # has no typed manifest to admit, and a late admission is refused anyway.
    with pytest.raises(Exception, match="typed result manifest|late or superseded"):
        await children.decide_result(
            "tenant-1",
            child.child_execution_id,
            "admit",
            parent_open=False,
            current_generation=1,
            decided_at=datetime.now(UTC),
        )

    # Privileged usage reconciliation settles the pending usage and the child's effect.
    reconciler = actor().model_copy(
        update={"permissions": actor().permissions | {ASYNC_CHILD_RECONCILE_PERMISSION}}
    )
    reconciled = await children.reconcile_usage(
        "tenant-1",
        child.child_execution_id,
        actor=reconciler,
        run_usage={
            "provider-run-1": AsyncSubagentUsage(
                provider_run_id="provider-run-1",
                attribution="provider_attributed",
                attributed_amounts={"tokens.total": 3},
            )
        },
        settlement_ref=f"settlement:{child.child_execution_id}:reconciled",
        reconciled_at=datetime.now(UTC),
    )
    assert reconciled.settled and reconciled.usage_disposition == "settled"
    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    assert effects.claims[async_child_effect_id(child.child_execution_id)].settlement is not None
    budget = await harness.run_control.get_budget("tenant-1", harness.run_id)
    assert not any(budget.pending_settlement.values())
    assert set(budget.reservations) == {"baseline"}, "only the run-level baseline remains"


class _FlakyEffects:
    """A parent-effects port that fails `decide_result` a given number of times, before or
    after delegating to the real run-control adapter."""

    def __init__(self, inner: Any, *, failures: int, after_write: bool) -> None:
        self.inner = inner
        self.failures = failures
        self.after_write = after_write
        self.calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def decide_result(self, *args: Any, **kwargs: Any) -> None:
        self.calls += 1
        if self.failures and not self.after_write:
            self.failures -= 1
            raise ConnectionError("run control unreachable before the decision")
        await self.inner.decide_result(*args, **kwargs)
        if self.failures:
            self.failures -= 1
            raise ConnectionError("crashed after the run-control decision")


@pytest.mark.asyncio
@pytest.mark.parametrize("after_write", [False, True])
async def test_child_result_decision_is_crash_safe_and_idempotent(after_write: bool) -> None:
    """Review F4: the decision is recorded in run control first (it refuses a child without
    a terminal lifecycle fact and is idempotent by command identity). A failure before or
    after that write leaves the authority and the link undecided; the retry completes the
    same decision once, and a repeated decision changes nothing."""

    from app.domain.operation_execution.contracts import AsyncSubagentContract as Contract
    from tests.acceptance.control_plane.test_wp_cp_045 import contract as base_contract

    harness = await recovery_harness()
    events: list[str] = []
    provider = DeterministicProvider(events)
    authority = InMemoryAsyncSubagentAuthority()
    effects = _FlakyEffects(
        RunControlAsyncChildEffects(harness.run_control, actor=actor()),
        failures=1,
        after_write=after_write,
    )
    children = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        authority,
        provider,
        parent_effects=effects,  # type: ignore[arg-type]
        allow_new_spawns=True,
    )
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    binding = bind_operation_execution_request(request)
    child_contract = Contract.create(
        **{
            **base_contract().model_dump(mode="python", exclude={"contract_digest"}),
            "budget_limits": {"tokens.total": 5},
        }
    )
    child = await children.spawn(
        AsyncSubagentSpawnRequest(
            request_scope="tenant-1",
            parent_run_id=harness.run_id,
            parent_operation_id=unit.semantic_operation_id,
            parent_binding_id=binding.binding_id,
            execution_generation=1,
            contract=child_contract,
            dependency_class=AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
            objective_ref="ref:objective:decide",
            objective="Report once.",
            context_slice_ref="ref:context-slice:decide",
            reservation_id="reservation:rrm-008-decide",
            idempotency_key="rrm-008-decide",
            requested_at=datetime.now(UTC),
            parent_reservation_id=request.budget_reservation_id,
        )
    )
    provider.next_status = "success"
    completed = await children.reconcile("tenant-1", child.child_execution_id)
    assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED

    async def decide() -> Any:
        return await children.decide_result(
            "tenant-1",
            child.child_execution_id,
            "admit",
            parent_open=True,
            current_generation=1,
            decided_at=datetime.now(UTC),
        )

    async def recorded_decisions() -> list[str]:
        run = await harness.run_control.get_run("tenant-1", harness.run_id)
        [state] = [
            item
            for item in run.async_children
            if item.child_execution_id == child.child_execution_id
        ]
        return [item.outcome.value for item in state.decisions]

    with pytest.raises(ConnectionError):
        await decide()
    link = await children.link("tenant-1", child.child_execution_id)
    assert link.result_decision is None, "the link is decided only after run control"
    assert await recorded_decisions() == (["accepted"] if after_write else [])

    decided = await decide()
    assert decided.result_decision == "admit"
    assert await recorded_decisions() == ["accepted"]
    again = await decide()
    assert again.result_decision == "admit"
    assert await recorded_decisions() == ["accepted"], "a repeated decision is idempotent"
    assert effects.calls == 3


# --- shared GoalDirected session namespace (RRM-004 review finding 4) --------------------------


async def _goal_harness(label: str) -> tuple[RecoveryHarness, Any, Any, str]:
    run_control = goal_run_control()
    harness = await recovery_harness(
        model=GoalScriptedModel(), real_authority=True, run_control=run_control
    )
    run_id = await admit_goal_run(run_control, f"rrm-008-session-{label}")
    started = await run_control.execute(
        command(run_id, 1, f"start-{label}", goal_start_action(run_id))
    )
    assert started.status == CommandStatus.ACCEPTED
    templates = _Templates(goal_templates(harness.binding, workspace=governed_workspace))
    prepare = preparer(
        run_control=run_control,
        templates=templates,
        bindings=harness.service._bindings,
        documents=RecordingGoalDocuments(),
    )
    return harness, prepare, templates, run_id


async def _claims(run_id: str) -> tuple[Any, Any]:
    """Iteration 1 and iteration 2 executor claims of one shared session (generation 1)."""

    interpreter = GoalDirectedInterpreter(goal_blueprint())
    state = interpreter.initial_state(goal_run_input(run_id, goal_blueprint(), baseline={}))
    _claimed, first = interpreter.claim_execution(state)
    _claimed, second = interpreter.claim_execution(
        replace(state, next_goal_iteration=2, next_agent_run=2)
    )
    assert first.session_id == second.session_id
    return first, second


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["cancelled_interrupted", "provider_failed_partial", "budget_after_terminal"]
)
async def test_shared_session_head_advances_over_a_unit_settled_without_completing(
    case: str,
) -> None:
    """Decision (RRM-004 finding 4, review F3): the head advances by a recorded transition
    to the unit's latest durable checkpoint, so the orphan branch never parks the next unit
    `in_doubt` as `foreign_descendant`. Only verified cognition is fed forward: after a
    cancellation (REQ-CP-EXEC-008 keeps the partial evidence) the next unit in the same
    session classifies `not_submitted` from that head and completes; after a provider
    `failed` or a budget violation the head is sealed, the next unit in that session is
    refused before any provider work, and the session continues only in a new session
    generation (REQ-BP-GD-012). The scripted model refuses unmatched tool calls, as a
    provider does."""

    harness, prepare, _templates, run_id = await _goal_harness(case)
    first_claim, second_claim = await _claims(run_id)
    run = await harness.run_control.get_run(SCOPE, run_id)
    first = await prepare.prepare(_preparation(run_id, first_claim, "executor", run.version, 0))
    operation = first.workflow_request.operation
    assert operation.runtime_unit is not None
    namespace = _namespace(operation)

    if case == "cancelled_interrupted":
        harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
        await harness.crash(operation)
        run = await harness.run_control.get_run(SCOPE, run_id)
        journaled = await harness.run_control.execute(
            command(run_id, run.version, "operator-cancel", CancelAction())
        )
        assert journaled.status == CommandStatus.ACCEPTED
        settled = await _cancel(harness, operation)
        assert settled.status == "cancelled"
        expected = CheckpointClassification.INTERRUPTED
    elif case == "provider_failed_partial":
        harness.model.fail_on(2, ValueError("provider rejected the second call"))
        settled = await harness.run(operation)
        assert (settled.status, settled.failure_code) == ("failed", "runtime_failed")
        expected = CheckpointClassification.INTERRUPTED
    else:
        # A budget violation after the terminal leaf: the unit fails, its lineage is complete.
        harness.model.tokens_per_call = operation.budget_limits["tokens.total"]
        settled = await harness.run(operation)
        assert (settled.status, settled.failure_code) == ("failed", "budget_exceeded")
        harness.model.tokens_per_call = 5
        expected = CheckpointClassification.NOT_SUBMITTED
    unit = operation.runtime_unit
    assert unit is not None
    transition = await harness.lineage.get_transition(SCOPE, unit.unit_key, 1)
    assert transition is not None, "the settlement recorded a transition over the partial lineage"
    assert transition.classification == expected
    assert settled.result_checkpoint == transition.result_key
    head = await harness.lineage.get_namespace_head(SCOPE, namespace)
    assert head == transition.result_key
    assert head is not None and head.checkpoint_id == await _leaf_id(harness, namespace)
    assert await harness.lineage.get_namespace_in_flight(SCOPE, namespace) is None
    assert transition.seedable is (case == "cancelled_interrupted")
    calls_before = len(harness.model.calls)

    run = await harness.run_control.get_run(SCOPE, run_id)
    second = await prepare.prepare(_preparation(run_id, second_claim, "executor", run.version, 1))
    next_operation = second.workflow_request.operation
    assert next_operation.runtime_unit is not None
    assert _namespace(next_operation) == namespace
    if case != "cancelled_interrupted":
        # The sealed head is refused before dispatch: no model call, no transition, the
        # head and the namespace untouched, no lease left behind.
        with pytest.raises(CheckpointLineageConflict, match="sealed"):
            await harness.run(next_operation)
        assert len(harness.model.calls) == calls_before
        assert (
            await harness.lineage.get_transition(SCOPE, next_operation.runtime_unit.unit_key, 1)
            is None
        )
        assert await harness.lineage.get_namespace_head(SCOPE, namespace) == head
        assert await harness.lineage.get_namespace_in_flight(SCOPE, namespace) is None
        assert _settlements(harness, next_operation) == []
        return

    # After a cancellation the run is cancelling (the journal is the authority), so no unit
    # runs; the lineage proof is that the next unit of the same session would be admitted
    # `not_submitted` from the advanced head (no orphan branch, no incident) and that the
    # head's partial evidence ends in answered tool calls, which a provider accepts.
    lineage_service = harness.service._lineage
    assert lineage_service is not None
    admitted = await lineage_service.admit_attempt(
        bind_operation_execution_request(next_operation), harness.attempt(next_operation)
    )
    try:
        assert admitted.admission.lease_granted
        assert admitted.admission.observation.expected_source == head
        assert admitted.admission.existing_transition is None
        assert admitted.admission.incident is None, "the orphan branch is explained by the head"
        plan = lineage_service.plan(admitted)
        assert plan is not None and plan.expected_source == head
    finally:
        await lineage_service.release(admitted)
    assert len(harness.model.calls) == calls_before


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["execute", "cancel"])
@pytest.mark.parametrize("case", ["provider_failed_partial", "budget_after_terminal"])
async def test_failed_unit_lost_before_its_journal_settlement_settles_its_recorded_result(
    case: str, route: str
) -> None:
    """Re-review blocker (REQ-CP-EXEC-011/014): a `failed` settlement records its result
    observation and its (non-seedable) transition before the journal settlement, sealing the
    session head. A worker lost between the two must not wedge the unit: its retry is the
    unit generation that sealed the head, so it is admitted, classifies `observed_unsettled`
    and settles exactly the recorded manifest once, with no provider work. This holds for the
    `operation.execute` retry and for the saga's `operation.cancel` (the run was cancelled
    meanwhile: the recorded `failed` result stands). The next unit of the session is still
    refused. (`timed_out` is not a separate path: the operation boundary never settles it.)"""

    harness, prepare, _templates, run_id = await _goal_harness(f"lost-{case}-{route}")
    first_claim, second_claim = await _claims(run_id)
    run = await harness.run_control.get_run(SCOPE, run_id)
    first = await prepare.prepare(_preparation(run_id, first_claim, "executor", run.version, 0))
    operation = first.workflow_request.operation
    unit = operation.runtime_unit
    assert unit is not None
    namespace = _namespace(operation)
    if case == "provider_failed_partial":
        harness.model.fail_on(2, ValueError("provider rejected the second call"))
        expected_code = "runtime_failed"
    else:
        harness.model.tokens_per_call = operation.budget_limits["tokens.total"]
        expected_code = "budget_exceeded"
    harness.crashable.crash_before_settlement = True
    await harness.crash(operation)
    harness.model.tokens_per_call = 5

    # The crash window: result and transition recorded, head sealed, nothing settled.
    recorded = await harness.lineage.get_result(SCOPE, unit.unit_key, 1)
    assert recorded is not None and recorded.status == "failed"
    transition = await harness.lineage.get_transition(SCOPE, unit.unit_key, 1)
    assert transition is not None and transition.seedable is False
    head = await harness.lineage.get_namespace_head(SCOPE, namespace)
    assert head == transition.result_key
    assert _settlements(harness, operation) == []
    calls_before = len(harness.model.calls)
    invocations_before = harness.runtime.invocations

    if route == "cancel":
        run = await harness.run_control.get_run(SCOPE, run_id)
        journaled = await harness.run_control.execute(
            command(run_id, run.version, "operator-cancel", CancelAction())
        )
        assert journaled.status == CommandStatus.ACCEPTED
        recovered = await _cancel(harness, operation)
    else:
        recovered = await harness.run(operation)

    assert (recovered.status, recovered.failure_code) == ("failed", expected_code), recovered
    assert recovered.result_checkpoint == transition.result_key
    assert recovered.checkpoint_transition_id == transition.transition_id
    assert len(harness.model.calls) == calls_before, "no provider work on recovery"
    assert harness.runtime.invocations == invocations_before
    [settlement] = _settlements(harness, operation)
    assert settlement.status == "failed"
    budget = await harness.run_control.get_budget(SCOPE, run_id)
    assert operation.budget_reservation_id not in budget.reservations, "reservation released"
    effects = await harness.run_control.get_effects(SCOPE, run_id)
    claim = effects.claims[_effect_claim_id(bind_operation_execution_request(operation))]
    assert claim.disposition.value == "failed" and claim.settlement is not None
    assert await harness.lineage.get_namespace_in_flight(SCOPE, namespace) is None
    assert await harness.lineage.get_namespace_head(SCOPE, namespace) == head
    # Settled exactly once: a further retry on either route returns the same settlement.
    assert await harness.run(operation) == recovered
    if route == "cancel":
        assert await _cancel(harness, operation) == recovered
    assert len(_settlements(harness, operation)) == 1
    assert len(harness.model.calls) == calls_before

    if route == "execute":
        # The sealed head still refuses the next unit of the session (REQ-BP-GD-012).
        run = await harness.run_control.get_run(SCOPE, run_id)
        second = await prepare.prepare(
            _preparation(run_id, second_claim, "executor", run.version, 1)
        )
        next_operation = second.workflow_request.operation
        with pytest.raises(CheckpointLineageConflict, match="sealed"):
            await harness.run(next_operation)
        assert len(harness.model.calls) == calls_before
        assert _settlements(harness, next_operation) == []
