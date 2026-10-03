"""RRM-008: the `OperationWorkflow` cancellation saga on Temporal (time-skipping server).

The real operation boundary (RRM-004 harness: a `create_deep_agent` graph, the journaled
coordinator, checkpoint lineage, in-memory run control) serves `operation.execute` and
`operation.cancel`. A Temporal cancel of the workflow reaches the running cognitive Activity
through its heartbeat and interrupts the in-flight model call; the holder settles the unit
`cancelled` with its latest durable checkpoint; the workflow completes normally with the
unit's disposition (it never fails in place of reconciliation), and the history replays.
A cancel before dispatch settles without cognition; a parked `in_doubt` unit is reached by
the cancel, keeps its incident, and settles only on the operator's decision.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    CANCELLATION_SAGA_PATCH,
    NUDGE_SNAPSHOT_PATCH,
    OperationWorkflow,
)
from mission_control.application.execution.operations.journaled_operation_execution import (
    _effect_claim_id,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.execution.checkpoint_lineage import CheckpointClassification
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.policies.contracts import CommandStatus, ReconcileUnitAction
from tests.fixtures.checkpoint_recovery import (
    RecoveryHarness,
    recovery_harness,
    stage_recovery_unit,
)
from tests.fixtures.temporal_history import patch_ids
from tests.unit.operations.test_checkpoint_recovery_classification import (
    _namespace,
    _write_foreign_root_checkpoint,
)

WORKFLOW_QUEUE = "rrm008-operation-workflows"
HEARTBEAT_SECONDS = 2
TIMEOUT_SECONDS = 60


def _request(request: OperationExecutionRequest) -> OperationWorkflowRequest:
    return OperationWorkflowRequest(
        semantic_attempt_id=request.identity.semantic_key,
        operation_kind="bound_operation",
        operation=request,
        timeout_seconds=TIMEOUT_SECONDS,
        heartbeat_timeout_seconds=HEARTBEAT_SECONDS,
    )


async def _until(predicate: Any, *, seconds: float = 60) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 10)):
            if await predicate():
                return
            await asyncio.sleep(0.1)
    raise AssertionError("condition did not hold in time")


def _scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


class _Stack:
    def __init__(self, harness: RecoveryHarness, environment: WorkflowEnvironment) -> None:
        self.harness = harness
        self.environment = environment
        self.activities = OperationExecutionActivities(
            harness.service, worker_identity="rrm008-worker"
        )

    def workers(self) -> tuple[Worker, Worker]:
        return (
            Worker(
                self.environment.client,
                task_queue=WORKFLOW_QUEUE,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                self.environment.client,
                task_queue=self.harness.binding.task_queue,
                activities=[self.activities.execute, self.activities.cancel],
            ),
        )

    async def start(
        self, request: OperationExecutionRequest, workflow_id: str
    ) -> WorkflowHandle[Any, Any]:
        return await self.environment.client.start_workflow(
            OperationWorkflow.run,
            _request(request),
            id=workflow_id,
            task_queue=WORKFLOW_QUEUE,
        )


async def _replays(handle: WorkflowHandle[Any, Any]) -> Any:
    history = await handle.fetch_history()
    await Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    return history


@pytest.mark.asyncio
async def test_temporal_cancel_reaches_running_cognition_and_the_unit_settles_cancelled() -> None:
    """REQ-CP-EXEC-008 step 3: the cancel is delivered to the Activity through its heartbeat;
    the in-flight model call is interrupted; the unit settles `cancelled` with its latest
    durable checkpoint; the workflow completes `cancelled` and replays."""

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        stack = _Stack(harness, environment)
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        entered, _gate = harness.model.gate_on(1)
        workflow_worker, activity_worker = stack.workers()
        async with workflow_worker, activity_worker:
            handle = await stack.start(request, "rrm008-cancel-during-model")
            await asyncio.wait_for(entered.wait(), timeout=60)
            await handle.cancel()
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

        assert result.disposition == "cancelled"
        settled = parse_operation_result(result.result)
        assert settled.status == "cancelled" and settled.failure_code == "cancelled"
        assert settled.result_checkpoint is not None
        transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
        assert transition is not None
        assert transition.classification == CheckpointClassification.INTERRUPTED
        assert settled.result_checkpoint == transition.result_key
        assert len(harness.model.calls) == 1, "the interrupted call never resumed"
        [settlement] = harness.journal.settlements.values()
        assert settlement.status == "cancelled"
        namespace = _namespace(request)
        assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None
        scheduled = _scheduled(history)
        assert scheduled[0] == "operation.execute"
        assert "operation.cancel" in scheduled, "the saga reconciled through operation.cancel"
        assert CANCELLATION_SAGA_PATCH in patch_ids(history)
        attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
        assert all(item.unit_key == unit.unit_key for item in attempts)
        print(
            "RRM-008 EVIDENCE operation workflow cancel during model work:",
            {
                "model_calls": harness.model.calls,
                "scheduled_activities": scheduled,
                "attempts": [(item.attempt.attempt, item.claim_fence) for item in attempts],
                "settlements": len(harness.journal.settlements),
                "classification": transition.classification.value,
            },
        )


@pytest.mark.asyncio
async def test_cancel_before_dispatch_settles_without_cognition_and_replays() -> None:
    """Window: before dispatch (the `request_cancel` signal arrives before the Activity is
    scheduled). Zero invocations; exactly one `operation.cancel`; one settlement."""

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        stack = _Stack(harness, environment)
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        workflow_worker, activity_worker = stack.workers()
        handle = await environment.client.start_workflow(
            OperationWorkflow.run,
            _request(request),
            id="rrm008-cancel-before-dispatch",
            task_queue=WORKFLOW_QUEUE,
            start_signal="request_cancel",
        )
        async with workflow_worker, activity_worker:
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)
        assert result.disposition == "cancelled"
        settled = parse_operation_result(result.result)
        assert settled.status == "cancelled" and settled.result_checkpoint is None
        assert harness.model.calls == [] and harness.runtime.invocations == 0
        assert _scheduled(history) == ["operation.cancel"]
        [settlement] = harness.journal.settlements.values()
        assert settlement.status == "cancelled"
        budget = await harness.run_control.get_budget("tenant-1", harness.run_id)
        assert request.budget_reservation_id not in budget.reservations


@pytest.mark.asyncio
async def test_parked_in_doubt_unit_is_reached_by_the_cancel_and_settled_by_the_operator() -> None:
    """RRM-004 note: the parked `OperationWorkflow` no longer ignores the cancel. It keeps
    the incident (nothing is re-executed), and the operator's `abandon_unit` settles the unit
    `cancelled`; the workflow then completes and replays."""

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        stack = _Stack(harness, environment)
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        await _write_foreign_root_checkpoint(harness, _namespace(request))
        workflow_worker, activity_worker = stack.workers()
        async with workflow_worker, activity_worker:
            handle = await stack.start(request, "rrm008-cancel-parked")

            async def parked() -> bool:
                incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
                return incident is not None

            await _until(parked)
            await handle.cancel()
            await _until(lambda: handle.query(OperationWorkflow.cancellation_requested))
            # The cancel re-ran the settlement once: still in doubt, no model call.
            await _until(lambda: _attempt_count(harness, unit.unit_key, 2))
            description = await handle.describe()
            assert description.status is not None and description.status.name == "RUNNING"
            assert harness.model.calls == []
            assert harness.journal.settlements == {}
            incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
            assert incident is not None and incident.status == "operator_required"

            decided = await harness.reconcile(
                request,
                "reconcile-abandon-under-cancel",
                ReconcileUnitAction(
                    unit_key=unit.unit_key,
                    execution_generation=1,
                    incident_id=incident.incident_id,
                    decision="abandon_unit",
                ),
            )
            assert decided.status == CommandStatus.ACCEPTED
            await handle.signal(OperationWorkflow.unit_reconciliation_recorded, "hint")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

        assert result.disposition == "cancelled"
        settled = parse_operation_result(result.result)
        assert (settled.status, settled.failure_code) == ("cancelled", "in_doubt_abandoned")
        assert harness.model.calls == []
        claim_id = _effect_claim_id(bind_operation_execution_request(request))
        assert harness.journal.settlements[claim_id].status == "cancelled"
        scheduled = _scheduled(history)
        assert scheduled[0] == "operation.execute"
        assert scheduled.count("operation.cancel") == 2, "cancel pass, then the decision"


async def _attempt_count(harness: RecoveryHarness, unit_key: str, expected: int) -> bool:
    return len(await harness.lineage.list_attempts("tenant-1", unit_key)) >= expected


@pytest.mark.asyncio
async def test_worker_shutdown_mid_model_call_is_not_a_cancel_and_the_retry_recovers() -> None:
    """Review F1 (REQ-CP-EXEC-011): the Activity worker shuts down while the model call is
    in flight. Temporal cancels the attempt's task with `worker_shutdown`, which is not a
    requested cancel: the holder re-raises (no `cancelled` settlement), the scheduler
    retries the Activity on the next worker, which classifies the interrupted lineage and
    completes the unit without re-appending the prompt. `operation.cancel` never runs."""

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        stack = _Stack(harness, environment)
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        entered, _gate = harness.model.gate_on(1)
        workflow_worker, first_activity_worker = stack.workers()
        async with workflow_worker:
            async with first_activity_worker:
                handle = await stack.start(request, "rrm008-worker-shutdown")
                await asyncio.wait_for(entered.wait(), timeout=60)
            # The first worker is gone: nothing was settled, the unit is not cancelled.
            assert harness.journal.settlements == {}
            assert not await handle.query(OperationWorkflow.cancellation_requested)
            second_activity_worker = Worker(
                environment.client,
                task_queue=harness.binding.task_queue,
                activities=[stack.activities.execute, stack.activities.cancel],
            )
            async with second_activity_worker:
                result = await asyncio.wait_for(handle.result(), timeout=120)
                history = await _replays(handle)

        assert result.disposition == "completed"
        settled = parse_operation_result(result.result)
        assert settled.status == "completed"
        [settlement] = harness.journal.settlements.values()
        assert settlement.status == "completed"
        assert all(human == 1 for human, _tools in harness.model.calls), "no re-appended prompt"
        attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
        assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [
            (1, 1),
            (2, 2),
        ]
        scheduled = _scheduled(history)
        assert scheduled == ["operation.execute"], "one Activity, retried by the scheduler"
        print(
            "RRM-008 EVIDENCE worker shutdown mid model call:",
            {
                "model_calls": harness.model.calls,
                "attempts": [(item.attempt.attempt, item.claim_fence) for item in attempts],
                "settlement": settlement.status,
                "scheduled_activities": scheduled,
            },
        )


class _GatedActivities(OperationExecutionActivities):
    """`operation.execute` that waits at a gate before the real attempt (review F2)."""

    def __init__(self, service: Any) -> None:
        super().__init__(service, worker_identity="rrm008-worker")
        self.started = asyncio.Event()
        self.gate = asyncio.Event()

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.started.set()
        await self.gate.wait()
        return await super().execute(payload)


@pytest.mark.asyncio
async def test_reconciliation_hint_during_the_activity_is_not_lost() -> None:
    """Review F2: the hint counter is snapshotted before each Activity. A `reconcile_unit`
    hint that lands while the attempt runs wakes the parked unit at once (a second
    `operation.execute` without a second hint) instead of being swallowed."""

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        harness = await recovery_harness()
        unit = stage_recovery_unit(harness.run_id)
        request = await harness.request(unit)
        await _write_foreign_root_checkpoint(harness, _namespace(request))
        activities = _GatedActivities(harness.service)
        workflow_worker = Worker(
            environment.client,
            task_queue=WORKFLOW_QUEUE,
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        )
        activity_worker = Worker(
            environment.client,
            task_queue=harness.binding.task_queue,
            activities=[activities.execute, activities.cancel],
        )
        async with workflow_worker, activity_worker:
            handle = await environment.client.start_workflow(
                OperationWorkflow.run,
                _request(request),
                id="rrm008-hint-during-activity",
                task_queue=WORKFLOW_QUEUE,
            )
            await asyncio.wait_for(activities.started.wait(), timeout=60)
            await handle.signal(OperationWorkflow.unit_reconciliation_recorded, "early")
            activities.gate.set()
            # The unit parks `in_doubt`; the early hint re-runs the classification once.
            await _until(lambda: _attempt_count(harness, unit.unit_key, 2))
            history = await handle.fetch_history()
            description = await handle.describe()
            assert description.status is not None and description.status.name == "RUNNING"
            await handle.terminate("review F2 proof complete")

        scheduled = _scheduled(history)
        assert scheduled.count("operation.execute") == 2
        assert "operation.cancel" not in scheduled
        signals = sum(
            1
            for event in history.events
            if event.HasField("workflow_execution_signaled_event_attributes")
        )
        assert signals == 1, "one hint, delivered during the first Activity"
        assert NUDGE_SNAPSHOT_PATCH in patch_ids(history)
        assert harness.model.calls == [] and harness.journal.settlements == {}
