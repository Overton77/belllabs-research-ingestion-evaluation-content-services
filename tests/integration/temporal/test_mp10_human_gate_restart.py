"""MP-10 review waits on the real local Temporal server and a disposable PostgreSQL 17.

Temporal: 127.0.0.1:7233 (`make temporal-up`), real timers and a real worker that is stopped
and started again while the human decides. PostgreSQL: the Human Task rows, events, outbox
and run-control reads on a scratch `mct_*` database (MISSION_CONTROL_TEST_ADMIN_DSN).

The family boundaries (StageGraph admission/decision/cycle, GoalDirected preparation and
reconciliation) and the cognitive `operation.execute` are FIXTURE activities: no model or
provider is contacted. The Human Gate control activation, its activities, the Human Task
repository, the service the API calls and the Temporal wake are the production code.

Proves, for a Stage Graph gate between executor stages and a GoalDirected review:
- the wait holds no activity (the gate workflow has no pending activity while it waits);
- the wait survives a worker stop/start and a resolution committed while no worker runs;
- exactly one attributed resolution per task; a stale reviewer and a changed packet digest
  cannot reuse an approval;
- `request_changes` reaches the declared remediation (StageGraph: the remediation stage
  re-runs with the feedback objective; GoalDirected: the next executor receives the
  feedback) under the same frozen criteria, with counters continuing, not reset.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.worker import Replayer, Worker

from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.temporal.activities.human_gate import HumanGateActivities
from mission_control.adapters.temporal.human_gate_wake import TemporalHumanGateWake
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.goal_directed import (
    GOAL_HUMAN_REVIEW_PATCH,
    GoalDirectedWorkflow,
)
from mission_control.adapters.temporal.workflows.human_gate import (
    HumanGateWorkflow,
    HumanGateWorkflowInput,
)
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import (
    STAGEGRAPH_HUMAN_GATE_PATCH,
    StageGraphWorkflow,
)
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.programs.human_gates import GateReservationSettlement
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    AllowedOperationVariant,
    FairnessGroup,
    GoalDirectedBlueprint,
    LateResultPolicy,
    LateResultRule,
    SlowSiblingPolicy,
    StageDependency,
    StageGraphBlueprint,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageOperationSlot,
    StageOutputSlot,
    WorkflowCyclePolicy,
)
from mission_control.domain.authoring.fixtures import GENERIC_GOAL_DIRECTED
from mission_control.domain.execution.contracts import NativeOperationExecutionPlacement
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.contracts import (
    GoalVerificationResult,
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
    StageGraphRunInput,
)
from mission_control.domain.programs.goal_directed_runtime import (
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from mission_control.domain.programs.human_gate import (
    HumanGateSpec,
    HumanResolutionRequest,
    HumanTaskView,
    open_activation,
    packet_item,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db, owner_rows  # noqa: F401
from tests.integration.temporal.test_wp_bp_010_temporal import FakeStageGraphActivities
from tests.integration.temporal.test_wp_bp_010_temporal import _operation as stage_operation
from tests.integration.temporal.test_wp_bp_020_temporal import FakeGoalDirectedActivities
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as goal_input
from tests.unit.run_control.test_run_control import request, service

pytestmark = pytest.mark.common_db

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
OWNER = ActorContext(actor_id="owner")
POLL_SECONDS = 5


def _placement(queue: str, placement_id: str) -> NativeOperationExecutionPlacement:
    return NativeOperationExecutionPlacement.create(
        placement_id=placement_id,
        revision=1,
        task_queue=queue,
        qualification_refs=("QUAL-MP10-FIXTURE",),
    )


# --- Stage Graph FIXTURE boundaries ---------------------------------------------------------


def _slot() -> StageOperationSlot:
    return StageOperationSlot(
        operation_slot_id="execute",
        reservation={"operation.attempts": 1},
        allowed_variants=(
            AllowedOperationVariant(
                operation_variant_id="default", operation_contract_ref="operation:mp10@1"
            ),
        ),
    )


def _edge(producer: str, consumer: str) -> StageDependency:
    return StageDependency(
        dependency_id=f"{producer}-to-{consumer}",
        consumer_stage_id=consumer,
        join_id=f"{consumer}-inputs",
        producer_stage_id=producer,
        producer_output_slot_id="result",
        consumer_input_slot_id=f"{producer}-input",
        dependency_class="required",
    )


def _stage(stage_id: str, *inputs: str) -> StageNode:
    return StageNode(
        stage_id=stage_id,
        input_slots=tuple(StageInputSlot(input_slot_id=f"{item}-input") for item in inputs),
        output_slots=(
            StageOutputSlot(output_slot_id="result", output_contract_ref=f"output:{stage_id}@1"),
        ),
        operation_slots=(_slot(),),
    )


def _review_graph() -> StageGraphBlueprint:
    """draft -> review (Human Gate) -> publish; one remediation cycle may re-run draft."""

    edges = (_edge("draft", "review"), _edge("review", "publish"))
    return StageGraphBlueprint(
        logical_id="mp10-review-graph",
        title="MP-10 review between executor stages",
        description="A draft, a human review gate and a publish stage (FIXTURE).",
        stages=(_stage("draft"), _stage("review", "draft"), _stage("publish", "review")),
        joins=tuple(
            StageJoin(
                consumer_stage_id=edge.consumer_stage_id,
                join_id=edge.join_id,
                kind="all",
                dependency_ids=(edge.dependency_id,),
                slow_sibling_policy=SlowSiblingPolicy(
                    triggers=("join_released",),
                    execution_action="continue",
                    arrival_route="evaluate_late_result",
                ),
            )
            for edge in edges
        ),
        dependencies=edges,
        fairness_groups=(FairnessGroup(group_id="default", weight=1),),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="admit-late", trigger="consumer_already_admitted", decision="admit"
                ),
            )
        ),
        workflow_evaluation_contract_ref="evaluation:mp10-review@1",
        workflow_cycle_policy=WorkflowCyclePolicy(
            max_cycles=1,
            evaluation_contract_ref="evaluation:mp10-review@1",
            objective_contract_ref="objective:mp10-review@1",
            reservation={"workflow.cycles": 1},
        ),
    )


class ReviewStageActivities(FakeStageGraphActivities):
    """FIXTURE StageGraph boundaries; records which stages ran and with which objective."""

    def __init__(self, queue: str) -> None:
        super().__init__()
        self.queue = queue
        self.executed: list[str] = []
        self.objectives: list[tuple[str, int, str | None]] = []

    @activity.defn(name="stagegraph.admit_operation")
    async def admit(
        self, request: StageGraphAdmissionActivityRequest
    ) -> StageGraphAdmissionActivityResult:
        candidate = request.proposal.identity.candidate
        instance = request.projection.stages[candidate.semantic_prefix]
        self.objectives.append(
            (candidate.stage_id, candidate.workflow_cycle_ordinal, instance.objective_override)
        )
        admitted = await super().admit(request)
        operation = stage_operation(request)
        placed = operation.model_copy(
            update={
                "operation": operation.operation.model_copy(
                    update={"native_placement": _placement(self.queue, "native.mp10.stage")}
                )
            }
        )
        return replace(admitted, operation=placed)

    @activity.defn(name="operation.execute")
    async def execute_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        stage = next(
            item for item in ("draft", "review", "publish") if f":stage:{item}:" in operation_id
        )
        self.executed.append(operation_id)
        # The draft's content digest changes with its cycle, as a re-run artifact would.
        return {"output_refs": [f"artifact://{stage}.md@{sha256_digest(operation_id)}"]}

    @property
    def functions(self) -> list[object]:
        return [
            self.initialize,
            self.admit,
            self.decide,
            self.apply_cycle,
            self.complete,
            self.execute_operation,
            self.cancel_operation,
        ]


# --- GoalDirected FIXTURE boundaries --------------------------------------------------------


class ReviewGoalActivities(FakeGoalDirectedActivities):
    """FIXTURE GoalDirected boundaries; outputs change per iteration and the executor's
    preparation records the review feedback and reservation it received."""

    def __init__(self, queue: str) -> None:
        super().__init__(complete_at_iteration=1)
        self.queue = queue
        self.executor_preparations: list[tuple[int, dict[str, int], tuple[str, ...]]] = []

    async def _prepare(self, request: GoalOperationPreparationRequest) -> GoalOperationDispatch:
        if request.operation_role == "executor":
            self.executor_preparations.append(
                (
                    request.goal_iteration,
                    dict(request.reservation),
                    tuple(str(item.comment) for item in request.review_feedback),
                )
            )
        dispatch = await super()._prepare(request)
        operation = dispatch.workflow_request.operation
        placed = dispatch.workflow_request.model_copy(
            update={
                "operation": operation.model_copy(
                    update={"native_placement": _placement(self.queue, "native.mp10.goal")}
                )
            }
        )
        return dispatch.model_copy(update={"workflow_request": placed})

    @activity.defn(name="goaldirected.prepare_executor")
    async def prepare_executor(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self._prepare(request)

    @activity.defn(name="goaldirected.prepare_verifier")
    async def prepare_verifier(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self._prepare(request)

    @activity.defn(name="goaldirected.reconcile_operation")
    async def reconcile(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        result = await super().reconcile(request)
        iteration = request.claim.identity.iteration.goal_iteration
        output = f"artifact://goal-result.md@{sha256_digest(f'iteration-{iteration}')}"
        if result.execution_result is not None:
            return result.model_copy(
                update={"execution_result": replace(result.execution_result, output_refs=(output,))}
            )
        if result.verification_result is not None:
            draft = replace(
                result.verification_result,
                admitted_executor_output_refs=(output,),
                verification_ref=f"verification-ref:{iteration}",
                verification_digest="pending",
            )
            payload = asdict(draft)
            payload.pop("verification_digest")
            verified: GoalVerificationResult = replace(
                draft, verification_digest=sha256_digest(payload)
            )
            return result.model_copy(update={"verification_result": verified})
        return result

    @property
    def functions(self) -> list[object]:
        return [
            self.prepare_executor,
            self.prepare_verifier,
            self.execute_operation,
            self.cancel_operation,
            self.reconcile,
            self.lifecycle,
        ]


# --- Helpers ----------------------------------------------------------------------------------


async def _admitted_run(pool: asyncpg.Pool, scope: str) -> tuple[Any, str]:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    return authority, admission.run_id


async def _until(probe: Callable[[], Awaitable[Any]], *, within: float = 90.0) -> Any:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        value = await probe()
        if value:
            return value
        await asyncio.sleep(0.25)
    raise AssertionError("condition not reached in time")


async def _open_task(
    repository: PostgresHumanTaskRepository, scope: str, run_id: str, review_round: int
) -> HumanTaskView:
    async def probe() -> HumanTaskView | None:
        tasks = await repository.list(scope, run_id=run_id, lifecycle="open")
        return next((task for task in tasks if task.activation.review_round == review_round), None)

    found: HumanTaskView = await _until(probe)
    return found


async def _assert_waiting_without_activities(client: Client, task: HumanTaskView) -> None:
    """The gate holds no activity (no cognitive slot) while it waits for the human."""

    handle = client.get_workflow_handle(task.activation.workflow_id)

    async def settled() -> bool:
        described = await handle.describe()
        return not described.raw_description.pending_activities

    await _until(settled, within=30)


def _answer(task: HumanTaskView, request_id: str, **updates: Any) -> HumanResolutionRequest:
    values: dict[str, Any] = {
        "request_id": request_id,
        "expected_task_version": task.version,
        "decision": "approve",
        "reviewed_packet_digest": task.activation.packet_digest,
    }
    values.update(updates)
    return HumanResolutionRequest.model_validate(values)


async def _resolutions(db: CommonDatabase, run_id: str) -> list[tuple[str, str, str]]:
    rows = await owner_rows(
        db,
        """
        SELECT task.task_key, resolution.actor_ref, resolution.answer->>'decision' AS decision
        FROM mission_control.human_task task
        JOIN mission_control.human_resolution resolution
          ON resolution.human_task_id = task.human_task_id
        WHERE task.target_ref LIKE $1 ORDER BY resolution.decided_at
        """,
        f"run:{run_id}/%",
    )
    return [(row["task_key"], row["actor_ref"], row["decision"]) for row in rows]


async def _assert_patched_and_replays(
    handle: WorkflowHandle[Any, Any], patch_id: str, workflows: list[type[Any]]
) -> None:
    """The new path is behind its patch marker and the recorded history replays."""

    history = await handle.fetch_history()
    patches = [
        json.loads(base64.b64decode(item["data"]))["id"]
        for event in history.to_json_dict()["events"]
        if event.get("eventType") == "EVENT_TYPE_MARKER_RECORDED"
        for detail in event["markerRecordedEventAttributes"]["details"].values()
        for item in detail["payloads"]
    ]
    assert patch_id in patches
    await Replayer(
        workflows=workflows, workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)


def _worker(
    client: Client, queue: str, workflows: list[type[Any]], activities: list[object]
) -> Worker:
    return Worker(
        client,
        task_queue=queue,
        workflows=workflows,
        activities=activities,
        workflow_runner=coordinator_workflow_runner(),
    )


# --- Tests ------------------------------------------------------------------------------------


async def test_stage_graph_review_survives_worker_restart_and_remediates_once(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        run_control, run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)
        queue = f"mp10-stagegraph-{uuid4().hex[:8]}"
        family = ReviewStageActivities(queue)
        gates = HumanGateActivities(repository, settlement=GateReservationSettlement(run_control))
        graph = _review_graph()
        gate = HumanGateSpec(
            gate_key="review",
            prompt="Accept the draft?",
            reviewers=("owner",),
            packet_sources=("draft.result",),
            remediation_target="draft",
            max_review_rounds=2,
        )
        run_input = StageGraphRunInput(
            run_id=run_id,
            request_scope=scope,
            effective_configuration_digest="sha256:" + "a" * 64,
            workflow_type_digest="sha256:" + "a" * 64,
            blueprint_digest=sha256_digest(graph),
            blueprint=graph.model_dump(mode="json"),
            max_concurrency=1,
            task_timeout_seconds=30,
            correlation_id=f"stagegraph:{run_id}",
            semantic_input_binding_ref="semantic-input:mp10",
            human_gates=(gate,),
            human_gate_poll_seconds=POLL_SECONDS,
        )
        workflows: list[type[Any]] = [StageGraphWorkflow, HumanGateWorkflow, OperationWorkflow]
        activities = [*family.functions, *gates.functions]
        api = HumanTaskService(repository, request_scope=scope, wake=TemporalHumanGateWake(client))
        handle: WorkflowHandle[Any, Any]
        async with _worker(client, queue, workflows, activities):
            handle = await client.start_workflow(
                StageGraphWorkflow.run,
                run_input,
                id=f"mp10-stagegraph-{run_id}",
                task_queue=queue,
                execution_timeout=timedelta(minutes=10),
            )
            first = await _open_task(repository, scope, run_id, 1)
            await _assert_waiting_without_activities(client, first)
            assert first.activation.permitted_decisions == ("approve", "deny", "request_changes")
        # Worker stopped. The API commits a resolution while no worker runs.
        with pytest.raises(HumanTaskRejected) as stale:
            await api.resolve(
                first.human_task_id, _answer(first, "stale", expected_task_version=2), OWNER
            )
        assert stale.value.code == "stale_version"
        changes = await api.resolve(
            first.human_task_id,
            _answer(
                first,
                "changes-1",
                decision="request_changes",
                comment="Cite the primary cohort study.",
                feedback_artifact_refs=("artifact://review-notes.md",),
            ),
            OWNER,
        )
        assert changes.status == "accepted"
        async with _worker(client, queue, workflows, activities):
            second = await _open_task(repository, scope, run_id, 2)
            await _assert_waiting_without_activities(client, second)
            assert second.human_task_id != first.human_task_id
            assert second.activation.packet_digest != first.activation.packet_digest
            assert second.activation.permitted_decisions == ("approve", "deny")
            # The round-1 decision and its packet digest cannot approve round 2.
            with pytest.raises(HumanTaskRejected) as reused:
                await api.resolve(
                    second.human_task_id,
                    _answer(second, "reuse", reviewed_packet_digest=first.activation.packet_digest),
                    OWNER,
                )
            assert reused.value.code == "packet_digest_mismatch"
            with pytest.raises(HumanTaskRejected) as intruder:
                await api.resolve(
                    second.human_task_id, _answer(second, "x"), ActorContext(actor_id="intruder")
                )
            assert intruder.value.code == "not_reviewer"
            await api.resolve(second.human_task_id, _answer(second, "approve-2"), OWNER)
            result = await handle.result()

        draft_runs = [item for item in family.executed if ":stage:draft:" in item]
        assert len(draft_runs) == 2, family.executed
        assert not any(":stage:review:" in item for item in family.executed), "no gate cognition"
        assert [(stage, cycle) for stage, cycle, _ in family.objectives] == [
            ("draft", 0),
            ("review", 0),
            ("draft", 1),
            ("review", 1),
            ("publish", 1),
        ]
        remediation = family.objectives[2][2]
        assert remediation is not None and "Cite the primary cohort study." in remediation
        assert family.objectives[3][2] is None  # only the declared remediation target
        assert result.workflow_cycles == 1
        assert str(result.output_refs["review"][0]).startswith("human-resolution:")
        assert await _resolutions(common_db, run_id) == [
            (first.task_key, "owner", "request_changes"),
            (second.task_key, "owner", "approve"),
        ]
        await _assert_patched_and_replays(handle, STAGEGRAPH_HUMAN_GATE_PATCH, workflows)
    finally:
        await pool.close()


async def test_goal_directed_review_survives_worker_restart_and_feeds_the_next_executor(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        run_control, run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)
        queue = f"mp10-goal-{uuid4().hex[:8]}"
        family = ReviewGoalActivities(queue)
        gates = HumanGateActivities(repository, settlement=GateReservationSettlement(run_control))
        blueprint = GoalDirectedBlueprint.model_validate(
            {**GENERIC_GOAL_DIRECTED.model_dump(mode="python"), "max_iterations": 3}
        )
        review = HumanGateSpec(
            gate_key="goal-review",
            prompt="Review the verified outputs.",
            reviewers=("owner",),
            remediation_target="goal/executor",
            max_review_rounds=2,
        )
        run_input = replace(
            goal_input(blueprint=blueprint, run_id=run_id),
            request_scope=scope,
            human_review=review,
            human_gate_poll_seconds=POLL_SECONDS,
        )
        workflows: list[type[Any]] = [GoalDirectedWorkflow, HumanGateWorkflow, OperationWorkflow]
        activities = [*family.functions, *gates.functions]
        api = HumanTaskService(repository, request_scope=scope, wake=TemporalHumanGateWake(client))
        async with _worker(client, queue, workflows, activities):
            handle = await client.start_workflow(
                GoalDirectedWorkflow.run,
                run_input,
                id=f"mp10-goal-{run_id}",
                task_queue=queue,
                execution_timeout=timedelta(minutes=10),
            )
            first = await _open_task(repository, scope, run_id, 1)
            await _assert_waiting_without_activities(client, first)
            assert {item.source for item in first.activation.packet} == {
                "goal.output",
                "goal.verification",
            }
        changes = await api.resolve(
            first.human_task_id,
            _answer(first, "changes-1", decision="request_changes", comment="Add the 2025 cohort."),
            OWNER,
        )
        assert changes.status == "accepted"
        # A retry of the same request while the worker is down is idempotent.
        retry = await api.resolve(
            first.human_task_id,
            _answer(first, "changes-1", decision="request_changes", comment="Add the 2025 cohort."),
            OWNER,
        )
        assert retry.status == "duplicate"
        async with _worker(client, queue, workflows, activities):
            second = await _open_task(repository, scope, run_id, 2)
            await _assert_waiting_without_activities(client, second)
            assert second.activation.packet_digest != first.activation.packet_digest
            with pytest.raises(HumanTaskRejected) as reused:
                await api.resolve(
                    second.human_task_id,
                    _answer(second, "reuse", reviewed_packet_digest=first.activation.packet_digest),
                    OWNER,
                )
            assert reused.value.code == "packet_digest_mismatch"
            await api.resolve(second.human_task_id, _answer(second, "approve-2"), OWNER)
            result = await handle.result()

        # Feedback reached the next executor only; the reservation per iteration is the
        # same frozen amount and the iteration counter continued (1 -> 2), never reset.
        assert [item[0] for item in family.executor_preparations] == [1, 2]
        assert family.executor_preparations[0][2] == ()
        assert family.executor_preparations[1][2] == ("Add the 2025 cohort.",)
        assert family.executor_preparations[0][1] == family.executor_preparations[1][1]
        assert result.goal_iterations == 2
        assert result.terminalization_proposal is not None
        assert result.terminalization_proposal.proposed_outcome == "complete"
        assert result.active_revision_id == run_input.initial_revision.revision_id
        assert family.lifecycle_kinds[-1] == "terminalize"
        assert await _resolutions(common_db, run_id) == [
            (first.task_key, "owner", "request_changes"),
            (second.task_key, "owner", "approve"),
        ]
        await _assert_patched_and_replays(handle, GOAL_HUMAN_REVIEW_PATCH, workflows)
    finally:
        await pool.close()


async def test_gate_deadlines_lost_wakes_and_cancel_on_real_timers(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    """`stop` expires on a real timer; `keep_waiting` passes its deadline and still needs a
    human; a resolution whose wake-up hint is never sent is observed by the bounded poll;
    cancelling the activation cancels its open task."""

    client = await Client.connect(ADDRESS, namespace=NAMESPACE)
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        _run_control, run_id = await _admitted_run(pool, scope)
        repository = PostgresHumanTaskRepository(pool)
        queue = f"mp10-gate-{uuid4().hex[:8]}"
        no_wake = HumanTaskService(repository, request_scope=scope)

        def gate_input(key: str, **policy: Any) -> HumanGateWorkflowInput:
            gate = HumanGateSpec(gate_key=key, prompt="Approve?", reviewers=("owner",), **policy)
            return HumanGateWorkflowInput(
                activation=open_activation(
                    request_scope=scope,
                    run_id=run_id,
                    family="StageGraph",
                    activation_key=f"probe:{key}",
                    execution_epoch=1,
                    review_round=1,
                    spec=gate,
                    packet=(packet_item("draft.result", "artifact://draft.md"),),
                    opened_at=datetime.now(UTC),
                ),
                poll_seconds=2,
                activity_timeout_seconds=30,
            )

        async with _worker(
            client, queue, [HumanGateWorkflow], HumanGateActivities(repository).functions
        ):
            stop = gate_input("stop", timeout_seconds=2, on_timeout="stop")
            waiting = gate_input("wait", timeout_seconds=2)
            lost = gate_input("lost")
            cancel = gate_input("cancel")
            handles = {
                name: await client.start_workflow(
                    HumanGateWorkflow.run,
                    item,
                    id=item.activation.workflow_id,
                    task_queue=queue,
                    execution_timeout=timedelta(minutes=5),
                )
                for name, item in (
                    ("stop", stop),
                    ("wait", waiting),
                    ("lost", lost),
                    ("cancel", cancel),
                )
            }
            stopped = await handles["stop"].result()
            assert stopped.status == "stopped_by_policy" and stopped.decision is None

            await asyncio.sleep(4)  # past the keep_waiting deadline
            kept = await repository.get(scope, waiting.activation.human_task_id)
            assert kept is not None and kept.lifecycle == "open"
            await no_wake.resolve(str(kept.human_task_id), _answer(kept, "late"), OWNER)
            late = await handles["wait"].result()
            assert late.status == "accepted" and late.actor_ref == "owner"

            opened = await _until(
                lambda: repository.get(scope, lost.activation.human_task_id), within=30
            )
            await no_wake.resolve(opened.human_task_id, _answer(opened, "no-wake"), OWNER)
            observed = await handles["lost"].result()
            assert observed.status == "accepted"

            await _until(lambda: repository.get(scope, cancel.activation.human_task_id), within=30)
            await handles["cancel"].cancel()
            with pytest.raises(WorkflowFailureError):
                await handles["cancel"].result()
            cancelled = await repository.get(scope, cancel.activation.human_task_id)
            assert cancelled is not None and cancelled.lifecycle == "cancelled"
    finally:
        await pool.close()
