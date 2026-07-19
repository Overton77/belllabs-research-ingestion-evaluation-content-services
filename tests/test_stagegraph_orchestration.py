from __future__ import annotations

from collections import Counter

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.application.orchestration import RunControlLifecycleGateway
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    StageCyclePolicy,
    StageGraphBlueprint,
    StageNode,
    WorkflowCyclePolicy,
)
from app.domain.orchestration.contracts import (
    ExecutionIdentity,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphRunInput,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.domain.orchestration.interpreter import StageGraphInterpreter
from app.temporal.stagegraph_workflow import StageGraphWorkflow
from tests.test_run_control import NOW, actor
from tests.test_run_control import request as run_request
from tests.test_run_control import service as run_control_service

DIGEST = "sha256:" + "a" * 64


def orchestration_fixture() -> StageGraphBlueprint:
    return StageGraphBlueprint(
        logical_id="fixture.durable-stagegraph",
        title="Durable StageGraph mechanism fixture",
        description="Contract fixture with joins and explicit semantic cycles.",
        max_parallel_stages=2,
        stages=(
            StageNode(
                stage_id="prepare",
                output_slots=frozenset({"prepared"}),
                reservation={"operation.attempts": 1},
            ),
            StageNode(
                stage_id="extract",
                depends_on=frozenset({"prepare"}),
                output_slots=frozenset({"extracted"}),
                reservation={"operation.attempts": 1},
                stage_cycle_policy=StageCyclePolicy(
                    max_cycles=1,
                    evaluation_contract_ref="evaluation:extract@1",
                    objective_contract_ref="objective:repair-extraction@1",
                    reservation={"stage.cycles": 1},
                ),
            ),
            StageNode(
                stage_id="review_a",
                depends_on=frozenset({"extract"}),
                fairness_group="review",
                output_slots=frozenset({"review_a"}),
                reservation={"operation.attempts": 1},
            ),
            StageNode(
                stage_id="review_b",
                depends_on=frozenset({"extract"}),
                fairness_group="review",
                output_slots=frozenset({"review_b"}),
                reservation={"operation.attempts": 1},
            ),
            StageNode(
                stage_id="join",
                depends_on=frozenset({"review_a", "review_b"}),
                join_policy="all",
                output_slots=frozenset({"joined"}),
                reservation={"operation.attempts": 1},
            ),
            StageNode(
                stage_id="publish",
                depends_on=frozenset({"join"}),
                output_slots=frozenset({"published"}),
                reservation={"operation.attempts": 1},
            ),
        ),
        declared_output_slots=frozenset(
            {"prepared", "extracted", "review_a", "review_b", "joined", "published"}
        ),
        workflow_cycle_policy=WorkflowCyclePolicy(
            max_cycles=1,
            evaluation_contract_ref="evaluation:whole-graph@1",
            objective_contract_ref="objective:repair-review-a@1",
            reservation={"workflow.cycles": 1},
        ),
    )


class FakeStageGraphActivities:
    def __init__(self) -> None:
        self.operation_requests: list[StageOperationRequest] = []
        self.lifecycle_requests: list[LifecycleCommandRequest] = []
        self.workflow_evaluations: list[WorkflowEvaluationRequest] = []
        self._failed_once = False

    @activity.defn(name="stagegraph.execute_operation")
    async def execute_operation(self, request: StageOperationRequest) -> StageOperationResult:
        self.operation_requests.append(request)
        if request.identity.stage_id == "review_b" and not self._failed_once:
            self._failed_once = True
            raise ApplicationError("simulated retryable worker loss")
        if request.identity.stage_id == "extract" and request.identity.stage_cycle == 0:
            return StageOperationResult(
                identity=request.identity,
                disposition="completed",
                output_refs=("artifact:extract:candidate",),
                evaluation="cycle",
                evaluation_ref="evaluation:extract:repair",
                next_objective="repair extraction against accepted evaluator findings",
                evaluation_contract_ref=request.cycle_evaluation_contract_ref,
                objective_contract_ref=request.cycle_objective_contract_ref,
                handoff_ref="handoff:extract:cycle-0",
                temporal_activity_attempt=activity.info().attempt,
            )
        return StageOperationResult(
            identity=request.identity,
            disposition="completed",
            output_refs=(
                f"artifact:{request.identity.stage_id}:"
                f"workflow-{request.identity.workflow_cycle}:"
                f"stage-{request.identity.stage_cycle}",
            ),
            evaluation="accept",
            evaluation_ref=f"evaluation:{request.identity.semantic_key}",
            evaluation_contract_ref=request.cycle_evaluation_contract_ref,
            objective_contract_ref=request.cycle_objective_contract_ref,
            handoff_ref=f"handoff:{request.identity.semantic_key}",
            temporal_activity_attempt=activity.info().attempt,
        )

    @activity.defn(name="stagegraph.evaluate_workflow")
    async def evaluate_workflow(
        self, request: WorkflowEvaluationRequest
    ) -> WorkflowEvaluationResult:
        self.workflow_evaluations.append(request)
        if request.workflow_cycle == 0:
            return WorkflowEvaluationResult(
                action="cycle",
                evaluation_ref="evaluation:workflow:repair-review-a",
                invalidation_frontier=("review_a",),
                next_objective="rerun review A and only its dependency descendants",
                evaluation_contract_ref=request.evaluation_contract_ref,
                objective_contract_ref=request.objective_contract_ref,
            )
        return WorkflowEvaluationResult(
            action="accept",
            evaluation_ref="evaluation:workflow:accepted",
            evaluation_contract_ref=request.evaluation_contract_ref,
        )

    @activity.defn(name="stagegraph.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        self.lifecycle_requests.append(request)
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="terminal" if request.action["kind"] == "terminalize" else "active",
            reason_code="accepted",
            evidence_frontier_digest=DIGEST,
            obligation_revision=DIGEST,
            accepted_obligation_evidence_digest=DIGEST,
            required_obligations_accepted=True,
        )


class StaticBindingVerifier:
    async def verify(
        self,
        effective_configuration_digest: str,
        blueprint_digest: str,
    ) -> None:
        assert effective_configuration_digest == DIGEST
        assert blueprint_digest == DIGEST


def test_interpreter_honors_dependencies_join_concurrency_and_scoped_wait() -> None:
    blueprint = orchestration_fixture()
    interpreter = StageGraphInterpreter(blueprint, effective_max_concurrency=2)
    state = interpreter.initial_state(ExecutionIdentity("run-pure"), run_version=1)

    assert interpreter.runnable(state) == ("prepare",)
    prepare = interpreter.operation_request(state, "prepare")
    interpreter.apply_stage_result(
        state,
        StageOperationResult(
            identity=prepare.identity,
            disposition="completed",
            output_refs=("artifact:prepared",),
        ),
    )
    assert interpreter.runnable(state) == ("extract",)

    extract = interpreter.operation_request(state, "extract")
    interpreter.apply_stage_result(
        state,
        StageOperationResult(
            identity=extract.identity,
            disposition="completed",
            evaluation="cycle",
            next_objective="repair extraction",
            evaluation_contract_ref="evaluation:extract@1",
            objective_contract_ref="objective:repair-extraction@1",
        ),
    )
    assert state.stages["extract"].stage_cycle == 1
    extract_cycle = interpreter.operation_request(state, "extract")
    interpreter.apply_stage_result(
        state,
        StageOperationResult(
            identity=extract_cycle.identity,
            disposition="completed",
            output_refs=("artifact:extracted",),
            evaluation_contract_ref="evaluation:extract@1",
        ),
    )
    assert interpreter.runnable(state) == ("review_a", "review_b")

    review_a = interpreter.operation_request(state, "review_a")
    review_b = interpreter.operation_request(state, "review_b")
    interpreter.apply_stage_result(
        state,
        StageOperationResult(
            identity=review_a.identity,
            disposition="waiting",
            wait_condition_id="wait:review-a",
        ),
    )
    interpreter.apply_stage_result(
        state,
        StageOperationResult(
            identity=review_b.identity,
            disposition="completed",
        ),
    )
    assert interpreter.runnable(state) == ()
    interpreter.clear_wait(state, "wait:review-a")
    assert interpreter.runnable(state) == ("review_a",)


def test_interpreter_applies_declared_skip_and_completion_classes() -> None:
    blueprint = StageGraphBlueprint(
        logical_id="fixture.skip-policy",
        title="Declared skip policy",
        description="An optional failed stage deterministically skips its dependent.",
        stages=(
            StageNode(
                stage_id="optional_source",
                completion_class="optional",
                reservation={"operation.attempts": 1},
            ),
            StageNode(
                stage_id="optional_consumer",
                depends_on=frozenset({"optional_source"}),
                completion_class="optional",
                skip_policy="when_dependencies_unsatisfied",
                reservation={"operation.attempts": 1},
            ),
        ),
    )
    interpreter = StageGraphInterpreter(blueprint, effective_max_concurrency=1)
    state = interpreter.initial_state(ExecutionIdentity("run-skip"), run_version=1)
    source = interpreter.operation_request(state, "optional_source")
    interpreter.apply_stage_result(
        state,
        StageOperationResult(identity=source.identity, disposition="failed"),
    )

    assert interpreter.resolve_blocked(state)
    assert state.stages["optional_consumer"].status == "skipped"
    assert interpreter.graph_complete(state)
    assert not interpreter.graph_failed(state)


@pytest.mark.asyncio
async def test_lifecycle_gateway_uses_public_idempotent_run_control_commands() -> None:
    service, _repository = run_control_service()
    decision = await service.admit(run_request())
    assert decision.run_id is not None
    gateway = RunControlLifecycleGateway(
        service,
        StaticBindingVerifier(),
        actor(),
    )
    start_request = LifecycleCommandRequest(
        command_id="orchestration:start",
        expected_run_version=1,
        action={"kind": "start"},
        reason="Start admitted StageGraph",
        occurred_at=NOW,
        run_id=decision.run_id,
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        idempotency_issuer="stagegraph-worker",
        correlation_id="correlation:stagegraph",
        blueprint_digest=DIGEST,
    )
    first = await gateway.execute(start_request)
    replayed = await gateway.execute(start_request)

    assert first == replayed
    assert first.accepted
    assert first.resulting_run_version == 2

    reserved = await gateway.execute(
        LifecycleCommandRequest(
            command_id="orchestration:reserve:operation-1",
            expected_run_version=2,
            action={
                "kind": "reserve_budget",
                "reservation_id": "reservation:operation-1",
                "amounts": {"tokens.total": 5},
            },
            reason="Reserve before operation dispatch",
            occurred_at=NOW,
            run_id=decision.run_id,
            request_scope="tenant-1",
            effective_configuration_digest=DIGEST,
            idempotency_issuer="stagegraph-worker",
            correlation_id="correlation:stagegraph",
            blueprint_digest=DIGEST,
        )
    )
    reconciled = await gateway.execute(
        LifecycleCommandRequest(
            command_id="orchestration:usage:operation-1",
            expected_run_version=reserved.resulting_run_version,
            action={
                "kind": "record_usage",
                "usage_id": "usage:operation-1",
                "actual_amounts": {"tokens.total": 3},
                "reservation_id": "reservation:operation-1",
                "release_amounts": {"tokens.total": 2},
                "pending_external_amounts": {},
            },
            reason="Reconcile observed usage",
            occurred_at=NOW,
            run_id=decision.run_id,
            request_scope="tenant-1",
            effective_configuration_digest=DIGEST,
            idempotency_issuer="stagegraph-worker",
            correlation_id="correlation:stagegraph",
            blueprint_digest=DIGEST,
        )
    )
    assert reconciled.accepted
    assert reconciled.resulting_run_version == 4


@pytest.mark.asyncio
async def test_real_temporal_stagegraph_runs_stage_and_workflow_cycles() -> None:
    activities = FakeStageGraphActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="stagegraph-acceptance",
            workflows=[StageGraphWorkflow],
            activities=[
                activities.execute_operation,
                activities.evaluate_workflow,
                activities.apply_lifecycle_command,
            ],
        ):
            result = await environment.client.execute_workflow(
                StageGraphWorkflow.run,
                StageGraphRunInput(
                    run_id="run-stagegraph-acceptance",
                    request_scope="tenant-1",
                    effective_configuration_digest=DIGEST,
                    blueprint_digest=sha256_digest(orchestration_fixture()),
                    blueprint=orchestration_fixture().model_dump(mode="json"),
                    max_concurrency=2,
                ),
                id="run-stagegraph-acceptance",
                task_queue="stagegraph-acceptance",
            )
            history = await environment.client.get_workflow_handle(
                "run-stagegraph-acceptance"
            ).fetch_history()
        await Replayer(workflows=[StageGraphWorkflow]).replay_workflow(history)

    attempts = Counter(request.identity.stage_id for request in activities.operation_requests)
    assert attempts == {
        "prepare": 1,
        "extract": 2,
        "review_a": 2,
        # One infrastructure retry, but only one semantic operation attempt.
        "review_b": 2,
        "join": 2,
        "publish": 2,
    }
    review_b_requests = [
        request
        for request in activities.operation_requests
        if request.identity.stage_id == "review_b"
    ]
    assert len({request.idempotency_key for request in review_b_requests}) == 1
    assert result.operation_attempts["review_b"] == 1
    assert (
        next(
            item.temporal_activity_attempt
            for item in result.lineage
            if item.identity.stage_id == "review_b"
        )
        == 2
    )
    assert result.stage_cycles["extract"] == 1
    assert result.workflow_cycles == 1
    assert result.execution_epoch == 1
    assert "review_b" in result.reused_output_refs
    assert "prepare" in result.reused_output_refs
    assert "extract" in result.reused_output_refs
    assert "review_a" not in result.reused_output_refs
    assert result.output_refs["publish"] == ("artifact:publish:workflow-1:stage-0",)

    scheduled = [entry.split(":stage:")[1].split(":")[0] for entry in result.schedule_trace]
    assert scheduled == [
        "prepare",
        "extract",
        "extract",
        "review_a",
        "review_b",
        "join",
        "publish",
        "review_a",
        "join",
        "publish",
    ]
    reservations = [
        request
        for request in activities.lifecycle_requests
        if request.action["kind"] == "reserve_budget"
    ]
    assert len({request.command_id for request in reservations}) == len(reservations)
    assert any(
        request.action.get("amounts", {}).get("stage.cycles") == 1 for request in reservations
    )
    assert any(
        request.action.get("amounts", {}).get("workflow.cycles") == 1 for request in reservations
    )


@pytest.mark.asyncio
async def test_workflow_rejects_cycle_beyond_frozen_policy() -> None:
    class AlwaysCycle(FakeStageGraphActivities):
        @activity.defn(name="stagegraph.evaluate_workflow")
        async def evaluate_workflow(
            self, request: WorkflowEvaluationRequest
        ) -> WorkflowEvaluationResult:
            return WorkflowEvaluationResult(
                action="cycle",
                evaluation_ref=f"evaluation:cycle:{request.workflow_cycle}",
                invalidation_frontier=("review_a",),
                next_objective="attempt another bounded repair",
                evaluation_contract_ref=request.evaluation_contract_ref,
                objective_contract_ref=request.objective_contract_ref,
            )

    activities = AlwaysCycle()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="stagegraph-cycle-bound",
            workflows=[StageGraphWorkflow],
            activities=[
                activities.execute_operation,
                activities.evaluate_workflow,
                activities.apply_lifecycle_command,
            ],
        ):
            with pytest.raises(WorkflowFailureError) as failure:
                await environment.client.execute_workflow(
                    StageGraphWorkflow.run,
                    StageGraphRunInput(
                        run_id="run-stagegraph-cycle-bound",
                        request_scope="tenant-1",
                        effective_configuration_digest=DIGEST,
                        blueprint_digest=sha256_digest(orchestration_fixture()),
                        blueprint=orchestration_fixture().model_dump(mode="json"),
                        max_concurrency=2,
                    ),
                    id="run-stagegraph-cycle-bound",
                    task_queue="stagegraph-cycle-bound",
                )
            assert "workflow cycle limit exceeded" in str(failure.value.cause)
