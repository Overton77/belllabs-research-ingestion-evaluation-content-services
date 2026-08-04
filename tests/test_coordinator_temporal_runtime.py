from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    StageGraphBlueprint,
    StageNode,
)
from app.domain.coordinator.launch import BlueprintFamily, WorkflowResultRecord
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalRevision,
    GoalVerificationRequest,
    GoalVerificationResult,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphRunInput,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.domain.run_control.contracts import RunOutcome
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal import worker as production_worker
from app.temporal.coordinator_runtime import (
    CoordinatorWorkerActivities,
    coordinator_task_queues,
    coordinator_worker_readiness,
    create_coordinator_workers,
)
from app.temporal.goal_directed_activities import GoalDirectedActivities
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow
from app.temporal.orchestration_activities import StageGraphActivities
from app.temporal.stagegraph_workflow import StageGraphWorkflow
from app.temporal.workflow_sandbox import coordinator_workflow_runner

DIGEST = "sha256:" + "a" * 64
PROTECTED_SCOPE = sha256_digest("coordinator-runtime-protected-scope")


class CompletingStageExecutor:
    async def execute(self, request: StageOperationRequest) -> StageOperationResult:
        return StageOperationResult(
            identity=request.identity,
            disposition="completed",
            output_refs=(f"artifact:{request.identity.stage_id}",),
            evaluation_ref=f"evaluation:{request.identity.semantic_key}",
        )


class AcceptingWorkflowEvaluator:
    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
    ) -> WorkflowEvaluationResult:
        return WorkflowEvaluationResult(
            action="accept",
            evaluation_ref=f"evaluation:workflow:{request.workflow_cycle}",
            evaluation_contract_ref=request.evaluation_contract_ref,
        )


class CompletingGoalExecutor:
    async def execute(self, claim: GoalExecutionClaim) -> GoalExecutionResult:
        return GoalExecutionResult(
            identity=claim.identity,
            disposition="completed",
            output_refs=("artifact:goal-result",),
            completion_claim=True,
            actual_usage={"goal.iterations": 1},
        )


class AcceptingGoalVerifier:
    async def verify(
        self,
        request: GoalVerificationRequest,
    ) -> GoalVerificationResult:
        return GoalVerificationResult(
            identity=request.claim.identity,
            action="verified_completion",
            verification_ref="verification:accepted",
            verifier_ref=request.verifier_ref,
            acceptance_contract_ref=request.acceptance_contract_ref,
            progress_made=True,
            evidence_refs=request.execution_result.output_refs,
        )


class UnusedHandoffPreparer:
    async def prepare(self, _request: GoalHandoffRequest) -> GoalHandoffResult:
        raise AssertionError("verified first-iteration completion must not prepare a handoff")


class UnusedCompletion:
    async def complete(self, _completion):
        raise AssertionError("typed completion should not run for legacy test inputs")


class RecordingCompletion:
    def __init__(self) -> None:
        self.records: list[WorkflowResultRecord] = []

    async def complete(self, completion):
        record = WorkflowResultRecord.model_validate(completion.model_dump())
        self.records.append(record)
        return record


class AcceptingLifecycle:
    async def execute(self, request: LifecycleCommandRequest) -> LifecycleCommandOutcome:
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="terminal" if request.action["kind"] == "terminalize" else "active",
            reason_code="accepted",
            evidence_frontier_digest=DIGEST,
            obligation_revision=DIGEST,
            accepted_obligation_evidence_digest=DIGEST,
            required_obligations_accepted=True,
            terminal_outcome=(
                RunOutcome.COMPLETED
                if request.action["kind"] == "terminalize"
                else None
            ),
        )


def stagegraph_input(*, materialize: bool = False) -> StageGraphRunInput:
    blueprint = StageGraphBlueprint(
        logical_id="test.coordinator-stagegraph",
        title="Coordinator StageGraph runtime test",
        description="One operation proves exact family worker registration.",
        stages=(
            StageNode(
                stage_id="execute",
                reservation={"operation.attempts": 1},
                output_slots=frozenset({"result"}),
            ),
        ),
        declared_output_slots=frozenset({"result"}),
    )
    return StageGraphRunInput(
        run_id="run-coordinator-stagegraph",
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        tenant_scope="tenant-1" if materialize else "",
        materialize_typed_result=materialize,
    )


def goal_directed_input(*, materialize: bool = False) -> GoalDirectedRunInput:
    blueprint = GoalDirectedBlueprint(
        logical_id="test.coordinator-goal-directed",
        title="Coordinator GoalDirected runtime test",
        description="One verified iteration proves exact family worker registration.",
        objective_contract="objective:test@1",
        acceptance_contract="acceptance:test@1",
        max_iterations=1,
    )
    revision = GoalRevision(
        revision_id="goal-revision:1",
        revision=1,
        parent_revision_id=None,
        protected_scope_digest=PROTECTED_SCOPE,
        objective="Produce one independently verified result.",
        evidence_refs=("input:test",),
        unmet_obligations=(),
        author="test",
        deciding_authority="authority:test",
        applicability="remaining_run",
    )
    return GoalDirectedRunInput(
        run_id="run-coordinator-goal-directed",
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        protected_scope_digest=PROTECTED_SCOPE,
        initial_revision=revision,
        tenant_scope="tenant-1" if materialize else "",
        materialize_typed_result=materialize,
    )


@pytest.mark.asyncio
async def test_fastmcp_first_all_application_worker_factories_validate() -> None:
    """FastMCP import hooks must never poison later Temporal worker validation."""

    import importlib

    importlib.import_module("fastmcp")
    importlib.import_module("app.mcp.coordinator_server")

    from app.temporal.artifact_activities import (
        ArtifactPromotionActivities,
        create_generic_artifact_worker,
    )
    from app.temporal.linked_run_activities import (
        LinkedRunActivities,
        create_linked_run_worker,
    )
    from app.temporal.operation_activities import (
        OperationExecutionActivities,
        create_operation_worker,
    )
    from app.temporal.schema_grounding_activities import (
        SchemaGroundingActivities,
        create_schema_grounding_activity_worker,
    )

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    operations = OperationExecutionActivities(cast(Any, object()))
    artifacts = ArtifactPromotionActivities(
        service=cast(Any, object()),
        candidates=cast(Any, object()),
    )
    linked_runs = LinkedRunActivities(cast(Any, object()))
    schema_grounding = SchemaGroundingActivities(
        catalog_builds=cast(Any, object()),
        derivations=cast(Any, object()),
        reconciliations=cast(Any, object()),
    )
    activities = CoordinatorWorkerActivities(
        stagegraph=StageGraphActivities(
            CompletingStageExecutor(),
            AcceptingWorkflowEvaluator(),
            AcceptingLifecycle(),
            completion=UnusedCompletion(),
        ),
        goal_directed=GoalDirectedActivities(
            executor=CompletingGoalExecutor(),
            verifier=AcceptingGoalVerifier(),
            handoffs=UnusedHandoffPreparer(),
            lifecycle=AcceptingLifecycle(),
            completion=UnusedCompletion(),
        ),
    )
    queues = coordinator_task_queues("fastmcp-first-worker-validation")

    async with environment:
        coordinator_workers = create_coordinator_workers(
            environment.client,
            task_queues=queues,
            activities=activities,
        )
        workers = (
            create_generic_artifact_worker(
                environment.client,
                task_queue="fastmcp-first-artifact",
                operations=operations,
                artifacts=artifacts,
            ),
            create_operation_worker(
                environment.client,
                task_queue="fastmcp-first-operation",
                activities=operations,
            ),
            create_linked_run_worker(
                environment.client,
                task_queue="fastmcp-first-linked-run",
                activities=linked_runs,
            ),
            create_schema_grounding_activity_worker(
                environment.client,
                task_queue="fastmcp-first-schema-activities",
                activities=schema_grounding,
            ),
            *coordinator_workers.workers,
        )
        for worker in workers:
            async with worker:
                pass


@pytest.mark.asyncio
async def test_family_submitter_and_worker_set_run_and_replay_both_families() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    queues = coordinator_task_queues("coordinator-runtime-test")
    completion = RecordingCompletion()
    activities = CoordinatorWorkerActivities(
        stagegraph=StageGraphActivities(
            CompletingStageExecutor(),
            AcceptingWorkflowEvaluator(),
            AcceptingLifecycle(),
            completion=completion,
        ),
        goal_directed=GoalDirectedActivities(
            executor=CompletingGoalExecutor(),
            verifier=AcceptingGoalVerifier(),
            handoffs=UnusedHandoffPreparer(),
            lifecycle=AcceptingLifecycle(),
            completion=completion,
        ),
    )
    async with environment:
        workers = create_coordinator_workers(
            environment.client,
            task_queues=queues,
            activities=activities,
        )
        submitter = TemporalWorkflowSubmitter(
            environment.client,
            stagegraph_task_queue=queues.stagegraph,
            goal_directed_task_queue=queues.goal_directed,
        )
        async with workers.stagegraph, workers.goal_directed:
            stage_submission = await submitter.submit(
                stagegraph_input(materialize=True),
                workflow_id="coordinator-stagegraph-runtime-test",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
            )
            goal_submission = await submitter.submit(
                goal_directed_input(materialize=True),
                workflow_id="coordinator-goal-directed-runtime-test",
                blueprint_family=BlueprintFamily.GOAL_DIRECTED,
            )
            stage_result = await environment.client.get_workflow_handle_for(
                StageGraphWorkflow.run,
                stage_submission.workflow_id,
            ).result()
            goal_result = await environment.client.get_workflow_handle_for(
                GoalDirectedWorkflow.run,
                goal_submission.workflow_id,
            ).result()
            repeated_stage_submission = await submitter.submit(
                stagegraph_input(materialize=True),
                workflow_id=stage_submission.workflow_id,
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
            )
            stage_history = await environment.client.get_workflow_handle(
                stage_submission.workflow_id
            ).fetch_history()
            goal_history = await environment.client.get_workflow_handle(
                goal_submission.workflow_id
            ).fetch_history()

        await Replayer(
            workflows=[StageGraphWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(stage_history)
        await Replayer(
            workflows=[GoalDirectedWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(goal_history)

    assert stage_submission.temporal_run_id
    assert goal_submission.temporal_run_id
    assert repeated_stage_submission == stage_submission
    assert stage_result.output_refs == {"execute": ("artifact:execute",)}
    assert goal_result.stop_reason == "verified_completion"
    assert {record.blueprint_family for record in completion.records} == {
        BlueprintFamily.STAGE_GRAPH,
        BlueprintFamily.GOAL_DIRECTED,
    }


@pytest.mark.asyncio
async def test_submitter_rejects_family_input_mismatch_before_temporal_call() -> None:
    submitter = TemporalWorkflowSubmitter(
        cast(Client, object()),
        stagegraph_task_queue="stagegraph",
        goal_directed_task_queue="goal-directed",
    )
    with pytest.raises(ValueError, match="GoalDirected submission requires"):
        await submitter.submit(
            stagegraph_input(),
            workflow_id="wrong-family",
            blueprint_family=BlueprintFamily.GOAL_DIRECTED,
        )


@dataclass
class FakeDescribeResponse:
    pollers: list[object]


class FakeWorkflowService:
    def __init__(self) -> None:
        self.queues: list[str] = []

    async def describe_task_queue(self, request, **_kwargs):
        self.queues.append(request.task_queue.name)
        return FakeDescribeResponse(pollers=[object()])


class FakeReadinessClient:
    namespace = "default"

    def __init__(self) -> None:
        self.workflow_service = FakeWorkflowService()


@pytest.mark.asyncio
async def test_worker_readiness_requires_actual_pollers_for_each_exact_queue() -> None:
    client = FakeReadinessClient()
    queues = coordinator_task_queues("coordinator")
    readiness = await coordinator_worker_readiness(
        cast(Client, client),
        task_queues=queues,
    )

    assert [item.family for item in readiness] == ["StageGraph", "GoalDirected"]
    assert all(item.available for item in readiness)
    assert client.workflow_service.queues == [
        "coordinator-stagegraph",
        "coordinator-goal-directed",
    ]


@pytest.mark.asyncio
async def test_production_worker_fails_closed_before_startup_without_real_adapters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        production_worker,
        "get_settings",
        lambda: SimpleNamespace(coordinator_launch_enabled=True),
    )
    with pytest.raises(RuntimeError, match="requires concrete StageGraph and GoalDirected"):
        await production_worker.main()
