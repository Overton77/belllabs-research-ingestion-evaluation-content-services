from __future__ import annotations

import inspect
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from app.application.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    InMemoryGoalOperationTemplateRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED, GENERIC_STAGE_GRAPH
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.orchestration.contracts import (
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.domain.run_control.contracts import ActorContext
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal import worker as production_worker
from app.temporal.activities.goal_directed import GoalDirectedActivities
from app.temporal.coordinator_runtime import (
    CoordinatorWorkerActivities,
    coordinator_task_queues,
    coordinator_worker_readiness,
    create_coordinator_workers,
)
from app.temporal.orchestration_activities import StageGraphActivities
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow

DIGEST = "sha256:" + "a" * 64


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


class UnusedCompletion:
    async def complete(self, _completion):
        raise AssertionError("typed completion should not run for worker-validation inputs")


class AcceptingLifecycle:
    async def execute(self, request):
        from app.domain.orchestration.contracts import LifecycleCommandOutcome
        from app.domain.run_control.contracts import RunOutcome

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
                RunOutcome.COMPLETED if request.action["kind"] == "terminalize" else None
            ),
        )


def _goal_directed_activities() -> GoalDirectedActivities:
    return GoalDirectedActivities(
        operations=GoalDirectedOperationPreparationService(
            templates=InMemoryGoalOperationTemplateRepository(),
            operation_bindings=cast(Any, object()),
            run_control=cast(Any, object()),
            documents=cast(Any, object()),
            actor=ActorContext(
                actor_id="coordinator-runtime-test",
                permissions=frozenset({"workflow_run.goal_directed"}),
            ),
        ),
        results=GoalDirectedOperationResultService(cast(Any, object())),
        lifecycle=AcceptingLifecycle(),  # type: ignore[arg-type]
        completion=UnusedCompletion(),  # type: ignore[arg-type]
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
        create_agent_cognitive_worker,
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
            decision_service=cast(Any, object()),
            operation_materializer=cast(Any, object()),
            lifecycle_gateway=AcceptingLifecycle(),
            completion=UnusedCompletion(),
        ),
        goal_directed=_goal_directed_activities(),
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
            create_agent_cognitive_worker(
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
async def test_submitter_rejects_family_input_mismatch_before_temporal_call() -> None:
    submitter = TemporalWorkflowSubmitter(
        cast(Client, object()),
        stagegraph_task_queue="stagegraph",
        goal_directed_task_queue="goal-directed",
    )
    from app.domain.orchestration.contracts import StageGraphRunInput

    stage_input = StageGraphRunInput(
        run_id="run-coordinator-stagegraph",
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        workflow_type_digest=DIGEST,
        blueprint_digest=sha256_digest(GENERIC_STAGE_GRAPH),
        blueprint=GENERIC_STAGE_GRAPH.model_dump(mode="json"),
    )
    with pytest.raises(ValueError, match="GoalDirected submission requires"):
        await submitter.submit(
            stage_input,
            workflow_id="wrong-family",
            blueprint_family=BlueprintFamily.GOAL_DIRECTED,
        )


def test_canonical_family_workflows_require_operation_workflow_children() -> None:
    assert "OperationWorkflow" in inspect.getsource(StageGraphWorkflow)
    assert "OperationWorkflow" in inspect.getsource(GoalDirectedWorkflow)
    assert GENERIC_GOAL_DIRECTED.logical_id.startswith("fixture.")
    assert GENERIC_STAGE_GRAPH.logical_id.startswith("fixture.")


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
        "coordinator-coordinator-family-stagegraph",
        "coordinator-coordinator-family-goal-directed",
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
    with pytest.raises(
        RuntimeError,
        match="requires a deployment WorkerActivityCompositionFactory",
    ):
        await production_worker.main()
