from __future__ import annotations

from dataclasses import dataclass

from temporalio.client import Client

from app.application.orchestration import RunControlLifecycleGateway
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
)
from app.application.orchestration_routing import (
    BoundStageOperationExecutor,
    BoundWorkflowEvaluator,
    SemanticHandlerRegistry,
)
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_selection import ReviewAgentPort, SelectionAgentPort
from app.application.schema_context_stage_handlers import (
    parse_schema_grounding_record_ref,
    register_schema_context_stage_handlers,
)
from app.application.schema_grounding_repository import SchemaGroundingRecordRepository
from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    StageGraphBlueprint,
)
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.orchestration.bindings import RunSemanticInputBinding
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalDirectedRunResult,
    StageGraphRunInput,
    StageGraphRunResult,
)
from app.integrations.control_plane_payloads import ContentAddressedPayloadStore
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal.activities.goal_directed import (
    compose_goal_directed_activities,
    create_goal_directed_worker,
)
from app.temporal.coordinator_runtime import GoalDirectedCoordinatorDependencies
from app.temporal.orchestration_activities import (
    StageGraphActivities,
    create_stagegraph_worker,
)
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow


@dataclass(frozen=True)
class SchemaContextTemporalSmokeResult:
    workflow_id: str
    temporal_run_id: str | None
    result: StageGraphRunResult
    accepted_selection_ref: str


@dataclass(frozen=True)
class SupportingGraphTemporalSmokeResult:
    workflow_id: str
    temporal_run_id: str | None
    result: GoalDirectedRunResult
    reconciliation_refs: tuple[str, ...]


async def run_schema_context_stagegraph_smoke(
    client: Client,
    *,
    task_queue: str,
    workflow_id: str,
    run_input: StageGraphRunInput,
    semantic_binding: RunSemanticInputBinding,
    bindings: RunSemanticInputBindingRepository,
    lifecycle: RunControlLifecycleGateway,
    records: SchemaGroundingRecordRepository,
    catalog_builds: SchemaCatalogBuildService,
    sources: ContentAddressedPayloadStore,
    catalog_payloads: ContentAddressedPayloadStore,
    selector: SelectionAgentPort,
    reviewer: ReviewAgentPort,
) -> SchemaContextTemporalSmokeResult:
    """Run the exact Scenario A graph through real routed Temporal activities."""

    blueprint = StageGraphBlueprint.model_validate(run_input.blueprint)
    _verify_shared_authority(run_input, semantic_binding)
    if blueprint.logical_id != "schema-context-selection-v1":
        raise ValueError("Scenario A smoke requires schema-context-selection-v1")
    handlers = SemanticHandlerRegistry()
    register_schema_context_stage_handlers(
        handlers,
        catalog_builds=catalog_builds,
        sources=sources,
        catalog_payloads=catalog_payloads,
        records=records,
        selector=selector,
        reviewer=reviewer,
    )
    persisted = await bindings.create(semantic_binding)
    if persisted.binding_digest != semantic_binding.binding_digest:
        raise ValueError("Scenario A binding persistence changed immutable authority")
    activities = StageGraphActivities(
        operation_executor=BoundStageOperationExecutor(bindings, handlers),
        workflow_evaluator=BoundWorkflowEvaluator(bindings, handlers),
        lifecycle_gateway=lifecycle,
    )
    worker = create_stagegraph_worker(
        client,
        task_queue=task_queue,
        activities=activities,
    )
    submitter = _submitter(client, task_queue, stagegraph=True)
    async with worker:
        submission = await submitter.submit(
            run_input,
            workflow_id=workflow_id,
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
        )
        result = await client.get_workflow_handle_for(
            StageGraphWorkflow.run,
            submission.workflow_id,
        ).result()
    outputs = result.output_refs.get("accept_selection", ())
    if len(outputs) != 1:
        raise RuntimeError("Scenario A did not produce one accepted selection reference")
    parsed = parse_schema_grounding_record_ref(outputs[0])
    if parsed is None or parsed[0] != "accepted_selection":
        raise RuntimeError("Scenario A final output is not accepted selection evidence")
    return SchemaContextTemporalSmokeResult(
        workflow_id=submission.workflow_id,
        temporal_run_id=submission.temporal_run_id,
        result=result,
        accepted_selection_ref=outputs[0],
    )


async def run_supporting_graph_goal_smoke(
    client: Client,
    *,
    task_queue: str,
    workflow_id: str,
    run_input: GoalDirectedRunInput,
    semantic_binding: RunSemanticInputBinding,
    bindings: RunSemanticInputBindingRepository,
    lifecycle: RunControlLifecycleGateway,
    goal_directed: GoalDirectedCoordinatorDependencies,
) -> SupportingGraphTemporalSmokeResult:
    """Run exact Scenario C semantics through the canonical GoalDirected OperationWorkflow path."""

    del bindings  # binding authority is admitted before smoke; templates live on goal_directed
    blueprint = GoalDirectedBlueprint.model_validate(run_input.blueprint)
    _verify_shared_authority(run_input, semantic_binding)
    if blueprint.logical_id != "supporting-graph-reconciliation-goal-directed-v1":
        raise ValueError(
            "Scenario C smoke requires supporting-graph-reconciliation-goal-directed-v1"
        )
    if not run_input.semantic_input_binding_ref:
        raise ValueError("Scenario C smoke requires an admitted semantic_input_binding_ref")
    if run_input.semantic_input_binding_ref != semantic_binding.binding_id:
        raise ValueError("Scenario C run input is not bound to the admitted semantic binding")
    activities = compose_goal_directed_activities(
        run_control=goal_directed.run_control,
        operation_bindings=goal_directed.operation_bindings,
        templates=goal_directed.templates,
        documents=goal_directed.documents,
        lifecycle=lifecycle,
        actor=goal_directed.actor,
    )
    worker = create_goal_directed_worker(
        client,
        task_queue=task_queue,
        activities=activities,
    )
    submitter = _submitter(client, task_queue, stagegraph=False)
    async with worker:
        submission = await submitter.submit(
            run_input,
            workflow_id=workflow_id,
            blueprint_family=BlueprintFamily.GOAL_DIRECTED,
        )
        result = await client.get_workflow_handle_for(
            GoalDirectedWorkflow.run,
            submission.workflow_id,
        ).result()
    refs = tuple(
        ref for execution in result.execution_results for ref in execution.output_refs
    )
    if result.stop_reason != "verified_completion" or not refs:
        raise RuntimeError("Scenario C did not independently verify reconciliation")
    return SupportingGraphTemporalSmokeResult(
        workflow_id=submission.workflow_id,
        temporal_run_id=submission.temporal_run_id,
        result=result,
        reconciliation_refs=refs,
    )


def _verify_shared_authority(
    run_input: StageGraphRunInput | GoalDirectedRunInput,
    binding: RunSemanticInputBinding,
) -> None:
    if (
        run_input.run_id != binding.run_id
        or run_input.request_scope != binding.request_scope
        or run_input.effective_configuration_digest != binding.effective_configuration_digest
        or run_input.blueprint_digest != binding.blueprint_digest
    ):
        raise ValueError("semantic binding does not match the frozen Temporal run authority")


def _submitter(
    client: Client,
    task_queue: str,
    *,
    stagegraph: bool,
) -> TemporalWorkflowSubmitter:
    family_queue = task_queue
    return TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue=family_queue if stagegraph else f"{task_queue}-unused-stagegraph",
        goal_directed_task_queue=(
            family_queue if not stagegraph else f"{task_queue}-unused-goal-directed"
        ),
        root_task_queue=task_queue,
    )


__all__ = [
    "SchemaContextTemporalSmokeResult",
    "SupportingGraphTemporalSmokeResult",
    "run_schema_context_stagegraph_smoke",
    "run_supporting_graph_goal_smoke",
]
