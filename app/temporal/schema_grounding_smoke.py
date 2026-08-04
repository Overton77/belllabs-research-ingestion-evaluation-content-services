from __future__ import annotations

from dataclasses import dataclass

from temporalio.client import Client

from app.application.orchestration import RunControlLifecycleGateway
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
)
from app.application.orchestration_routing import SemanticHandlerRegistry
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_selection import ReviewAgentPort, SelectionAgentPort
from app.application.schema_context_stage_handlers import (
    parse_schema_grounding_record_ref,
    register_schema_context_stage_handlers,
)
from app.application.schema_grounding_repository import SchemaGroundingRecordRepository
from app.application.schema_grounding_semantic_handlers import (
    register_supporting_graph_goal_handlers,
)
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.domain.control_plane.canonical import sha256_digest
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
from app.temporal.coordinator_runtime import create_routed_coordinator_activities
from app.temporal.goal_directed_activities import create_goal_directed_worker
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow
from app.temporal.orchestration_activities import create_stagegraph_worker
from app.temporal.stagegraph_workflow import StageGraphWorkflow


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
    activities = create_routed_coordinator_activities(
        bindings=bindings,
        handlers=handlers,
        lifecycle=lifecycle,
    )
    worker = create_stagegraph_worker(
        client,
        task_queue=task_queue,
        activities=activities.stagegraph,
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
    records: SchemaGroundingRecordRepository,
    reconciliation: SupportingGraphReconciliationWorkflow,
) -> SupportingGraphTemporalSmokeResult:
    """Run exact Scenario C semantics through the generic GoalDirected workflow."""

    blueprint = GoalDirectedBlueprint.model_validate(run_input.blueprint)
    _verify_shared_authority(run_input, semantic_binding)
    if blueprint.logical_id != "supporting-graph-reconciliation-goal-directed-v1":
        raise ValueError(
            "Scenario C smoke requires supporting-graph-reconciliation-goal-directed-v1"
        )
    handlers = SemanticHandlerRegistry()
    register_supporting_graph_goal_handlers(
        handlers,
        workflow=reconciliation,
        records=records,
    )
    persisted = await bindings.create(semantic_binding)
    if persisted.binding_digest != semantic_binding.binding_digest:
        raise ValueError("Scenario C binding persistence changed immutable authority")
    activities = create_routed_coordinator_activities(
        bindings=bindings,
        handlers=handlers,
        lifecycle=lifecycle,
    )
    worker = create_goal_directed_worker(
        client,
        task_queue=task_queue,
        activities=activities.goal_directed,
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
        ref
        for execution in result.execution_results
        for ref in execution.output_refs
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
    blueprint = (
        StageGraphBlueprint.model_validate(run_input.blueprint)
        if isinstance(run_input, StageGraphRunInput)
        else GoalDirectedBlueprint.model_validate(run_input.blueprint)
    )
    if (
        sha256_digest(blueprint) != run_input.blueprint_digest
        or binding.binding_id != run_input.semantic_input_binding_ref
        or binding.request_scope != run_input.request_scope
        or binding.run_id != run_input.run_id
        or binding.effective_configuration_digest
        != run_input.effective_configuration_digest
        or binding.blueprint_digest != run_input.blueprint_digest
    ):
        raise ValueError("Temporal smoke input and semantic binding authority differ")


def _submitter(
    client: Client,
    task_queue: str,
    *,
    stagegraph: bool,
) -> TemporalWorkflowSubmitter:
    return TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue=(
            task_queue if stagegraph else f"{task_queue}-unused-stagegraph"
        ),
        goal_directed_task_queue=(
            f"{task_queue}-unused-goal" if stagegraph else task_queue
        ),
    )


__all__ = [
    "SchemaContextTemporalSmokeResult",
    "SupportingGraphTemporalSmokeResult",
    "run_schema_context_stagegraph_smoke",
    "run_supporting_graph_goal_smoke",
]
