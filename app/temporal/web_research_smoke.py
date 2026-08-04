from __future__ import annotations

from dataclasses import dataclass

from temporalio.client import Client
from temporalio.worker import Worker

from app.application.coordinator_results import TerminalWorkflowCompletionPort
from app.application.orchestration import RunControlLifecycleGateway
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
)
from app.application.orchestration_routing import (
    BoundStageOperationExecutor,
    BoundWorkflowEvaluator,
    OperationExecutionBindingReader,
    SemanticHandlerRegistry,
)
from app.application.web_research_repository import web_research_record_ref
from app.application.web_research_semantic_handlers import (
    WebResearchHandlerDependencies,
    register_web_research_stagegraph_handlers,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import StageGraphBlueprint
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.orchestration.bindings import RunSemanticInputBinding
from app.domain.orchestration.contracts import StageGraphRunInput, StageGraphRunResult
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal.orchestration_activities import (
    StageGraphActivities,
    create_stagegraph_worker,
)
from app.temporal.stagegraph_workflow import StageGraphWorkflow


@dataclass(frozen=True)
class WebResearchTemporalSmokeResult:
    workflow_id: str
    temporal_run_id: str | None
    run_result: StageGraphRunResult
    final_result_ref: str
    exact_evidence_refs: tuple[str, ...]


def create_web_research_stagegraph_worker(
    client: Client,
    *,
    task_queue: str,
    bindings: RunSemanticInputBindingRepository,
    lifecycle: RunControlLifecycleGateway,
    dependencies: WebResearchHandlerDependencies,
    operation_bindings: OperationExecutionBindingReader,
    completion: TerminalWorkflowCompletionPort | None = None,
) -> Worker:
    """Compose a real Scenario D worker; every unknown semantic route fails closed."""

    handlers = SemanticHandlerRegistry()
    register_web_research_stagegraph_handlers(handlers, dependencies)
    return create_stagegraph_worker(
        client,
        task_queue=task_queue,
        activities=StageGraphActivities(
            operation_executor=BoundStageOperationExecutor(bindings, handlers, operation_bindings),
            workflow_evaluator=BoundWorkflowEvaluator(bindings, handlers, operation_bindings),
            lifecycle_gateway=lifecycle,
            completion=completion,
        ),
    )


async def run_web_research_stagegraph_smoke(
    client: Client,
    *,
    task_queue: str,
    workflow_id: str,
    run_input: StageGraphRunInput,
    semantic_binding: RunSemanticInputBinding,
    bindings: RunSemanticInputBindingRepository,
    lifecycle: RunControlLifecycleGateway,
    dependencies: WebResearchHandlerDependencies,
    operation_bindings: OperationExecutionBindingReader,
) -> WebResearchTemporalSmokeResult:
    """Persist exact binding, run one local Temporal worker, and return durable refs."""

    _verify_smoke_authority(run_input, semantic_binding)
    persisted = await bindings.create(semantic_binding)
    if persisted.binding_digest != semantic_binding.binding_digest:
        raise ValueError("Temporal smoke binding persistence changed immutable authority")
    worker = create_web_research_stagegraph_worker(
        client,
        task_queue=task_queue,
        bindings=bindings,
        lifecycle=lifecycle,
        dependencies=dependencies,
        operation_bindings=operation_bindings,
    )
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue=task_queue,
        goal_directed_task_queue=f"{task_queue}-unused-goal-directed",
    )
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
    final_refs = result.output_refs.get("promote_verified_result", ())
    if len(final_refs) != 1:
        raise RuntimeError("Scenario D Temporal smoke did not produce one verified result")
    final = await dependencies.records.get(
        run_input.request_scope,
        run_input.run_id,
        final_refs[0],
    )
    if final.record_kind != "verified_result" or web_research_record_ref(final) != final_refs[0]:
        raise RuntimeError(
            "Scenario D Temporal smoke final reference is not durable verified evidence"
        )
    evidence_refs = tuple(
        output_ref
        for stage_id in (
            "admit_public_goal",
            "search_firecrawl",
            "search_tavily",
            "synthesize_citations",
            "browser_verify",
            "promote_verified_result",
        )
        for output_ref in result.output_refs.get(stage_id, ())
    )
    return WebResearchTemporalSmokeResult(
        workflow_id=submission.workflow_id,
        temporal_run_id=submission.temporal_run_id,
        run_result=result,
        final_result_ref=final_refs[0],
        exact_evidence_refs=evidence_refs,
    )


def _verify_smoke_authority(
    run_input: StageGraphRunInput,
    binding: RunSemanticInputBinding,
) -> None:
    blueprint = StageGraphBlueprint.model_validate(run_input.blueprint)
    if blueprint.logical_id != "web-research-browser-verification-v1":
        raise ValueError("Temporal smoke requires the exact Scenario D StageGraph")
    if sha256_digest(blueprint) != run_input.blueprint_digest:
        raise ValueError("Temporal smoke StageGraph digest mismatch")
    if (
        binding.blueprint_family != "StageGraph"
        or binding.binding_id != run_input.semantic_input_binding_ref
        or binding.request_scope != run_input.request_scope
        or binding.run_id != run_input.run_id
        or binding.blueprint_digest != run_input.blueprint_digest
        or binding.effective_configuration_digest != run_input.effective_configuration_digest
    ):
        raise ValueError(
            "Temporal smoke input and semantic binding do not share exact run authority"
        )
