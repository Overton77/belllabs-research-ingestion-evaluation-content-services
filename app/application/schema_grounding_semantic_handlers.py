from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.application.goal_directed import GoalOperationTemplateRepository
from app.application.orchestration_routing import SemanticRoutingError
from app.application.semantic_operation_bindings import (
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    EffectiveRunConfiguration,
    GoalDirectedBlueprint,
)
from app.domain.coordinator.launch import (
    BlueprintFamily,
    PreparedLaunchTicket,
    SemanticBindingPlan,
    WorkflowLaunchProposal,
)
from app.domain.orchestration.bindings import (
    GoalOperationHandlerBinding,
    RunSemanticInputBinding,
    SemanticHandlerBinding,
    SemanticInputPayload,
)
from app.domain.schema_context.contracts import GraphReconciliationEvidence
from app.domain.schema_grounding.contracts import (
    SupportingGraphReconciliationRecord,
    SupportingGraphReconciliationRequest,
)

SUPPORTING_GRAPH_ITERATION_HANDLER = "schema-grounding.reconcile"
SUPPORTING_GRAPH_VERIFIER_HANDLER = "schema-grounding.verify-reconciliation"
SUPPORTING_GRAPH_HANDOFF_HANDLER = "schema-grounding.reconciliation-handoff"
SUPPORTING_GRAPH_HANDLER_REVISION = 1


class SupportingGraphBindingPlanInput(BaseModel):
    """Pre-admission inputs required to author Scenario C's run binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request: SupportingGraphReconciliationRequest
    evidence: GraphReconciliationEvidence | None = None
    minimum_successful_intents: int = Field(default=1, ge=1)
    handoff_instructions: str = Field(min_length=1)
    operation_bindings: SemanticOperationBindingTemplates
    created_at: datetime


class SupportingGraphSemanticBindingProvider:
    """Freeze and author the exact GoalDirected Scenario C semantic authority."""

    def __init__(
        self,
        inputs: SupportingGraphBindingPlanInput,
        operation_bindings: SemanticOperationExecutionBindingService,
        operation_templates: GoalOperationTemplateRepository,
    ) -> None:
        self._inputs = inputs
        self._operation_bindings = operation_bindings
        self._operation_templates = operation_templates

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
    ) -> SemanticBindingPlan:
        blueprint = configuration.selected_blueprint
        if (
            configuration.workflow_type.logical_id != "supporting-graph-reconciliation"
            or not isinstance(blueprint, GoalDirectedBlueprint)
            or blueprint.logical_id != "supporting-graph-reconciliation-goal-directed-v1"
            or blueprint.acceptance_contract != "evaluation:supporting-graph-reconciliation:v1"
        ):
            raise SemanticRoutingError(
                "supporting-graph binding provider requires the exact published "
                "supporting-graph-reconciliation GoalDirected blueprint"
            )
        if (
            proposal.request_scope != self._inputs.request.request_scope
            or proposal.request_scope != proposal.compilation.context.authority_scope
        ):
            raise SemanticRoutingError(
                "supporting-graph binding inputs belong to a different request scope"
            )
        request = self._inputs.request
        if set(self._inputs.operation_bindings.operations) != {
            "goal_executor",
            "goal_verifier",
        }:
            raise SemanticRoutingError(
                "supporting-graph GoalDirected execution requires exact executor and "
                "independent-verifier Operation Execution Request templates"
            )
        exact_refs: tuple[str, ...] = (
            (
                "schema-deployment-manifest:"
                f"{request.admission.deployment_manifest.manifest_id}@"
                f"{request.admission.deployment_manifest.manifest_digest}"
            )
            if request.admission.deployment_manifest is not None
            else "schema-deployment-manifest:missing",
        )
        exact_refs += (
            (
                "schema-workspace-binding:"
                f"{request.admission.workspace_binding.binding_id}@"
                f"{request.admission.workspace_binding.binding_digest}"
            )
            if request.admission.workspace_binding is not None
            else "schema-workspace-binding:missing",
            (
                "graph-capability-grant:"
                f"{request.admission.graph_capability.grant_id}@"
                f"{request.admission.graph_capability.grant_digest}"
            )
            if request.admission.graph_capability is not None
            else "graph-capability-grant:missing",
            f"schema-operation-projection:{request.projection.projection_id}@"
            f"{request.projection.projection_digest}",
            *(
                f"graph-query-intent:{intent.intent_id}@"
                f"{sha256_digest(intent.model_dump(mode='json'))}"
                for intent in request.intents
            ),
            *(
                "operation-execution-request-template:"
                f"{operation_id}@{sha256_digest(template.model_dump(mode='json'))}"
                for operation_id, template in sorted(
                    self._inputs.operation_bindings.operations.items()
                )
            ),
        )
        return SemanticBindingPlan.create(
            plan_ref=(
                "semantic-binding-plan:supporting-graph-reconciliation:" + request.reconciliation_id
            ),
            blueprint_family=BlueprintFamily.GOAL_DIRECTED,
            exact_input_refs=exact_refs,
            payload=self._inputs.model_dump(mode="json"),
        )

    async def author(
        self,
        plan: SemanticBindingPlan,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
    ) -> RunSemanticInputBinding:
        if (
            plan.blueprint_family != BlueprintFamily.GOAL_DIRECTED
            or ticket.blueprint_family != BlueprintFamily.GOAL_DIRECTED
            or plan.plan_ref != ticket.semantic_binding_plan_ref
            or plan.plan_digest != ticket.semantic_binding_plan_digest
            or ticket.workflow_type_ref.logical_id != "supporting-graph-reconciliation"
            or ticket.blueprint_ref.logical_id != "supporting-graph-reconciliation-goal-directed-v1"
        ):
            raise SemanticRoutingError(
                "supporting-graph semantic plan differs from the frozen launch ticket"
            )
        inputs = SupportingGraphBindingPlanInput.model_validate(plan.payload)
        request = _bind_supporting_graph_request_to_run(
            inputs.request,
            request_scope=ticket.request_scope,
            run_id=run_id,
        )
        operation_binding_refs = await self._operation_bindings.freeze(
            inputs.operation_bindings,
            ticket,
            run_id=run_id,
            bound_at=inputs.created_at,
        )
        binding = build_supporting_graph_run_binding(
            request=request,
            effective_configuration_digest=ticket.effective_configuration_digest,
            blueprint_digest=ticket.blueprint_ref.digest,
            acceptance_contract_ref=("evaluation:supporting-graph-reconciliation:v1"),
            created_at=inputs.created_at,
            evidence=inputs.evidence,
            minimum_successful_intents=inputs.minimum_successful_intents,
            handoff_instructions=inputs.handoff_instructions,
            operation_execution_binding_refs=operation_binding_refs,
        )
        await self._operation_templates.persist_templates(
            request_scope=ticket.request_scope,
            semantic_input_binding_ref=binding.binding_id,
            executor=inputs.operation_bindings.operations["goal_executor"],
            verifier=inputs.operation_bindings.operations["goal_verifier"],
            recorded_at=inputs.created_at,
        )
        return binding


def build_supporting_graph_run_binding(
    *,
    request: SupportingGraphReconciliationRequest,
    effective_configuration_digest: str,
    blueprint_digest: str,
    acceptance_contract_ref: str,
    created_at: datetime,
    evidence: GraphReconciliationEvidence | None = None,
    operation_class: str = "goal_iteration",
    minimum_successful_intents: int = 1,
    handoff_instructions: str = (
        "Resume the frozen supporting-graph objective from immutable query evidence."
    ),
    operation_execution_binding_refs: dict[str, str] | None = None,
) -> RunSemanticInputBinding:
    """Author the exact production binding consumed by the registered Goal handlers."""

    operation_refs = operation_execution_binding_refs or {}
    iteration = SemanticHandlerBinding(
        handler_id=SUPPORTING_GRAPH_ITERATION_HANDLER,
        handler_revision=SUPPORTING_GRAPH_HANDLER_REVISION,
        input=SemanticInputPayload.from_value(
            schema_ref="schema:supporting-graph-goal-iteration:v1",
            value={
                "request": request.model_dump(mode="json"),
                "evidence": (evidence.model_dump(mode="json") if evidence is not None else None),
            },
        ),
        output_contract_ref="schema:supporting-graph-reconciliation-record:v1",
        operation_execution_binding_ref=operation_refs.get("goal_executor"),
    )
    verifier = SemanticHandlerBinding(
        handler_id=SUPPORTING_GRAPH_VERIFIER_HANDLER,
        handler_revision=SUPPORTING_GRAPH_HANDLER_REVISION,
        input=SemanticInputPayload.from_value(
            schema_ref="schema:supporting-graph-goal-verification:v1",
            value={
                "reconciliation_id": request.reconciliation_id,
                "acceptance_contract_ref": acceptance_contract_ref,
                "minimum_successful_intents": minimum_successful_intents,
            },
        ),
        output_contract_ref="schema:supporting-graph-verification-result:v1",
        operation_execution_binding_ref=operation_refs.get("goal_verifier"),
    )
    handoff = SemanticHandlerBinding(
        handler_id=SUPPORTING_GRAPH_HANDOFF_HANDLER,
        handler_revision=SUPPORTING_GRAPH_HANDLER_REVISION,
        input=SemanticInputPayload.from_value(
            schema_ref="schema:supporting-graph-goal-handoff:v1",
            value={"instructions": handoff_instructions},
        ),
        output_contract_ref="schema:goal-handoff-checkpoint:v1",
    )
    return RunSemanticInputBinding.create(
        request_scope=request.request_scope,
        run_id=request.run_id,
        blueprint_family="GoalDirected",
        effective_configuration_digest=effective_configuration_digest,
        blueprint_digest=blueprint_digest,
        goal_operation_handlers=(
            GoalOperationHandlerBinding(
                operation_class=operation_class,
                handler=iteration,
            ),
        ),
        goal_verifier=verifier,
        goal_handoff=handoff,
        created_at=created_at,
    )


def supporting_graph_result_ref(record: SupportingGraphReconciliationRecord) -> str:
    return (
        f"belllabs://schema-grounding/reconciliations/{record.reconciliation_id}/"
        f"{sha256_digest(record).removeprefix('sha256:')}"
    )


def _bind_supporting_graph_request_to_run(
    request: SupportingGraphReconciliationRequest,
    *,
    request_scope: str,
    run_id: str,
) -> SupportingGraphReconciliationRequest:
    admission = request.admission
    workspace_binding = admission.workspace_binding
    graph_capability = admission.graph_capability
    bound_admission = admission.model_copy(
        update={
            "request_scope": request_scope,
            "run_id": run_id,
            "workspace_binding": (
                workspace_binding.model_copy(
                    update={"request_scope": request_scope, "run_id": run_id}
                )
                if workspace_binding is not None
                else None
            ),
            "graph_capability": (
                graph_capability.model_copy(
                    update={"request_scope": request_scope, "run_id": run_id}
                )
                if graph_capability is not None
                else None
            ),
        }
    )
    return request.model_copy(
        update={
            "request_scope": request_scope,
            "run_id": run_id,
            "admission": bound_admission,
        }
    )
