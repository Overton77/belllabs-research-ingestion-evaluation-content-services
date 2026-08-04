from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.application.orchestration_routing import SemanticHandlerRegistry, SemanticRoutingError
from app.application.schema_grounding_repository import SchemaGroundingRecordRepository
from app.application.semantic_operation_bindings import (
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
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
from app.domain.orchestration.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffCheckpoint,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalVerificationRequest,
    GoalVerificationResult,
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


class SupportingGraphIterationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: SupportingGraphReconciliationRequest
    evidence: GraphReconciliationEvidence | None = None


class SupportingGraphVerificationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    reconciliation_id: str = Field(min_length=1)
    acceptance_contract_ref: str = Field(min_length=1)
    minimum_successful_intents: int = Field(default=1, ge=1)


class SupportingGraphHandoffInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instructions: str = Field(min_length=1)


class SupportingGraphBindingPlanInput(BaseModel):
    """Pre-admission inputs required to author Scenario C's run binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request: SupportingGraphReconciliationRequest
    evidence: GraphReconciliationEvidence | None = None
    minimum_successful_intents: int = Field(default=1, ge=1)
    handoff_instructions: str = Field(min_length=1)
    operation_bindings: SemanticOperationBindingTemplates
    created_at: datetime


ITERATION_INPUT_ADAPTER = TypeAdapter(SupportingGraphIterationInput)
VERIFICATION_INPUT_ADAPTER = TypeAdapter(SupportingGraphVerificationInput)
HANDOFF_INPUT_ADAPTER = TypeAdapter(SupportingGraphHandoffInput)


class SupportingGraphGoalIterationHandler:
    """Execute the application-owned bounded reconciliation service for one claim."""

    def __init__(self, workflow: SupportingGraphReconciliationWorkflow) -> None:
        self._workflow = workflow

    async def execute(
        self,
        claim: GoalExecutionClaim,
        binding: SemanticHandlerBinding,
    ) -> GoalExecutionResult:
        value = binding.input.decode(ITERATION_INPUT_ADAPTER)
        request = value.request
        if (
            request.request_scope != claim.request_scope
            or request.run_id != claim.identity.iteration.run_id
        ):
            raise SemanticRoutingError(
                "supporting-graph request is outside the active GoalDirected run"
            )
        record = await self._workflow.run(request, evidence=value.evidence)
        output_ref = supporting_graph_result_ref(record)
        if record.status == "completed":
            return GoalExecutionResult(
                identity=claim.identity,
                disposition="completed",
                output_refs=(output_ref,),
                completion_claim=True,
                actual_usage={"graph.reads": len(record.intent_result_references)},
                output_contract_ref=binding.output_contract_ref,
            )
        if record.status == "rejected":
            return GoalExecutionResult(
                identity=claim.identity,
                disposition="failed",
                output_refs=(output_ref,),
                authority_breach_ref=(
                    "schema-grounding:admission:"
                    + (record.admission_decision.failure_code or "rejected")
                ),
                output_contract_ref=binding.output_contract_ref,
            )
        return GoalExecutionResult(
            identity=claim.identity,
            disposition="failed",
            output_refs=(output_ref,),
            irrecoverable_failure_ref=(
                f"schema-grounding:reconciliation-failed:{record.reconciliation_id}"
            ),
            output_contract_ref=binding.output_contract_ref,
        )


class SupportingGraphGoalVerifier:
    """Independently rehydrate and verify the immutable reconciliation record."""

    def __init__(self, records: SchemaGroundingRecordRepository) -> None:
        self._records = records

    async def verify(
        self,
        request: GoalVerificationRequest,
        binding: SemanticHandlerBinding,
    ) -> GoalVerificationResult:
        value = binding.input.decode(VERIFICATION_INPUT_ADAPTER)
        if value.acceptance_contract_ref != request.acceptance_contract_ref:
            raise SemanticRoutingError(
                "supporting-graph verifier input does not match the frozen acceptance contract"
            )
        envelope = await self._records.get(
            request.claim.request_scope,
            "reconciliation",
            value.reconciliation_id,
        )
        record = SupportingGraphReconciliationRecord.model_validate(envelope.payload)
        expected_ref = supporting_graph_result_ref(record)
        evidence_bound = (
            expected_ref in request.execution_result.output_refs
            and envelope.run_id == request.claim.identity.iteration.run_id
            and record.run_id == request.claim.identity.iteration.run_id
        )
        completed = (
            evidence_bound
            and record.status == "completed"
            and record.successful_count >= value.minimum_successful_intents
            and record.failed_count == 0
            and record.rejected_count == 0
            and record.evidence is not None
            and record.evidence.intent_result_references == record.intent_result_references
        )
        return GoalVerificationResult(
            identity=request.claim.identity,
            action="verified_completion" if completed else "repair",
            verification_ref=(
                "verification:supporting-graph:"
                + sha256_digest(
                    {
                        "record": record,
                        "expected_ref": expected_ref,
                        "acceptance_contract_ref": value.acceptance_contract_ref,
                    }
                ).removeprefix("sha256:")
            ),
            verifier_ref=request.verifier_ref,
            acceptance_contract_ref=request.acceptance_contract_ref,
            progress_made=record.successful_count > 0,
            evidence_refs=(expected_ref,) if evidence_bound else (),
            unmet_obligations=(() if completed else ("verified-supporting-graph-reconciliation",)),
            output_contract_ref=binding.output_contract_ref,
        )


class SupportingGraphGoalHandoffHandler:
    """Create a deterministic continuation checkpoint from accepted run facts."""

    async def prepare(
        self,
        request: GoalHandoffRequest,
        binding: SemanticHandlerBinding,
    ) -> GoalHandoffResult:
        value = binding.input.decode(HANDOFF_INPUT_ADAPTER)
        checkpoint = GoalHandoffCheckpoint(
            checkpoint_id=(
                "checkpoint:supporting-graph:"
                + sha256_digest(
                    {
                        "semantic_key": request.claim.identity.semantic_key,
                        "verification_ref": request.verification_ref,
                        "fallback": request.fallback,
                    }
                ).removeprefix("sha256:")
            ),
            agent_run_identity=request.claim.identity,
            goal_revision_id=request.claim.identity.iteration.goal_revision_id,
            protected_scope_digest=request.protected_scope_digest,
            instructions=(
                value.instructions
                if not request.fallback
                else "System fallback. " + value.instructions
            ),
            state_refs=request.execution_result.output_refs,
            artifact_refs=request.execution_result.output_refs,
            workspace_ref=request.claim.workspace_namespace,
        )
        return GoalHandoffResult(
            checkpoint=checkpoint,
            fallback_used=request.fallback,
            output_contract_ref=binding.output_contract_ref,
        )


class SupportingGraphSemanticBindingProvider:
    """Freeze and author the exact GoalDirected Scenario C semantic authority."""

    def __init__(
        self,
        inputs: SupportingGraphBindingPlanInput,
        operation_bindings: SemanticOperationExecutionBindingService,
    ) -> None:
        self._inputs = inputs
        self._operation_bindings = operation_bindings

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
        if set(self._inputs.operation_bindings.operations) != {"goal_iteration"}:
            raise SemanticRoutingError(
                "supporting-graph model-backed iteration requires one exact "
                "Operation Execution Binding template"
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
        return build_supporting_graph_run_binding(
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


def register_supporting_graph_goal_handlers(
    registry: SemanticHandlerRegistry,
    *,
    workflow: SupportingGraphReconciliationWorkflow,
    records: SchemaGroundingRecordRepository,
) -> None:
    registry.register_goal_iteration(
        SUPPORTING_GRAPH_ITERATION_HANDLER,
        SUPPORTING_GRAPH_HANDLER_REVISION,
        SupportingGraphGoalIterationHandler(workflow),
    )
    registry.register_goal_verifier(
        SUPPORTING_GRAPH_VERIFIER_HANDLER,
        SUPPORTING_GRAPH_HANDLER_REVISION,
        SupportingGraphGoalVerifier(records),
    )
    registry.register_goal_handoff(
        SUPPORTING_GRAPH_HANDOFF_HANDLER,
        SUPPORTING_GRAPH_HANDLER_REVISION,
        SupportingGraphGoalHandoffHandler(),
    )


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
        operation_execution_binding_ref=operation_refs.get(operation_class),
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
