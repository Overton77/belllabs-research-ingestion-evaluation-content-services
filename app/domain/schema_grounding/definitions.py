from __future__ import annotations

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    ControlProfileDefinition,
    Definition,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    ExtensionIdentity,
    GoalDirectedBlueprint,
    LinkedRunSlotConstraint,
    NamespacedExtension,
    ObligationRealization,
    OutputContractRealization,
    RuntimeProfileDefinition,
    StageGraphBlueprint,
    StageNode,
    WorkflowConfigurationDefinition,
    WorkflowCyclePolicy,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.extensions import ExtensionPayload, ExtensionRegistry

SCHEMA_GROUNDING_EXTENSION_NAMESPACE = "belllabs.schema-grounding"
SCHEMA_GROUNDING_EXTENSION_VERSION = "1"
SCHEMA_GROUNDING_EXTENSION_DISCRIMINATOR = "operation-contracts"

SHARED_BUDGETS = BudgetCeiling(
    dimensions={
        "tokens.input": 400_000,
        "tokens.output": 80_000,
        "tokens.total": 480_000,
        "time.elapsed_ms": 1_800_000,
        "time.active_compute_ms": 1_200_000,
        "model.turns": 80,
        "tool.calls.total": 100,
        "operation.attempts": 20,
        "goal.iterations": 12,
        "stage.cycles": 2,
        "workflow.cycles": 2,
        "concurrency.slots": 2,
    }
)


class SchemaGroundingOperationContracts(ExtensionPayload):
    operation_contract_refs: tuple[str, ...]
    output_schema_refs: tuple[str, ...]
    semantic_overlay_ref: str
    catalog_generator_version: str
    deployment_manifest_authority: str
    workspace_binding_authority: str


def register_schema_grounding_extensions(registry: ExtensionRegistry) -> None:
    registry.register(
        SCHEMA_GROUNDING_EXTENSION_NAMESPACE,
        SCHEMA_GROUNDING_EXTENSION_VERSION,
        SCHEMA_GROUNDING_EXTENSION_DISCRIMINATOR,
        SchemaGroundingOperationContracts,
    )


def schema_grounding_definitions() -> tuple[Definition, ...]:
    """Return immutable revision-one definitions in safe publication order."""
    selection_blueprint = _selection_blueprint()
    reconciliation_blueprint = _reconciliation_blueprint()
    reconciliation_goal_blueprint = _reconciliation_goal_blueprint()
    selection_blueprint_ref = _ref(selection_blueprint)
    reconciliation_blueprint_ref = _ref(reconciliation_blueprint)
    reconciliation_goal_blueprint_ref = _ref(reconciliation_goal_blueprint)

    selection_control = ControlProfileDefinition(
        logical_id="schema-context-selection-control-v1",
        title="Schema Context Selection control profile",
        description="Bounds semantic revisions, reviewer retries, concurrency, and usage.",
        blueprint_ref=selection_blueprint_ref,
        authority_ceiling=AuthorityCeiling(
            capabilities=frozenset(
                {
                    "schema.catalog.read",
                    "schema.selection.write",
                    "operation.execute.agent",
                }
            ),
            budgets=SHARED_BUDGETS,
            max_concurrency=1,
        ),
        overlayable_fields=frozenset({"budgets"}),
        strengthen_only_fields=frozenset({"budgets"}),
    )
    reconciliation_control = ControlProfileDefinition(
        logical_id="supporting-graph-reconciliation-control-v1",
        title="Supporting Graph Reconciliation control profile",
        description="Bounds graph read intents, projection scope, and observational evidence.",
        blueprint_ref=reconciliation_blueprint_ref,
        authority_ceiling=AuthorityCeiling(
            capabilities=frozenset(
                {
                    "schema.catalog.read",
                    "schema.selection.read",
                    "schema.derivation.execute",
                    "schema.workspace.read",
                    "graph.read.bounded",
                    "operation.execute.agent",
                }
            ),
            budgets=SHARED_BUDGETS,
            max_concurrency=2,
        ),
        overlayable_fields=frozenset({"budgets", "max_concurrency"}),
        strengthen_only_fields=frozenset({"budgets", "max_concurrency"}),
    )
    reconciliation_goal_control = ControlProfileDefinition(
        logical_id="supporting-graph-reconciliation-goal-directed-control-v1",
        title="GoalDirected Supporting Graph Reconciliation control profile",
        description=(
            "Bounds objective-driven query iterations while preserving host admission, "
            "validation, independent evidence checks, and stopping policy."
        ),
        blueprint_ref=reconciliation_goal_blueprint_ref,
        selected_variants=frozenset({"required-seed-intents"}),
        authority_ceiling=AuthorityCeiling(
            capabilities=reconciliation_control.authority_ceiling.capabilities,
            budgets=SHARED_BUDGETS,
            max_concurrency=1,
        ),
        overlayable_fields=frozenset({"budgets"}),
        strengthen_only_fields=frozenset({"budgets"}),
    )
    selection_runtime = RuntimeProfileDefinition(
        logical_id="schema-context-selection-runtime-v1",
        title="Governed schema selection runtime",
        description="Executes selector and reviewer through immutable operation bindings.",
        binding="temporal-stagegraph+operation-execution",
        required_capabilities=frozenset(
            {"schema.catalog.read", "operation.execute.agent"}
        ),
    )
    reconciliation_runtime = RuntimeProfileDefinition(
        logical_id="supporting-graph-reconciliation-runtime-v1",
        title="Governed supporting graph reconciliation runtime",
        description="Executes derivation, admission, agent planning, and bounded Neo4j reads.",
        binding="temporal-stagegraph+operation-execution+neo4j-bounded-read",
        required_capabilities=frozenset(
            {
                "schema.catalog.read",
                "schema.workspace.read",
                "graph.read.bounded",
                "operation.execute.agent",
            }
        ),
    )
    selection_workspace = WorkspaceTemplateDefinition(
        logical_id="schema-context-selection-workspace-v1",
        title="Schema selection workspace",
        description="Stage-scoped read-only Tier 0/candidate inputs and exclusive outputs.",
        slots=(
            WorkspaceSlot(
                name="selection_tier0",
                path="/schema/selection-tier0",
                access="read_only",
                purpose="selector orientation",
            ),
            WorkspaceSlot(
                name="selection_candidates",
                path="/schema/selection-candidates",
                access="read_only",
                purpose="selector and reviewer exact candidate details",
            ),
            WorkspaceSlot(
                name="selection_output",
                path="/outputs/schema-selection",
                access="exclusive_write",
                purpose="selection, validation, review, and acceptance candidates",
            ),
        ),
        required_capabilities=frozenset({"schema.catalog.read"}),
    )
    reconciliation_workspace = WorkspaceTemplateDefinition(
        logical_id="supporting-graph-reconciliation-workspace-v1",
        title="Supporting graph reconciliation workspace",
        description="Read-only runtime projection and exclusive evidence output slots.",
        slots=(
            WorkspaceSlot(
                name="graph_query_runtime",
                path="/schema/graph-query-runtime",
                access="read_only",
                purpose="accepted selection, closure, projection, and capability snapshot",
            ),
            WorkspaceSlot(
                name="reconciliation_output",
                path="/outputs/supporting-graph-reconciliation",
                access="exclusive_write",
                purpose="immutable bounded intent/result evidence candidates",
            ),
        ),
        required_capabilities=frozenset(
            {"schema.workspace.read", "graph.read.bounded"}
        ),
    )
    selection_evaluation = EvaluationProfileDefinition(
        logical_id="schema-context-selection-evaluation-v1",
        title="Schema Context Selection evaluation",
        description="Requires structural validity, exact binding, independent review, and metrics.",
        gate_contract_refs=frozenset(
            {
                "gate:selection-structurally-valid:v1",
                "gate:selection-review-binding-exact:v1",
                "gate:selection-independently-accepted:v1",
                "metric:selection-stability:v1",
                "metric:stage-resource-token-timing:v1",
            }
        ),
    )
    reconciliation_evaluation = EvaluationProfileDefinition(
        logical_id="supporting-graph-reconciliation-evaluation-v1",
        title="Supporting Graph Reconciliation evaluation",
        description="Pins the nine accepted TruDiagnostic gates and repeated runtime metrics.",
        gate_contract_refs=frozenset(
            {
                "gate:identical-workload-digests:v1",
                "gate:candidate-completed:v1",
                "gate:selection-independently-accepted:v1",
                "gate:required-core-membership:v1",
                "gate:exact-implements-discrimination:v1",
                "gate:oracle-recall-1.0:v1",
                "gate:all-offered-products:v1",
                "gate:required-queries-succeeded:v1",
                "gate:exact-schema-deployment-compatibility:v1",
                "metric:stage-resource-token-timing:v1",
                "metric:repeated-run-latency:v1",
                "metric:selection-stability:v1",
            }
        ),
        required_capabilities=frozenset({"graph.read.bounded"}),
    )

    selection_config = WorkflowConfigurationDefinition(
        logical_id="schema-context-selection-official-v1",
        title="Official Schema Context Selection configuration",
        description="Pins the official semantic overlay and typed catalog generator.",
        workflow_type_logical_id="schema-context-selection",
        extensions=(_operation_extension(selection=True),),
    )
    reconciliation_config = WorkflowConfigurationDefinition(
        logical_id="supporting-graph-reconciliation-official-v1",
        title="Official Supporting Graph Reconciliation configuration",
        description="Pins bounded read contracts and Issue 12/13 authority boundaries.",
        workflow_type_logical_id="supporting-graph-reconciliation",
        extensions=(_operation_extension(selection=False),),
    )

    selection_type = WorkflowTypeDefinition(
        logical_id="schema-context-selection",
        title="Schema Context Selection Workflow",
        description="Selects purpose-bound semantic schema membership with independent review.",
        purpose="Produce one accepted purpose-bound Schema Context Selection.",
        non_goals=frozenset(
            {"graph access", "schema authoring", "structural closure as semantic membership"}
        ),
        input_admission_contract="admission:schema-context-selection:v1",
        invariants=frozenset(
            {
                "invariant:schema-selection-independent-review:v1",
                "invariant:schema-selection-exact-lineage:v1",
            }
        ),
        obligations=frozenset(
            {
                "obligation:semantic-selection:v1",
                "obligation:structural-validation:v1",
                "obligation:independent-review:v1",
            }
        ),
        output_contracts=frozenset({"schema:accepted-schema-context-selection:v1"}),
        allowed_blueprints=frozenset({selection_blueprint_ref}),
        allowed_control_profiles=frozenset({_ref(selection_control)}),
        allowed_runtime_profiles=frozenset({_ref(selection_runtime)}),
        allowed_workspace_templates=frozenset({_ref(selection_workspace)}),
        allowed_evaluation_profiles=frozenset({_ref(selection_evaluation)}),
        allowed_workflow_configurations=frozenset({_ref(selection_config)}),
        authority_ceiling=selection_control.authority_ceiling,
        workspace_contract=WorkflowWorkspaceContract(slots=selection_workspace.slots),
        allowed_overlay_extensions=frozenset({_extension_identity()}),
    )
    selection_type_ref = _ref(selection_type)
    reconciliation_type = WorkflowTypeDefinition(
        logical_id="supporting-graph-reconciliation",
        title="Supporting Graph Reconciliation Workflow",
        description="Answers one bounded observational graph reconciliation question.",
        purpose="Persist bounded typed graph read evidence for one admitted question.",
        non_goals=frozenset(
            {
                "Knowledge Preflight coverage",
                "identity resolution",
                "graph mutation",
                "schema authoring",
            }
        ),
        input_admission_contract="admission:supporting-graph-reconciliation:v1",
        invariants=frozenset(
            {
                "invariant:exact-schema-deployment-compatibility:v1",
                "invariant:independent-graph-capability:v1",
                "invariant:no-arbitrary-cypher:v1",
                "invariant:observational-no-graph-mutation:v1",
            }
        ),
        obligations=frozenset(
            {
                "obligation:schema-context-derived:v1",
                "obligation:graph-gate-admitted:v1",
                "obligation:bounded-query-evidence:v1",
            }
        ),
        output_contracts=frozenset(
            {"schema:supporting-graph-reconciliation-record:v1"}
        ),
        allowed_blueprints=frozenset(
            {reconciliation_blueprint_ref, reconciliation_goal_blueprint_ref}
        ),
        allowed_control_profiles=frozenset(
            {_ref(reconciliation_control), _ref(reconciliation_goal_control)}
        ),
        allowed_runtime_profiles=frozenset({_ref(reconciliation_runtime)}),
        allowed_workspace_templates=frozenset({_ref(reconciliation_workspace)}),
        allowed_evaluation_profiles=frozenset({_ref(reconciliation_evaluation)}),
        allowed_workflow_configurations=frozenset({_ref(reconciliation_config)}),
        authority_ceiling=reconciliation_control.authority_ceiling,
        workspace_contract=WorkflowWorkspaceContract(
            slots=reconciliation_workspace.slots
        ),
        linked_run_slots=(
            LinkedRunSlotConstraint(
                slot_id="schema_context_selection",
                allowed_child_workflow_types=frozenset({selection_type_ref}),
                dependency_class="required_blocking",
                wait_policy="wait",
                cancellation_policy="request_cancel",
                result_admission_policy="linked-result:schema-selection-exact-purpose:v1",
                delegation_ceiling=selection_control.authority_ceiling,
                budget_reservation_ceiling=SHARED_BUDGETS,
            ),
        ),
        allowed_overlay_extensions=frozenset({_extension_identity()}),
    )
    selection_implementation = WorkflowImplementationBindingDefinition(
        logical_id="schema-context-selection.implementation",
        title="Default staged Schema Context Selection implementation",
        description=(
            "The approved selector, deterministic validator, independent reviewer, "
            "and bounded workflow-cycle implementation."
        ),
        workflow_type_ref=_ref(selection_type),
        blueprint_ref=selection_blueprint_ref,
        control_profile_ref=_ref(selection_control),
        runtime_profile_ref=_ref(selection_runtime),
        workspace_template_ref=_ref(selection_workspace),
        evaluation_profile_ref=_ref(selection_evaluation),
        workflow_configuration_ref=_ref(selection_config),
        obligation_realizations=(
            ObligationRealization(
                obligation_ref="obligation:semantic-selection:v1",
                realization_kind="stage",
                realization_ref="semantic_selector",
            ),
            ObligationRealization(
                obligation_ref="obligation:structural-validation:v1",
                realization_kind="stage",
                realization_ref="structural_validation",
            ),
            ObligationRealization(
                obligation_ref="obligation:independent-review:v1",
                realization_kind="stage",
                realization_ref="independent_reviewer",
            ),
        ),
        output_contract_realizations=(
            OutputContractRealization(
                output_contract_ref="schema:accepted-schema-context-selection:v1",
                output_slot="accepted_selection",
            ),
        ),
        conformance_evidence_refs=frozenset(
            {
                "test:test_schema_context_selection:v1",
                "evaluation:schema-selection:v1",
            }
        ),
    )
    reconciliation_stage_implementation = WorkflowImplementationBindingDefinition(
        logical_id="supporting-graph-reconciliation.implementation",
        title="Default staged Supporting Graph Reconciliation implementation",
        description=(
            "Executes the admitted host-compiled required intents in deterministic order "
            "and verifies their exact immutable evidence."
        ),
        workflow_type_ref=_ref(reconciliation_type),
        blueprint_ref=reconciliation_blueprint_ref,
        control_profile_ref=_ref(reconciliation_control),
        runtime_profile_ref=_ref(reconciliation_runtime),
        workspace_template_ref=_ref(reconciliation_workspace),
        evaluation_profile_ref=_ref(reconciliation_evaluation),
        workflow_configuration_ref=_ref(reconciliation_config),
        obligation_realizations=(
            ObligationRealization(
                obligation_ref="obligation:schema-context-derived:v1",
                realization_kind="stage",
                realization_ref="derive_schema_context",
            ),
            ObligationRealization(
                obligation_ref="obligation:graph-gate-admitted:v1",
                realization_kind="stage",
                realization_ref="graph_authority_gate",
            ),
            ObligationRealization(
                obligation_ref="obligation:bounded-query-evidence:v1",
                realization_kind="stage",
                realization_ref="execute_bounded_intents",
            ),
        ),
        output_contract_realizations=(
            OutputContractRealization(
                output_contract_ref="schema:supporting-graph-reconciliation-record:v1",
                output_slot="reconciliation_result",
            ),
        ),
        conformance_evidence_refs=frozenset(
            {
                "test:test_schema_grounding_services:v1",
                "evaluation:supporting-graph-reconciliation:v1",
            }
        ),
    )
    reconciliation_goal_implementation = WorkflowImplementationBindingDefinition(
        logical_id="supporting-graph-reconciliation.implementation",
        title="Alternative GoalDirected Supporting Graph Reconciliation implementation",
        description=(
            "Allows a bounded agent planner to add admitted query intents after mandatory "
            "host seed intents, while deterministic host gates remain authoritative."
        ),
        workflow_type_ref=_ref(reconciliation_type),
        blueprint_ref=reconciliation_goal_blueprint_ref,
        control_profile_ref=_ref(reconciliation_goal_control),
        runtime_profile_ref=_ref(reconciliation_runtime),
        workspace_template_ref=_ref(reconciliation_workspace),
        evaluation_profile_ref=_ref(reconciliation_evaluation),
        workflow_configuration_ref=_ref(reconciliation_config),
        obligation_realizations=tuple(
            ObligationRealization(
                obligation_ref=obligation,
                realization_kind="goal_acceptance",
                realization_ref="evaluation:supporting-graph-reconciliation:v1",
            )
            for obligation in sorted(reconciliation_type.obligations)
        ),
        output_contract_realizations=(
            OutputContractRealization(
                output_contract_ref="schema:supporting-graph-reconciliation-record:v1",
                output_slot="goal_result",
            ),
        ),
        conformance_evidence_refs=frozenset(
            {
                "experiment:official-catalog-v1-live-20260723-3",
                "evaluation:supporting-graph-reconciliation:v1",
            }
        ),
    )
    return (
        selection_blueprint,
        reconciliation_blueprint,
        reconciliation_goal_blueprint,
        selection_control,
        reconciliation_control,
        reconciliation_goal_control,
        selection_runtime,
        reconciliation_runtime,
        selection_workspace,
        reconciliation_workspace,
        selection_evaluation,
        reconciliation_evaluation,
        selection_config,
        reconciliation_config,
        selection_type,
        reconciliation_type,
        selection_implementation,
        reconciliation_stage_implementation,
        reconciliation_goal_implementation,
    )


def _selection_blueprint() -> StageGraphBlueprint:
    return StageGraphBlueprint(
        logical_id="schema-context-selection-v1",
        title="Schema Context Selection StageGraph",
        description="Materialize, select, validate, independently review, and accept.",
        stages=(
            StageNode(
                stage_id="materialize_selection_context",
                reservation={"operation.attempts": 1},
                output_slots=frozenset({"selection_workspace_binding"}),
            ),
            StageNode(
                stage_id="semantic_selector",
                depends_on=frozenset({"materialize_selection_context"}),
                reservation={"operation.attempts": 1},
                obligation_refs=frozenset({"obligation:semantic-selection:v1"}),
                output_slots=frozenset({"selection_draft"}),
            ),
            StageNode(
                stage_id="structural_validation",
                depends_on=frozenset({"semantic_selector"}),
                reservation={"operation.attempts": 1},
                obligation_refs=frozenset({"obligation:structural-validation:v1"}),
                output_slots=frozenset({"selection_validation"}),
            ),
            StageNode(
                stage_id="independent_reviewer",
                depends_on=frozenset({"structural_validation"}),
                reservation={"operation.attempts": 1},
                obligation_refs=frozenset({"obligation:independent-review:v1"}),
                output_slots=frozenset({"selection_review"}),
            ),
            StageNode(
                stage_id="accept_selection",
                depends_on=frozenset({"independent_reviewer"}),
                reservation={"operation.attempts": 1},
                output_slots=frozenset({"accepted_selection"}),
            ),
        ),
        declared_output_slots=frozenset(
            {
                "selection_workspace_binding",
                "selection_draft",
                "selection_validation",
                "selection_review",
                "accepted_selection",
            }
        ),
        max_parallel_stages=1,
        workflow_evaluation_contract_ref="evaluation:schema-selection:v1",
        workflow_cycle_policy=WorkflowCyclePolicy(
            max_cycles=2,
            evaluation_contract_ref="evaluation:schema-selection:v1",
            objective_contract_ref="objective:bounded-semantic-revision:v1",
            reservation={"workflow.cycles": 1},
        ),
    )


def _reconciliation_blueprint() -> StageGraphBlueprint:
    stage_ids = (
        "admission",
        "derive_schema_context",
        "materialize_runtime_projection",
        "graph_authority_gate",
        "plan_bounded_queries",
        "execute_bounded_intents",
        "verify_evidence",
        "evaluate",
        "promote_result",
    )
    output_slots = {
        "admission": "admission_decision",
        "derive_schema_context": "schema_derivation",
        "materialize_runtime_projection": "runtime_workspace_binding",
        "graph_authority_gate": "graph_admission_decision",
        "plan_bounded_queries": "query_plan",
        "execute_bounded_intents": "query_evidence",
        "verify_evidence": "verified_evidence",
        "evaluate": "evaluation",
        "promote_result": "reconciliation_result",
    }
    obligations = {
        "derive_schema_context": frozenset(
            {"obligation:schema-context-derived:v1"}
        ),
        "graph_authority_gate": frozenset(
            {"obligation:graph-gate-admitted:v1"}
        ),
        "execute_bounded_intents": frozenset(
            {"obligation:bounded-query-evidence:v1"}
        ),
    }
    return StageGraphBlueprint(
        logical_id="supporting-graph-reconciliation-v1",
        title="Supporting Graph Reconciliation StageGraph",
        description="Derive, gate, execute bounded reads, verify evidence, and promote.",
        stages=tuple(
            StageNode(
                stage_id=stage_id,
                depends_on=(
                    frozenset({stage_ids[index - 1]}) if index else frozenset()
                ),
                reservation={"operation.attempts": 1},
                obligation_refs=obligations.get(stage_id, frozenset()),
                output_slots=frozenset({output_slots[stage_id]}),
            )
            for index, stage_id in enumerate(stage_ids)
        ),
        declared_output_slots=frozenset(output_slots.values()),
        max_parallel_stages=1,
        workflow_evaluation_contract_ref="evaluation:supporting-graph-reconciliation:v1",
    )


def _reconciliation_goal_blueprint() -> GoalDirectedBlueprint:
    return GoalDirectedBlueprint(
        logical_id="supporting-graph-reconciliation-goal-directed-v1",
        title="GoalDirected Supporting Graph Reconciliation",
        description=(
            "Pursues one bounded reconciliation objective through host-validated query "
            "iterations and requires independent acceptance verification."
        ),
        objective_contract="objective:supporting-graph-reconciliation:v1",
        acceptance_contract="evaluation:supporting-graph-reconciliation:v1",
        max_iterations=12,
        variant_names=frozenset({"required-seed-intents"}),
    )


def _operation_extension(*, selection: bool) -> NamespacedExtension:
    payload = SchemaGroundingOperationContracts(
        operation_contract_refs=(
            (
                "operation-contract:schema-context-selector:v1",
                "operation-contract:schema-context-reviewer:v1",
                "operation-contract:schema-context-derive:v1",
                "operation-contract:schema-workspace-materialize:v1",
            )
            if selection
            else (
                "operation-contract:schema-context-derive:v1",
                "operation-contract:schema-workspace-materialize:v1",
                "operation-contract:neo4j-bounded-read:v1",
                "operation-contract:supporting-graph-reconciliation-planner:v1",
            )
        ),
        output_schema_refs=(
            ("schema:accepted-schema-context-selection:v1",)
            if selection
            else (
                "schema:schema-operation-projection:v1",
                "schema:bounded-query-plan:v1",
                "schema:supporting-graph-reconciliation-record:v1",
            )
        ),
        semantic_overlay_ref="schema-overlay:trudiagnostic:v1",
        catalog_generator_version="typed-schema-catalog-v1",
        deployment_manifest_authority="issue-12:schema-deployment-manifest",
        workspace_binding_authority="issue-13:schema-workspace-binding",
    )
    return NamespacedExtension(
        namespace=SCHEMA_GROUNDING_EXTENSION_NAMESPACE,
        schema_version=SCHEMA_GROUNDING_EXTENSION_VERSION,
        discriminator=SCHEMA_GROUNDING_EXTENSION_DISCRIMINATOR,
        payload=payload.model_dump(mode="python"),
    )


def _extension_identity() -> ExtensionIdentity:
    return ExtensionIdentity(
        namespace=SCHEMA_GROUNDING_EXTENSION_NAMESPACE,
        schema_version=SCHEMA_GROUNDING_EXTENSION_VERSION,
        discriminator=SCHEMA_GROUNDING_EXTENSION_DISCRIMINATOR,
    )


def _ref(definition: Definition) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=1,
        digest=sha256_digest(definition),
    )
