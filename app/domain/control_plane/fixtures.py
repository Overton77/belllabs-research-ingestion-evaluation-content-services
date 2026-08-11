"""Generic contract fixtures; these are not product Workflow Type definitions."""

from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    AuthorityCeiling,
    BudgetCeiling,
    DependencyClass,
    GoalConvergencePolicy,
    GoalDirectedBlueprint,
    GoalHandoffPolicy,
    GoalSessionRolloverPolicy,
    GoalVerifierPolicy,
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
)

GENERIC_STAGE_GRAPH = StageGraphBlueprint(
    logical_id="fixture.generic-stage-graph",
    title="Generic StageGraph contract fixture",
    description="Validates structure only; workflow-specific semantics remain intentionally unset.",
    stages=(
        StageNode(
            stage_id="prepare",
            output_slots=(
                StageOutputSlot(
                    output_slot_id="intermediate",
                    output_contract_ref="contract:fixture-intermediate@1",
                ),
            ),
            operation_slots=(
                StageOperationSlot(
                    operation_slot_id="execute",
                    allowed_variants=(
                        AllowedOperationVariant(
                            operation_variant_id="default",
                            operation_contract_ref="operation:fixture-prepare@1",
                        ),
                    ),
                ),
            ),
        ),
        StageNode(
            stage_id="finish",
            input_slots=(StageInputSlot(input_slot_id="intermediate"),),
            output_slots=(
                StageOutputSlot(
                    output_slot_id="result",
                    output_contract_ref="contract:fixture-result@1",
                ),
            ),
            operation_slots=(
                StageOperationSlot(
                    operation_slot_id="execute",
                    allowed_variants=(
                        AllowedOperationVariant(
                            operation_variant_id="default",
                            operation_contract_ref="operation:fixture-finish@1",
                        ),
                    ),
                ),
            ),
        ),
    ),
    joins=(
        StageJoin(
            consumer_stage_id="finish",
            join_id="inputs",
            kind="all",
            dependency_ids=("prepare-to-finish",),
            slow_sibling_policy=SlowSiblingPolicy(
                triggers=("join_released",),
                execution_action="continue",
                arrival_route="evaluate_late_result",
            ),
        ),
    ),
    dependencies=(
        StageDependency(
            dependency_id="prepare-to-finish",
            consumer_stage_id="finish",
            join_id="inputs",
            producer_stage_id="prepare",
            producer_output_slot_id="intermediate",
            consumer_input_slot_id="intermediate",
            dependency_class=DependencyClass.REQUIRED,
        ),
    ),
    late_result_policy=LateResultPolicy(
        rules=(
            LateResultRule(
                rule_id="admit-consumer-late-result",
                trigger="consumer_already_admitted",
                decision="admit",
            ),
        )
    ),
)

GENERIC_GOAL_DIRECTED = GoalDirectedBlueprint(
    logical_id="fixture.generic-goal-directed",
    title="Generic GoalDirected contract fixture",
    description="Validates bounded iteration only; no product objective or threshold is implied.",
    objective_contract="TODO(workflow specification): pin an exact objective contract reference",
    acceptance_contract="TODO(workflow specification): pin exact independent acceptance criteria",
    admitted_input_classes=frozenset({"fixture-input"}),
    authority_ceiling=AuthorityCeiling(
        capabilities=frozenset({"fixture-operation"}),
        budgets=BudgetCeiling(dimensions={"goal.iterations": 1}),
    ),
    prohibited_work=frozenset({"product work"}),
    required_output_contracts=frozenset({"fixture-output"}),
    required_obligation_refs=frozenset({"fixture-obligation"}),
    verifier_policy=GoalVerifierPolicy(
        operation_class="fixture_verifier",
        binding_ref="verifier:fixture@1",
        rubric_ref="rubric:fixture@1",
        rubric_version=1,
        acceptance_version=1,
        output_contract_ref="fixture-output",
    ),
    allowed_operation_classes=frozenset({"fixture_operation"}),
    allowed_async_subgoal_classes=frozenset({"fixture_subgoal"}),
    allowed_linked_run_slot_ids=frozenset({"fixture_link"}),
    session_policy=GoalSessionRolloverPolicy(
        context_selection_policy_ref="context-selection:fixture@1",
        context_compaction_policy_ref="context-compaction:fixture@1",
        protected_fact_classes=frozenset({"objective"}),
        max_rollovers=0,
        compaction_failure_action="pause",
    ),
    handoff_policy=GoalHandoffPolicy(
        max_instruction_bytes=1024,
        allowed_workspace_ref_classes=frozenset({"fixture-workspace"}),
        allowed_snapshot_ref_classes=frozenset({"fixture-snapshot"}),
    ),
    convergence_policy=GoalConvergencePolicy(
        authority_breach_action="fail",
        no_progress_action="partial_or_fail",
        repeated_blocker_action="partial_or_fail",
        soft_budget_action="continue",
    ),
    max_iterations=1,
)
