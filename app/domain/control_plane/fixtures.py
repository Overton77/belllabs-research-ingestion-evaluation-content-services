"""Generic contract fixtures; these are not product Workflow Type definitions."""

from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    DependencyClass,
    GoalDirectedBlueprint,
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
    max_iterations=1,
)
