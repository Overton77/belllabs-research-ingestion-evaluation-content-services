"""Generic contract fixtures; these are not product Workflow Type definitions."""

from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    StageGraphBlueprint,
    StageNode,
)

GENERIC_STAGE_GRAPH = StageGraphBlueprint(
    logical_id="fixture.generic-stage-graph",
    title="Generic StageGraph contract fixture",
    description="Validates structure only; workflow-specific semantics remain intentionally unset.",
    stages=(
        StageNode(stage_id="prepare", output_slots=frozenset({"intermediate"})),
        StageNode(
            stage_id="finish",
            depends_on=frozenset({"prepare"}),
            output_slots=frozenset({"result"}),
        ),
    ),
    declared_output_slots=frozenset({"intermediate", "result"}),
)

GENERIC_GOAL_DIRECTED = GoalDirectedBlueprint(
    logical_id="fixture.generic-goal-directed",
    title="Generic GoalDirected contract fixture",
    description="Validates bounded iteration only; no product objective or threshold is implied.",
    objective_contract="TODO(workflow specification): pin an exact objective contract reference",
    acceptance_contract="TODO(workflow specification): pin exact independent acceptance criteria",
    max_iterations=1,
)
