from __future__ import annotations

from app.domain.graph_runtime.definitions import GraphAssemblyDefinition, RunPlan, RunPlanV3


def graph_from_exact_definitions(
    *,
    assembly: GraphAssemblyDefinition,
    run_plan: RunPlan | RunPlanV3,
) -> object:
    """Reject retired Agent Server macro families; Temporal owns StageGraph/GoalDirected."""

    if run_plan.graph_assembly.graph_assembly_ref.digest != assembly.graph.graph_assembly_digest:
        raise ValueError("RunPlan and GraphAssemblyDefinition digests do not match")
    if run_plan.graph_assembly.state_schema_digest != assembly.state_schema_ref.digest:
        raise ValueError("RunPlan and graph state schema digests do not match")
    if assembly.graph.graph_family in {"StageGraph", "GoalDirected"}:
        raise ValueError(
            "Agent Server StageGraph/GoalDirected macro graphs were deleted; "
            "use Temporal family workflows under app/temporal/workflows/"
        )
    raise ValueError(f"unsupported Agent Server graph family: {assembly.graph.graph_family}")
