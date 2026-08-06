from __future__ import annotations

from app.agent_server.graphs import goal_directed_graph, stagegraph_graph
from app.domain.graph_runtime.definitions import GraphAssemblyDefinition, RunPlan


def graph_from_exact_definitions(
    *,
    assembly: GraphAssemblyDefinition,
    run_plan: RunPlan,
) -> object:
    """Select immutable topology only after exact assembly/RunPlan agreement."""

    if (
        run_plan.graph_assembly.graph_assembly_ref.digest
        != assembly.graph.graph_assembly_digest
    ):
        raise ValueError("RunPlan and GraphAssemblyDefinition digests do not match")
    if (
        run_plan.graph_assembly.state_schema_digest
        != assembly.state_schema_ref.digest
    ):
        raise ValueError("RunPlan and graph state schema digests do not match")
    if assembly.graph.graph_family == "StageGraph":
        return stagegraph_graph
    if assembly.graph.graph_family == "GoalDirected":
        return goal_directed_graph
    raise ValueError("Stage 2 exports only StageGraph and GoalDirected families")
