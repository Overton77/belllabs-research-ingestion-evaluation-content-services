"""Stable graph registry. Importing this module performs no external I/O."""

from app.agent_server.goal_directed.graph import graph as goal_directed_graph
from app.agent_server.stagegraph.graph import graph as stagegraph_graph

GRAPH_REGISTRY = {
    "belllabs_stagegraph": stagegraph_graph,
    "belllabs_goal_directed": goal_directed_graph,
}

__all__ = ["GRAPH_REGISTRY", "goal_directed_graph", "stagegraph_graph"]
