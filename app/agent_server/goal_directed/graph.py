from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from app.agent_server.bootstrap import make_bootstrap_node
from app.agent_server.goal_directed.nodes import (
    admit_goal_binding,
    bounded_agent_placeholder,
    independent_verifier_placeholder,
)
from app.agent_server.goal_directed.state import (
    GoalDirectedInput,
    GoalDirectedOutput,
    GoalDirectedState,
)
from app.application.runtime_bootstrap import RuntimeBootstrapReconciler

NODE_BOOTSTRAP_RUNTIME_AUTHORITY = "bootstrap_runtime_authority"
NODE_ADMIT_GOAL_BINDING = "admit_goal_binding"
NODE_BOUNDED_AGENT = "bounded_agent"
NODE_INDEPENDENT_VERIFIER = "independent_verifier"


def build_goal_directed_graph(
    bootstrap_reconciler: RuntimeBootstrapReconciler | None = None,
) -> object:
    builder = StateGraph(
        GoalDirectedState,
        input_schema=GoalDirectedInput,
        output_schema=GoalDirectedOutput,
    )
    builder.add_node(
        NODE_BOOTSTRAP_RUNTIME_AUTHORITY,
        make_bootstrap_node(bootstrap_reconciler),
    )
    builder.add_node(NODE_ADMIT_GOAL_BINDING, admit_goal_binding)
    builder.add_node(NODE_BOUNDED_AGENT, bounded_agent_placeholder)
    builder.add_node(NODE_INDEPENDENT_VERIFIER, independent_verifier_placeholder)
    builder.add_edge(START, NODE_BOOTSTRAP_RUNTIME_AUTHORITY)
    builder.add_edge(NODE_BOOTSTRAP_RUNTIME_AUTHORITY, NODE_ADMIT_GOAL_BINDING)
    builder.add_edge(NODE_ADMIT_GOAL_BINDING, NODE_BOUNDED_AGENT)
    builder.add_edge(NODE_BOUNDED_AGENT, NODE_INDEPENDENT_VERIFIER)
    builder.add_edge(NODE_INDEPENDENT_VERIFIER, END)
    return builder.compile()


graph = build_goal_directed_graph()
