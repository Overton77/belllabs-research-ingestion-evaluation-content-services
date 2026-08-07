from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from app.agent_server.bootstrap import make_bootstrap_node
from app.agent_server.stagegraph.nodes import (
    admit_runtime_binding,
    interpret_next_stage,
)
from app.agent_server.stagegraph.state import (
    StageGraphInput,
    StageGraphOutput,
    StageGraphState,
)
from app.application.runtime_bootstrap import RuntimeBootstrapReconciler

NODE_BOOTSTRAP_RUNTIME_AUTHORITY = "bootstrap_runtime_authority"
NODE_ADMIT_RUNTIME_BINDING = "admit_runtime_binding"
NODE_INTERPRET_NEXT_STAGE = "interpret_next_stage"


def build_stagegraph(
    bootstrap_reconciler: RuntimeBootstrapReconciler | None = None,
) -> object:
    builder = StateGraph(
        StageGraphState,
        input_schema=StageGraphInput,
        output_schema=StageGraphOutput,
    )
    builder.add_node(
        NODE_BOOTSTRAP_RUNTIME_AUTHORITY,
        make_bootstrap_node(bootstrap_reconciler),
    )
    builder.add_node(NODE_ADMIT_RUNTIME_BINDING, admit_runtime_binding)
    builder.add_node(NODE_INTERPRET_NEXT_STAGE, interpret_next_stage)
    builder.add_edge(START, NODE_BOOTSTRAP_RUNTIME_AUTHORITY)
    builder.add_edge(NODE_BOOTSTRAP_RUNTIME_AUTHORITY, NODE_ADMIT_RUNTIME_BINDING)
    builder.add_edge(NODE_ADMIT_RUNTIME_BINDING, NODE_INTERPRET_NEXT_STAGE)
    builder.add_edge(NODE_INTERPRET_NEXT_STAGE, END)
    return builder.compile()


graph = build_stagegraph()
