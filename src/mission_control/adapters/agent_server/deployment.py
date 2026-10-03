"""Server-selected graph topology for the single Agent Server deployment config."""

from __future__ import annotations

import os
from uuid import UUID, uuid5

from langgraph_sdk import Auth

GRAPH_NAMESPACE = UUID("6ba7b821-9dad-11d1-80b4-00c04fd430c8")
CHILD_GRAPH = "belllabs_async_technical_child"
QUALIFICATION_GRAPHS = frozenset(
    {"block_c_qualification", "block_c_qualification_n1", "block_c_wait"}
)
PROFILES = {
    "runtime": frozenset({CHILD_GRAPH}),
    "qualification": QUALIFICATION_GRAPHS,
    "qualification_n1": frozenset({"block_c_qualification_n1"}),
}


def profile() -> str:
    value = os.environ.get("MISSION_CONTROL_AGENT_SERVER_PROFILE", "runtime")
    if value not in PROFILES:
        raise RuntimeError("unknown Mission Control Agent Server profile")
    return value


def allowed_graphs() -> frozenset[str]:
    return PROFILES[profile()]


def require_graph(graph_id: str) -> None:
    if graph_id not in allowed_graphs():
        raise Auth.exceptions.HTTPException(
            status_code=403, detail=f"graph {graph_id} disabled by Agent Server profile"
        )


def require_assistant(assistant_id: object) -> None:
    # Pinned langgraph-api 0.12.0 graph.register_graph uses uuid5(NAMESPACE_GRAPH,
    # graph_id), with the exact vendor graph namespace pinned above.
    allowed = {str(uuid5(GRAPH_NAMESPACE, item)) for item in allowed_graphs()}
    allowed.update(allowed_graphs())
    if str(assistant_id) not in allowed:
        raise Auth.exceptions.HTTPException(
            status_code=403, detail="assistant disabled by Agent Server profile"
        )
