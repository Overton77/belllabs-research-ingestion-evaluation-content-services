from __future__ import annotations

from typing import Any

from app.agent_server.context import require_runtime_scope
from app.agent_server.goal_directed.state import GoalDirectedState
from app.agent_server.stagegraph.nodes import _validate_common_state


async def admit_goal_binding(
    state: GoalDirectedState,
    runtime: Any,
) -> dict[str, tuple[str, ...]]:
    _validate_common_state(state)
    require_runtime_scope(runtime, state["request_scope"])
    goal_ref = state["goal_ref"].strip()
    verifier_ref = state["verifier_ref"].strip()
    if not goal_ref or not verifier_ref:
        raise ValueError("goal and independent verifier references are required")
    return {"event_refs": (f"goal-binding-admitted:{goal_ref}",)}


async def bounded_agent_placeholder(
    state: GoalDirectedState,
) -> dict[str, tuple[str, ...]]:
    return {"event_refs": (f"bounded-agent-placeholder:{state['graph_assembly_digest']}",)}


async def independent_verifier_placeholder(
    state: GoalDirectedState,
) -> dict[str, tuple[str, ...]]:
    return {"event_refs": (f"verifier-placeholder:{state['verifier_ref']}",)}
