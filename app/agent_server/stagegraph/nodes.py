from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.agent_server.context import require_runtime_scope
from app.agent_server.stagegraph.state import StageGraphState
from app.domain.graph_runtime.identities import DIGEST_PATTERN


async def admit_runtime_binding(state: StageGraphState, runtime: Any) -> dict[str, tuple[str, ...]]:
    _validate_common_state(state)
    require_runtime_scope(runtime, state["request_scope"])
    return {
        "event_refs": (
            f"runtime-binding-admitted:{state['graph_assembly_digest']}",
        )
    }


async def interpret_next_stage(
    state: StageGraphState,
) -> dict[str, tuple[str, ...]]:
    stage_ref = state["next_stage_ref"].strip()
    if not stage_ref or len(stage_ref) > 512:
        raise ValueError("next_stage_ref must be a compact authoritative reference")
    return {"event_refs": (f"stage-placeholder:{stage_ref}",)}


def _validate_common_state(state: Mapping[str, Any]) -> None:
    import re

    if not state["request_scope"] or not state["belllabs_run_id"]:
        raise ValueError("qualified BellLabs run identity is required")
    if state["execution_epoch"] < 1:
        raise ValueError("execution epoch must be positive")
    if re.fullmatch(DIGEST_PATTERN, state["graph_assembly_digest"]) is None:
        raise ValueError("invalid graph assembly digest")
    if re.fullmatch(DIGEST_PATTERN, state["run_plan_digest"]) is None:
        raise ValueError("invalid RunPlan digest")
