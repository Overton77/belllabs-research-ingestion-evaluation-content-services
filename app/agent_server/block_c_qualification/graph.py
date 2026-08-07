"""Import-safe Block C qualification graph (compatibility version N).

No network clients, pools, sandboxes, or tracing bootstrap at import time.
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send, interrupt

from app.agent_server.block_c_qualification.compat import COMPAT_VERSION_N
from app.agent_server.block_c_qualification.state import QualificationState

STABLE_CLAIM = "stable-claim:block-c-single"
DECISION_REF_SINGLE = "decision:block-c-single"
DECISION_REF_PARALLEL_A = "decision:block-c-parallel-a"
DECISION_REF_PARALLEL_B = "decision:block-c-parallel-b"


def stamp_compat(state: QualificationState) -> dict[str, Any]:
    del state
    return {"compat_version": COMPAT_VERSION_N, "events": ("compat-stamped",)}


def route_scenario(state: QualificationState) -> list[Send] | str:
    scenario = state.get("scenario") or "single_interrupt"
    if scenario == "parallel_interrupts":
        return [
            Send("parallel_lane_a", state),
            Send("parallel_lane_b", state),
        ]
    return "record_claim"


def record_claim(state: QualificationState) -> dict[str, Any]:
    """Durable pre-interrupt evidence. Re-runs on resume but stays idempotent."""

    prior = tuple(state.get("claim_tokens") or ())
    updates: dict[str, Any] = {
        "compat_version": COMPAT_VERSION_N,
        "decision_refs": (DECISION_REF_SINGLE,),
        "events": ("entered-record-claim",),
    }
    if STABLE_CLAIM not in prior:
        updates["claim_tokens"] = [STABLE_CLAIM]
        updates["events"] = ("entered-record-claim", "claim-recorded")
    else:
        updates["events"] = ("entered-record-claim", "claim-already-present")
    return updates


def single_interrupt(state: QualificationState) -> dict[str, Any]:
    del state
    decision = interrupt(
        {
            "decision_ref": DECISION_REF_SINGLE,
            "claim_token": STABLE_CLAIM,
            "compat_version": COMPAT_VERSION_N,
        }
    )
    return {
        "decisions": [str(decision)],
        "events": ("resumed-single-interrupt",),
    }


def parallel_lane_a(state: QualificationState) -> dict[str, Any]:
    del state
    decision = interrupt(
        {
            "decision_ref": DECISION_REF_PARALLEL_A,
            "lane": "a",
            "compat_version": COMPAT_VERSION_N,
        }
    )
    return {
        "decisions": [f"a:{decision}"],
        "decision_refs": (DECISION_REF_PARALLEL_A,),
        "events": ("resumed-parallel-a",),
    }


def parallel_lane_b(state: QualificationState) -> dict[str, Any]:
    del state
    decision = interrupt(
        {
            "decision_ref": DECISION_REF_PARALLEL_B,
            "lane": "b",
            "compat_version": COMPAT_VERSION_N,
        }
    )
    return {
        "decisions": [f"b:{decision}"],
        "decision_refs": (DECISION_REF_PARALLEL_B,),
        "events": ("resumed-parallel-b",),
    }


def build_graph() -> object:
    builder = StateGraph(QualificationState)
    builder.add_node("stamp_compat", stamp_compat)
    builder.add_node("record_claim", record_claim)
    builder.add_node("single_interrupt", single_interrupt)
    builder.add_node("parallel_lane_a", parallel_lane_a)
    builder.add_node("parallel_lane_b", parallel_lane_b)
    builder.add_edge(START, "stamp_compat")
    builder.add_conditional_edges(
        "stamp_compat",
        route_scenario,
        ["record_claim", "parallel_lane_a", "parallel_lane_b"],
    )
    builder.add_edge("record_claim", "single_interrupt")
    builder.add_edge("single_interrupt", END)
    builder.add_edge("parallel_lane_a", END)
    builder.add_edge("parallel_lane_b", END)
    return builder.compile()


graph = build_graph()
