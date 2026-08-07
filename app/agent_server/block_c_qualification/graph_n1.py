"""Import-safe Block C N+1 graph with intentionally incompatible channels/nodes."""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from app.agent_server.block_c_qualification.compat import COMPAT_VERSION_N1
from app.agent_server.block_c_qualification.state import QualificationStateN1

STABLE_CLAIM_V2 = "stable-claim:block-c-single-v2"
DECISION_REF_N1 = "decision:block-c-single-n1"


def record_claim_v2(state: QualificationStateN1) -> dict[str, Any]:
    prior = tuple(state.get("claim_tokens_v2") or ())
    updates: dict[str, Any] = {
        "compat_version": COMPAT_VERSION_N1,
        "decision_refs": (DECISION_REF_N1,),
        "events": ("entered-record-claim-n1",),
    }
    if STABLE_CLAIM_V2 not in prior:
        updates["claim_tokens_v2"] = [STABLE_CLAIM_V2]
        updates["events"] = ("entered-record-claim-n1", "claim-recorded-n1")
    return updates


def approve_v2(state: QualificationStateN1) -> dict[str, Any]:
    del state
    decision = interrupt(
        {
            "decision_ref": DECISION_REF_N1,
            "claim_token": STABLE_CLAIM_V2,
            "compat_version": COMPAT_VERSION_N1,
        }
    )
    return {
        "decisions": [str(decision)],
        "events": ("resumed-single-interrupt-n1",),
    }


def build_graph_n1() -> object:
    builder = StateGraph(QualificationStateN1)
    builder.add_node("record_claim_v2", record_claim_v2)
    builder.add_node("approve_v2", approve_v2)
    builder.add_edge(START, "record_claim_v2")
    builder.add_edge("record_claim_v2", "approve_v2")
    builder.add_edge("approve_v2", END)
    return builder.compile()


graph = build_graph_n1()
