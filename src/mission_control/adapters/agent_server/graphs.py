"""Canonical bounded graph registry; no mission or family schedulers."""

from mission_control.adapters.agent_server import entrypoints

GRAPH_REGISTRY: dict[str, object] = {
    "belllabs_async_technical_child": entrypoints.child,
    "block_c_qualification": entrypoints.qualification,
    "block_c_qualification_n1": entrypoints.qualification_n1,
    "block_c_wait": entrypoints.wait,
}

__all__ = ["GRAPH_REGISTRY"]
