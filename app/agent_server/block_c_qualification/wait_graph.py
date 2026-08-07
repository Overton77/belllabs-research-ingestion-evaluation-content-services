"""Cancellable long-wait qualification graph with cleanup-visible typed state.

Agent Server cancel (action=interrupt) cancels the run coroutine with
``CancelledError(UserInterrupt)``. The hold node catches that cancellation,
returns a typed cleanup update, and lets the checkpoint commit so callers can
observe ``wait_status=cancelled`` and ``resource_open=False``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from langgraph.graph import END, START, StateGraph

from app.agent_server.block_c_qualification.compat import COMPAT_VERSION_N
from app.agent_server.block_c_qualification.state import WaitState


async def enter_wait(state: WaitState) -> dict[str, Any]:
    del state
    return {
        "compat_version": COMPAT_VERSION_N,
        "wait_status": "waiting",
        "resource_open": True,
        "events": ("wait-resource-opened",),
    }


async def hold(state: WaitState) -> dict[str, Any]:
    seconds = float(state.get("hold_seconds") or 120.0)
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        # Swallow framework cancel so the cleanup write is committed. Re-raising
        # would skip the typed cancelled outcome (see langgraph pregel commit for
        # CancelledError). Agent Server still records run cancellation separately.
        return {
            "wait_status": "cancelled",
            "resource_open": False,
            "events": ("wait-resource-closed-cancelled",),
        }
    return {
        "wait_status": "completed",
        "resource_open": False,
        "events": ("wait-resource-closed-completed",),
    }


def build_wait_graph() -> object:
    builder = StateGraph(WaitState)
    builder.add_node("enter_wait", enter_wait)
    builder.add_node("hold", hold)
    builder.add_edge(START, "enter_wait")
    builder.add_edge("enter_wait", "hold")
    builder.add_edge("hold", END)
    return builder.compile()


graph = build_wait_graph()
