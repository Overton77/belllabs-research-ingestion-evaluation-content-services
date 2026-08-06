"""Disposable import-safe graph factory for Stage 0 qualification.

This module deliberately owns no network client, secret, database pool, sandbox,
or tracing bootstrap at import or graph-construction time.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph_sdk.runtime import ServerRuntime


def _merge_unique(left: tuple[str, ...], right: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted(set(left) | set(right)))


class QualificationState(TypedDict):
    request_scope: str
    events: Annotated[tuple[str, ...], _merge_unique]


@dataclass
class ResourceLedger:
    opened: int = 0
    closed: int = 0


LEDGER = ResourceLedger()


@asynccontextmanager
async def execution_resource() -> AsyncIterator[str]:
    """Represent a per-execution MCP/sandbox/session resource."""

    LEDGER.opened += 1
    try:
        yield "execution-resource"
    finally:
        LEDGER.closed += 1


async def execute(state: QualificationState) -> dict[str, tuple[str, ...]]:
    async with execution_resource() as resource:
        return {"events": (f"{state['request_scope']}:{resource}",)}


def build_graph(*, execution: bool) -> object:
    builder = StateGraph(QualificationState)
    if execution:
        builder.add_node("execute", execute)
        builder.add_edge(START, "execute")
        builder.add_edge("execute", END)
    else:
        builder.add_node("inspect", lambda _state: {"events": ("inspection",)})
        builder.add_edge(START, "inspect")
        builder.add_edge("inspect", END)
    return builder.compile()


async def graph_factory(
    _config: RunnableConfig,
    runtime: ServerRuntime[None],
) -> object:
    """Build an execution graph only for ``threads.create_run``.

    In langgraph-sdk 0.4.2, ``ServerRuntime`` is a type alias over execution
    and read dataclasses. ``execution_runtime`` returns the execution variant for
    ``threads.create_run`` and ``None`` for read/update/assistant contexts; only the
    execution variant has ``context``.
    """

    return build_graph(execution=runtime.access_context == "threads.create_run")
