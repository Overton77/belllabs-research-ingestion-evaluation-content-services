"""Trusted deployment hook for an exact hosted graph; never selected by a request."""

from __future__ import annotations

import importlib
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from langchain_core.runnables import RunnableConfig

from mission_control.adapters.agent_server.async_subagents.bindings import (
    HostedAsyncSubagentDefinition,
    technical_child_definition,
)
from mission_control.adapters.agent_server.deployment import CHILD_GRAPH


@dataclass(frozen=True)
class HostedGraph:
    definition: HostedAsyncSubagentDefinition
    factory: Callable[[RunnableConfig], Awaitable[Any]]


@lru_cache(maxsize=1)
def hosted_graph() -> HostedGraph:
    specification = os.environ.get("MISSION_CONTROL_AGENT_HOSTING_FACTORY", "")
    if specification:
        if not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", specification):
            raise ValueError("hosting factory must be an installed module:callable")
        module, name = specification.split(":")
        registration = getattr(importlib.import_module(module), name)()
    else:
        from mission_control.adapters.agent_server.async_subagents.graph import graph

        registration = HostedGraph(technical_child_definition(), graph)
    if not isinstance(registration, HostedGraph) or not callable(registration.factory):
        raise TypeError("hosting factory must return HostedGraph")
    if registration.definition.graph_id != CHILD_GRAPH:
        raise ValueError("hosting registration does not match the canonical graph ID")
    return registration
