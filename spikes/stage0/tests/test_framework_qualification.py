from __future__ import annotations

import asyncio
import dataclasses
import inspect
import operator
import random
from contextlib import suppress
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, TypedDict, get_args

import pytest
from deepagents import create_deep_agent
from deepagents.backends.langsmith import LangSmithSandbox
from deepagents.middleware.async_subagents import (
    AsyncSubAgent,
    AsyncSubAgentMiddleware,
    AsyncSubAgentState,
)
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_quickjs import CodeInterpreterMiddleware
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.memory import InMemoryStore
from langgraph.types import Command, Overwrite, Send, interrupt
from langgraph_sdk.runtime import AccessContext, ServerRuntime
from langsmith.sandbox import SandboxClient

from graph_factory import LEDGER, build_graph, execution_resource, graph_factory


def test_q01_exact_server_runtime_access_contexts_and_fields() -> None:
    assert set(get_args(AccessContext)) == {
        "threads.create_run",
        "threads.update",
        "threads.read",
        "assistants.read",
    }
    runtime_variants = get_args(ServerRuntime.__value__)
    field_sets = {
        variant.__origin__.__name__: {
            field.name for field in dataclasses.fields(variant.__origin__)
        }
        for variant in runtime_variants
    }
    assert field_sets == {
        "_ExecutionRuntime": {"access_context", "user", "store", "context"},
        "_ReadRuntime": {"access_context", "user", "store"},
    }
    variants = {variant.__origin__.__name__: variant.__origin__ for variant in runtime_variants}
    store = InMemoryStore()
    execution_runtime = variants["_ExecutionRuntime"](
        access_context="threads.create_run",
        store=store,
        context={"request_scope": "tenant-a"},
    )
    read_runtime = variants["_ReadRuntime"](
        access_context="threads.read",
        store=store,
    )
    assert execution_runtime.execution_runtime is execution_runtime
    assert read_runtime.execution_runtime is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("access_context", "execution"),
    [
        ("threads.create_run", True),
        ("threads.update", False),
        ("threads.read", False),
        ("assistants.read", False),
    ],
)
async def test_q01_graph_construction_has_no_resource_side_effect(
    access_context: AccessContext,
    execution: bool,
) -> None:
    before = dataclasses.replace(LEDGER)
    graph = await graph_factory(
        {},
        SimpleNamespace(access_context=access_context),  # type: ignore[arg-type]
    )
    assert dataclasses.replace(LEDGER) == before
    result = await graph.ainvoke({"request_scope": "tenant-a", "events": ()})
    assert ("tenant-a:execution-resource" in result["events"]) is execution
    assert LEDGER.opened - before.opened == int(execution)
    assert LEDGER.closed - before.closed == int(execution)


@pytest.mark.asyncio
async def test_q01_resource_cleanup_on_failure_and_cancellation() -> None:
    before = dataclasses.replace(LEDGER)
    with pytest.raises(RuntimeError):
        async with execution_resource():
            raise RuntimeError("synthetic failure")

    entered = asyncio.Event()

    async def hold_resource() -> None:
        async with execution_resource():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold_resource())
    await entered.wait()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert LEDGER.opened - before.opened == 2
    assert LEDGER.closed - before.closed == 2


class FrontierState(TypedDict):
    roots: tuple[str, ...]
    results: Annotated[list[tuple[str, str]], operator.add]


def _dispatch(state: FrontierState) -> list[Send]:
    return [Send("worker", {"root": root}) for root in state["roots"]]


def _worker(state: dict[str, str]) -> dict[str, list[tuple[str, str]]]:
    return {"results": [(state["root"], f"result:{state['root']}")]}


def test_q07_send_two_roots_and_join() -> None:
    graph = (
        StateGraph(FrontierState)
        .add_node("worker", _worker)
        .add_conditional_edges(START, _dispatch, ["worker"])
        .add_edge("worker", END)
        .compile()
    )
    result = graph.invoke({"roots": ("a", "b"), "results": []})
    assert sorted(result["results"]) == [("a", "result:a"), ("b", "result:b")]


def _idempotent_merge(
    left: tuple[tuple[str, str], ...],
    right: tuple[tuple[str, str], ...],
) -> tuple[tuple[str, str], ...]:
    by_key = dict(left)
    for key, value in right:
        prior = by_key.setdefault(key, value)
        if prior != value:
            raise ValueError(f"conflicting duplicate for {key}")
    return tuple(sorted(by_key.items()))


def test_q07_reducer_laws_under_random_merge_order() -> None:
    chunks = [(("a", "1"),), (("b", "2"),), (("a", "1"),), (("c", "3"),)]
    expected = (("a", "1"), ("b", "2"), ("c", "3"))
    for seed in range(100):
        shuffled = list(chunks)
        random.Random(seed).shuffle(shuffled)
        merged: tuple[tuple[str, str], ...] = ()
        for chunk in shuffled:
            merged = _idempotent_merge(merged, chunk)
        assert merged == expected
        assert _idempotent_merge(merged, merged) == merged
    with pytest.raises(ValueError, match="conflicting duplicate"):
        _idempotent_merge((("a", "1"),), (("a", "different"),))


class InterruptState(TypedDict):
    effects: Annotated[list[str], operator.add]
    decisions: Annotated[list[str], operator.add]


def test_q08_interrupt_resume_idempotent_claim_and_overwrite() -> None:
    pre_interrupt_claims: set[str] = set()

    def approval(state: InterruptState) -> dict[str, list[str]]:
        pre_interrupt_claims.add("stable-claim")
        decision = interrupt({"decision_ref": "decision:stable-claim"})
        return {"decisions": [str(decision)]}

    graph = (
        StateGraph(InterruptState)
        .add_node("approval", approval)
        .add_edge(START, "approval")
        .add_edge("approval", END)
        .compile(checkpointer=InMemorySaver())
    )
    config = {"configurable": {"thread_id": "tenant-a:run-a:epoch-1"}}
    first = graph.invoke({"effects": [], "decisions": []}, config)
    assert first["__interrupt__"]
    resumed = graph.invoke(Command(resume="approved-from-authority"), config)
    assert resumed["decisions"] == ["approved-from-authority"]
    assert pre_interrupt_claims == {"stable-claim"}

    graph.update_state(config, {"decisions": ["appended"]})
    assert graph.get_state(config).values["decisions"][-1] == "appended"
    graph.update_state(config, {"decisions": Overwrite(["privileged-repair"])})
    assert graph.get_state(config).values["decisions"] == ["privileged-repair"]


def test_q09_mcp_adapter_pin_and_session_surface_are_compatible() -> None:
    assert version("langchain-mcp-adapters") == "0.3.1"
    assert version("mcp") == "1.29.0"
    client = inspect.signature(MultiServerMCPClient).parameters
    assert {"connections", "callbacks", "tool_interceptors", "tool_name_prefix"} <= set(client)
    assert "server_name" in inspect.signature(MultiServerMCPClient.session).parameters
    loader = inspect.signature(load_mcp_tools).parameters
    assert {"session", "connection", "tool_interceptors", "server_name"} <= set(loader)
    spike_root = Path(__file__).parents[1]
    assert '"mcp==1.29.0"' in (spike_root / "pyproject.toml").read_text()
    lock = (spike_root / "uv.lock").read_text()
    assert 'name = "mcp"\nversion = "1.29.0"' in lock


def test_q11_async_subagent_preview_surface_is_explicit() -> None:
    assert AsyncSubAgent.__annotations__ == {
        "name": str,
        "description": str,
        "graph_id": str,
        "url": pytest.importorskip("typing").NotRequired[str],
        "headers": pytest.importorskip("typing").NotRequired[dict[str, str]],
    }
    assert "async_tasks" in AsyncSubAgentState.__annotations__
    signature = inspect.signature(AsyncSubAgentMiddleware)
    assert "async_subagents" in signature.parameters


def test_q13_langsmith_sandbox_surface_is_explicit_but_not_entitlement_proven() -> None:
    create = inspect.signature(SandboxClient.create_sandbox).parameters
    assert {
        "snapshot_id",
        "idle_ttl_seconds",
        "delete_after_stop_seconds",
        "vcpus",
        "mem_bytes",
        "fs_capacity_bytes",
        "mount_config",
        "proxy_config",
    } <= set(create)
    assert tuple(inspect.signature(LangSmithSandbox).parameters) == ("sandbox",)


def test_q14_deep_agent_and_quickjs_surfaces_are_explicit() -> None:
    deep_agent_parameters = inspect.signature(create_deep_agent).parameters
    assert {"middleware", "subagents", "skills", "memory", "permissions"} <= set(
        deep_agent_parameters
    )
    quickjs = inspect.signature(CodeInterpreterMiddleware).parameters
    assert quickjs["subagents"].default is True
    assert quickjs["mode"].default is None
    assert quickjs["memory_limit"].default == 67_108_864
    assert quickjs["timeout"].default == 5.0
    assert quickjs["max_ptc_calls"].default == 256

    agent = create_deep_agent(model=FakeListChatModel(responses=["done"]))
    assert set(agent.nodes) == {
        "__start__",
        "model",
        "tools",
        "PatchToolCallsMiddleware.before_agent",
    }
    assert set(agent.nodes["tools"].bound.tools_by_name) == {
        "delete",
        "edit_file",
        "execute",
        "glob",
        "grep",
        "ls",
        "read_file",
        "task",
        "write_file",
    }


def test_graph_module_build_is_import_safe() -> None:
    before = dataclasses.replace(LEDGER)
    build_graph(execution=True)
    build_graph(execution=False)
    assert dataclasses.replace(LEDGER) == before
