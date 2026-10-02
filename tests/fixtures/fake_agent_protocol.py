"""A deterministic fake of the Agent Protocol surface the async subagent adapter uses.

Offline regression only: it mirrors the LangGraph SDK calls the stock Deep Agents 0.7.5
middleware and the BellLabs adapter make (threads, runs, thread state, the served-graph
route), with crash hooks around run creation. It never satisfies a live gate (RRM-013).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx
from langgraph_sdk.errors import APIStatusError

from app.domain.operation_execution.async_subagent_reconciliation import AsyncServedGraphIdentity


def _not_found(message: str) -> APIStatusError:
    request = httpx.Request("GET", "http://fake-agent-protocol.invalid/")
    return APIStatusError(
        message, response=httpx.Response(404, request=request, text=message), body=None
    )


@dataclass
class FakeAgentProtocolClient:
    served: AsyncServedGraphIdentity
    calls: list[str] = field(default_factory=list)
    threads_store: dict[str, dict[str, Any]] = field(default_factory=dict)
    runs_store: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Crash hooks around run creation: raise before the run exists, or after it does.
    fail_before_create: Exception | None = None
    fail_after_create: Exception | None = None
    fail_list: Exception | None = None
    # Arms `fail_list` once a run exists: the submission happened but cannot be observed.
    fail_list_after_create: Exception | None = None
    stamp_identity: bool = True
    tokens_per_turn: int | None = 7

    def __post_init__(self) -> None:
        self.http = _Http(self)
        self.threads = _Threads(self)
        self.runs = _Runs(self)

    # ---------------------------------------------------------------- scripted provider

    def runs_of(self, thread_id: str) -> list[dict[str, Any]]:
        return self.runs_store.get(thread_id, [])

    def add_foreign_run(self, thread_id: str, spawn_key: str) -> str:
        """A second run carrying the spawn key (an ambiguity the adapter must not create)."""

        run_id = f"run-{uuid4().hex[:8]}"
        self.runs_store.setdefault(thread_id, []).append(
            {
                "run_id": run_id,
                "thread_id": thread_id,
                "status": "running",
                "metadata": {"belllabs_spawn_key": spawn_key},
                "created_at": f"2026-10-01T00:00:{len(self.runs_store[thread_id]):02d}Z",
            }
        )
        return run_id

    def complete(self, thread_id: str, text: str, *, run_id: str | None = None) -> None:
        for run in self.runs_of(thread_id):
            if run_id is None or run["run_id"] == run_id:
                run["status"] = "success"
        thread = self.threads_store.setdefault(thread_id, {"metadata": {}, "values": {}})
        message: dict[str, Any] = {"type": "ai", "content": text}
        if self.tokens_per_turn is not None:
            message["usage_metadata"] = {
                "input_tokens": 3,
                "output_tokens": 4,
                "total_tokens": self.tokens_per_turn,
            }
        values: dict[str, Any] = {
            "messages": [{"type": "human", "content": "objective"}, message],
        }
        if self.stamp_identity:
            values["belllabs_served_graph"] = self.served.model_dump(mode="json")
        thread["values"] = values
        thread["checkpoint"] = {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": f"ckpt-{uuid4().hex[:8]}",
        }


class _Http:
    def __init__(self, client: FakeAgentProtocolClient) -> None:
        self._client = client

    async def get(self, path: str, **_kwargs: object) -> dict[str, Any]:
        self._client.calls.append(f"http.get:{path}")
        return {"graphs": [self._client.served.model_dump(mode="json")]}


class _Threads:
    def __init__(self, client: FakeAgentProtocolClient) -> None:
        self._client = client

    async def create(self, **kwargs: Any) -> dict[str, Any]:
        thread_id = str(kwargs.get("thread_id") or uuid4())
        self._client.calls.append("sdk.threads.create")
        self._client.threads_store.setdefault(
            thread_id, {"metadata": dict(kwargs.get("metadata") or {}), "values": {}}
        )
        return {"thread_id": thread_id}

    async def get(self, thread_id: str, **_kwargs: Any) -> dict[str, Any]:
        thread = self._client.threads_store.get(thread_id)
        if thread is None:
            raise _not_found("thread not found")
        return {"thread_id": thread_id, "values": thread.get("values", {})}

    async def get_state(self, thread_id: str, **_kwargs: Any) -> dict[str, Any]:
        thread = self._client.threads_store.get(thread_id)
        if thread is None:
            raise _not_found("thread not found")
        return {
            "values": thread.get("values", {}),
            "checkpoint": thread.get(
                "checkpoint", {"thread_id": thread_id, "checkpoint_ns": "", "checkpoint_id": None}
            ),
            "next": [],
            "tasks": [],
            "interrupts": [],
        }


class _Runs:
    def __init__(self, client: FakeAgentProtocolClient) -> None:
        self._client = client

    async def list(self, thread_id: str, **_kwargs: Any) -> list[dict[str, Any]]:
        self._client.calls.append("sdk.runs.list")
        if self._client.fail_list is not None:
            raise self._client.fail_list
        if thread_id not in self._client.threads_store:
            raise _not_found("thread not found")
        return list(self._client.runs_of(thread_id))

    async def create(self, thread_id: str, assistant_id: str, **kwargs: Any) -> dict[str, Any]:
        self._client.calls.append("sdk.runs.create")
        if self._client.fail_before_create is not None:
            error, self._client.fail_before_create = self._client.fail_before_create, None
            raise error
        run = {
            "run_id": f"run-{uuid4().hex[:8]}",
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "status": "running",
            "metadata": dict(kwargs.get("metadata") or {}),
            "multitask_strategy": kwargs.get("multitask_strategy"),
            "created_at": f"2026-10-01T00:00:{len(self._client.runs_of(thread_id)):02d}Z",
        }
        self._client.runs_store.setdefault(thread_id, []).append(run)
        if self._client.fail_list_after_create is not None:
            self._client.fail_list = self._client.fail_list_after_create
            self._client.fail_list_after_create = None
        if self._client.fail_after_create is not None:
            error, self._client.fail_after_create = self._client.fail_after_create, None
            raise error
        return dict(run)

    async def get(self, thread_id: str, run_id: str, **_kwargs: Any) -> dict[str, Any]:
        self._client.calls.append("sdk.runs.get")
        for run in self._client.runs_of(thread_id):
            if run["run_id"] == run_id:
                return dict(run)
        raise _not_found("run not found")

    async def cancel(self, thread_id: str, run_id: str, **_kwargs: Any) -> None:
        self._client.calls.append("sdk.runs.cancel")
        for run in self._client.runs_of(thread_id):
            if run["run_id"] == run_id and run["status"] not in {"success", "error"}:
                run["status"] = "interrupted"


def install(monkeypatch: Any, client: FakeAgentProtocolClient) -> None:
    """Route the adapter's `deepagents_async.get_client` to the fake."""

    from deepagents.middleware import async_subagents as deepagents_async

    def get_client(**_kwargs: object) -> FakeAgentProtocolClient:
        return client

    monkeypatch.setattr(deepagents_async, "get_client", get_client)


ClientFactory = Callable[[], FakeAgentProtocolClient]
