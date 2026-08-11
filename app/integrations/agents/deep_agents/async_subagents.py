from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from deepagents.middleware import async_subagents as deepagents_async
from deepagents.middleware.async_subagents import AsyncSubAgent, AsyncSubAgentMiddleware
from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from app.application.async_subagents.service import ProviderAsyncObservation
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentExecution,
    AsyncSubagentMessage,
)


class DeepAgentsAsyncSubagentAdapter:
    """BellLabs wrapper around the exact Deep Agents 0.7.5 async middleware tools."""

    tool_names = (
        "start_async_task",
        "check_async_task",
        "update_async_task",
        "cancel_async_task",
        "list_async_tasks",
    )

    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        self._now = now or (lambda: datetime.now(UTC))
        self._middleware: dict[str, AsyncSubAgentMiddleware] = {}
        self._contracts: dict[str, AsyncSubagentContract] = {}

    def _tools(self, contract: AsyncSubagentContract) -> dict[str, Any]:
        middleware = self._middleware.get(contract.contract_digest)
        if middleware is None:
            spec: AsyncSubAgent = {
                "name": contract.name,
                "description": contract.description,
                "graph_id": contract.graph_id,
                "url": contract.agent_protocol_url,
            }
            middleware = AsyncSubAgentMiddleware(async_subagents=[spec])
            self._middleware[contract.contract_digest] = middleware
            self._contracts[contract.contract_digest] = contract
        tools = {tool.name: tool for tool in middleware.tools}
        if tuple(tools) != self.tool_names:
            raise RuntimeError("Deep Agents async middleware tool surface drifted from 0.7.5")
        return tools

    async def start(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        objective: str,
    ) -> ProviderAsyncObservation:
        # The stock tool invents its thread ID. Injecting the already-persisted
        # BellLabs child identity closes the timeout/crash ambiguity window while
        # retaining the exact 0.7.5 Agent Protocol mechanism and tool surface.
        self._tools(contract)
        client = deepagents_async.get_client(
            url=contract.agent_protocol_url,
            headers={"x-auth-scheme": "langsmith"},
        )
        thread_id = execution.child_execution_id
        await client.threads.create(
            thread_id=thread_id,
            if_exists="do_nothing",
            metadata={"belllabs_child_execution_id": thread_id},
        )
        runs = await client.runs.list(thread_id=thread_id, limit=100)
        run = next(
            (
                item
                for item in runs
                if (item.get("metadata") or {}).get("belllabs_spawn_key") == thread_id
            ),
            None,
        )
        if run is None:
            run = await client.runs.create(
                thread_id=thread_id,
                assistant_id=contract.graph_id,
                input=cast(Any, {"messages": [{"role": "user", "content": objective}]}),
                metadata={"belllabs_spawn_key": thread_id},
            )
        task = {
            "thread_id": thread_id,
            "run_id": str(run["run_id"]),
            "status": str(run.get("status", "running")),
        }
        return self._observation(task, task["status"])

    async def check(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        self.bind_contract(contract)
        state = self._state(execution, contract.name)
        result = await self._invoke(
            self._tools(contract)["check_async_task"], state, task_id=execution.provider_thread_id
        )
        task = self._single_task(result)
        payload = self._tool_payload(result)
        return self._observation(task, str(payload.get("status", task["status"])), payload)

    async def update(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        message: AsyncSubagentMessage,
    ) -> ProviderAsyncObservation:
        self.bind_contract(contract)
        result = await self._invoke(
            self._tools(contract)["update_async_task"],
            self._state(execution, contract.name),
            task_id=execution.provider_thread_id,
            message=message.payload_ref,
        )
        return self._observation(self._single_task(result), "running")

    async def cancel(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        self.bind_contract(contract)
        result = await self._invoke(
            self._tools(contract)["cancel_async_task"],
            self._state(execution, contract.name),
            task_id=execution.provider_thread_id,
        )
        return self._observation(self._single_task(result), "cancelled")

    async def list(
        self, executions: tuple[tuple[AsyncSubagentContract, AsyncSubagentExecution], ...]
    ) -> tuple[ProviderAsyncObservation, ...]:
        return tuple([await self.check(contract, item) for contract, item in executions])

    def bind_contract(self, contract: AsyncSubagentContract) -> None:
        """Bind an exact contract before reconnecting a persisted execution."""
        self._tools(contract)

    @staticmethod
    async def _invoke(tool: Any, state: dict[str, object], **kwargs: object) -> Command[Any]:
        runtime = ToolRuntime(
            state=state,
            context=None,
            config={},
            stream_writer=lambda _value: None,
            tool_call_id="belllabs-async-subagent",
            store=None,
            tools=[],
        )
        result = await tool.coroutine(runtime=runtime, **kwargs)
        if not isinstance(result, Command):
            raise RuntimeError(f"Deep Agents async tool rejected provider operation: {result}")
        return result

    @staticmethod
    def _single_task(command: Command[Any]) -> dict[str, str]:
        update = cast(dict[str, Any], command.update)
        tasks = cast(dict[str, dict[str, str]], update.get("async_tasks", {}))
        if len(tasks) != 1:
            raise RuntimeError("Deep Agents async tool did not return one exact task binding")
        return next(iter(tasks.values()))

    @staticmethod
    def _tool_payload(command: Command[Any]) -> dict[str, object]:
        messages = cast(list[ToolMessage], cast(dict[str, Any], command.update).get("messages", []))
        if not messages:
            return {}
        try:
            value = json.loads(str(messages[-1].content))
        except json.JSONDecodeError:
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _state(
        execution: AsyncSubagentExecution, agent_name: str | None = None
    ) -> dict[str, object]:
        if execution.provider_thread_id is None or execution.provider_run_id is None:
            raise RuntimeError("provider operation requires a submitted child binding")
        now = execution.updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        task = {
            "task_id": execution.provider_thread_id,
            "agent_name": agent_name or execution.contract_id,
            "thread_id": execution.provider_thread_id,
            "run_id": execution.provider_run_id,
            "status": execution.lifecycle.value,
            "created_at": execution.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_checked_at": now,
            "last_updated_at": now,
        }
        return {"async_tasks": {execution.provider_thread_id: task}}

    def _observation(
        self, task: dict[str, str], status: str, payload: dict[str, object] | None = None
    ) -> ProviderAsyncObservation:
        normalized = {
            "success": "success",
            "error": "error",
            "cancelled": "cancelled",
            "interrupted": "waiting",
            "timeout": "error",
        }.get(status, "running")
        output_ref = None
        if normalized == "success":
            output_ref = "ref:async-output:" + sha256_digest(
                (payload or {}).get("result", "")
            ).removeprefix("sha256:")
        return ProviderAsyncObservation(
            status=cast(Any, normalized),
            thread_id=task["thread_id"],
            run_id=task["run_id"],
            output_ref=output_ref,
            usage_ref=f"ref:async-usage:{task['run_id']}",
            checkpoint_ref=f"ref:agent-protocol-thread:{task['thread_id']}",
            observed_at=self._now(),
        )
