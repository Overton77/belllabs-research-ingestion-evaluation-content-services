"""Deep Agents 0.7.5 async subagents under BellLabs authority (REQ-CP-DA-008/011/019).

Two adapters live here:

- `DeepAgentsAsyncSubagentAdapter` is the provider port of `AsyncSubagentService`. It wraps the
  exact `AsyncSubAgentMiddleware` tools for check, update, cancel and list, and performs the
  governed start: the BellLabs `child_execution_id` is the provider thread, the run carries the
  BellLabs spawn key, an existing run is looked up before one is created, and the provider is
  asked to reject a concurrent run on the thread. It verifies the served graph identity before
  submission and on every reconnect, and reads the qualified checkpoint and provider-attributed
  usage of a completed run.
- `BellLabsAsyncSubagentMiddleware` is what the parent Deep Agent sees inside `operation.execute`:
  the five stock tool names and schemas, whose coroutines pass through the BellLabs service, so
  the model's `start_async_task` reserves and links the child before any provider submission.

Credentials are resolved from references only; no secret value is persisted or logged.

Annotations are evaluated eagerly on purpose: `StructuredTool` recognises the injected
`ToolRuntime` parameter from the coroutine signature's runtime annotation, so this module
must not postpone annotation evaluation.
"""

import asyncio
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal, cast

from deepagents.middleware import async_subagents as deepagents_async
from deepagents.middleware.async_subagents import (
    AsyncSubAgent,
    AsyncSubAgentMiddleware,
    AsyncSubAgentState,
    AsyncTask,
)
from langchain.agents.middleware.types import AgentMiddleware
from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langchain_core.tools import StructuredTool
from langgraph.types import Command
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.errors import APIStatusError

from app.application.async_subagents.service import (
    AsyncProviderAmbiguity,
    AsyncServedGraphMismatch,
    AsyncSubagentError,
    AsyncSubagentService,
    AsyncSubagentSpawnRequest,
    ProviderAsyncObservation,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.async_subagent_reconciliation import (
    AsyncProviderRunObservation,
    AsyncProviderRunRecord,
    AsyncServedGraphIdentity,
    AsyncSpawnKeyObservation,
)
from app.domain.operation_execution.contracts import (
    AsyncProviderCheckpointKey,
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
    AsyncSubagentUsage,
    OperationExecutionBinding,
)

SPAWN_KEY_METADATA = "belllabs_spawn_key"
REQUEST_SCOPE_HEADER = "x-belllabs-request-scope"
SERVED_GRAPHS_PATH = "/belllabs/async-subagents/served-graphs"
SERVED_GRAPH_STATE_KEY = "belllabs_served_graph"
PROVIDER_USAGE_STATE_KEY = "belllabs_provider_usage"
_CANCEL_ACK_POLLS = 10
# Every Agent Protocol request of the governed adapter is bounded well below the submission
# lease (120 s by default), so a fence holder never outlives its lease on a slow server.
SDK_REQUEST_TIMEOUT_SECONDS = 60.0
_CANCEL_ACK_INTERVAL_SECONDS = 0.5
_TERMINAL_RUN_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})


class DeepAgentsAsyncSubagentAdapter:
    """BellLabs wrapper around the exact Deep Agents 0.7.5 async middleware tools."""

    tool_names = (
        "start_async_task",
        "check_async_task",
        "update_async_task",
        "cancel_async_task",
        "list_async_tasks",
    )

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        secrets: Mapping[str, str] | None = None,
        request_scope: str | None = None,
        cancel_ack_interval_seconds: float = _CANCEL_ACK_INTERVAL_SECONDS,
    ) -> None:
        self._now = now or (lambda: datetime.now(UTC))
        self._cancel_ack_interval = cancel_ack_interval_seconds
        self._secrets = dict(secrets or {})
        self._request_scope = request_scope
        self._middleware: dict[str, AsyncSubAgentMiddleware] = {}
        self._contracts: dict[str, AsyncSubagentContract] = {}
        self._clients: dict[str, LangGraphClient] = {}

    # ------------------------------------------------------------------ exact tool surface

    def _headers(self, contract: AsyncSubagentContract) -> dict[str, str]:
        headers = {"x-auth-scheme": "langsmith"}
        if contract.deployment_credential_ref is not None:
            try:
                token = self._secrets[contract.deployment_credential_ref]
            except KeyError as error:
                raise AsyncSubagentError(
                    "async subagent deployment credential reference was not resolved"
                ) from error
            headers["Authorization"] = f"Bearer {token}"
        if self._request_scope is not None:
            headers[REQUEST_SCOPE_HEADER] = self._request_scope
        return headers

    def _tools(self, contract: AsyncSubagentContract) -> dict[str, Any]:
        middleware = self._middleware.get(contract.contract_digest)
        if middleware is None:
            spec: AsyncSubAgent = {
                "name": contract.name,
                "description": contract.description,
                "graph_id": contract.graph_id,
                "url": contract.agent_protocol_url,
                "headers": self._headers(contract),
            }
            middleware = AsyncSubAgentMiddleware(async_subagents=[spec])
            self._middleware[contract.contract_digest] = middleware
            self._contracts[contract.contract_digest] = contract
        tools = {tool.name: tool for tool in middleware.tools}
        if tuple(tools) != self.tool_names:
            raise RuntimeError("Deep Agents async middleware tool surface drifted from 0.7.5")
        return tools

    def stock_tools(self, contract: AsyncSubagentContract) -> dict[str, StructuredTool]:
        """The drift-checked stock tools, for the governed middleware's schemas and descriptions."""

        return cast(dict[str, StructuredTool], self._tools(contract))

    def _client(self, contract: AsyncSubagentContract) -> LangGraphClient:
        client = self._clients.get(contract.contract_digest)
        if client is None:
            client = deepagents_async.get_client(
                url=contract.agent_protocol_url,
                headers=self._headers(contract),
                timeout=SDK_REQUEST_TIMEOUT_SECONDS,
            )
            self._clients[contract.contract_digest] = client
        return client

    # ------------------------------------------------------------------ DA-019

    async def verify_served_graph(
        self, contract: AsyncSubagentContract
    ) -> AsyncServedGraphIdentity:
        """Read the deployment's served graph identities; the contract's graph must be exact."""

        self._tools(contract)
        payload = await self._client(contract).http.get(SERVED_GRAPHS_PATH)
        graphs = payload.get("graphs", []) if isinstance(payload, dict) else []
        for item in graphs:
            if isinstance(item, dict) and item.get("graph_id") == contract.graph_id:
                served = AsyncServedGraphIdentity.model_validate(item)
                if not served.matches(contract):
                    raise AsyncServedGraphMismatch(
                        "served graph identity differs from the frozen async subagent contract"
                    )
                return served
        raise AsyncServedGraphMismatch(f"the deployment does not serve graph {contract.graph_id!r}")

    # ------------------------------------------------------------------ start and observe

    async def start(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        objective: str,
    ) -> ProviderAsyncObservation:
        """Submit once per child (the caller holds the child's submission fence).

        The stock tool invents its thread ID. Injecting the already-persisted BellLabs child
        identity closes the timeout/crash ambiguity window while keeping the exact 0.7.5 Agent
        Protocol mechanism. The run carries the spawn key, an existing run is adopted before one
        is created, and the server rejects a concurrent run on the thread.
        """

        self._tools(contract)
        client = self._client(contract)
        thread_id = execution.child_execution_id
        metadata = {
            "belllabs_child_execution_id": thread_id,
            "belllabs_parent_run_id": execution.parent_run_id,
            "belllabs_parent_binding_id": execution.parent_binding_id,
            "belllabs_contract_digest": execution.contract_digest,
            **({"request_scope": self._request_scope} if self._request_scope else {}),
        }
        await client.threads.create(
            thread_id=thread_id,
            if_exists="do_nothing",
            metadata=metadata,
            graph_id=contract.graph_id,
        )
        existing = await self.observe_spawn_key(contract, execution)
        if len(existing.runs) > 1:
            raise AsyncProviderAmbiguity(
                "more than one provider run carries the child's spawn key",
                reason="multiple_provider_runs",
                observation=existing,
            )
        if existing.runs:
            run = existing.runs[0]
            if run.served_graph is not None and not run.served_graph.matches(contract):
                raise AsyncProviderAmbiguity(
                    "the existing provider run was served by a different graph",
                    reason="graph_identity_mismatch",
                    observation=existing,
                )
            return await self._observe_run(contract, execution, run.run_id)
        created = await client.runs.create(
            thread_id=thread_id,
            assistant_id=contract.graph_id,
            input=cast(Any, {"messages": [{"role": "user", "content": objective}]}),
            metadata={**metadata, SPAWN_KEY_METADATA: thread_id},
            multitask_strategy="reject",
        )
        return self._observation(
            thread_id, str(created["run_id"]), str(created.get("status", "pending"))
        )

    async def observe_spawn_key(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> AsyncSpawnKeyObservation:
        """Every provider run that carries this child's spawn key, with the served identity."""

        self._tools(contract)
        client = self._client(contract)
        thread_id = execution.child_execution_id
        try:
            runs = await client.runs.list(thread_id, limit=100)
        except APIStatusError as error:
            if error.response.status_code == 404:
                return AsyncSpawnKeyObservation(thread_id=thread_id, observed_at=self._now())
            raise
        matching = [
            item
            for item in runs
            if (item.get("metadata") or {}).get(SPAWN_KEY_METADATA) == thread_id
        ]
        served: AsyncServedGraphIdentity | None = None
        if matching:
            served = await self._served_identity(client, thread_id)
        return AsyncSpawnKeyObservation(
            thread_id=thread_id,
            runs=tuple(
                AsyncProviderRunObservation(
                    run_id=str(item["run_id"]),
                    status=str(item.get("status") or "pending"),
                    served_graph=served,
                )
                for item in sorted(matching, key=lambda item: str(item.get("created_at") or ""))
            ),
            observed_at=self._now(),
        )

    async def _served_identity(
        self, client: LangGraphClient, thread_id: str
    ) -> AsyncServedGraphIdentity | None:
        """The identity the hosted graph stamped into thread state, if it has run at all."""

        try:
            state = await client.threads.get_state(thread_id)
        except APIStatusError as error:
            if error.response.status_code == 404:
                return None
            raise
        values = state.get("values") or {}
        stamped = values.get(SERVED_GRAPH_STATE_KEY) if isinstance(values, dict) else None
        if not isinstance(stamped, dict):
            return None
        return AsyncServedGraphIdentity.model_validate(stamped)

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
        status = str(payload.get("status", task["status"]))
        if status == "success":
            return await self._completed_observation(contract, execution, task, payload)
        return self._observation(task["thread_id"], task["run_id"], status)

    async def _observe_run(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution, run_id: str
    ) -> ProviderAsyncObservation:
        bound = execution.model_copy(
            update={
                "lifecycle": AsyncSubagentLifecycle.RUNNING,
                "provider_thread_id": execution.child_execution_id,
                "provider_run_id": run_id,
                "in_doubt_reason": None,
                "incident_id": None,
            }
        )
        return await self.check(contract, bound)

    async def _completed_observation(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        task: dict[str, str],
        payload: dict[str, object],
    ) -> ProviderAsyncObservation:
        """REQ-CP-DA-011: qualified provider checkpoint, verified identity, attributed usage."""

        client = self._client(contract)
        state = await client.threads.get_state(task["thread_id"])
        values = cast(dict[str, Any], state.get("values") or {})
        stamped = values.get(SERVED_GRAPH_STATE_KEY)
        if not isinstance(stamped, dict):
            raise AsyncProviderAmbiguity(
                "the completed thread carries no served graph identity",
                reason="graph_identity_mismatch",
                observation=await self.observe_spawn_key(contract, execution),
            )
        served = AsyncServedGraphIdentity.model_validate(stamped)
        if not served.matches(contract):
            raise AsyncProviderAmbiguity(
                "the completed thread was served by a different graph",
                reason="graph_identity_mismatch",
                observation=await self.observe_spawn_key(contract, execution),
            )
        checkpoint = cast(dict[str, Any], state.get("checkpoint") or {})
        checkpoint_id = checkpoint.get("checkpoint_id")
        if not checkpoint_id:
            raise AsyncSubagentError("the provider reported no checkpoint for the completed run")
        key = AsyncProviderCheckpointKey(
            agent_protocol_url=contract.agent_protocol_url,
            thread_id=task["thread_id"],
            checkpoint_ns=str(checkpoint.get("checkpoint_ns") or ""),
            checkpoint_id=str(checkpoint_id),
            graph_id=served.graph_id,
            graph_revision=served.graph_revision,
            graph_binding_digest=served.graph_binding_digest,
        )
        output_text = str(payload.get("result", ""))
        return ProviderAsyncObservation(
            status="success",
            thread_id=task["thread_id"],
            run_id=task["run_id"],
            output_ref="ref:async-output:" + sha256_digest(output_text).removeprefix("sha256:"),
            output_text=output_text,
            usage=attribute_usage(
                task["run_id"],
                values.get("messages"),
                contract.budget_limits,
                provider_usage=values.get(PROVIDER_USAGE_STATE_KEY),
            ),
            checkpoint=key,
            observed_at=self._now(),
        )

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
        task = self._single_task(result)
        return self._observation(task["thread_id"], task["run_id"], "running")

    async def cancel(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        """Cancel through the stock tool, then record the provider's acknowledgement honestly."""

        self.bind_contract(contract)
        result = await self._invoke(
            self._tools(contract)["cancel_async_task"],
            self._state(execution, contract.name),
            task_id=execution.provider_thread_id,
        )
        task = self._single_task(result)
        status, receipt = await self._cancel_receipt(contract, task["thread_id"], task["run_id"])
        return self._observation(
            task["thread_id"], task["run_id"], status, cancellation_receipt=receipt
        )

    async def cancel_run(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        run_id: str,
    ) -> AsyncProviderRunRecord:
        """Cancel a duplicate or orphaned run and record the outcome honestly (REQ-CP-DA-008).

        A terminal status observed after the cancel yields a cancelled record with pending
        usage. If no terminal status is observed within the acknowledgement poll, the record is
        `cancel_ambiguous` with ambiguous usage: the run stays a candidate of later spawn-key
        classifications until a terminal status is observed (RRM-013 review N2).
        """

        self._tools(contract)
        client = self._client(contract)
        thread_id = execution.child_execution_id
        try:
            await client.runs.cancel(thread_id, run_id)
        except APIStatusError as error:
            if error.response.status_code not in {404, 409}:
                raise
        status, receipt = await self._cancel_receipt(contract, thread_id, run_id)
        terminal = receipt == "provider_acknowledged" or status in _TERMINAL_RUN_STATUSES
        return AsyncProviderRunRecord(
            child_execution_id=thread_id,
            provider_thread_id=thread_id,
            provider_run_id=run_id,
            disposition="duplicate_cancelled" if terminal else "cancel_ambiguous",
            provider_status="interrupted" if status == "cancelled" else status,
            usage=AsyncSubagentUsage(
                provider_run_id=run_id, attribution="pending" if terminal else "ambiguous"
            ),
            observed_at=self._now(),
        )

    async def _cancel_receipt(
        self, contract: AsyncSubagentContract, thread_id: str, run_id: str
    ) -> tuple[str, Literal["provider_acknowledged", "ambiguous"]]:
        client = self._client(contract)
        last = "running"
        for _ in range(_CANCEL_ACK_POLLS):
            try:
                run = await client.runs.get(thread_id, run_id)
            except APIStatusError as error:
                if error.response.status_code == 404:
                    return "cancelled", "provider_acknowledged"
                raise
            last = str(run.get("status") or "running")
            if last in {"interrupted", "cancelled"}:
                return "cancelled", "provider_acknowledged"
            if last in _TERMINAL_RUN_STATUSES:
                return last, "ambiguous"
            await asyncio.sleep(self._cancel_ack_interval)
        return last, "ambiguous"

    async def list(
        self, executions: tuple[tuple[AsyncSubagentContract, AsyncSubagentExecution], ...]
    ) -> tuple[ProviderAsyncObservation, ...]:
        return tuple([await self.check(contract, item) for contract, item in executions])

    def bind_contract(self, contract: AsyncSubagentContract) -> None:
        """Bind an exact contract before reconnecting a persisted execution."""
        self._tools(contract)

    # ------------------------------------------------------------------ helpers

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
        self,
        thread_id: str,
        run_id: str,
        status: str,
        *,
        cancellation_receipt: Literal["provider_acknowledged", "ambiguous"] | None = None,
    ) -> ProviderAsyncObservation:
        normalized = {
            "pending": "pending",
            "running": "running",
            "error": "error",
            "cancelled": "cancelled",
            "interrupted": "cancelled" if cancellation_receipt is not None else "waiting",
            "timeout": "error",
        }.get(status, "running")
        return ProviderAsyncObservation(
            status=cast(Any, normalized),
            thread_id=thread_id,
            run_id=run_id,
            cancellation_receipt=cancellation_receipt,
            observed_at=self._now(),
        )


def attribute_usage(
    run_id: str,
    messages: object,
    budget_limits: Mapping[str, int],
    *,
    provider_usage: object = None,
) -> AsyncSubagentUsage:
    """Provider-attributed usage from the hosted graph's usage stamps, or pending.

    The hosted graph records each model call's provider-reported usage in the thread state
    channel `belllabs_provider_usage` (the Agent Server's serialized messages omit
    `usage_metadata`); a message-level `usage_metadata` is accepted as a fallback.
    `tokens.total` sums `total_tokens`; `model.turns` counts AI messages. Only the contract's
    budget dimensions are reported. The usage is `provider_attributed` only when every AI
    turn is reported; otherwise, and when there is no AI turn at all, it is pending with no
    invented amounts (REQ-CP-DA-011; RRM-013 review N6).
    """

    if not isinstance(messages, list):
        return AsyncSubagentUsage(provider_run_id=run_id, attribution="pending")
    stamped = {
        str(item.get("message_id")): int(item.get("total_tokens") or 0)
        for item in (provider_usage if isinstance(provider_usage, list) else [])
        if isinstance(item, dict)
    }
    turns = 0
    tokens = 0
    unreported = 0
    for message in messages:
        if not isinstance(message, dict) or message.get("type") != "ai":
            continue
        turns += 1
        usage = message.get("usage_metadata")
        if str(message.get("id")) in stamped:
            tokens += stamped[str(message.get("id"))]
        elif isinstance(usage, dict) and usage.get("total_tokens") is not None:
            tokens += int(usage.get("total_tokens") or 0)
        else:
            unreported += 1
    amounts: dict[str, int] = {}
    if "model.turns" in budget_limits:
        amounts["model.turns"] = turns
    if "tokens.total" in budget_limits:
        amounts["tokens.total"] = tokens
    if turns == 0 or unreported:
        return AsyncSubagentUsage(
            provider_run_id=run_id,
            attribution="pending",
            pending_amounts={key: value for key, value in amounts.items() if key == "model.turns"},
        )
    return AsyncSubagentUsage(
        provider_run_id=run_id, attribution="provider_attributed", attributed_amounts=amounts
    )


class BellLabsAsyncSubagentMiddleware(AgentMiddleware[Any, Any, Any]):
    """The exact 0.7.5 async tool surface, governed by the BellLabs service.

    The parent Deep Agent calls `start_async_task`; BellLabs reserves and links the child in
    Mongo and PostgreSQL, claims it as an effect of the parent operation, then submits once
    through the fenced path. `check_async_task` reconciles the child and reveals the child's
    output only once the parent authority admitted its result (REQ-CP-DA-011).
    """

    state_schema = AsyncSubAgentState

    def __init__(
        self,
        *,
        service: AsyncSubagentService,
        adapter: DeepAgentsAsyncSubagentAdapter,
        binding: OperationExecutionBinding,
        contracts: tuple[AsyncSubagentContract, ...],
        dependency_class: AsyncSubagentDependencyClass = AsyncSubagentDependencyClass.NONBLOCKING,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__()
        if not contracts:
            raise ValueError("at least one exact async subagent contract is required")
        deep_binding = binding.deep_agent_binding
        if deep_binding is None:
            raise ValueError("the governed async middleware requires the Deep Agent binding")
        self._service = service
        self._adapter = adapter
        self._binding = binding
        self._generation = deep_binding.execution_generation
        self._contracts = {contract.name: contract for contract in contracts}
        self._dependency_class = dependency_class
        self._now = now or (lambda: datetime.now(UTC))
        stock = adapter.stock_tools(contracts[0])
        self.tools = [
            StructuredTool.from_function(
                name="start_async_task",
                coroutine=self._start,
                description=stock["start_async_task"].description,
                infer_schema=False,
                args_schema=stock["start_async_task"].args_schema,
            ),
            StructuredTool.from_function(
                name="check_async_task",
                coroutine=self._check,
                description=stock["check_async_task"].description,
                infer_schema=False,
                args_schema=stock["check_async_task"].args_schema,
            ),
            StructuredTool.from_function(
                name="update_async_task",
                coroutine=self._update,
                description=stock["update_async_task"].description,
                infer_schema=False,
                args_schema=stock["update_async_task"].args_schema,
            ),
            StructuredTool.from_function(
                name="cancel_async_task",
                coroutine=self._cancel,
                description=stock["cancel_async_task"].description,
                infer_schema=False,
                args_schema=stock["cancel_async_task"].args_schema,
            ),
            StructuredTool.from_function(
                name="list_async_tasks",
                coroutine=self._list,
                description=stock["list_async_tasks"].description,
                infer_schema=False,
                args_schema=stock["list_async_tasks"].args_schema,
            ),
        ]

    @property
    def scope(self) -> str:
        return self._binding.request_scope

    def spawn_request(
        self, *, description: str, contract: AsyncSubagentContract, tool_call_id: str
    ) -> AsyncSubagentSpawnRequest:
        """The deterministic spawn request of one model tool call; retries rebuild it exactly."""

        binding = self._binding
        return AsyncSubagentSpawnRequest(
            request_scope=binding.request_scope,
            parent_run_id=binding.run_id,
            parent_operation_id=binding.operation_id,
            parent_binding_id=binding.binding_id,
            execution_generation=self._generation,
            contract=contract,
            dependency_class=self._dependency_class,
            objective_ref="ref:async-objective:"
            + sha256_digest(description).removeprefix("sha256:"),
            objective=description,
            context_slice_ref=f"ref:async-context-slice:{binding.binding_id}:{contract.context_slice_id}",
            reservation_id=f"{binding.budget_reservation_id}:async:{sha256_digest(tool_call_id)[7:23]}",
            idempotency_key=tool_call_id,
            requested_at=self._now(),
            parent_reservation_id=binding.budget_reservation_id,
        )

    async def _start(
        self, description: str, subagent_type: str, runtime: ToolRuntime
    ) -> str | Command:
        contract = self._contracts.get(subagent_type)
        if contract is None:
            allowed = ", ".join(f"`{name}`" for name in self._contracts)
            return f"Unknown async subagent type `{subagent_type}`. Available types: {allowed}"
        tool_call_id = str(runtime.tool_call_id or "")
        if not tool_call_id:
            return "Failed to launch async subagent: the tool call carries no identity"
        try:
            execution = await self._service.spawn(
                self.spawn_request(
                    description=description, contract=contract, tool_call_id=tool_call_id
                )
            )
        except AsyncSubagentError as error:
            return f"Failed to launch async subagent '{subagent_type}': {error}"
        task = self._task(execution, contract.name)
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        f"Launched async subagent. task_id: {execution.child_execution_id}",
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
                "async_tasks": {execution.child_execution_id: task},
            }
        )

    async def _check(self, task_id: str, runtime: ToolRuntime) -> str | Command:
        tracked = self._tracked(task_id, runtime)
        if isinstance(tracked, str):
            return tracked
        try:
            execution = await self._service.reconcile(self.scope, tracked["task_id"])
        except AsyncSubagentError as error:
            return f"Failed to get run status: {error}"
        result: dict[str, object] = {
            "status": execution.lifecycle.value,
            "thread_id": execution.child_execution_id,
        }
        if execution.lifecycle == AsyncSubagentLifecycle.COMPLETED:
            link = await self._service.link(self.scope, execution.child_execution_id)
            if link.result_decision in {"admit", "conditionally_admit"}:
                result["status"] = "success"
                result["result"] = execution.result_output_text or ""
            else:
                result["status"] = "completed_pending_admission"
                result["note"] = (
                    "The child completed; its result is provider evidence until the parent "
                    "authority admits it."
                )
        elif execution.lifecycle == AsyncSubagentLifecycle.FAILED:
            result["status"] = "error"
            result["error"] = "The async subagent run failed."
        elif execution.lifecycle == AsyncSubagentLifecycle.IN_DOUBT:
            result["note"] = (
                f"The child is in doubt ({execution.in_doubt_reason}); an operator decision or "
                "a later observation resolves it. Never start a second child for this task."
            )
        return Command(
            update={
                "messages": [ToolMessage(json.dumps(result), tool_call_id=runtime.tool_call_id)],
                "async_tasks": {
                    execution.child_execution_id: self._task(execution, tracked["agent_name"])
                },
            }
        )

    async def _update(self, task_id: str, message: str, runtime: ToolRuntime) -> str | Command:
        tracked = self._tracked(task_id, runtime)
        if isinstance(tracked, str):
            return tracked
        try:
            await self._service.send_message(
                self.scope,
                tracked["task_id"],
                payload_ref=message,
                correlation_id=str(runtime.tool_call_id or tracked["task_id"]),
                created_at=self._now(),
            )
            execution = await self._service.execution(self.scope, tracked["task_id"])
        except AsyncSubagentError as error:
            return f"Failed to update async subagent: {error}"
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        f"Updated async subagent. task_id: {tracked['task_id']}",
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
                "async_tasks": {
                    execution.child_execution_id: self._task(execution, tracked["agent_name"])
                },
            }
        )

    async def _cancel(self, task_id: str, runtime: ToolRuntime) -> str | Command:
        tracked = self._tracked(task_id, runtime)
        if isinstance(tracked, str):
            return tracked
        try:
            execution = await self._service.cancel(
                self.scope, tracked["task_id"], "parent cognition cancelled the task", self._now()
            )
        except AsyncSubagentError as error:
            return f"Failed to cancel run: {error}"
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        f"Cancelled async subagent task: {tracked['task_id']}",
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
                "async_tasks": {
                    execution.child_execution_id: self._task(execution, tracked["agent_name"])
                },
            }
        )

    async def _list(
        self,
        runtime: ToolRuntime,
        status_filter: Literal["running", "success", "error", "cancelled", "all"] | None = None,
    ) -> str | Command:
        tasks = cast(dict[str, AsyncTask], runtime.state.get("async_tasks") or {})
        filtered = [
            task
            for task in tasks.values()
            if not status_filter or status_filter == "all" or task["status"] == status_filter
        ]
        if not filtered:
            return "No async subagent tasks tracked."
        updated: dict[str, AsyncTask] = {}
        entries: list[str] = []
        for task in filtered:
            try:
                execution = await self._service.reconcile(self.scope, task["task_id"])
            except AsyncSubagentError:
                updated[task["task_id"]] = task
                entries.append(_task_entry(task, task["status"]))
                continue
            refreshed = self._task(execution, task["agent_name"])
            updated[task["task_id"]] = refreshed
            entries.append(_task_entry(task, refreshed["status"]))
        return Command(
            update={
                "messages": [
                    ToolMessage(
                        f"{len(entries)} tracked task(s):\n" + "\n".join(entries),
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
                "async_tasks": updated,
            }
        )

    @staticmethod
    def _tracked(task_id: str, runtime: ToolRuntime) -> AsyncTask | str:
        tasks = cast(dict[str, AsyncTask], runtime.state.get("async_tasks") or {})
        tracked = tasks.get(task_id.strip())
        if not tracked:
            return f"No tracked task found for task_id: {task_id!r}"
        return tracked

    def _task(self, execution: AsyncSubagentExecution, agent_name: str) -> AsyncTask:
        now = self._now().strftime("%Y-%m-%dT%H:%M:%SZ")
        status = {
            AsyncSubagentLifecycle.COMPLETED: "success",
            AsyncSubagentLifecycle.FAILED: "error",
            AsyncSubagentLifecycle.CANCELLED: "cancelled",
            AsyncSubagentLifecycle.ORPHANED: "cancelled",
        }.get(execution.lifecycle, execution.lifecycle.value)
        return AsyncTask(
            task_id=execution.child_execution_id,
            agent_name=agent_name,
            thread_id=execution.provider_thread_id or execution.child_execution_id,
            run_id=execution.provider_run_id or "pending",
            status=status,
            created_at=execution.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            last_checked_at=now,
            last_updated_at=execution.updated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )


def _task_entry(task: AsyncTask, status: str) -> str:
    return f"- task_id: {task['task_id']}  agent: {task['agent_name']}  status: {status}"
