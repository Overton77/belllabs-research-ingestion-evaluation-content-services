"""Deterministic async-subagent cognition and reconciliation qualification helpers."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.checkpoint_recovery import governed_workspace
from tests.unit.run_control.test_run_control import actor, command
from tests.unit.run_control.test_run_control import request as run_request

from mission_control.adapters.agent_server.async_subagents.bindings import (
    technical_child_definition,
)
from mission_control.adapters.deep_agents import (
    DeepAgentsAsyncSubagentAdapter,
)
from mission_control.adapters.deep_agents.async_subagents import BellLabsAsyncSubagentMiddleware
from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationExecutionService,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.subordinates.service import (
    AsyncSubagentService,
    AsyncSubagentSpawnRequest,
    ProviderAsyncObservation,
)
from mission_control.bootstrap.operation_recovery_composition import (
    OperationRecoveryComposition,
)
from mission_control.domain.authoring.contracts import SecretRef
from mission_control.domain.execution.async_subagent_reconciliation import (
    ASYNC_CHILD_RECONCILE_PERMISSION,
)
from mission_control.domain.execution.checkpoint_lineage import OperationActivityAttempt
from mission_control.domain.execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionBinding,
    OperationExecutionRequest,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    CommandStatus,
    ReserveBudgetAction,
    StartAction,
)

SAVER_SCHEMA = "rrm013_live_saver"
CLAIMED_BY = "operation-runtime:rrm-013"
TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
TOKEN_REF = f"environment:{TOKEN_ENV}"
SCOPE = "tenant-1"
CHILD_NAME = "technical-child"
PARENT_SPAWN_TOOL_CALL_ID = "rrm013-start-async-task"


class ParentSpawnModel(BaseChatModel):
    """Deterministic parent cognition: one `start_async_task` call, then a report.

    Every call is appended to a shared JSON log with the process ID so the crash-window proofs
    can assert which process executed which turn.
    """

    objective: str = "Reply with exactly the word PONG."
    subagent_type: str = CHILD_NAME
    log_path: str | None = None

    @property
    def _llm_type(self) -> str:
        return "rrm-013-parent-spawn"

    def bind_tools(
        self, tools: Sequence[Any], *, tool_choice: Any = None, **kwargs: Any
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        tool_messages = [item for item in messages if isinstance(item, ToolMessage)]
        if self.log_path:
            with Path(self.log_path).open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({"pid": os.getpid(), "tool_messages": len(tool_messages)}) + "\n"
                )
        usage = {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5}
        if not tool_messages:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "start_async_task",
                        "args": {
                            "description": self.objective,
                            "subagent_type": self.subagent_type,
                        },
                        "id": PARENT_SPAWN_TOOL_CALL_ID,
                        "type": "tool_call",
                    }
                ],
                usage_metadata=usage,
            )
        else:
            message = AIMessage(
                content=json.dumps({"spawned": str(tool_messages[-1].content)}),
                usage_metadata=usage,
            )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _generate(
        self, messages: list[BaseMessage], stop: Any = None, run_manager: Any = None, **kwargs: Any
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)

    async def _agenerate(
        self, messages: list[BaseMessage], stop: Any = None, run_manager: Any = None, **kwargs: Any
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)


class CrashWindowProvider:
    """The real adapter with a stall injected around provider submission (crash windows).

    `before_submit`: stall after BellLabs reservation, link, claim and fence, before any
    provider call. `after_submit`: let the provider create the run, then stall before the
    observation is applied. The marker file tells the test the window was reached; the test
    then kills the process.
    """

    def __init__(
        self, inner: DeepAgentsAsyncSubagentAdapter, *, window: str | None, marker: Path | None
    ) -> None:
        self._inner = inner
        self._window = window
        self._marker = marker

    async def start(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution, objective: str
    ) -> ProviderAsyncObservation:
        if self._window == "before_submit":
            await self._stall()
        observation = await self._inner.start(contract, execution, objective)
        if self._window == "after_submit":
            await self._stall()
        return observation

    async def _stall(self) -> None:
        assert self._marker is not None
        self._marker.write_text(str(os.getpid()), encoding="utf-8")
        await asyncio.Event().wait()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@dataclass
class LiveStack:
    pool: asyncpg.Pool
    saver: AsyncPostgresSaver
    run_control: RunControlService
    recovery: OperationRecoveryComposition
    service: OperationExecutionService
    async_subagents: AsyncSubagentService
    provider: DeepAgentsAsyncSubagentAdapter
    authority: PostgresAsyncSubagentAuthority
    binding: DeepAgentExecutionBinding
    contract: AsyncSubagentContract
    model: ParentSpawnModel


class _MiddlewareFactory:
    def __init__(self, stack_ref: dict[str, LiveStack]) -> None:
        self._stack_ref = stack_ref

    def middleware(
        self,
        binding: OperationExecutionBinding,
        contracts: tuple[AsyncSubagentContract, ...],
        resolved_secrets: Any,
    ) -> BellLabsAsyncSubagentMiddleware:
        del resolved_secrets
        stack = self._stack_ref["stack"]
        return BellLabsAsyncSubagentMiddleware(
            service=stack.async_subagents,
            adapter=stack.provider,
            binding=binding,
            contracts=contracts,
            dependency_class=AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
        )


def saver_dsn(dsn: str) -> str:
    return f"{dsn}?options=" + quote(f"-c search_path={SAVER_SCHEMA}")


def live_contract(endpoint: str) -> AsyncSubagentContract:
    # The fixture parent run declares `tokens.total` with a hard cap of 100 (run-control test
    # request); the child's reservation is carved from it, so it declares only that dimension.
    return technical_child_definition().contract(
        agent_protocol_url=endpoint.rstrip("/"), budget_limits={"tokens.total": 20}
    )


async def admit_parent_run(stack: LiveStack, request_id: str) -> str:
    """Admit a parent run with a unique request identity.

    BellLabs child identities derive from the parent binding and the tool call, and the Agent
    Server keeps its threads durably, so a repeated run identity would reconnect to a previous
    drill's provider thread (correct, but not what a fresh drill wants).
    """

    from uuid import uuid4

    request_id = f"{request_id}-{uuid4().hex[:8]}"
    admitted = await stack.run_control.admit(run_request(request_id=request_id))
    assert admitted.run_id is not None
    started = await stack.run_control.execute(
        command(admitted.run_id, 1, f"{request_id}-start", StartAction())
    )
    assert started.status == CommandStatus.ACCEPTED
    return admitted.run_id


async def bound_parent_request(
    stack: LiveStack, run_id: str, *, stage: str = "spawn"
) -> OperationExecutionRequest:
    """Reserve the parent operation's budget and bind one Deep Agent unit with the contract."""

    from tests.unit.operations.test_operation_execution import operation_request

    unit = stage_unit(
        request_scope=SCOPE,
        run_id=run_id,
        operation_id=(
            f"execution-epoch:1:stage:{stage}:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default"
        ),
        stage_id=stage,
    )
    run = await stack.run_control.get_run(SCOPE, run_id)
    reservation_id = f"reservation:{unit.unit_key}"
    reserved = await stack.run_control.execute(
        command(
            run_id,
            run.version,
            f"reserve:{unit.unit_key}",
            ReserveBudgetAction(reservation_id=reservation_id, amounts={"tokens.total": 30}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    workspace = governed_workspace(stack.binding.workspace)
    deep_binding = bind_unit(
        stack.binding,
        unit,
        control_revision=reserved.resulting_run_version,
        reservation_id=reservation_id,
        workspace=workspace,
    )
    base = operation_request().model_dump(mode="python")
    return OperationExecutionRequest.model_validate(
        {
            **base,
            "workspace": workspace,
            "identity": OperationAttemptIdentity(
                run_id=run_id,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            "run_control_revision": reserved.resulting_run_version,
            "execution_runtime": "deep_agent",
            "native_placement": None,
            "deep_agent_binding": deep_binding,
            "runtime_unit": unit,
            "budget_reservation_id": reservation_id,
            "budget_limits": {"tokens.total": 30},
            "secret_refs": (
                SecretRef(provider="environment", key="OPENAI_API_KEY"),
                SecretRef(provider="environment", key=TOKEN_ENV),
            ),
            "idempotency_key": f"rrm-013-live:{unit.unit_key}",
        }
    )


def activity_attempt(
    request: OperationExecutionRequest, number: int, *, lease: timedelta | None = None
) -> OperationActivityAttempt:
    return OperationActivityAttempt(
        workflow_id=f"operation/{request.identity.semantic_key}",
        workflow_run_id="rrm013-live-run",
        activity_id="1",
        attempt=number,
        worker_identity=f"rrm013-worker:{os.getpid()}:{number}",
        lease_expires_at=(datetime.now(UTC) + lease) if lease is not None else None,
    )


def spawn_request_for(
    stack: LiveStack,
    binding_id: str,
    *,
    run_id: str,
    objective: str,
    key: str,
    reservation_id: str,
    parent_reservation_id: str,
) -> AsyncSubagentSpawnRequest:
    """A direct spawn request (the service path) for drills that need no parent cognition."""

    from mission_control.domain.authoring.canonical import sha256_digest

    return AsyncSubagentSpawnRequest(
        request_scope=SCOPE,
        parent_run_id=run_id,
        parent_operation_id="rrm013-drill",
        parent_binding_id=binding_id,
        execution_generation=1,
        contract=stack.contract,
        dependency_class=AsyncSubagentDependencyClass.NONBLOCKING,
        objective_ref="ref:async-objective:" + sha256_digest(objective).removeprefix("sha256:"),
        objective=objective,
        context_slice_ref=f"ref:async-context-slice:{binding_id}",
        reservation_id=reservation_id,
        idempotency_key=key,
        requested_at=datetime.now(UTC),
        parent_reservation_id=parent_reservation_id,
    )


def tools_of(definition_tools: Sequence[BaseTool]) -> tuple[str, ...]:
    return tuple(tool.name for tool in definition_tools)


def reconciler() -> ActorContext:
    """A privileged operator for the typed in_doubt decisions (RRM-013 review N5)."""

    return actor().model_copy(
        update={"permissions": actor().permissions | {ASYNC_CHILD_RECONCILE_PERMISSION}}
    )
