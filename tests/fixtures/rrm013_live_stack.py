"""RRM-013 live stack: a parent operation whose Deep Agent spawns a real async child.

The parent runs in-process (or in the crash-window worker process) over the disposable
application PostgreSQL (run control, operation journal, checkpoint lineage), a real
`AsyncPostgresSaver`, the production Mongo detail repositories, and the PostgreSQL async child
authority. Its cognition is a real `create_deep_agent` graph with a deterministic scripted
parent model whose only tool call is `start_async_task`; the child is the hosted technical
child on the real Agent Server named by `AGENT_SERVER_ENDPOINT`, which calls the real model.

Environment (names only; never commit values): `AGENT_SERVER_ENDPOINT`,
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI`.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
from beanie import init_beanie
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.memory import InMemoryStore
from pymongo import AsyncMongoClient

from app.agent_server.async_subagents.bindings import technical_child_definition
from app.application.async_subagents.mongo_async_subagent_repository import (
    MongoAsyncSubagentDetailRepository,
)
from app.application.async_subagents.parent_effects import RunControlAsyncChildEffects
from app.application.async_subagents.postgres_async_subagents import PostgresAsyncSubagentAuthority
from app.application.async_subagents.service import (
    AsyncSubagentService,
    AsyncSubagentSpawnRequest,
    ProviderAsyncObservation,
)
from app.application.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from app.application.operations.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operations.operation_execution import OperationExecutionService
from app.application.operations.operation_journal import OperationJournalService
from app.application.operations.operation_recovery_composition import (
    OperationRecoveryComposition,
    compose_postgres_operation_recovery,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.service import RunControlService
from app.domain.control_plane.contracts import SecretRef
from app.domain.operation_execution.checkpoint_lineage import OperationActivityAttempt
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionBinding,
    OperationExecutionRequest,
)
from app.domain.run_control.contracts import CommandStatus, ReserveBudgetAction, StartAction
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    DeepAgentsAsyncSubagentAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    StateSandboxFactory,
)
from app.integrations.agents.deep_agents.async_subagents import BellLabsAsyncSubagentMiddleware
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from app.integrations.mongodb import BEANIE_MODELS
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.checkpoint_recovery import governed_workspace, run_control_authority
from tests.fixtures.rrm004_persistent_stack import FileArtifactPayloadStore
from tests.unit.operations.test_operation_execution import MCP_DIGEST, SKILL_DIGEST
from tests.unit.run_control.test_run_control import actor, command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

SAVER_SCHEMA = "rrm013_live_saver"
CLAIMED_BY = "operation-runtime:rrm-013"
TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
TOKEN_REF = f"environment:{TOKEN_ENV}"
SCOPE = "tenant-1"
CHILD_NAME = "technical-child"
PARENT_SPAWN_TOOL_CALL_ID = "rrm013-start-async-task"


def live_opt_in() -> tuple[bool, str]:
    """Whether the live RRM-013 gate is opted in, and why not otherwise."""

    if os.getenv("BELLABS_RUN_RRM_013_LIVE") != "1":
        return False, "BELLABS_RUN_RRM_013_LIVE=1 is required for the live Agent Server gate"
    for name in (
        "AGENT_SERVER_ENDPOINT",
        TOKEN_ENV,
        "TEST_APPLICATION_POSTGRES_DSN",
        "TEST_MONGODB_URI",
    ):
        if not os.getenv(name, "").strip():
            return False, f"{name} is required for the live Agent Server gate"
    return True, ""


class ParentSpawnModel(BaseChatModel):
    """Deterministic parent cognition: one `start_async_task` call, then a report.

    Every call is appended to a shared JSON log with the process ID so the crash-window proofs
    can assert which process executed which turn.
    """

    objective: str = "Reply with exactly the word PONG."
    log_path: str | None = None
    _calls: list[int] = []

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
                        "args": {"description": self.objective, "subagent_type": CHILD_NAME},
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
    ) -> ChatResult:  # noqa: E501
        del stop, run_manager, kwargs
        return self._reply(messages)

    async def _agenerate(
        self, messages: list[BaseMessage], stop: Any = None, run_manager: Any = None, **kwargs: Any
    ) -> ChatResult:  # noqa: E501
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
    return technical_child_definition().contract(agent_protocol_url=endpoint.rstrip("/"))


@asynccontextmanager
async def open_live_stack(
    dsn: str,
    *,
    mongo_uri: str,
    mongo_database: str,
    endpoint: str,
    objective: str = "Reply with exactly the word PONG.",
    crash_window: str | None = None,
    crash_marker: Path | None = None,
    model_log: Path | None = None,
    submitter_identity: str | None = None,
    submission_lease: timedelta = timedelta(seconds=20),
) -> AsyncIterator[LiveStack]:
    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

    token = os.environ[TOKEN_ENV]
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8)
    mongo: AsyncMongoClient[Any] = AsyncMongoClient(
        mongo_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    try:
        await init_beanie(database=mongo[mongo_database], document_models=BEANIE_MODELS)
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        async with AsyncPostgresSaver.from_conn_string(saver_dsn(dsn)) as saver:
            contract = live_contract(endpoint)
            base, _profile, bundle = exact_fixture()
            binding = DeepAgentExecutionBinding.create(
                **base.model_dump(mode="python", exclude={"binding_digest"}),
                async_subagents=(contract,),
            )
            model = ParentSpawnModel(
                objective=objective, log_path=str(model_log) if model_log else None
            )
            stack_ref: dict[str, LiveStack] = {}
            adapter = DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(
                    ExactComponentRegistry(
                        model_factories={binding.model.ref.digest: lambda _b, _s: model},
                        skill_bundles={bundle.bundle_digest: bundle},
                        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
                        checkpointers={binding.checkpointer_ref.digest: saver},
                        stores={binding.store_ref.digest: InMemoryStore()},
                    )
                ),
                async_subagents=_MiddlewareFactory(stack_ref),
            )
            provider = DeepAgentsAsyncSubagentAdapter(
                secrets={TOKEN_REF: token}, request_scope=SCOPE
            )
            provider_port: Any = CrashWindowProvider(
                provider, window=crash_window, marker=crash_marker
            )
            authority = PostgresAsyncSubagentAuthority(pool)
            async_service = AsyncSubagentService(
                MongoAsyncSubagentDetailRepository(),
                authority,
                provider_port,
                parent_effects=RunControlAsyncChildEffects(run_control, actor=actor()),
                allow_new_spawns=True,
                submitter_identity=submitter_identity or f"rrm013-submitter:{os.getpid()}",
                submission_lease=submission_lease,
            )
            recovery = compose_postgres_operation_recovery(pool, run_control=run_control)
            assets = ConformanceAssetVerifier(
                mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
                asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
            )
            service = OperationExecutionService(
                authority=run_control_authority(run_control),
                bindings=MongoOperationBindingRepository(),
                runtime=adapter,
                sandbox=ConformanceSandbox(),
                assets=assets,
                mcp=assets,
                secrets=ConformanceSecretResolver(
                    {"environment:OPENAI_API_KEY": "unused-by-the-parent", TOKEN_REF: token}
                ),
                events=ConformanceEventSink(),
                budget=ConformanceBudgetAuthority(),
                journal=JournaledOperationExecutionCoordinator(
                    journal=OperationJournalService(PostgresAtomicOperationJournalRepository(pool)),
                    run_control=run_control,
                    results=FileArtifactPayloadStore(Path(os.environ["RRM013_RESULTS"])),
                    actor=actor(),
                ),
                journal_claimed_by=CLAIMED_BY,
                lineage=recovery.lineage,
            )
            stack = LiveStack(
                pool=pool,
                saver=saver,
                run_control=run_control,
                recovery=recovery,
                service=service,
                async_subagents=async_service,
                provider=provider,
                authority=authority,
                binding=binding,
                contract=contract,
                model=model,
            )
            stack_ref["stack"] = stack
            yield stack
    finally:
        await mongo.close()
        await pool.close()


async def admit_parent_run(stack: LiveStack, request_id: str) -> str:
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

    from app.domain.control_plane.canonical import sha256_digest

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
