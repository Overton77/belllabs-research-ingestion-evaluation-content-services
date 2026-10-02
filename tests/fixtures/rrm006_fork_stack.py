"""RRM-006 demonstration stack: real persistence for semantic forks of both families.

Application PostgreSQL holds run control (with the family writer pool), the operation journal,
checkpoint lineage, snapshots and fork authority; a real `AsyncPostgresSaver` holds cognition
in a dedicated schema; MongoDB holds the operation binding store; result payloads are
content-addressed files. Cognition is a real `create_deep_agent` graph with a deterministic
technical model (one `write_todos` call, then a JSON answer). No company input, no live model.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
from beanie import init_beanie
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.memory import InMemoryStore
from pymongo import AsyncMongoClient

from app.api.run_forks import RunForkServices, compose_run_fork_services
from app.application.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from app.application.operations.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operations.operation_execution import OperationExecutionService
from app.application.operations.operation_journal import OperationJournalService
from app.application.operations.operation_recovery_composition import (
    compose_postgres_operation_recovery,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.runtime.postgres_run_forks import PostgresForkMaterializationStore
from app.application.runtime.run_forks import ForkPatchPolicyRegistry, ForkReuseResolver
from app.domain.operation_execution.contracts import DeepAgentExecutionBinding
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    StateSandboxFactory,
)
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from app.integrations.mongodb import BEANIE_MODELS
from tests.fixtures.checkpoint_recovery import AcceptingAuthority, run_control_authority
from tests.fixtures.rrm004_persistent_stack import FileArtifactPayloadStore
from tests.unit.operations.test_operation_execution import MCP_DIGEST, SKILL_DIGEST
from tests.unit.run_control.test_run_control import ConfigurationVerifier, actor

SAVER_SCHEMA = "rrm006_fork_saver"
CLAIMED_BY = "operation-runtime:rrm-006"
TOKENS_PER_CALL = 5
ANSWER_MARKER = "RRM006-OK"


def _text(messages: Sequence[BaseMessage], kinds: tuple[type, ...]) -> str:
    return "\n".join(str(item.content) for item in messages if isinstance(item, kinds))


def text_digest(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()[:16]


class TechnicalModel(BaseChatModel):
    """Deterministic two-turn cognition for one operation; every call is logged.

    StageGraph units answer with an artifact ref derived from their admitted input (so a
    patched objective yields a distinct artifact). GoalDirected units answer with the typed
    executor/verifier observation; their artifact ref is derived from the run-bound goal
    context.
    """

    run_id: str
    operation_id: str
    # `Any`: pydantic copies a validated list, which would hide the shared call log.
    log: Any

    @property
    def _llm_type(self) -> str:
        return "rrm-006-technical"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        tools = sum(isinstance(item, ToolMessage) for item in messages)
        humans = sum(isinstance(item, HumanMessage) for item in messages)
        self.log.append(
            {
                "run_id": self.run_id,
                "operation": self.operation_id,
                "tools": tools,
                "humans": humans,
            }
        )
        usage = {"input_tokens": 2, "output_tokens": 3, "total_tokens": TOKENS_PER_CALL}
        if tools == 0:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_todos",
                        "args": {"todos": [{"content": "technical", "status": "completed"}]},
                        "id": f"rrm006-todos-{text_digest(self.operation_id)}",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=usage,
            )
            return ChatResult(generations=[ChatGeneration(message=message)])
        if ":stage:" in self.operation_id:
            stage = self.operation_id.split(":stage:", 1)[1].split(":", 1)[0]
            admitted = _text(messages, (HumanMessage,))
            payload: dict[str, Any] = {
                "answer": ANSWER_MARKER,
                "output_refs": [f"artifact:rrm006:{stage}:{text_digest(admitted)}"],
            }
        else:
            role = self.operation_id.rsplit("/", 1)[-1]
            context = text_digest(_text(messages, (BaseMessage,)))
            payload = (
                {
                    "schema_version": "belllabs.goal-executor-observation.v1",
                    "disposition": "completed",
                    "output_refs": [f"artifact:rrm006-goal:{context}"],
                    "completion_claim": True,
                    "accepted_fact_refs": ["fact:rrm006-technical"],
                    "evidence_refs": [f"evidence:rrm006-goal:executor:{context}"],
                    "handoff": None,
                    "output_contract_ref": "fixture-output",
                }
                if role == "executor"
                else {
                    "schema_version": "belllabs.goal-verifier-observation.v1",
                    "decision": "accepted",
                    "progress_made": True,
                    "accepted_obligation_refs": ["fixture-obligation"],
                    "findings": [],
                    "evidence_refs": [f"evidence:rrm006-goal:verifier:{context}"],
                    "unmet_obligations": [],
                    "obligation_applicability": [["fixture-obligation", True]],
                    "output_contract_ref": "fixture-output",
                }
            )
        message = AIMessage(content=json.dumps(payload, sort_keys=True), usage_metadata=usage)
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)


def saver_dsn(dsn: str) -> str:
    return f"{dsn}?options=" + quote(f"-c search_path={SAVER_SCHEMA}")


@dataclass
class ForkStack:
    pool: asyncpg.Pool
    writer_pool: asyncpg.Pool
    repository: PostgresRunControlRepository
    run_control: RunControlService
    saver: AsyncPostgresSaver
    binding: DeepAgentExecutionBinding
    service: OperationExecutionService
    results: FileArtifactPayloadStore
    bindings: MongoOperationBindingRepository
    forks: RunForkServices
    store: PostgresForkMaterializationStore
    model_log: list[dict[str, Any]] = field(default_factory=list)


@asynccontextmanager
async def open_fork_stack(
    dsn: str,
    root: Path,
    *,
    mongo_uri: str,
    mongo_database: str,
    families: FamilyAdmissionRegistry,
    policies: ForkPatchPolicyRegistry,
    required_obligations: frozenset[str] = frozenset(),
    journaled: bool = True,
) -> AsyncIterator[ForkStack]:
    """Compose the stack. `journaled=False` composes the operation boundary without the journal
    and with an accepting authority (the pre-RRM-016 GoalDirected shape, kept for comparison);
    since RRM-016 every family, GoalDirected included, runs journaled."""

    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8)
    writer_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    mongo: AsyncMongoClient[Any] = AsyncMongoClient(
        mongo_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    try:
        await init_beanie(database=mongo[mongo_database], document_models=BEANIE_MODELS)
        repository = PostgresRunControlRepository(pool, family_writer_pool=writer_pool)
        admission = AdmissionPolicyRegistry()
        admission.register("contract:input@1", lambda _request, _configuration: None)
        admission.register("contract:invariant@1", lambda _request, _configuration: None)
        run_control = RunControlService(
            repository, ConfigurationVerifier(required_obligations), admission, families
        )
        async with AsyncPostgresSaver.from_conn_string(saver_dsn(dsn)) as saver:
            await saver.setup()
            binding, _profile, bundle = exact_fixture()
            log: list[dict[str, Any]] = []

            def model_factory(bound: Any, _secrets: Any) -> BaseChatModel:
                return TechnicalModel(run_id=bound.run_id, operation_id=bound.operation_id, log=log)

            adapter = DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(
                    ExactComponentRegistry(
                        model_factories={binding.model.ref.digest: model_factory},
                        skill_bundles={bundle.bundle_digest: bundle},
                        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
                        checkpointers={binding.checkpointer_ref.digest: saver},
                        stores={binding.store_ref.digest: InMemoryStore()},
                    )
                )
            )
            recovery = compose_postgres_operation_recovery(pool, run_control=run_control)
            results = FileArtifactPayloadStore(root / "results")
            bindings = MongoOperationBindingRepository()
            store = PostgresForkMaterializationStore(pool)
            assets = ConformanceAssetVerifier(
                mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
                asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
            )
            service = OperationExecutionService(
                authority=(
                    run_control_authority(run_control) if journaled else AcceptingAuthority()
                ),
                bindings=bindings,
                runtime=adapter,
                sandbox=ConformanceSandbox(),
                assets=assets,
                mcp=assets,
                secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "unused"}),
                events=ConformanceEventSink(),
                budget=ConformanceBudgetAuthority(),
                journal=(
                    JournaledOperationExecutionCoordinator(
                        journal=OperationJournalService(
                            PostgresAtomicOperationJournalRepository(pool)
                        ),
                        run_control=run_control,
                        results=results,
                        actor=actor(),
                    )
                    if journaled
                    else None
                ),
                journal_claimed_by=CLAIMED_BY,
                lineage=recovery.lineage,
                fork_reuse=ForkReuseResolver(store, results=results, bindings=bindings),
            )
            yield ForkStack(
                pool=pool,
                writer_pool=writer_pool,
                repository=repository,
                run_control=run_control,
                saver=saver,
                binding=binding,
                service=service,
                results=results,
                bindings=bindings,
                forks=compose_run_fork_services(pool, run_control, policies=policies),
                store=store,
                model_log=log,
            )
    finally:
        await mongo.close()
        await writer_pool.close()
        await pool.close()


def utc_now() -> datetime:
    return datetime.now(UTC)
