"""C1 integration: a local-model Deep Agents unit on the real stack leaves provider frames.

A real Deep Agent (deterministic tool-calling chat model) runs through
`OperationExecutionService`, the PostgreSQL operation journal, run control and checkpoint
lineage, the real `AsyncPostgresSaver`, and the PostgreSQL Native Event Store, all as
restricted logins under forced RLS. The run leaves `session_init`, `turn_started`,
`tool_call_completed`, `usage`, `turn_ended` and `run_result` frames in arrival order;
re-observing the same unit adds zero rows; another tenant reads nothing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.memory import InMemoryStore
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    canonical_scope,
    provision_runtime,
)
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.operations.test_operation_execution import (
    MCP_DIGEST,
    SKILL_DIGEST,
    operation_request,
)
from tests.unit.run_control.test_run_control import actor, command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

from mission_control.adapters.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    StateSandboxFactory,
)
from mission_control.adapters.deep_agents.persistence import runtime_checkpoint_conninfo
from mission_control.adapters.operations.conformance import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from mission_control.adapters.postgres.operations.operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
)
from mission_control.application.execution.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
)
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalService,
)
from mission_control.application.execution.service import RunControlService
from mission_control.domain.execution.checkpoint_lineage import OperationActivityAttempt
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionRequest,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import (
    CommandStatus,
    ReserveBudgetAction,
    StartAction,
)

pytestmark = pytest.mark.common_db

SAVER_SCHEMA = "mission_control_runtime"
SCOPE = canonical_scope("tenant-1")
SECRET = "fixture-openai-key-value-123"


class ToolCallingUsageModel(BaseChatModel):
    """Reads its pinned SKILL.md through `read_file`, then answers; reports token usage."""

    calls: int = 0

    @property
    def _llm_type(self) -> str:
        return "c1-tool-calling-usage"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        self.calls += 1
        if not any(isinstance(item, ToolMessage) for item in messages):
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "read_file",
                        "args": {"file_path": "/skills/exact-binding-proof/SKILL.md", "limit": 50},
                        "id": "c1-read",
                        "type": "tool_call",
                    }
                ],
                usage_metadata={"input_tokens": 3, "output_tokens": 1, "total_tokens": 4},
            )
        else:
            message = AIMessage(
                content="FRAMES-OK",
                usage_metadata={"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
            )
        return ChatResult(generations=[ChatGeneration(message=message)])


class AcceptingAuthority:
    async def verify(self, request: OperationExecutionRequest) -> None:
        del request

    async def verify_continuation(self, request: OperationExecutionRequest, binding: Any) -> None:
        del request, binding

    async def verify_cancellation(self, request: OperationExecutionRequest, binding: Any) -> None:
        del request, binding


@dataclass
class Stack:
    pool: asyncpg.Pool
    run_control: RunControlService
    run_id: str
    service: OperationExecutionService
    frames: PostgresFrameRepository
    model: ToolCallingUsageModel
    binding: DeepAgentExecutionBinding


def scoped_command(run_id: str, version: int, command_id: str, action: Any) -> Any:
    return command(run_id, version, command_id, action).model_copy(update={"request_scope": SCOPE})


@pytest.fixture
async def stack(common_db: CommonDatabase) -> AsyncIterator[Stack]:
    checkpoint_dsn = await provision_runtime(common_db)
    pool = await common_db.pool(max_size=6)
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_control.admit(
            run_request(request_scope=SCOPE, request_id=f"c1-frames-{uuid4()}")
        )
        assert admitted.run_id is not None
        started = await run_control.execute(
            scoped_command(admitted.run_id, 1, "c1-start", StartAction())
        )
        assert started.status == CommandStatus.ACCEPTED
        saver_dsn = runtime_checkpoint_conninfo(checkpoint_dsn, SAVER_SCHEMA)
        async with AsyncPostgresSaver.from_conn_string(saver_dsn) as saver:
            binding, _profile, bundle = exact_fixture()
            model = ToolCallingUsageModel()
            frames = PostgresFrameRepository(pool)
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
                frames=frames,
            )
            coordinator = JournaledOperationExecutionCoordinator(
                journal=OperationJournalService(PostgresAtomicOperationJournalRepository(pool)),
                run_control=run_control,
                results=InMemoryArtifactPayloadStore(),
                actor=actor(),
            )
            assets = ConformanceAssetVerifier(
                mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
                asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
            )
            service = OperationExecutionService(
                authority=AcceptingAuthority(),
                bindings=InMemoryOperationBindingRepository(),
                runtime=adapter,
                sandbox=ConformanceSandbox(),
                assets=assets,
                mcp=assets,
                secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": SECRET}),
                events=ConformanceEventSink(),
                budget=ConformanceBudgetAuthority(),
                journal=coordinator,
                journal_claimed_by="worker:c1",
                lineage=CheckpointLineageService(PostgresCheckpointLineageRepository(pool)),
            )
            yield Stack(
                pool=pool,
                run_control=run_control,
                run_id=admitted.run_id,
                service=service,
                frames=frames,
                model=model,
                binding=binding,
            )
    finally:
        await pool.close()


async def bound_request(stack: Stack, unit: RuntimeUnitIdentity) -> OperationExecutionRequest:
    run = await stack.run_control.get_run(SCOPE, stack.run_id)
    reservation_id = f"reservation:{unit.unit_key}"
    reserved = await stack.run_control.execute(
        scoped_command(
            stack.run_id,
            run.version,
            f"reserve:{unit.unit_key}",
            ReserveBudgetAction(reservation_id=reservation_id, amounts={"tokens.total": 50}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    deep_binding = bind_unit(
        stack.binding,
        unit,
        control_revision=reserved.resulting_run_version,
        reservation_id=reservation_id,
    )
    return OperationExecutionRequest.model_validate(
        {
            **operation_request().model_dump(mode="python"),
            "request_scope": SCOPE,
            "identity": OperationAttemptIdentity(
                run_id=stack.run_id,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            "run_control_revision": reserved.resulting_run_version,
            "execution_runtime": "deep_agent",
            "native_placement": None,
            "deep_agent_binding": deep_binding,
            "runtime_unit": unit,
            "budget_reservation_id": reservation_id,
            "budget_limits": {"tokens.total": 50},
            "idempotency_key": f"c1:{unit.unit_key}",
        }
    )


def delivery(attempt: int, unit: RuntimeUnitIdentity) -> OperationActivityAttempt:
    return OperationActivityAttempt(
        workflow_id=f"operation/{unit.semantic_operation_id}",
        workflow_run_id="temporal-run-1",
        activity_id="1",
        attempt=attempt,
        worker_identity="worker:c1",
    )


@pytest.mark.asyncio
async def test_local_model_unit_leaves_ordered_deduplicated_frames(
    stack: Stack, common_db: CommonDatabase
) -> None:
    unit = stage_unit(
        request_scope=SCOPE,
        run_id=stack.run_id,
        operation_id="execution-epoch:1:stage:collect:mapped:none:workflow-cycle:0:"
        "stage-cycle:0:slot:default",
        stage_id="collect",
    )
    request = await bound_request(stack, unit)
    result = await stack.service.execute(request, delivery(1, unit))
    assert result.status == "completed"
    assert stack.model.calls == 2

    run_uuid = await stack.frames.run_uuid(SCOPE, stack.run_id)
    assert run_uuid is not None
    frames = await stack.frames.frames_for_run(SCOPE, run_uuid)
    kinds = [frame.kind for frame in frames]
    required = [
        FrameKind.SESSION_INIT,
        FrameKind.TURN_STARTED,
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.USAGE,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.USAGE,
        FrameKind.TURN_ENDED,
        FrameKind.RUN_RESULT,
    ]
    cursor = 0
    for kind in required:  # each required kind appears after the previous one
        cursor = kinds.index(kind, cursor) + 1
    assert [frame.arrival_ordinal for frame in frames] == list(range(1, len(frames) + 1))
    tool = next(frame for frame in frames if frame.kind == FrameKind.TOOL_CALL_COMPLETED)
    assert tool.tool_call_ref == "c1-read"
    assert all(SECRET not in frame.body_excerpt for frame in frames)

    # The second delivery of the settled unit returns the settlement; the frames stay put.
    again = await stack.service.execute(request, delivery(2, unit))
    assert again.status == "completed"
    assert len(await stack.frames.frames_for_run(SCOPE, run_uuid)) == len(frames)

    # Another tenant reads nothing (forced RLS).
    assert await stack.frames.frames_for_run(canonical_scope("tenant-2"), run_uuid) == ()
    readonly = await common_db.pool("mission_control_readonly")
    try:
        assert len(await PostgresFrameRepository(readonly).frames_for_run(SCOPE, run_uuid)) == len(
            frames
        )
    finally:
        await readonly.close()
