"""RRM-003 persistent technical integration: real `AsyncPostgresSaver` + application PostgreSQL.

A real Deep Agent (deterministic fake chat model) runs through `OperationExecutionService`,
the PostgreSQL operation journal and run control, and the PostgreSQL checkpoint lineage
repository, on a fresh database with the common `mission_control` component installed. The
repositories run as the restricted `mission_control_runtime` login under a canonical scope;
the saver uses the provisioned `mission_control_runtime` checkpoint schema through the
checkpointer-only login (never `setup()`, never a business identity).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from deepagents import create_deep_agent
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.memory import InMemoryStore
from tests.acceptance.control_plane.test_wp_cp_040 import SessionProbeModel, exact_fixture
from tests.fixtures.checkpoint_lineage import bind_unit, goal_unit, stage_unit
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
    _effect_claim_id,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
    bind_operation_execution_request,
)
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalService,
)
from mission_control.application.execution.service import RunControlService
from mission_control.domain.execution.checkpoint_lineage import (
    STAMP_INVOCATION_ID,
    STAMP_STATE_SCHEMA_DIGEST,
    STAMP_UNIT_KEY,
    OperationActivityAttempt,
)
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionRequest,
)
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import (
    CommandStatus,
    ReserveBudgetAction,
    StartAction,
)

pytestmark = pytest.mark.common_db

SAVER_SCHEMA = "mission_control_runtime"
PROMPT = "Return BINDING-OK"
SCOPE = canonical_scope("tenant-1")


def scoped_command(run_id: str, version: int, command_id: str, action: Any) -> Any:
    return command(run_id, version, command_id, action).model_copy(update={"request_scope": SCOPE})


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
    owner: asyncpg.Pool
    saver: AsyncPostgresSaver
    run_control: RunControlService
    run_id: str
    lineage: PostgresCheckpointLineageRepository
    journal: PostgresAtomicOperationJournalRepository
    coordinator: JournaledOperationExecutionCoordinator
    service: OperationExecutionService
    model: SessionProbeModel
    binding: DeepAgentExecutionBinding
    drifted_binding: DeepAgentExecutionBinding


@pytest.fixture
async def stack(common_db: CommonDatabase) -> AsyncIterator[Stack]:
    assert common_db.scope("tenant-1") == SCOPE
    checkpoint_dsn = await provision_runtime(common_db)
    pool = await common_db.pool(max_size=4)
    owner = await common_db.owner_pool()
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_control.admit(
            run_request(request_scope=SCOPE, request_id=f"rrm-003-{uuid4()}")
        )
        assert admitted.run_id is not None
        started = await run_control.execute(
            scoped_command(admitted.run_id, 1, "rrm-003-start", StartAction())
        )
        assert started.status == CommandStatus.ACCEPTED
        saver_dsn = runtime_checkpoint_conninfo(checkpoint_dsn, SAVER_SCHEMA)
        async with AsyncPostgresSaver.from_conn_string(saver_dsn) as saver:
            # The runtime schema is provisioned by the descriptor; never set up at runtime.
            binding, _profile, bundle = exact_fixture()
            drifted_binding, _drifted_profile, _drifted_bundle = exact_fixture(
                with_child_slices=True
            )
            model = SessionProbeModel(observed_human_counts=[])
            adapter = DeepAgentRuntimeAdapter(
                ExactDeepAgentMaterializer(
                    ExactComponentRegistry(
                        model_factories={binding.model.ref.digest: lambda _b, _s: model},
                        skill_bundles={bundle.bundle_digest: bundle},
                        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
                        checkpointers={binding.checkpointer_ref.digest: saver},
                        stores={binding.store_ref.digest: InMemoryStore()},
                    )
                )
            )
            journal = PostgresAtomicOperationJournalRepository(pool)
            coordinator = JournaledOperationExecutionCoordinator(
                journal=OperationJournalService(journal),
                run_control=run_control,
                results=InMemoryArtifactPayloadStore(),
                actor=actor(),
            )
            lineage = PostgresCheckpointLineageRepository(pool)
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
                secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "unused"}),
                events=ConformanceEventSink(),
                budget=ConformanceBudgetAuthority(),
                journal=coordinator,
                journal_claimed_by="worker:rrm-003",
                lineage=CheckpointLineageService(lineage),
            )
            yield Stack(
                pool=pool,
                owner=owner,
                saver=saver,
                run_control=run_control,
                run_id=admitted.run_id,
                lineage=lineage,
                journal=journal,
                coordinator=coordinator,
                service=service,
                model=model,
                binding=binding,
                drifted_binding=drifted_binding,
            )
    finally:
        await pool.close()
        await owner.close()


async def bound_request(
    stack: Stack,
    unit: RuntimeUnitIdentity,
    *,
    binding: DeepAgentExecutionBinding | None = None,
) -> OperationExecutionRequest:
    """Reserve a budget slice, then bind one Deep Agent unit at the current run version."""

    run = await stack.run_control.get_run(SCOPE, stack.run_id)
    reservation_id = f"reservation:{unit.unit_key}"
    reserved = await stack.run_control.execute(
        scoped_command(
            stack.run_id,
            run.version,
            f"reserve:{unit.unit_key}",
            ReserveBudgetAction(reservation_id=reservation_id, amounts={"tokens.total": 10}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    version = reserved.resulting_run_version
    deep_binding = bind_unit(
        binding or stack.binding,
        unit,
        control_revision=version,
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
            "run_control_revision": version,
            "execution_runtime": "deep_agent",
            "native_placement": None,
            "deep_agent_binding": deep_binding,
            "runtime_unit": unit,
            "budget_reservation_id": reservation_id,
            "budget_limits": {"tokens.total": 10},
            "idempotency_key": f"rrm-003:{unit.unit_key}",
        }
    )


def delivery(attempt: int, unit: RuntimeUnitIdentity) -> OperationActivityAttempt:
    return OperationActivityAttempt(
        workflow_id=f"operation/{unit.semantic_operation_id}",
        workflow_run_id="temporal-run-1",
        activity_id="1",
        attempt=attempt,
        worker_identity="worker:rrm-003",
    )


def root(namespace: str, checkpoint_id: str | None = None) -> RunnableConfig:
    configurable: dict[str, Any] = {"thread_id": namespace, "checkpoint_ns": ""}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


async def root_lineage(saver: AsyncPostgresSaver, namespace: str) -> list[Any]:
    """Root checkpoints from the latest back to the first, following parent links."""

    chain = []
    cursor = await saver.aget_tuple(root(namespace))
    while cursor is not None:
        chain.append(cursor)
        parent = cursor.parent_config
        cursor = (
            await saver.aget_tuple(root(namespace, parent["configurable"]["checkpoint_id"]))
            if parent is not None
            else None
        )
    return chain


@pytest.mark.asyncio
async def test_operation_before_after_checkpoints_and_idempotent_duplicate_delivery(
    stack: Stack,
) -> None:
    """REQ-CP-EXEC-013/014, REQ-CP-DA-016/017 on the real persistent saver and app DB."""

    unit = stage_unit(
        request_scope=SCOPE,
        run_id=stack.run_id,
        operation_id="execution-epoch:1:stage:draft:mapped:none:workflow-cycle:0:"
        "stage-cycle:0:slot:default",
        stage_id="draft",
    )
    request = await bound_request(stack, unit)
    namespace = f"belllabs/stage/{unit.unit_key}/gen/1"

    # Before: no root checkpoint and no namespace head.
    assert await stack.saver.aget_tuple(root(namespace)) is None
    assert await stack.lineage.get_namespace_head(SCOPE, namespace) is None

    result = await stack.service.execute(request, delivery(2, unit))

    # After: the captured result checkpoint is the namespace head, linked to the manifest.
    transition = await stack.lineage.get_transition(SCOPE, unit.unit_key, 1)
    assert transition is not None
    head = await stack.lineage.get_namespace_head(SCOPE, namespace)
    latest = await stack.saver.aget_tuple(root(namespace))
    assert latest is not None
    assert head == transition.result_key
    assert transition.source_key is None
    assert transition.namespace == namespace
    assert transition.result_key.checkpoint_id == latest.config["configurable"]["checkpoint_id"]
    assert result.status == "completed"
    assert result.unit_key == unit.unit_key
    assert result.result_checkpoint == transition.result_key
    assert result.checkpoint_transition_id == transition.transition_id

    chain = await root_lineage(stack.saver, namespace)
    assert [item.metadata[STAMP_INVOCATION_ID] for item in chain] == (
        [transition.invocation_id] * len(chain)
    )
    assert {item.metadata[STAMP_UNIT_KEY] for item in chain} == {unit.unit_key}
    assert {item.metadata[STAMP_STATE_SCHEMA_DIGEST] for item in chain} == {
        stack.binding.cognitive_state_schema.schema_digest
    }

    binding = bind_operation_execution_request(request)
    journal_settlement = await stack.journal.get_settlement(SCOPE, _effect_claim_id(binding))
    manifest = await stack.coordinator.get_settlement(binding)
    claim = await stack.journal.get_claim(SCOPE, _effect_claim_id(binding))
    assert journal_settlement is not None and manifest is not None and claim is not None
    assert journal_settlement.result_manifest_ref == transition.result_manifest_ref
    assert journal_settlement.result_manifest_digest == transition.result_manifest_digest
    assert manifest.result_checkpoint == transition.result_key
    assert manifest.checkpoint_transition_id == transition.transition_id
    assert claim.unit_key == unit.unit_key

    async with stack.owner.acquire() as connection:
        technical_attempts = await connection.fetch(
            "SELECT technical_attempt FROM mission_control.operation_technical_attempt"
            " WHERE claim_key = $1",
            _effect_claim_id(binding),
        )
        lineage_rows = await connection.fetchval(
            "SELECT string_agg(transition_payload::text, '') FROM"
            " mission_control.checkpoint_transition"
        ) + await connection.fetchval(
            "SELECT string_agg(observation_payload::text, '') FROM"
            " mission_control.activity_attempt_observation"
        )
    assert [row["technical_attempt"] for row in technical_attempts] == [2]
    assert PROMPT not in lineage_rows and "unused" not in lineage_rows

    # Duplicate delivery: the settled unit returns unchanged with no provider work.
    calls_before = list(stack.model.observed_human_counts)
    duplicate = await stack.service.execute(request, delivery(3, unit))
    # RRM-004: the manifest commits to the digest-bound output payload, so the settled
    # replay restores `output_text` and `structured_output` and returns unchanged.
    assert duplicate == result
    assert duplicate.output_text == "human-count:1"
    assert stack.model.observed_human_counts == calls_before == [1]
    assert len(await root_lineage(stack.saver, namespace)) == len(chain)
    assert await stack.lineage.list_transitions(SCOPE, namespace) == (transition,)
    attempts = await stack.lineage.list_attempts(SCOPE, unit.unit_key)
    assert [(item.attempt.attempt, item.dispatching) for item in attempts] == [(2, True)]
    assert attempts[0].expected_source is None
    print(
        "RRM-003 EVIDENCE before/after:",
        json.dumps(
            {
                "namespace": namespace,
                "before": {"root_checkpoint": None, "namespace_head": None},
                "after": transition.result_key.model_dump(mode="json"),
                "stamped_root_checkpoints": len(chain),
                "transition_id": transition.transition_id,
                "result_manifest_digest": transition.result_manifest_digest,
                "technical_attempt": 2,
                "duplicate_delivery_model_calls_added": 0,
            },
            sort_keys=True,
        ),
    )


@pytest.mark.asyncio
async def test_goal_session_lineage_is_linear_rollover_is_empty_and_schema_gated(
    stack: Stack,
) -> None:
    """REQ-BP-GD-012, REQ-CP-DA-016, REQ-CP-CS-007 on the persistent saver."""

    def executor(iteration: int, session_generation: int = 1) -> RuntimeUnitIdentity:
        return goal_unit(
            request_scope=SCOPE,
            run_id=stack.run_id,
            operation_id=f"goal-iteration/{iteration}/executor",
            goal_iteration=iteration,
            session_generation=session_generation,
        )

    first, second = executor(1), executor(2)
    verifier = goal_unit(
        request_scope=SCOPE,
        run_id=stack.run_id,
        operation_id="goal-iteration/1/verifier",
        goal_iteration=1,
        role="verifier",
    )
    rollover = executor(3, session_generation=2)
    for unit in (first, second, verifier, rollover):
        request = await bound_request(stack, unit)
        assert (await stack.service.execute(request, delivery(1, unit))).status == "completed"

    session = f"belllabs/goal/{stack.run_id}/epoch/1/session/1/role/executor"
    one = await stack.lineage.get_transition(SCOPE, first.unit_key, 1)
    two = await stack.lineage.get_transition(SCOPE, second.unit_key, 1)
    rolled = await stack.lineage.get_transition(SCOPE, rollover.unit_key, 1)
    verified = await stack.lineage.get_transition(SCOPE, verifier.unit_key, 1)
    assert one is not None and two is not None and rolled is not None and verified is not None
    assert one.namespace == two.namespace == session
    assert two.source_key == one.result_key
    assert await stack.lineage.list_transitions(SCOPE, session) == (one, two)
    assert rolled.namespace.endswith("/session/2/role/executor")
    assert rolled.source_key is None
    assert verified.namespace.endswith("/session/1/role/verifier")
    assert stack.model.observed_human_counts == [1, 2, 1, 1]

    # Linear stamped lineage: the shared thread attributes every checkpoint to its writer.
    owners = [
        item.metadata[STAMP_UNIT_KEY] for item in reversed(await root_lineage(stack.saver, session))
    ]
    boundary = owners.index(second.unit_key)
    assert set(owners[:boundary]) == {first.unit_key}
    assert set(owners[boundary:]) == {second.unit_key}

    # A unit bound to a different cognitive state schema cannot continue the session.
    drifted = executor(4)
    drifted_request = await bound_request(stack, drifted, binding=stack.drifted_binding)
    # RRM-004 (REQ-CP-DA-018): a digest mismatch is `in_doubt` with a typed incident, not an
    # error; the model is never called and the session head does not move.
    parked = await stack.service.execute(drifted_request, delivery(1, drifted))
    assert parked.status == "in_doubt" and parked.failure_code == "schema_mismatch"
    incident = await stack.lineage.get_incident(SCOPE, drifted.unit_key, 1)
    assert incident is not None and incident.reason == "schema_mismatch"
    assert parked.reconciliation_incident_id == incident.incident_id
    assert stack.model.observed_human_counts == [1, 2, 1, 1]
    assert await stack.lineage.get_namespace_head(SCOPE, session) == two.result_key


@pytest.mark.asyncio
async def test_root_checkpoint_namespace_decision_reverified_on_postgres_saver(
    stack: Stack,
) -> None:
    """RRM-001 section 8 #1: a custom root `checkpoint_ns` breaks state reads, so BellLabs
    isolates by thread and always runs the root graph at `checkpoint_ns=""`."""

    agent = create_deep_agent(
        model=SessionProbeModel(observed_human_counts=[]), checkpointer=stack.saver
    )
    custom: RunnableConfig = {
        "configurable": {"thread_id": f"rrm-003-ns-probe-{uuid4()}", "checkpoint_ns": "belllabs"}
    }
    await agent.ainvoke(
        {"messages": [{"role": "user", "content": "probe"}]}, config=custom, durability="sync"
    )
    with pytest.raises(ValueError, match="Subgraph belllabs not found"):
        await agent.aget_state(custom)
    written = {
        item.config["configurable"]["checkpoint_ns"]
        async for item in stack.saver.alist(
            {"configurable": {"thread_id": custom["configurable"]["thread_id"]}}
        )
    }
    assert written == {""}
