"""RRM-008 demonstration: accepted terminal cancellation on real Temporal (persistent stores).

A real Temporal dev server (`WorkflowEnvironment.start_local`) runs the stable root (started
through `TemporalWorkflowSubmitter`), the family and its `OperationWorkflow` children. Run
control, the boundary-command ledger, the operation journal and checkpoint lineage are
application PostgreSQL; the Deep Agent checkpointer is a real `AsyncPostgresSaver`; OEB
bindings and GoalDirected templates are MongoDB. Every cancel enters through the public
facade (`POST /run-control/v1/runs/{run_id}/commands`), is journaled first, delivered
root-first in the `cancel` space and recorded `applied` by the reducer's `cancelled` outcome.

1. StageGraph (RRM-007 governed harness, fixture operations): the cancel interrupts the
   running sibling's Activity; the family cancels the unadmitted dependency and
   terminalizes `cancelled`.
2. GoalDirected (governed composition, real cognition): the cancel interrupts the
   executor's model call through the Activity heartbeat; the unit settles `cancelled` once
   with its latest durable checkpoint (PostgreSQL journal and lineage); the family consumes
   it, releases the baseline and terminalizes `cancelled` with no liability left.
3. GoalDirected with a real async child on the RRM-013 Agent Server (opt-in
   `BELLABS_RUN_RRM_008_LIVE=1`): the executor's Deep Agent spawned a real child that is
   running when the cancel lands; the child is cancelled on the server (acknowledgement
   recorded), its usage stays pending and keeps the run `cancelling` until a privileged
   usage reconciliation settles it; the family then terminalizes `cancelled`.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` (disposable stack only).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import asyncpg
import pytest
from beanie import init_beanie
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.agent_server.async_subagents.bindings import technical_child_definition
from app.application.async_subagents.mongo_async_subagent_repository import (
    MongoAsyncSubagentDetailRepository,
)
from app.application.async_subagents.parent_effects import (
    RunControlAsyncChildEffects,
    async_child_effect_id,
    async_child_usage_id,
)
from app.application.async_subagents.postgres_async_subagents import PostgresAsyncSubagentAuthority
from app.application.async_subagents.service import AsyncSubagentService
from app.application.operations.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operations.operation_recovery_composition import (
    compose_postgres_operation_recovery,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.application.run_control.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from app.application.run_control.postgres_run_control_repository import (
    PostgresRunControlRepository,
)
from app.domain.control_plane.contracts import SecretRef
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.operation_execution.contracts import (
    AsyncSubagentDependencyClass,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
    DeepAgentExecutionBinding,
)
from app.domain.run_control.contracts import CancelAction, EffectDisposition, RunOutcome
from app.integrations.agents.deep_agents import DeepAgentsAsyncSubagentAdapter
from app.integrations.agents.deep_agents.async_subagents import (
    PROVIDER_USAGE_STATE_KEY,
    BellLabsAsyncSubagentMiddleware,
    attribute_usage,
)
from app.integrations.mongodb import BEANIE_MODELS
from app.integrations.temporal_boundary_commands import TemporalBoundaryCommandTransport
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow
from tests.acceptance.control_plane.test_rrm_007_interventions import Facade, _receipt_rows
from tests.acceptance.control_plane.test_rrm_016_goal_directed_demo import (
    _journal_rows,
    _run_control,
    _transitions,
)
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    SEMANTIC_INPUT,
    GoalComposition,
    GoalScriptedModel,
    RecordingGoalDocuments,
    admit_goal_run,
    compose_goal_directed,
    executor_payload,
    goal_blueprint,
    goal_run_input,
    goal_templates,
)
from tests.fixtures.mongo_database import disposable_mongo_database
from tests.fixtures.rrm004_persistent_stack import FileArtifactPayloadStore
from tests.fixtures.rrm013_live_stack import TOKEN_ENV, TOKEN_REF, reconciler
from tests.integration.agent_server.test_rrm_013_async_subagent_live import (
    await_provider_status,
    provider_runs,
    sdk_client,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    Authority,
    _state_is,
    replay,
    until,
)
from tests.integration.temporal.test_rrm_008_family_cancellation import (
    CancellableStageGraphActivities,
)
from tests.integration.temporal.test_wp_bp_010_temporal import QUEUE, _blueprint
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_input
from tests.unit.run_control.test_run_control import actor
from tests.unit.run_control.test_run_control import service as run_control_service

GOAL_QUEUE = "rrm008-goal-directed"
SAVER_SCHEMA = "rrm008_cancel_saver"
WORKFLOWS = [BellLabsRunWorkflow, StageGraphWorkflow, GoalDirectedWorkflow, OperationWorkflow]
BASELINE = {"tokens.total": 20}
CLAIMED_BY = "operation-runtime:rrm-008"
CHILD_BUDGET = {"tokens.total": 5}
LIVE_FLAG = "BELLABS_RUN_RRM_008_LIVE"


def _evidence(label: str, payload: dict[str, Any]) -> None:
    print(f"RRM-008 EVIDENCE {label}: {json.dumps(payload, sort_keys=True, default=str)}")


mongo_database = disposable_mongo_database("rrm008_cancel")


async def _reset(dsn: str) -> None:
    require_disposable_postgres(dsn)
    owner = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {SAVER_SCHEMA} CASCADE")
            await connection.execute(f"CREATE SCHEMA {SAVER_SCHEMA}")
    finally:
        await owner.close()
    async with AsyncPostgresSaver.from_conn_string(_schema_dsn(dsn)) as saver:
        await saver.setup()


def _schema_dsn(dsn: str) -> str:
    return f"{dsn}?options=" + quote(f"-c search_path={SAVER_SCHEMA}")


def _submitter(client: Client) -> TemporalWorkflowSubmitter:
    return TemporalWorkflowSubmitter(
        client, stagegraph_task_queue=QUEUE, goal_directed_task_queue=GOAL_QUEUE
    )


def _facade_authority(env: WorkflowEnvironment, run_control: Any) -> Authority:
    authority = Authority(env.client, run_control)
    authority.facade = BoundaryInterventionService(
        run_control,
        BoundaryCommandDeliveryService(run_control, TemporalBoundaryCommandTransport(env.client)),
    )
    return authority


# --- 1. StageGraph ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stagegraph_cancellation_on_real_temporal_with_postgres_authority(
    test_application_postgres_dsn: str,
) -> None:
    dsn = test_application_postgres_dsn
    await _reset(dsn)
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8)
    try:
        run_control, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        async with await WorkflowEnvironment.start_local(dev_server_log_level="error") as env:
            authority = _facade_authority(env, run_control)
            activities = CancellableStageGraphActivities(authority)
            run_id = await authority.admit("rrm-008-demo-stagegraph")
            run_input = replace(stage_input(_blueprint()), run_id=run_id)
            family_id = f"family/{run_id}/1"
            async with Facade(authority) as facade:
                async with Worker(
                    env.client,
                    task_queue=QUEUE,
                    workflows=WORKFLOWS,
                    workflow_runner=coordinator_workflow_runner(),
                    activities=activities.functions,
                ):
                    submitted = await _submitter(env.client).submit(
                        run_input,
                        workflow_id="ignored",
                        blueprint_family=BlueprintFamily.STAGE_GRAPH,
                    )
                    root = env.client.get_workflow_handle(submitted.workflow_id)
                    family = env.client.get_workflow_handle(family_id)
                    await asyncio.wait_for(activities.slow_started.wait(), timeout=90)
                    accepted = await facade.command(run_id, "cancel", CancelAction())
                    assert accepted["phase"] == "cancelling"
                    await until(
                        lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60
                    )
                    root_result = await asyncio.wait_for(root.result(), timeout=180)
                    root_runs = await replay(root, [BellLabsRunWorkflow])
                    family_runs = await replay(family, [StageGraphWorkflow, OperationWorkflow])
                projection = await facade.projection(run_id)
                ledger = await facade.ledger(run_id)
        rows = await _receipt_rows(pool, run_id)
        assert [(row[0], row[2], row[3]) for row in rows] == [
            ("cancel", "accepted", "run_control"),
            ("cancel", "delivered", "boundary-delivery"),
            ("cancel", "applied", "run_control"),
        ]
        assert projection["phase"] == "terminal" and projection["terminal_outcome"] == "cancelled"
        assert ledger[0]["command"]["target"]["sequence_space"] == "cancel"
        assert root_result["completion_proposal"]["cancelled"] is True
        assert activities.slow_cancelled.is_set() and not activities.slow_completed.is_set()
        assert "downstream" not in activities.admission_order
        assert (root_runs, family_runs) == (1, 1)
        _evidence(
            "stagegraph",
            {
                "run_id": run_id,
                "root_workflow_id": submitted.workflow_id,
                "family_workflow_id": family_id,
                "receipts": [(row[0], row[1], row[2], row[3]) for row in rows],
                "admission_order": activities.admission_order,
                "cancelled_units": activities.cancel_settlements,
                "replayed": {"root": root_runs, "family": family_runs},
                "terminal_outcome": projection["terminal_outcome"],
            },
        )
    finally:
        await pool.close()


# --- 2 and 3. GoalDirected -------------------------------------------------------------------


class SpawningGoalModel(GoalScriptedModel):
    """The executor's cognition: one `start_async_task` call (a real child), then the
    observation. Call 2 is gated so the cancel lands while the child runs."""

    objective: str = "Call wait_seconds with seconds=120, then reply with exactly PONG."

    def _reply(self, tools: int) -> ChatResult:  # type: ignore[override]
        usage = {"input_tokens": 2, "output_tokens": 3, "total_tokens": self.tokens_per_call}
        turn = self.turns[-1]
        if tools == 0 and turn["role"] == "executor":
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "start_async_task",
                        "args": {"description": self.objective, "subagent_type": "technical-child"},
                        "id": f"rrm008-spawn-{turn['iteration']}",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=usage,
            )
            return ChatResult(generations=[ChatGeneration(message=message)])
        if tools and turn["role"] == "executor":
            payload = executor_payload(turn["iteration"], self.accept_at)
            return ChatResult(
                generations=[
                    ChatGeneration(
                        message=AIMessage(content=json.dumps(payload), usage_metadata=usage)
                    )
                ]
            )
        return super()._reply(tools)


class _MiddlewareFactory:
    def __init__(self) -> None:
        self.service: AsyncSubagentService | None = None
        self.adapter: DeepAgentsAsyncSubagentAdapter | None = None

    def middleware(self, binding: Any, contracts: Any, resolved_secrets: Any) -> Any:
        del resolved_secrets
        assert self.service is not None and self.adapter is not None
        return BellLabsAsyncSubagentMiddleware(
            service=self.service,
            adapter=self.adapter,
            binding=binding,
            contracts=contracts,
            dependency_class=AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
        )


@asynccontextmanager
async def _goal_worker(
    env: WorkflowEnvironment,
    pool: asyncpg.Pool,
    family_pool: asyncpg.Pool,
    dsn: str,
    results: Path,
    identity: str,
    documents: RecordingGoalDocuments,
    model: GoalScriptedModel,
    *,
    live: bool = False,
) -> AsyncIterator[tuple[GoalComposition, AsyncSubagentService | None]]:
    run_control = _run_control(pool, family_pool)
    recovery = compose_postgres_operation_recovery(pool, run_control=run_control)
    templates = MongoGoalDirectedDocumentRepository()
    extras: dict[str, Any] = {}
    async_service: AsyncSubagentService | None = None
    if live:
        from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

        base, _profile, _bundle = exact_fixture()
        contract = technical_child_definition().contract(
            agent_protocol_url=os.environ["AGENT_SERVER_ENDPOINT"].rstrip("/"),
            budget_limits=CHILD_BUDGET,
        )
        binding = DeepAgentExecutionBinding.create(
            **{
                **base.model_dump(mode="python", exclude={"binding_digest", "async_subagents"}),
                "async_subagents": (contract,),
            }
        )
        factory = _MiddlewareFactory()
        factory.adapter = DeepAgentsAsyncSubagentAdapter(
            secrets={TOKEN_REF: os.environ[TOKEN_ENV]}, request_scope=SCOPE
        )
        async_service = AsyncSubagentService(
            MongoAsyncSubagentDetailRepository(),
            PostgresAsyncSubagentAuthority(pool),
            factory.adapter,
            parent_effects=RunControlAsyncChildEffects(run_control, actor=actor()),
            allow_new_spawns=True,
            submitter_identity=f"rrm008-submitter:{identity}",
        )
        factory.service = async_service
        extras = {
            "binding": binding,
            "async_subagents": factory,
            "children": async_service,
            "secrets": {TOKEN_REF: os.environ[TOKEN_ENV]},
            "template_secret_refs": (SecretRef(provider="environment", key=TOKEN_ENV),),
        }
    async with AsyncPostgresSaver.from_conn_string(_schema_dsn(dsn)) as saver:
        composition = await compose_goal_directed(
            run_control=run_control,
            journal=PostgresAtomicOperationJournalRepository(pool),
            lineage=recovery.lineage,
            results=FileArtifactPayloadStore(results),
            bindings=MongoOperationBindingRepository(),
            saver=saver,
            model=model,
            blueprint=goal_blueprint(),
            claimed_by=CLAIMED_BY,
            template_provider=templates,
            documents=documents,
            **extras,
        )
        async with (
            Worker(
                env.client,
                task_queue=GOAL_QUEUE,
                workflows=WORKFLOWS,
                workflow_runner=coordinator_workflow_runner(),
                activities=coordinator_activities("GoalDirected", composition.family),
                identity=f"{identity}:family",
            ),
            Worker(
                env.client,
                task_queue=composition.binding.task_queue,
                activities=agent_cognitive_activities(
                    OperationExecutionActivities(
                        composition.service, worker_identity=f"{identity}:cognitive"
                    )
                ),
                identity=f"{identity}:cognitive",
            ),
        ):
            yield composition, async_service


async def _persist_templates(
    binding: DeepAgentExecutionBinding, secret_refs: tuple[Any, ...]
) -> None:
    templates = goal_templates(binding, secret_refs=secret_refs)
    await MongoGoalDirectedDocumentRepository().persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=SEMANTIC_INPUT,
        executor=templates["executor"],
        verifier=templates["verifier"],
        recorded_at=templates["executor"].requested_at,
    )


async def _goal_cancellation(
    dsn: str,
    mongo_uri: str,
    mongo_database: str,
    tmp_path: Path,
    *,
    live: bool,
) -> dict[str, Any]:
    await _reset(dsn)
    mongo: AsyncMongoClient[Any] = AsyncMongoClient(
        mongo_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10)
    family_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    documents = RecordingGoalDocuments()
    evidence: dict[str, Any] = {}
    try:
        await init_beanie(database=mongo[mongo_database], document_models=BEANIE_MODELS)
        run_control = _run_control(pool, family_pool)
        from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

        base, _profile, _bundle = exact_fixture()
        secret_refs: tuple[Any, ...] = ()
        template_binding = base
        if live:
            contract = technical_child_definition().contract(
                agent_protocol_url=os.environ["AGENT_SERVER_ENDPOINT"].rstrip("/"),
                budget_limits=CHILD_BUDGET,
            )
            template_binding = DeepAgentExecutionBinding.create(
                **{
                    **base.model_dump(mode="python", exclude={"binding_digest", "async_subagents"}),
                    "async_subagents": (contract,),
                }
            )
            secret_refs = (SecretRef(provider="environment", key=TOKEN_ENV),)
        await _persist_templates(template_binding, secret_refs)
        model: GoalScriptedModel = SpawningGoalModel() if live else GoalScriptedModel()
        async with await WorkflowEnvironment.start_local(dev_server_log_level="error") as env:
            authority = _facade_authority(env, run_control)
            run_id = await admit_goal_run(
                run_control, f"rrm-008-demo-goal{'-live' if live else ''}-{uuid4().hex[:6]}"
            )
            family_id = f"family/{run_id}/1"
            run_input = replace(
                goal_run_input(run_id, goal_blueprint(), baseline=BASELINE),
                cancellation_retry_seconds=5,
            )
            async with Facade(authority) as facade:
                async with _goal_worker(
                    env,
                    pool,
                    family_pool,
                    dsn,
                    tmp_path / "results",
                    "rrm008-worker",
                    documents,
                    model,
                    live=live,
                ) as (composition, async_service):
                    entered, _gate = model.gate_on(2 if live else 1)
                    submitted = await _submitter(env.client).submit(
                        run_input,
                        workflow_id="ignored",
                        blueprint_family=BlueprintFamily.GOAL_DIRECTED,
                    )
                    root = env.client.get_workflow_handle(submitted.workflow_id)
                    family = env.client.get_workflow_handle(family_id)
                    await asyncio.wait_for(entered.wait(), timeout=240)
                    child: Any = None
                    if live:
                        assert async_service is not None
                        [view] = await PostgresAsyncSubagentAuthority(pool).list_children(
                            SCOPE, run_id
                        )
                        child = await async_service.execution(SCOPE, view.child_execution_id)
                        assert child.provider_run_id is not None
                        await await_provider_status(
                            child.child_execution_id, child.provider_run_id, {"running"}
                        )
                    accepted = await facade.command(run_id, "cancel", CancelAction())
                    assert accepted["phase"] == "cancelling"
                    await until(
                        lambda: _state_is(authority, run_id, "cancel", "delivered"), seconds=60
                    )
                    if live:
                        assert async_service is not None
                        child = await _await_child(async_service, child.child_execution_id)
                        link = await async_service.link(SCOPE, child.child_execution_id)
                        evidence["child"] = {
                            "child_execution_id": child.child_execution_id,
                            "provider_run_id": child.provider_run_id,
                            "lifecycle": child.lifecycle.value,
                            "cancellation_receipt": link.cancellation_receipt,
                            "result_decision": link.result_decision,
                            "usage_disposition": link.usage_disposition,
                        }
                        # The pending usage keeps the run cancelling; a privileged operator
                        # reconciles it from the provider's durable thread state.
                        await until(
                            lambda: _run_phase_is(run_control, run_id, "cancelling"), seconds=60
                        )
                        evidence["usage_reconciliation"] = await _reconcile_child_usage(
                            async_service, run_control, run_id, child
                        )
                        await family.signal(GoalDirectedWorkflow.liability_reconciled, "usage")
                    root_result = await asyncio.wait_for(root.result(), timeout=300)
                    turns = list(model.calls)
                    root_runs = await replay(root, [BellLabsRunWorkflow])
                    family_runs = await replay(family, [GoalDirectedWorkflow, OperationWorkflow])
                projection = await facade.projection(run_id)
        rows = await _journal_rows(pool, run_id)
        receipt_rows = await _receipt_rows(pool, run_id)
        transitions = await _transitions(pool, run_id)
        budget = await run_control.get_budget(SCOPE, run_id)
        effects = await run_control.get_effects(SCOPE, run_id)
        run = await run_control.get_run(SCOPE, run_id)
        assert root_result["status"] == "cancelled"
        assert run.terminal_outcome == RunOutcome.CANCELLED
        assert projection["terminal_outcome"] == "cancelled"
        assert [(row["operation"], row["status"]) for row in rows] == [
            ("goal-iteration/1/executor:attempt:1", "cancelled")
        ]
        assert budget.reservations == {} and not any(budget.pending_settlement.values())
        assert all(claim.settlement is not None for claim in effects.claims.values())
        unit_claim = next(
            claim for claim in effects.claims.values() if claim.effect_kind == "operation.runtime"
        )
        assert unit_claim.disposition == EffectDisposition.CANCELLED
        assert [item["classification"] for item in transitions] == ["interrupted"]
        assert [(row[0], row[2]) for row in receipt_rows if row[0] == "cancel"] == [
            ("cancel", "accepted"),
            ("cancel", "delivered"),
            ("cancel", "applied"),
        ]
        assert (root_runs, family_runs) == (1, 1)
        if live:
            assert child is not None
            runs = await provider_runs(child.child_execution_id)
            assert len(runs) == 1 and runs[0]["status"] in {"interrupted", "cancelled"}
            child_claim = effects.claims[async_child_effect_id(child.child_execution_id)]
            assert child_claim.disposition == EffectDisposition.CANCELLED
            assert child_claim.settlement is not None
            evidence["child"]["provider_status"] = runs[0]["status"]
        evidence.update(
            {
                "run_id": run_id,
                "root_workflow_id": submitted.workflow_id,
                "family_workflow_id": family_id,
                "operations": [
                    {key: row[key] for key in ("operation", "status", "usage")} for row in rows
                ],
                "transitions": [
                    (item["classification"], item["claim_fence"]) for item in transitions
                ],
                "budget": {
                    "consumed": budget.consumed,
                    "reservations": sorted(budget.reservations),
                },
                "effects": {
                    claim.effect_kind: claim.disposition.value for claim in effects.claims.values()
                },
                "receipts": [(row[0], row[1], row[2], row[3]) for row in receipt_rows],
                "model_calls": turns,
                "replayed": {"root": root_runs, "family": family_runs},
                "terminal_outcome": str(run.terminal_outcome),
            }
        )
    finally:
        await pool.close()
        await family_pool.close()
        await mongo.close()
    return evidence


async def _await_child(service: AsyncSubagentService, child_id: str) -> Any:
    """Wait for the child's terminal lifecycle and the parent's settlement of it.

    The parent's `cancel_children` settles the child (`usage_disposition`) after the
    provider's terminal state is observed; reading the budget before that settlement would
    observe the still-open child effect, not the pending usage it leaves behind.
    """

    async with asyncio.timeout(240):
        while True:
            execution = await service.execution(SCOPE, child_id)
            link = await service.link(SCOPE, child_id)
            if (
                execution.lifecycle
                in {
                    AsyncSubagentLifecycle.CANCELLED,
                    AsyncSubagentLifecycle.COMPLETED,
                    AsyncSubagentLifecycle.FAILED,
                    AsyncSubagentLifecycle.ORPHANED,
                }
                and link.usage_disposition is not None
            ):
                return execution
            await asyncio.sleep(2)


async def _run_phase_is(run_control: Any, run_id: str, phase: str) -> bool:
    return (await run_control.get_run(SCOPE, run_id)).phase.value == phase


async def _reconcile_child_usage(
    service: AsyncSubagentService, run_control: Any, run_id: str, child: Any
) -> dict[str, Any]:
    budget = await run_control.get_budget(SCOPE, run_id)
    pending = budget.usage_records[async_child_usage_id(child.child_execution_id)]
    state = await sdk_client().threads.get_state(child.child_execution_id)
    values = state.get("values") or {}
    observed = attribute_usage(
        child.provider_run_id,
        values.get("messages"),
        CHILD_BUDGET,
        provider_usage=values.get(PROVIDER_USAGE_STATE_KEY),
    )
    if observed.attribution != "provider_attributed":
        # The interrupted run completed no model turn the provider attributes: the operator
        # asserts the attributable usage (zero) as a privileged, recorded decision.
        observed = AsyncSubagentUsage(
            provider_run_id=child.provider_run_id,
            attribution="provider_attributed",
            attributed_amounts={"tokens.total": 0},
        )
    reconciled = await service.reconcile_usage(
        SCOPE,
        child.child_execution_id,
        actor=reconciler(),
        run_usage={child.provider_run_id: observed},
        settlement_ref=f"settlement:{child.child_execution_id}:reconciled",
        reconciled_at=datetime.now(UTC),
    )
    assert reconciled.settled is True
    return {
        "pending_before": dict(pending.pending_external_amounts),
        "attributed": dict(observed.attributed_amounts),
        "settlement_revision": reconciled.settlement_revision,
    }


@pytest.mark.asyncio
async def test_goal_directed_cancellation_on_real_temporal_with_postgres_authority(
    test_application_postgres_dsn: str,
    mongo_database: str,
    test_mongodb_uri: str,
    tmp_path: Path,
) -> None:
    evidence = await _goal_cancellation(
        test_application_postgres_dsn, test_mongodb_uri, mongo_database, tmp_path, live=False
    )
    assert evidence["model_calls"] == [(1, 0)], "the interrupted call never resumed"
    _evidence("goal-directed", evidence)


@pytest.mark.skipif(
    os.getenv(LIVE_FLAG) != "1"
    or not os.getenv("AGENT_SERVER_ENDPOINT")
    or not os.getenv(TOKEN_ENV),
    reason=f"{LIVE_FLAG}=1, AGENT_SERVER_ENDPOINT and {TOKEN_ENV} are required",
)
@pytest.mark.asyncio
async def test_goal_directed_cancellation_with_a_real_async_child(
    test_application_postgres_dsn: str,
    mongo_database: str,
    test_mongodb_uri: str,
    tmp_path: Path,
) -> None:
    evidence = await _goal_cancellation(
        test_application_postgres_dsn, test_mongodb_uri, mongo_database, tmp_path, live=True
    )
    assert evidence["child"]["lifecycle"] == "cancelled"
    assert evidence["child"]["cancellation_receipt"] == "provider_acknowledged"
    assert evidence["child"]["usage_disposition"] == "pending_usage"
    assert evidence["child"]["result_decision"] == "reject"
    _evidence("goal-directed-async-child", evidence)
