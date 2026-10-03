"""RRM-016 demonstration: journaled GoalDirected operations on real Temporal.

A real Temporal dev server (`WorkflowEnvironment.start_local`) runs the stable root
(`BellLabsRunWorkflow`, started through `TemporalWorkflowSubmitter`), the GoalDirected family
and its `OperationWorkflow` children. Every store is durable:

* application PostgreSQL: run control, the boundary-command ledger, the operation journal and
  checkpoint lineage (`compose_postgres_operation_recovery`);
* a real `AsyncPostgresSaver` (dedicated schema) as the Deep Agent checkpointer;
* MongoDB: the OEB binding store and the GoalDirected templates and documents.

Cognition is a real `create_deep_agent` graph with a deterministic scripted model. Two
iterations run (the verifier rejects iteration 1 and accepts iteration 2). A pause is
requested through the governed API while the first executor runs and is applied at the
iteration boundary; worker 1 is then stopped, a fresh composition (worker 2) takes over, and
the resume continues the frontier. Each executor and verifier operation is verified by
`RunControlOperationAuthority`, claimed, fenced, observed and settled once in run control;
the family consumes each settlement and records no operation usage.

RRM-018/RRM-019: the GoalDirected family documents (Goal Revision, iterations, verifications)
are MongoDB documents, and each iteration produces its own output ref. The unchanged Goal
Revision is persisted by both executor preparations (one per worker) and stays one immutable
document; the run completes and promotes exactly the verified final output.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` (disposable stack only).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
import pytest
from beanie import init_beanie
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.operations.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operations.operation_execution import operation_settlement_id
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
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.run_control.contracts import EffectDisposition, RunOutcome, RunPhase
from app.integrations.mongodb import BEANIE_MODELS
from app.integrations.temporal_boundary_commands import TemporalBoundaryCommandTransport
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.models.goal_directed import (
    GoalIterationDocument,
    GoalRevisionDocument,
    GoalVerificationDocument,
)
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import (
    JOURNALED_SETTLEMENT_PATCH,
    VERIFIED_TERMINAL_OUTPUTS_PATCH,
    GoalDirectedWorkflow,
)
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow
from tests.acceptance.control_plane.test_rrm_007_interventions import Facade, _receipt_rows
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    SEMANTIC_INPUT,
    TOKENS_PER_OPERATION,
    GoalComposition,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
    goal_templates,
    turns_by_operation,
)
from tests.fixtures.mongo_database import disposable_mongo_database
from tests.fixtures.rrm004_persistent_stack import FileArtifactPayloadStore
from tests.fixtures.temporal_history import patch_ids, scheduled_activity_inputs
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.integration.temporal.test_rrm_007_boundary_interventions import (
    Authority,
    _phase_is,
    _state_is,
    pause,
    replay,
    resume,
    until,
)

GOAL_QUEUE = "rrm016-goal-directed"
SAVER_SCHEMA = "rrm016_goal_saver"
WORKFLOWS = [BellLabsRunWorkflow, StageGraphWorkflow, GoalDirectedWorkflow, OperationWorkflow]
BASELINE = {"tokens.total": 20}
CLAIMED_BY = "operation-runtime:rrm-016"  # deployment-stable, never per worker


mongo_database = disposable_mongo_database("rrm016_goal")


def _saver_dsn(dsn: str) -> str:
    return f"{dsn}?options=" + quote(f"-c search_path={SAVER_SCHEMA}")


@asynccontextmanager
async def _worker(
    env: WorkflowEnvironment,
    pool: asyncpg.Pool,
    family_pool: asyncpg.Pool,
    dsn: str,
    results: Path,
    identity: str,
) -> AsyncIterator[GoalComposition]:
    """One worker process's composition: every object is new; only durable stores are
    shared. The family documents and templates are MongoDB (RRM-018)."""

    run_control = _run_control(pool, family_pool)
    recovery = compose_postgres_operation_recovery(pool, run_control=run_control)
    documents = MongoGoalDirectedDocumentRepository()
    async with AsyncPostgresSaver.from_conn_string(_saver_dsn(dsn)) as saver:
        composition = await compose_goal_directed(
            run_control=run_control,
            journal=PostgresAtomicOperationJournalRepository(pool),
            lineage=recovery.lineage,
            results=FileArtifactPayloadStore(results),
            bindings=MongoOperationBindingRepository(),
            saver=saver,
            # RRM-019: each iteration produces its own output ref.
            model=GoalScriptedModel(stable_output_ref=False),
            blueprint=goal_blueprint(),
            claimed_by=CLAIMED_BY,
            template_provider=documents,
            documents=documents,
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
            yield composition


def _run_control(pool: asyncpg.Pool, family_pool: asyncpg.Pool) -> Any:
    """Run control on PostgreSQL; family admissions commit through their own writer pool."""

    return goal_run_control(
        PostgresRunControlRepository(pool, family_writer_pool=family_pool)  # type: ignore[arg-type]
    )


def _submitter(client: Client) -> TemporalWorkflowSubmitter:
    return TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="rrm016-stagegraph-unused",
        goal_directed_task_queue=GOAL_QUEUE,
    )


async def _journal_rows(pool: asyncpg.Pool, run_id: str) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT c.semantic_binding_id, c.semantic_attempt_key, s.settlement_id, s.status,
                   s.usage_payload, s.settlement_revision
            FROM belllabs_control.operation_effect_claims c
            JOIN belllabs_control.operation_settlements s
              ON s.request_scope = c.request_scope AND s.effect_claim_id = c.effect_claim_id
            WHERE c.belllabs_run_id = $1
            ORDER BY c.semantic_attempt_key
            """,
            run_id,
        )
    return [
        {
            "binding": row["semantic_binding_id"],
            "operation": row["semantic_attempt_key"].split(":operation:")[1],
            "settlement_id": row["settlement_id"],
            "status": row["status"],
            "usage": json.loads(row["usage_payload"]),
            "revision": row["settlement_revision"],
        }
        for row in rows
    ]


async def _consumption_entries(pool: asyncpg.Pool, run_id: str) -> dict[str, int]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT idempotency_id, count(*) AS entries
            FROM belllabs_control.budget_ledger
            WHERE run_id = $1 AND kind = 'consumption'
            GROUP BY idempotency_id
            """,
            run_id,
        )
    return {row["idempotency_id"]: row["entries"] for row in rows}


async def _transitions(pool: asyncpg.Pool, run_id: str) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT t.cognitive_namespace, t.source_checkpoint_id, t.result_checkpoint_id,
                   t.classification, t.claim_fence
            FROM belllabs_control.runtime_checkpoint_transitions t
            WHERE t.cognitive_namespace LIKE $1
            ORDER BY t.observed_at
            """,
            f"belllabs/goal/{run_id}/%",
        )
    return [dict(row) for row in rows]


@pytest.mark.asyncio
async def test_journaled_goal_directed_run_on_real_temporal(
    test_application_postgres_dsn: str,
    mongo_database: str,
    test_mongodb_uri: str,
    tmp_path: Path,
) -> None:
    dsn = test_application_postgres_dsn
    require_disposable_postgres(dsn)
    owner = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {SAVER_SCHEMA} CASCADE")
            await connection.execute(f"CREATE SCHEMA {SAVER_SCHEMA}")
    finally:
        await owner.close()
    async with AsyncPostgresSaver.from_conn_string(_saver_dsn(dsn)) as saver:
        await saver.setup()
    mongo: AsyncMongoClient[Any] = AsyncMongoClient(
        test_mongodb_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10)
    family_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    results = tmp_path / "results"
    # Bindings, templates and the family documents are MongoDB; all authority is PostgreSQL.
    evidence: dict[str, Any] = {}
    try:
        await init_beanie(database=mongo[mongo_database], document_models=BEANIE_MODELS)
        run_control = _run_control(pool, family_pool)
        # The per-run operation templates, persisted once (immutable) in MongoDB.
        from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

        binding, _profile, _bundle = exact_fixture()
        templates = goal_templates(binding)
        await MongoGoalDirectedDocumentRepository().persist_templates(
            request_scope=SCOPE,
            semantic_input_binding_ref=SEMANTIC_INPUT,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=templates["executor"].requested_at,
        )
        async with await WorkflowEnvironment.start_local(dev_server_log_level="error") as env:
            authority = Authority(env.client, run_control)
            authority.facade = BoundaryInterventionService(
                run_control,
                BoundaryCommandDeliveryService(
                    run_control, TemporalBoundaryCommandTransport(env.client)
                ),
            )
            run_id = await admit_goal_run(run_control, "rrm-016-demo-goal")
            family_id = f"family/{run_id}/1"
            run_input = goal_run_input(run_id, goal_blueprint(), baseline=BASELINE)
            async with Facade(authority) as facade:
                async with _worker(
                    env, pool, family_pool, dsn, results, "rrm016-worker-1"
                ) as first:
                    entered, gate = first.model.gate_on(1)
                    submitted = await _submitter(env.client).submit(
                        run_input,
                        workflow_id="ignored",
                        blueprint_family=BlueprintFamily.GOAL_DIRECTED,
                    )
                    await asyncio.wait_for(entered.wait(), timeout=90)
                    await facade.command(run_id, "pause", pause("hold-run"))
                    await until(
                        lambda: _state_is(authority, run_id, "pause", "delivered"), seconds=60
                    )
                    assert (await facade.projection(run_id))["phase"] == "active"
                    gate.set()
                    await until(lambda: _phase_is(authority, run_id, RunPhase.PAUSED), seconds=120)
                    first_turns = turns_by_operation(first.model)
                # Worker 1 is gone while the run is paused.
                paused_budget = await run_control.get_budget(SCOPE, run_id)
                paused_rows = await _journal_rows(pool, run_id)
                projection = await facade.projection(run_id)
                assert projection["phase"] == "paused"
                assert set(paused_budget.reservations) == {"baseline"}
                assert [row["operation"] for row in paused_rows] == [
                    "goal-iteration/1/executor:attempt:1",
                    "goal-iteration/1/verifier:attempt:1",
                ]

                async with _worker(
                    env, pool, family_pool, dsn, results, "rrm016-worker-2"
                ) as second:
                    root = env.client.get_workflow_handle(submitted.workflow_id)
                    family = env.client.get_workflow_handle(family_id)
                    state = await family.query(GoalDirectedWorkflow.boundary_state)
                    assert state["paused"]["next_goal_iteration"] == 2
                    await facade.command(run_id, "resume", resume("hold-run", "release-run"))
                    root_result = await asyncio.wait_for(root.result(), timeout=240)
                    second_turns = turns_by_operation(second.model)
                    family_history = await family.fetch_history()
                    root_runs = await replay(root, [BellLabsRunWorkflow])
                    family_runs = await replay(family, [GoalDirectedWorkflow, OperationWorkflow])

        # --- Assertions over durable authority ------------------------------------------
        assert root_result["convergence_proposal"]["action"] == "complete"
        assert root_result["goal_iterations"] == 2
        run = await run_control.get_run(SCOPE, run_id)
        budget = await run_control.get_budget(SCOPE, run_id)
        effects = await run_control.get_effects(SCOPE, run_id)
        assert run.terminal_outcome == RunOutcome.COMPLETED

        rows = await _journal_rows(pool, run_id)
        assert [row["operation"] for row in rows] == [
            "goal-iteration/1/executor:attempt:1",
            "goal-iteration/1/verifier:attempt:1",
            "goal-iteration/2/executor:attempt:1",
            "goal-iteration/2/verifier:attempt:1",
        ]
        assert {(row["status"], row["revision"]) for row in rows} == {("completed", 1)}
        assert all(row["usage"] == {"tokens.total": TOKENS_PER_OPERATION} for row in rows)
        assert all(row["settlement_id"] == operation_settlement_id(row["binding"]) for row in rows)
        settlements = {row["settlement_id"]: row["binding"] for row in rows}
        # REQ-CP-RUN-006/009: each operation's usage recorded exactly once, by its settlement.
        consumption = await _consumption_entries(pool, run_id)
        assert consumption == {**{key: 1 for key in settlements}, "goal-usage:baseline": 1}
        assert set(budget.usage_records) == {*settlements, "goal-usage:baseline"}
        assert all(
            budget.usage_records[key].authority_ref == value for key, value in settlements.items()
        )
        assert budget.consumed.get("tokens.total") == 4 * TOKENS_PER_OPERATION
        assert not any(budget.reserved.values()) and budget.reservations == {}
        # REQ-CP-RUN-007: every effect claimed, observed and settled; evidence accepted.
        assert sorted(item.operation_ref for item in effects.claims.values()) == sorted(
            settlements.values()
        )
        assert {item.disposition for item in effects.claims.values()} == {
            EffectDisposition.SUCCEEDED
        }
        assert {
            (item.settlement_id, item.accepted_by_authority_ref)
            for item in run.accepted_operation_settlement_evidence
        } == set(settlements.items())

        # REQ-BP-GD-011 receipts, root-first delivery, worker restart while paused.
        receipt_rows = await _receipt_rows(pool, run_id)
        assert [(row[0], row[1], row[2]) for row in receipt_rows] == [
            ("pause", 1, "accepted"),
            ("pause", 1, "delivered"),
            ("pause", 1, "applied"),
            ("resume", 2, "accepted"),
            ("resume", 2, "delivered"),
            ("resume", 2, "applied"),
        ]
        # REQ-BP-GD-012: the executor session continues across the restart from its head;
        # the verifier uses its own namespace.
        transitions = await _transitions(pool, run_id)
        executor = [
            item for item in transitions if item["cognitive_namespace"].endswith("/role/executor")
        ]
        verifier = [
            item for item in transitions if item["cognitive_namespace"].endswith("/role/verifier")
        ]
        assert len(executor) == len(verifier) == 2
        assert executor[0]["cognitive_namespace"] == executor[1]["cognitive_namespace"]
        assert executor[1]["source_checkpoint_id"] == executor[0]["result_checkpoint_id"]
        assert executor[0]["source_checkpoint_id"] is None
        assert {item["classification"] for item in transitions} == {"not_submitted"}
        assert first_turns == [
            ("executor", 1, 1),
            ("executor", 1, 1),
            ("verifier", 1, 1),
            ("verifier", 1, 1),
        ]
        assert second_turns == [
            ("executor", 2, 2),
            ("executor", 2, 2),
            ("verifier", 2, 2),
            ("verifier", 2, 2),
        ]
        # The family recorded no operation usage; the new histories carry the RRM-016 patch.
        usage_commands = [
            str(item["command_id"])
            for item in scheduled_activity_inputs(
                family_history, "goaldirected.apply_lifecycle_command"
            )
            if str(item["command_id"]).startswith("goal:usage:")
        ]
        assert usage_commands == ["goal:usage:baseline"]
        assert JOURNALED_SETTLEMENT_PATCH in patch_ids(family_history)
        assert (root_runs, family_runs) == (1, 1)

        # RRM-018: both executor preparations (worker 1, then worker 2 after the restart)
        # persisted the unchanged Goal Revision; MongoDB holds it once, with iteration 1's time.
        prepared_revisions = [
            item["goal_revision"]["revision_id"]
            for item in scheduled_activity_inputs(family_history, "goaldirected.prepare_executor")
        ]
        assert prepared_revisions == [run_input.initial_revision.revision_id] * 2
        revision_documents = await GoalRevisionDocument.find({"run_id": run_id}).to_list()
        iteration_documents = await GoalIterationDocument.find({"run_id": run_id}).to_list()
        verification_documents = await GoalVerificationDocument.find({"run_id": run_id}).to_list()
        assert [item.goal_revision_id for item in revision_documents] == [
            run_input.initial_revision.revision_id
        ]
        assert len(iteration_documents) == len(verification_documents) == 2
        iteration_outputs = sorted(
            tuple(item.payload["output_refs"]) for item in iteration_documents
        )
        assert iteration_outputs == [("artifact:rrm016:1",), ("artifact:rrm016:2",)]
        # RRM-019: distinct output refs per iteration; the family result keeps both as lineage,
        # the run promotes and terminalizes exactly the verified final output.
        assert root_result["output_refs"] == ["artifact:rrm016:1", "artifact:rrm016:2"]
        assert root_result["terminalization_proposal"]["output_refs"] == ["artifact:rrm016:2"]
        assert [item.output_ref for item in run.accepted_output_evidence] == ["artifact:rrm016:2"]
        assert VERIFIED_TERMINAL_OUTPUTS_PATCH in patch_ids(family_history)

        evidence = {
            "run_id": run_id,
            "root_workflow_id": submitted.workflow_id,
            "family_workflow_id": family_id,
            "operations": [
                {key: row[key] for key in ("operation", "status", "usage")} for row in rows
            ],
            "budget": {
                "consumed": budget.consumed,
                "reserved": {k: v for k, v in budget.reserved.items() if v},
                "consumption_entries": sorted(consumption.values()),
                "usage_record_ids": sorted(
                    "operation-settlement" if key in settlements else key
                    for key in budget.usage_records
                ),
            },
            "effects_settled": len(effects.claims),
            "accepted_settlement_evidence": len(run.accepted_operation_settlement_evidence),
            "receipts": [(row[0], row[1], row[2]) for row in receipt_rows],
            "executor_session_chain": [
                (item["source_checkpoint_id"] is not None, item["claim_fence"]) for item in executor
            ],
            "model_turns_worker_1": first_turns,
            "model_turns_worker_2": second_turns,
            "family_usage_commands": usage_commands,
            "patches": sorted(patch_ids(family_history)),
            "replayed": {"root": root_runs, "family": family_runs},
            "terminal_outcome": str(run.terminal_outcome),
            "rrm018_mongo_documents": {
                "revisions": [item.goal_revision_id for item in revision_documents],
                "revision_recorded_at": str(revision_documents[0].recorded_at),
                "executor_preparations_persisting_revision": len(prepared_revisions),
                "iterations": len(iteration_documents),
                "verifications": len(verification_documents),
            },
            "rrm019_outputs": {
                "iteration_outputs": iteration_outputs,
                "family_result_output_refs": root_result["output_refs"],
                "proposal_output_refs": root_result["terminalization_proposal"]["output_refs"],
                "accepted_output_evidence": [
                    item.output_ref for item in run.accepted_output_evidence
                ],
            },
        }
    finally:
        await pool.close()
        await family_pool.close()
        await mongo.close()
    print("RRM-016 EVIDENCE demonstration: " + json.dumps(evidence, sort_keys=True, default=str))
