from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.experiments.langgraph_temporal_stagegraph.config import load_settings
from app.experiments.langgraph_temporal_stagegraph.contracts import CompletionRecord, digest_text
from app.experiments.langgraph_temporal_stagegraph.graph import (
    choose_synthesis_inputs,
    compile_experiment_graph,
    launch_stage,
    synthesis_ready,
)
from app.experiments.langgraph_temporal_stagegraph.repository import (
    ExperimentRepository,
    prepare_database,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def test_any_join_and_deterministic_frozen_selection() -> None:
    assert not synthesis_ready({})
    assert synthesis_ready({"fast_research": "fast"})
    assert synthesis_ready({"slow_research": "slow"})
    assert synthesis_ready({"fast_research": "fast", "slow_research": "slow"})
    now = datetime.now(UTC)
    attempts = [
        {
            "stage_id": "slow_research",
            "status": "ADMITTED",
            "admitted_at": now + timedelta(seconds=1),
        },
        {"stage_id": "fast_research", "status": "ADMITTED", "admitted_at": now},
    ]
    assert tuple(item["stage_id"] for item in choose_synthesis_inputs(attempts)) == (
        "fast_research",
    )
    attempts[0]["admitted_at"] = now
    assert tuple(item["stage_id"] for item in choose_synthesis_inputs(attempts)) == (
        "fast_research",
    )


class _FakeTemporal:
    def __init__(self) -> None:
        self.starts = 0
        self.handle = SimpleNamespace(result_run_id="temporal-run-1", run_id="temporal-run-1")

    async def start_workflow(self, *_args, **kwargs):
        self.starts += 1
        if self.starts > 1:
            raise WorkflowAlreadyStartedError(kwargs["id"], "TemporalStageWorkflow")
        return self.handle

    def get_workflow_handle(self, _workflow_id):
        return self.handle


class _FakeRepository:
    def __init__(self) -> None:
        self.bindings: list[tuple[str, str, str | None]] = []

    async def bind_temporal_execution(self, attempt_id, workflow_id, run_id):
        binding = (attempt_id, workflow_id, run_id)
        if binding not in self.bindings:
            self.bindings.append(binding)


@pytest.mark.asyncio
async def test_launch_idempotency_uses_one_workflow_identity() -> None:
    settings = load_settings(require_openai=False)
    repository = _FakeRepository()
    temporal = _FakeTemporal()
    state = {
        "run_id": "run-test",
        "thread_id": "thread-test",
        "dispatch_item": {
            "attempt_id": "attempt:run-test:fast_research:1",
            "stage_id": "fast_research",
            "prompt": "bounded",
            "delay_seconds": 0.0,
        },
    }
    first = await launch_stage(state, repository=repository, temporal=temporal, settings=settings)
    second = await launch_stage(state, repository=repository, temporal=temporal, settings=settings)
    assert first["launch_receipts"] == second["launch_receipts"]
    assert len(repository.bindings) == 1


@pytest.mark.asyncio
async def test_completion_and_admission_are_idempotent_and_cover_early_wake() -> None:
    settings = load_settings(require_openai=False)
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    run_id = f"test-{uuid.uuid4().hex[:10]}"
    thread_id = f"thread:{run_id}"
    try:
        await prepare_database(settings.application_migration_database_dsn)
        await repository.create_run(run_id, thread_id)
        item = await repository.reserve_attempt(run_id, "fast_research", "bounded", 0)
        workflow_id = f"stagegraph-experiment:{item['attempt_id']}"
        await repository.bind_temporal_execution(item["attempt_id"], workflow_id, "run-1")
        output = "A short public-research result."
        completion = CompletionRecord(
            run_id=run_id,
            thread_id=thread_id,
            attempt_id=item["attempt_id"],
            stage_id="fast_research",
            temporal_workflow_id=workflow_id,
            temporal_run_id="run-1",
            disposition="succeeded",
            output_text=output,
            output_digest=digest_text(output),
            error_type=None,
        )
        # Completion happens before any graph interrupt. Reconciliation reads authority directly.
        await repository.record_completion_and_wake(completion)
        await repository.record_completion_and_wake(completion)
        attempts = await repository.load_attempts(run_id)
        assert attempts[0]["status"] == "READY_TO_RECONCILE"
        first = await repository.admit_success_idempotently(item["attempt_id"])
        second = await repository.admit_success_idempotently(item["attempt_id"])
        assert first == second
        timeline = await repository.timeline(run_id)
        assert len(timeline["attempts"]) == 1
        assert len(timeline["outbox"]) == 1
        assert timeline["attempts"][0]["status"] == "ADMITTED"
        assert synthesis_ready({"fast_research": first})
    finally:
        await repository.close()


@pytest.mark.asyncio
async def test_postgres_checkpoint_persists_interrupt() -> None:
    settings = load_settings(require_openai=False)
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    run_id = f"checkpoint-{uuid.uuid4().hex[:10]}"
    thread_id = f"thread:{run_id}"
    temporal = _FakeTemporal()
    try:
        await prepare_database(settings.application_migration_database_dsn)
        await repository.create_run(run_id, thread_id)
        async with AsyncPostgresSaver.from_conn_string(settings.application_database_dsn) as saver:
            graph = compile_experiment_graph(
                repository=repository,
                temporal=temporal,
                settings=settings,
                checkpointer=saver,
            )
            config = {"configurable": {"thread_id": thread_id}}
            await graph.ainvoke(
                {
                    "run_id": run_id,
                    "thread_id": thread_id,
                    "dispatch_batch": (),
                    "launch_receipts": (),
                    "admitted_outputs": {},
                    "waiting_attempt_ids": (),
                    "synthesized_output": None,
                    "frozen_synthesis_stages": (),
                    "event_log": (),
                },
                config=config,
            )
            snapshot = await graph.aget_state(config)
            assert any(task.interrupts for task in snapshot.tasks)
            assert snapshot.config["configurable"]["thread_id"] == thread_id
            assert len(snapshot.values["launch_receipts"]) == 2
    finally:
        await repository.close()
