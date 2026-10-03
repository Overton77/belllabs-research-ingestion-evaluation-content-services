"""RRM-004 real worker-restart recovery (REQ-CP-DA-018 verification, REQ-CP-EXEC-003/014).

A real Temporal dev server (`WorkflowEnvironment.start_local`) stays up across two workers.
Worker 1 runs in its own process with the persistent stack (real `AsyncPostgresSaver`,
application PostgreSQL run control, journal and checkpoint lineage) and is killed right
after the Deep Agent's post-tool checkpoint is durable. Temporal times the lost Activity
attempt out; worker 2 (a fresh composition in this process) receives the retry, takes over
the expired claim lease by advancing the fence, classifies the unit `interrupted`, resumes
the pinned checkpoint without re-appending the prompt, and settles once. Both workers
compose the production `RunControlOperationAuthority`: worker 2's recovery is admitted as a
continuation of the bound attempt although the claim moved the run version.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` (disposable stack only). The user's docker
compose Temporal stack is never used.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from langchain_core.runnables import RunnableConfig
from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.application.operations.journaled_operation_execution import _effect_claim_id
from app.application.operations.operation_execution import bind_operation_execution_request
from app.config import PROJECT_ROOT
from app.domain.operation_execution.checkpoint_lineage import (
    STAMP_INVOCATION_ID,
    CheckpointClassification,
    submission_invocation_id,
)
from app.domain.operation_execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from app.domain.run_control.contracts import CommandStatus, ReserveBudgetAction, StartAction
from app.temporal.operation_activities import OperationExecutionActivities, parse_operation_result
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.operation import OperationWorkflow
from tests.fixtures.checkpoint_lineage import bind_unit
from tests.fixtures.checkpoint_recovery import (
    RESULT_MARKER,
    governed_workspace,
    stage_recovery_unit,
)
from tests.fixtures.mongo_database import disposable_mongo_database
from tests.fixtures.rrm004_persistent_stack import (
    SAVER_SCHEMA,
    WORKFLOW_TASK_QUEUE,
    PersistentStack,
    StackPaths,
    open_persistent_stack,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_run_control import command
from tests.unit.run_control.test_run_control import request as run_request

# The scripted graph's sixth root checkpoint records the executed tool call; the next step
# is the second model call. Worker 1 is lost right after that checkpoint is durable.
HANG_AFTER_CHECKPOINT = 6
ACTIVITY_TIMEOUT_SECONDS = 15


mongo_database = disposable_mongo_database("rrm004_restart")


def _root(namespace: str, checkpoint_id: str | None = None) -> RunnableConfig:
    configurable: dict[str, Any] = {"thread_id": namespace, "checkpoint_ns": ""}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


async def _bound_request(stack: PersistentStack, run_id: str) -> OperationExecutionRequest:
    unit = stage_recovery_unit(run_id)
    run = await stack.run_control.get_run("tenant-1", run_id)
    reservation_id = f"reservation:{unit.unit_key}"
    reserved = await stack.run_control.execute(
        command(
            run_id,
            run.version,
            f"reserve:{unit.unit_key}",
            ReserveBudgetAction(reservation_id=reservation_id, amounts={"tokens.total": 10}),
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
    return OperationExecutionRequest.model_validate(
        {
            **operation_request().model_dump(mode="python"),
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
            "budget_limits": {"tokens.total": 10},
            "idempotency_key": f"rrm-004-restart:{unit.unit_key}",
        }
    )


def _exists(path: Path) -> bool:
    return path.exists()


async def _wait_for(path: Path, process: subprocess.Popen[bytes], seconds: float) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 10)):
            if _exists(path):
                return
            if process.poll() is not None:
                raise AssertionError(f"worker 1 exited early with {process.returncode}")
            await asyncio.sleep(0.1)
    raise AssertionError(f"{path.name} did not appear")


def _spawn_worker_1(
    target: str, dsn: str, root: Path, mongo_uri: str, mongo_database: str
) -> subprocess.Popen[bytes]:
    """Worker 1 runs in its own OS process so that it can be killed like a lost host.

    (A plain `Popen`: asyncio subprocesses are unavailable on the selector loop that the
    Psycopg saver requires on Windows.)
    """

    return subprocess.Popen(
        [sys.executable, "-m", "tests.fixtures.rrm004_restart_worker"],
        cwd=PROJECT_ROOT,
        env={
            **os.environ,
            "RRM004_TEMPORAL_TARGET": target,
            "RRM004_DSN": dsn,
            "RRM004_MONGO_URI": mongo_uri,
            "RRM004_MONGO_DATABASE": mongo_database,
            "RRM004_ROOT": str(root),
            "RRM004_HANG_AFTER_CHECKPOINT": str(HANG_AFTER_CHECKPOINT),
            # A different string-hash seed from this process: the cross-process replay of
            # journal authority must not depend on set iteration order.
            "PYTHONHASHSEED": "7",
        },
        stdout=subprocess.DEVNULL,
        stderr=(root / "worker-1.log").open("wb"),
    )


def _model_calls(paths: StackPaths) -> list[dict[str, int]]:
    if not paths.model_log.exists():
        return []
    return [json.loads(line) for line in paths.model_log.read_text().splitlines() if line]


@pytest.mark.asyncio
async def test_worker_restart_resumes_the_interrupted_unit_and_settles_once(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    tmp_path: Path,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    paths = StackPaths(tmp_path)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {SAVER_SCHEMA} CASCADE")
            await connection.execute(f"CREATE SCHEMA {SAVER_SCHEMA}")
    finally:
        await owner.close()

    async with open_persistent_stack(
        test_application_postgres_dsn,
        paths,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
    ) as stack:
        await stack.saver.setup()
        admitted = await stack.run_control.admit(run_request(request_id="rrm-004-restart"))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        started = await stack.run_control.execute(
            command(run_id, 1, "rrm-004-restart-start", StartAction())
        )
        assert started.status == CommandStatus.ACCEPTED
        request = await _bound_request(stack, run_id)
    unit = request.runtime_unit
    assert unit is not None and request.deep_agent_binding is not None
    namespace = request.deep_agent_binding.cognitive_session_namespace
    assert namespace is not None
    workflow_request = OperationWorkflowRequest(
        semantic_attempt_id=request.identity.semantic_key,
        operation_kind="bound_operation",
        operation=request,
        timeout_seconds=ACTIVITY_TIMEOUT_SECONDS,
    )

    async with await WorkflowEnvironment.start_local(dev_server_log_level="error") as env:
        target = env.client.service_client.config.target_host
        worker_1 = _spawn_worker_1(
            target, test_application_postgres_dsn, tmp_path, test_mongodb_uri, mongo_database
        )
        try:
            await _wait_for(tmp_path / "worker-1-ready", worker_1, 120)
            handle: WorkflowHandle[Any, Any] = await env.client.start_workflow(
                OperationWorkflow.run,
                workflow_request,
                id=workflow_request.workflow_id,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
            await _wait_for(paths.crash_marker, worker_1, 120)
        finally:
            # Stop worker 1 mid-operation: a hard kill, no shutdown hooks, no lease release.
            worker_1.kill()
            worker_1.wait(timeout=60)
        killed_pid = int(paths.crash_marker.read_text(encoding="utf-8"))

        async with open_persistent_stack(
            test_application_postgres_dsn,
            paths,
            mongo_uri=test_mongodb_uri,
            mongo_database=mongo_database,
        ) as stack:
            lineage = stack.recovery.lineage.repository
            # The durable state worker 1 left: a dispatching attempt, six stamped root
            # checkpoints, no transition, no result, no settlement.
            crash_attempts = await lineage.list_attempts("tenant-1", unit.unit_key)
            assert [(item.attempt.attempt, item.dispatching) for item in crash_attempts] == [
                (1, True)
            ]
            crash_checkpoints = [
                item async for item in stack.saver.alist(_root(namespace))
            ]
            assert len(crash_checkpoints) == HANG_AFTER_CHECKPOINT
            crash_leaf = str(crash_checkpoints[0].config["configurable"]["checkpoint_id"])
            assert await lineage.get_transition("tenant-1", unit.unit_key, 1) is None
            assert [call["pid"] for call in _model_calls(paths)] == [killed_pid]

            activities = OperationExecutionActivities(
                stack.service, worker_identity=f"rrm004-worker-2:{os.getpid()}"
            )
            async with (
                Worker(
                    env.client,
                    task_queue=WORKFLOW_TASK_QUEUE,
                    workflows=[OperationWorkflow],
                    workflow_runner=coordinator_workflow_runner(),
                ),
                Worker(
                    env.client,
                    task_queue=stack.binding.task_queue,
                    activities=[activities.execute, activities.cancel],
                ),
            ):
                workflow_result = await asyncio.wait_for(handle.result(), timeout=240)
            history = await handle.fetch_history()

            result = parse_operation_result(workflow_result.result or {})
            assert workflow_result.disposition == "completed"
            assert result.status == "completed"
            assert result.structured_output == {"answer": RESULT_MARKER, "tool_results": 1}

            # Model invocations and prompts: one call before the loss, one after; each saw
            # exactly one human message; the tool ran once (its result was durable).
            calls = _model_calls(paths)
            assert [call["pid"] for call in calls] == [killed_pid, os.getpid()]
            assert [(call["human"], call["tools"]) for call in calls] == [(1, 0), (1, 1)]

            attempts = await lineage.list_attempts("tenant-1", unit.unit_key)
            assert [
                (item.attempt.attempt, item.dispatching, item.claim_fence) for item in attempts
            ] == [(1, True, 1), (2, True, 2)]
            assert attempts[0].attempt.worker_identity.startswith("rrm004-worker-1:")
            assert attempts[1].attempt.worker_identity.startswith("rrm004-worker-2:")
            assert attempts[1].expected_source is None

            transition = await lineage.get_transition("tenant-1", unit.unit_key, 1)
            assert transition is not None
            assert transition.classification == CheckpointClassification.INTERRUPTED
            assert transition.claim_fence == 2
            assert result.result_checkpoint == transition.result_key
            assert await lineage.get_namespace_head("tenant-1", namespace) == (
                transition.result_key
            )
            # Ancestry: the result descends from worker 1's last durable checkpoint, and
            # every root checkpoint of the lineage is this unit generation's submission.
            chain: list[Any] = []
            cursor = await stack.saver.aget_tuple(
                _root(namespace, transition.result_key.checkpoint_id)
            )
            while cursor is not None:
                chain.append(cursor)
                parent = cursor.parent_config
                cursor = (
                    await stack.saver.aget_tuple(
                        _root(namespace, parent["configurable"]["checkpoint_id"])
                    )
                    if parent is not None
                    else None
                )
            chain_ids = [str(item.config["configurable"]["checkpoint_id"]) for item in chain]
            assert crash_leaf in chain_ids
            assert {item.metadata[STAMP_INVOCATION_ID] for item in chain} == {
                submission_invocation_id(unit.unit_key, 1)
            }
            assert len([item async for item in stack.saver.alist(_root(namespace))]) == len(
                chain
            ), "no branch: the resume continued the pinned checkpoint"

            binding = bind_operation_execution_request(request)
            claim_id = _effect_claim_id(binding)
            async with stack.pool.acquire() as connection:
                technical = await connection.fetch(
                    "SELECT technical_attempt FROM belllabs_control.operation_execution_attempts"
                    " WHERE effect_claim_id = $1",
                    claim_id,
                )
                settlements = await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.operation_settlements"
                    " WHERE effect_claim_id = $1",
                    claim_id,
                )
            assert [row["technical_attempt"] for row in technical] == [2]
            assert settlements == 1

            # Duplicate delivery after recovery returns the identical settled result.
            replay = await stack.service.execute(
                request,
                attempts[1].attempt.model_copy(update={"attempt": 3}),
            )
            assert replay == result

        await Replayer(
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)
        scheduled = [
            event
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert len(scheduled) == 1, "the workflow scheduled one activity; Temporal retried it"
        print(
            "RRM-004 EVIDENCE worker restart:",
            json.dumps(
                {
                    "namespace_digest": namespace.split("/")[2][:24] + "...",
                    "worker_1_pid": killed_pid,
                    "worker_2_pid": os.getpid(),
                    "crash_after_root_checkpoint": HANG_AFTER_CHECKPOINT,
                    "attempts": [
                        {
                            "activity_attempt": item.attempt.attempt,
                            "claim_fence": item.claim_fence,
                            "dispatching": item.dispatching,
                        }
                        for item in attempts
                    ],
                    "classification": transition.classification.value,
                    "model_calls_by_worker": [
                        "worker-1" if call["pid"] == killed_pid else "worker-2" for call in calls
                    ],
                    "human_messages_per_call": [call["human"] for call in calls],
                    "stamped_root_checkpoints": len(chain),
                    "result_checkpoint_descends_from_crash_leaf": crash_leaf in chain_ids,
                    "technical_attempt": [row["technical_attempt"] for row in technical],
                    "settlements": settlements,
                    "result_manifest_digest": transition.result_manifest_digest,
                },
                sort_keys=True,
            ),
        )
