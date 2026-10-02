"""RRM-013 live qualification: real async subagents on the persistent local Agent Server.

Opt-in: `BELLABS_RUN_RRM_013_LIVE=1`, `AGENT_SERVER_ENDPOINT`,
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI`
(disposable stack only). The child is the
hosted technical child, which calls the real model on tiny objectives; the parent's cognition
is a deterministic scripted model whose only tool call is `start_async_task`.

Requirements: REQ-CP-DA-008 (reservation and link before submission, one provider run per
child, `in_doubt` and its exits), REQ-CP-DA-011 (qualified checkpoint, attributed usage,
admission, late results), REQ-CP-DA-019 (served identity), REQ-CP-RUN-009 (parent budget),
REQ-CP-EXEC-008 (cancel acknowledgement) and the RRM-013 crash-window and restart drills.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from langgraph_sdk import get_client
from langgraph_sdk.client import LangGraphClient
from pymongo import AsyncMongoClient

from app.application.async_subagents.parent_effects import (
    async_child_effect_id,
    async_child_usage_id,
)
from app.application.async_subagents.service import AsyncSubagentError
from app.application.operations.operation_execution import OperationExecutionInProgress
from app.config import PROJECT_ROOT
from app.domain.operation_execution.async_subagent_reconciliation import (
    ASYNC_SUBAGENT_INCIDENT_TYPE,
)
from app.domain.operation_execution.contracts import (
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
)
from app.domain.run_control.contracts import EffectDisposition
from app.integrations.agents.deep_agents.async_subagents import (
    REQUEST_SCOPE_HEADER,
    SPAWN_KEY_METADATA,
)
from tests.fixtures.rrm013_live_stack import (
    SAVER_SCHEMA,
    SCOPE,
    TOKEN_ENV,
    LiveStack,
    activity_attempt,
    admit_parent_run,
    bound_parent_request,
    live_opt_in,
    open_live_stack,
    spawn_request_for,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)

_OPTED_IN, _REASON = live_opt_in()
pytestmark = pytest.mark.skipif(not _OPTED_IN, reason=_REASON)

CHILD_TIMEOUT_SECONDS = 240
API_CONTAINER = os.getenv(
    "RRM013_AGENT_SERVER_API_CONTAINER", "rrm013-agent-server-langgraph-api-1"
)
TERMINAL = {
    AsyncSubagentLifecycle.COMPLETED,
    AsyncSubagentLifecycle.FAILED,
    AsyncSubagentLifecycle.CANCELLED,
    AsyncSubagentLifecycle.ORPHANED,
}


def _evidence(label: str, payload: dict[str, Any]) -> None:
    print(f"RRM-013 EVIDENCE {label}: {json.dumps(payload, sort_keys=True, default=str)}")


@pytest.fixture
async def mongo_database(test_mongodb_uri: str) -> AsyncIterator[str]:
    name = f"rrm013_live_{uuid4().hex[:12]}"
    yield name
    client: AsyncMongoClient[Any] = AsyncMongoClient(test_mongodb_uri)
    try:
        await client.drop_database(name)
    finally:
        await client.close()


@pytest.fixture
async def fresh_schemas(test_application_postgres_dsn: str) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {SAVER_SCHEMA} CASCADE")
            await connection.execute(f"CREATE SCHEMA {SAVER_SCHEMA}")
    finally:
        await owner.close()


@pytest.fixture
def results_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "results"
    root.mkdir()
    monkeypatch.setenv("RRM013_RESULTS", str(root))
    return root


def sdk_client() -> LangGraphClient:
    return get_client(
        url=os.environ["AGENT_SERVER_ENDPOINT"].rstrip("/"),
        headers={
            "Authorization": f"Bearer {os.environ[TOKEN_ENV]}",
            REQUEST_SCOPE_HEADER: SCOPE,
        },
    )


async def provider_runs(child_id: str) -> list[dict[str, Any]]:
    runs = await sdk_client().runs.list(child_id, limit=100)
    return [run for run in runs if (run.get("metadata") or {}).get(SPAWN_KEY_METADATA) == child_id]


async def await_lifecycle(
    stack: LiveStack,
    child_id: str,
    predicate: Callable[[AsyncSubagentExecution], bool],
    *,
    limit_seconds: float = CHILD_TIMEOUT_SECONDS,
) -> AsyncSubagentExecution:
    deadline = time.monotonic() + limit_seconds
    last: AsyncSubagentExecution | None = None
    while time.monotonic() < deadline:
        last = await stack.async_subagents.reconcile(SCOPE, child_id)
        if predicate(last):
            return last
        await asyncio.sleep(2)
    raise AssertionError(f"child {child_id} did not reach the expected lifecycle; last={last}")


async def await_provider_status(
    child_id: str, run_id: str, statuses: set[str], limit_seconds: float = 90
) -> str:
    client = sdk_client()
    deadline = time.monotonic() + limit_seconds
    last = ""
    while time.monotonic() < deadline:
        last = str((await client.runs.get(child_id, run_id)).get("status") or "")
        if last in statuses:
            return last
        await asyncio.sleep(1)
    raise AssertionError(f"run {run_id} stayed {last!r}")


async def facts(dsn: str, child_id: str) -> list[tuple[str, str]]:
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT fact_kind, fact_ref FROM belllabs_control.async_subagent_facts
                   WHERE child_execution_id = $1 ORDER BY recorded_at, fact_id""",
                child_id,
            )
            return [(row["fact_kind"], row["fact_ref"]) for row in rows]
    finally:
        await pool.close()


async def incident_rows(dsn: str, child_id: str) -> list[dict[str, Any]]:
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT status, incident_payload
                   FROM belllabs_control.runtime_reconciliation_incidents
                   WHERE incident_type = $1 AND incident_payload->>'child_execution_id' = $2""",
                ASYNC_SUBAGENT_INCIDENT_TYPE,
                child_id,
            )
            return [
                {"status": row["status"], **json.loads(row["incident_payload"])} for row in rows
            ]
    finally:
        await pool.close()


def trace_correlation(child_id: str, parent_binding_id: str) -> dict[str, Any]:
    """Subordinate evidence: LangSmith traces of parent and child share BellLabs identities.

    The child's Agent Server run carries `belllabs_child_execution_id` and
    `belllabs_parent_binding_id` in its metadata (the server copies run metadata onto the
    trace); the parent's `operation.execute` trace carries `binding_id`. Ingestion is
    asynchronous, so this polls briefly and records what it saw; it never gates the test.
    """

    if not os.getenv("LANGSMITH_API_KEY"):
        return {"available": False, "reason": "LANGSMITH_API_KEY not set"}
    try:
        from langsmith import Client
    except Exception as error:  # noqa: BLE001 - subordinate evidence only
        return {"available": False, "reason": f"langsmith client unavailable: {error}"}
    project = os.getenv("LANGSMITH_PROJECT", "BellLabsBiotech-AsyncSubagents-Local")
    client = Client()
    child_filter = f'has(metadata, \'{{"belllabs_child_execution_id": "{child_id}"}}\')'
    parent_filter = f'has(metadata, \'{{"binding_id": "{parent_binding_id}"}}\')'
    child_runs: list[Any] = []
    parent_runs: list[Any] = []
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and not (child_runs and parent_runs):
        try:
            child_runs = list(
                client.list_runs(project_name=project, filter=child_filter, is_root=True, limit=5)
            )
            parent_runs = list(
                client.list_runs(project_name=project, filter=parent_filter, is_root=True, limit=5)
            )
        except Exception as error:  # noqa: BLE001 - subordinate evidence only
            return {"available": False, "reason": f"LangSmith query failed: {type(error).__name__}"}
        if not (child_runs and parent_runs):
            time.sleep(5)
    return {
        "available": True,
        "project": project,
        "child_root_runs": [str(run.id) for run in child_runs],
        "child_parent_binding_ids": sorted(
            {
                str((run.extra or {}).get("metadata", {}).get("belllabs_parent_binding_id"))
                for run in child_runs
            }
        ),
        "parent_root_runs": [str(run.id) for run in parent_runs],
    }


async def settle_child(stack: LiveStack, child_id: str, *, admit: bool) -> None:
    now = datetime.now(UTC)
    if admit:
        await stack.async_subagents.decide_result(
            SCOPE, child_id, "admit", parent_open=True, current_generation=1, decided_at=now
        )
    await stack.async_subagents.settle(SCOPE, child_id, f"settlement:{child_id}", now)


# --------------------------------------------------------------------------- real spawn path


@pytest.mark.asyncio
async def test_parent_deep_agent_spawns_one_real_child_and_admits_its_result(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    fresh_schemas: None,
    results_root: Path,
) -> None:
    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
    ) as stack:
        await stack.saver.setup()
        run_id = await admit_parent_run(stack, "rrm-013-spawn")
        request = await bound_parent_request(stack, run_id)
        started = time.monotonic()
        result = await stack.service.execute(request, activity_attempt(request, 1))
        assert result.status == "completed", result
        assert result.structured_output is not None
        assert "Launched async subagent. task_id:" in str(result.structured_output["spawned"])

        views = await stack.authority.list_children(SCOPE, run_id)
        assert len(views) == 1
        view = views[0]
        child_id = view.child_execution_id
        assert str(result.structured_output["spawned"]).endswith(child_id)
        # REQ-CP-DA-008: Mongo detail, PostgreSQL authority and the parent's effect claim all
        # exist, and the provider holds exactly one run carrying the spawn key.
        execution = await stack.async_subagents.execution(SCOPE, child_id)
        assert execution.lifecycle in {
            AsyncSubagentLifecycle.SUBMITTED,
            AsyncSubagentLifecycle.RUNNING,
            AsyncSubagentLifecycle.COMPLETED,
        }
        assert view.parent_binding_id == result.binding_id
        assert view.graph_binding_digest == stack.contract.graph_binding_digest
        assert view.submission_fence == 1 and view.submission_holder is None
        effects = await stack.run_control.get_effects(SCOPE, run_id)
        claim = effects.claims[async_child_effect_id(child_id)]
        assert claim.operation_ref == result.binding_id
        assert claim.effect_kind == "async_subagent.child"
        projection = await stack.run_control.get_run(SCOPE, run_id)
        assert [item.child_execution_id for item in projection.async_children] == [child_id]
        runs = await provider_runs(child_id)
        assert len(runs) == 1
        assert runs[0]["run_id"] == execution.provider_run_id
        assert execution.provider_thread_id == child_id

        completed = await await_lifecycle(stack, child_id, lambda item: item.lifecycle in TERMINAL)
        assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED, completed
        manifest = completed.result_manifest
        assert manifest is not None
        served = stack.contract
        assert manifest.provider_checkpoint.graph_id == served.graph_id
        assert manifest.provider_checkpoint.graph_revision == served.graph_revision
        assert manifest.provider_checkpoint.graph_binding_digest == served.graph_binding_digest
        assert manifest.provider_checkpoint.thread_id == child_id
        assert manifest.provider_checkpoint.checkpoint_id
        assert manifest.usage.attribution == "provider_attributed"
        assert manifest.usage.attributed_amounts["tokens.total"] > 0
        assert set(manifest.usage.attributed_amounts) <= set(stack.contract.budget_limits)
        assert "PONG" in (completed.result_output_text or "")
        assert len(await provider_runs(child_id)) == 1

        # Stock middleware tools against the real server: check and list.
        checked = await stack.provider.check(stack.contract, completed)
        assert checked.status == "success" and checked.run_id == completed.provider_run_id
        listed = await stack.provider.list(((stack.contract, completed),))
        assert listed[0].status == "success"

        # REQ-CP-DA-011: a late result cannot mutate a settled parent; it is recorded.
        with pytest.raises(AsyncSubagentError, match="late or superseded"):
            await stack.async_subagents.decide_result(
                SCOPE,
                child_id,
                "admit",
                parent_open=False,
                current_generation=1,
                decided_at=datetime.now(UTC),
            )
        recorded = await facts(test_application_postgres_dsn, child_id)
        assert any(kind == "result" and ref.startswith("late_rejected:") for kind, ref in recorded)

        await settle_child(stack, child_id, admit=True)
        budget = await stack.run_control.get_budget(SCOPE, run_id)
        usage = budget.usage_records[async_child_usage_id(child_id)]
        assert (
            usage.actual_amounts["tokens.total"]
            == manifest.usage.attributed_amounts["tokens.total"]
        )
        assert not usage.pending_external_amounts
        effects = await stack.run_control.get_effects(SCOPE, run_id)
        settled = effects.claims[async_child_effect_id(child_id)]
        assert settled.disposition == EffectDisposition.SUCCEEDED
        final_view = (await stack.authority.list_children(SCOPE, run_id))[0]
        assert final_view.lifecycle == AsyncSubagentLifecycle.COMPLETED
        assert final_view.result_decision == "admit"
        assert final_view.settlement_ref == f"settlement:{child_id}"
        assert [record.disposition for record in final_view.provider_runs] == ["bound"]

        # A redelivered parent attempt returns the settled result: no second child, no run.
        replay = await stack.service.execute(request, activity_attempt(request, 2))
        assert replay.status == "completed" and replay.binding_id == result.binding_id
        assert len(await stack.authority.list_children(SCOPE, run_id)) == 1
        assert len(await provider_runs(child_id)) == 1
        correlation = trace_correlation(child_id, result.binding_id)
        _evidence(
            "spawn",
            {
                "child_execution_id": child_id,
                "provider_run_id": completed.provider_run_id,
                "provider_runs": 1,
                "trace_correlation": correlation,
                "graph": manifest.provider_checkpoint.model_dump(mode="json"),
                "usage": manifest.usage.model_dump(mode="json"),
                "manifest_digest": manifest.manifest_digest,
                "parent_binding_id": result.binding_id,
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "parent_model_calls": 2,
            },
        )


# --------------------------------------------------------------------------- cancellation


@pytest.mark.asyncio
async def test_parent_cancel_reaches_the_provider_run_and_records_the_acknowledgement(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    fresh_schemas: None,
    results_root: Path,
) -> None:
    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
    ) as stack:
        run_id = await admit_parent_run(stack, "rrm-013-cancel")
        request = await bound_parent_request(stack, run_id, stage="cancel")
        spawn = spawn_request_for(
            stack,
            "binding:rrm-013-cancel",
            run_id=run_id,
            objective="Call wait_seconds with seconds=60, then reply with exactly PONG.",
            key="cancel-drill",
            reservation_id="reservation:rrm-013-cancel-child",
            parent_reservation_id=request.budget_reservation_id,
        )
        child = await stack.async_subagents.spawn(spawn)
        assert child.provider_run_id is not None
        await await_provider_status(child.child_execution_id, child.provider_run_id, {"running"})
        cancelled = await stack.async_subagents.cancel(
            SCOPE, child.child_execution_id, "parent cancelled the drill", datetime.now(UTC)
        )
        link = await stack.async_subagents.link(SCOPE, child.child_execution_id)
        status = str(
            (await sdk_client().runs.get(child.child_execution_id, child.provider_run_id)).get(
                "status"
            )
        )
        assert link.cancellation_requested
        assert link.cancellation_receipt in {"provider_acknowledged", "ambiguous"}
        if status in {"interrupted", "cancelled"}:
            assert link.cancellation_receipt == "provider_acknowledged"
            assert cancelled.lifecycle == AsyncSubagentLifecycle.CANCELLED
        recorded = await facts(test_application_postgres_dsn, child.child_execution_id)
        assert ("cancellation", link.cancellation_receipt) in recorded
        assert len(await provider_runs(child.child_execution_id)) == 1
        if cancelled.lifecycle in TERMINAL:
            await settle_child(stack, child.child_execution_id, admit=False)
            effects = await stack.run_control.get_effects(SCOPE, run_id)
            assert effects.claims[async_child_effect_id(child.child_execution_id)].disposition in {
                EffectDisposition.CANCELLED,
                EffectDisposition.FAILED,
                EffectDisposition.SUCCEEDED,
            }
        _evidence(
            "cancel",
            {
                "child_execution_id": child.child_execution_id,
                "provider_run_id": child.provider_run_id,
                "provider_status_after_cancel": status,
                "cancellation_receipt": link.cancellation_receipt,
                "lifecycle": cancelled.lifecycle.value,
            },
        )


# --------------------------------------------------------------------------- in_doubt


@pytest.mark.asyncio
async def test_duplicate_provider_run_is_in_doubt_until_adopt_provider_run(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    fresh_schemas: None,
    results_root: Path,
) -> None:
    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
    ) as stack:
        run_id = await admit_parent_run(stack, "rrm-013-in-doubt")
        request = await bound_parent_request(stack, run_id, stage="in-doubt")
        spawn = spawn_request_for(
            stack,
            "binding:rrm-013-in-doubt",
            run_id=run_id,
            objective="Call wait_seconds with seconds=25, then reply with exactly PONG.",
            key="in-doubt-drill",
            reservation_id="reservation:rrm-013-in-doubt-child",
            parent_reservation_id=request.budget_reservation_id,
        )
        child = await stack.async_subagents.spawn(spawn)
        child_id = child.child_execution_id
        assert child.provider_run_id is not None
        # A second run carrying the spawn key: the ambiguity a lost fence could leave behind.
        duplicate = await sdk_client().runs.create(
            child_id,
            stack.contract.graph_id,
            input={"messages": [{"role": "user", "content": "duplicate"}]},
            metadata={SPAWN_KEY_METADATA: child_id, "request_scope": SCOPE},
            multitask_strategy="enqueue",
        )
        in_doubt = await stack.async_subagents.reconcile(SCOPE, child_id)
        assert in_doubt.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
        assert in_doubt.in_doubt_reason == "multiple_provider_runs"
        incidents = await incident_rows(test_application_postgres_dsn, child_id)
        assert [item["status"] for item in incidents] == ["operator_required"]
        assert set(incidents[0]["candidate_run_ids"]) == {
            child.provider_run_id,
            duplicate["run_id"],
        }
        effects = await stack.run_control.get_effects(SCOPE, run_id)
        assert effects.claims[async_child_effect_id(child_id)].disposition == (
            EffectDisposition.AMBIGUOUS
        )
        # Observation alone cannot resolve two runs; the spawn path never adds a third.
        assert (
            await stack.async_subagents.spawn(spawn)
        ).lifecycle == AsyncSubagentLifecycle.IN_DOUBT
        assert len(await provider_runs(child_id)) == 2

        adopted = await stack.async_subagents.reconcile_in_doubt(
            SCOPE,
            child_id,
            "adopt_provider_run",
            decision_id=f"decision:{child_id}:adopt",
            run_id=child.provider_run_id,
            reason="operator adopts the fenced submission's run",
            decided_at=datetime.now(UTC),
        )
        assert adopted.lifecycle not in {AsyncSubagentLifecycle.IN_DOUBT}
        assert adopted.provider_run_id == child.provider_run_id
        duplicate_status = await await_provider_status(
            child_id,
            duplicate["run_id"],
            {"interrupted", "cancelled", "error", "success"},
            limit_seconds=30,
        )
        incidents = await incident_rows(test_application_postgres_dsn, child_id)
        assert incidents[0]["status"] == "resolved"
        assert incidents[0]["resolution"] == "adopt_provider_run"
        completed = await await_lifecycle(stack, child_id, lambda item: item.lifecycle in TERMINAL)
        view = (await stack.authority.list_children(SCOPE, run_id))[0]
        dispositions = {record.provider_run_id: record.disposition for record in view.provider_runs}
        assert dispositions[child.provider_run_id] == "bound"
        assert dispositions[duplicate["run_id"]] == "duplicate_cancelled"
        pending = next(
            record for record in view.provider_runs if record.provider_run_id == duplicate["run_id"]
        )
        assert pending.usage.attribution == "pending"
        _evidence(
            "in_doubt_adopt",
            {
                "child_execution_id": child_id,
                "adopted_run_id": child.provider_run_id,
                "duplicate_run_id": duplicate["run_id"],
                "duplicate_status_after_adopt": duplicate_status,
                "final_lifecycle": completed.lifecycle.value,
                "incident_id": incidents[0]["incident_id"],
            },
        )


# --------------------------------------------------------------------------- server restart


@pytest.mark.asyncio
async def test_agent_server_restart_during_an_active_child_is_reconciled_from_durable_state(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    fresh_schemas: None,
    results_root: Path,
) -> None:
    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
    ) as stack:
        run_id = await admit_parent_run(stack, "rrm-013-restart")
        request = await bound_parent_request(stack, run_id, stage="restart")
        spawn = spawn_request_for(
            stack,
            "binding:rrm-013-restart",
            run_id=run_id,
            objective="Call wait_seconds with seconds=45, then reply with exactly PONG.",
            key="restart-drill",
            reservation_id="reservation:rrm-013-restart-child",
            parent_reservation_id=request.budget_reservation_id,
        )
        child = await stack.async_subagents.spawn(spawn)
        child_id = child.child_execution_id
        assert child.provider_run_id is not None
        await await_provider_status(child_id, child.provider_run_id, {"running"})
        restart_started = time.monotonic()
        _restart_api_container()
        client = sdk_client()
        deadline = time.monotonic() + 180
        while True:
            try:
                await client.http.get("/ok")
                break
            except Exception:  # noqa: BLE001 - the server is restarting
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(2)
        restart_seconds = round(time.monotonic() - restart_started, 1)
        terminal = await await_lifecycle(stack, child_id, lambda item: item.lifecycle in TERMINAL)
        runs = await provider_runs(child_id)
        assert len(runs) == 1
        assert runs[0]["run_id"] == child.provider_run_id
        recorded = await facts(test_application_postgres_dsn, child_id)
        assert ("lifecycle", terminal.lifecycle.value) in recorded or any(
            kind == "result" for kind, _ref in recorded
        )
        if terminal.lifecycle == AsyncSubagentLifecycle.COMPLETED:
            assert terminal.result_manifest is not None
            assert "PONG" in (terminal.result_output_text or "")
        _evidence(
            "server_restart",
            {
                "child_execution_id": child_id,
                "provider_run_id": child.provider_run_id,
                "provider_runs": len(runs),
                "provider_status": runs[0]["status"],
                "lifecycle": terminal.lifecycle.value,
                "restart_to_ready_seconds": restart_seconds,
            },
        )


# --------------------------------------------------------------------------- crash windows


def _restart_api_container() -> None:
    """Restart only the API container of the RRM-013 stack; Postgres and Redis stay up."""

    subprocess.run(["docker", "restart", API_CONTAINER], check=True, capture_output=True)


def _spawn_worker(
    root: Path,
    mongo_database: str,
    objective: str,
    window: str,
    lease_seconds: int,
    results_root: Path,
) -> subprocess.Popen[bytes]:
    """Worker 1 runs in its own OS process so that it can be killed like a lost host."""

    return subprocess.Popen(
        [sys.executable, "-m", "tests.fixtures.rrm013_child_worker"],
        cwd=PROJECT_ROOT,
        env={
            **os.environ,
            "RRM013_ROOT": str(root),
            "RRM013_MONGO_DATABASE": mongo_database,
            "RRM013_OBJECTIVE": objective,
            "RRM013_CRASH_WINDOW": window,
            "RRM013_LEASE_SECONDS": str(lease_seconds),
            "RRM013_RESULTS": str(results_root),
            "PYTHONHASHSEED": "7",
        },
        stdout=subprocess.DEVNULL,
        stderr=(root / "worker-1.log").open("wb"),
    )


def _wait_for(path: Path, process: subprocess.Popen[bytes], seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            raise AssertionError(f"worker 1 exited early with {process.returncode}")
        time.sleep(0.2)
    raise AssertionError(f"{path.name} did not appear")


def _model_calls(path: Path) -> list[dict[str, int]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


@pytest.mark.asyncio
@pytest.mark.parametrize("window", ["before_submit", "after_submit"])
async def test_worker_killed_in_the_submission_window_recovers_with_one_provider_run(
    window: str,
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    fresh_schemas: None,
    results_root: Path,
    tmp_path: Path,
) -> None:
    root = tmp_path / "worker"
    root.mkdir()
    objective = "Reply with exactly the word PONG."
    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
        objective=objective,
    ) as stack:
        await stack.saver.setup()
        run_id = await admit_parent_run(stack, f"rrm-013-crash-{window}")
        request = await bound_parent_request(stack, run_id, stage=f"crash-{window}")
    (root / "request.json").write_text(
        json.dumps(request.model_dump(mode="json")), encoding="utf-8"
    )
    lease_seconds = 20
    worker = _spawn_worker(root, mongo_database, objective, window, lease_seconds, results_root)
    try:
        _wait_for(root / "worker-1-ready", worker, 180)
        _wait_for(root / "crash-marker", worker, 180)
    finally:
        worker.kill()
        worker.wait(timeout=60)
    killed_pid = int((root / "crash-marker").read_text(encoding="utf-8"))

    async with open_live_stack(
        test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        endpoint=os.environ["AGENT_SERVER_ENDPOINT"],
        objective=objective,
        model_log=root / "model-calls.jsonl",
        submitter_identity=f"rrm013-worker-2:{os.getpid()}",
    ) as stack:
        views = await stack.authority.list_children(SCOPE, run_id)
        assert len(views) == 1
        child_id = views[0].child_execution_id
        crash_state = await stack.async_subagents.execution(SCOPE, child_id)
        assert crash_state.lifecycle == AsyncSubagentLifecycle.ADMITTED
        assert crash_state.provider_run_id is None
        runs_at_crash = await provider_runs(child_id)
        assert len(runs_at_crash) == (0 if window == "before_submit" else 1)
        assert views[0].submission_holder is not None  # the lost worker never released it
        effects = await stack.run_control.get_effects(SCOPE, run_id)
        assert async_child_effect_id(child_id) in effects.claims

        # Worker 2: Temporal would redeliver after the lease; retry until the lease expires.
        deadline = time.monotonic() + lease_seconds + 60
        attempt = 2
        while True:
            try:
                result = await stack.service.execute(request, activity_attempt(request, attempt))
                break
            except OperationExecutionInProgress:
                if time.monotonic() > deadline:
                    raise
                attempt += 1
                await asyncio.sleep(3)
        assert result.status == "completed", result
        recovered = await stack.async_subagents.execution(SCOPE, child_id)
        assert recovered.lifecycle in {
            AsyncSubagentLifecycle.SUBMITTED,
            AsyncSubagentLifecycle.RUNNING,
            AsyncSubagentLifecycle.COMPLETED,
        }
        runs = await provider_runs(child_id)
        assert len(runs) == 1
        if window == "after_submit":
            assert runs[0]["run_id"] == runs_at_crash[0]["run_id"]
        assert recovered.provider_run_id == runs[0]["run_id"]
        view = (await stack.authority.list_children(SCOPE, run_id))[0]
        assert view.submission_fence == 2 and view.submission_holder is None
        calls = _model_calls(root / "model-calls.jsonl")
        assert [call["pid"] for call in calls] == [killed_pid, os.getpid()]
        assert [call["tool_messages"] for call in calls] == [0, 1]
        completed = await await_lifecycle(stack, child_id, lambda item: item.lifecycle in TERMINAL)
        assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED, completed
        assert "PONG" in (completed.result_output_text or "")
        assert len(await provider_runs(child_id)) == 1
        await settle_child(stack, child_id, admit=True)
        _evidence(
            f"crash_window_{window}",
            {
                "child_execution_id": child_id,
                "provider_runs_at_crash": len(runs_at_crash),
                "provider_runs_after_recovery": 1,
                "provider_run_id": completed.provider_run_id,
                "submission_fence": view.submission_fence,
                "model_calls": calls,
                "worker_1_pid": killed_pid,
                "worker_2_pid": os.getpid(),
                "recovery_attempt": attempt,
            },
        )
