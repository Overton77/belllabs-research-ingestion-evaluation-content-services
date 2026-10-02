"""RRM-005 demonstration: inspect a tiny technical run and read a historical checkpoint.

One StageGraph unit runs through a real `OperationWorkflow` on a real Temporal dev server
(`WorkflowEnvironment.start_local`, BellLabs Search Attributes registered, policy
`required`). Cognition is a real `create_deep_agent` graph with the deterministic scripted
model (one `write_todos` tool call, then the answer) over the real `AsyncPostgresSaver`;
run control, the operation journal, and checkpoint lineage are application PostgreSQL;
the operation binding store is MongoDB. No company research and no live model.

The public inspection facade then lists the run, reads the run and the unit, lists the
unit's checkpoint history (at least two checkpoints), and reads a redacted summary of an
earlier, selected checkpoint. Every application table and every saver table is unchanged
by those reads. The captured workflow history replays.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` (disposable stack).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import pytest
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.runtime_inspection import get_runtime_inspection_service
from app.application.run_control.inspection import InspectionCursorCodec, RuntimeInspectionService
from app.application.run_control.postgres_inspection_repository import (
    PostgresInspectionReadRepository,
)
from app.domain.operation_execution.contracts import OperationWorkflowRequest
from app.domain.run_control.contracts import CommandStatus, StartAction
from app.integrations.agents.deep_agents.checkpoint_history import (
    LangGraphCheckpointHistoryReader,
)
from app.integrations.temporal_visibility import TemporalVisibilityInspectionReader
from app.server import api
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    operation_workflow_search_attributes,
    typed_search_attributes,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.operation import OperationWorkflow
from tests.fixtures import rrm004_persistent_stack
from tests.fixtures.checkpoint_recovery import RESULT_MARKER
from tests.fixtures.rrm004_persistent_stack import (
    WORKFLOW_TASK_QUEUE,
    StackPaths,
    open_persistent_stack,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.integration.postgres.test_runtime_inspection_postgres import _schema_digest
from tests.integration.temporal.test_rrm_004_worker_restart_recovery import _bound_request
from tests.unit.run_control.test_run_control import command
from tests.unit.run_control.test_run_control import request as run_request

SAVER_SCHEMA = "rrm005_inspection_saver"
PROMPT_MARKERS = ("Return BINDING-OK", "Use only the exact bound capabilities", RESULT_MARKER)


@pytest.fixture
async def mongo_database(test_mongodb_uri: str) -> AsyncIterator[str]:
    name = f"rrm005_inspection_{uuid4().hex[:12]}"
    yield name
    client: AsyncMongoClient[Any] = AsyncMongoClient(test_mongodb_uri)
    try:
        await client.drop_database(name)
    finally:
        await client.close()


async def _visible(client: Client, query: str, expected: int) -> None:
    """Visibility is eventually consistent: wait for the started execution to be listed."""

    async with asyncio.timeout(30):
        for _ in range(120):
            if (await client.count_workflows(query)).count == expected:
                return
            await asyncio.sleep(0.25)
    raise AssertionError(f"Visibility did not list {expected} execution(s) for {query}")


async def _saver_digest(pool: asyncpg.Pool) -> dict[str, str]:
    async with pool.acquire() as connection:
        tables = [
            row["table_name"]
            for row in await connection.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
                SAVER_SCHEMA,
            )
        ]
        return {
            table: await connection.fetchval(
                f"SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))"
                f" FROM {SAVER_SCHEMA}.{table} t"
            )
            for table in sorted(tables)
        }


@pytest.mark.asyncio
async def test_inspect_a_two_checkpoint_technical_run_and_read_a_historical_checkpoint(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    monkeypatch.setattr(rrm004_persistent_stack, "SAVER_SCHEMA", SAVER_SCHEMA)
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
        StackPaths(tmp_path),
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
    ) as stack:
        await stack.saver.setup()
        admitted = await stack.run_control.admit(run_request(request_id="rrm-005-demo"))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        started = await stack.run_control.execute(
            command(run_id, 1, "rrm-005-demo-start", StartAction())
        )
        assert started.status == CommandStatus.ACCEPTED
        request = await _bound_request(stack, run_id)
        unit = request.runtime_unit
        assert unit is not None and request.deep_agent_binding is not None
        workflow_request = OperationWorkflowRequest(
            semantic_attempt_id=request.identity.semantic_key,
            operation_kind="bound_operation",
            operation=request,
            timeout_seconds=60,
            search_attribute_policy="required",
        )

        async with await WorkflowEnvironment.start_local(
            search_attributes=BELLLABS_SEARCH_ATTRIBUTE_KEYS, dev_server_log_level="error"
        ) as env:
            activities = OperationExecutionActivities(
                stack.service, worker_identity=f"rrm005-worker:{os.getpid()}"
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
                    activities=[activities.execute],
                ),
            ):
                handle = await env.client.start_workflow(
                    OperationWorkflow.run,
                    workflow_request,
                    id=workflow_request.workflow_id,
                    task_queue=WORKFLOW_TASK_QUEUE,
                    search_attributes=typed_search_attributes(
                        operation_workflow_search_attributes(workflow_request)
                    ),
                )
                outcome = await asyncio.wait_for(handle.result(), timeout=180)
            assert outcome.disposition == "completed"
            history = await handle.fetch_history()
            await Replayer(
                workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
            ).replay_workflow(history)
            await _visible(env.client, f"BellLabsUnitKey = '{unit.unit_key}'", 1)

            inspection = RuntimeInspectionService(
                PostgresInspectionReadRepository(stack.pool),
                cursors=InspectionCursorCodec(b"rrm005-demo-cursor-key"),
                visibility=TemporalVisibilityInspectionReader(env.client),
                checkpoints=LangGraphCheckpointHistoryReader(
                    {stack.binding.checkpointer_ref.digest: stack.saver}
                ),
            )
            evidence = await _inspect(inspection, stack.pool, run_id, unit.unit_key)

    evidence["workflow_id"] = workflow_request.workflow_id
    evidence["replayed_events"] = len(history.events)
    print("RRM-005 EVIDENCE inspection:", json.dumps(evidence, sort_keys=True))


async def _inspect(
    inspection: RuntimeInspectionService, pool: asyncpg.Pool, run_id: str, unit_key: str
) -> dict[str, Any]:
    application_before = await _schema_digest(pool)
    saver_before = await _saver_digest(pool)
    params = {"request_scope": "tenant-1"}
    base = f"/run-control/v1/inspection/runs/{run_id}"
    api.dependency_overrides[get_runtime_inspection_service] = lambda: inspection
    api.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="operator",
        roles=frozenset({"auditor", "state_inspector"}),
        tenant_scopes=frozenset({"tenant-1"}),
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://inspection"
        ) as client:
            listed = (await client.get("/run-control/v1/inspection/runs", params=params)).json()
            run = (await client.get(base, params=params)).json()
            unit = (await client.get(f"{base}/units/{unit_key}", params=params)).json()
            history = (
                await client.get(f"{base}/units/{unit_key}/checkpoints", params=params)
            ).json()
            entries = history["data"]["entries"]
            # Select an earlier checkpoint: the one that recorded the model's tool call,
            # with the tool as its pending task, before the result checkpoint.
            selected = next(entry for entry in entries if "tools" in entry["pending_task_names"])
            summary_response = await client.get(
                f"{base}/units/{unit_key}/checkpoints/{selected['key']['checkpoint_id']}/summary",
                params=params,
            )
            texts = [
                json.dumps(listed),
                json.dumps(run),
                json.dumps(unit),
                json.dumps(history),
                summary_response.text,
            ]
    finally:
        api.dependency_overrides.pop(get_runtime_inspection_service, None)
        api.dependency_overrides.pop(get_control_plane_principal, None)
    assert await _schema_digest(pool) == application_before
    assert await _saver_digest(pool) == saver_before

    assert [item["run_id"] for item in listed["data"]["items"]] == [run_id]
    assert run["data"]["reconciliation_state"] == "none"
    assert [item["status"] for item in run["data"]["units"]] == ["settled"]
    assert run["sections"]["temporal"]["freshness"] == "current"
    executions = run["data"]["temporal_executions"]
    assert [(item["workflow_kind"], item["unit_key"], item["status"]) for item in executions] == [
        ("operation", unit_key, "COMPLETED")
    ]

    generation = unit["data"]["generations"][0]
    transition = generation["transition"]
    attempt = generation["attempts"][0]
    assert attempt["attempt"]["workflow_id"] == executions[0]["workflow_id"]
    assert attempt["attempt"]["workflow_run_id"] == executions[0]["temporal_run_id"]
    assert attempt["dispatching"] is True and attempt["claim_fence"] == 1
    assert generation["lease_state"] in {"expired", "released"}
    assert unit["data"]["status"] == "settled"
    journal = unit["data"]["journal"][0]
    assert [item["technical_attempt"] for item in journal["technical_attempts"]] == [1]
    settlement = journal["settlements"][0]
    assert settlement["status"] == "completed"
    assert settlement["result_manifest_digest"] == transition["result_manifest_digest"]
    assert generation["result"]["checkpoint_transition_id"] == transition["transition_id"]
    assert unit["sections"]["journal"]["source"] == "postgres_authority"

    assert history["sections"]["checkpoints"]["freshness"] == "current"
    assert len(entries) >= 2
    assert all(
        entry["stamped"] and entry["binding_compatible"] and entry["state_schema_compatible"]
        for entry in entries
    )
    assert entries[-1]["key"]["checkpoint_id"] == transition["result_key"]["checkpoint_id"]
    assert set(entries[-1]["roles"]) == {"result", "namespace_head"}
    parents = [entry["key"]["parent_checkpoint_id"] for entry in entries[1:]]
    assert parents == [entry["key"]["checkpoint_id"] for entry in entries[:-1]]
    assert selected["key"]["checkpoint_id"] != entries[-1]["key"]["checkpoint_id"]

    assert summary_response.status_code == 200
    summary = summary_response.json()
    facts = summary["data"]["facts"]
    # The human input and the model's tool-call message (counted, never shown).
    assert facts["message_count"] == 2
    assert "messages" in facts["channel_names"]
    assert summary["data"]["pending_task_names"] == selected["pending_task_names"]
    assert summary["data"]["stamped_digests"]["belllabs_unit_key"] == unit_key
    assert summary["sections"]["checkpoint"]["redaction"]["withheld_field_count"] >= len(
        facts["channel_names"]
    )
    for text in texts:
        for marker in PROMPT_MARKERS:
            assert marker not in text, marker
    return {
        "run_phase": run["data"]["projection"]["phase"],
        "unit_status": unit["data"]["status"],
        "temporal_join": [executions[0]["workflow_kind"], executions[0]["status"]],
        "attempt": [attempt["attempt"]["attempt"], attempt["claim_fence"]],
        "history": [
            {
                "step": entry["step"],
                "roles": entry["roles"],
                "pending": entry["pending_task_names"],
            }
            for entry in entries
        ],
        "selected_step": selected["step"],
        "summary_facts": {
            "channel_names": facts["channel_names"],
            "message_count": facts["message_count"],
            "todo_count": facts["todo_count"],
            "has_structured_response": facts["has_structured_response"],
        },
        "summary_digest": summary["data"]["summary_digest"],
        "settlement": settlement["status"],
        "tables_unchanged": True,
    }
