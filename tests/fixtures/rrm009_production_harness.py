"""Shared inspection and control helpers for PostgreSQL production qualifications.

The RRM-009 qualifications (composition, live capabilities,
object store, cancellation drill) share this harness; the technical models, catalog and
templates it runs are in `tests.fixtures.rrm009_production_stack`.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import asyncpg
import httpx
import pytest
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.rrm009_production_stack import (
    LANGGRAPH_SCHEMA,
    OPERATOR,
    REPORT_PATH,
    SCOPE,
    WAIT_ID,
    TechnicalBinding,
    TechnicalCatalog,
    admission_request,
    publish_technical_catalog,
    stage_templates,
)

from mission_control.adapters.operations.runtime_ports import (
    FilesystemArtifactPayloadStore,
)
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.worker import (
    WorkerActivityComposition,
)
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import (
    StageGraphWorkflow,
    wait_condition_id,
)
from mission_control.application.artifacts.artifact_promotion import (
    ArtifactPayloadAddress,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.bootstrap.settings import Settings
from mission_control.bootstrap.technical_api import api
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.execution.contracts import (
    ArtifactPromotionPlan,
    GenericArtifactWorkflowRequest,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    LifecycleCommand,
    ReserveBudgetAction,
    SatisfyWaitAction,
    StartAction,
)
from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
)

TEMPORAL_PORT = 7341
PRINCIPAL = ControlPlanePrincipal(
    actor_id=OPERATOR,
    roles=frozenset({"operator", "fork_operator", "relay", "auditor", "state_inspector"}),
    tenant_scopes=frozenset({SCOPE}),
    authority_refs=frozenset({"authority:lifecycle"}),
    sponsorship_refs=frozenset({"sponsorship:test"}),
    approval_refs=frozenset({"approval:test"}),
)
STAGE_OBJECTIVE = "Review the draft against the stricter technical checklist."
API_STATE_ATTRIBUTES = (
    "run_control_service",
    "boundary_intervention_service",
    "unit_reconciliation_service",
    "runtime_inspection_service",
    "run_fork_services",
    "admission_policy_registry",
    "run_control_family_admission_registry",
    "control_plane_service",
    "run_launch_service",
    "generic_artifact_submitter",
    "runtime_control",
    "boundary_command_relay",
    "workflow_submitter",
    "fork_patch_policies",
    "temporal_client",
    "temporal_visibility_reader",
    "inspection_checkpoint_reader",
    "inspection_async_child_details",
    "inspection_cursor_key",
    "boundary_command_transport",
    "unit_reconciliation_nudge",
    "unit_reconciliation_verifier",
    "family_liability_hints",
    "async_child_usage_reconciliation",
)


@dataclass
class ProductionStack:
    settings: Settings
    env: WorkflowEnvironment
    client: Client
    owner_pool: asyncpg.Pool
    worker_pool: asyncpg.Pool
    control_plane: ControlPlaneService
    technical: TechnicalBinding
    composition: WorkerActivityComposition
    factory: ProductionWorkerActivityCompositionFactory
    http: httpx.AsyncClient
    payload_root: Path
    temporal_db: Path
    model_log: list[dict[str, Any]] = field(default_factory=list)
    worker_stack: AsyncExitStack = field(default_factory=AsyncExitStack)
    worker_queues: tuple[str, ...] = ()
    database: CommonDatabase | None = None

    async def restart_temporal(self) -> None:
        """Stop the dev server and start it again on the same port and database file."""

        await self.env.shutdown()
        self.env = await _start_local(self.temporal_db)


async def _start_local(database: Path) -> WorkflowEnvironment:
    try:
        # No Search Attributes at start: the deployment's administrative step registers them
        # (REQ-CP-EXEC-015), and the database file keeps them across the restart drill.
        return await WorkflowEnvironment.start_local(
            port=TEMPORAL_PORT,
            dev_server_extra_args=["--db-filename", str(database)],
            dev_server_log_level="error",
        )
    except RuntimeError as error:
        pytest.skip(f"Temporal dev server is unavailable: {error}")


def _reset_api_state() -> None:
    for name in API_STATE_ATTRIBUTES:
        if hasattr(api.state, name):
            delattr(api.state, name)


# --- Facade helpers --------------------------------------------------------------------------


async def _until(predicate: Callable[[], Awaitable[bool]], seconds: float = 180) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 4)):
            if await predicate():
                return
            await asyncio.sleep(0.25)
    raise AssertionError("condition not reached")


async def _diagnose(stack: ProductionStack, run_id: str) -> str:
    """Compact history tails of a run's root, family and operations plus the model log
    (printed on failure only, before the dev server is torn down)."""

    lines: list[str] = [f"model_log={json.dumps(stack.model_log)[:2000]}"]
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    lines.append(f"budget={budget.text[:3000]}")
    async for execution in stack.client.list_workflows(f"BellLabsRunId = '{run_id}'"):
        try:
            history = await stack.client.get_workflow_handle(execution.id).fetch_history()
        except Exception as error:
            lines.append(f"{execution.id}: {type(error).__name__}")
            continue
        lines.append(
            f"{execution.id} [{execution.status}]: "
            + ", ".join(str(event.event_type) for event in history.events[-6:])
        )
        for event in history.events:
            if event.event_type == 10:  # ActivityTaskScheduled: the queue the worker must poll
                attributes = event.activity_task_scheduled_event_attributes
                pollers = await stack.client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(
                        namespace=stack.client.namespace,
                        task_queue=TaskQueue(name=attributes.task_queue.name),
                        task_queue_type=TaskQueueType.Value("TASK_QUEUE_TYPE_ACTIVITY"),
                    )
                )
                lines.append(
                    f"scheduled {attributes.activity_type.name} on {attributes.task_queue.name} "
                    f"(activity pollers: {len(pollers.pollers)})"
                )
            if event.event_type in {3, 14, 24, 26, 42}:  # failed runs, activities and tasks
                lines.append(str(event)[:900])
    return " | ".join(lines)


async def _wait_for(
    stack: ProductionStack, run_id: str, predicate: Callable[[], Awaitable[bool]], seconds: float
) -> None:
    try:
        await _until(predicate, seconds)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(stack, run_id)}") from error


async def _admit(stack: ProductionStack, catalog: TechnicalCatalog, request_id: str) -> str:
    response = await stack.http.post(
        "/run-control/v1/run-requests",
        json=admission_request(catalog, request_id).model_dump(mode="json"),
    )
    assert response.status_code == 201, response.text
    assert response.json()["status"] == "accepted", response.text
    return cast(str, response.json()["run_id"])


async def _launch(stack: ProductionStack, run_id: str, body: dict[str, Any]) -> dict[str, Any]:
    response = await stack.http.post(f"/run-control/v1/runs/{run_id}/launch", json=body)
    assert response.status_code == 202, response.text
    return cast(dict[str, Any], response.json())


async def _run(stack: ProductionStack, run_id: str) -> dict[str, Any]:
    response = await stack.http.get(
        f"/run-control/v1/runs/{run_id}", params={"request_scope": SCOPE}
    )
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


async def _holds_wait(stack: ProductionStack, run_id: str) -> bool:
    run = await _run(stack, run_id)
    return any(item["condition_id"] == wait_condition_id(WAIT_ID) for item in run["active_waits"])


async def _terminal(stack: ProductionStack, run_id: str) -> bool:
    return (await _run(stack, run_id))["phase"] == "terminal"


def _command(run_id: str, version: int, command_id: str, action: Any, permission: str) -> dict:
    return LifecycleCommand(
        command_id=command_id,
        idempotency_issuer=OPERATOR,
        request_scope=SCOPE,
        run_id=run_id,
        expected_run_version=version,
        actor=ActorContext(actor_id=OPERATOR, permissions=frozenset({permission})),
        action=action,
        reason=f"RRM-009 qualification {command_id}",
        occurred_at=datetime.now(UTC),
        correlation_id=f"rrm009:{run_id}",
    ).model_dump(mode="json")


async def _send(stack: ProductionStack, run_id: str, command: dict[str, Any]) -> dict[str, Any]:
    response = await stack.http.post(f"/run-control/v1/runs/{run_id}/commands", json=command)
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


async def _receipt_states(stack: ProductionStack, run_id: str, command_id: str) -> list[str]:
    response = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/boundary-commands", params={"request_scope": SCOPE}
    )
    assert response.status_code == 200, response.text
    for status in response.json():
        if status["command"]["command_id"] == command_id:
            return [receipt["state"] for receipt in status["receipts"]]
    return []


async def _release_wait(stack: ProductionStack, run_id: str) -> None:
    await _until(lambda: _holds_wait(stack, run_id))
    run = await _run(stack, run_id)
    command_id = f"release:{run_id[:8]}"
    result = await _send(
        stack,
        run_id,
        _command(
            run_id,
            run["version"],
            command_id,
            SatisfyWaitAction(
                condition_id=wait_condition_id(WAIT_ID),
                verification_evidence_ref="evidence:rrm009-review",
            ),
            "workflow_run.observe_wait",
        ),
    )
    assert result["reason_code"] == "accepted_pending_application", result

    async def applied() -> bool:
        return (await _receipt_states(stack, run_id, command_id))[-1:] == ["applied"]

    await _until(applied)


async def _visible(client: Client, query: str, expected: int) -> int:
    """The Visibility count once it reaches `expected`, or the last count after 60 s."""

    count = -1
    for _ in range(240):
        count = (await client.count_workflows(query)).count
        if count == expected:
            return count
        await asyncio.sleep(0.25)
    return count


async def _replay(client: Client, workflow_ids: list[str]) -> int:
    replayer = Replayer(
        workflows=[
            BellLabsRunWorkflow,
            StageGraphWorkflow,
            GoalDirectedWorkflow,
            OperationWorkflow,
        ],
        workflow_runner=coordinator_workflow_runner(),
    )
    events = 0
    for workflow_id in workflow_ids:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        await replayer.replay_workflow(history)
        events += len(history.events)
    return events


async def _operation_payloads(stack: ProductionStack, run_id: str) -> list[dict[str, Any]]:
    """The digest-bound output payloads of the run's journaled settlements.

    Read exactly as the journal restores them: the settlement row's result manifest address,
    then the manifest's output payload address, both verified by the payload store.
    """

    store = FilesystemArtifactPayloadStore(stack.payload_root)
    async with stack.owner_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT result_manifest_ref, result_manifest_digest, result_manifest_size_bytes
            FROM (
                SELECT DISTINCT ON (s.claim_key) s.*
                FROM mission_control.operation_settlement s
                JOIN mission_control.operation_claim c
                  ON (c.installation_id, c.application_id, c.tenant_id, c.claim_key)
                   = (s.installation_id, s.application_id, s.tenant_id, s.claim_key)
                WHERE c.run_key = $1
                ORDER BY s.claim_key, s.settlement_revision DESC
            ) latest
            WHERE result_manifest_ref IS NOT NULL
            ORDER BY settled_at
            """,
            run_id,
        )
    payloads: list[dict[str, Any]] = []
    for row in rows:
        manifest = json.loads(
            await store.retrieve(
                ArtifactPayloadAddress(
                    object_ref=row["result_manifest_ref"],
                    content_digest=row["result_manifest_digest"],
                    size_bytes=row["result_manifest_size_bytes"],
                )
            )
        )
        if manifest.get("output_payload_ref") is None:
            continue
        payloads.append(
            json.loads(
                await store.retrieve(
                    ArtifactPayloadAddress(
                        object_ref=manifest["output_payload_ref"],
                        content_digest=manifest["output_payload_digest"],
                        size_bytes=manifest["output_payload_size_bytes"],
                    )
                )
            )
        )
    return payloads


def _pinned_summary(stack: ProductionStack) -> dict[str, Any]:
    """The worker's mounted pins by digest (the full disclosure is in the factory)."""

    assert stack.factory.operation is not None
    disclosure = stack.factory.operation.capabilities.disclosure()
    return {
        "mcp_servers": {
            item["server_id"]: {
                "module_digest": item["module_digest"],
                "schema_digest": item["schema_digest"],
                "tools": len(item["tools"]),
                "credential_ref": item["credential_ref"],
            }
            for item in cast(list[dict[str, Any]], disclosure["mcp_servers"])
        },
        "skills": {
            item["skill_name"]: item["bundle_digest"]
            for item in cast(list[dict[str, Any]], disclosure["skills"])
        },
        "tools": {
            item["tool_name"]: item["entrypoint_digest"]
            for item in cast(list[dict[str, Any]], disclosure["tools"])
        },
        "checkpointer_digests": disclosure["checkpointer_digests"],
        "store_digests": disclosure["store_digests"],
    }


def _lineages(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event["capability_lineage"]
        for payload in payloads
        for event in payload.get("event_payloads", ())
        if "capability_lineage" in event
    ]


async def _saver_checkpoints(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as connection:
        return int(
            await connection.fetchval(f"SELECT count(*) FROM {LANGGRAPH_SCHEMA}.checkpoints")
        )


def _calls(stack: ProductionStack, run_id: str) -> dict[str, dict[str, int]]:
    calls: dict[str, dict[str, int]] = {}
    for item in stack.model_log:
        if item["run_id"] != run_id:
            continue
        per_operation = calls.setdefault(item["operation"], {"parent": 0, "child": 0})
        per_operation[item["model"]] += 1
    return calls


# --- Durable outputs through the generic artifact path --------------------------------------


async def promote_generic_artifact(stack: ProductionStack) -> tuple[str, dict[str, Any]]:
    """One governed operation through `POST /runs/{run_id}/operations` (GenericArtifactWorkflow:
    `operation.execute`, candidate capture, `artifact.promote`); returns the run and result.
    Shared by the filesystem and the S3 object-store qualifications."""

    catalog = await publish_technical_catalog(
        stack.control_plane, family="StageGraph", now=datetime.now(UTC)
    )
    run_id = await _admit(stack, catalog, "rrm009-artifact-run")
    run = await _run(stack, run_id)
    started = await _send(
        stack,
        run_id,
        _command(
            run_id, run["version"], f"start:{run_id[:8]}", StartAction(), "workflow_run.start"
        ),
    )
    assert started["status"] == "accepted"
    unit = stage_unit(
        request_scope=SCOPE,
        run_id=run_id,
        operation_id=(
            "execution-epoch:1:stage:report:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default"
        ),
        stage_id="report",
    )
    reservation_id = f"reservation:{unit.unit_key}"
    run = await _run(stack, run_id)
    reserved = await _send(
        stack,
        run_id,
        _command(
            run_id,
            run["version"],
            f"reserve:{run_id[:8]}",
            ReserveBudgetAction(
                reservation_id=reservation_id, amounts={"tokens.total": 40, "model.turns": 8}
            ),
            "workflow_run.reserve_budget",
        ),
    )
    assert reserved["status"] == "accepted"
    template = stage_templates(stack.technical, catalog)["draft/execute/default"]
    workspace = template.workspace.model_copy(
        update={
            "namespace_id": f"workspace-namespace:{run_id}",
            "workspace_id": f"workspace:{run_id}:report",
        }
    )
    revision = reserved["resulting_run_version"]
    deep_binding = bind_unit(
        cast(Any, template.deep_agent_binding),
        unit,
        control_revision=revision,
        reservation_id=reservation_id,
        workspace=workspace,
        erc_digest=catalog.erc.digest,
    )
    operation = OperationExecutionRequest.model_validate(
        {
            **template.model_dump(mode="python"),
            "identity": OperationAttemptIdentity(
                run_id=run_id,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            "effective_configuration_digest": catalog.erc.digest,
            "run_control_revision": revision,
            "workspace": workspace,
            "deep_agent_binding": deep_binding,
            "runtime_unit": unit,
            "budget_reservation_id": reservation_id,
            "budget_limits": {"tokens.total": 40, "model.turns": 8},
            "requested_at": datetime.now(UTC),
            "idempotency_key": f"rrm009-artifact:{unit.unit_key}",
        }
    )
    submission = GenericArtifactWorkflowRequest(
        request_scope=SCOPE,
        run_id=run_id,
        operation=operation,
        promotion=ArtifactPromotionPlan(
            namespace_id=workspace.namespace_id,
            workspace_id=workspace.workspace_id,
            output_slot="output",
            logical_path=REPORT_PATH,
            owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:draft"),
            permission_ref="permission:rrm009",
            permission_outcome="allowed",
            output_contract_ref=operation.operation_contract_ref,
        ),
    )
    response = await stack.http.post(
        f"/run-control/v1/runs/{run_id}/operations", json=submission.model_dump(mode="json")
    )
    assert response.status_code == 201, response.text
    result = cast(dict[str, Any], response.json())
    assert result["operation"]["status"] == "completed"
    artifact = result["artifact"]
    assert artifact["status"] == "admitted"
    assert artifact["durable_reference"].startswith(f"artifact://{SCOPE}/{run_id}/")
    return run_id, result


# --- Canonical mission_control evidence --------------------------------------------------------


def _scoped(alias: str) -> str:
    return (
        f"{alias}.installation_id = $1 AND {alias}.application_id = $2 AND {alias}.tenant_id = $3"
    )


async def canonical_evidence(
    stack: ProductionStack | asyncpg.Pool,
    run_key: str,
    *,
    request_scope: str = SCOPE,
    units: bool = True,
    children: bool = False,
    artifacts: bool = False,
) -> dict[str, Any]:
    """Assert the run's canonical `mission_control` rows exist and link; return their ids and
    per-table counts (ids and scopes only, never payload contents).

    Read through the owner connection once the scenario settled: mission, mission_revision,
    definition_snapshot and compiled_program behind the mission_run; activations and
    attempts of the run's units; operation_intent with exactly one terminal
    operation_receipt per settled claim; the mission's event sequence contiguous from 1 with
    its outbox rows; subordinate executions of hosted children; artifacts the run produced.
    """

    scope = parse_request_scope(request_scope)
    key = (scope.installation_id, scope.application_id, scope.tenant_id)
    pool = stack if isinstance(stack, asyncpg.Pool) else stack.owner_pool
    async with pool.acquire() as connection:
        run = await connection.fetchrow(
            f"""
            SELECT r.run_id, r.mission_id, r.revision_id, r.lifecycle, r.terminal_outcome,
                   (SELECT count(*) FROM mission_control.compiled_program p
                     WHERE {_scoped("p")} AND p.revision_id = r.revision_id) AS programs,
                   (SELECT count(*) FROM mission_control.definition_snapshot d
                     WHERE {_scoped("d")}
                       AND d.definition_snapshot_id = rev.definition_snapshot_id) AS snapshots
            FROM mission_control.mission_run r
            JOIN mission_control.mission m
              ON (m.installation_id, m.application_id, m.tenant_id, m.mission_id)
               = (r.installation_id, r.application_id, r.tenant_id, r.mission_id)
            JOIN mission_control.mission_revision rev
              ON (rev.installation_id, rev.application_id, rev.tenant_id, rev.revision_id)
               = (r.installation_id, r.application_id, r.tenant_id, r.revision_id)
             AND rev.mission_id = r.mission_id
            WHERE {_scoped("r")} AND r.run_key = $4
            """,
            *key,
            run_key,
        )
        assert run is not None, f"no canonical mission_run for {run_key}"
        assert run["programs"] >= 1 and run["snapshots"] == 1, dict(run)
        run_id, mission_id = run["run_id"], run["mission_id"]
        activations = await connection.fetch(
            f"""SELECT a.activation_id FROM mission_control.activation a
                WHERE {_scoped("a")} AND a.run_id = $4 ORDER BY a.activation_id""",
            *key,
            run_id,
        )
        attempts = await connection.fetch(
            f"""SELECT t.attempt_id, t.activation_id FROM mission_control.attempt t
                WHERE {_scoped("t")} AND t.run_id = $4 ORDER BY t.attempt_id""",
            *key,
            run_id,
        )
        activation_ids = {row["activation_id"] for row in activations}
        assert all(row["activation_id"] in activation_ids for row in attempts)
        if units:
            assert activations and attempts, (run_key, len(activations), len(attempts))
        intents = await connection.fetch(
            f"""
            SELECT i.operation_intent_id, i.state, i.activation_id, c.status AS claim_status,
                   (SELECT count(*) FROM mission_control.operation_receipt o
                     WHERE {_scoped("o")}
                       AND o.operation_intent_id = i.operation_intent_id) AS receipts
            FROM mission_control.operation_intent i
            LEFT JOIN mission_control.operation_claim c
              ON (c.installation_id, c.application_id, c.tenant_id, c.operation_intent_id)
               = (i.installation_id, i.application_id, i.tenant_id, i.operation_intent_id)
            WHERE {_scoped("i")} AND i.run_id = $4
            """,
            *key,
            run_id,
        )
        settled = [row for row in intents if row["claim_status"] == "settled"]
        assert all(row["receipts"] == 1 for row in settled), [dict(row) for row in intents]
        assert all(row["receipts"] <= 1 for row in intents), [dict(row) for row in intents]
        assert all(
            row["activation_id"] is None or row["activation_id"] in activation_ids
            for row in intents
        )
        sequence = [
            row["seq"]
            for row in await connection.fetch(
                f"""SELECT e.seq FROM mission_control.mission_event e
                    WHERE {_scoped("e")} AND e.mission_id = $4 ORDER BY e.seq""",
                *key,
                mission_id,
            )
        ]
        assert sequence and sequence == list(range(1, len(sequence) + 1)), sequence
        outbox = int(
            await connection.fetchval(
                f"""SELECT count(*) FROM mission_control.outbox o
                    JOIN mission_control.mission_event e
                      ON (e.installation_id, e.application_id, e.tenant_id, e.event_id)
                       = (o.installation_id, o.application_id, o.tenant_id, o.event_id)
                    WHERE {_scoped("o")} AND e.mission_id = $4""",
                *key,
                mission_id,
            )
        )
        assert outbox >= 1, f"no outbox rows for {run_key}"
        subordinates = await connection.fetch(
            f"""SELECT s.subordinate_id, s.execution_kind
                FROM mission_control.subordinate_execution s
                WHERE {_scoped("s")} AND s.run_id = $4 ORDER BY s.subordinate_id""",
            *key,
            run_id,
        )
        if children:
            assert any(row["execution_kind"] == "async" for row in subordinates), run_key
        produced = await connection.fetch(
            f"""SELECT a.artifact_id FROM mission_control.artifact a
                WHERE {_scoped("a")} AND a.producer_run_id = $4 ORDER BY a.artifact_id""",
            *key,
            run_id,
        )
        if artifacts:
            assert produced, f"no artifact produced by {run_key}"
        settlements = int(
            await connection.fetchval(
                f"""SELECT count(*) FROM mission_control.operation_settlement s
                    JOIN mission_control.operation_claim c
                      ON (c.installation_id, c.application_id, c.tenant_id, c.claim_key)
                       = (s.installation_id, s.application_id, s.tenant_id, s.claim_key)
                    WHERE {_scoped("s")} AND c.run_key = $4""",
                *key,
                run_key,
            )
        )
        budget_entries = int(
            await connection.fetchval(
                f"""SELECT count(*) FROM mission_control.budget_entry b
                    WHERE {_scoped("b")} AND b.run_id = $4""",
                *key,
                run_id,
            )
        )
    return {
        "request_scope": request_scope,
        "run_key": run_key,
        "run_id": str(run_id),
        "mission_id": str(mission_id),
        "revision_id": str(run["revision_id"]),
        "lifecycle": run["lifecycle"],
        "terminal_outcome": run["terminal_outcome"],
        "activation_ids": [str(row["activation_id"]) for row in activations],
        "attempt_ids": [str(row["attempt_id"]) for row in attempts],
        "subordinate_ids": [str(row["subordinate_id"]) for row in subordinates],
        "artifact_ids": [str(row["artifact_id"]) for row in produced],
        "counts": {
            "compiled_program": int(run["programs"]),
            "activation": len(activations),
            "attempt": len(attempts),
            "operation_intent": len(intents),
            "operation_receipt": sum(int(row["receipts"]) for row in intents),
            "settled_claims": len(settled),
            "operation_settlement": settlements,
            "mission_event": len(sequence),
            "outbox_for_mission_events": outbox,
            "budget_entry": budget_entries,
            "subordinate_execution": len(subordinates),
            "artifact": len(produced),
        },
    }


def record_parity_trace(name: str, evidence: dict[str, Any]) -> None:
    """Merge one scenario's canonical evidence (ids, scopes, counts) into the JSON file named
    by ``MISSION_CONTROL_PARITY_TRACE`` (no-op when unset)."""

    target = os.environ.get("MISSION_CONTROL_PARITY_TRACE")
    if not target:
        return
    path = Path(target)
    try:
        current = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    except json.JSONDecodeError:
        current = {}
    current[name] = {"recorded_at": datetime.now(UTC).isoformat(), **evidence}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(current, indent=2, sort_keys=True, default=str), encoding="utf-8")
