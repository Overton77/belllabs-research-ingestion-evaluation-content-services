"""MP-08 x MP-11 on a disposable PostgreSQL 17: codex approval server requests bound to durable
approval Human Tasks through the production repositories.

SCHEMA: the scratch database is the released common migration chain (0001..0033, component
1.2.0) built by `tests.fixtures.mission_control_common_db` (the same chain the
`tests/integration/postgres/test_mp11_*` fixtures use); `approval_correlation` comes from
migration 0033.

Mission Control side, all production code: `PostgresApprovalTaskRepository`,
`PostgresApprovalCorrelationRepository`, `PostgresStopFenceRepository`,
`PostgresApprovalContextProbe` (generation from the MP-06 session owner, policy digest from
`harness_execution`), `ApprovalBroker`, `HumanTaskService`, `LaneTurnService` with
`PostgresFrameRepository` / `PostgresLaneExecutionStateStore`. The app-server is the MP-08
FIXTURE (`tests/unit/codex/fixture_app_server.py`): no Codex runs, nothing is paid.

Proves on real rows: the codex request is persisted as an approval task and a live
correlation (connection_ref = worker owner + app-server epoch) before any wait; the reviewer's
approval revalidates against the production probe as `approved` (not `policy_changed`): the
lane's policy digest IS the `harness_execution` binding digest the probe reads; the native
answer is the pinned `{"decision": "accept"}`; after a relaunch the old connection's live
correlation is `lost` (never reused) and the durable task stays for reconciliation.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from mission_control.adapters.codex.approvals import ApprovalContext
from mission_control.adapters.codex.transport import InboundEvent
from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
from mission_control.adapters.postgres.approvals.correlations import (
    PostgresApprovalCorrelationRepository,
)
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.approvals import (
    ApprovalResolutionRequest,
    ApprovalTaskView,
)
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationExecutionResult,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds, ReattachRequest
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.lane_turns import RecordingSignals, lane_stack
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db, owner_rows  # noqa: F401
from tests.unit.codex.drive import fields, started
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import (
    REVIEWER,
    CodexStack,
    build_harness,
    codex_operation,
)

pytestmark = pytest.mark.common_db

WORKER = "worker-codex-pg#1"
BOUNDS = LaneSegmentBounds(
    max_frames=200, max_duration_s=30, start_to_close_s=60, heartbeat_timeout_s=3
)


async def _released(database: CommonDatabase) -> None:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        table = await connection.fetchval(
            "SELECT to_regclass('mission_control.approval_correlation') IS NOT NULL"
        )
    finally:
        await connection.close()
    assert table, "migration 0033 (approval_correlation) is part of the installed chain"


class _Composition:
    """One worker's production approval composition on a shared pool."""

    def __init__(self, pool: asyncpg.Pool, scope: str) -> None:
        self.tasks = PostgresApprovalTaskRepository(pool)
        self.correlations = PostgresApprovalCorrelationRepository(pool)
        self.probe = PostgresApprovalContextProbe(pool)
        self.broker = ApprovalBroker(
            self.tasks,
            self.correlations,
            probe=self.probe,
            connection_ref=WORKER,
            fences=PostgresStopFenceRepository(pool),
            poll_seconds=0.05,
        )
        self.service = HumanTaskService(
            PostgresHumanTaskRepository(pool),
            request_scope=scope,
            approvals=self.tasks,
            approval_wake=self.broker.hub,
        )


async def _codex_unit(
    pool: asyncpg.Pool, db: CommonDatabase, tmp_path: Path, *scripts: str
) -> tuple[CodexStack, _Composition, LaneTurnService]:
    admitted = await admit_unit_attempt(pool, db)
    unit = admitted.unit
    payload = codex_operation().model_dump(mode="python")
    payload.update(
        request_scope=unit.request_scope,
        identity=OperationAttemptIdentity(
            run_id=admitted.run_key,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        runtime_unit=unit,
        idempotency_key=f"mp08-approvals:{unit.unit_key}",
    )
    operation = OperationExecutionRequest.model_validate(payload)
    composition = _Composition(pool, unit.request_scope)
    launcher = FixtureLauncher(scripts=[load_script(name) for name in scripts])
    harness, leaser, artifacts, auth = build_harness(tmp_path, launcher, broker=composition.broker)
    lanes = lane_stack(harness, operation=operation)
    stack = CodexStack(
        harness=harness,
        launcher=launcher,
        leaser=leaser,
        artifacts=artifacts,
        auth=auth,
        rig=None,
        operation=operation,
        lanes=lanes,
    )
    frames = PostgresFrameRepository(pool)
    service = LaneTurnService(
        lanes=lanes.service._lanes,
        boundary=lanes.boundary,
        frames=frames,
        states=PostgresLaneExecutionStateStore(pool),
        frame_reader=frames,
        sessions=WorkerSessionManager(owner_ref=WORKER),
    )
    return stack, composition, service


async def _open_task(composition: _Composition, scope: str, run_id: str) -> ApprovalTaskView:
    for _ in range(600):
        open_tasks = await composition.tasks.list_tasks(scope, run_id=run_id, lifecycle="open")
        if open_tasks:
            return open_tasks[0]
        await asyncio.sleep(0.05)
    raise AssertionError("no approval task was persisted")


async def test_a_codex_approval_is_a_durable_task_revalidated_by_the_production_probe(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    await _released(common_db)
    pool = await common_db.pool("mission_control_runtime", max_size=8)
    try:
        stack, composition, service = await _codex_unit(
            pool, common_db, tmp_path, "turn_with_approval"
        )
        identity = stack.identity
        scope, run_key = identity.request_scope, identity.run_key
        turn = asyncio.create_task(
            service.turn(stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames))
        )
        task = await _open_task(composition, scope, run_key)
        # Bound before anyone decided: the task and a live correlation are rows already.
        server = stack.launcher.server
        assert server.approvals == [], "nothing answered before the human decided"
        binding = task.packet.binding
        assert task.kind == "approval:provider_permission"
        assert binding.lane_profile == "codex" and binding.tool_name == "commandExecution"
        assert binding.native.native_request_ref == "1" and binding.native.connection_scoped
        assert binding.replay_strategy == "park_for_reconciliation"
        epoch = stack.launcher.launches[0].connection.epoch
        rows = await owner_rows(
            common_db,
            "SELECT state, connection_ref FROM mission_control.approval_correlation "
            "WHERE human_task_id = $1",
            task.human_task_id,
        )
        assert [(row["state"], row["connection_ref"]) for row in rows] == [
            ("live", f"{WORKER}:codex:{epoch}")
        ]
        # The lane's policy digest is the binding digest the production probe reads.
        state = await composition.probe.current(
            scope, run_id=run_key, harness_execution_id=str(identity.harness_execution_id)
        )
        recorded = await owner_rows(
            common_db,
            "SELECT coalesce(actual_binding_digest, intended_binding_digest) AS digest "
            "FROM mission_control.harness_execution WHERE harness_execution_id = $1",
            identity.harness_execution_id,
        )
        assert state.granted and state.generation == binding.generation == 1
        assert binding.policy_digest == state.policy_digest == recorded[0]["digest"]

        await composition.service.resolve_approval(
            task.human_task_id,
            ApprovalResolutionRequest(
                request_id="pg-review-1",
                expected_task_version=task.version,
                decision="approve",
                reviewed_digest=task.packet.review_digest,
            ),
            ActorContext(actor_id=REVIEWER),
        )
        result = await asyncio.wait_for(turn, timeout=60)

        assert result.done
        settled = OperationExecutionResult.model_validate(result.operation_result)
        assert settled.status == "completed"
        assert server.approvals == [
            ("item/commandExecution/requestApproval", {"result": {"decision": "accept"}})
        ], "approved, revalidated (not policy_changed), answered with the pinned shape"
        rows = await owner_rows(
            common_db,
            "SELECT state, reply->>'action' AS action, close_reason "
            "FROM mission_control.approval_correlation WHERE human_task_id = $1",
            task.human_task_id,
        )
        assert [(row["state"], row["action"]) for row in rows] == [("answered", "allow")]
        resolved = await composition.tasks.get_task(scope, task.human_task_id)
        assert resolved is not None and resolved.lifecycle == "resolved"
    finally:
        await pool.close()


async def test_a_relaunch_marks_the_dead_connections_correlation_lost(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    await _released(common_db)
    pool = await common_db.pool("mission_control_runtime", max_size=8)
    try:
        stack, composition, _service = await _codex_unit(pool, common_db, tmp_path, "turn_hold")
        identity = stack.identity
        session = await started(stack)
        old = stack.launcher.launches[0].connection
        request: dict[str, Any] = {
            "threadId": "thr-0001",
            "turnId": "turn-x",
            "itemId": "item-pg-1",
            "startedAtMs": 1,
        }
        pending = asyncio.create_task(
            stack.harness.approvals.serve(
                InboundEvent(3, "server_request", "item/fileChange/requestApproval", request, 4),
                ApprovalContext(
                    request_scope=identity.request_scope,
                    run_id=identity.run_key,
                    harness_execution_id=stack.heid,
                    generation=1,
                    native_session_ref="thr-0001",
                    policy_digest=stack.operation.effective_configuration_digest,
                    connection_epoch=old.epoch,
                ),
            )
        )
        task = await _open_task(composition, identity.request_scope, identity.run_key)
        await old.close("the app-server died")
        await stack.harness.reattach(
            ReattachRequest(**fields(stack), native_session_ref=session.native_session_ref or "")
        )
        rows = await owner_rows(
            common_db,
            "SELECT state, connection_ref FROM mission_control.approval_correlation "
            "WHERE human_task_id = $1",
            task.human_task_id,
        )
        assert [(row["state"], row["connection_ref"]) for row in rows] == [
            ("lost", f"{WORKER}:codex:{old.epoch}")
        ]
        (item,) = stack.harness.approvals.recovered
        assert item.action == "park_for_reconciliation"
        answer = await asyncio.wait_for(pending, 10)
        assert answer.result == {"decision": "cancel"}, "a lost handle never approves"
        kept = await composition.tasks.get_task(identity.request_scope, task.human_task_id)
        assert kept is not None and kept.lifecycle == "open"
        await stack.harness._terminate(stack.harness._sessions[stack.heid])
    finally:
        await pool.close()
