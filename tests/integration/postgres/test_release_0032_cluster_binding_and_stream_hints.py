"""Migration 0032 on a real disposable PostgreSQL 17 (MISSION_CONTROL_TEST_ADMIN_DSN).

- `run_cluster_binding` (MP-22 delta 1): write-once under the run's tenant scope, immutable,
  invisible to another tenant; the first cluster wins and a later bind from another cluster
  returns the stored row for the launch service to refuse.
- Stream hints (MP-14 delta 4): the `AFTER INSERT` triggers on `mission_event` and
  `provider_frame` notify `mc_stream_hint`, and `PostgresStreamHints` delivers a `StreamHint`
  with the scope and the mission id (never event data). Nothing here is a live provider.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.canonical import begin
from mission_control.adapters.postgres.run_control.cluster_bindings import (
    PostgresRunClusterBindings,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.realtime.stream_hints_postgres import PostgresStreamHints
from mission_control.application.execution.run_launch import TemporalClusterIdentity
from mission_control.application.streams.ports import StreamHint
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.run_control.test_run_control import request, service

pytestmark = pytest.mark.common_db

LOCAL = TemporalClusterIdentity(
    cluster_id="local:127.0.0.1:7233/default",
    target="local",
    address="127.0.0.1:7233",
    namespace="default",
    task_queue="mc-root",
)
CLOUD = TemporalClusterIdentity(
    cluster_id="cloud:example.tmprl.cloud:7233/mc.acct",
    target="cloud",
    address="example.tmprl.cloud:7233",
    namespace="mc.acct",
    task_queue="mc-root",
)


async def _admitted_run(pool: asyncpg.Pool, scope: str) -> tuple[Any, str]:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    return authority, admission.run_id


async def test_run_cluster_binding_is_write_once_scoped_and_immutable(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=4)
    owner = await common_db.owner_pool(max_size=2)
    try:
        scope = common_db.scope()
        _authority, run_id = await _admitted_run(pool, scope)
        bindings = PostgresRunClusterBindings(pool)
        now = datetime.now(UTC)
        workflow_id = f"belllabs-run/{run_id}"
        first = await bindings.bind(scope, run_id, workflow_id=workflow_id, cluster=LOCAL, now=now)
        assert first.cluster == LOCAL and first.workflow_id == workflow_id
        # A later bind from another cluster never overwrites; the caller compares and refuses.
        again = await bindings.bind(
            scope, run_id, workflow_id=workflow_id, cluster=CLOUD, now=now + timedelta(seconds=5)
        )
        assert again == first
        assert not again.cluster.same_cluster(CLOUD)
        assert await bindings.get(scope, run_id) == first
        # Another tenant of the same application sees nothing (forced RLS).
        assert await bindings.get(common_db.scope("tenant-2"), run_id) is None
        # Immutable even for the owner.
        async with owner.acquire() as connection, connection.transaction():
            await begin(connection, scope)
            with pytest.raises(asyncpg.RestrictViolationError):
                await connection.execute(
                    "UPDATE mission_control.run_cluster_binding SET cluster_id = 'x' "
                    "WHERE run_key = $1",
                    run_id,
                )
    finally:
        await owner.close()
        await pool.close()


async def test_commits_notify_stream_hints_that_the_listener_delivers(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=4)
    delivered: list[StreamHint] = []
    stop = asyncio.Event()
    listener = PostgresStreamHints(common_db.dsn(), reconnect_seconds=0.2)
    task = asyncio.create_task(listener.run(delivered.append, stop))
    try:
        scope = common_db.scope()
        for _ in range(200):
            if listener.connections:
                break
            await asyncio.sleep(0.05)
        assert listener.connections == 1
        _authority, run_id = await _admitted_run(pool, scope)
        async with pool.acquire() as connection, connection.transaction():
            await begin(connection, scope)
            mission_id = await connection.fetchval(
                "SELECT mission_id FROM mission_control.mission_run WHERE run_key = $1",
                run_id,
            )
        for _ in range(200):
            if delivered:
                break
            await asyncio.sleep(0.05)
        assert delivered, "the admission's mission events did not notify the listener"
        assert {hint.request_scope for hint in delivered} == {scope}
        assert {hint.mission_id for hint in delivered} == {UUID(str(mission_id))}
        assert all(hint.harness_execution_id is None for hint in delivered)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
        await pool.close()
