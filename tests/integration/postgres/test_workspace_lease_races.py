"""MP-04 on a disposable PostgreSQL 17: workspace slot acquisition races and stale-generation
writes through the runtime role under forced RLS (migrations 0003 + 0030 G3 grants; no new
DDL). Several pool connections contend for real, so the advisory lock, the unique lease key
and the fenced UPDATE guard are what decide the outcomes."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mission_control.adapters.cursor.git_workspaces import GitWorkspaceBackend
from mission_control.adapters.postgres.lanes.workspace_leases import PostgresWorkspaceLeaseStore
from mission_control.application.execution.harness.leases import (
    WorkspaceLease,
    WorkspaceLeaseHeld,
)
from mission_control.application.workspaces.service import WorkspaceAllocator
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.workspaces.lease_scenarios import (
    NOW,
    claim,
    concurrent_acquisition_has_one_winner,
    stale_generation_writes_are_rejected,
    stale_write_racing_a_takeover_never_both_apply,
)
from tests.unit.workspaces.workspace_fixtures import MemoryArtifacts, make_primary, present, request

pytestmark = pytest.mark.common_db


async def test_concurrent_slot_acquisition_has_exactly_one_winner(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime", min_size=8, max_size=8)
    try:
        await concurrent_acquisition_has_one_winner(
            PostgresWorkspaceLeaseStore(pool), common_db.scope("tenant-1")
        )
    finally:
        await pool.close()


async def test_stale_generation_writes_are_rejected(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        await stale_generation_writes_are_rejected(
            PostgresWorkspaceLeaseStore(pool), common_db.scope("tenant-1")
        )
    finally:
        await pool.close()


async def test_a_stale_write_racing_a_takeover_never_both_apply(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime", min_size=4, max_size=4)
    try:
        await stale_write_racing_a_takeover_never_both_apply(
            PostgresWorkspaceLeaseStore(pool), common_db.scope("tenant-1")
        )
    finally:
        await pool.close()


async def test_v03_two_agents_race_for_one_writable_workspace(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    """V03 end to end: real git worktrees, the Postgres ledger, two allocators (workers)."""

    pool = await common_db.pool("mission_control_runtime", min_size=4, max_size=4)
    try:
        store = PostgresWorkspaceLeaseStore(pool)
        primary = make_primary(tmp_path / "primary")
        root = tmp_path / "leases"
        backend = GitWorkspaceBackend(root)
        workers = [
            WorkspaceAllocator(ledger=store, backend=backend, root=root, allocator_ref=name)
            for name in ("worker-a", "worker-b")
        ]
        scope = common_db.scope("tenant-1")
        outcomes = await asyncio.gather(
            *(
                worker.allocate(
                    request(primary, run_id="run-v03").model_copy(update={"request_scope": scope})
                )
                for worker in workers
            ),
            return_exceptions=True,
        )
        winners = [item for item in outcomes if isinstance(item, WorkspaceLease)]
        losers = [item for item in outcomes if isinstance(item, WorkspaceLeaseHeld)]
        assert len(winners) == 1 and len(losers) == 1, outcomes
        (lease,) = winners
        owner = workers[outcomes.index(lease)]
        assert len(await store.slot_leases(scope, "run:run-v03")) == 1
        worktree = Path(lease.path)
        (worktree / "a.txt").write_bytes(b"edited\n")
        (worktree / "added.bin").write_bytes(bytes(range(256)))
        artifacts = MemoryArtifacts()
        custody = await owner.snapshot(lease, artifacts=artifacts)
        assert custody.manifest is not None
        assert [entry.path for entry in custody.manifest.untracked] == ["added.bin"]
        stored = await store.get(scope, lease.lease_id)
        assert stored is not None and stored.workspace_snapshot == custody.snapshot
        released = await owner.release(lease, custody=custody)
        assert released.released and released.snapshot_ref == custody.snapshot_ref
        assert await owner.cleanup(released, artifacts=artifacts) == "removed"
        final = await store.get(scope, lease.lease_id)
        assert final is not None and final.cleanup_status == "completed"
        assert not present(worktree)
    finally:
        await pool.close()


async def test_slots_are_tenant_scoped_and_detail_round_trips(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        store = PostgresWorkspaceLeaseStore(pool)
        tenant_1, tenant_2 = common_db.scope("tenant-1"), common_db.scope("tenant-2")
        first = await store.acquire(claim(tenant_1, "run:shared-name"), now=NOW)
        other = await store.acquire(claim(tenant_2, "run:shared-name"), now=NOW)
        assert first.fence == other.fence == 1
        assert await store.slot_leases(tenant_2, "run:shared-name") == (other,)
        assert await store.get(tenant_2, first.lease_id) is None
        assert first.policy is not None and first.policy.reuse == "within_run"
        assert first.allocator_ref == "worker-a" and first.branch is not None
        assert first.observed_state == "active" and first.cleanup_status == "pending"
    finally:
        await pool.close()
