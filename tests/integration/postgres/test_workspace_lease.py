"""FT-G3 migration 0030 (G3 section) on a disposable PostgreSQL 17: workspace leases, hook
task tokens and Operation Intents through the runtime role under forced RLS, and the Kernel
Hook callback over the real Stop Fence and frame stores.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.hook_tokens import (
    PostgresHookIntentLedger,
    PostgresHookTokenStore,
)
from mission_control.adapters.postgres.lanes.workspace_leases import PostgresWorkspaceLeaseStore
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackRejected,
    HookCallbackService,
    HookTokenContext,
    KernelHookCall,
)
from mission_control.application.execution.harness.leases import WorkspaceLease, lease_identity
from mission_control.application.execution.stop_fence import KernelHookFenceGate
from mission_control.domain.capabilities.hooks import HookDecision, HookEvent
from mission_control.domain.frames.contracts import FrameKind, LaneProfile
from mission_control.domain.policies.stop_fence import StopFence
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401

pytestmark = pytest.mark.common_db
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)


def _lease(scope: str, **changes: object) -> WorkspaceLease:
    heid = uuid4()
    lease_id, key = lease_identity("cursor_local", heid, 1)
    fields: dict[str, object] = {
        "lease_id": lease_id,
        "request_scope": scope,
        "lease_key": key,
        "lane_profile": "cursor_local",
        "harness_execution_id": heid,
        "generation": 1,
        "run_id": "run-lease",
        "attempt_no": 1,
        "path": "/leases/abc/a1-g1",
        "repository": "/repos/target",
        "base_ref": "main",
        "base_commit": "0" * 40,
        "fence": 1,
        "expires_at": NOW + timedelta(hours=4),
        **changes,
    }
    return WorkspaceLease.model_validate(fields)


async def test_workspace_leases_are_recorded_once_and_released_after_the_patch(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        store = PostgresWorkspaceLeaseStore(pool)
        scope = common_db.scope("tenant-1")
        lease = _lease(scope)
        recorded = await store.record(lease)
        assert recorded == lease
        # First wins: a second record with other content returns the stored lease.
        again = await store.record(lease.model_copy(update={"path": "/elsewhere"}))
        assert again.path == lease.path
        released = await store.release(
            scope, lease.lease_id, patch_artifact_ref="payload://patch", released_at=NOW
        )
        assert released.released and released.patch_artifact_ref == "payload://patch"
        twice = await store.release(
            scope, lease.lease_id, patch_artifact_ref="payload://other", released_at=NOW
        )
        assert twice.patch_artifact_ref == "payload://patch", "a lease is released once"
        assert await store.get(common_db.scope("tenant-2"), lease.lease_id) is None
    finally:
        await pool.close()


async def test_released_lease_records_the_lane_snapshot_a_fork_restores(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    """FT-G4: the newest released lease of a run names its frozen `cursor-snapshot:` ref."""

    pool = await common_db.pool("mission_control_runtime")
    try:
        store = PostgresWorkspaceLeaseStore(pool)
        scope = common_db.scope("tenant-1")
        run = f"run-{uuid4()}"
        assert await store.sandbox_snapshot_refs(scope, run) == ()
        first, second = _lease(scope, run_id=run), _lease(scope, run_id=run)
        for lease in (first, second):
            await store.record(lease)
        await store.release(
            scope,
            first.lease_id,
            patch_artifact_ref="payload://patch-1",
            released_at=NOW,
            snapshot_ref="cursor-snapshot:payload://snap-1",
        )
        assert await store.sandbox_snapshot_refs(scope, run) == (
            "cursor-snapshot:payload://snap-1",
        )
        released = await store.release(
            scope,
            second.lease_id,
            patch_artifact_ref="payload://patch-2",
            released_at=NOW,
            snapshot_ref="cursor-snapshot:payload://snap-2",
        )
        assert released.snapshot_ref == "cursor-snapshot:payload://snap-2"
        assert await store.sandbox_snapshot_refs(scope, run) == (
            "cursor-snapshot:payload://snap-2",
        )
        # Tenant scoped under forced RLS; another run has none.
        assert await store.sandbox_snapshot_refs(common_db.scope("tenant-2"), run) == ()
        assert await store.sandbox_snapshot_refs(scope, "run-other") == ()
    finally:
        await pool.close()


async def test_tokens_store_only_digests_and_intents_are_insert_only(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        start = admitted.start(lane=LaneProfile.CURSOR_LOCAL, native_session_ref="agent-77")
        frames = PostgresFrameRepository(pool)
        await frames.open_execution(start)
        scope = start.request_scope
        fences = PostgresStopFenceRepository(pool)
        service = HookCallbackService(
            tokens=PostgresHookTokenStore(pool),
            intents=PostgresHookIntentLedger(pool),
            mapper=CursorHookMapper(),
            fences=KernelHookFenceGate(fences),
            frames=frames,
        )
        context = HookTokenContext(
            request_scope=scope,
            run_id=admitted.run_key,
            attempt_no=1,
            generation=1,
            harness_execution_id=start.harness_execution_id,
            lane_profile="cursor_local",
            execution_start=start,
            workspace_root="/lease",
        )
        token = await service.issue(context, ttl=timedelta(hours=1))
        stored = await owner.fetch(
            "SELECT token_hash, context::text FROM mission_control.hook_task_token"
        )
        assert len(stored) == 1 and token not in stored[0]["context"]
        assert token not in stored[0]["token_hash"]
        parts = scope.split("/")
        scope_block = {
            "installation_id": parts[1],
            "application_id": parts[2],
            "tenant_id": parts[3],
        }

        def call(hook: str, event: str, payload: dict[str, object]) -> KernelHookCall:
            return KernelHookCall(
                kernel_hook_id=hook,
                event=HookEvent(event),
                scope=scope_block,  # type: ignore[arg-type]
                harness_execution_id=str(start.harness_execution_id),
                generation=1,
                payload=payload,
            )

        payload = {"tool_name": "Shell", "tool_input": {"command": "ls"}, "tool_use_id": "c-1"}
        allowed = await service.handle(call("mc.operation_intent", "before_tool", payload), token)
        assert allowed.decision == HookDecision.ALLOW
        intents = await owner.fetch("SELECT effect_ref FROM mission_control.hook_effect_intent")
        assert [row["effect_ref"] for row in intents] == ["tool_use:c-1"]
        await fences.persist(
            StopFence(
                request_scope=scope,
                run_id=admitted.run_key,
                generation=1,
                command_id="cancel-g3",
                reason="stop now",
                requested_at=NOW,
            )
        )
        fenced = await service.handle(
            call("mc.stop_fence", "before_tool", {**payload, "tool_use_id": "c-2"}), token
        )
        assert fenced.decision == HookDecision.DENY
        recorded = await frames.frames_for_execution(scope, start.harness_execution_id, 1)
        assert sum(frame.kind == FrameKind.HOOK_RESULT for frame in recorded) == 2
        async with pool.acquire() as connection:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute("DELETE FROM mission_control.hook_effect_intent")
        await service.revoke(context)
        with pytest.raises(HookCallbackRejected):
            await service.handle(call("mc.usage", "stop", {}), token)
        # Another tenant's scope does not know the token.
        other = list(scope_block.items())
        foreign = dict(other)
        foreign["tenant_id"] = str(common_db.tenants["tenant-2"])
        with pytest.raises(HookCallbackRejected) as unknown:
            await service.handle(
                KernelHookCall(
                    kernel_hook_id="mc.usage",
                    event=HookEvent.STOP,
                    scope=foreign,  # type: ignore[arg-type]
                    harness_execution_id=str(start.harness_execution_id),
                    generation=1,
                ),
                token,
            )
        assert unknown.value.code == "unknown_task_token"
    finally:
        await owner.close()
        await pool.close()


async def test_the_readonly_role_reads_intents_but_never_tokens(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        grants = {
            (row["table_name"], row["privilege_type"])
            for row in await owner.fetch(
                "SELECT table_name, privilege_type FROM information_schema.table_privileges "
                "WHERE table_schema = 'mission_control' AND grantee = 'mission_control_readonly' "
                "AND table_name IN ('hook_task_token', 'hook_effect_intent', 'workspace_lease')"
            )
        }
        assert ("hook_effect_intent", "SELECT") in grants
        assert ("workspace_lease", "SELECT") in grants
        assert not any(table == "hook_task_token" for table, _p in grants)
        forced = await owner.fetch(
            "SELECT relname, relforcerowsecurity FROM pg_class WHERE relname IN "
            "('hook_task_token', 'hook_effect_intent')"
        )
        assert all(row["relforcerowsecurity"] for row in forced) and len(forced) == 2
    finally:
        await owner.close()
