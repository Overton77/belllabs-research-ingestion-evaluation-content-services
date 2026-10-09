"""Ledger scenarios shared by the in-memory unit tests and the real-PostgreSQL integration
tests (MP-04 acceptance: concurrent acquisition and stale-generation writes are rejected)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from mission_control.application.execution.harness.leases import (
    StaleWorkspaceLease,
    WorkspaceLease,
    WorkspaceLeaseHeld,
    WorkspaceLeaseLedger,
    lease_identity,
)
from mission_control.domain.execution.bindings import WorkspacePolicyPin, WorkspaceSnapshot

NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)
POLICY = WorkspacePolicyPin(
    mode="managed_worktree",
    reuse="within_run",
    dirty_input="reject",
    cleanup="retain_until_artifacts_registered",
)


def claim(
    scope: str,
    slot: str,
    *,
    harness_execution_id: UUID | None = None,
    generation: int = 1,
    ttl: timedelta = timedelta(hours=1),
    now: datetime = NOW,
) -> WorkspaceLease:
    heid = harness_execution_id or uuid4()
    lease_id, key = lease_identity("claude_agent_sdk", heid, generation)
    return WorkspaceLease(
        lease_id=lease_id,
        request_scope=scope,
        lease_key=key,
        lane_profile="claude_agent_sdk",
        harness_execution_id=heid,
        generation=generation,
        run_id="run-race",
        attempt_no=1,
        path=f"/leases/{slot.replace(':', '_')}/worktree",
        repository="https://example.invalid/repo.git",
        base_ref="main",
        base_commit="a" * 40,
        fence=0,
        expires_at=now + ttl,
        slot=slot,
        branch="mc/ws/" + slot.replace(":", "-"),
        policy=POLICY,
        allocator_ref="worker-a",
    )


def snapshot_of(lease: WorkspaceLease) -> WorkspaceSnapshot:
    return WorkspaceSnapshot(
        lane_profile="claude_agent_sdk",
        base_commit=lease.base_commit,
        head_commit="b" * 40,
        manifest_digest="sha256:" + "c" * 64,
        producer_lease_id=str(lease.lease_id),
        producer_generation=lease.generation,
        captured_at=NOW,
    )


async def concurrent_acquisition_has_one_winner(
    ledger: WorkspaceLeaseLedger, scope: str, *, contenders: int = 8, rounds: int = 6
) -> None:
    for round_no in range(rounds):
        slot = f"run:race-{uuid4()}-{round_no}"
        claims = [claim(scope, slot) for _ in range(contenders)]
        outcomes = await asyncio.gather(
            *(ledger.acquire(item, now=NOW) for item in claims), return_exceptions=True
        )
        winners = [item for item in outcomes if isinstance(item, WorkspaceLease)]
        held = [item for item in outcomes if isinstance(item, WorkspaceLeaseHeld)]
        assert len(winners) == 1, outcomes
        assert len(held) == contenders - 1, outcomes
        (winner,) = winners
        assert winner.fence == 1 and winner.slot == slot
        assert {error.holder_key for error in held} == {winner.lease_key}
        stored = await ledger.slot_leases(scope, slot)
        assert [lease.lease_id for lease in stored] == [winner.lease_id]
        # A loser was never recorded: it has nothing to write through.
        loser = next(item for item in claims if item.lease_id != winner.lease_id)
        with pytest.raises(LookupError):
            await ledger.release_fenced(
                scope,
                loser.lease_id,
                fence=1,
                patch_artifact_ref=None,
                snapshot_ref=None,
                released_at=NOW,
                cleanup_status="pending",
            )
        # And it cannot borrow the winner's fence under the winner's identity once the
        # winner was fenced out (covered by stale_generation_writes_are_rejected).
        assert (await ledger.get(scope, winner.lease_id)) == winner


async def stale_generation_writes_are_rejected(ledger: WorkspaceLeaseLedger, scope: str) -> None:
    heid = uuid4()
    slot = f"session:claude_agent_sdk:{heid}"
    first = await ledger.acquire(claim(scope, slot, harness_execution_id=heid), now=NOW)
    assert first.fence == 1
    again = await ledger.acquire(claim(scope, slot, harness_execution_id=heid), now=NOW)
    assert again.lease_id == first.lease_id and again.fence == 1, "same holder: idempotent"
    second_claim = claim(scope, slot, harness_execution_id=heid, generation=2)
    with pytest.raises(WorkspaceLeaseHeld):
        await ledger.acquire(second_claim, now=NOW)
    later = NOW + timedelta(hours=2)
    second = await ledger.acquire(second_claim, now=later)
    assert second.fence == 2
    lost = await ledger.get(scope, first.lease_id)
    assert lost is not None and lost.observed_state == "lost" and lost.released
    assert lost.lost_to == second.lease_id
    stale_writes = (
        ledger.renew(scope, first.lease_id, fence=1, expires_at=later + timedelta(hours=1)),
        ledger.record_snapshot(
            scope,
            first.lease_id,
            fence=1,
            snapshot=snapshot_of(first),
            snapshot_ref="workspace-snapshot:payload://stale",
        ),
        ledger.release_fenced(
            scope,
            first.lease_id,
            fence=1,
            patch_artifact_ref="payload://stale-patch",
            snapshot_ref=None,
            released_at=later,
            cleanup_status="pending",
        ),
        # The successor's row under the old fence is refused as well.
        ledger.release_fenced(
            scope,
            second.lease_id,
            fence=1,
            patch_artifact_ref=None,
            snapshot_ref=None,
            released_at=later,
            cleanup_status="pending",
        ),
    )
    for write in stale_writes:
        with pytest.raises(StaleWorkspaceLease):
            await write
    with pytest.raises(StaleWorkspaceLease):
        await ledger.acquire(claim(scope, slot, harness_execution_id=heid), now=later)
    unchanged = await ledger.get(scope, first.lease_id)
    assert unchanged is not None and unchanged.patch_artifact_ref is None
    assert unchanged.workspace_snapshot is None
    recorded = await ledger.record_snapshot(
        scope,
        second.lease_id,
        fence=2,
        snapshot=snapshot_of(second),
        snapshot_ref="workspace-snapshot:payload://current",
    )
    assert recorded.workspace_snapshot == snapshot_of(second)
    released = await ledger.release_fenced(
        scope,
        second.lease_id,
        fence=2,
        patch_artifact_ref="payload://patch",
        snapshot_ref="workspace-snapshot:payload://current",
        released_at=later,
        cleanup_status="pending",
    )
    assert released.released and released.snapshot_ref == "workspace-snapshot:payload://current"
    # A generation lower than one the slot already issued is superseded, never admitted.
    fifth = await ledger.acquire(
        claim(scope, slot, harness_execution_id=heid, generation=5), now=later
    )
    assert fifth.fence == 5
    with pytest.raises(StaleWorkspaceLease):
        await ledger.acquire(claim(scope, slot, harness_execution_id=heid, generation=4), now=later)
    cleaned = await ledger.release_fenced(
        scope,
        fifth.lease_id,
        fence=5,
        patch_artifact_ref=None,
        snapshot_ref=None,
        released_at=later,
        cleanup_status="pending",
    )
    assert (
        await ledger.mark_cleanup(scope, cleaned.lease_id, status="completed")
    ).cleanup_status == "completed"


async def stale_write_racing_a_takeover_never_both_apply(
    ledger: WorkspaceLeaseLedger, scope: str, *, rounds: int = 12
) -> None:
    """The fenced-out holder's release and its successor's takeover race; exactly one of them
    decides the old row, and the outcomes agree with what was stored."""

    later = NOW + timedelta(hours=2)
    for _ in range(rounds):
        slot = f"run:takeover-{uuid4()}"
        old = await ledger.acquire(claim(scope, slot, ttl=timedelta(minutes=1)), now=NOW)
        successor = claim(scope, slot, now=later)
        write, takeover = await asyncio.gather(
            ledger.release_fenced(
                scope,
                old.lease_id,
                fence=old.fence,
                patch_artifact_ref="payload://late",
                snapshot_ref=None,
                released_at=later,
                cleanup_status="pending",
            ),
            ledger.acquire(successor, now=later),
            return_exceptions=True,
        )
        assert isinstance(takeover, WorkspaceLease) and takeover.fence == 2, takeover
        stored = await ledger.get(scope, old.lease_id)
        assert stored is not None and stored.released
        if isinstance(write, WorkspaceLease):
            assert stored.observed_state == "released"
            assert stored.patch_artifact_ref == "payload://late"
        else:
            assert isinstance(write, StaleWorkspaceLease), write
            assert stored.observed_state == "lost" and stored.patch_artifact_ref is None


__all__ = [
    "NOW",
    "claim",
    "concurrent_acquisition_has_one_winner",
    "snapshot_of",
    "stale_generation_writes_are_rejected",
    "stale_write_racing_a_takeover_never_both_apply",
]
