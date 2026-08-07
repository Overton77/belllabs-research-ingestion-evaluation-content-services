from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.runtime_resources import (
    InMemoryResourceLeaseJournal,
    ResourceCapacity,
    ResourceExhausted,
)
from app.domain.graph_runtime.kernel import (
    ResourceKind,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
)
from app.domain.run_control.errors import IdempotencyConflict

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def request(
    lease_id: str,
    semantic_identity: str,
    resources: tuple[ResourceKind, ...],
    *,
    digest: str = DIGEST,
    ttl_seconds: int = 60,
) -> ResourceLeaseRequest:
    return ResourceLeaseRequest(
        lease_id=lease_id,
        request_scope="tenant-1",
        semantic_identity=semantic_identity,
        envelope_digest=digest,
        resources=resources,
        requested_at=NOW,
        deadline=NOW + timedelta(minutes=10),
        ttl_seconds=ttl_seconds,
    )


def journal() -> InMemoryResourceLeaseJournal:
    return InMemoryResourceLeaseJournal(
        ResourceCapacity(
            limits={
                ResourceKind.TENANT: 4,
                ResourceKind.OPERATION_WORKER: 1,
                ResourceKind.RESUMPTION: 1,
                ResourceKind.MODEL_CALL: 1,
            }
        )
    )


@pytest.mark.asyncio
async def test_duplicate_same_digest_is_idempotent_and_changed_envelope_conflicts() -> None:
    leases = journal()
    original = request(
        "lease-1",
        "operation:1",
        (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
    )

    first = await leases.acquire(original, now=NOW)
    replay = await leases.acquire(original, now=NOW)

    assert first == replay
    with pytest.raises(IdempotencyConflict, match="different envelope"):
        await leases.acquire(
            request(
                "lease-2",
                "operation:1",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
                digest=OTHER_DIGEST,
            ),
            now=NOW,
        )


@pytest.mark.asyncio
async def test_saturated_worker_capacity_does_not_consume_resumption_capacity() -> None:
    leases = journal()
    await leases.acquire(
        request(
            "worker-1",
            "operation:1",
            (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
        ),
        now=NOW,
    )

    with pytest.raises(ResourceExhausted, match="operation_worker"):
        await leases.acquire(
            request(
                "worker-2",
                "operation:2",
                (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
            ),
            now=NOW,
        )
    resumed = await leases.acquire(
        request(
            "resume-1",
            "resume:operation:waiting",
            (ResourceKind.TENANT, ResourceKind.RESUMPTION),
        ),
        now=NOW,
    )

    assert resumed.status == ResourceLeaseStatus.ACQUIRED


@pytest.mark.asyncio
async def test_wait_releases_worker_and_retains_only_declared_lease() -> None:
    leases = journal()
    worker = await leases.acquire(
        request(
            "worker-1",
            "operation:1",
            (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
        ),
        now=NOW,
    )
    model = await leases.acquire(
        request(
            "model-1",
            "model:1",
            (ResourceKind.TENANT, ResourceKind.MODEL_CALL),
        ),
        now=NOW,
    )

    projection = await leases.transition_to_wait(
        request_scope="tenant-1",
        wait_binding_ref="wait:1",
        lease_ids=("worker-1", "model-1"),
        retain=frozenset({"model-1"}),
        now=NOW,
    )
    replacement = await leases.acquire(
        request(
            "worker-2",
            "operation:2",
            (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
        ),
        now=NOW,
    )

    assert projection.released_reservations == (worker.request.lease_id,)
    assert projection.retained_reservations == (model.request.lease_id,)
    assert replacement.status == ResourceLeaseStatus.ACQUIRED


@pytest.mark.asyncio
async def test_process_loss_expiry_releases_capacity_for_reconciliation() -> None:
    leases = journal()
    acquired = await leases.acquire(
        request(
            "worker-1",
            "operation:1",
            (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
            ttl_seconds=1,
        ),
        now=NOW,
    )

    expired = await leases.expire_due(
        request_scope="tenant-1",
        now=NOW + timedelta(seconds=2),
    )
    replacement = await leases.acquire(
        request(
            "worker-2",
            "operation:2",
            (ResourceKind.TENANT, ResourceKind.OPERATION_WORKER),
        ),
        now=NOW + timedelta(seconds=2),
    )

    assert expired[0].request.lease_id == acquired.request.lease_id
    assert expired[0].status == ResourceLeaseStatus.EXPIRED
    assert replacement.status == ResourceLeaseStatus.ACQUIRED
