from __future__ import annotations

import asyncio
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
from typing import cast

from pydantic import BaseModel, ConfigDict, Field

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.kernel import (
    ResourceKind,
    ResourceKindV2,
    ResourceLeaseRecord,
    ResourceLeaseRecordV2,
    ResourceLeaseRequest,
    ResourceLeaseRequestV2,
    ResourceLeaseStatus,
    WaitLeaseProjection,
)
from app.domain.run_control.errors import IdempotencyConflict


class ResourceCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    limits: dict[ResourceKind | ResourceKindV2, int] = Field(default_factory=dict)


class ResourceExhausted(RuntimeError):
    pass


class InMemoryResourceLeaseJournal:
    """Atomic behavioral journal for hierarchical reservations and crash recovery."""

    def __init__(self, capacity: ResourceCapacity) -> None:
        self._capacity = capacity
        self._lock = asyncio.Lock()
        self._records: dict[
            tuple[str, str], ResourceLeaseRecord | ResourceLeaseRecordV2
        ] = {}
        self._semantic: dict[tuple[str, str], str] = {}

    async def acquire(
        self,
        request: ResourceLeaseRequest | ResourceLeaseRequestV2,
        *,
        now: datetime,
    ) -> ResourceLeaseRecord | ResourceLeaseRecordV2:
        if now >= request.deadline:
            raise TimeoutError("resource lease deadline elapsed before acquisition")
        key = (request.request_scope, request.lease_id)
        semantic_key = (request.request_scope, request.semantic_identity)
        canonical_digest = sha256_digest(request)
        async with self._lock:
            prior_id = self._semantic.get(semantic_key)
            if prior_id is not None:
                prior = self._records[(request.request_scope, prior_id)]
                if prior.request != request or prior.canonical_digest != canonical_digest:
                    raise IdempotencyConflict(
                        "resource semantic identity was reused with a different envelope"
                    )
                return deepcopy(prior)
            if key in self._records:
                raise IdempotencyConflict("resource lease identity was reused")
            active = self._active_counts(request.request_scope, now=now)
            requested = Counter(request.resources)
            for resource_value, units in requested.items():
                resource = cast(ResourceKind | ResourceKindV2, resource_value)
                limit = self._capacity.limits.get(resource, 0)
                if active[resource] + units > limit:
                    raise ResourceExhausted(f"capacity exhausted for {resource.value}")
            expires_at = min(
                request.deadline,
                now + timedelta(seconds=request.ttl_seconds),
            )
            if isinstance(request, ResourceLeaseRequestV2):
                record: ResourceLeaseRecord | ResourceLeaseRecordV2 = ResourceLeaseRecordV2(
                    request=request,
                    status=ResourceLeaseStatus.ACQUIRED,
                    acquired_at=now,
                    expires_at=expires_at,
                    canonical_digest=canonical_digest,
                )
            else:
                record = ResourceLeaseRecord(
                    request=request,
                    status=ResourceLeaseStatus.ACQUIRED,
                    acquired_at=now,
                    expires_at=expires_at,
                    canonical_digest=canonical_digest,
                )
            self._records[key] = record
            self._semantic[semantic_key] = request.lease_id
            return deepcopy(record)

    async def renew(
        self,
        *,
        request_scope: str,
        lease_id: str,
        expected_digest: str,
        now: datetime,
    ) -> ResourceLeaseRecord | ResourceLeaseRecordV2:
        key = (request_scope, lease_id)
        async with self._lock:
            prior = self._require(key)
            self._require_digest(prior, expected_digest)
            if prior.status not in {
                ResourceLeaseStatus.ACQUIRED,
                ResourceLeaseStatus.RETAINED,
            }:
                raise ValueError("only live resource leases can be renewed")
            if prior.expires_at is None or now >= prior.expires_at:
                expired = prior.model_copy(update={"status": ResourceLeaseStatus.EXPIRED})
                self._records[key] = expired
                raise TimeoutError("resource lease expired before renewal")
            renewed = prior.model_copy(
                update={
                    "expires_at": min(
                        prior.request.deadline,
                        now + timedelta(seconds=prior.request.ttl_seconds),
                    )
                }
            )
            self._records[key] = renewed
            return deepcopy(renewed)

    async def release(
        self,
        *,
        request_scope: str,
        lease_id: str,
        expected_digest: str,
        now: datetime,
    ) -> ResourceLeaseRecord | ResourceLeaseRecordV2:
        key = (request_scope, lease_id)
        async with self._lock:
            prior = self._require(key)
            self._require_digest(prior, expected_digest)
            if prior.status == ResourceLeaseStatus.RELEASED:
                return deepcopy(prior)
            released = prior.model_copy(
                update={
                    "status": ResourceLeaseStatus.RELEASED,
                    "released_at": now,
                }
            )
            self._records[key] = released
            return deepcopy(released)

    async def transition_to_wait(
        self,
        *,
        request_scope: str,
        wait_binding_ref: str,
        lease_ids: tuple[str, ...],
        retain: frozenset[str],
        now: datetime,
    ) -> WaitLeaseProjection:
        if not retain <= set(lease_ids):
            raise ValueError("wait retention references an unowned lease")
        retained: list[str] = []
        released: list[str] = []
        async with self._lock:
            records = [self._require((request_scope, lease_id)) for lease_id in lease_ids]
            for record in records:
                if record.request.lease_id in retain:
                    updated = record.model_copy(update={"status": ResourceLeaseStatus.RETAINED})
                    retained.append(record.request.lease_id)
                else:
                    updated = record.model_copy(
                        update={
                            "status": ResourceLeaseStatus.RELEASED,
                            "released_at": now,
                        }
                    )
                    released.append(record.request.lease_id)
                self._records[(request_scope, record.request.lease_id)] = updated
        return WaitLeaseProjection(
            wait_binding_ref=wait_binding_ref,
            retained_reservations=tuple(sorted(retained)),
            released_reservations=tuple(sorted(released)),
        )

    async def expire_due(
        self,
        *,
        request_scope: str,
        now: datetime,
    ) -> tuple[ResourceLeaseRecord | ResourceLeaseRecordV2, ...]:
        expired: list[ResourceLeaseRecord | ResourceLeaseRecordV2] = []
        async with self._lock:
            for key, prior in tuple(self._records.items()):
                if key[0] != request_scope:
                    continue
                if (
                    prior.status in {ResourceLeaseStatus.ACQUIRED, ResourceLeaseStatus.RETAINED}
                    and prior.expires_at is not None
                    and now >= prior.expires_at
                ):
                    updated = prior.model_copy(update={"status": ResourceLeaseStatus.EXPIRED})
                    self._records[key] = updated
                    expired.append(deepcopy(updated))
        return tuple(expired)

    def _active_counts(
        self, request_scope: str, *, now: datetime
    ) -> Counter[ResourceKind | ResourceKindV2]:
        counts: Counter[ResourceKind | ResourceKindV2] = Counter()
        for (scope, _), record in self._records.items():
            if scope != request_scope:
                continue
            if record.status not in {
                ResourceLeaseStatus.ACQUIRED,
                ResourceLeaseStatus.RETAINED,
            }:
                continue
            if record.expires_at is not None and record.expires_at <= now:
                continue
            counts.update(record.request.resources)
        return counts

    def _require(
        self, key: tuple[str, str]
    ) -> ResourceLeaseRecord | ResourceLeaseRecordV2:
        try:
            return self._records[key]
        except KeyError as error:
            raise LookupError("resource lease not found in request scope") from error

    @staticmethod
    def _require_digest(
        record: ResourceLeaseRecord | ResourceLeaseRecordV2,
        expected_digest: str,
    ) -> None:
        if record.canonical_digest != expected_digest:
            raise IdempotencyConflict("resource lease digest does not match")
