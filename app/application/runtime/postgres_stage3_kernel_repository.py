"""RLS-scoped PostgreSQL persistence for the Stage 3 durable runtime kernel."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol

import asyncpg

from app.application.runtime.runtime_decisions import DurableDecisionRecord
from app.application.runtime.runtime_lineage import PersistedExecutionLineage
from app.application.runtime.runtime_reconciliation import (
    RuntimeIncidentDecision,
    RuntimeIncidentObservation,
    RuntimeRepairAuditRecord,
)
from app.application.runtime.runtime_recovery import ForkAdmission
from app.application.runtime.runtime_resources import ResourceCapacity, ResourceExhausted
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import ForkReceipt, ForkRequest
from app.domain.graph_runtime.kernel import (
    RESOURCE_ACQUISITION_ORDER,
    DecisionRequest,
    DecisionResponse,
    ResourceKind,
    ResourceLeaseRecord,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
    WaitLeaseProjection,
)
from app.domain.run_control.errors import IdempotencyConflict

RETENTION_DAYS = 90
_LIVE_LEASE_STATUSES = frozenset(
    {
        ResourceLeaseStatus.REQUESTED,
        ResourceLeaseStatus.ACQUIRED,
        ResourceLeaseStatus.RETAINED,
    }
)
_CAPACITY_STATUSES = frozenset(
    {
        ResourceLeaseStatus.ACQUIRED,
        ResourceLeaseStatus.RETAINED,
    }
)


class RetentionAuthority(Protocol):
    async def authorize_deletion(
        self,
        *,
        request_scope: str,
        actor_id: str,
        record_class: str,
    ) -> bool: ...


class DenyByDefaultRetentionAuthority:
    async def authorize_deletion(
        self,
        *,
        request_scope: str,
        actor_id: str,
        record_class: str,
    ) -> bool:
        del request_scope, actor_id, record_class
        return False


class PostgresExecutionLineageRepository:
    """Immutable lineage journal with explicit parent traversal."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def append(self, lineage: PersistedExecutionLineage) -> PersistedExecutionLineage:
        scope = lineage.envelope.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"lineage:{scope}:{lineage.lineage_id}")
            prior = await connection.fetchrow(
                """
                SELECT lineage_payload
                FROM belllabs_control.runtime_lineage_records
                WHERE request_scope = $1 AND lineage_id = $2
                FOR UPDATE
                """,
                scope,
                lineage.lineage_id,
            )
            if prior is not None:
                persisted = PersistedExecutionLineage.model_validate(
                    _json(prior["lineage_payload"])
                )
                if persisted != lineage:
                    raise IdempotencyConflict("lineage identity was reused with conflicting facts")
                return persisted
            digest_owner = await connection.fetchval(
                """
                SELECT lineage_id
                FROM belllabs_control.runtime_lineage_records
                WHERE request_scope = $1 AND lineage_digest = $2
                """,
                scope,
                lineage.lineage_digest,
            )
            if digest_owner is not None:
                raise IdempotencyConflict("lineage digest is already bound to another identity")
            parent_id = lineage.envelope.parent_lineage_id
            if parent_id is not None:
                parent_exists = await connection.fetchval(
                    """
                    SELECT 1
                    FROM belllabs_control.runtime_lineage_records
                    WHERE request_scope = $1 AND lineage_id = $2
                    """,
                    scope,
                    parent_id,
                )
                if parent_exists is None:
                    raise ValueError("lineage parent must be persisted before its child")
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_lineage_records (
                    lineage_id, request_scope, belllabs_run_id, execution_epoch,
                    lineage_digest, result_manifest_ref, lineage_payload,
                    recorded_at, retain_until
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
                """,
                lineage.lineage_id,
                scope,
                lineage.envelope.belllabs_run_id,
                lineage.envelope.execution_epoch,
                lineage.lineage_digest,
                lineage.envelope.result_manifest_ref,
                _dump(lineage),
                lineage.recorded_at,
                lineage.retain_until,
            )
            for parent_edge in lineage.parent_edges:
                edge = parent_edge.model_dump(mode="json")
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.runtime_lineage_edges (
                        request_scope, lineage_id, parent_identity_key, child_identity_key,
                        relationship, edge_digest, recorded_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    scope,
                    lineage.lineage_id,
                    parent_edge.parent.canonical_key,
                    parent_edge.child.canonical_key,
                    parent_edge.relationship,
                    sha256_digest(edge),
                    lineage.recorded_at,
                )
            return lineage

    async def provenance_for_result(
        self,
        request_scope: str,
        result_manifest_ref: str,
    ) -> tuple[PersistedExecutionLineage, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT lineage_id, lineage_payload
                FROM belllabs_control.runtime_lineage_records
                WHERE request_scope = $1 AND result_manifest_ref = $2
                """,
                request_scope,
                result_manifest_ref,
            )
            if not rows:
                raise LookupError("result manifest has no persisted lineage")
            by_id = {
                row["lineage_id"]: PersistedExecutionLineage.model_validate(
                    _json(row["lineage_payload"])
                )
                for row in rows
            }
            collected: dict[str, PersistedExecutionLineage] = {}
            pending = list(by_id.values())
            while pending:
                item = pending.pop()
                if item.lineage_id in collected:
                    continue
                collected[item.lineage_id] = item
                parent_id = item.envelope.parent_lineage_id
                if parent_id is None or parent_id in collected:
                    continue
                if parent_id in by_id:
                    pending.append(by_id[parent_id])
                    continue
                parent_payload = await connection.fetchval(
                    """
                    SELECT lineage_payload
                    FROM belllabs_control.runtime_lineage_records
                    WHERE request_scope = $1 AND lineage_id = $2
                    """,
                    request_scope,
                    parent_id,
                )
                if parent_payload is None:
                    raise ValueError("persisted lineage contains a parent gap")
                parent = PersistedExecutionLineage.model_validate(_json(parent_payload))
                by_id[parent.lineage_id] = parent
                pending.append(parent)
        return tuple(
            sorted(
                collected.values(),
                key=lambda item: (item.recorded_at, item.lineage_id),
            )
        )


class PostgresDecisionRepository:
    """Durable decision request/response journal with conflicting-replay rejection."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(self, request: DecisionRequest) -> DurableDecisionRecord:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"decision:{scope}:{request.decision_id}")
            prior = await connection.fetchrow(
                """
                SELECT req.request_payload, req.status, resp.response_payload
                FROM belllabs_control.runtime_decision_requests req
                LEFT JOIN belllabs_control.runtime_decision_responses resp
                  ON resp.request_scope = req.request_scope
                 AND resp.decision_id = req.decision_id
                WHERE req.request_scope = $1 AND req.decision_id = $2
                FOR UPDATE OF req
                """,
                scope,
                request.decision_id,
            )
            if prior is not None:
                persisted_request = DecisionRequest.model_validate(_json(prior["request_payload"]))
                if persisted_request != request:
                    raise IdempotencyConflict("decision identity has conflicting intent")
                response = (
                    DecisionResponse.model_validate(_json(prior["response_payload"]))
                    if prior["response_payload"] is not None
                    else None
                )
                return DurableDecisionRecord(
                    request=persisted_request,
                    status=prior["status"],
                    response=response,
                )
            retain_until = request.requested_at + timedelta(days=RETENTION_DAYS)
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_decision_requests (
                    decision_id, request_scope, binding_id, decision_type,
                    request_schema_ref, request_digest, expected_belllabs_version,
                    policy_ref, request_payload, status, requested_at, expires_at,
                    retain_until
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, 'pending', $10, $11, $12
                )
                """,
                request.decision_id,
                scope,
                request.binding_id,
                request.decision_type,
                request.schema_ref,
                request.request_digest,
                request.expected_lifecycle_version,
                request.policy_ref,
                _dump(request),
                request.requested_at,
                request.expires_at,
                retain_until,
            )
            return DurableDecisionRecord(request=request)

    async def get(
        self,
        request_scope: str,
        decision_id: str,
    ) -> DurableDecisionRecord | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT req.request_payload, req.status, resp.response_payload
                FROM belllabs_control.runtime_decision_requests req
                LEFT JOIN belllabs_control.runtime_decision_responses resp
                  ON resp.request_scope = req.request_scope
                 AND resp.decision_id = req.decision_id
                WHERE req.request_scope = $1 AND req.decision_id = $2
                """,
                request_scope,
                decision_id,
            )
        if row is None:
            return None
        response = (
            DecisionResponse.model_validate(_json(row["response_payload"]))
            if row["response_payload"] is not None
            else None
        )
        return DurableDecisionRecord(
            request=DecisionRequest.model_validate(_json(row["request_payload"])),
            status=row["status"],
            response=response,
        )

    async def answer(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DurableDecisionRecord:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"decision:{scope}:{request.decision_id}")
            prior = await connection.fetchrow(
                """
                SELECT req.request_payload, req.status, resp.response_payload
                FROM belllabs_control.runtime_decision_requests req
                LEFT JOIN belllabs_control.runtime_decision_responses resp
                  ON resp.request_scope = req.request_scope
                 AND resp.decision_id = req.decision_id
                WHERE req.request_scope = $1 AND req.decision_id = $2
                FOR UPDATE OF req
                """,
                scope,
                request.decision_id,
            )
            if prior is None:
                raise LookupError("durable decision request not found")
            persisted_request = DecisionRequest.model_validate(_json(prior["request_payload"]))
            if persisted_request != request:
                raise LookupError("durable decision request not found")
            if prior["response_payload"] is not None:
                persisted_response = DecisionResponse.model_validate(
                    _json(prior["response_payload"])
                )
                if persisted_response != response:
                    raise IdempotencyConflict("decision already has a different response")
                return DurableDecisionRecord(
                    request=persisted_request,
                    status="answered",
                    response=persisted_response,
                )
            actor_type, actor_id = _split_actor_ref(response.actor_ref)
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_decision_responses (
                    response_id, request_scope, decision_id, response_digest,
                    actor_id, actor_type, response_payload, decided_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                """,
                response.response_id,
                scope,
                response.decision_id,
                response.response_digest,
                actor_id,
                actor_type,
                _dump(response),
                response.decided_at,
            )
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_decision_requests
                SET status = 'answered'
                WHERE request_scope = $1 AND decision_id = $2
                """,
                scope,
                request.decision_id,
            )
            return DurableDecisionRecord(
                request=persisted_request,
                status="answered",
                response=response,
            )


class PostgresResourceLeaseJournal:
    """Atomic hierarchical resource lease journal with capacity and expiry recovery."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        capacity: ResourceCapacity,
        *,
        owner_instance_id: str = "runtime",
    ) -> None:
        self._pool = pool
        self._capacity = capacity
        self._owner_instance_id = owner_instance_id

    async def acquire(
        self,
        request: ResourceLeaseRequest,
        *,
        now: datetime,
    ) -> ResourceLeaseRecord:
        if now >= request.deadline:
            raise TimeoutError("resource lease deadline elapsed before acquisition")
        scope = request.request_scope
        canonical_digest = sha256_digest(request)
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"resource-lease:{scope}")
            await self._expire_due_locked(connection, request_scope=scope, now=now)
            semantic = await connection.fetchrow(
                """
                SELECT lease_id, lease_payload, canonical_digest
                FROM belllabs_control.execution_resource_leases
                WHERE request_scope = $1 AND semantic_identity = $2
                FOR UPDATE
                """,
                scope,
                request.semantic_identity,
            )
            if semantic is not None:
                prior = ResourceLeaseRecord.model_validate(_json(semantic["lease_payload"]))
                if prior.request != request or prior.canonical_digest != canonical_digest:
                    raise IdempotencyConflict(
                        "resource semantic identity was reused with a different envelope"
                    )
                return prior
            existing = await connection.fetchval(
                """
                SELECT 1
                FROM belllabs_control.execution_resource_leases
                WHERE request_scope = $1 AND lease_id = $2
                """,
                scope,
                request.lease_id,
            )
            if existing is not None:
                raise IdempotencyConflict("resource lease identity was reused")
            active = await self._active_counts_locked(connection, scope, now=now)
            requested = Counter(request.resources)
            for resource, units in requested.items():
                limit = self._capacity.limits.get(resource, 0)
                if active[resource] + units > limit:
                    raise ResourceExhausted(f"capacity exhausted for {resource.value}")
            expires_at = min(
                request.deadline,
                now + timedelta(seconds=request.ttl_seconds),
            )
            record = ResourceLeaseRecord(
                request=request,
                status=ResourceLeaseStatus.ACQUIRED,
                acquired_at=now,
                expires_at=expires_at,
                canonical_digest=canonical_digest,
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.execution_resource_leases (
                    lease_id, request_scope, semantic_identity, envelope_digest,
                    canonical_digest, resources, acquisition_order, status,
                    retained_for_wait, owner_instance_id, version, acquired_at,
                    renewed_at, expires_at, released_at, lease_payload
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6::jsonb, $7, $8, false, $9, 1,
                    $10, NULL, $11, NULL, $12::jsonb
                )
                """,
                request.lease_id,
                scope,
                request.semantic_identity,
                request.envelope_digest,
                canonical_digest,
                _dump([item.value for item in request.resources]),
                RESOURCE_ACQUISITION_ORDER.index(request.resources[0]) + 1,
                ResourceLeaseStatus.ACQUIRED.value,
                self._owner_instance_id,
                now,
                expires_at,
                _dump(record),
            )
            return record

    async def renew(
        self,
        *,
        request_scope: str,
        lease_id: str,
        expected_digest: str,
        now: datetime,
    ) -> ResourceLeaseRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"resource-lease:{request_scope}")
            prior = await self._require_locked(connection, request_scope, lease_id)
            self._require_digest(prior, expected_digest)
            if prior.status not in {
                ResourceLeaseStatus.ACQUIRED,
                ResourceLeaseStatus.RETAINED,
            }:
                raise ValueError("only live resource leases can be renewed")
            if prior.expires_at is None or now >= prior.expires_at:
                expired = prior.model_copy(update={"status": ResourceLeaseStatus.EXPIRED})
                await self._write_locked(connection, expired, version_bump=True)
                raise TimeoutError("resource lease expired before renewal")
            renewed = prior.model_copy(
                update={
                    "expires_at": min(
                        prior.request.deadline,
                        now + timedelta(seconds=prior.request.ttl_seconds),
                    )
                }
            )
            await self._write_locked(
                connection,
                renewed,
                version_bump=True,
                renewed_at=now,
            )
            return renewed

    async def release(
        self,
        *,
        request_scope: str,
        lease_id: str,
        expected_digest: str,
        now: datetime,
    ) -> ResourceLeaseRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"resource-lease:{request_scope}")
            prior = await self._require_locked(connection, request_scope, lease_id)
            self._require_digest(prior, expected_digest)
            if prior.status == ResourceLeaseStatus.RELEASED:
                return prior
            released = prior.model_copy(
                update={
                    "status": ResourceLeaseStatus.RELEASED,
                    "released_at": now,
                }
            )
            await self._write_locked(connection, released, version_bump=True)
            return released

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
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"resource-lease:{request_scope}")
            records = [
                await self._require_locked(connection, request_scope, lease_id)
                for lease_id in lease_ids
            ]
            for record in records:
                if record.request.lease_id in retain:
                    updated = record.model_copy(update={"status": ResourceLeaseStatus.RETAINED})
                    retained.append(record.request.lease_id)
                    await self._write_locked(
                        connection,
                        updated,
                        version_bump=True,
                        retained_for_wait=True,
                    )
                else:
                    updated = record.model_copy(
                        update={
                            "status": ResourceLeaseStatus.RELEASED,
                            "released_at": now,
                        }
                    )
                    released.append(record.request.lease_id)
                    await self._write_locked(
                        connection,
                        updated,
                        version_bump=True,
                        retained_for_wait=False,
                    )
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
    ) -> tuple[ResourceLeaseRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"resource-lease:{request_scope}")
            return await self._expire_due_locked(connection, request_scope=request_scope, now=now)

    async def _expire_due_locked(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        now: datetime,
    ) -> tuple[ResourceLeaseRecord, ...]:
        rows = await connection.fetch(
            """
            SELECT lease_id, lease_payload
            FROM belllabs_control.execution_resource_leases
            WHERE request_scope = $1
              AND status = ANY($2::text[])
              AND expires_at IS NOT NULL
              AND expires_at <= $3
            FOR UPDATE
            """,
            request_scope,
            [status.value for status in _LIVE_LEASE_STATUSES],
            now,
        )
        expired: list[ResourceLeaseRecord] = []
        for row in rows:
            prior = ResourceLeaseRecord.model_validate(_json(row["lease_payload"]))
            if prior.status not in {
                ResourceLeaseStatus.ACQUIRED,
                ResourceLeaseStatus.RETAINED,
                ResourceLeaseStatus.REQUESTED,
            }:
                continue
            updated = prior.model_copy(update={"status": ResourceLeaseStatus.EXPIRED})
            await self._write_locked(connection, updated, version_bump=True)
            expired.append(updated)
        return tuple(expired)

    async def _active_counts_locked(
        self,
        connection: asyncpg.Connection,
        request_scope: str,
        *,
        now: datetime,
    ) -> Counter[ResourceKind]:
        rows = await connection.fetch(
            """
            SELECT lease_payload
            FROM belllabs_control.execution_resource_leases
            WHERE request_scope = $1
              AND status = ANY($2::text[])
            """,
            request_scope,
            [status.value for status in _CAPACITY_STATUSES],
        )
        counts: Counter[ResourceKind] = Counter()
        for row in rows:
            record = ResourceLeaseRecord.model_validate(_json(row["lease_payload"]))
            if record.expires_at is not None and record.expires_at <= now:
                continue
            counts.update(record.request.resources)
        return counts

    async def _require_locked(
        self,
        connection: asyncpg.Connection,
        request_scope: str,
        lease_id: str,
    ) -> ResourceLeaseRecord:
        payload = await connection.fetchval(
            """
            SELECT lease_payload
            FROM belllabs_control.execution_resource_leases
            WHERE request_scope = $1 AND lease_id = $2
            FOR UPDATE
            """,
            request_scope,
            lease_id,
        )
        if payload is None:
            raise LookupError("resource lease not found in request scope")
        return ResourceLeaseRecord.model_validate(_json(payload))

    async def _write_locked(
        self,
        connection: asyncpg.Connection,
        record: ResourceLeaseRecord,
        *,
        version_bump: bool,
        renewed_at: datetime | None = None,
        retained_for_wait: bool | None = None,
    ) -> None:
        version = await connection.fetchval(
            """
            SELECT version
            FROM belllabs_control.execution_resource_leases
            WHERE request_scope = $1 AND lease_id = $2
            """,
            record.request.request_scope,
            record.request.lease_id,
        )
        next_version = int(version) + 1 if version_bump else int(version)
        await connection.execute(
            """
            UPDATE belllabs_control.execution_resource_leases
            SET status = $3,
                retained_for_wait = COALESCE($4, retained_for_wait),
                version = $5,
                acquired_at = $6,
                renewed_at = COALESCE($7, renewed_at),
                expires_at = $8,
                released_at = $9,
                lease_payload = $10::jsonb
            WHERE request_scope = $1 AND lease_id = $2
            """,
            record.request.request_scope,
            record.request.lease_id,
            record.status.value,
            retained_for_wait,
            next_version,
            record.acquired_at,
            renewed_at,
            record.expires_at,
            record.released_at,
            _dump(record),
        )

    @staticmethod
    def _require_digest(record: ResourceLeaseRecord, expected_digest: str) -> None:
        if record.canonical_digest != expected_digest:
            raise IdempotencyConflict("resource lease digest does not match")


class PostgresForkRepository:
    """Durable fork request/admission/receipt state for process-loss recovery."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def guard(self, request: ForkRequest) -> AsyncIterator[None]:
        key = f"fork-execution:{request.source_epoch.request_scope}:{request.request_id}"
        async with self._pool.acquire() as connection:
            await connection.execute(
                "SELECT pg_advisory_lock(hashtextextended($1, 0))",
                key,
            )
            try:
                yield
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                    key,
                )

    async def reserve(self, request: ForkRequest) -> bool:
        scope = request.source_epoch.request_scope
        digest = sha256_digest(request.model_dump(mode="json"))
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            prior = await connection.fetchrow(
                """
                SELECT request_digest, request_payload
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1
                  AND (request_id = $2 OR idempotency_key = $3)
                FOR UPDATE
                """,
                scope,
                request.request_id,
                request.idempotency_key,
            )
            if prior is not None:
                persisted = ForkRequest.model_validate(_json(prior["request_payload"]))
                if prior["request_digest"] != digest or persisted != request:
                    raise IdempotencyConflict("fork identity has conflicting intent")
                return False
            binding_id = await connection.fetchval(
                """
                SELECT binding_id
                FROM belllabs_control.runtime_execution_bindings
                WHERE request_scope = $1 AND belllabs_run_id = $2
                  AND execution_epoch = $3
                """,
                scope,
                request.source_epoch.belllabs_run_id,
                request.source_epoch.execution_epoch,
            )
            if binding_id is None:
                raise LookupError("fork source binding is unavailable")
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_fork_requests (
                    request_scope, request_id, idempotency_key, source_binding_id,
                    request_digest, request_payload, status, requested_at,
                    updated_at, retain_until
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'reserved', $7, $7, $8)
                """,
                scope,
                request.request_id,
                request.idempotency_key,
                binding_id,
                digest,
                _dump(request),
                request.requested_at,
                request.requested_at + timedelta(days=RETENTION_DAYS),
            )
            return True

    async def get(self, request_scope: str, request_id: str) -> ForkReceipt | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT receipt_payload
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2
                """,
                request_scope,
                request_id,
            )
        return ForkReceipt.model_validate(_json(payload)) if payload else None

    async def claim_admission(self, request: ForkRequest) -> bool:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            result = await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET status = 'admitting', updated_at = $3
                WHERE request_scope = $1 AND request_id = $2 AND status = 'reserved'
                """,
                scope,
                request.request_id,
                request.requested_at,
            )
            return _rowcount(result) == 1

    async def release_admission_claim(self, request: ForkRequest) -> None:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET status = 'reserved', updated_at = $3
                WHERE request_scope = $1 AND request_id = $2 AND status = 'admitting'
                """,
                scope,
                request.request_id,
                request.requested_at,
            )

    async def claim_copy(self, request: ForkRequest) -> bool:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            result = await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET status = 'copying', updated_at = $3
                WHERE request_scope = $1 AND request_id = $2 AND status = 'admitted'
                """,
                scope,
                request.request_id,
                request.requested_at,
            )
            return _rowcount(result) == 1

    async def release_copy_claim(self, request: ForkRequest) -> None:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET status = 'admitted', updated_at = $3
                WHERE request_scope = $1 AND request_id = $2 AND status = 'copying'
                """,
                scope,
                request.request_id,
                request.requested_at,
            )

    async def get_admission(
        self,
        request_scope: str,
        request_id: str,
    ) -> ForkAdmission | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT admission_payload
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2
                """,
                request_scope,
                request_id,
            )
        return ForkAdmission.model_validate(_json(payload)) if payload else None

    async def record_admission(
        self,
        request: ForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            row = await connection.fetchrow(
                """
                SELECT request_payload, admission_payload, status
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2
                FOR UPDATE
                """,
                scope,
                request.request_id,
            )
            if row is None or ForkRequest.model_validate(_json(row["request_payload"])) != request:
                raise LookupError("fork reservation is unavailable")
            if row["admission_payload"] is not None:
                persisted = ForkAdmission.model_validate(_json(row["admission_payload"]))
                if persisted != admission:
                    raise IdempotencyConflict("fork admission has conflicting identities")
                return persisted
            if row["status"] != "admitting":
                raise IdempotencyConflict("fork admission was not atomically claimed")
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET admission_payload = $3::jsonb, status = 'admitted', updated_at = $4
                WHERE request_scope = $1 AND request_id = $2
                """,
                scope,
                request.request_id,
                _dump(admission),
                request.requested_at,
            )
            return admission

    async def record(self, request: ForkRequest, receipt: ForkReceipt) -> ForkReceipt:
        scope = request.source_epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"fork:{scope}:{request.request_id}")
            row = await connection.fetchrow(
                """
                SELECT request_payload, receipt_payload, status
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2
                FOR UPDATE
                """,
                scope,
                request.request_id,
            )
            if row is None or ForkRequest.model_validate(_json(row["request_payload"])) != request:
                raise LookupError("fork reservation is unavailable")
            if row["receipt_payload"] is not None:
                persisted = ForkReceipt.model_validate(_json(row["receipt_payload"]))
                if persisted != receipt:
                    raise IdempotencyConflict("fork receipt has conflicting identities")
                return persisted
            if row["status"] != "copying":
                raise IdempotencyConflict("fork copy was not atomically claimed")
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET receipt_payload = $3::jsonb, status = 'accepted', updated_at = $4
                WHERE request_scope = $1 AND request_id = $2
                """,
                scope,
                request.request_id,
                _dump(receipt),
                receipt.recorded_at,
            )
            return receipt


class PostgresRuntimeIncidentRepository:
    """Idempotent, version-fact incident journal for Stage 3 reconciliation."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def reserve_incident(self, observation: RuntimeIncidentObservation) -> bool:
        scope = observation.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(
                connection,
                f"incident:{scope}:{observation.incident_type.value}:{observation.identity_digest}",
            )
            prior = await connection.fetchrow(
                """
                SELECT incident_id, incident_payload
                FROM belllabs_control.runtime_reconciliation_incidents
                WHERE request_scope = $1
                  AND incident_type = $2
                  AND identity_digest = $3
                FOR UPDATE
                """,
                scope,
                observation.incident_type.value,
                observation.identity_digest,
            )
            if prior is not None:
                persisted = RuntimeIncidentObservation.model_validate(
                    _json(prior["incident_payload"])["observation"]
                )
                if persisted != observation or prior["incident_id"] != observation.incident_id:
                    raise ValueError("incident identity has conflicting observations")
                return False
            by_id = await connection.fetchrow(
                """
                SELECT incident_payload
                FROM belllabs_control.runtime_reconciliation_incidents
                WHERE request_scope = $1 AND incident_id = $2
                FOR UPDATE
                """,
                scope,
                observation.incident_id,
            )
            if by_id is not None:
                persisted = RuntimeIncidentObservation.model_validate(
                    _json(by_id["incident_payload"])["observation"]
                )
                if persisted != observation:
                    raise ValueError("incident identity has conflicting observations")
                return False
            retain_until = observation.observed_at + timedelta(days=RETENTION_DAYS)
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_reconciliation_incidents (
                    incident_id, request_scope, binding_id, incident_type, severity,
                    status, identity_digest, before_version, after_version, actor_ref,
                    reason, evidence_refs, retry_at, incident_payload, version,
                    recorded_at, updated_at, retain_until
                )
                VALUES (
                    $1, $2, $3, $4, 'warning', 'open', $5, $6, NULL,
                    'service:runtime-reconciler', 'reserved', $7::jsonb, NULL,
                    $8::jsonb, 1, $9, $9, $10
                )
                """,
                observation.incident_id,
                scope,
                observation.binding_id,
                observation.incident_type.value,
                observation.identity_digest,
                observation.observed_version,
                _dump(list(observation.evidence_refs)),
                _dump({"observation": observation.model_dump(mode="json")}),
                observation.observed_at,
                retain_until,
            )
            return True

    async def record_incident_decision(
        self,
        observation: RuntimeIncidentObservation,
        decision: RuntimeIncidentDecision,
    ) -> RuntimeIncidentDecision:
        if (
            decision.incident_id != observation.incident_id
            or decision.request_scope != observation.request_scope
        ):
            raise ValueError("incident decision does not match observation identity")
        scope = observation.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(
                connection,
                f"incident:{scope}:{observation.incident_type.value}:{observation.identity_digest}",
            )
            prior = await connection.fetchrow(
                """
                SELECT incident_payload, version, status
                FROM belllabs_control.runtime_reconciliation_incidents
                WHERE request_scope = $1 AND incident_id = $2
                FOR UPDATE
                """,
                scope,
                observation.incident_id,
            )
            if prior is None:
                raise LookupError("incident reservation not found")
            payload = _json(prior["incident_payload"])
            persisted_observation = RuntimeIncidentObservation.model_validate(
                payload["observation"]
            )
            if persisted_observation != observation:
                raise ValueError("incident identity has conflicting observations")
            if "decision" in payload:
                persisted_decision = RuntimeIncidentDecision.model_validate(payload["decision"])
                if persisted_decision != decision:
                    raise ValueError("incident replay changed its decision")
                return persisted_decision
            status = _incident_status(decision)
            updated_payload = {
                "observation": observation.model_dump(mode="json"),
                "decision": decision.model_dump(mode="json"),
            }
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_reconciliation_incidents
                SET status = $3,
                    before_version = $4,
                    after_version = $5,
                    actor_ref = $6,
                    reason = $7,
                    evidence_refs = $8::jsonb,
                    retry_at = $9,
                    incident_payload = $10::jsonb,
                    version = $11,
                    updated_at = $12
                WHERE request_scope = $1 AND incident_id = $2
                """,
                scope,
                observation.incident_id,
                status,
                decision.before_version,
                decision.after_version,
                decision.actor_ref,
                decision.reason,
                _dump(list(decision.evidence_refs)),
                decision.retry_at,
                _dump(updated_payload),
                int(prior["version"]) + 1,
                observation.observed_at,
            )
            return decision

    async def record_repair_audit(
        self,
        record: RuntimeRepairAuditRecord,
    ) -> RuntimeRepairAuditRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, record.request_scope)
            await _lock(
                connection,
                f"repair-audit:{record.request_scope}:{record.audit_id}",
            )
            prior = await connection.fetchrow(
                """
                SELECT command_id, actor_id, reason, expected_belllabs_version,
                       expected_checkpoint_id, before_digest, after_digest,
                       evidence_refs, recorded_at, incident_id
                FROM belllabs_control.runtime_repair_audit
                WHERE request_scope = $1 AND audit_id = $2
                """,
                record.request_scope,
                record.audit_id,
            )
            if prior is not None:
                persisted = RuntimeRepairAuditRecord(
                    request_scope=record.request_scope,
                    audit_id=record.audit_id,
                    incident_id=prior["incident_id"],
                    command_id=prior["command_id"],
                    actor_id=prior["actor_id"],
                    reason=prior["reason"],
                    expected_belllabs_version=prior["expected_belllabs_version"],
                    expected_checkpoint_id=prior["expected_checkpoint_id"],
                    before_digest=prior["before_digest"],
                    after_digest=prior["after_digest"],
                    evidence_refs=tuple(_json(prior["evidence_refs"])),
                    recorded_at=prior["recorded_at"],
                )
                if persisted != record:
                    raise IdempotencyConflict("repair audit identity has conflicting facts")
                return persisted
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_repair_audit (
                    request_scope, audit_id, incident_id, command_id, actor_id,
                    reason, expected_belllabs_version, expected_checkpoint_id,
                    before_digest, after_digest, evidence_refs, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
                """,
                record.request_scope,
                record.audit_id,
                record.incident_id,
                record.command_id,
                record.actor_id,
                record.reason,
                record.expected_belllabs_version,
                record.expected_checkpoint_id,
                record.before_digest,
                record.after_digest,
                _dump(list(record.evidence_refs)),
                record.recorded_at,
            )
            return record


class PostgresStage3RetentionRepository:
    """Audited tenant-scoped deletion for Stage 3 records past retain_until."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        authority: RetentionAuthority | None = None,
    ) -> None:
        self._pool = pool
        self._authority = authority or DenyByDefaultRetentionAuthority()

    async def delete_expired(
        self,
        *,
        request_scope: str,
        record_class: Literal[
            "checkpoint",
            "event",
            "incident",
            "lineage",
            "decision",
            "fork",
        ],
        cutoff_at: datetime,
        actor_id: str,
        reason: str,
        deletion_id: str,
        recorded_at: datetime,
    ) -> int:
        if not await self._authority.authorize_deletion(
            request_scope=request_scope,
            actor_id=actor_id,
            record_class=record_class,
        ):
            raise PermissionError("retention deletion lacks scoped operator authorization")
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"retention:{request_scope}:{record_class}")
            prior = await connection.fetchrow(
                """
                SELECT deleted_count, record_class, cutoff_at, actor_id, reason
                FROM belllabs_control.runtime_retention_deletion_audit
                WHERE request_scope = $1 AND deletion_id = $2
                """,
                request_scope,
                deletion_id,
            )
            if prior is not None:
                if (
                    prior["record_class"] != record_class
                    or prior["cutoff_at"] != cutoff_at
                    or prior["actor_id"] != actor_id
                    or prior["reason"] != reason
                ):
                    raise IdempotencyConflict("retention deletion identity has conflicting facts")
                return int(prior["deleted_count"])
            if record_class == "lineage":
                deleted_count = await self._delete_expired_lineage(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            elif record_class == "incident":
                deleted_count = await self._delete_expired_incidents(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            elif record_class == "checkpoint":
                deleted_count = await self._delete_expired_checkpoints(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            elif record_class == "event":
                deleted_count = await self._delete_expired_events(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            elif record_class == "decision":
                deleted_count = await self._delete_expired_decisions(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            else:
                deleted_count = await self._delete_expired_forks(
                    connection,
                    request_scope=request_scope,
                    cutoff_at=cutoff_at,
                )
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_retention_deletion_audit (
                    request_scope, deletion_id, record_class, cutoff_at,
                    deleted_count, actor_id, reason, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                request_scope,
                deletion_id,
                record_class,
                cutoff_at,
                deleted_count,
                actor_id,
                reason,
                recorded_at,
            )
            return deleted_count

    async def _delete_expired_lineage(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        lineage_ids = [
            row["lineage_id"]
            for row in await connection.fetch(
                """
                SELECT lineage_id
                FROM belllabs_control.runtime_lineage_records
                WHERE request_scope = $1 AND retain_until <= $2
                FOR UPDATE
                """,
                request_scope,
                cutoff_at,
            )
        ]
        if not lineage_ids:
            return 0
        await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_lineage_edges
            WHERE request_scope = $1 AND lineage_id = ANY($2::text[])
            """,
            request_scope,
            lineage_ids,
        )
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_lineage_records
            WHERE request_scope = $1 AND lineage_id = ANY($2::text[])
            """,
            request_scope,
            lineage_ids,
        )
        return _rowcount(result)

    async def _delete_expired_incidents(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        incident_ids = [
            row["incident_id"]
            for row in await connection.fetch(
                """
                SELECT incident_id
                FROM belllabs_control.runtime_reconciliation_incidents
                WHERE request_scope = $1 AND retain_until <= $2
                FOR UPDATE
                """,
                request_scope,
                cutoff_at,
            )
        ]
        if not incident_ids:
            return 0
        await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_repair_audit
            WHERE request_scope = $1 AND incident_id = ANY($2::text[])
            """,
            request_scope,
            incident_ids,
        )
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_reconciliation_incidents
            WHERE request_scope = $1 AND incident_id = ANY($2::text[])
            """,
            request_scope,
            incident_ids,
        )
        return _rowcount(result)

    async def _delete_expired_checkpoints(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_checkpoint_observations
            WHERE request_scope = $1 AND retain_until <= $2
            """,
            request_scope,
            cutoff_at,
        )
        return _rowcount(result)

    async def _delete_expired_events(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.outbox event
            USING belllabs_control.workflow_runs run
            WHERE event.aggregate_id = run.run_id
              AND run.request_scope = $1
              AND event.recorded_at + interval '90 days' <= $2
              AND event.delivered_at IS NOT NULL
            """,
            request_scope,
            cutoff_at,
        )
        return _rowcount(result)

    async def _delete_expired_decisions(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        decision_ids = [
            row["decision_id"]
            for row in await connection.fetch(
                """
                SELECT decision_id
                FROM belllabs_control.runtime_decision_requests
                WHERE request_scope = $1 AND retain_until <= $2
                FOR UPDATE
                """,
                request_scope,
                cutoff_at,
            )
        ]
        if not decision_ids:
            return 0
        await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_decision_responses
            WHERE request_scope = $1 AND decision_id = ANY($2::text[])
            """,
            request_scope,
            decision_ids,
        )
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_decision_requests
            WHERE request_scope = $1 AND decision_id = ANY($2::text[])
            """,
            request_scope,
            decision_ids,
        )
        return _rowcount(result)

    async def _delete_expired_forks(
        self,
        connection: asyncpg.Connection,
        *,
        request_scope: str,
        cutoff_at: datetime,
    ) -> int:
        result = await connection.execute(
            """
            DELETE FROM belllabs_control.runtime_fork_requests
            WHERE request_scope = $1 AND retain_until <= $2
            """,
            request_scope,
            cutoff_at,
        )
        return _rowcount(result)


def _incident_status(decision: RuntimeIncidentDecision) -> str:
    if decision.disposition == "automatic":
        return "resolved"
    if decision.disposition == "retry_scheduled":
        return "retry_scheduled"
    return "operator_required"


def _split_actor_ref(actor_ref: str) -> tuple[str, str]:
    if ":" in actor_ref:
        actor_type, _, remainder = actor_ref.partition(":")
        return actor_type or "actor", remainder or actor_ref
    return "actor", actor_ref


def _rowcount(command_tag: str) -> int:
    parts = command_tag.split()
    return int(parts[-1]) if parts and parts[-1].isdigit() else 0


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


async def _lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        key,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
