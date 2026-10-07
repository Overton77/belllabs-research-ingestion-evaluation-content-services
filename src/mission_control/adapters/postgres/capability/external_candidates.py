"""Installation-catalog append-only discovery and inspection custody.

Records live in ``mission_control.capability_discovery_record`` under the trusted
installation catalog scope (``mc/{installation}/{app}/catalog``); a catalog scope is never
coerced into a tenant request scope. Custody is evidence only: nothing here admits,
installs or grants execution permission for a discovered capability.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    CATALOG_SERVICE_ACTOR,
    json_value,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope, parse_catalog_scope
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionReport,
    ExternalCandidateInspectionWorkspace,
)
from mission_control.application.capabilities.external_candidate_repository import (
    ExternalCandidateNotFound,
    PersistedDiscoveryEvidence,
    PersistedExternalCandidate,
    _candidate_evidence,
    _evidence_by_digest,
    _persisted_candidate,
    _persisted_evidence,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalDiscoveryBatch,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.policies.errors import IdempotencyConflict


@dataclass(frozen=True)
class _Stored:
    identity: str
    payload: dict[str, Any]
    recorded_at: datetime


class _CatalogCustody:
    """Immutable put/get over the discovery custody table in one catalog scope."""

    def __init__(self, pool: asyncpg.Pool, catalog_scope: str, actor_ref: str) -> None:
        if not catalog_scope:
            raise ValueError("trusted catalog scope is required")
        self.pool = pool
        self.scope = catalog_scope
        self.installation: tuple[UUID, str] = parse_catalog_scope(catalog_scope)
        self.actor = actor_ref

    async def bind(self, connection: asyncpg.Connection) -> None:
        await apply_catalog_scope(connection, *self.installation)

    async def put_on(
        self,
        connection: asyncpg.Connection,
        *,
        contract: str,
        identity: str,
        payload: dict[str, Any],
        recorded_at: datetime,
    ) -> _Stored:
        if not identity or not contract:
            raise ValueError("custody contract and identity are required")
        if recorded_at.utcoffset() is None:
            raise ValueError("custody observation time must be timezone aware")
        digest = sha256_digest(payload)
        # A conflicting concurrent insert is awaited by PostgreSQL; the following read
        # sees the committed row under READ COMMITTED and compares immutable content.
        await connection.execute(
            """INSERT INTO mission_control.capability_discovery_record
               (installation_id, application_id, discovery_record_id, contract, record_key,
                payload, payload_digest, recorded_at, created_at, created_by_actor_ref)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,clock_timestamp(),$9)
               ON CONFLICT (installation_id, application_id, contract, record_key) DO NOTHING""",
            *self.installation,
            uuid7(),
            contract,
            identity,
            json.dumps(payload, ensure_ascii=False, allow_nan=False),
            digest,
            recorded_at,
            self.actor,
        )
        stored = await self.get_on(connection, contract=contract, identity=identity)
        if stored is None:
            raise IdempotencyConflict("custody record is unavailable after insert")
        if sha256_digest(stored.payload) != digest:
            raise IdempotencyConflict("custody record identity has conflicting content")
        return stored

    async def get_on(
        self, connection: asyncpg.Connection, *, contract: str, identity: str
    ) -> _Stored | None:
        row = await connection.fetchrow(
            """SELECT record_key, payload, payload_digest, recorded_at
               FROM mission_control.capability_discovery_record
               WHERE installation_id=$1 AND application_id=$2 AND contract=$3
                 AND record_key=$4""",
            *self.installation,
            contract,
            identity,
        )
        return _stored(row) if row is not None else None

    async def put(self, **kwargs: Any) -> _Stored:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.bind(connection)
            return await self.put_on(connection, **kwargs)

    async def get(self, *, contract: str, identity: str) -> _Stored | None:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.bind(connection)
            return await self.get_on(connection, contract=contract, identity=identity)

    async def list(self, *, contract: str) -> tuple[_Stored, ...]:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.bind(connection)
            rows = await connection.fetch(
                """SELECT record_key, payload, payload_digest, recorded_at
                   FROM mission_control.capability_discovery_record
                   WHERE installation_id=$1 AND application_id=$2 AND contract=$3
                   ORDER BY record_key""",
                *self.installation,
                contract,
            )
            return tuple(_stored(row) for row in rows)


def _stored(row: asyncpg.Record) -> _Stored:
    payload = json_value(row["payload"])
    if not isinstance(payload, dict) or sha256_digest(payload) != row["payload_digest"]:
        raise ValueError("discovery custody payload digest mismatch")
    return _Stored(row["record_key"], payload, row["recorded_at"])


class PostgresExternalCandidateRepository:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        catalog_scope: str,
        clock: Callable[[], datetime] | None = None,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
    ) -> None:
        if not catalog_scope:
            raise ValueError("trusted catalog scope is required")
        self.pool = pool
        self.scope = catalog_scope
        self.store = _CatalogCustody(pool, catalog_scope, actor_ref)
        self.clock = clock or (lambda: datetime.now(UTC))

    async def record(self, batch: ExternalDiscoveryBatch) -> ExternalDiscoveryBatch:
        now = self.clock()
        evidence = tuple(_persisted_evidence(item, recorded_at=now) for item in batch.evidence)
        index = _evidence_by_digest(evidence)
        candidates = tuple(
            _persisted_candidate(item, evidence=_candidate_evidence(item, index), recorded_at=now)
            for item in batch.candidates
        )
        async with self.pool.acquire() as connection, connection.transaction():
            await self.store.bind(connection)
            for item in evidence:
                await self.store.put_on(
                    connection,
                    contract="external-discovery-evidence/1",
                    identity=item.evidence_id,
                    payload=stable_json_dump(item.evidence),
                    recorded_at=now,
                )
            for candidate in candidates:
                await self.store.put_on(
                    connection,
                    contract="external-discovery-candidate/1",
                    identity=candidate.candidate_record_id,
                    payload={
                        "candidate": stable_json_dump(candidate.candidate),
                        "evidence_id": candidate.evidence_id,
                    },
                    recorded_at=now,
                )
        return batch.model_copy(update={"candidates": tuple(item.candidate for item in candidates)})

    async def get_evidence(self, evidence_id: str) -> PersistedDiscoveryEvidence:
        row = await self.store.get(contract="external-discovery-evidence/1", identity=evidence_id)
        if row is None:
            raise ExternalCandidateNotFound("discovery evidence not found")
        from mission_control.application.capabilities.external_capability_discovery import (
            ExternalDiscoveryEvidence,
        )

        return _persisted_evidence(
            ExternalDiscoveryEvidence.model_validate(row.payload), recorded_at=row.recorded_at
        )

    async def get_candidate_record(self, candidate_record_id: str) -> PersistedExternalCandidate:
        row = await self.store.get(
            contract="external-discovery-candidate/1", identity=candidate_record_id
        )
        if row is None:
            raise ExternalCandidateNotFound("candidate record not found")
        return _candidate(row.identity, row.payload, row.recorded_at)

    async def list_candidate_records(
        self, candidate_id: str
    ) -> tuple[PersistedExternalCandidate, ...]:
        rows = await self.store.list(contract="external-discovery-candidate/1")
        records = (_candidate(row.identity, row.payload, row.recorded_at) for row in rows)
        return tuple(
            sorted(
                (r for r in records if r.candidate.candidate_id == candidate_id),
                key=lambda r: (r.candidate.discovered_at, r.recorded_at, r.candidate_record_id),
            )
        )

    async def get_candidate(self, candidate_id: str) -> PersistedExternalCandidate:
        records = await self.list_candidate_records(candidate_id)
        if not records:
            raise ExternalCandidateNotFound("candidate not found")
        return records[-1]


def _candidate(identity: str, payload: dict, recorded_at: datetime) -> PersistedExternalCandidate:
    from mission_control.application.capabilities.external_capability_discovery import (
        ExternalDiscoveryCandidate,
    )

    candidate = ExternalDiscoveryCandidate.model_validate(payload["candidate"])
    digest = sha256_digest(candidate)
    if identity != f"candidate-record:{digest}":
        raise ValueError("candidate record identity does not match content")
    return PersistedExternalCandidate(
        candidate_record_id=identity,
        content_digest=digest,
        evidence_id=payload["evidence_id"],
        candidate=candidate,
        recorded_at=recorded_at,
    )


class PostgresExternalCandidateInspectionRepository:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        catalog_scope: str,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
    ) -> None:
        if not catalog_scope:
            raise ValueError("trusted catalog scope is required")
        self.pool = pool
        self.scope = catalog_scope
        self.store = _CatalogCustody(pool, catalog_scope, actor_ref)

    async def append_workspace(
        self, workspace: ExternalCandidateInspectionWorkspace
    ) -> ExternalCandidateInspectionWorkspace:
        await self.store.put(
            contract="external-inspection-workspace/1",
            identity=workspace.workspace_id,
            payload=stable_json_dump(workspace),
            recorded_at=workspace.allocated_at,
        )
        return workspace

    async def append_report(
        self, report: ExternalCandidateInspectionReport
    ) -> ExternalCandidateInspectionReport:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.store.bind(connection)
            # One immutable report per workspace, atomically bound with the report body.
            await self.store.put_on(
                connection,
                contract="external-inspection-workspace-report/1",
                identity=report.workspace_id,
                payload={"inspection_id": report.inspection_id},
                recorded_at=report.completed_at,
            )
            await self.store.put_on(
                connection,
                contract="external-inspection-report/1",
                identity=report.inspection_id,
                payload=stable_json_dump(report),
                recorded_at=report.completed_at,
            )
        return report

    async def get_report(self, inspection_id: str) -> ExternalCandidateInspectionReport:
        row = await self.store.get(contract="external-inspection-report/1", identity=inspection_id)
        if row is None:
            raise ExternalCandidateNotFound("inspection report not found")
        return ExternalCandidateInspectionReport.model_validate(row.payload)

    async def get_workspace(self, workspace_id: str) -> ExternalCandidateInspectionWorkspace:
        row = await self.store.get(
            contract="external-inspection-workspace/1", identity=workspace_id
        )
        if row is None:
            raise ExternalCandidateNotFound("inspection workspace not found")
        return ExternalCandidateInspectionWorkspace.model_validate(row.payload)
