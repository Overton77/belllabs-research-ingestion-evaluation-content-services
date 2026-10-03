"""Scoped append-only discovery and inspection custody; no installation side effects."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import asyncpg

from mission_control.adapters.postgres.documents import PostgresDocumentStore
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
from mission_control.domain.authoring.canonical import stable_json_dump


class PostgresExternalCandidateRepository:
    def __init__(
        self, pool: asyncpg.Pool, *, catalog_scope: str, clock: Callable[[], datetime] | None = None
    ) -> None:
        if not catalog_scope:
            raise ValueError("trusted catalog scope is required")
        self.pool = pool
        self.scope = catalog_scope
        self.store = PostgresDocumentStore(pool)
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
            await self.store.set_scope(connection, self.scope)
            for item in evidence:
                await self.store.put_on(
                    connection,
                    request_scope=self.scope,
                    contract="external-discovery-evidence/1",
                    identity=item.evidence_id,
                    payload=stable_json_dump(item.evidence),
                    recorded_at=now,
                )
            for candidate in candidates:
                await self.store.put_on(
                    connection,
                    request_scope=self.scope,
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
        row = await self.store.get(
            request_scope=self.scope, contract="external-discovery-evidence/1", identity=evidence_id
        )
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
            request_scope=self.scope,
            contract="external-discovery-candidate/1",
            identity=candidate_record_id,
        )
        if row is None:
            raise ExternalCandidateNotFound("candidate record not found")
        return _candidate(row.identity, row.payload, row.recorded_at)

    async def list_candidate_records(
        self, candidate_id: str
    ) -> tuple[PersistedExternalCandidate, ...]:
        rows = await self.store.list(
            request_scope=self.scope, contract="external-discovery-candidate/1"
        )
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
    from mission_control.domain.authoring.canonical import sha256_digest

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
    def __init__(self, pool: asyncpg.Pool, *, catalog_scope: str) -> None:
        if not catalog_scope:
            raise ValueError("trusted catalog scope is required")
        self.pool = pool
        self.scope = catalog_scope
        self.store = PostgresDocumentStore(pool)

    async def append_workspace(
        self, workspace: ExternalCandidateInspectionWorkspace
    ) -> ExternalCandidateInspectionWorkspace:
        await self.store.put(
            request_scope=self.scope,
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
            await self.store.set_scope(connection, self.scope)
            # One immutable report per workspace, atomically bound with the report body.
            await self.store.put_on(
                connection,
                request_scope=self.scope,
                contract="external-inspection-workspace-report/1",
                identity=report.workspace_id,
                payload={"inspection_id": report.inspection_id},
                recorded_at=report.completed_at,
            )
            await self.store.put_on(
                connection,
                request_scope=self.scope,
                contract="external-inspection-report/1",
                identity=report.inspection_id,
                payload=stable_json_dump(report),
                recorded_at=report.completed_at,
            )
        return report

    async def get_report(self, inspection_id: str) -> ExternalCandidateInspectionReport:
        row = await self.store.get(
            request_scope=self.scope,
            contract="external-inspection-report/1",
            identity=inspection_id,
        )
        if row is None:
            raise ExternalCandidateNotFound("inspection report not found")
        return ExternalCandidateInspectionReport.model_validate(row.payload)

    async def get_workspace(self, workspace_id: str) -> ExternalCandidateInspectionWorkspace:
        row = await self.store.get(
            request_scope=self.scope,
            contract="external-inspection-workspace/1",
            identity=workspace_id,
        )
        if row is None:
            raise ExternalCandidateNotFound("inspection workspace not found")
        return ExternalCandidateInspectionWorkspace.model_validate(row.payload)
