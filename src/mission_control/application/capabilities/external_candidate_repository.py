from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.capabilities.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
)
from mission_control.domain.authoring.canonical import sha256_digest


class ExternalCandidatePersistenceError(RuntimeError):
    pass


class ExternalCandidateNotFound(LookupError):
    pass


class PersistenceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PersistedDiscoveryEvidence(PersistenceContract):
    evidence_id: str = Field(min_length=1)
    record_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence: ExternalDiscoveryEvidence
    recorded_at: datetime


class PersistedExternalCandidate(PersistenceContract):
    candidate_record_id: str = Field(min_length=1)
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence_id: str = Field(min_length=1)
    candidate: ExternalDiscoveryCandidate
    recorded_at: datetime


class InMemoryExternalCandidateRepository:
    """Append-only discovery evidence and candidate observations."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = asyncio.Lock()
        self._evidence: dict[str, PersistedDiscoveryEvidence] = {}
        self._candidates: dict[str, PersistedExternalCandidate] = {}

    async def record(self, batch: ExternalDiscoveryBatch) -> ExternalDiscoveryBatch:
        recorded_at = self._clock()
        evidence_records = tuple(
            _persisted_evidence(evidence, recorded_at=recorded_at) for evidence in batch.evidence
        )
        evidence_by_digest = _evidence_by_digest(evidence_records)
        candidate_records = tuple(
            _persisted_candidate(
                candidate,
                evidence=_candidate_evidence(candidate, evidence_by_digest),
                recorded_at=recorded_at,
            )
            for candidate in batch.candidates
        )
        async with self._lock:
            for evidence in evidence_records:
                _append_immutable(
                    self._evidence,
                    evidence.evidence_id,
                    evidence,
                    subject="external discovery evidence",
                )
            for candidate in candidate_records:
                _append_immutable(
                    self._candidates,
                    candidate.candidate_record_id,
                    candidate,
                    subject="external discovery candidate",
                )
        return batch.model_copy(
            update={
                "candidates": tuple(record.candidate for record in candidate_records),
            }
        )

    async def get_candidate(self, candidate_id: str) -> PersistedExternalCandidate:
        matches = [
            record
            for record in self._candidates.values()
            if record.candidate.candidate_id == candidate_id
        ]
        if not matches:
            raise ExternalCandidateNotFound(f"candidate not found: {candidate_id}")
        return max(
            matches,
            key=lambda record: (
                record.candidate.discovered_at,
                record.recorded_at,
                record.candidate_record_id,
            ),
        ).model_copy(deep=True)

    async def get_candidate_record(
        self,
        candidate_record_id: str,
    ) -> PersistedExternalCandidate:
        try:
            return self._candidates[candidate_record_id].model_copy(deep=True)
        except KeyError as error:
            raise ExternalCandidateNotFound(
                f"candidate record not found: {candidate_record_id}"
            ) from error

    async def get_evidence(self, evidence_id: str) -> PersistedDiscoveryEvidence:
        try:
            return self._evidence[evidence_id].model_copy(deep=True)
        except KeyError as error:
            raise ExternalCandidateNotFound(
                f"discovery evidence not found: {evidence_id}"
            ) from error

    async def list_candidate_records(
        self,
        candidate_id: str,
    ) -> tuple[PersistedExternalCandidate, ...]:
        matches = (
            record
            for record in self._candidates.values()
            if record.candidate.candidate_id == candidate_id
        )
        return tuple(
            record.model_copy(deep=True)
            for record in sorted(
                matches,
                key=lambda item: (
                    item.candidate.discovered_at,
                    item.recorded_at,
                    item.candidate_record_id,
                ),
            )
        )


def _persisted_evidence(
    evidence: ExternalDiscoveryEvidence,
    *,
    recorded_at: datetime,
) -> PersistedDiscoveryEvidence:
    record_digest = sha256_digest(evidence)
    return PersistedDiscoveryEvidence(
        evidence_id=f"discovery-evidence:{record_digest}",
        record_digest=record_digest,
        evidence=evidence,
        recorded_at=recorded_at,
    )


def _persisted_candidate(
    candidate: ExternalDiscoveryCandidate,
    *,
    evidence: PersistedDiscoveryEvidence,
    recorded_at: datetime,
) -> PersistedExternalCandidate:
    raw_response_ref = (
        f"mission-control://external-discovery-evidence/{evidence.evidence_id}#sanitized-metadata"
    )
    enriched = candidate.model_copy(update={"raw_response_ref": raw_response_ref})
    content_digest = sha256_digest(enriched)
    return PersistedExternalCandidate(
        candidate_record_id=f"candidate-record:{content_digest}",
        content_digest=content_digest,
        evidence_id=evidence.evidence_id,
        candidate=enriched,
        recorded_at=recorded_at,
    )


def _evidence_by_digest(
    evidence: tuple[PersistedDiscoveryEvidence, ...],
) -> dict[tuple[str, str, str], PersistedDiscoveryEvidence]:
    result: dict[tuple[str, str, str], PersistedDiscoveryEvidence] = {}
    for record in evidence:
        key = (
            record.evidence.source.value,
            record.evidence.query,
            record.evidence.raw_response_digest,
        )
        result[key] = record
    return result


def _candidate_evidence(
    candidate: ExternalDiscoveryCandidate,
    evidence_by_digest: dict[tuple[str, str, str], PersistedDiscoveryEvidence],
) -> PersistedDiscoveryEvidence:
    key = (
        candidate.source.value,
        candidate.query,
        candidate.raw_response_digest,
    )
    try:
        return evidence_by_digest[key]
    except KeyError as error:
        raise ExternalCandidatePersistenceError(
            "candidate does not reference evidence from its discovery batch"
        ) from error


def _append_immutable[T](
    records: dict[str, T],
    identity: str,
    value: T,
    *,
    subject: str,
) -> None:
    prior = records.get(identity)
    if prior is None:
        records[identity] = value
    elif prior != value:
        raise ExternalCandidatePersistenceError(f"immutable {subject} identity conflict")
