from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import DuplicateKeyError

from app.application.web_research.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
)
from app.domain.control_plane.canonical import sha256_digest
from app.models.external_capability import (
    ExternalDiscoveryCandidateDocument,
    ExternalDiscoveryEvidenceDocument,
)


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


class BeanieExternalCandidateRepository:
    """Production Mongo adapter for immutable sanitized discovery records."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))

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
        for evidence_record in evidence_records:
            await self._append_evidence(evidence_record)
        for candidate_record in candidate_records:
            await self._append_candidate(candidate_record)
        return batch.model_copy(
            update={
                "candidates": tuple(record.candidate for record in candidate_records),
            }
        )

    async def _append_evidence(self, record: PersistedDiscoveryEvidence) -> None:
        document = ExternalDiscoveryEvidenceDocument(
            evidence_id=record.evidence_id,
            source=record.evidence.source.value,
            source_version=record.evidence.source_version,
            query=record.evidence.query,
            retrieved_at=record.evidence.retrieved_at,
            raw_response_digest=record.evidence.raw_response_digest,
            raw_response_size_bytes=record.evidence.raw_response_size_bytes,
            record_digest=record.record_digest,
            payload=record.evidence.model_dump(mode="json"),
            recorded_at=record.recorded_at,
        )
        try:
            await document.insert()
        except DuplicateKeyError:
            existing = await ExternalDiscoveryEvidenceDocument.find_one(
                ExternalDiscoveryEvidenceDocument.evidence_id == record.evidence_id
            )
            if existing is None or _evidence_from_document(existing) != record:
                raise ExternalCandidatePersistenceError(
                    "immutable external discovery evidence identity conflict"
                ) from None

    async def _append_candidate(self, record: PersistedExternalCandidate) -> None:
        candidate = record.candidate
        document = ExternalDiscoveryCandidateDocument(
            candidate_record_id=record.candidate_record_id,
            candidate_id=candidate.candidate_id,
            evidence_id=record.evidence_id,
            source=candidate.source.value,
            upstream_identity=candidate.upstream_identity,
            upstream_version=candidate.upstream_version,
            query=candidate.query,
            discovered_at=candidate.discovered_at,
            raw_response_digest=candidate.raw_response_digest,
            content_digest=record.content_digest,
            payload=candidate.model_dump(mode="json"),
            recorded_at=record.recorded_at,
        )
        try:
            await document.insert()
        except DuplicateKeyError:
            existing = await ExternalDiscoveryCandidateDocument.find_one(
                ExternalDiscoveryCandidateDocument.candidate_record_id == record.candidate_record_id
            )
            if existing is None or _candidate_from_document(existing) != record:
                raise ExternalCandidatePersistenceError(
                    "immutable external discovery candidate identity conflict"
                ) from None

    async def get_candidate(self, candidate_id: str) -> PersistedExternalCandidate:
        document = (
            await ExternalDiscoveryCandidateDocument.find(
                ExternalDiscoveryCandidateDocument.candidate_id == candidate_id
            )
            .sort("-discovered_at", "-recorded_at", "-candidate_record_id")
            .first_or_none()
        )
        if document is None:
            raise ExternalCandidateNotFound(f"candidate not found: {candidate_id}")
        return _candidate_from_document(document)

    async def get_candidate_record(
        self,
        candidate_record_id: str,
    ) -> PersistedExternalCandidate:
        document = await ExternalDiscoveryCandidateDocument.find_one(
            ExternalDiscoveryCandidateDocument.candidate_record_id == candidate_record_id
        )
        if document is None:
            raise ExternalCandidateNotFound(f"candidate record not found: {candidate_record_id}")
        return _candidate_from_document(document)

    async def get_evidence(self, evidence_id: str) -> PersistedDiscoveryEvidence:
        document = await ExternalDiscoveryEvidenceDocument.find_one(
            ExternalDiscoveryEvidenceDocument.evidence_id == evidence_id
        )
        if document is None:
            raise ExternalCandidateNotFound(f"discovery evidence not found: {evidence_id}")
        return _evidence_from_document(document)

    async def list_candidate_records(
        self,
        candidate_id: str,
    ) -> tuple[PersistedExternalCandidate, ...]:
        documents = (
            await ExternalDiscoveryCandidateDocument.find(
                ExternalDiscoveryCandidateDocument.candidate_id == candidate_id
            )
            .sort("+discovered_at", "+recorded_at", "+candidate_record_id")
            .to_list()
        )
        return tuple(_candidate_from_document(document) for document in documents)


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
        f"mongodb://external-discovery-evidence/{evidence.evidence_id}#sanitized-metadata"
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


def _evidence_from_document(
    document: ExternalDiscoveryEvidenceDocument,
) -> PersistedDiscoveryEvidence:
    record = PersistedDiscoveryEvidence(
        evidence_id=document.evidence_id,
        record_digest=document.record_digest,
        evidence=ExternalDiscoveryEvidence.model_validate(document.payload),
        recorded_at=document.recorded_at,
    )
    if sha256_digest(record.evidence) != record.record_digest:
        raise ExternalCandidatePersistenceError("external discovery evidence digest mismatch")
    return record


def _candidate_from_document(
    document: ExternalDiscoveryCandidateDocument,
) -> PersistedExternalCandidate:
    record = PersistedExternalCandidate(
        candidate_record_id=document.candidate_record_id,
        content_digest=document.content_digest,
        evidence_id=document.evidence_id,
        candidate=ExternalDiscoveryCandidate.model_validate(document.payload),
        recorded_at=document.recorded_at,
    )
    if sha256_digest(record.candidate) != record.content_digest:
        raise ExternalCandidatePersistenceError("external discovery candidate digest mismatch")
    if record.candidate_record_id != f"candidate-record:{record.content_digest}":
        raise ExternalCandidatePersistenceError(
            "external discovery candidate record identity mismatch"
        )
    return record
