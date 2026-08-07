from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
)
from app.domain.operation_execution.journal import (
    EffectClaimStatus,
    OperationEffectClaim,
    OperationJournalSettlement,
)

MIGRATION_PRINCIPAL = "migration:legacy-mongo-operation-journal:v1"
MIGRATION_STREAM = "legacy-mongo-operation-journal-v1"
CLAIMS_COLLECTION: Literal["operation_execution_claims"] = "operation_execution_claims"
SETTLEMENTS_COLLECTION: Literal["operation_execution_settlements"] = (
    "operation_execution_settlements"
)


@dataclass(frozen=True)
class LegacyMongoRecord:
    collection: Literal[
        "operation_execution_claims",
        "operation_execution_settlements",
    ]
    document_id: str
    payload: dict[str, Any]
    recorded_at: datetime

    @property
    def cursor(self) -> str:
        return f"{self.collection}:{self.document_id}"

    @property
    def canonical_digest(self) -> str:
        return sha256_digest(
            {
                "payload": self.payload,
                "recorded_at": self.recorded_at,
            }
        )


@dataclass(frozen=True)
class SourceSnapshot:
    request_scope: str
    claim_high_watermark: str | None
    settlement_high_watermark: str | None
    record_count: int
    aggregate_digest: str
    captured_at: datetime

    @property
    def snapshot_digest(self) -> str:
        return sha256_digest(
            {
                "request_scope": self.request_scope,
                "claim_high_watermark": self.claim_high_watermark,
                "settlement_high_watermark": self.settlement_high_watermark,
                "record_count": self.record_count,
                "aggregate_digest": self.aggregate_digest,
            }
        )


@dataclass(frozen=True)
class SourceLineage:
    source_system: Literal["mongodb"] = "mongodb"
    source_collection: str = ""
    source_document_id: str = ""
    source_recorded_at: datetime | None = None
    source_canonical_digest: str = ""


@dataclass(frozen=True)
class ClaimAdmission:
    claim: OperationEffectClaim
    lineage: SourceLineage


@dataclass(frozen=True)
class SettlementAdmission:
    settlement: OperationJournalSettlement
    lineage: SourceLineage


@dataclass(frozen=True)
class QuarantineAdmission:
    quarantine_id: str
    migration_stream: str
    source_collection: str
    source_document_id: str
    reason_code: str
    observed_digest: str | None
    expected_digest: str | None
    observed_request_scope: str | None
    quarantined_at: datetime


@dataclass(frozen=True)
class BackfillBatch:
    run_id: str
    request_scope: str
    previous_cursor: str | None
    cursor: str | None
    source_snapshot: SourceSnapshot
    claims: tuple[ClaimAdmission, ...] = ()
    settlements: tuple[SettlementAdmission, ...] = ()
    quarantines: tuple[QuarantineAdmission, ...] = ()
    source_aggregate_digest: str | None = None
    target_aggregate_digest: str | None = None
    completed: bool = False
    dry_run: bool = False

    @property
    def batch_digest(self) -> str:
        return sha256_digest(
            {
                "run_id": self.run_id,
                "request_scope": self.request_scope,
                "previous_cursor": self.previous_cursor,
                "cursor": self.cursor,
                "source_snapshot_digest": self.source_snapshot.snapshot_digest,
                "claims": tuple(
                    {
                        "claim": item.claim,
                        "lineage": _source_lineage_content(item.lineage),
                    }
                    for item in self.claims
                ),
                "settlements": tuple(
                    {
                        "settlement": item.settlement,
                        "lineage": _source_lineage_content(item.lineage),
                    }
                    for item in self.settlements
                ),
                "quarantines": tuple(
                    {
                        "quarantine_id": item.quarantine_id,
                        "migration_stream": item.migration_stream,
                        "source_collection": item.source_collection,
                        "source_document_id": item.source_document_id,
                        "reason_code": item.reason_code,
                        "observed_digest": item.observed_digest,
                        "expected_digest": item.expected_digest,
                        "observed_request_scope": item.observed_request_scope,
                    }
                    for item in self.quarantines
                ),
                "source_aggregate_digest": self.source_aggregate_digest,
                "target_aggregate_digest": self.target_aggregate_digest,
                "completed": self.completed,
            }
        )


@dataclass(frozen=True)
class BackfillProgress:
    run_id: str
    request_scope: str
    cursor: str | None
    source_count: int
    admitted_claim_count: int
    admitted_settlement_count: int
    quarantine_count: int
    source_aggregate_digest: str
    target_aggregate_digest: str
    source_snapshot: SourceSnapshot
    completed: bool
    dry_run: bool


class LegacyOperationJournalSource(Protocol):
    async def capture_snapshot(self, *, request_scope: str) -> SourceSnapshot: ...

    async def read_batch(
        self,
        *,
        request_scope: str,
        after_cursor: str | None,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> tuple[LegacyMongoRecord, ...]: ...

    async def get_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> OperationExecutionBinding | None: ...

    async def get_claim_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None: ...

    async def get_settlement_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None: ...


class OperationJournalBackfillTarget(Protocol):
    async def load_progress(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress | None: ...

    async def apply_batch(self, batch: BackfillBatch) -> None: ...

    async def verify(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress: ...


class OperationJournalBackfillService:
    def __init__(
        self,
        *,
        source: LegacyOperationJournalSource,
        target: OperationJournalBackfillTarget,
        batch_size: int = 100,
    ) -> None:
        if batch_size < 1 or batch_size > 10_000:
            raise ValueError("backfill batch size must be between 1 and 10000")
        self._source = source
        self._target = target
        self._batch_size = batch_size

    async def run(
        self,
        *,
        request_scope: str,
        run_id: str,
        dry_run: bool = False,
    ) -> BackfillProgress:
        progress = None
        if not dry_run:
            progress = await self._target.load_progress(
                request_scope=request_scope,
                run_id=run_id,
            )
        captured_snapshot = await self._source.capture_snapshot(
            request_scope=request_scope
        )
        if progress is not None:
            if (
                progress.source_snapshot.snapshot_digest
                != captured_snapshot.snapshot_digest
            ):
                raise RuntimeError("legacy source changed since the backfill snapshot")
            source_snapshot = progress.source_snapshot
        else:
            source_snapshot = captured_snapshot
        cursor = progress.cursor if progress is not None else None
        source_aggregate_digest = (
            progress.source_aggregate_digest if progress is not None else sha256_digest([])
        )
        target_aggregate_digest = (
            progress.target_aggregate_digest if progress is not None else sha256_digest([])
        )
        source_count = progress.source_count if progress is not None else 0
        admitted_claim_count = (
            progress.admitted_claim_count if progress is not None else 0
        )
        admitted_settlement_count = (
            progress.admitted_settlement_count if progress is not None else 0
        )
        quarantine_count = progress.quarantine_count if progress is not None else 0

        while True:
            previous_cursor = cursor
            records = await self._source.read_batch(
                request_scope=request_scope,
                after_cursor=cursor,
                limit=self._batch_size,
                snapshot=source_snapshot,
            )
            if not records:
                break
            claims: list[ClaimAdmission] = []
            settlements: list[SettlementAdmission] = []
            quarantines: list[QuarantineAdmission] = []
            for record in records:
                source_count += 1
                source_aggregate_digest = _extend_digest(
                    source_aggregate_digest,
                    record.canonical_digest,
                )
                admission = await self._transform(record, request_scope=request_scope)
                if isinstance(admission, ClaimAdmission):
                    claims.append(admission)
                    target_aggregate_digest = _extend_digest(
                        target_aggregate_digest,
                        sha256_digest(admission.claim),
                    )
                elif isinstance(admission, SettlementAdmission):
                    settlements.append(admission)
                    target_aggregate_digest = _extend_digest(
                        target_aggregate_digest,
                        sha256_digest(admission.settlement),
                    )
                else:
                    quarantines.append(admission)
            admitted_claim_count += len(claims)
            admitted_settlement_count += len(settlements)
            quarantine_count += len(quarantines)
            cursor = records[-1].cursor
            batch = BackfillBatch(
                run_id=run_id,
                request_scope=request_scope,
                previous_cursor=previous_cursor,
                cursor=cursor,
                source_snapshot=source_snapshot,
                claims=tuple(claims),
                settlements=tuple(settlements),
                quarantines=tuple(quarantines),
                source_aggregate_digest=source_aggregate_digest,
                target_aggregate_digest=target_aggregate_digest,
                dry_run=dry_run,
            )
            if not dry_run:
                await self._target.apply_batch(batch)

        if (
            source_count != source_snapshot.record_count
            or source_aggregate_digest != source_snapshot.aggregate_digest
        ):
            raise RuntimeError("backfill scan does not match the frozen source snapshot")
        result = BackfillProgress(
            run_id=run_id,
            request_scope=request_scope,
            cursor=cursor,
            source_count=source_count,
            admitted_claim_count=admitted_claim_count,
            admitted_settlement_count=admitted_settlement_count,
            quarantine_count=quarantine_count,
            source_aggregate_digest=source_aggregate_digest,
            target_aggregate_digest=target_aggregate_digest,
            source_snapshot=source_snapshot,
            completed=True,
            dry_run=dry_run,
        )
        verified_source = await self._source.capture_snapshot(
            request_scope=request_scope
        )
        if verified_source.snapshot_digest != source_snapshot.snapshot_digest:
            raise RuntimeError("legacy source changed during backfill verification")
        if dry_run:
            return result
        await self._target.apply_batch(
            BackfillBatch(
                run_id=run_id,
                request_scope=request_scope,
                previous_cursor=cursor,
                cursor=cursor,
                source_snapshot=source_snapshot,
                source_aggregate_digest=result.source_aggregate_digest,
                target_aggregate_digest=result.target_aggregate_digest,
                completed=True,
            )
        )
        verified = await self._target.verify(
            request_scope=request_scope,
            run_id=run_id,
        )
        if (
            verified.source_aggregate_digest != result.source_aggregate_digest
            or verified.target_aggregate_digest != result.target_aggregate_digest
        ):
            raise RuntimeError("backfill full-set verification digest mismatch")
        return verified

    async def _transform(
        self,
        record: LegacyMongoRecord,
        *,
        request_scope: str,
    ) -> ClaimAdmission | SettlementAdmission | QuarantineAdmission:
        binding_id = str(record.payload.get("binding_id", ""))
        binding = (
            await self._source.get_binding(
                request_scope=request_scope,
                binding_id=binding_id,
            )
            if binding_id
            else None
        )
        if binding is None:
            return _quarantine(record, "missing_or_invalid_binding")
        observed_scope = record.payload.get("request_scope")
        if binding.request_scope != request_scope:
            return _quarantine(
                record,
                "cross_scope_record",
                observed_request_scope=str(observed_scope or binding.request_scope),
            )
        if observed_scope is not None and observed_scope != binding.request_scope:
            return _quarantine(
                record,
                "request_scope_mismatch",
                observed_request_scope=str(observed_scope),
            )
        try:
            if record.collection == CLAIMS_COLLECTION:
                return transform_legacy_claim(record, binding)
            claim = await self._source.get_claim_for_binding(
                request_scope=request_scope,
                binding_id=binding.binding_id,
            )
            if claim is None:
                return _quarantine(record, "orphan_settlement")
            claim_admission = transform_legacy_claim(claim, binding)
            return transform_legacy_settlement(record, binding, claim_admission.claim)
        except (KeyError, TypeError, ValueError, ValidationError):
            return _quarantine(
                record,
                "malformed_or_conflicting_source",
                observed_request_scope=(
                    str(observed_scope) if observed_scope is not None else None
                ),
            )


def transform_legacy_claim(
    record: LegacyMongoRecord,
    binding: OperationExecutionBinding,
) -> ClaimAdmission:
    if record.collection != CLAIMS_COLLECTION:
        raise ValueError("claim transformation received another source collection")
    if record.payload.get("binding_id") != binding.binding_id:
        raise ValueError("legacy claim references another operation binding")
    if record.payload.get("side_effect_key") != binding.side_effect_key:
        raise ValueError("legacy claim side-effect identity conflicts with binding")
    source_scope = record.payload.get("request_scope")
    if source_scope is not None and source_scope != binding.request_scope:
        raise ValueError("legacy claim request scope conflicts with binding")
    claim = OperationEffectClaim(
        effect_claim_id=_effect_claim_id(binding),
        request_scope=binding.request_scope,
        belllabs_run_id=binding.run_id,
        operation_contract_digest=sha256_digest(binding.operation_contract_ref),
        idempotency_key=binding.side_effect_key,
        request_digest=binding.request_fingerprint,
        semantic_binding_id=binding.binding_id,
        semantic_binding_digest=sha256_digest(binding),
        semantic_attempt_key=binding.semantic_attempt_key,
        claim_mode="active",
        status=EffectClaimStatus.CLAIMED,
        claimed_by=MIGRATION_PRINCIPAL,
        claimed_at=record.recorded_at,
    )
    return ClaimAdmission(claim=claim, lineage=_lineage(record))


def transform_legacy_settlement(
    record: LegacyMongoRecord,
    binding: OperationExecutionBinding,
    claim: OperationEffectClaim,
) -> SettlementAdmission:
    if record.collection != SETTLEMENTS_COLLECTION:
        raise ValueError("settlement transformation received another source collection")
    source_scope = record.payload.get("request_scope")
    if source_scope is not None and source_scope != binding.request_scope:
        raise ValueError("legacy settlement request scope conflicts with binding")
    settlement = OperationSettlement.model_validate(record.payload.get("payload"))
    if settlement.binding_id != binding.binding_id:
        raise ValueError("legacy settlement references another operation binding")
    safe_detail: dict[str, str] = {"schema_version": "1"}
    if settlement.failure_code:
        safe_detail["reason_code"] = settlement.failure_code
    if settlement.provider_run_id:
        safe_detail["reconciliation_ref"] = settlement.provider_run_id
    journal_settlement = OperationJournalSettlement.create(
        settlement_id=settlement.settlement_id,
        request_scope=binding.request_scope,
        effect_claim_id=claim.effect_claim_id,
        settlement_revision=1,
        status=settlement.status,
        usage=settlement.usage.amounts,
        pending_external_usage=settlement.usage.pending_external_amounts,
        failure_code=settlement.failure_code,
        detail=safe_detail,
        settled_at=settlement.settled_at,
    )
    return SettlementAdmission(
        settlement=journal_settlement,
        lineage=_lineage(record),
    )


def _lineage(record: LegacyMongoRecord) -> SourceLineage:
    return SourceLineage(
        source_collection=record.collection,
        source_document_id=record.document_id,
        source_recorded_at=record.recorded_at,
        source_canonical_digest=record.canonical_digest,
    )


def _source_lineage_content(lineage: SourceLineage) -> dict[str, object]:
    return {
        "source_system": lineage.source_system,
        "source_collection": lineage.source_collection,
        "source_document_id": lineage.source_document_id,
        "source_recorded_at": lineage.source_recorded_at,
        "source_canonical_digest": lineage.source_canonical_digest,
    }


def _effect_claim_id(binding: OperationExecutionBinding) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"operation-effect:{binding.request_scope}:{binding.binding_id}",
        )
    )


def _extend_digest(aggregate_digest: str, item_digest: str) -> str:
    return sha256_digest(
        {
            "previous_aggregate_digest": aggregate_digest,
            "item_digest": item_digest,
        }
    )


def _quarantine(
    record: LegacyMongoRecord,
    reason_code: str,
    *,
    expected_digest: str | None = None,
    observed_request_scope: str | None = None,
) -> QuarantineAdmission:
    return QuarantineAdmission(
        quarantine_id=str(
            uuid5(
                NAMESPACE_URL,
                f"{MIGRATION_STREAM}:{record.collection}:{record.document_id}",
            )
        ),
        migration_stream=MIGRATION_STREAM,
        source_collection=record.collection,
        source_document_id=record.document_id,
        reason_code=reason_code,
        observed_digest=record.canonical_digest,
        expected_digest=expected_digest,
        observed_request_scope=observed_request_scope,
        quarantined_at=datetime.now(UTC),
    )
