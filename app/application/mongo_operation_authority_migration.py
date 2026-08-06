from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pymongo.errors import DuplicateKeyError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
)
from app.models.operation_execution import (
    OperationExecutionBindingAuthorityV2Document,
    OperationExecutionBindingDocument,
    OperationMigrationQuarantineDocument,
)


class MigrationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BindingAuthorityRecord(MigrationContract):
    schema_version: Literal["1", "2"]
    binding: OperationExecutionBinding
    canonical_digest: str
    source_collection: str
    source_document_id: str
    source_bound_at: datetime


class BindingBackfillReport(MigrationContract):
    scanned: int = Field(ge=0)
    inserted: int = Field(ge=0)
    existing: int = Field(ge=0)
    quarantined: int = Field(ge=0)
    source_count: int = Field(ge=0)
    target_count: int = Field(ge=0)
    source_digest: str
    target_digest: str
    next_cursor: str | None = None
    complete: bool
    verified: bool


class OperationBindingAuthorityRepository(Protocol):
    async def get_by_id(
        self,
        request_scope: str,
        binding_id: str,
        *,
        read_authority: Literal["v2", "legacy"],
        allow_legacy_fallback: bool,
    ) -> BindingAuthorityRecord | None: ...


class LegacyOperationBindingRepository(Protocol):
    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding: ...

    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationSettlement | None: ...

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool: ...

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str,
    ) -> OperationSettlement: ...


class MongoOperationBindingAuthorityMigrationRepository:
    """Bounded dual-read and copy-on-migrate authority without legacy mutation."""

    async def get_by_id(
        self,
        request_scope: str,
        binding_id: str,
        *,
        read_authority: Literal["v2", "legacy"] = "v2",
        allow_legacy_fallback: bool = True,
    ) -> BindingAuthorityRecord | None:
        if read_authority == "v2":
            current = await OperationExecutionBindingAuthorityV2Document.find_one(
                OperationExecutionBindingAuthorityV2Document.binding_id == binding_id,
                OperationExecutionBindingAuthorityV2Document.request_scope == request_scope,
            )
            if current is not None:
                binding = OperationExecutionBinding.model_validate(current.payload)
                if binding.request_scope != request_scope:
                    raise ValueError("v2 Mongo authority payload crosses request scope")
                digest = sha256_digest(binding)
                if digest != current.canonical_digest:
                    raise ValueError(
                        "v2 Mongo authority record failed canonical digest verification"
                    )
                return BindingAuthorityRecord(
                    schema_version="2",
                    binding=binding,
                    canonical_digest=digest,
                    source_collection=current.source_collection,
                    source_document_id=current.source_document_id,
                    source_bound_at=current.source_bound_at,
                )
            if not allow_legacy_fallback:
                return None
        legacy = await OperationExecutionBindingDocument.find_one(
            OperationExecutionBindingDocument.binding_id == binding_id
        )
        if legacy is None:
            return None
        record = _legacy_record(legacy)
        return record if record.binding.request_scope == request_scope else None

    async def create_binding(
        self,
        request_scope: str,
        binding: OperationExecutionBinding,
        *,
        migrated_at: datetime | None = None,
    ) -> BindingAuthorityRecord:
        if binding.request_scope != request_scope:
            raise ValueError("v2 Mongo authority writes cannot cross request scope")
        digest = sha256_digest(binding)
        source_id = f"native-v2:{binding.request_scope}:{binding.binding_id}"
        document = OperationExecutionBindingAuthorityV2Document(
            binding_id=binding.binding_id,
            semantic_attempt_key=binding.semantic_attempt_key,
            request_scope=binding.request_scope,
            canonical_digest=digest,
            payload=binding.model_dump(mode="json"),
            source_collection="native_v2",
            source_document_id=source_id,
            source_bound_at=binding.bound_at,
            migrated_at=migrated_at or datetime.now(UTC),
        )
        try:
            await document.insert()
        except DuplicateKeyError:
            prior = await OperationExecutionBindingAuthorityV2Document.find_one(
                OperationExecutionBindingAuthorityV2Document.binding_id
                == binding.binding_id,
                OperationExecutionBindingAuthorityV2Document.request_scope
                == binding.request_scope,
            )
            if (
                prior is None
                or prior.canonical_digest != digest
                or prior.payload != binding.model_dump(mode="json")
            ):
                raise ValueError("v2 operation binding authority conflict") from None
        return BindingAuthorityRecord(
            schema_version="2",
            binding=binding,
            canonical_digest=digest,
            source_collection="native_v2",
            source_document_id=source_id,
            source_bound_at=binding.bound_at,
        )

    async def backfill(
        self,
        *,
        limit: int = 1_000,
        after_binding_id: str | None = None,
    ) -> BindingBackfillReport:
        query = (
            OperationExecutionBindingDocument.find(
                OperationExecutionBindingDocument.binding_id > after_binding_id
            )
            if after_binding_id is not None
            else OperationExecutionBindingDocument.find_all()
        )
        source_documents = await query.sort("+binding_id").limit(limit).to_list()
        inserted = 0
        existing = 0
        quarantined = 0
        source_records: list[BindingAuthorityRecord] = []
        for document in source_documents:
            source_id = str(document.id)
            try:
                record = _legacy_record(document)
                source_records.append(record)
                target = OperationExecutionBindingAuthorityV2Document(
                    binding_id=record.binding.binding_id,
                    semantic_attempt_key=record.binding.semantic_attempt_key,
                    request_scope=record.binding.request_scope,
                    canonical_digest=record.canonical_digest,
                    payload=record.binding.model_dump(mode="json"),
                    source_collection=OperationExecutionBindingDocument.Settings.name,
                    source_document_id=source_id,
                    source_bound_at=record.source_bound_at,
                    migrated_at=datetime.now(UTC),
                )
                try:
                    await target.insert()
                    inserted += 1
                except DuplicateKeyError:
                    prior = await OperationExecutionBindingAuthorityV2Document.find_one(
                        OperationExecutionBindingAuthorityV2Document.binding_id
                        == record.binding.binding_id,
                        OperationExecutionBindingAuthorityV2Document.request_scope
                        == record.binding.request_scope,
                    )
                    if (
                        prior is None
                        or prior.canonical_digest != record.canonical_digest
                        or prior.source_document_id != source_id
                    ):
                        await _quarantine(
                            source_id=source_id,
                            reason_code="target_digest_or_lineage_conflict",
                            observed_digest=(
                                prior.canonical_digest if prior is not None else None
                            ),
                            expected_digest=record.canonical_digest,
                        )
                        quarantined += 1
                    else:
                        existing += 1
            except Exception:
                await _quarantine(
                    source_id=source_id,
                    reason_code="legacy_binding_invalid",
                )
                quarantined += 1

        target_documents = (
            await OperationExecutionBindingAuthorityV2Document.find_all()
            .sort("+binding_id")
            .to_list()
        )
        target_records = [
            BindingAuthorityRecord(
                schema_version="2",
                binding=OperationExecutionBinding.model_validate(document.payload),
                canonical_digest=document.canonical_digest,
                source_collection=document.source_collection,
                source_document_id=document.source_document_id,
                source_bound_at=document.source_bound_at,
            )
            for document in target_documents
        ]
        source_digest = _records_digest(source_records)
        target_source_records = [
            record
            for record in target_records
            if record.source_document_id
            in {source.source_document_id for source in source_records}
        ]
        target_digest = _records_digest(target_source_records)
        batch_verified = (
            quarantined == 0
            and len(source_records) == len(target_source_records)
            and source_digest == target_digest
        )
        next_cursor = source_documents[-1].binding_id if source_documents else after_binding_id
        remaining = (
            await OperationExecutionBindingDocument.find(
                OperationExecutionBindingDocument.binding_id > next_cursor
            ).count()
            if next_cursor is not None
            else 0
        )
        complete = remaining == 0
        if complete:
            all_source_documents = (
                await OperationExecutionBindingDocument.find_all()
                .sort("+binding_id")
                .to_list()
            )
            source_records = []
            invalid_source_ids: set[str] = set()
            for document in all_source_documents:
                try:
                    source_records.append(_legacy_record(document))
                except (TypeError, ValueError, ValidationError):
                    invalid_source_ids.add(str(document.id))
            source_ids = {record.source_document_id for record in source_records}
            target_source_records = [
                record
                for record in target_records
                if record.source_collection
                == OperationExecutionBindingDocument.Settings.name
                and record.source_document_id in source_ids
            ]
            source_digest = _records_digest(source_records)
            target_digest = _records_digest(target_source_records)
            quarantine_documents = (
                await OperationMigrationQuarantineDocument.find_all().to_list()
            )
            quarantined_source_ids = {
                document.source_document_id for document in quarantine_documents
            }
            batch_verified = (
                invalid_source_ids.issubset(quarantined_source_ids)
                and len(source_records) == len(target_source_records)
                and source_digest == target_digest
            )
        return BindingBackfillReport(
            scanned=len(source_documents),
            inserted=inserted,
            existing=existing,
            quarantined=quarantined,
            source_count=len(source_records),
            target_count=len(target_source_records),
            source_digest=source_digest,
            target_digest=target_digest,
            next_cursor=next_cursor,
            complete=complete,
            verified=batch_verified and complete,
        )


class VersionedMongoOperationBindingRepository:
    """Selects exactly one semantic write authority; reads may fall back during rollback."""

    def __init__(
        self,
        *,
        legacy: LegacyOperationBindingRepository,
        v2: MongoOperationBindingAuthorityMigrationRepository,
        write_authority: Literal["legacy", "v2"],
        allow_legacy_read_fallback: bool,
    ) -> None:
        self._legacy = legacy
        self._v2 = v2
        self._write_authority = write_authority
        self._allow_legacy_read_fallback = allow_legacy_read_fallback

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
        requested_schema_version: Literal["1", "2"] | None = None,
    ) -> OperationExecutionBinding:
        if binding.request_scope != request_scope:
            raise ValueError("semantic binding write cannot cross request scope")
        authority = self._select(requested_schema_version)
        if authority == "v2":
            return (await self._v2.create_binding(request_scope, binding)).binding
        return await self._legacy.create_binding(
            binding,
            request_scope=request_scope,
        )

    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None:
        authority = self._select(None)
        if authority == "legacy":
            return await self._legacy.get_binding(
                semantic_attempt_key,
                request_scope=request_scope,
            )
        document = await OperationExecutionBindingAuthorityV2Document.find_one(
            OperationExecutionBindingAuthorityV2Document.request_scope == request_scope,
            OperationExecutionBindingAuthorityV2Document.semantic_attempt_key
            == semantic_attempt_key,
        )
        if document is None:
            return None
        binding = OperationExecutionBinding.model_validate(document.payload)
        if (
            binding.request_scope != request_scope
            or sha256_digest(binding) != document.canonical_digest
        ):
            raise ValueError("v2 Mongo authority record failed scope or digest validation")
        return binding

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
        requested_schema_version: Literal["1", "2"] | None = None,
    ) -> OperationExecutionBinding | None:
        authority = self._select(requested_schema_version)
        if authority == "v2":
            record = await self._v2.get_by_id(
                request_scope,
                binding_id,
                read_authority="v2",
                allow_legacy_fallback=self._allow_legacy_read_fallback,
            )
            return record.binding if record is not None else None
        legacy = await self._legacy.get_binding_by_id(
            binding_id,
            request_scope=request_scope,
        )
        if legacy is None or legacy.request_scope != request_scope:
            return None
        return legacy

    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationSettlement | None:
        self._require_legacy_execution_journal()
        return await self._legacy.get_settlement(
            binding_id,
            request_scope=request_scope,
        )

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool:
        self._require_legacy_execution_journal()
        return await self._legacy.claim_execution(binding)

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str,
    ) -> OperationSettlement:
        self._require_legacy_execution_journal()
        return await self._legacy.settle(
            settlement,
            request_scope=request_scope,
        )

    def _select(
        self,
        requested_schema_version: Literal["1", "2"] | None,
    ) -> Literal["v2", "legacy"]:
        requested = requested_schema_version or (
            "2" if self._write_authority == "v2" else "1"
        )
        return select_authority_version(
            requested_schema_version=requested,
            v2_available=True,
            rollback_window_open=(
                self._write_authority == "legacy" or self._allow_legacy_read_fallback
            ),
        )

    def _require_legacy_execution_journal(self) -> None:
        if self._write_authority != "legacy":
            raise RuntimeError(
                "Mongo claim and settlement methods are disabled after v2 binding cutover"
            )


def select_authority_version(
    *,
    requested_schema_version: Literal["1", "2"] | None,
    v2_available: bool,
    rollback_window_open: bool,
) -> Literal["v2", "legacy"]:
    if requested_schema_version == "2":
        if not v2_available:
            raise LookupError("requested v2 operation binding authority is unavailable")
        return "v2"
    if requested_schema_version == "1":
        if not rollback_window_open:
            raise ValueError("legacy authority is outside the accepted rollback window")
        return "legacy"
    return "v2" if v2_available else "legacy"


def _legacy_record(
    document: OperationExecutionBindingDocument,
) -> BindingAuthorityRecord:
    binding = OperationExecutionBinding.model_validate(document.payload)
    mirrored = {
        "request_scope": document.request_scope,
        "binding_id": document.binding_id,
        "semantic_attempt_key": document.semantic_attempt_key,
        "request_fingerprint": document.request_fingerprint,
        "run_id": document.run_id,
        "operation_id": document.operation_id,
        "operation_attempt": document.operation_attempt,
        "bound_at": document.bound_at,
    }
    binding_values = {
        "request_scope": binding.request_scope,
        "binding_id": binding.binding_id,
        "semantic_attempt_key": binding.semantic_attempt_key,
        "request_fingerprint": binding.request_fingerprint,
        "run_id": binding.run_id,
        "operation_id": binding.operation_id,
        "operation_attempt": binding.operation_attempt,
        "bound_at": binding.bound_at,
    }
    for field_name, observed in mirrored.items():
        if observed is not None and observed != binding_values[field_name]:
            raise ValueError(
                f"legacy binding mirror field conflicts with payload: {field_name}"
            )
    return BindingAuthorityRecord(
        schema_version="1",
        binding=binding,
        canonical_digest=sha256_digest(binding),
        source_collection=OperationExecutionBindingDocument.Settings.name,
        source_document_id=str(document.id),
        source_bound_at=document.bound_at,
    )


async def _quarantine(
    *,
    source_id: str,
    reason_code: str,
    observed_digest: str | None = None,
    expected_digest: str | None = None,
) -> None:
    quarantine = OperationMigrationQuarantineDocument(
        quarantine_id=str(
            uuid5(
                NAMESPACE_URL,
                f"operation-migration:{OperationExecutionBindingDocument.Settings.name}:"
                f"{source_id}",
            )
        ),
        source_collection=OperationExecutionBindingDocument.Settings.name,
        source_document_id=source_id,
        reason_code=reason_code,
        observed_digest=observed_digest,
        expected_digest=expected_digest,
        quarantined_at=datetime.now(UTC),
    )
    try:
        await quarantine.insert()
    except DuplicateKeyError:
        existing = await OperationMigrationQuarantineDocument.find_one(
            {
                "source_collection": quarantine.source_collection,
                "source_document_id": quarantine.source_document_id,
            }
        )
        if existing is None or any(
            getattr(existing, field_name) != getattr(quarantine, field_name)
            for field_name in (
                "quarantine_id",
                "reason_code",
                "observed_digest",
                "expected_digest",
            )
        ):
            raise RuntimeError(
                "operation migration quarantine identity has conflicting evidence"
            ) from None


def _records_digest(records: list[BindingAuthorityRecord]) -> str:
    canonical = [
        {
            "binding_id": record.binding.binding_id,
            "canonical_digest": record.canonical_digest,
            "source_collection": record.source_collection,
            "source_document_id": record.source_document_id,
            "source_bound_at": record.source_bound_at,
        }
        for record in sorted(
            records,
            key=lambda item: (
                item.source_bound_at,
                item.binding.binding_id,
                item.source_document_id,
            ),
        )
    ]
    return sha256_digest(canonical)
