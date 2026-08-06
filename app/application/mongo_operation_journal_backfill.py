from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bson import ObjectId

from app.application.operation_journal_backfill import (
    CLAIMS_COLLECTION,
    SETTLEMENTS_COLLECTION,
    LegacyMongoRecord,
    LegacyOperationJournalSource,
    SourceSnapshot,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import OperationExecutionBinding
from app.models.operation_execution import (
    OperationExecutionBindingDocument,
    OperationExecutionClaimDocument,
    OperationSettlementDocument,
)


class MongoLegacyOperationJournalSource(LegacyOperationJournalSource):
    """Read-only, cursor-ordered access to immutable legacy operation records."""

    async def capture_snapshot(self, *, request_scope: str) -> SourceSnapshot:
        scope_query = _scope_query(request_scope)
        claim_high_watermark = await _high_watermark(
            OperationExecutionClaimDocument,
            scope_query,
        )
        settlement_high_watermark = await _high_watermark(
            OperationSettlementDocument,
            scope_query,
        )
        captured_at = datetime.now(UTC)
        provisional = SourceSnapshot(
            request_scope=request_scope,
            claim_high_watermark=claim_high_watermark,
            settlement_high_watermark=settlement_high_watermark,
            record_count=0,
            aggregate_digest=sha256_digest([]),
            captured_at=captured_at,
        )
        count = 0
        aggregate = sha256_digest([])
        cursor: str | None = None
        while True:
            records = await self.read_batch(
                request_scope=request_scope,
                after_cursor=cursor,
                limit=500,
                snapshot=provisional,
            )
            if not records:
                break
            for record in records:
                count += 1
                aggregate = _extend_digest(aggregate, record.canonical_digest)
            cursor = records[-1].cursor
        return SourceSnapshot(
            request_scope=request_scope,
            claim_high_watermark=claim_high_watermark,
            settlement_high_watermark=settlement_high_watermark,
            record_count=count,
            aggregate_digest=aggregate,
            captured_at=captured_at,
        )

    async def read_batch(
        self,
        *,
        request_scope: str,
        after_cursor: str | None,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> tuple[LegacyMongoRecord, ...]:
        if limit < 1:
            return ()
        if snapshot.request_scope != request_scope:
            raise ValueError("source snapshot belongs to another request scope")
        scope_query = _scope_query(request_scope)
        after_collection, after_document_id = _split_cursor(after_cursor)
        claims = []
        if (
            snapshot.claim_high_watermark is not None
            and after_collection in {None, CLAIMS_COLLECTION}
        ):
            claims = await OperationExecutionClaimDocument.find(
                _cursor_query(
                    scope_query,
                    after_document_id if after_collection == CLAIMS_COLLECTION else None,
                    snapshot.claim_high_watermark,
                )
            ).sort("_id").limit(limit).to_list()
        settlements = []
        if snapshot.settlement_high_watermark is not None:
            settlements = await OperationSettlementDocument.find(
                _cursor_query(
                    scope_query,
                    (
                        after_document_id
                        if after_collection == SETTLEMENTS_COLLECTION
                        else None
                    ),
                    snapshot.settlement_high_watermark,
                )
            ).sort("_id").limit(limit).to_list()
        records = [
            LegacyMongoRecord(
                collection=CLAIMS_COLLECTION,
                document_id=str(item.id),
                payload={
                    "request_scope": item.request_scope,
                    "side_effect_key": item.side_effect_key,
                    "binding_id": item.binding_id,
                },
                recorded_at=_aware(item.claimed_at),
            )
            for item in claims
        ] + [
            LegacyMongoRecord(
                collection=SETTLEMENTS_COLLECTION,
                document_id=str(item.id),
                payload={
                    "request_scope": item.request_scope,
                    "settlement_id": item.settlement_id,
                    "binding_id": item.binding_id,
                    "payload": item.payload,
                },
                recorded_at=_aware(item.settled_at),
            )
            for item in settlements
        ]
        records.sort(key=lambda item: item.cursor)
        return tuple(
            item
            for item in records
            if after_cursor is None or item.cursor > after_cursor
        )[:limit]

    async def get_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> OperationExecutionBinding | None:
        document = await _find_scoped(
            OperationExecutionBindingDocument,
            request_scope=request_scope,
            binding_id=binding_id,
        )
        if document is None:
            return None
        try:
            binding = OperationExecutionBinding.model_validate(document.payload)
        except ValueError:
            return None
        if binding.binding_id != document.binding_id:
            return None
        if document.request_scope is not None and (
            binding.request_scope != document.request_scope
        ):
            return None
        return binding

    async def get_claim_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None:
        document = await _find_scoped(
            OperationExecutionClaimDocument,
            request_scope=request_scope,
            binding_id=binding_id,
        )
        if document is None:
            return None
        return LegacyMongoRecord(
            collection=CLAIMS_COLLECTION,
            document_id=str(document.id),
            payload={
                "request_scope": document.request_scope,
                "side_effect_key": document.side_effect_key,
                "binding_id": document.binding_id,
            },
            recorded_at=_aware(document.claimed_at),
        )

    async def get_settlement_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None:
        document = await _find_scoped(
            OperationSettlementDocument,
            request_scope=request_scope,
            binding_id=binding_id,
        )
        if document is None:
            return None
        return LegacyMongoRecord(
            collection=SETTLEMENTS_COLLECTION,
            document_id=str(document.id),
            payload={
                "request_scope": document.request_scope,
                "settlement_id": document.settlement_id,
                "binding_id": document.binding_id,
                "payload": document.payload,
            },
            recorded_at=_aware(document.settled_at),
        )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _split_cursor(cursor: str | None) -> tuple[str | None, str | None]:
    if cursor is None:
        return None, None
    collection, separator, document_id = cursor.partition(":")
    if (
        not separator
        or collection not in {CLAIMS_COLLECTION, SETTLEMENTS_COLLECTION}
        or not ObjectId.is_valid(document_id)
    ):
        raise ValueError("invalid legacy operation journal cursor")
    return collection, document_id


def _scope_query(request_scope: str) -> dict[str, Any]:
    return {
        "$or": [
            {"request_scope": request_scope},
            {"request_scope": None},
            {"request_scope": {"$exists": False}},
        ]
    }


def _cursor_query(
    scope_query: dict[str, Any],
    document_id: str | None,
    high_watermark: str | None,
) -> dict[str, Any]:
    bounds: dict[str, ObjectId] = {}
    if document_id is not None:
        bounds["$gt"] = ObjectId(document_id)
    if high_watermark is not None:
        bounds["$lte"] = ObjectId(high_watermark)
    if not bounds:
        return scope_query
    return {
        "$and": [
            scope_query,
            {"_id": bounds},
        ]
    }


async def _high_watermark(model: Any, query: dict[str, Any]) -> str | None:
    documents = await model.find(query).sort("-_id").limit(1).to_list()
    return str(documents[0].id) if documents else None


async def _find_scoped(
    model: Any,
    *,
    request_scope: str,
    binding_id: str,
) -> Any | None:
    document = await model.find_one(
        {"binding_id": binding_id, "request_scope": request_scope}
    )
    if document is not None:
        return document
    return await model.find_one(
        {
            "binding_id": binding_id,
            "$or": [
                {"request_scope": None},
                {"request_scope": {"$exists": False}},
            ],
        }
    )


def _extend_digest(aggregate_digest: str, item_digest: str) -> str:
    return sha256_digest(
        {
            "previous_aggregate_digest": aggregate_digest,
            "item_digest": item_digest,
        }
    )
