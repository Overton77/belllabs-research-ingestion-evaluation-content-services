from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from typing import Protocol

from pymongo.errors import DuplicateKeyError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.schema_grounding.contracts import (
    SchemaGroundingRecordEnvelope,
    SchemaGroundingRecordType,
)
from app.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    SchemaGroundingRecordNotFound,
)
from app.models.schema_grounding import SchemaGroundingRecordDocument


class SchemaGroundingRecordRepository(Protocol):
    async def append(
        self, record: SchemaGroundingRecordEnvelope
    ) -> SchemaGroundingRecordEnvelope: ...

    async def get(
        self,
        request_scope: str,
        record_type: SchemaGroundingRecordType,
        record_id: str,
    ) -> SchemaGroundingRecordEnvelope: ...

    async def list_for_run(
        self,
        request_scope: str,
        run_id: str,
        *,
        record_type: SchemaGroundingRecordType | None = None,
    ) -> tuple[SchemaGroundingRecordEnvelope, ...]: ...


class InMemorySchemaGroundingRecordRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[
            tuple[str, SchemaGroundingRecordType, str], SchemaGroundingRecordEnvelope
        ] = {}

    async def append(
        self, record: SchemaGroundingRecordEnvelope
    ) -> SchemaGroundingRecordEnvelope:
        _verify_record(record)
        key = (record.request_scope, record.record_type, record.record_id)
        async with self._lock:
            prior = self._records.get(key)
            if prior is not None:
                if prior != record:
                    raise CatalogPublicationConflict(
                        "immutable schema-grounding record identity was reused with new content"
                    )
                return prior.model_copy(deep=True)
            self._records[key] = record.model_copy(deep=True)
        return record.model_copy(deep=True)

    async def get(
        self,
        request_scope: str,
        record_type: SchemaGroundingRecordType,
        record_id: str,
    ) -> SchemaGroundingRecordEnvelope:
        try:
            record = self._records[(request_scope, record_type, record_id)]
        except KeyError as error:
            raise SchemaGroundingRecordNotFound(
                f"{record_type} record not found: {record_id}"
            ) from error
        _verify_record(record)
        return record.model_copy(deep=True)

    async def list_for_run(
        self,
        request_scope: str,
        run_id: str,
        *,
        record_type: SchemaGroundingRecordType | None = None,
    ) -> tuple[SchemaGroundingRecordEnvelope, ...]:
        values = [
            value
            for value in self._records.values()
            if value.request_scope == request_scope
            and value.run_id == run_id
            and (record_type is None or value.record_type == record_type)
        ]
        return tuple(
            item.model_copy(deep=True)
            for item in sorted(values, key=lambda value: (value.created_at, value.record_type))
        )


class BeanieSchemaGroundingRecordRepository:
    async def append(
        self, record: SchemaGroundingRecordEnvelope
    ) -> SchemaGroundingRecordEnvelope:
        _verify_record(record)
        document = SchemaGroundingRecordDocument(**record.model_dump(mode="python"))
        try:
            await document.insert()
            return record
        except DuplicateKeyError:
            prior = await SchemaGroundingRecordDocument.find_one(
                SchemaGroundingRecordDocument.request_scope == record.request_scope,
                SchemaGroundingRecordDocument.record_type == record.record_type,
                SchemaGroundingRecordDocument.record_id == record.record_id,
            )
            if prior is None:
                raise CatalogPublicationConflict(
                    "schema-grounding record uniqueness conflict"
                ) from None
            existing = SchemaGroundingRecordEnvelope.model_validate(
                prior.model_dump(mode="python", exclude={"id"})
            )
            _verify_record(existing)
            if existing != record:
                raise CatalogPublicationConflict(
                    "immutable schema-grounding record identity was reused with new content"
                ) from None
            return existing

    async def get(
        self,
        request_scope: str,
        record_type: SchemaGroundingRecordType,
        record_id: str,
    ) -> SchemaGroundingRecordEnvelope:
        document = await SchemaGroundingRecordDocument.find_one(
            SchemaGroundingRecordDocument.request_scope == request_scope,
            SchemaGroundingRecordDocument.record_type == record_type,
            SchemaGroundingRecordDocument.record_id == record_id,
        )
        if document is None:
            raise SchemaGroundingRecordNotFound(
                f"{record_type} record not found: {record_id}"
            )
        record = SchemaGroundingRecordEnvelope.model_validate(
            document.model_dump(mode="python", exclude={"id"})
        )
        _verify_record(record)
        return record

    async def list_for_run(
        self,
        request_scope: str,
        run_id: str,
        *,
        record_type: SchemaGroundingRecordType | None = None,
    ) -> tuple[SchemaGroundingRecordEnvelope, ...]:
        query = SchemaGroundingRecordDocument.find(
            SchemaGroundingRecordDocument.request_scope == request_scope,
            SchemaGroundingRecordDocument.run_id == run_id,
        )
        if record_type is not None:
            query = query.find(SchemaGroundingRecordDocument.record_type == record_type)
        documents = await query.sort("+created_at").to_list()
        records = tuple(
            SchemaGroundingRecordEnvelope.model_validate(
                document.model_dump(mode="python", exclude={"id"})
            )
            for document in documents
        )
        for record in records:
            _verify_record(record)
        return records


def schema_grounding_record(
    *,
    record_type: SchemaGroundingRecordType,
    record_id: str,
    request_scope: str,
    payload: dict[str, object],
    created_at: datetime,
    run_id: str | None = None,
) -> SchemaGroundingRecordEnvelope:
    return SchemaGroundingRecordEnvelope(
        record_type=record_type,
        record_id=record_id,
        request_scope=request_scope,
        run_id=run_id,
        content_digest=sha256_digest(payload),
        payload=deepcopy(payload),
        created_at=created_at,
    )


def _verify_record(record: SchemaGroundingRecordEnvelope) -> None:
    actual = sha256_digest(record.payload)
    if actual != record.content_digest:
        raise CatalogPublicationConflict(
            f"schema-grounding record payload digest mismatch: expected "
            f"{record.content_digest}, got {actual}"
        )
