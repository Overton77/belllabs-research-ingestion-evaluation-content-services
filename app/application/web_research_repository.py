from __future__ import annotations

import asyncio
from typing import Protocol

from pymongo.errors import DuplicateKeyError

from app.domain.coordinator.web_research_runtime import WebResearchRecordEnvelope
from app.models.web_research import WebResearchRecordDocument


class WebResearchRecordConflict(ValueError):
    """An immutable web-research intent or record identity was reused."""


class WebResearchRecordNotFound(LookupError):
    """A governed web-research record could not be resolved in the active scope."""


class WebResearchRecordRepository(Protocol):
    async def append(
        self,
        record: WebResearchRecordEnvelope,
    ) -> WebResearchRecordEnvelope: ...

    async def get(
        self,
        request_scope: str,
        run_id: str,
        record_ref: str,
    ) -> WebResearchRecordEnvelope: ...

    async def get_by_intent(
        self,
        request_scope: str,
        run_id: str,
        intent_key: str,
    ) -> WebResearchRecordEnvelope | None: ...


class InMemoryWebResearchRecordRepository:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], WebResearchRecordEnvelope] = {}
        self._intent_index: dict[tuple[str, str, str], str] = {}
        self._lock = asyncio.Lock()

    async def append(
        self,
        record: WebResearchRecordEnvelope,
    ) -> WebResearchRecordEnvelope:
        record_ref = web_research_record_ref(record)
        record_key = (record.request_scope, record_ref)
        intent_key = (record.request_scope, record.run_id, record.intent_key)
        async with self._lock:
            prior = self._records.get(record_key)
            prior_ref = self._intent_index.get(intent_key)
            if prior is not None:
                if prior != record:
                    raise WebResearchRecordConflict(
                        "web-research record identity was reused with different content"
                    )
                return prior.model_copy(deep=True)
            if prior_ref is not None:
                prior_for_intent = self._records[(record.request_scope, prior_ref)]
                if prior_for_intent != record:
                    raise WebResearchRecordConflict(
                        "web-research intent was reused with different content"
                    )
                return prior_for_intent.model_copy(deep=True)
            self._records[record_key] = record.model_copy(deep=True)
            self._intent_index[intent_key] = record_ref
        return record.model_copy(deep=True)

    async def get(
        self,
        request_scope: str,
        run_id: str,
        record_ref: str,
    ) -> WebResearchRecordEnvelope:
        record = self._records.get((request_scope, record_ref))
        if record is None or record.run_id != run_id:
            raise WebResearchRecordNotFound(f"web-research record not found: {record_ref}")
        return record.model_copy(deep=True)

    async def get_by_intent(
        self,
        request_scope: str,
        run_id: str,
        intent_key: str,
    ) -> WebResearchRecordEnvelope | None:
        record_ref = self._intent_index.get((request_scope, run_id, intent_key))
        if record_ref is None:
            return None
        return self._records[(request_scope, record_ref)].model_copy(deep=True)


class BeanieWebResearchRecordRepository:
    async def append(
        self,
        record: WebResearchRecordEnvelope,
    ) -> WebResearchRecordEnvelope:
        document = WebResearchRecordDocument(**record.model_dump(mode="python"))
        try:
            await document.insert()
            return record
        except DuplicateKeyError:
            prior = await self.get_by_intent(
                record.request_scope,
                record.run_id,
                record.intent_key,
            )
            if prior is None:
                prior_document = await WebResearchRecordDocument.find_one(
                    WebResearchRecordDocument.request_scope == record.request_scope,
                    WebResearchRecordDocument.record_id == record.record_id,
                )
                prior = (
                    _from_document(prior_document)
                    if prior_document is not None
                    else None
                )
            if prior is None or prior != record:
                raise WebResearchRecordConflict(
                    "web-research record uniqueness conflict"
                ) from None
            return prior

    async def get(
        self,
        request_scope: str,
        run_id: str,
        record_ref: str,
    ) -> WebResearchRecordEnvelope:
        record_id = _record_id_from_ref(record_ref)
        document = await WebResearchRecordDocument.find_one(
            WebResearchRecordDocument.request_scope == request_scope,
            WebResearchRecordDocument.run_id == run_id,
            WebResearchRecordDocument.record_id == record_id,
        )
        if document is None:
            raise WebResearchRecordNotFound(f"web-research record not found: {record_ref}")
        record = _from_document(document)
        if web_research_record_ref(record) != record_ref:
            raise WebResearchRecordNotFound(
                f"web-research record digest mismatch: {record_ref}"
            )
        return record

    async def get_by_intent(
        self,
        request_scope: str,
        run_id: str,
        intent_key: str,
    ) -> WebResearchRecordEnvelope | None:
        document = await WebResearchRecordDocument.find_one(
            WebResearchRecordDocument.request_scope == request_scope,
            WebResearchRecordDocument.run_id == run_id,
            WebResearchRecordDocument.intent_key == intent_key,
        )
        return _from_document(document) if document is not None else None


def web_research_record_ref(record: WebResearchRecordEnvelope) -> str:
    return (
        f"belllabs://web-research/{record.run_id}/{record.record_kind}/"
        f"{record.record_id}/{record.content_digest.removeprefix('sha256:')}"
    )


def _record_id_from_ref(record_ref: str) -> str:
    parts = record_ref.split("/")
    if (
        len(parts) < 7
        or not record_ref.startswith("belllabs://web-research/")
        or len(parts[-1]) != 64
    ):
        raise WebResearchRecordNotFound(
            f"invalid web-research record reference: {record_ref}"
        )
    return parts[-2]


def _from_document(
    document: WebResearchRecordDocument,
) -> WebResearchRecordEnvelope:
    return WebResearchRecordEnvelope.model_validate(
        document.model_dump(mode="python", exclude={"id"})
    )
