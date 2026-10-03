from __future__ import annotations

import asyncio
from typing import Protocol

from biotech_mission_adapters.domain.coordinator.web_research_runtime import (
    WebResearchRecordEnvelope,
)


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
        raise WebResearchRecordNotFound(f"invalid web-research record reference: {record_ref}")
    return parts[-2]
