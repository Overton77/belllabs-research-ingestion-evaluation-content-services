from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, DESCENDING, IndexModel


class WebResearchRecordDocument(Document):
    record_kind: str
    record_id: str
    intent_key: str
    request_scope: str
    run_id: str
    payload: dict[str, Any]
    content_digest: str
    created_at: datetime

    class Settings:
        name = "web_research_records"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("record_id", ASCENDING),
                ],
                unique=True,
            ),
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("run_id", ASCENDING),
                    ("intent_key", ASCENDING),
                ],
                unique=True,
            ),
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("run_id", ASCENDING),
                    ("record_kind", ASCENDING),
                    ("created_at", DESCENDING),
                ]
            ),
            IndexModel([("content_digest", ASCENDING)]),
        ]
