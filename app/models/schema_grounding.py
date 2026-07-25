from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, DESCENDING, IndexModel


class SchemaGroundingRecordDocument(Document):
    record_type: str
    record_id: str
    request_scope: str
    run_id: str | None = None
    content_digest: str
    payload: dict[str, Any]
    created_at: datetime

    class Settings:
        name = "schema_grounding_records"
        indexes = [
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("record_type", ASCENDING),
                    ("record_id", ASCENDING),
                ],
                unique=True,
            ),
            IndexModel([("content_digest", ASCENDING)]),
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("run_id", ASCENDING),
                    ("record_type", ASCENDING),
                    ("created_at", DESCENDING),
                ]
            ),
        ]
