from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, IndexModel


class ArtifactMetadataRevisionDocument(Document):
    promotion_id: str
    artifact_id: str
    intent_key: str
    promotion_identity: str
    revision: int
    state: str
    run_id: str
    workspace_id: str
    content_digest: str
    payload: dict[str, Any]
    recorded_at: datetime

    class Settings:
        name = "artifact_metadata_revisions"
        indexes = [
            IndexModel([("promotion_id", ASCENDING), ("revision", ASCENDING)], unique=True),
            IndexModel([("artifact_id", ASCENDING), ("revision", ASCENDING)], unique=True),
            IndexModel([("intent_key", ASCENDING), ("revision", ASCENDING)], unique=True),
            IndexModel([("promotion_identity", ASCENDING), ("revision", ASCENDING)]),
            IndexModel([("run_id", ASCENDING), ("workspace_id", ASCENDING)]),
        ]
