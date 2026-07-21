from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, IndexModel


class WorkspaceSlotReservationDocument(Document):
    namespace_id: str
    workspace_id: str
    logical_path: str
    owner_id: str
    reservation_token: str
    reserved_at: datetime

    class Settings:
        name = "workspace_slot_reservations"
        indexes = [
            IndexModel([("namespace_id", ASCENDING), ("logical_path", ASCENDING)], unique=True),
            IndexModel([("workspace_id", ASCENDING), ("owner_id", ASCENDING)]),
        ]


class WorkspaceMaterializationManifestDocument(Document):
    manifest_id: str
    namespace_id: str
    workspace_id: str
    revision: int
    manifest_digest: str
    prior_manifest_digest: str | None = None
    payload: dict[str, Any]
    created_at: datetime

    class Settings:
        name = "workspace_materialization_manifests"
        indexes = [
            IndexModel([("manifest_id", ASCENDING)], unique=True),
            IndexModel([("manifest_digest", ASCENDING)], unique=True),
            IndexModel(
                [("namespace_id", ASCENDING), ("workspace_id", ASCENDING), ("revision", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [
                    ("namespace_id", ASCENDING),
                    ("workspace_id", ASCENDING),
                    ("created_at", ASCENDING),
                ]
            ),
        ]
