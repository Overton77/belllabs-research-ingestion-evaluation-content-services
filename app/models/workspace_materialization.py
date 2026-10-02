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


class WorkspaceCandidateDocument(Document):
    """A captured writable-slot candidate's descriptor and its object-store address.

    RRM-009: candidate bytes live in the content-addressed artifact payload store, so a
    replacement worker or the promotion activity reads them without the capturing worker's
    local disk. The descriptor is insert-once per candidate identity.
    """

    candidate_id: str
    namespace_id: str
    workspace_id: str
    logical_path: str
    content_digest: str
    descriptor: dict[str, Any]
    object_ref: str
    size_bytes: int
    recorded_at: datetime

    class Settings:
        name = "workspace_candidates"
        indexes = [
            IndexModel([("candidate_id", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("namespace_id", ASCENDING),
                    ("workspace_id", ASCENDING),
                    ("logical_path", ASCENDING),
                    ("recorded_at", ASCENDING),
                ]
            ),
        ]
