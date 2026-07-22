from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, IndexModel


class SandboxSnapshotDocument(Document):
    snapshot_id: str
    creation_identity: str
    request_scope: str
    source_namespace_id: str
    source_workspace_id: str
    parent_snapshot_id: str | None = None
    provider: str
    provider_snapshot_id: str
    payload_digest: str
    object_ref: str
    producer_binding_id: str
    created_at: datetime
    retain_until: datetime | None = None
    payload: dict[str, Any]

    class Settings:
        name = "sandbox_snapshots"
        indexes = [
            IndexModel([("snapshot_id", ASCENDING)], unique=True),
            IndexModel([("creation_identity", ASCENDING)], unique=True),
            IndexModel([("payload_digest", ASCENDING)]),
            IndexModel([("object_ref", ASCENDING)]),
            IndexModel([("parent_snapshot_id", ASCENDING), ("created_at", ASCENDING)]),
            IndexModel(
                [
                    ("request_scope", ASCENDING),
                    ("source_workspace_id", ASCENDING),
                    ("created_at", ASCENDING),
                ]
            ),
            IndexModel([("retain_until", ASCENDING)]),
        ]


class SandboxSnapshotCloneDocument(Document):
    clone_id: str
    snapshot_id: str
    parent_workspace_id: str
    target_namespace_id: str
    target_workspace_id: str
    binding_id: str
    created_at: datetime
    payload: dict[str, Any]

    class Settings:
        name = "sandbox_snapshot_clones"
        indexes = [
            IndexModel([("clone_id", ASCENDING)], unique=True),
            IndexModel(
                [("target_namespace_id", ASCENDING), ("target_workspace_id", ASCENDING)],
                unique=True,
            ),
            IndexModel([("snapshot_id", ASCENDING), ("created_at", ASCENDING)]),
            IndexModel([("binding_id", ASCENDING)]),
        ]


class SandboxSnapshotClaimDocument(Document):
    claim_key: str
    claim_kind: str
    identity: str
    fingerprint: str
    claimed_at: datetime

    class Settings:
        name = "sandbox_snapshot_claims"
        indexes = [
            IndexModel([("claim_key", ASCENDING)], unique=True),
            IndexModel([("claim_kind", ASCENDING), ("identity", ASCENDING)], unique=True),
            IndexModel([("claimed_at", ASCENDING)]),
        ]
