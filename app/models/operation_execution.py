from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pymongo import ASCENDING, IndexModel


class OperationExecutionBindingDocument(Document):
    request_scope: str | None = None
    binding_id: str
    semantic_attempt_key: str
    request_fingerprint: str
    run_id: str
    operation_id: str
    operation_attempt: int
    payload: dict[str, Any]
    bound_at: datetime

    class Settings:
        name = "operation_execution_bindings"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("binding_id", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("request_scope", ASCENDING), ("semantic_attempt_key", ASCENDING)],
                unique=True,
            ),
            IndexModel([("run_id", ASCENDING), ("operation_id", ASCENDING)]),
        ]


class OperationSettlementDocument(Document):
    request_scope: str | None = None
    settlement_id: str
    binding_id: str
    payload: dict[str, Any]
    settled_at: datetime

    class Settings:
        name = "operation_execution_settlements"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("settlement_id", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("request_scope", ASCENDING), ("binding_id", ASCENDING)],
                unique=True,
            ),
        ]


class OperationExecutionClaimDocument(Document):
    request_scope: str | None = None
    side_effect_key: str
    binding_id: str
    claimed_at: datetime

    class Settings:
        name = "operation_execution_claims"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("side_effect_key", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("request_scope", ASCENDING), ("binding_id", ASCENDING)],
                unique=True,
            ),
        ]


class OperationExecutionBindingAuthorityV2Document(Document):
    """Digest-verified Mongo authority projection; legacy source remains read-only."""

    schema_version: str = "2"
    binding_id: str
    semantic_attempt_key: str
    request_scope: str
    canonical_digest: str
    payload: dict[str, Any]
    source_collection: str
    source_document_id: str
    source_bound_at: datetime
    migrated_at: datetime

    class Settings:
        name = "operation_execution_binding_authority_v2"
        indexes = [
            IndexModel(
                [("request_scope", ASCENDING), ("binding_id", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("request_scope", ASCENDING), ("semantic_attempt_key", ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [("source_collection", ASCENDING), ("source_document_id", ASCENDING)],
                unique=True,
            ),
        ]


class OperationMigrationQuarantineDocument(Document):
    quarantine_id: str
    source_collection: str
    source_document_id: str
    reason_code: str
    observed_digest: str | None = None
    expected_digest: str | None = None
    quarantined_at: datetime

    class Settings:
        name = "operation_execution_migration_quarantine"
        indexes = [
            IndexModel([("quarantine_id", ASCENDING)], unique=True),
            IndexModel(
                [("source_collection", ASCENDING), ("source_document_id", ASCENDING)],
                unique=True,
            ),
        ]
