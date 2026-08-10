from __future__ import annotations

from datetime import datetime
from typing import Any

from beanie import Document
from pydantic import Field
from pymongo import ASCENDING, IndexModel


class DefinitionHeadDocument(Document):
    kind: str
    logical_id: str
    published_revision: int = Field(default=0, ge=0)
    draft_revision: int = Field(default=0, ge=0)
    draft_definition: dict[str, Any] | None = None
    updated_at: datetime
    updated_by: str

    class Settings:
        name = "control_plane_definition_heads"
        use_revision = True
        indexes = [IndexModel([("kind", ASCENDING), ("logical_id", ASCENDING)], unique=True)]


class DefinitionAliasDocument(Document):
    kind: str
    logical_id: str
    alias: str
    target_revision: int
    target_digest: str
    moved_at: datetime
    moved_by: str

    class Settings:
        name = "control_plane_definition_aliases"
        use_revision = True
        indexes = [
            IndexModel(
                [("kind", ASCENDING), ("logical_id", ASCENDING), ("alias", ASCENDING)],
                unique=True,
            )
        ]


class DefinitionAliasMovementDocument(Document):
    kind: str
    logical_id: str
    alias: str
    target_revision: int
    target_digest: str
    moved_at: datetime
    moved_by: str

    class Settings:
        name = "control_plane_definition_alias_movements"
        indexes = [
            IndexModel(
                [
                    ("kind", ASCENDING),
                    ("logical_id", ASCENDING),
                    ("alias", ASCENDING),
                    ("moved_at", ASCENDING),
                ]
            )
        ]


class PublishedDefinitionDocument(Document):
    contract_id: str = "CON-CP-DEFINITION-REF-V1"
    schema_version: str = "1"
    kind: str
    logical_id: str
    revision: int = Field(ge=1)
    digest: str
    definition: dict[str, Any]
    published_at: datetime
    published_by: str
    lifecycle_status: str = "published"
    payload_ref: dict[str, Any] | None = None

    class Settings:
        name = "control_plane_published_definitions"
        indexes = [
            IndexModel(
                [("kind", ASCENDING), ("logical_id", ASCENDING), ("revision", ASCENDING)],
                unique=True,
            ),
            IndexModel([("digest", ASCENDING)]),
        ]


class DefinitionRetirementDocument(Document):
    kind: str
    logical_id: str
    revision: int = Field(ge=1)
    digest: str
    retired_at: datetime
    retired_by: str

    class Settings:
        name = "control_plane_definition_retirements"
        indexes = [
            IndexModel(
                [("kind", ASCENDING), ("logical_id", ASCENDING), ("revision", ASCENDING)],
                unique=True,
            )
        ]


class EffectiveRunConfigurationDocument(Document):
    contract_id: str = "CON-CP-ERC-V1"
    schema_version: str = "1"
    digest: str
    compiler_version: str
    compilation_id: str
    compiled_at: datetime
    payload: dict[str, Any] | None = None
    payload_ref: dict[str, Any] | None = None

    class Settings:
        name = "control_plane_effective_run_configurations"
        indexes = [
            IndexModel([("digest", ASCENDING)], unique=True),
            IndexModel([("compilation_id", ASCENDING)], unique=True),
        ]


class CatalogProjectionEventDocument(Document):
    event_id: str
    tenant_scope: str
    asset_kind: str
    logical_id: str
    revision: int = Field(ge=1)
    source_digest: str
    operation: str
    state: str = "pending"
    attempt_count: int = Field(default=0, ge=0)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime
    last_error_code: str | None = None
    poison_reason: str | None = None
    completed_at: datetime | None = None
    created_at: datetime

    class Settings:
        name = "catalog_projection_events"
        use_revision = True
        indexes = [
            IndexModel([("event_id", ASCENDING)], unique=True),
            IndexModel(
                [
                    ("state", ASCENDING),
                    ("next_attempt_at", ASCENDING),
                    ("lease_expires_at", ASCENDING),
                ]
            ),
            IndexModel(
                [
                    ("tenant_scope", ASCENDING),
                    ("asset_kind", ASCENDING),
                    ("logical_id", ASCENDING),
                    ("revision", ASCENDING),
                ]
            ),
        ]


class CatalogProjectionAlertDocument(Document):
    alert_id: str
    event_id: str
    error_code: str
    emitted_at: datetime
    acknowledged_at: datetime | None = None

    class Settings:
        name = "catalog_projection_alerts"
        indexes = [
            IndexModel([("alert_id", ASCENDING)], unique=True),
            IndexModel([("event_id", ASCENDING), ("emitted_at", ASCENDING)]),
            IndexModel([("acknowledged_at", ASCENDING), ("emitted_at", ASCENDING)]),
        ]
