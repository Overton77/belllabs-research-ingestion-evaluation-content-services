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
    kind: str
    logical_id: str
    revision: int = Field(ge=1)
    digest: str
    definition: dict[str, Any]
    published_at: datetime
    published_by: str

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
