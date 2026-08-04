from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.config import Settings
from app.integrations.control_plane_payloads import S3PayloadStore

SchemaGroundingInputKind = Literal["schema", "semantic_overlay", "report"]


@dataclass(frozen=True, slots=True)
class SchemaGroundingInputObjectFormat:
    media_type: str
    suffix: str


SCHEMA_GROUNDING_INPUT_FORMATS: dict[
    SchemaGroundingInputKind,
    SchemaGroundingInputObjectFormat,
] = {
    "schema": SchemaGroundingInputObjectFormat(
        media_type="application/graphql",
        suffix=".graphql",
    ),
    "semantic_overlay": SchemaGroundingInputObjectFormat(
        media_type="application/json",
        suffix=".json",
    ),
    "report": SchemaGroundingInputObjectFormat(
        media_type="text/markdown; charset=utf-8",
        suffix=".md",
    ),
}


def schema_grounding_input_store(
    settings: Settings,
    bucket: str,
    kind: SchemaGroundingInputKind,
) -> S3PayloadStore:
    object_format = SCHEMA_GROUNDING_INPUT_FORMATS[kind]
    return S3PayloadStore(
        settings,
        bucket,
        prefix="schema-grounding/live-inputs",
        media_type=object_format.media_type,
        suffix=object_format.suffix,
    )


def schema_grounding_input_uri(
    bucket: str,
    digest: str,
    kind: SchemaGroundingInputKind,
) -> str:
    object_format = SCHEMA_GROUNDING_INPUT_FORMATS[kind]
    key = digest.removeprefix("sha256:")
    return (
        f"s3://{bucket}/schema-grounding/live-inputs/{key}{object_format.suffix}"
    )


__all__ = [
    "SCHEMA_GROUNDING_INPUT_FORMATS",
    "SchemaGroundingInputKind",
    "SchemaGroundingInputObjectFormat",
    "schema_grounding_input_store",
    "schema_grounding_input_uri",
]
