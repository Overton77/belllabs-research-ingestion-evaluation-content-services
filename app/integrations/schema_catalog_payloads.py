from __future__ import annotations

from app.config import Settings
from app.integrations.control_plane_payloads import (
    ContentAddressedPayloadStore,
    InMemoryPayloadStore,
    S3PayloadStore,
    UnavailablePayloadStore,
)


def schema_catalog_payload_store(
    settings: Settings,
) -> ContentAddressedPayloadStore:
    """Select the durable catalog bundle authority without falling back to local folders."""
    if settings.s3_bucket:
        return S3PayloadStore(
            settings,
            settings.s3_bucket,
            prefix="schema-grounding/catalog-builds",
        )
    return UnavailablePayloadStore()


__all__ = [
    "ContentAddressedPayloadStore",
    "InMemoryPayloadStore",
    "schema_catalog_payload_store",
]
