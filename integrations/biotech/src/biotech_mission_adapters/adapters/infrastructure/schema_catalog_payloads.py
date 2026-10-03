from __future__ import annotations

from mission_control.adapters.storage.control_plane_payloads import (
    InMemoryPayloadStore,
    S3PayloadStore,
    UnavailablePayloadStore,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.bootstrap.settings import Settings


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
