from __future__ import annotations

from datetime import datetime

from app.application.catalog_projection_admin import ProjectionEventCompletionPort
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.models import CatalogProjectionEventDocument, PublishedDefinitionDocument


class BeanieProjectionEventCompletionRepository(ProjectionEventCompletionPort):
    async def complete_for_ref(
        self,
        ref: ExactDefinitionRef,
        *,
        tenant_scope: str,
        completed_at: datetime,
    ) -> int:
        result = await CatalogProjectionEventDocument.get_pymongo_collection().update_many(
            {
                "tenant_scope": tenant_scope,
                "asset_kind": ref.kind.value,
                "logical_id": ref.logical_id,
                "revision": ref.revision,
                "source_digest": ref.digest,
                "$or": [
                    {"state": {"$in": ["pending", "retry", "poison"]}},
                    {
                        "state": "processing",
                        "lease_expires_at": {"$lte": completed_at},
                    },
                ],
            },
            {
                "$set": {
                    "state": "completed",
                    "completed_at": completed_at,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": None,
                    "poison_reason": None,
                }
            },
        )
        return int(result.modified_count)


async def list_published_definition_refs() -> tuple[ExactDefinitionRef, ...]:
    documents = await PublishedDefinitionDocument.find_all().to_list()
    refs = (
        ExactDefinitionRef(
            kind=DefinitionKind(document.kind),
            logical_id=document.logical_id,
            revision=document.revision,
            digest=document.digest,
        )
        for document in documents
    )
    return tuple(
        sorted(
            refs,
            key=lambda item: (
                item.kind.value,
                item.logical_id,
                item.revision,
                item.digest,
            ),
        )
    )
