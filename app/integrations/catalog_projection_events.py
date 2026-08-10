from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pymongo import ASCENDING, ReturnDocument

from app.application.catalog_projection_events import (
    CatalogProjectionEvent,
    ProjectionEventFailure,
    ProjectionEventRepository,
    ProjectionEventState,
    bounded_projection_backoff,
    projection_alert,
)
from app.models import (
    CatalogProjectionAlertDocument,
    CatalogProjectionEventDocument,
)


class BeanieProjectionEventRepository(ProjectionEventRepository):
    async def claim_batch(
        self,
        *,
        owner: str,
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[CatalogProjectionEvent, ...]:
        if not owner or lease_duration <= timedelta(0) or limit < 1:
            raise ValueError("projection event claim configuration is invalid")
        collection = CatalogProjectionEventDocument.get_pymongo_collection()
        claimed: list[CatalogProjectionEvent] = []
        for _ in range(limit):
            document = await collection.find_one_and_update(
                {
                    "state": {
                        "$in": [
                            ProjectionEventState.PENDING.value,
                            ProjectionEventState.RETRY.value,
                            ProjectionEventState.PROCESSING.value,
                        ]
                    },
                    "next_attempt_at": {"$lte": now},
                    "$or": [
                        {"lease_expires_at": None},
                        {"lease_expires_at": {"$lte": now}},
                    ],
                },
                {
                    "$set": {
                        "state": ProjectionEventState.PROCESSING.value,
                        "lease_owner": owner,
                        "lease_expires_at": now + lease_duration,
                    },
                    "$inc": {"attempt_count": 1},
                },
                sort=[
                    ("next_attempt_at", ASCENDING),
                    ("created_at", ASCENDING),
                    ("event_id", ASCENDING),
                ],
                return_document=ReturnDocument.AFTER,
            )
            if document is None:
                break
            claimed.append(_event(document))
        return tuple(claimed)

    async def complete(
        self,
        event: CatalogProjectionEvent,
        *,
        owner: str,
        completed_at: datetime,
    ) -> bool:
        result = await CatalogProjectionEventDocument.get_pymongo_collection().update_one(
            {
                "event_id": event.event_id,
                "state": ProjectionEventState.PROCESSING.value,
                "attempt_count": event.attempt_count,
                "lease_owner": owner,
                "lease_expires_at": {"$gt": completed_at},
            },
            {
                "$set": {
                    "state": ProjectionEventState.COMPLETED.value,
                    "completed_at": completed_at,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": None,
                    "poison_reason": None,
                }
            },
        )
        return result.modified_count == 1

    async def fail(
        self,
        event: CatalogProjectionEvent,
        *,
        owner: str,
        failed_at: datetime,
        failure: ProjectionEventFailure,
        max_attempts: int,
        base_backoff: timedelta,
        max_backoff: timedelta,
    ) -> CatalogProjectionEvent | None:
        poison = not failure.retryable or event.attempt_count >= max_attempts
        delay = bounded_projection_backoff(
            event.attempt_count,
            base=base_backoff,
            maximum=max_backoff,
        )
        collection = CatalogProjectionEventDocument.get_pymongo_collection()
        async with collection.database.client.start_session() as session:
            async with await session.start_transaction():
                updated = await collection.find_one_and_update(
                    {
                        "event_id": event.event_id,
                        "state": ProjectionEventState.PROCESSING.value,
                        "attempt_count": event.attempt_count,
                        "lease_owner": owner,
                        "lease_expires_at": {"$gt": failed_at},
                    },
                    {
                        "$set": {
                            "state": (
                                ProjectionEventState.POISON.value
                                if poison
                                else ProjectionEventState.RETRY.value
                            ),
                            "lease_owner": None,
                            "lease_expires_at": None,
                            "next_attempt_at": (failed_at if poison else failed_at + delay),
                            "last_error_code": failure.error_code,
                            "poison_reason": (failure.error_code if poison else None),
                        }
                    },
                    return_document=ReturnDocument.AFTER,
                    session=session,
                )
                if updated is not None and poison:
                    projected_event = _event(updated)
                    alert = projection_alert(
                        projected_event,
                        failure.error_code,
                        failed_at,
                    )
                    await CatalogProjectionAlertDocument.get_pymongo_collection().update_one(
                        {"alert_id": alert.alert_id},
                        {
                            "$setOnInsert": {
                                **alert.model_dump(mode="python"),
                                "acknowledged_at": None,
                            }
                        },
                        upsert=True,
                        session=session,
                    )
        if updated is None:
            return None
        projected_event = _event(updated)
        return projected_event


def _event(document: dict[str, Any]) -> CatalogProjectionEvent:
    return CatalogProjectionEvent.model_validate(
        {key: value for key, value in document.items() if key not in {"_id", "revision_id"}}
    )
