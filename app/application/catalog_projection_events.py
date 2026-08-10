from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.application.catalog_projection import (
    CatalogProjectionError,
    CatalogProjectionInput,
    CatalogProjector,
)
from app.application.catalog_projection_generation import (
    ProjectionGenerationRepository,
)
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.errors import CoordinatorDomainError


class ProjectionEventState(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY = "retry"
    COMPLETED = "completed"
    POISON = "poison"


class ProjectionEventContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogProjectionEvent(ProjectionEventContract):
    event_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    tenant_scope: str = Field(min_length=1)
    asset_kind: DefinitionKind
    logical_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    operation: str = Field(pattern=r"^(upsert|retire)$")
    state: ProjectionEventState
    attempt_count: int = Field(ge=0)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime
    last_error_code: str | None = None
    poison_reason: str | None = None
    completed_at: datetime | None = None
    created_at: datetime

    @property
    def exact_ref(self) -> ExactDefinitionRef:
        return ExactDefinitionRef(
            kind=self.asset_kind,
            logical_id=self.logical_id,
            revision=self.revision,
            digest=self.source_digest,
        )


class ProjectionEventFailure(ProjectionEventContract):
    error_code: str = Field(min_length=1, max_length=128)
    retryable: bool


class ProjectionOperationalAlert(ProjectionEventContract):
    alert_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    event_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    error_code: str = Field(min_length=1, max_length=128)
    emitted_at: datetime


class ProjectionEventRepository(Protocol):
    async def claim_batch(
        self,
        *,
        owner: str,
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[CatalogProjectionEvent, ...]: ...

    async def complete(
        self,
        event: CatalogProjectionEvent,
        *,
        owner: str,
        completed_at: datetime,
    ) -> bool: ...

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
    ) -> CatalogProjectionEvent | None: ...


class InMemoryProjectionEventRepository:
    def __init__(self, events: tuple[CatalogProjectionEvent, ...] = ()) -> None:
        self._events = {event.event_id: event for event in events}
        self._alerts: dict[str, ProjectionOperationalAlert] = {}
        self._lock = asyncio.Lock()

    async def add(self, event: CatalogProjectionEvent) -> None:
        async with self._lock:
            self._events.setdefault(event.event_id, event)

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
        async with self._lock:
            eligible = sorted(
                (
                    event
                    for event in self._events.values()
                    if event.state
                    in {
                        ProjectionEventState.PENDING,
                        ProjectionEventState.RETRY,
                        ProjectionEventState.PROCESSING,
                    }
                    and event.next_attempt_at <= now
                    and (event.lease_expires_at is None or event.lease_expires_at <= now)
                ),
                key=lambda event: (event.next_attempt_at, event.created_at, event.event_id),
            )[:limit]
            claimed: list[CatalogProjectionEvent] = []
            for event in eligible:
                updated = event.model_copy(
                    update={
                        "state": ProjectionEventState.PROCESSING,
                        "attempt_count": event.attempt_count + 1,
                        "lease_owner": owner,
                        "lease_expires_at": now + lease_duration,
                    }
                )
                self._events[event.event_id] = updated
                claimed.append(updated)
            return tuple(claimed)

    async def complete(
        self,
        event: CatalogProjectionEvent,
        *,
        owner: str,
        completed_at: datetime,
    ) -> bool:
        async with self._lock:
            current = self._events.get(event.event_id)
            if (
                current is None
                or current.state != ProjectionEventState.PROCESSING
                or current.lease_owner != owner
                or current.lease_expires_at is None
                or current.lease_expires_at <= completed_at
                or current.attempt_count != event.attempt_count
            ):
                return False
            self._events[event.event_id] = current.model_copy(
                update={
                    "state": ProjectionEventState.COMPLETED,
                    "completed_at": completed_at,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_error_code": None,
                    "poison_reason": None,
                }
            )
            return True

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
        async with self._lock:
            current = self._events.get(event.event_id)
            if (
                current is None
                or current.state != ProjectionEventState.PROCESSING
                or current.lease_owner != owner
                or current.attempt_count != event.attempt_count
            ):
                return None
            poison = not failure.retryable or current.attempt_count >= max_attempts
            delay = bounded_projection_backoff(
                current.attempt_count,
                base=base_backoff,
                maximum=max_backoff,
            )
            updated = current.model_copy(
                update={
                    "state": (
                        ProjectionEventState.POISON if poison else ProjectionEventState.RETRY
                    ),
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "next_attempt_at": (failed_at if poison else failed_at + delay),
                    "last_error_code": failure.error_code,
                    "poison_reason": failure.error_code if poison else None,
                }
            )
            self._events[event.event_id] = updated
            if poison:
                alert = projection_alert(updated, failure.error_code, failed_at)
                self._alerts.setdefault(alert.alert_id, alert)
            return updated

    async def get(self, event_id: str) -> CatalogProjectionEvent | None:
        async with self._lock:
            return self._events.get(event_id)

    async def alerts(self) -> tuple[ProjectionOperationalAlert, ...]:
        async with self._lock:
            return tuple(sorted(self._alerts.values(), key=lambda alert: alert.alert_id))


class ProjectionProcessingSummary(ProjectionEventContract):
    claimed: int = Field(ge=0)
    completed: int = Field(ge=0)
    retried: int = Field(ge=0)
    poisoned: int = Field(ge=0)
    lease_lost: int = Field(ge=0)


class CatalogProjectionEventProcessor:
    def __init__(
        self,
        *,
        events: ProjectionEventRepository,
        generations: ProjectionGenerationRepository,
        projector_factory: Callable[[str], CatalogProjector],
        lease_duration: timedelta = timedelta(minutes=2),
        max_attempts: int = 6,
        base_backoff: timedelta = timedelta(seconds=5),
        max_backoff: timedelta = timedelta(minutes=15),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if (
            lease_duration <= timedelta(0)
            or max_attempts < 1
            or base_backoff <= timedelta(0)
            or max_backoff < base_backoff
        ):
            raise ValueError("projection event processor configuration is invalid")
        self._events = events
        self._generations = generations
        self._projector_factory = projector_factory
        self._lease_duration = lease_duration
        self._max_attempts = max_attempts
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._clock = clock or (lambda: datetime.now(UTC))

    async def process_batch(
        self,
        *,
        owner: str,
        limit: int = 64,
    ) -> ProjectionProcessingSummary:
        claimed = await self._events.claim_batch(
            owner=owner,
            now=self._clock(),
            lease_duration=self._lease_duration,
            limit=limit,
        )
        groups: dict[tuple[str, str], list[CatalogProjectionEvent]] = defaultdict(list)
        without_generation: list[CatalogProjectionEvent] = []
        for event in claimed:
            generation = await self._generations.active_for_kind(
                event.tenant_scope,
                event.asset_kind,
            )
            if generation is None:
                without_generation.append(event)
            else:
                groups[(event.tenant_scope, generation)].append(event)

        completed = 0
        retried = 0
        poisoned = 0
        lease_lost = 0
        for event in without_generation:
            outcome = await self._fail(
                event,
                owner,
                ProjectionEventFailure(
                    error_code="PROJECTION_GENERATION_UNAVAILABLE",
                    retryable=True,
                ),
            )
            retried += int(outcome == ProjectionEventState.RETRY)
            poisoned += int(outcome == ProjectionEventState.POISON)
            lease_lost += int(outcome is None)

        for (tenant_scope, generation), events in groups.items():
            projector = self._projector_factory(generation)
            try:
                await projector.project_many(
                    tuple(CatalogProjectionInput(ref=event.exact_ref) for event in events),
                    tenant_scope=tenant_scope,
                )
            except Exception as error:
                failure = classify_projection_failure(error)
                if not failure.retryable and len(events) > 1:
                    for event in events:
                        try:
                            await projector.project_many(
                                (CatalogProjectionInput(ref=event.exact_ref),),
                                tenant_scope=tenant_scope,
                            )
                        except Exception as isolated_error:
                            outcome = await self._fail(
                                event,
                                owner,
                                classify_projection_failure(isolated_error),
                            )
                            retried += int(outcome == ProjectionEventState.RETRY)
                            poisoned += int(outcome == ProjectionEventState.POISON)
                            lease_lost += int(outcome is None)
                            continue
                        did_complete = await self._events.complete(
                            event,
                            owner=owner,
                            completed_at=self._clock(),
                        )
                        completed += int(did_complete)
                        lease_lost += int(not did_complete)
                    continue
                for event in events:
                    outcome = await self._fail(event, owner, failure)
                    retried += int(outcome == ProjectionEventState.RETRY)
                    poisoned += int(outcome == ProjectionEventState.POISON)
                    lease_lost += int(outcome is None)
                continue
            for event in events:
                did_complete = await self._events.complete(
                    event,
                    owner=owner,
                    completed_at=self._clock(),
                )
                completed += int(did_complete)
                lease_lost += int(not did_complete)
        return ProjectionProcessingSummary(
            claimed=len(claimed),
            completed=completed,
            retried=retried,
            poisoned=poisoned,
            lease_lost=lease_lost,
        )

    async def _fail(
        self,
        event: CatalogProjectionEvent,
        owner: str,
        failure: ProjectionEventFailure,
    ) -> ProjectionEventState | None:
        updated = await self._events.fail(
            event,
            owner=owner,
            failed_at=self._clock(),
            failure=failure,
            max_attempts=self._max_attempts,
            base_backoff=self._base_backoff,
            max_backoff=self._max_backoff,
        )
        return updated.state if updated is not None else None


def bounded_projection_backoff(
    attempt_count: int,
    *,
    base: timedelta,
    maximum: timedelta,
) -> timedelta:
    exponent = max(attempt_count - 1, 0)
    return min(base * (2**exponent), maximum)


def classify_projection_failure(error: Exception) -> ProjectionEventFailure:
    if isinstance(error, CoordinatorDomainError):
        return ProjectionEventFailure(
            error_code=error.code.value,
            retryable=error.retryable,
        )
    if isinstance(error, (CatalogProjectionError, ValueError)):
        return ProjectionEventFailure(
            error_code="PROJECTION_CONTRACT_ERROR",
            retryable=False,
        )
    return ProjectionEventFailure(
        error_code="PROJECTION_DEPENDENCY_ERROR",
        retryable=True,
    )


def projection_alert(
    event: CatalogProjectionEvent,
    error_code: str,
    emitted_at: datetime,
) -> ProjectionOperationalAlert:
    from hashlib import sha256

    material = f"{event.event_id}|{event.attempt_count}|{error_code}|poison"
    return ProjectionOperationalAlert(
        alert_id=f"sha256:{sha256(material.encode()).hexdigest()}",
        event_id=event.event_id,
        error_code=error_code,
        emitted_at=emitted_at,
    )
