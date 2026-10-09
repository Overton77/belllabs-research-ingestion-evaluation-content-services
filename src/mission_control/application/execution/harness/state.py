"""Harness execution lane state: native identity, provider cursor, usage disposition (FT-G2).

`lane.turn` writes the native identity of a session and turn *before* it observes (native
identity first, then observation), the provider cursor at every segment boundary, and the
usage disposition at settlement. A resumed segment reads them back: a recorded native turn
means the turn was sent, so a resume never sends it again. The production store updates
`mission_control.harness_execution` (migration 0030, G2 section); the row itself is opened
by the frame store (SPEC-03) before any of these writes.

MP-06: the same row carries the session owner and the dispatch journal (`dispatch.py`), so
identity, ownership and every journaled create/send change under one row lock.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.application.execution.harness.dispatch import (
    DispatchClaim,
    DispatchKind,
    DispatchOutcome,
    DispatchRecord,
    SessionOwner,
    assert_holder,
    claim_ownership,
    dispatch_key,
    intend,
    renew_ownership,
    resolve,
)
from mission_control.domain.execution.lanes import UsageDisposition


class LaneExecutionState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    harness_execution_id: UUID
    native_session_ref: str | None = Field(default=None, min_length=1)
    native_turn_ref: str | None = Field(default=None, min_length=1)
    provider_cursor: str | None = Field(default=None, min_length=1)
    usage_disposition: UsageDisposition | None = None
    last_segment_at: AwareDatetime | None = None
    cursor_sdk_version: str | None = None
    bridge_state_root: str | None = None
    cloud_branch: str | None = None
    cloud_agent_url: str | None = None
    # MP-06: the worker-owned session manager holding the session, and the journal of
    # every native create/send keyed by `dispatch_key(kind, idempotency_key)`.
    owner: SessionOwner | None = None
    dispatches: Mapping[str, DispatchRecord] = Field(default_factory=dict)

    def dispatch(self, kind: DispatchKind, idempotency_key: str) -> DispatchRecord | None:
        return self.dispatches.get(dispatch_key(kind, idempotency_key))


class LaneExecutionUpdate(BaseModel):
    """Fields one write sets; `None` leaves a field unchanged (identity is never cleared).

    FT-G4: a recorded native identity moves only by an explicit supersession naming the
    identity it replaces (compare-and-set): `supersedes_turn_ref` for a `cancel_and_replace`
    replacement turn on the same agent, `supersedes_session_ref` for a continuation handed to
    a fresh agent. The two guards are never stored.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    native_session_ref: str | None = Field(default=None, min_length=1)
    native_turn_ref: str | None = Field(default=None, min_length=1)
    provider_cursor: str | None = Field(default=None, min_length=1)
    usage_disposition: UsageDisposition | None = None
    last_segment_at: AwareDatetime | None = None
    cursor_sdk_version: str | None = None
    bridge_state_root: str | None = None
    cloud_branch: str | None = None
    cloud_agent_url: str | None = None
    supersedes_session_ref: str | None = Field(default=None, min_length=1)
    supersedes_turn_ref: str | None = Field(default=None, min_length=1)

    def fields(self) -> dict[str, object]:
        return {
            name: value
            for name, value in self.model_dump(
                exclude={"supersedes_session_ref", "supersedes_turn_ref"}
            ).items()
            if value is not None
        }


class LaneExecutionStateStore(Protocol):
    async def load(
        self, request_scope: str, harness_execution_id: UUID
    ) -> LaneExecutionState | None: ...

    async def record(
        self, request_scope: str, harness_execution_id: UUID, update: LaneExecutionUpdate
    ) -> LaneExecutionState: ...

    # --- MP-06 session ownership and dispatch journal ---------------------------------------

    async def claim_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        *,
        owner_ref: str,
        generation: int,
        now: datetime,
        lease: timedelta,
    ) -> SessionOwner:
        """Claim or renew the session (`claim_ownership`); a live foreign lease refuses."""
        ...

    async def renew_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        owner: SessionOwner,
        *,
        expires_at: datetime,
    ) -> SessionOwner: ...

    async def assert_owner(
        self, request_scope: str, harness_execution_id: UUID, owner: SessionOwner
    ) -> None:
        """Raise `StaleSessionOwner` unless `owner` still holds the session."""
        ...

    async def intend_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        record: DispatchRecord,
        *,
        owner: SessionOwner,
    ) -> DispatchClaim: ...

    async def resolve_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        kind: DispatchKind,
        idempotency_key: str,
        *,
        outcome: DispatchOutcome,
        owner: SessionOwner,
        at: datetime,
        native_ref: str | None = None,
        reason: str | None = None,
    ) -> DispatchRecord: ...


class NativeIdentityConflict(RuntimeError):
    """A write named a different native session or turn than the one already recorded."""


def merge_state(current: LaneExecutionState, update: LaneExecutionUpdate) -> LaneExecutionState:
    """Apply an update; a recorded native identity never changes to another value."""

    for name, guard in (
        ("native_session_ref", update.supersedes_session_ref),
        ("native_turn_ref", update.supersedes_turn_ref),
    ):
        recorded = getattr(current, name)
        incoming = getattr(update, name)
        if recorded is not None and incoming is not None and recorded != incoming:
            if guard is None or guard != recorded:
                raise NativeIdentityConflict(f"{name} is already {recorded}, not {incoming}")
    return current.model_copy(update=update.fields())


class InMemoryLaneExecutionStateStore:
    """Process-local store with the production merge rule (tests and local proof)."""

    def __init__(self) -> None:
        self._states: dict[tuple[str, UUID], LaneExecutionState] = {}
        self._lock = asyncio.Lock()

    async def load(
        self, request_scope: str, harness_execution_id: UUID
    ) -> LaneExecutionState | None:
        return self._states.get((request_scope, harness_execution_id))

    async def record(
        self, request_scope: str, harness_execution_id: UUID, update: LaneExecutionUpdate
    ) -> LaneExecutionState:
        async with self._lock:
            merged = merge_state(self._current(request_scope, harness_execution_id), update)
            self._states[(request_scope, harness_execution_id)] = merged
            return merged

    def _current(self, request_scope: str, harness_execution_id: UUID) -> LaneExecutionState:
        return self._states.get((request_scope, harness_execution_id)) or LaneExecutionState(
            harness_execution_id=harness_execution_id
        )

    def _put(self, request_scope: str, state: LaneExecutionState) -> None:
        self._states[(request_scope, state.harness_execution_id)] = state

    async def claim_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        *,
        owner_ref: str,
        generation: int,
        now: datetime,
        lease: timedelta,
    ) -> SessionOwner:
        async with self._lock:
            current = self._current(request_scope, harness_execution_id)
            owner = claim_ownership(
                current.owner, owner_ref=owner_ref, generation=generation, now=now, lease=lease
            )
            self._put(request_scope, current.model_copy(update={"owner": owner}))
            return owner

    async def renew_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        owner: SessionOwner,
        *,
        expires_at: datetime,
    ) -> SessionOwner:
        async with self._lock:
            current = self._current(request_scope, harness_execution_id)
            renewed = renew_ownership(current.owner, owner, expires_at=expires_at)
            self._put(request_scope, current.model_copy(update={"owner": renewed}))
            return renewed

    async def assert_owner(
        self, request_scope: str, harness_execution_id: UUID, owner: SessionOwner
    ) -> None:
        assert_holder(self._current(request_scope, harness_execution_id).owner, owner)

    async def intend_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        record: DispatchRecord,
        *,
        owner: SessionOwner,
    ) -> DispatchClaim:
        async with self._lock:
            current = self._current(request_scope, harness_execution_id)
            assert_holder(current.owner, owner)
            claim = intend(current.dispatches.get(record.key), record, owner)
            if claim.fresh:
                self._put(request_scope, _with_dispatch(current, claim.record))
            return claim

    async def resolve_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        kind: DispatchKind,
        idempotency_key: str,
        *,
        outcome: DispatchOutcome,
        owner: SessionOwner,
        at: datetime,
        native_ref: str | None = None,
        reason: str | None = None,
    ) -> DispatchRecord:
        async with self._lock:
            current = self._current(request_scope, harness_execution_id)
            assert_holder(current.owner, owner)
            record = resolve(
                current.dispatches.get(dispatch_key(kind, idempotency_key)),
                outcome=outcome,
                owner=owner,
                at=at,
                native_ref=native_ref,
                reason=reason,
            )
            self._put(request_scope, _with_dispatch(current, record))
            return record


def _with_dispatch(state: LaneExecutionState, record: DispatchRecord) -> LaneExecutionState:
    return state.model_copy(update={"dispatches": {**state.dispatches, record.key: record}})


def segment_update(cursor: str | None, at: datetime) -> LaneExecutionUpdate:
    return LaneExecutionUpdate(provider_cursor=cursor, last_segment_at=at)


__all__ = [
    "InMemoryLaneExecutionStateStore",
    "LaneExecutionState",
    "LaneExecutionStateStore",
    "LaneExecutionUpdate",
    "NativeIdentityConflict",
    "merge_state",
    "segment_update",
]
