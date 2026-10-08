"""Harness execution lane state: native identity, provider cursor, usage disposition (FT-G2).

`lane.turn` writes the native identity of a session and turn *before* it observes (native
identity first, then observation), the provider cursor at every segment boundary, and the
usage disposition at settlement. A resumed segment reads them back: a recorded native turn
means the turn was sent, so a resume never sends it again. The production store updates
`mission_control.harness_execution` (migration 0030, G2 section); the row itself is opened
by the frame store (SPEC-03) before any of these writes.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

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
            key = (request_scope, harness_execution_id)
            current = self._states.get(key) or LaneExecutionState(
                harness_execution_id=harness_execution_id
            )
            merged = merge_state(current, update)
            self._states[key] = merged
            return merged


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
