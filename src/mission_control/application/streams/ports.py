"""Ports for scoped mission streams (multi-provider SPEC-04, MP-14).

A `StreamSource` is bound to one installation/application/tenant scope and reads the two
durable streams from their authorities: the reducer journal (`mission_event.seq` per
mission, high-watermark `mission.last_event_seq`) and the Native Event Store
(`provider_frame.arrival_ordinal` per harness execution and generation). It never writes.

A `StreamHintSource` only wakes readers early; a hint carries no event data and a lost hint
delays delivery until the next periodic high-watermark comparison, never loses it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from mission_control.domain.frames.contracts import ProviderFrame
from mission_control.domain.subscriptions.contracts import MissionEventEnvelope
from mission_control.domain.subscriptions.streams import StreamTarget


class StreamTargetNotFound(LookupError):
    """No target with that id is visible in this scope (absent and foreign look the same)."""


@dataclass(frozen=True)
class LinkedMission:
    """A chain link naming the target's mission as its upstream (`linked_mission` lineage).

    Read under the same tenant scope as everything else, so a descendant the caller cannot
    see is simply absent; its own journal is a separate cursor domain.
    """

    link_id: UUID
    link_key: str
    from_mission_id: UUID
    to_mission_id: UUID
    state: str
    released_run_key: str | None = None


@dataclass(frozen=True)
class ResolvedTarget:
    """A target under the caller's scope. A `chain` target names its first member as
    `mission_id` and every member mission (each its own journal) in `members`."""

    target: StreamTarget
    mission_id: UUID
    run_id: UUID | None = None
    run_key: str | None = None
    harness_execution_id: UUID | None = None
    chain_id: UUID | None = None
    members: tuple[UUID, ...] = ()


@dataclass(frozen=True)
class ExecutionRef:
    """One harness execution of a run or mission target and its current generation."""

    harness_execution_id: UUID
    generation: int


@dataclass(frozen=True)
class ChainState:
    """A chain's lifecycle as the reducer recorded it, plus its links (scope-filtered)."""

    chain_id: UUID
    chain_key: str
    lifecycle: str
    phase: str
    terminal_outcome: str | None
    version: int
    links: tuple[LinkedMission, ...] = ()


@dataclass(frozen=True)
class FrameRetentionProbe:
    """What retention left around a provider-frame position.

    `cursor_present`: the frame at the position still exists; `next_ordinal`: the first
    retained ordinal after it; `horizon_passed`: the execution has frames (or started)
    before the application's retention cutoff, so expiry may have deleted some."""

    cursor_present: bool
    next_ordinal: int | None
    horizon_passed: bool


class StreamSource(Protocol):
    @property
    def request_scope(self) -> str: ...

    async def resolve(self, target: StreamTarget) -> ResolvedTarget: ...

    async def mission_high_watermark(self, mission_id: UUID) -> int: ...

    async def mission_low_watermark(self, mission_id: UUID) -> int | None: ...

    async def mission_events(
        self, target: ResolvedTarget, *, after_seq: int, upto_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]: ...

    async def linked_missions(self, mission_id: UUID) -> tuple[LinkedMission, ...]:
        """Chain links whose upstream is `mission_id`, visible in this scope."""
        ...

    async def frame_generation(self, harness_execution_id: UUID) -> int | None: ...

    async def executions(self, target: ResolvedTarget, *, limit: int) -> tuple[ExecutionRef, ...]:
        """Harness executions of a run (or of every run of a mission), oldest first."""
        ...

    async def chain(self, chain_id: UUID) -> ChainState:
        """The chain under this scope; `StreamTargetNotFound` when absent or foreign."""
        ...

    async def frame_retention(
        self, harness_execution_id: UUID, generation: int, *, after_ordinal: int
    ) -> FrameRetentionProbe: ...

    async def frame_high_watermark(self, harness_execution_id: UUID, generation: int) -> int:
        """The newest persisted arrival ordinal of the generation (0 when none)."""
        ...

    async def frames(
        self,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int,
        upto_ordinal: int,
        limit: int,
    ) -> tuple[ProviderFrame, ...]: ...


@dataclass(frozen=True)
class StreamHint:
    """A wake-up: something committed for this scope and mission or execution."""

    request_scope: str
    mission_id: UUID | None = None
    harness_execution_id: UUID | None = None


class StreamHintSource(Protocol):
    async def run(self, deliver: Callable[[StreamHint], None], stop: asyncio.Event) -> None:
        """Call `deliver` for each received hint until `stop` is set."""
        ...


class StreamHintPublisher(Protocol):
    async def publish(self, hint: StreamHint) -> None: ...


__all__ = [
    "ChainState",
    "ExecutionRef",
    "FrameRetentionProbe",
    "LinkedMission",
    "ResolvedTarget",
    "StreamHint",
    "StreamHintPublisher",
    "StreamHintSource",
    "StreamSource",
    "StreamTargetNotFound",
]
