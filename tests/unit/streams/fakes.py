"""In-memory `StreamSource` FIXTURE for unit tests (not PostgreSQL; the PG proofs live in
`tests/integration/postgres/test_mission_socket_postgres.py`)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid5

from mission_control.application.streams.ports import (
    ResolvedTarget,
    StreamTargetNotFound,
)
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame
from mission_control.domain.subscriptions.contracts import MissionEventEnvelope
from mission_control.domain.subscriptions.streams import StreamTarget
from tests.fixtures.provider_frames import FIXTURE_HARNESS, FIXTURE_RUN, SCOPE, provider_frame

MISSION = UUID("5a1b2c3d-0000-4000-8000-0000000000a1")
RUN_KEY = "run-fixture"
T0 = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
DIGEST = "sha256:" + "0" * 64


def mission_event(seq: int, event_type: str = "workflow_run.progress", **extra: Any) -> Any:
    return MissionEventEnvelope(
        event_id=uuid5(MISSION, f"event:{seq}"),
        application_id="biotech",
        mission_id=MISSION,
        run_id=extra.pop("run_id", FIXTURE_RUN),
        seq=seq,
        event_type=event_type,
        event_version=1,
        actor_ref="operator",
        happened_at=T0 + timedelta(seconds=seq),
        recorded_at=T0 + timedelta(seconds=seq),
        payload_ref=f"mc://applications/biotech/missions/{MISSION}/events/{seq}",
        payload_digest=DIGEST,
        **extra,
    )


@dataclass
class FakeStreamSource:
    request_scope: str = SCOPE
    events: list[MissionEventEnvelope] = field(default_factory=list)
    stored_frames: dict[int, list[ProviderFrame]] = field(default_factory=dict)
    generation: int = 1
    reads: int = 0
    fail_reads: int = 0

    def commit_events(self, count: int, event_type: str = "workflow_run.progress") -> None:
        start = len(self.events) + 1
        for seq in range(start, start + count):
            self.events.append(mission_event(seq, event_type))

    def commit_frames(self, *kinds: FrameKind, generation: int | None = None, **kw: Any) -> None:
        generation = generation or self.generation
        stored = self.stored_frames.setdefault(generation, [])
        ordinal = max(
            (f.arrival_ordinal for g in self.stored_frames.values() for f in g), default=0
        )
        for kind in kinds:
            ordinal += 1
            stored.append(provider_frame(ordinal, kind, generation=generation, **kw))

    def _read(self) -> None:
        self.reads += 1
        if self.fail_reads:
            self.fail_reads -= 1
            raise ConnectionError("fixture source down")

    async def resolve(self, target: StreamTarget) -> ResolvedTarget:
        if self.request_scope != SCOPE:
            raise StreamTargetNotFound(target.id)
        if target.kind == "mission" and target.id == str(MISSION):
            return ResolvedTarget(target, MISSION)
        if target.kind == "run" and target.id == RUN_KEY:
            return ResolvedTarget(target, MISSION, FIXTURE_RUN, RUN_KEY)
        if target.kind == "execution" and target.id == str(FIXTURE_HARNESS):
            return ResolvedTarget(target, MISSION, FIXTURE_RUN, RUN_KEY, FIXTURE_HARNESS)
        raise StreamTargetNotFound(target.id)

    async def mission_high_watermark(self, mission_id: UUID) -> int:
        self._read()
        return len(self.events)

    async def mission_low_watermark(self, mission_id: UUID) -> int | None:
        return 1 if self.events else None

    async def mission_events(
        self, target: ResolvedTarget, *, after_seq: int, upto_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]:
        self._read()
        selected = [e for e in self.events if after_seq < e.seq <= upto_seq]
        if target.run_id is not None:
            selected = [e for e in selected if e.run_id == target.run_id]
        return tuple(selected[:limit])

    async def frame_generation(self, harness_execution_id: UUID) -> int | None:
        return self.generation if harness_execution_id == FIXTURE_HARNESS else None

    async def frame_high_watermark(self, harness_execution_id: UUID, generation: int) -> int:
        return max((f.arrival_ordinal for f in self.stored_frames.get(generation, [])), default=0)

    async def frames(
        self,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int,
        upto_ordinal: int,
        limit: int,
    ) -> tuple[ProviderFrame, ...]:
        self._read()
        stored = self.stored_frames.get(generation, [])
        selected = [f for f in stored if after_ordinal < f.arrival_ordinal <= upto_ordinal]
        return tuple(selected[:limit])
