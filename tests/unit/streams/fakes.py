"""In-memory `StreamSource` FIXTURE for unit tests (not PostgreSQL; the PG proofs live in
`tests/integration/postgres/test_mission_socket_postgres.py`)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid5

from mission_control.application.streams.ports import (
    ChainState,
    ExecutionRef,
    FrameRetentionProbe,
    LinkedMission,
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
    """One mission (`MISSION`) with the fixture run and execution; `other_frames` adds more
    executions of the same run, `member_events` more member missions of a chain."""

    request_scope: str = SCOPE
    events: list[MissionEventEnvelope] = field(default_factory=list)
    stored_frames: dict[int, list[ProviderFrame]] = field(default_factory=dict)
    generation: int = 1
    reads: int = 0
    fail_reads: int = 0
    links: list[LinkedMission] = field(default_factory=list)
    other_frames: dict[UUID, list[ProviderFrame]] = field(default_factory=dict)
    member_events: dict[UUID, list[MissionEventEnvelope]] = field(default_factory=dict)
    chains: dict[UUID, ChainState] = field(default_factory=dict)
    retention_horizon_passed: bool = False

    def commit_events(self, count: int, event_type: str = "workflow_run.progress") -> None:
        start = len(self.events) + 1
        for seq in range(start, start + count):
            self.events.append(mission_event(seq, event_type))

    def commit_member_events(self, mission: UUID, count: int) -> None:
        journal = self.member_events.setdefault(mission, [])
        for seq in range(len(journal) + 1, len(journal) + count + 1):
            journal.append(
                mission_event(seq).model_copy(
                    update={"mission_id": mission, "event_id": uuid5(mission, f"event:{seq}")}
                )
            )

    def commit_frames(self, *kinds: FrameKind, generation: int | None = None, **kw: Any) -> None:
        generation = generation or self.generation
        stored = self.stored_frames.setdefault(generation, [])
        ordinal = max(
            (f.arrival_ordinal for g in self.stored_frames.values() for f in g), default=0
        )
        for kind in kinds:
            ordinal += 1
            stored.append(provider_frame(ordinal, kind, generation=generation, **kw))

    def commit_other_frames(self, execution: UUID, *kinds: FrameKind) -> None:
        stored = self.other_frames.setdefault(execution, [])
        ordinal = max((f.arrival_ordinal for f in stored), default=0)
        for kind in kinds:
            ordinal += 1
            stored.append(provider_frame(ordinal, kind, harness=execution))

    def expire_frames(self, *ordinals: int, generation: int = 1) -> None:
        """Retention deleted these frames in place (and the execution is old enough)."""

        self.stored_frames[generation] = [
            f for f in self.stored_frames.get(generation, []) if f.arrival_ordinal not in ordinals
        ]
        self.retention_horizon_passed = True

    def _read(self) -> None:
        self.reads += 1
        if self.fail_reads:
            self.fail_reads -= 1
            raise ConnectionError("fixture source down")

    def _journal(self, mission_id: UUID) -> list[MissionEventEnvelope]:
        return self.events if mission_id == MISSION else self.member_events.get(mission_id, [])

    def _stored(self, execution: UUID, generation: int) -> list[ProviderFrame]:
        if execution == FIXTURE_HARNESS:
            return self.stored_frames.get(generation, [])
        return self.other_frames.get(execution, []) if generation == 1 else []

    async def resolve(self, target: StreamTarget) -> ResolvedTarget:
        if self.request_scope != SCOPE:
            raise StreamTargetNotFound(target.id)
        if target.kind == "mission" and target.id == str(MISSION):
            return ResolvedTarget(target, MISSION)
        if target.kind == "run" and target.id == RUN_KEY:
            return ResolvedTarget(target, MISSION, FIXTURE_RUN, RUN_KEY)
        if target.kind == "execution" and target.id == str(FIXTURE_HARNESS):
            return ResolvedTarget(target, MISSION, FIXTURE_RUN, RUN_KEY, FIXTURE_HARNESS)
        if target.kind == "chain":
            for chain in self.chains.values():
                if target.id in {str(chain.chain_id), chain.chain_key}:
                    members = tuple(
                        sorted(
                            {link.from_mission_id for link in chain.links}
                            | {link.to_mission_id for link in chain.links}
                        )
                    )
                    return ResolvedTarget(
                        target, members[0], chain_id=chain.chain_id, members=members
                    )
        raise StreamTargetNotFound(target.id)

    async def chain(self, chain_id: UUID) -> ChainState:
        if chain_id not in self.chains:
            raise StreamTargetNotFound(str(chain_id))
        return self.chains[chain_id]

    async def executions(self, target: ResolvedTarget, *, limit: int) -> tuple[ExecutionRef, ...]:
        refs = [ExecutionRef(FIXTURE_HARNESS, self.generation)]
        refs.extend(ExecutionRef(execution, 1) for execution in self.other_frames)
        return tuple(refs[:limit])

    async def mission_high_watermark(self, mission_id: UUID) -> int:
        self._read()
        return len(self._journal(mission_id))

    async def mission_low_watermark(self, mission_id: UUID) -> int | None:
        return 1 if self._journal(mission_id) else None

    async def mission_events(
        self, target: ResolvedTarget, *, after_seq: int, upto_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]:
        self._read()
        journal = self._journal(target.mission_id)
        selected = [e for e in journal if after_seq < e.seq <= upto_seq]
        if target.run_id is not None:
            selected = [e for e in selected if e.run_id == target.run_id]
        return tuple(selected[:limit])

    async def linked_missions(self, mission_id: UUID) -> tuple[LinkedMission, ...]:
        self._read()
        return tuple(link for link in self.links if link.from_mission_id == mission_id)

    async def frame_generation(self, harness_execution_id: UUID) -> int | None:
        if harness_execution_id == FIXTURE_HARNESS:
            return self.generation
        return 1 if harness_execution_id in self.other_frames else None

    async def frame_high_watermark(self, harness_execution_id: UUID, generation: int) -> int:
        stored = self._stored(harness_execution_id, generation)
        return max((f.arrival_ordinal for f in stored), default=0)

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
        stored = self._stored(harness_execution_id, generation)
        selected = [f for f in stored if after_ordinal < f.arrival_ordinal <= upto_ordinal]
        return tuple(selected[:limit])

    async def frame_retention(
        self, harness_execution_id: UUID, generation: int, *, after_ordinal: int
    ) -> FrameRetentionProbe:
        ordinals = [f.arrival_ordinal for f in self._stored(harness_execution_id, generation)]
        later = [ordinal for ordinal in ordinals if ordinal > after_ordinal]
        return FrameRetentionProbe(
            cursor_present=after_ordinal in ordinals,
            next_ordinal=min(later) if later else None,
            horizon_passed=self.retention_horizon_passed,
        )
