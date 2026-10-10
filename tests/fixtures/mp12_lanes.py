"""MP-12 fixtures: continuation-aware FIXTURE lanes, a fake hydrator and a crash hook.

FIXTURES ONLY. `ContinuingLane` and `CompactingFixtureLane` are scripted `cursor_local`-shaped
fakes over `ScriptedSessionLane`: they expose a context occupancy and an explicit compaction
the way a qualified provider lane would, and leave declared outputs missing when a test
wants "work continues". `FakeHydrator` provisions a fresh fake session per hydration.
`Crashes` raises once after a chosen phase was persisted (a worker lost right there).
Nothing here proves any live provider's behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tests.fixtures.lane_turns import ScriptedSessionLane

from mission_control.application.context.continuation import (
    ContinuationTransfer,
    HydrationReceipt,
    HydrationRequest,
)
from mission_control.application.context.lane_support import (
    CompactionReceipt,
    CompactionRequest,
)
from mission_control.domain.context.phases import ContinuationPhase
from mission_control.domain.context.pressure import ContextOccupancy
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import LaneFrame, SessionHandle, TurnHandle

SOURCE_SESSION = "agent-fake-1"


@dataclass
class ContinuingLane(ScriptedSessionLane):
    """FIXTURE `cursor_local` lane whose turns leave declared outputs missing (work
    continues), optionally exposing a context occupancy and staging turn text."""

    missing: tuple[str, ...] = ("/outputs/report.md",)
    occupancy: ContextOccupancy | None = None
    occupancy_after: ContextOccupancy | None = None
    compactions: list[CompactionRequest] = field(default_factory=list)
    compact_fails: bool = False
    staged_text: dict[str, str] = field(default_factory=dict)
    session_refs: list[str] = field(default_factory=list)

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        facts = super().closing_facts(turn, frame)
        return facts.model_copy(update={"missing_outputs": self.missing})

    async def context_occupancy(
        self, harness_execution_id: str, session: SessionHandle, turn: TurnHandle | None
    ) -> ContextOccupancy | None:
        return self.occupancy

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None:
        self.staged_text[instruction_ref] = text


class CompactingFixtureLane(ContinuingLane):
    """FIXTURE: the explicit compaction operation a qualified lane (Codex) would expose."""

    async def compact(self, request: CompactionRequest) -> CompactionReceipt:
        self.compactions.append(request)
        if self.compact_fails:
            raise RuntimeError("compaction endpoint refused")
        return CompactionReceipt(
            completed=True,
            native_ref=f"compaction-{request.epoch}",
            summary_digest="sha256:" + "1" * 64,
            occupancy_after=self.occupancy_after,
        )


@dataclass
class FakeHydrator:
    """FIXTURE hydrator: a fresh session id per hydration; `corrupt` changes one digest."""

    corrupt: bool = False
    fail_first: bool = False
    requests: list[HydrationRequest] = field(default_factory=list)

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        self.requests.append(request)
        if self.fail_first and len(self.requests) == 1:
            raise ConnectionError("the hydration receipt was lost")
        restored = dict(request.snapshot.manifest)
        if self.corrupt:
            key = next(iter(sorted(restored)), None)
            if key is not None:
                restored[key] = "sha256:" + "f" * 64
        return HydrationReceipt(
            target_session_ref=f"agent-fake-{1 + len(self.requests)}", restored=restored
        )


@dataclass
class Crashes:
    """FIXTURE: raise once after a given phase is persisted (a worker lost right there)."""

    at: set[ContinuationPhase] = field(default_factory=set)
    seen: list[ContinuationPhase] = field(default_factory=list)

    async def __call__(self, phase: ContinuationPhase, transfer: ContinuationTransfer) -> None:
        self.seen.append(phase)
        if phase in self.at:
            self.at.discard(phase)
            raise RuntimeError(f"worker lost after {phase.value} was persisted")


__all__ = [
    "SOURCE_SESSION",
    "CompactingFixtureLane",
    "ContinuingLane",
    "Crashes",
    "FakeHydrator",
]
