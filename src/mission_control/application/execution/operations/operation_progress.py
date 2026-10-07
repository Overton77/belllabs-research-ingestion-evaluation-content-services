"""Compact, safe progress of one cognitive Activity attempt (RRM-008, REQ-CP-EXEC-008 step 3).

The `operation.execute` Activity heartbeats this progress so that a Temporal cancel reaches
running cognition and so that a lost worker is detected by the heartbeat timeout instead of
the full start-to-close timeout. The payload carries only identities and keys: the unit key,
execution generation, Activity attempt, the latest durable root checkpoint key the attempt
has seen, and the phase. It never carries prompts, transcripts, outputs or secrets.

The progress object travels through a context variable: the Activity sets it, the operation
boundary reports phases, and the Deep Agent adapter registers a reader of the latest durable
checkpoint so the heartbeat loop can observe it without the adapter knowing about Temporal.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Literal

CognitionPhase = Literal[
    "admitting",
    "classifying",
    "dispatching",
    "cognition",
    "cancelling",
    "settling",
    "settled",
]

CheckpointReader = Callable[[], Awaitable[dict[str, str | None] | None]]


@dataclass
class CognitionProgress:
    unit_key: str | None
    execution_generation: int
    activity_attempt: int
    phase: CognitionPhase = "admitting"
    latest_checkpoint: dict[str, str | None] | None = None
    heartbeats: int = 0
    cancel_observed: bool = False
    # Registered by the runtime adapter while cognition runs; read by the heartbeat loop.
    checkpoint_reader: CheckpointReader | None = field(default=None, repr=False)

    async def observe(self) -> dict[str, object]:
        """The next heartbeat payload, refreshing the latest durable checkpoint if readable."""

        reader = self.checkpoint_reader
        if reader is not None:
            try:
                latest = await reader()
            except Exception:
                latest = None
            if latest is not None:
                self.latest_checkpoint = latest
        self.heartbeats += 1
        return self.payload()

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": "belllabs.cognition-heartbeat.v1",
            "unit_key": self.unit_key,
            "execution_generation": self.execution_generation,
            "activity_attempt": self.activity_attempt,
            "phase": self.phase,
            "latest_checkpoint": self.latest_checkpoint,
            "heartbeat": self.heartbeats,
        }


CURRENT_PROGRESS: ContextVar[CognitionProgress | None] = ContextVar(
    "belllabs_cognition_progress", default=None
)


def current_progress() -> CognitionProgress | None:
    return CURRENT_PROGRESS.get()


# RRM-008 review F1: the Activity runner reports *why* the attempt's task was cancelled. A
# unit settles `cancelled` only for a requested cancellation; every other cause of a Temporal
# cancellation (worker shutdown, heartbeat or start-to-close timeout, pause, reset, not found)
# re-raises so the holder stands down and the next attempt recovers the unit
# (REQ-CP-EXEC-011). Outside an Activity no probe is registered: nothing is a cancel.
CancellationProbe = Callable[[], bool]
CURRENT_CANCEL_PROBE: ContextVar[CancellationProbe | None] = ContextVar(
    "belllabs_cancel_probe", default=None
)


def cancel_requested() -> bool:
    probe = CURRENT_CANCEL_PROBE.get()
    return probe is not None and bool(probe())


def report_phase(phase: CognitionPhase) -> None:
    progress = CURRENT_PROGRESS.get()
    if progress is not None:
        progress.phase = phase


def register_checkpoint_reader(reader: CheckpointReader | None) -> None:
    progress = CURRENT_PROGRESS.get()
    if progress is not None:
        progress.checkpoint_reader = reader


__all__ = [
    "CURRENT_CANCEL_PROBE",
    "CURRENT_PROGRESS",
    "CancellationProbe",
    "CheckpointReader",
    "CognitionPhase",
    "CognitionProgress",
    "cancel_requested",
    "current_progress",
    "register_checkpoint_reader",
    "report_phase",
]
