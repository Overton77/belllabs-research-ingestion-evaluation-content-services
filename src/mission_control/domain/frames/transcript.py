"""`mc.transcript_entry.v1`: one line of a run's materialized Transcript (SPEC-03, C3).

The Transcript is a read view, never state: it merges the run's canonical mission events
(by `seq`) with provider frames (by arrival order, placed under their governing event)
into entries with an opaque cursor that increases strictly within a run.

Cursor: ``tc1:<seq>:<observed_us>:<ordinal>:<tag>`` (fixed-width, so it also sorts as a
string). A mission event is ``(seq, 0, 0, "00000000")``; a frame is ``(seq of its
governing event, effective observed time in microseconds, arrival ordinal, first eight
hex digits of its harness execution)``; a `frame_expired` placeholder sits right after the
event that cites the expired frame. The governing mission `seq` comes first, so the
transcript and the canonical event stream advance together.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from pydantic import AwareDatetime, Field

from mission_control.domain.frames.contracts import FrameContract
from mission_control.domain.frames.usage import UsageReport

TRANSCRIPT_ENTRY_SCHEMA = "mc.transcript_entry.v1"
CURSOR_PATTERN = re.compile(r"^tc1:(\d{12}):(\d{17}):(\d{12}):([0-9a-z]{8})$")
EVENT_TAG = "00000000"

TranscriptSource = Literal["mission_event", "provider_frame", "artifact", "human", "command"]
TranscriptRole = Literal["system", "agent", "tool", "human", "mission_control"]


class CursorExpired(ValueError):
    """`CURSOR_EXPIRED`: the cursor is malformed or names no position in this run."""

    code = "CURSOR_EXPIRED"


@dataclass(frozen=True, order=True)
class TranscriptCursor:
    seq: int
    observed_us: int = 0
    ordinal: int = 0
    tag: str = EVENT_TAG

    def encode(self) -> str:
        return f"tc1:{self.seq:012d}:{self.observed_us:017d}:{self.ordinal:012d}:{self.tag}"

    @classmethod
    def decode(cls, value: str) -> TranscriptCursor:
        match = CURSOR_PATTERN.fullmatch(value or "")
        if match is None:
            raise CursorExpired("transcript cursor is malformed")
        seq, observed, ordinal, tag = match.groups()
        return cls(int(seq), int(observed), int(ordinal), tag)


class TranscriptRefs(FrameContract):
    event_id: str | None = None
    frame_id: str | None = None
    native_event_ref: str | None = None
    artifact_ref: str | None = None
    tool_call_ref: str | None = None
    command_id: str | None = None
    human_task_id: str | None = None
    body_digest: str | None = None


class TranscriptEntry(FrameContract):
    """`TranscriptEntry@1`."""

    schema_version: Literal["mc.transcript_entry.v1"] = "mc.transcript_entry.v1"
    cursor: str = Field(pattern=CURSOR_PATTERN.pattern)
    run_id: str = Field(min_length=1)
    activation_id: str | None = None
    attempt_no: int | None = None
    node_key: str | None = None
    iteration_id: str | None = None
    subordinate_ref: str | None = None
    at: AwareDatetime
    source: TranscriptSource
    kind: str = Field(min_length=1)
    role: TranscriptRole | None = None
    title: str = Field(min_length=1, max_length=512)
    body_excerpt: str | None = None
    refs: TranscriptRefs = Field(default_factory=TranscriptRefs)
    usage: UsageReport | None = None
    canonical: bool

    @property
    def position(self) -> TranscriptCursor:
        return TranscriptCursor.decode(self.cursor)


class TranscriptQuery(FrameContract):
    """Filters shared by the CLI, HTTP and MCP surfaces."""

    since: str | None = None
    activation: str | None = None
    node: str | None = None
    kinds: frozenset[str] = frozenset()
    canonical_only: bool = False
    subordinate: str | None = None
    limit: int = Field(default=500, ge=1, le=5_000)


class TranscriptPage(FrameContract):
    run_id: str
    entries: tuple[TranscriptEntry, ...]
    next_cursor: str | None = None
    has_more: bool = False


def transcript_json_schema() -> dict[str, object]:
    return TranscriptEntry.model_json_schema()
