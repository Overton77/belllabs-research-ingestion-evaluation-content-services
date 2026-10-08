"""Deterministic transcript fixtures: a run's mission events and provider frames (SPEC-03 C3)."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any
from uuid import UUID

from tests.fixtures.provider_frames import (
    FIXTURE_ACTIVATION,
    FIXTURE_RUN,
    FRAME_NOW,
    provider_frame,
)

from mission_control.application.frames.transcript import MissionEventRecord
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, native_event_ref
from mission_control.domain.policies.contracts import ActorContext

RUN_KEY = "run-transcript-1"
READER = ActorContext(
    actor_id="reader",
    authority_refs=frozenset(),
    permissions=frozenset({"workflow_run.read"}),
)
SECRET_VALUE = "render-only-secret-4242"


def _event(
    seq: int, event_type: str, seconds: float, payload: dict[str, Any]
) -> MissionEventRecord:
    return MissionEventRecord(
        seq=seq,
        event_id=f"00000000-0000-7000-8000-{seq:012d}",
        event_type=event_type,
        recorded_at=FRAME_NOW + timedelta(seconds=seconds),
        actor_ref="mission-control",
        payload={"event_type": event_type, "payload": payload},
    )


def _execution(frame: Any) -> dict[str, Any]:
    return {
        "activation_id": str(FIXTURE_ACTIVATION),
        "attempt_no": 1,
        "harness_execution_id": str(frame.harness_execution_id),
        "generation": 1,
        "native_session_ref": "thread-1",
        "native_turn_ref": "turn-1",
        "arrival_ordinal": frame.arrival_ordinal,
    }


def transcript_frames() -> list[Any]:
    """Frames observed at FRAME_NOW + ordinal seconds (provider_frame's clock)."""

    k = FrameKind
    return [
        provider_frame(1, k.SESSION_INIT, {"thread_id": "thread-1"}),
        provider_frame(2, k.TURN_STARTED, {"invocation_id": "turn-1"}),
        provider_frame(3, k.MESSAGE_DELTA, {"text": f"Searching with key {SECRET_VALUE}"}),
        provider_frame(
            4,
            k.TOOL_CALL_STARTED,
            {"tool_call_id": "call-1", "name": "pubmed_search", "args": {"q": "rapamycin"}},
            tool_call_ref="call-1",
        ),
        provider_frame(
            5,
            k.TOOL_CALL_COMPLETED,
            {
                "tool_call_id": "call-1",
                "name": "pubmed_search",
                "status": "success",
                "content": "12 results",
            },
            tool_call_ref="call-1",
        ),
        provider_frame(6, k.USAGE, {"input_tokens": 8, "output_tokens": 3, "total_tokens": 11}),
        provider_frame(
            7,
            k.TURN_ENDED,
            {"outcome": "succeeded", "stop_reason": "end_turn"},
        ),
        provider_frame(8, k.RUN_RESULT, {"status": "finished"}, subordinate_ref=None),
    ]


def transcript_events(
    frames: list[Any], *, include_expired: bool = False
) -> list[MissionEventRecord]:
    by_ordinal = {frame.arrival_ordinal: frame for frame in frames}
    tool = by_ordinal[5]
    ended = by_ordinal[7]
    events = [
        _event(
            1, "workflow_run.admit", 0.0, {"command_id": "admit-1", "resulting_phase": "pending"}
        ),
        _event(
            2,
            "workflow_run.start",
            0.5,
            {"command_id": "start-1", "prior_phase": "pending", "resulting_phase": "active"},
        ),
        _event(
            3,
            "session.turn_started",
            1.5,
            {
                "execution": _execution(by_ordinal[2]),
                "source": {
                    "kind": "adapter",
                    "native_event_ref": native_event_ref(by_ordinal[2].frame_id),
                },
                "turn_ordinal": 1,
                "node_key": "collect",
            },
        ),
        _event(
            4,
            "tool_call.completed",
            5.5,
            {
                "execution": _execution(tool),
                "source": {"kind": "adapter", "native_event_ref": native_event_ref(tool.frame_id)},
                "tool_call_ref": "call-1",
                "name": "pubmed_search",
                "status": "completed",
                "result_digest": tool.body_digest,
            },
        ),
        _event(
            5,
            "artifact.registered",
            7.2,
            {
                "artifact_ref": "mc://artifacts/biotech/run-transcript-1/sources.json",
                "content_digest": "sha256:" + "a" * 64,
                "media_type": "application/json",
            },
        ),
        _event(
            6,
            "human_task.resolved",
            7.4,
            {"human_task_id": "task-1", "resolution": "approved"},
        ),
        _event(
            7,
            "session.turn_completed",
            7.5,
            {
                "execution": _execution(ended),
                "source": {"kind": "adapter", "native_event_ref": native_event_ref(ended.frame_id)},
                "turn_ordinal": 1,
                "usage": {
                    "schema_version": "mc.usage_report.v1",
                    "dimensions": {
                        "input_tokens": {
                            "value": 8,
                            "disposition": "settled",
                            "source_frame_id": None,
                        },
                        "cached_input_tokens": {
                            "value": None,
                            "disposition": "unknown",
                            "source_frame_id": None,
                        },
                        "output_tokens": {
                            "value": 3,
                            "disposition": "settled",
                            "source_frame_id": None,
                        },
                        "reasoning_tokens": {
                            "value": None,
                            "disposition": "unknown",
                            "source_frame_id": None,
                        },
                        "cost_micros": {
                            "value": None,
                            "disposition": "unknown",
                            "source_frame_id": None,
                        },
                    },
                },
                "stop_reason": "end_turn",
            },
        ),
    ]
    if include_expired:
        events.append(
            _event(
                8,
                "tool_call.completed",
                9.0,
                {
                    "execution": _execution(tool),
                    "source": {
                        "kind": "adapter",
                        "native_event_ref": native_event_ref(UUID(int=77)),
                    },
                    "tool_call_ref": "call-0",
                    "status": "completed",
                    "result_digest": "sha256:" + "e" * 64,
                },
            )
        )
    return events


class InMemoryMissionEvents:
    def __init__(self, events: list[MissionEventRecord], run_uuid: UUID = FIXTURE_RUN) -> None:
        self._events = events
        self._run_uuid = run_uuid

    async def run_identity(self, request_scope: str, run_key: str) -> UUID | None:
        del request_scope
        return self._run_uuid if run_key == RUN_KEY else None

    async def events_for_run(
        self, request_scope: str, run_key: str, *, limit: int = 20_000
    ) -> tuple[MissionEventRecord, ...]:
        del request_scope
        return tuple(self._events[:limit]) if run_key == RUN_KEY else ()


class StaticFrames:
    """A FrameReader over a fixed frame list (offline transcript tests)."""

    def __init__(self, frames: list[Any]) -> None:
        self._frames = frames

    async def frames_for_run(
        self, request_scope: str, run_id: UUID, *, closing_only: bool = False, limit: int = 10_000
    ) -> tuple[Any, ...]:
        del request_scope
        return tuple(
            frame
            for frame in self._frames
            if frame.run_id == run_id and (frame.closing or not closing_only)
        )[:limit]

    async def frames_for_execution(self, *args: Any, **kwargs: Any) -> tuple[Any, ...]:
        del args, kwargs
        return ()

    async def current_generation(
        self, request_scope: str, harness_execution_id: UUID
    ) -> int | None:
        del request_scope, harness_execution_id
        return 1


_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_CURSOR = re.compile(r"tc1:\d{12}:\d{17}:\d{12}:[0-9a-z]{8}")
_STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})")
_CLOCK = re.compile(r"\b\d{2}:\d{2}:\d{2}\b")


def normalize(text: str) -> str:
    """Golden comparison modulo ids, digests, cursors and timestamps."""

    for pattern, token in (
        (_CURSOR, "<cursor>"),
        (_UUID, "<uuid>"),
        (_DIGEST, "<digest>"),
        (_STAMP, "<time>"),
        (_CLOCK, "<hh:mm:ss>"),
    ):
        text = pattern.sub(token, text)
    return text


LANE = LaneProfile.DEEP_AGENTS
