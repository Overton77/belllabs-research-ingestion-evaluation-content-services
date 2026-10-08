"""Shared provider-frame fixtures (SPEC-03): canonical scopes, observations and DA harnesses."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from mission_control.application.frames.sink import InMemoryFrameStore, harness_execution_id
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.contracts import (
    FrameKind,
    FrameObservation,
    HarnessExecutionHandle,
    HarnessExecutionStart,
    LaneProfile,
)

INSTALLATION = UUID("0192a4f0-0000-7000-8000-00000000b10e")
TENANT = UUID("1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a01")
OTHER_TENANT = UUID("1b0e1c4e-6d0f-5c6b-9a4e-0d6d6f1c2a02")
SCOPE = f"mc/{INSTALLATION}/biotech/{TENANT}"
OTHER_SCOPE = f"mc/{INSTALLATION}/biotech/{OTHER_TENANT}"
FRAME_NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
BINDING_DIGEST = "sha256:" + "b" * 64


class StepClock:
    """A deterministic clock advancing one second per reading."""

    def __init__(self, start: datetime = FRAME_NOW) -> None:
        self._current = start

    def __call__(self) -> datetime:
        value = self._current
        self._current = value + timedelta(seconds=1)
        return value


def harness_start(
    *,
    request_scope: str = SCOPE,
    run_key: str = "run-1",
    activation_key: str = "unit-1",
    attempt_no: int = 1,
    generation: int = 1,
    lane: LaneProfile = LaneProfile.DEEP_AGENTS,
    native_session_ref: str = "thread-1",
) -> HarnessExecutionStart:
    return HarnessExecutionStart(
        harness_execution_id=harness_execution_id(
            request_scope=request_scope,
            run_key=run_key,
            activation_key=activation_key,
            attempt_no=attempt_no,
            lane=lane.value,
        ),
        request_scope=request_scope,
        run_key=run_key,
        activation_key=activation_key,
        attempt_no=attempt_no,
        generation=generation,
        lane_profile=lane,
        native_session_ref=native_session_ref,
        runtime_kind="fixture",
        provider_kind="fixture",
        placement_kind="local_in_worker",
        intended_binding_digest=BINDING_DIGEST,
    )


def in_memory_store(
    *,
    request_scope: str = SCOPE,
    run_key: str = "run-1",
    activation_keys: tuple[str, ...] = ("unit-1",),
) -> tuple[InMemoryFrameStore, UUID]:
    store = InMemoryFrameStore()
    run_id = uuid5(INSTALLATION, f"run:{request_scope}:{run_key}")
    store.register_run(
        request_scope,
        run_key,
        run_id,
        {key: uuid5(run_id, f"activation:{key}") for key in activation_keys},
    )
    return store, run_id


async def opened_writer(
    store: Any, start: HarnessExecutionStart | None = None, **kwargs: Any
) -> tuple[FrameWriter, HarnessExecutionHandle]:
    handle = await store.open_execution(start or harness_start())
    return FrameWriter(store, handle, clock=StepClock(), **kwargs), handle


def observation(
    key: str,
    kind: FrameKind,
    body: Any = None,
    *,
    raw_kind: str | None = None,
    turn: str | None = "turn-1",
    tool_call_ref: str | None = None,
    subordinate_ref: str | None = None,
) -> FrameObservation:
    return FrameObservation(
        provider_key=key,
        raw_kind=raw_kind or f"fixture.{kind.value}",
        kind=kind,
        body=body if body is not None else {"key": key},
        native_turn_ref=turn,
        tool_call_ref=tool_call_ref,
        subordinate_ref=subordinate_ref,
    )


def turn_observations(turn: str = "turn-1", *, tool_call: str = "call-1") -> list[FrameObservation]:
    """A complete Deep-Agents-shaped turn: init, start, deltas, tool call, usage, end, result."""

    return [
        observation(
            f"{turn}:session", FrameKind.SESSION_INIT, {"thread_id": "thread-1"}, turn=turn
        ),
        observation(f"{turn}:start", FrameKind.TURN_STARTED, {"invocation_id": turn}, turn=turn),
        observation(f"{turn}:delta:0", FrameKind.MESSAGE_DELTA, {"text": "Look"}, turn=turn),
        observation(
            f"{turn}:{tool_call}:started",
            FrameKind.TOOL_CALL_STARTED,
            {"tool_call_id": tool_call, "name": "ls", "args": {"path": "/"}},
            turn=turn,
            tool_call_ref=tool_call,
        ),
        observation(
            f"{turn}:usage:1",
            FrameKind.USAGE,
            {"input_tokens": 3, "output_tokens": 1, "total_tokens": 4},
            turn=turn,
        ),
        observation(
            f"{turn}:{tool_call}:completed",
            FrameKind.TOOL_CALL_COMPLETED,
            {"tool_call_id": tool_call, "name": "ls", "status": "success", "content": "a.txt"},
            turn=turn,
            tool_call_ref=tool_call,
        ),
        observation(
            f"{turn}:usage:2",
            FrameKind.USAGE,
            {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
            turn=turn,
        ),
        observation(
            f"{turn}:end",
            FrameKind.TURN_ENDED,
            {"invocation_id": turn, "outcome": "succeeded", "stop_reason": "end_turn"},
            turn=turn,
        ),
        observation(f"{turn}:result", FrameKind.RUN_RESULT, {"status": "finished"}, turn=turn),
    ]


def fresh_run_key() -> str:
    return f"run-{uuid4()}"


K = FrameKind
FIXTURE_HARNESS = UUID("7d4f0c1e-1111-4a2b-9c3d-000000000001")
FIXTURE_RUN = UUID("7d4f0c1e-1111-4a2b-9c3d-000000000002")
FIXTURE_ACTIVATION = UUID("7d4f0c1e-1111-4a2b-9c3d-000000000003")


def provider_frame(
    ordinal: int,
    kind: FrameKind,
    body: Any = None,
    *,
    raw_kind: str | None = None,
    lane: LaneProfile = LaneProfile.DEEP_AGENTS,
    generation: int = 1,
    session: str = "thread-1",
    turn: str | None = "turn-1",
    tool_call_ref: str | None = None,
    subordinate_ref: str | None = None,
    harness: UUID = FIXTURE_HARNESS,
) -> Any:
    """A persisted-shape ProviderFrame built through the real body handling."""

    from mission_control.domain.frames.body import frame_body
    from mission_control.domain.frames.contracts import FrameScope, ProviderFrame, is_closing

    payload = frame_body(body if body is not None else {"ordinal": ordinal})
    return ProviderFrame(
        frame_id=uuid5(harness, f"{generation}:{ordinal}"),
        scope=FrameScope.from_request_scope(SCOPE),
        run_id=FIXTURE_RUN,
        activation_id=FIXTURE_ACTIVATION,
        attempt_no=1,
        harness_execution_id=harness,
        generation=generation,
        lane_profile=lane,
        native_session_ref=session,
        native_turn_ref=turn,
        provider_key=f"{generation}:{ordinal}:{kind.value}",
        arrival_ordinal=ordinal,
        observed_at=FRAME_NOW + timedelta(seconds=ordinal),
        kind=kind,
        closing=is_closing(kind),
        subordinate_ref=subordinate_ref,
        tool_call_ref=tool_call_ref,
        body_digest=payload.digest,
        body_bytes=payload.body_bytes,
        body_media_type="application/json",
        body_excerpt=payload.excerpt,
        redactions=payload.redactions,
        raw_kind=raw_kind or f"fixture.{kind.value}",
    )


def deep_agents_turn_frames(generation: int = 1) -> list[Any]:
    """The frame sequence a Deep Agents invocation persists (C1 writer shape)."""

    return [
        provider_frame(1, K.SESSION_INIT, {"thread_id": "thread-1"}, generation=generation),
        provider_frame(2, K.TURN_STARTED, {"invocation_id": "turn-1"}, generation=generation),
        provider_frame(3, K.MESSAGE_DELTA, {"text": "Looking"}, generation=generation),
        provider_frame(4, K.MESSAGE, {"type": "ai", "id": "m1"}, generation=generation),
        provider_frame(
            5,
            K.TOOL_CALL_STARTED,
            {"tool_call_id": "call-1", "name": "read_file"},
            tool_call_ref="call-1",
            generation=generation,
        ),
        provider_frame(
            6,
            K.USAGE,
            {"input_tokens": 3, "output_tokens": 1, "total_tokens": 4},
            generation=generation,
        ),
        provider_frame(
            7,
            K.TOOL_CALL_COMPLETED,
            {
                "tool_call_id": "call-1",
                "name": "read_file",
                "status": "success",
                "result_digest": "sha256:" + "1" * 64,
            },
            tool_call_ref="call-1",
            generation=generation,
        ),
        provider_frame(8, K.MESSAGE, {"type": "ai", "id": "m2"}, generation=generation),
        provider_frame(
            9,
            K.USAGE,
            {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
            generation=generation,
        ),
        provider_frame(
            10,
            K.TURN_ENDED,
            {"outcome": "succeeded", "stop_reason": "end_turn", "input_tokens": 8},
            generation=generation,
        ),
        provider_frame(11, K.RUN_RESULT, {"status": "finished"}, generation=generation),
    ]
