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
