"""The FrameSink port: one lane-neutral persistence path for provider frames (SPEC-03).

Deep Agents, Cursor Local, Cursor Cloud and hook invocations all append through this
port. A lane opens its harness execution once per attempt (`open_execution`), stamps
every observation with the returned handle and a monotonic arrival ordinal, and appends
in arrival order before any derivation. `append` is idempotent on
`(harness_execution_id, generation, provider_key)`; a frame of a generation older than
the execution's current one is fenced (`stale`) and never stored.

`InMemoryFrameStore` implements the full contract for unit tests and offline harnesses;
the production implementation is `adapters/postgres/frames/repository.py`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid5

from mission_control.domain.frames.contracts import (
    AppendReceipt,
    FrameKind,
    FrameScope,
    HarnessExecutionHandle,
    HarnessExecutionStart,
    ProviderCursor,
    ProviderFrame,
)

_HARNESS_NAMESPACE = UUID("4f1c8f8e-1d2b-4b0c-9e43-7b6f2c0d5a11")


def harness_execution_id(
    *, request_scope: str, run_key: str, activation_key: str, attempt_no: int, lane: str
) -> UUID:
    """Deterministic harness execution identity: one per attempt and lane profile."""

    return uuid5(
        _HARNESS_NAMESPACE,
        "|".join((request_scope, run_key, activation_key, str(attempt_no), lane)),
    )


class FrameOrdinalConflict(RuntimeError):
    """Another writer claimed an arrival ordinal; the writer reseeds from `last_cursor`."""


class HarnessExecutionNotFound(LookupError):
    """The run, activation or attempt named by a harness execution start does not exist."""


@runtime_checkable
class FrameSink(Protocol):
    async def append(self, frames: Sequence[ProviderFrame]) -> AppendReceipt: ...

    async def last_cursor(
        self, harness_execution_id: UUID, generation: int, *, request_scope: str
    ) -> ProviderCursor | None: ...


@runtime_checkable
class HarnessExecutionRegistry(Protocol):
    async def open_execution(self, start: HarnessExecutionStart) -> HarnessExecutionHandle: ...


class FrameStore(FrameSink, HarnessExecutionRegistry, Protocol):
    """What a lane writer needs: open its execution, then append frames."""


class FrameReader(Protocol):
    """Read side used by fact derivation and transcript materialization."""

    async def frames_for_execution(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int = 0,
        closing_only: bool = False,
        limit: int = 1_000,
    ) -> tuple[ProviderFrame, ...]: ...

    async def frames_for_run(
        self,
        request_scope: str,
        run_id: UUID,
        *,
        closing_only: bool = False,
        limit: int = 10_000,
    ) -> tuple[ProviderFrame, ...]: ...

    async def current_generation(
        self, request_scope: str, harness_execution_id: UUID
    ) -> int | None: ...


@dataclass
class _Execution:
    handle: HarnessExecutionHandle
    generation: int
    frames: dict[tuple[int, str], ProviderFrame] = field(default_factory=dict)
    ordinals: set[tuple[int, int]] = field(default_factory=set)
    sessions: dict[str, dict[str, object]] = field(default_factory=dict)
    turns: list[dict[str, object]] = field(default_factory=list)
    open_turn: dict[str, object] | None = None


@dataclass(frozen=True)
class InMemoryRunIdentity:
    scope: FrameScope
    run_id: UUID
    activations: dict[str, UUID]


class InMemoryFrameStore:
    """A complete in-process FrameStore + FrameReader with the Postgres semantics."""

    def __init__(self) -> None:
        self._runs: dict[tuple[str, str], InMemoryRunIdentity] = {}
        self._executions: dict[UUID, _Execution] = {}

    def register_run(
        self, request_scope: str, run_key: str, run_id: UUID, activations: dict[str, UUID]
    ) -> None:
        self._runs[(request_scope, run_key)] = InMemoryRunIdentity(
            FrameScope.from_request_scope(request_scope), run_id, dict(activations)
        )

    async def open_execution(self, start: HarnessExecutionStart) -> HarnessExecutionHandle:
        run = self._runs.get((start.request_scope, start.run_key))
        if run is None or start.activation_key not in run.activations:
            raise HarnessExecutionNotFound("run or activation is not recorded")
        existing = self._executions.get(start.harness_execution_id)
        generation = max(start.generation, existing.generation if existing else 0)
        handle = HarnessExecutionHandle(
            harness_execution_id=start.harness_execution_id,
            scope=run.scope,
            run_id=run.run_id,
            activation_id=run.activations[start.activation_key],
            attempt_no=start.attempt_no,
            generation=start.generation,
            lane_profile=start.lane_profile,
            native_session_ref=start.native_session_ref,
        )
        if existing is None:
            self._executions[start.harness_execution_id] = _Execution(handle, generation)
        else:
            existing.generation = generation
        cursor = await self.last_cursor(
            start.harness_execution_id, start.generation, request_scope=start.request_scope
        )
        return handle.model_copy(update={"last_cursor": cursor})

    async def append(self, frames: Sequence[ProviderFrame]) -> AppendReceipt:
        new = duplicate = stale = 0
        new_ids: list[UUID] = []
        for frame in frames:
            execution = self._executions.get(frame.harness_execution_id)
            if execution is None:
                raise HarnessExecutionNotFound("frame names an unopened harness execution")
            if frame.scope != execution.handle.scope:
                raise PermissionError("frame scope differs from its harness execution")
            if frame.generation < execution.generation:
                stale += 1
                continue
            execution.generation = max(execution.generation, frame.generation)
            key = (frame.generation, frame.provider_key)
            if key in execution.frames:
                duplicate += 1
                continue
            if (frame.generation, frame.arrival_ordinal) in execution.ordinals:
                raise FrameOrdinalConflict("arrival ordinal already used")
            execution.frames[key] = frame
            execution.ordinals.add((frame.generation, frame.arrival_ordinal))
            new += 1
            new_ids.append(frame.frame_id)
            self._identity(execution, frame)
        return AppendReceipt(
            new=new, duplicate=duplicate, stale=stale, new_frame_ids=tuple(new_ids)
        )

    def _identity(self, execution: _Execution, frame: ProviderFrame) -> None:
        session = frame.native_session_ref
        if frame.kind == FrameKind.SESSION_INIT or session not in execution.sessions:
            execution.sessions.setdefault(
                session, {"state": "open", "started_frame": frame.frame_id}
            )
        if frame.kind == FrameKind.TURN_STARTED:
            execution.open_turn = {
                "native_turn_ref": frame.native_turn_ref,
                "started_frame_id": frame.frame_id,
            }
        elif frame.kind == FrameKind.TURN_ENDED:
            opened = execution.open_turn or {}
            execution.turns.append(
                {
                    "turn_no": len(execution.turns) + 1,
                    "native_turn_ref": frame.native_turn_ref,
                    "started_frame_id": opened.get("started_frame_id"),
                    "ended_frame_id": frame.frame_id,
                    "generation": frame.generation,
                }
            )
            execution.open_turn = None
        elif frame.kind == FrameKind.RUN_RESULT:
            execution.sessions[session]["state"] = "closed"

    async def last_cursor(
        self, harness_execution_id: UUID, generation: int, *, request_scope: str
    ) -> ProviderCursor | None:
        execution = self._executions.get(harness_execution_id)
        if execution is None or execution.handle.scope.request_scope != request_scope:
            return None
        frames = [frame for (gen, _key), frame in execution.frames.items() if gen == generation]
        if not frames:
            return None
        last = max(frames, key=lambda frame: frame.arrival_ordinal)
        return ProviderCursor(
            harness_execution_id=harness_execution_id,
            generation=generation,
            arrival_ordinal=last.arrival_ordinal,
            provider_key=last.provider_key,
        )

    async def frames_for_execution(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int = 0,
        closing_only: bool = False,
        limit: int = 1_000,
    ) -> tuple[ProviderFrame, ...]:
        execution = self._executions.get(harness_execution_id)
        if execution is None or execution.handle.scope.request_scope != request_scope:
            return ()
        frames = sorted(
            (
                frame
                for (gen, _key), frame in execution.frames.items()
                if gen == generation
                and frame.arrival_ordinal > after_ordinal
                and (frame.closing or not closing_only)
            ),
            key=lambda frame: frame.arrival_ordinal,
        )
        return tuple(frames[:limit])

    async def frames_for_run(
        self,
        request_scope: str,
        run_id: UUID,
        *,
        closing_only: bool = False,
        limit: int = 10_000,
    ) -> tuple[ProviderFrame, ...]:
        frames = [
            frame
            for execution in self._executions.values()
            if execution.handle.run_id == run_id
            and execution.handle.scope.request_scope == request_scope
            for frame in execution.frames.values()
            if frame.closing or not closing_only
        ]
        frames.sort(key=lambda frame: (frame.observed_at, frame.arrival_ordinal))
        return tuple(frames[:limit])

    async def current_generation(
        self, request_scope: str, harness_execution_id: UUID
    ) -> int | None:
        execution = self._executions.get(harness_execution_id)
        if execution is None or execution.handle.scope.request_scope != request_scope:
            return None
        return execution.generation

    # Test introspection of the identity records the Postgres writers produce.
    def turns(self, harness_execution_id: UUID) -> list[dict[str, object]]:
        return list(self._executions[harness_execution_id].turns)

    def sessions(self, harness_execution_id: UUID) -> dict[str, dict[str, object]]:
        return dict(self._executions[harness_execution_id].sessions)
