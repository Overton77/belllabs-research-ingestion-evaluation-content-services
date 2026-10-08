"""FrameWriter + FrameSink semantics on the in-memory store (SPEC-03 write path)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from mission_control.application.frames.sink import (
    FrameOrdinalConflict,
    FrameSink,
    HarnessExecutionNotFound,
    InMemoryFrameStore,
)
from mission_control.application.frames.writer import FrameWriter, FullBodyPolicy
from mission_control.domain.frames.contracts import (
    AppendReceipt,
    FrameKind,
    HarnessExecutionHandle,
    ProviderCursor,
    ProviderFrame,
)
from tests.fixtures.provider_frames import (
    SCOPE,
    StepClock,
    harness_start,
    in_memory_store,
    observation,
    opened_writer,
    turn_observations,
)


@pytest.mark.asyncio
async def test_append_is_idempotent_and_ordered_and_identity_records_follow() -> None:
    store, run_id = in_memory_store()
    writer, handle = await opened_writer(store)
    receipt = await writer.write(turn_observations())
    assert (receipt.new, receipt.duplicate, receipt.stale) == (9, 0, 0)
    frames = await store.frames_for_execution(SCOPE, handle.harness_execution_id, 1)
    assert [frame.arrival_ordinal for frame in frames] == list(range(1, 10))
    assert all(
        frame.run_id == run_id and frame.native_session_ref == "thread-1" for frame in frames
    )
    assert [frame.kind for frame in frames if frame.closing] == [
        FrameKind.USAGE,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.USAGE,
        FrameKind.TURN_ENDED,
        FrameKind.RUN_RESULT,
    ]
    # Re-running the writer over the same stream adds zero rows.
    replay = await writer.write(turn_observations())
    assert (replay.new, replay.duplicate) == (0, 9)
    assert len(await store.frames_for_run(SCOPE, run_id)) == 9
    turns = store.turns(handle.harness_execution_id)
    assert len(turns) == 1 and turns[0]["started_frame_id"] == frames[1].frame_id
    assert turns[0]["ended_frame_id"] == frames[7].frame_id
    assert store.sessions(handle.harness_execution_id)["thread-1"]["state"] == "closed"


@pytest.mark.asyncio
async def test_resumed_writer_continues_the_arrival_sequence() -> None:
    store, _run_id = in_memory_store()
    first, handle = await opened_writer(store)
    await first.write(turn_observations()[:4])
    reopened = await store.open_execution(harness_start())
    assert reopened.last_cursor is not None and reopened.last_cursor.arrival_ordinal == 4
    resumed = FrameWriter(store, reopened, clock=StepClock())
    receipt = await resumed.write(turn_observations())
    assert (receipt.new, receipt.duplicate) == (5, 4)
    frames = await store.frames_for_execution(SCOPE, handle.harness_execution_id, 1)
    assert [frame.arrival_ordinal for frame in frames] == [1, 2, 3, 4, 9, 10, 11, 12, 13]
    assert [frame.provider_key for frame in frames] == [
        item.provider_key for item in turn_observations()
    ]


@pytest.mark.asyncio
async def test_frames_of_an_older_generation_are_stale_and_never_stored() -> None:
    store, _run_id = in_memory_store()
    old, _handle = await opened_writer(store, harness_start(generation=1))
    newer = await store.open_execution(harness_start(generation=2))
    current = FrameWriter(store, newer, clock=StepClock())
    assert (await current.write(turn_observations()[:2])).new == 2
    stale = await old.write([observation("late", FrameKind.TOOL_CALL_COMPLETED)])
    assert (stale.new, stale.duplicate, stale.stale) == (0, 0, 1)
    assert await store.current_generation(SCOPE, newer.harness_execution_id) == 2
    assert await store.frames_for_execution(SCOPE, newer.harness_execution_id, 1) == ()


@pytest.mark.asyncio
async def test_unknown_run_or_activation_fails_closed() -> None:
    store, _run_id = in_memory_store()
    with pytest.raises(HarnessExecutionNotFound):
        await store.open_execution(harness_start(activation_key="missing"))


class _CollidingSink:
    """Rejects the first append with an ordinal collision, as a racing writer would."""

    def __init__(self, inner: InMemoryFrameStore, taken: int) -> None:
        self._inner = inner
        self._collide = True
        self._taken = taken

    async def append(self, frames: Sequence[ProviderFrame]) -> AppendReceipt:
        if self._collide:
            self._collide = False
            raise FrameOrdinalConflict("taken")
        return await self._inner.append(frames)

    async def last_cursor(
        self, harness_execution_id: Any, generation: int, *, request_scope: str
    ) -> ProviderCursor | None:
        return ProviderCursor(
            harness_execution_id=harness_execution_id,
            generation=generation,
            arrival_ordinal=self._taken,
            provider_key="someone-else",
        )


@pytest.mark.asyncio
async def test_ordinal_collision_reseeds_from_the_store_cursor() -> None:
    store, _run_id = in_memory_store()
    handle = await store.open_execution(harness_start())
    sink: FrameSink = _CollidingSink(store, taken=7)
    writer = FrameWriter(sink, handle, clock=StepClock())
    await writer.write([observation("k1", FrameKind.MESSAGE)])
    frames = await store.frames_for_execution(SCOPE, handle.harness_execution_id, 1)
    assert [frame.arrival_ordinal for frame in frames] == [8]


class _Promoter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def promote_frame_body(
        self,
        handle: HarnessExecutionHandle,
        *,
        promotion_key: str,
        kind: FrameKind,
        digest: str,
        media_type: str,
        payload: bytes,
    ) -> str:
        self.calls.append(
            {"key": promotion_key, "kind": kind, "digest": digest, "size": len(payload)}
        )
        return f"artifact://fixture/{digest}"


@pytest.mark.asyncio
async def test_oversized_closing_bodies_are_promoted_when_the_lane_flag_allows() -> None:
    store, _run_id = in_memory_store()
    promoter = _Promoter()
    writer, handle = await opened_writer(store, excerpt_cap_bytes=256, promoter=promoter)
    big = {"content": "x" * 5_000, "status": "success"}
    await writer.write(
        [
            observation("tool", FrameKind.TOOL_CALL_COMPLETED, big),
            observation("delta", FrameKind.MESSAGE_DELTA, big),
            observation("small", FrameKind.TOOL_CALL_COMPLETED, {"status": "success"}),
            observation("message", FrameKind.MESSAGE, big),
        ]
    )
    frames = {
        frame.provider_key: frame
        for frame in await store.frames_for_execution(SCOPE, handle.harness_execution_id, 1)
    }
    assert [call["key"].rsplit(":", 1)[-1] for call in promoter.calls] == ["tool"]
    assert frames["tool"].body_artifact_ref == f"artifact://fixture/{frames['tool'].body_digest}"
    assert len(frames["tool"].body_excerpt.encode()) <= 256 and frames["tool"].body_bytes > 5_000
    assert frames["delta"].body_artifact_ref is None
    assert frames["message"].body_artifact_ref is None  # flag off for messages by default
    all_kinds = FullBodyPolicy(kinds=frozenset({FrameKind.MESSAGE, FrameKind.MESSAGE_DELTA}))
    assert all_kinds.eligible(FrameKind.MESSAGE, 5_000, 256)
    assert not all_kinds.eligible(FrameKind.MESSAGE_DELTA, 5_000, 256), "deltas never promote"
    assert not FullBodyPolicy().eligible(FrameKind.TOOL_CALL_COMPLETED, 3 * 1024 * 1024, 256)
