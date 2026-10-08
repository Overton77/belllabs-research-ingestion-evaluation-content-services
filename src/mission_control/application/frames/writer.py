"""Lane-neutral frame writer: identity, arrival ordinal and body handling (SPEC-03).

A lane writer turns provider events into `FrameObservation`s (provider key, raw kind,
classified kind, JSON body, native refs). `FrameWriter` stamps each with the harness
execution handle, a monotonic `arrival_ordinal` (seeded from the store's last cursor so a
resumed activity continues the sequence), redacts, digests and excerpts the body under
the application's cap, optionally promotes an oversized closing body to a
`provider_frame_body` artifact, and appends in arrival order. It is shared by the Deep
Agents writer, the Cursor writers (G3/G5) and hook callbacks.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from mission_control.application.frames.sink import FrameOrdinalConflict, FrameSink
from mission_control.contracts.identities import uuid7
from mission_control.domain.frames.body import frame_body
from mission_control.domain.frames.contracts import (
    DEFAULT_EXCERPT_CAP_BYTES,
    FULL_BODY_KINDS,
    AppendReceipt,
    FrameKind,
    FrameObservation,
    HarnessExecutionHandle,
    ProviderFrame,
    is_closing,
)

MAX_ORDINAL_RETRIES = 4
FULL_BODY_MAX_BYTES = 2 * 1024 * 1024


class FrameBodyPromoter(Protocol):
    """Registers an oversized frame body as a `provider_frame_body` artifact."""

    async def promote_frame_body(
        self,
        handle: HarnessExecutionHandle,
        *,
        promotion_key: str,
        kind: FrameKind,
        digest: str,
        media_type: str,
        payload: bytes,
    ) -> str: ...


@dataclass(frozen=True)
class FullBodyPolicy:
    """Which oversized bodies become artifacts (lane profile `persist_full_bodies`)."""

    enabled: bool = True
    kinds: frozenset[FrameKind] = frozenset({FrameKind.TOOL_CALL_COMPLETED})
    max_bytes: int = FULL_BODY_MAX_BYTES

    def eligible(self, kind: FrameKind, body_bytes: int, cap: int) -> bool:
        # Deltas are never promoted; only tool results, whole messages and run results.
        return (
            self.enabled
            and kind in self.kinds
            and kind in FULL_BODY_KINDS
            and cap < body_bytes <= self.max_bytes
        )


DEFAULT_FULL_BODY_POLICY = FullBodyPolicy()


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FrameWriter:
    """Writes one harness execution's frames in arrival order through a FrameSink."""

    def __init__(
        self,
        sink: FrameSink,
        handle: HarnessExecutionHandle,
        *,
        excerpt_cap_bytes: int = DEFAULT_EXCERPT_CAP_BYTES,
        secret_values: Iterable[str] = (),
        clock: Callable[[], datetime] = _utc_now,
        promoter: FrameBodyPromoter | None = None,
        full_bodies: FullBodyPolicy = DEFAULT_FULL_BODY_POLICY,
    ) -> None:
        self._sink = sink
        self._handle = handle
        self._cap = excerpt_cap_bytes
        self._secrets = tuple(secret_values)
        self._clock = clock
        self._promoter = promoter
        self._full_bodies = full_bodies
        self._next = (handle.last_cursor.arrival_ordinal if handle.last_cursor else 0) + 1
        self.receipt = AppendReceipt(new=0, duplicate=0, stale=0)

    @property
    def handle(self) -> HarnessExecutionHandle:
        return self._handle

    async def write(self, observations: Sequence[FrameObservation]) -> AppendReceipt:
        if not observations:
            return AppendReceipt(new=0, duplicate=0, stale=0)
        prepared = [await self._prepare(item) for item in observations]
        for _attempt in range(MAX_ORDINAL_RETRIES):
            frames = [
                frame.model_copy(update={"arrival_ordinal": self._next + offset})
                for offset, frame in enumerate(prepared)
            ]
            try:
                receipt = await self._sink.append(frames)
            except FrameOrdinalConflict:
                cursor = await self._sink.last_cursor(
                    self._handle.harness_execution_id,
                    self._handle.generation,
                    request_scope=self._handle.request_scope,
                )
                self._next = (cursor.arrival_ordinal if cursor else 0) + 1
                continue
            self._next += len(frames)
            self.receipt = self.receipt + receipt
            return receipt
        raise FrameOrdinalConflict("arrival ordinals kept colliding with another writer")

    async def _prepare(self, observation: FrameObservation) -> ProviderFrame:
        body = frame_body(
            observation.body, excerpt_cap_bytes=self._cap, secret_values=self._secrets
        )
        artifact_ref: str | None = None
        if self._promoter is not None and self._full_bodies.eligible(
            observation.kind, body.body_bytes, self._cap
        ):
            artifact_ref = await self._promoter.promote_frame_body(
                self._handle,
                promotion_key=(
                    f"{self._handle.harness_execution_id}:{self._handle.generation}:"
                    f"{observation.provider_key}"
                ),
                kind=observation.kind,
                digest=body.digest,
                media_type=observation.body_media_type,
                payload=body.canonical,
            )
        handle = self._handle
        return ProviderFrame(
            frame_id=uuid7(),
            scope=handle.scope,
            run_id=handle.run_id,
            activation_id=handle.activation_id,
            attempt_no=handle.attempt_no,
            harness_execution_id=handle.harness_execution_id,
            generation=handle.generation,
            lane_profile=handle.lane_profile,
            native_session_ref=observation.native_session_ref or handle.native_session_ref,
            native_turn_ref=observation.native_turn_ref,
            provider_key=observation.provider_key,
            arrival_ordinal=self._next,
            observed_at=observation.observed_at or self._clock(),
            provider_timestamp=observation.provider_timestamp,
            kind=observation.kind,
            closing=is_closing(observation.kind),
            subordinate_ref=observation.subordinate_ref,
            tool_call_ref=observation.tool_call_ref,
            body_digest=body.digest,
            body_bytes=body.body_bytes,
            body_media_type=observation.body_media_type,
            body_excerpt=body.excerpt,
            body_artifact_ref=artifact_ref,
            redactions=body.redactions,
            raw_kind=observation.raw_kind,
        )
