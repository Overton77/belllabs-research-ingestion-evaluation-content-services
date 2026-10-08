"""Ports the Context Pack Service captures candidates through (SPEC-02, ADR-0027).

The packer in :mod:`mission_control.domain.context.packet` is pure. Everything that touches
storage or a tokenizer happens here, through these ports, before packing:

- :class:`ArtifactBytesPort` reads artifact metadata and (bounded) bytes for a durable ref.
- :class:`TokenCounterPort` resolves a deterministic :class:`TokenCounter` for a
  ``tokenizer_ref``; an unknown tokenizer yields the conservative bound.
- :class:`ContextSelectionRepository` persists a sealed packet with its
  ``mc.context_selection.v1`` record in the caller's transaction.

The ``pack_for_stage`` / ``pack_for_iteration`` / ``pack_for_chain_link`` entry points are
added by the tickets that wire them (FT-B2, FT-B3, FT-D2).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from mission_control.domain.context.packet import (
    ConservativeTokenCounter,
    ContextPacket,
    TokenCounter,
)
from mission_control.domain.context.render import ContextSelectionRecord


@dataclass(frozen=True, slots=True)
class ArtifactContent:
    """Captured view of one artifact: identity, digest, size and optionally its text."""

    source_ref: str
    durable_ref: str
    """Locator the workspace materializer fetches (``<object_ref>#<sha256>:<size>``)."""
    content_digest: str
    size_bytes: int
    media_type: str
    file_name: str | None = None
    text: str | None = None
    """Decoded text when the artifact is textual and within the capture limit."""
    summary: str | None = None


class ArtifactBytesPort(Protocol):
    async def capture(self, source_ref: str, *, max_text_bytes: int) -> ArtifactContent:
        """Return metadata for ``source_ref``; ``text`` only when textual and small enough."""
        ...


class TokenCounterPort(Protocol):
    def counter_for(self, tokenizer_ref: str) -> TokenCounter: ...


class ContextSelectionRepository(Protocol):
    async def record(self, packet: ContextPacket, selection: ContextSelectionRecord) -> None:
        """Persist the packet and its selection record; idempotent on ``packet_digest``."""
        ...

    async def get(self, packet_id: str) -> ContextPacket | None: ...


class StaticTokenCounters:
    """A :class:`TokenCounterPort` over a fixed mapping; unknown tokenizers get the bound."""

    def __init__(self, counters: Mapping[str, TokenCounter] | None = None) -> None:
        self._counters = dict(counters or {})
        self._fallback = ConservativeTokenCounter()

    def counter_for(self, tokenizer_ref: str) -> TokenCounter:
        return self._counters.get(tokenizer_ref, self._fallback)
