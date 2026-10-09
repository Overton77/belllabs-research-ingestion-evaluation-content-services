"""MP-06 fixtures: fake provider sessions for dispatch recovery and cancel races.

FIXTURES ONLY. Nothing here is a provider: `FakeProviderLedger` stands for the remote side
of a provider (what it accepted, by idempotency key), and the lanes below are scripted
`cursor_local`-shaped fakes built on `ScriptedSessionLane`. They prove Mission Control's
journal/ownership/fence logic, not any live provider's behaviour.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

from tests.fixtures.lane_turns import ScriptedSessionLane

from mission_control.application.execution.harness.dispatch import (
    DispatchLookup,
    DispatchRecord,
    ProviderCapacityLimited,
)
from mission_control.application.execution.usage_admission import ProviderLimitSignal
from mission_control.domain.execution.lanes import SendTurnRequest, SessionHandle, TurnHandle

LossMode = Literal["none", "raise", "hang"]


@dataclass
class FakeProviderLedger:
    """FIXTURE: the provider side. A send it accepted is real work, receipt or not."""

    accepted: dict[str, str] = field(default_factory=dict)

    def accept(self, idempotency_key: str, turn_ref: str) -> str:
        return self.accepted.setdefault(idempotency_key, turn_ref)


@dataclass
class LossyReceiptLane(ScriptedSessionLane):
    """FIXTURE: the provider accepts the send, then the local receipt is lost once.

    `raise`: the response never arrives (a reset connection after accept). `hang`: the
    worker process stops making progress after the provider accepted (the test kills it).
    """

    ledger: FakeProviderLedger = field(default_factory=FakeProviderLedger)
    loss: LossMode = "raise"
    accepted_event: asyncio.Event = field(default_factory=asyncio.Event)
    hang_release: asyncio.Event = field(default_factory=asyncio.Event)
    native_sends: list[str] = field(default_factory=list)

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        self.calls.append("send_turn")
        self.native_sends.append(request.idempotency_key)
        self.sends.append(request.idempotency_key)
        turn_ref = self.ledger.accept(request.idempotency_key, "run-fake-1")
        loss, self.loss = self.loss, "none"
        self.accepted_event.set()
        if loss == "raise":
            raise ConnectionError("connection reset after the provider accepted the turn")
        if loss == "hang":
            await self.hang_release.wait()
        return TurnHandle(
            session=request.session, turn_no=request.turn_no, native_turn_ref=turn_ref
        )


@dataclass
class ReconcilingLossyLane(LossyReceiptLane):
    """FIXTURE: a lane that can ask the provider by idempotency key (`DispatchReconcilingLane`).

    `authoritative=False` answers `unknown` for an absent key (the lane cannot be sure)."""

    authoritative: bool = True
    lookups: list[str] = field(default_factory=list)

    async def reconcile_dispatch(
        self, record: DispatchRecord, *, session: SessionHandle | None
    ) -> DispatchLookup:
        self.lookups.append(record.key)
        if record.kind == "create":
            return DispatchLookup(outcome="found", native_ref="agent-fake-1")
        found = self.ledger.accepted.get(record.idempotency_key)
        if found is not None:
            return DispatchLookup(outcome="found", native_ref=found)
        return DispatchLookup(outcome="not_received" if self.authoritative else "unknown")


@dataclass
class NeverAcceptedLane(ReconcilingLossyLane):
    """FIXTURE: the connection drops *before* the provider accepts (nothing was sent)."""

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        if self.loss == "raise":
            self.loss = "none"
            self.calls.append("send_turn")
            raise ConnectionError("connection refused before the provider accepted anything")
        return await super().send_turn(request)


@dataclass
class CapacityLimitedLane(ScriptedSessionLane):
    """FIXTURE: the first `limited` sends are refused by a provider rate limit (nothing
    accepted), with a reset `reset_in_s` seconds after the refusal."""

    limited: int = 1
    reset_in_s: float = 2.0
    refusals: int = 0

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        if self.limited > 0:
            self.limited -= 1
            self.refusals += 1
            raise ProviderCapacityLimited(
                ProviderLimitSignal(
                    lane_profile="cursor_local",
                    kind="rate_limited",
                    resets_at=datetime.now(UTC) + timedelta(seconds=self.reset_in_s),
                    source="fixture:rate_limit",
                    fixture=True,
                )
            )
        return await super().send_turn(request)


__all__ = [
    "CapacityLimitedLane",
    "FakeProviderLedger",
    "LossyReceiptLane",
    "NeverAcceptedLane",
    "ReconcilingLossyLane",
]
