"""interrupt_and_inject at the lane boundary (FT-F2; SPEC-06 "Interrupt and inject", SPEC-07
section 4.3 and 7; ADR-0032, ADR-0031).

An admitted `interrupt_and_inject` Command waits in the Run's mailbox. The lane boundary of
the turn that is running when it arrives takes it at once and acts by the lane's declared
semantics (`describe().delivery_semantics["interrupt_and_inject"]`):

- `cooperative_inject` (a lane that steers natively; no first-wave lane does): the content is
  handed to the running turn, which continues.
- `cancel_and_replace`: the running turn is cancelled (activity cancel, then the lane's
  idempotent cancel), its uncertain effects are settled (an effect claim whose outcome is
  unknown must settle within a grace period, otherwise the unit parks `in_doubt` with an
  incident and no replacement runs), then a replacement turn runs on the same session with
  the injected item sealed into a `follow_up_turn` Context Packet.
- `unsupported`: the Command is rejected with a typed Delivery Report; the turn continues.

`InterruptAndInjectService.run_turn` drives the bounded execute path of a lane (Deep Agents:
the operation boundary's `execute`). `cancel_and_replace_turn` is the same sequence over the
AgentHarness protocol (`cancel_turn`, `observe`, `send_turn`) for session lanes, which FT-G4's
Cursor lanes call from the `lane.turn` segment loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.execution.harness.protocol import AgentHarness, OperationLane
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    RuntimeInvocation,
    RuntimeResult,
)
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    LaneDescribe,
    LaneFrame,
    ObserveRequest,
    SendTurnRequest,
    TurnHandle,
)
from mission_control.domain.policies.mailbox import MailboxEntry, MailboxFamily, MailboxState

INJECT_KIND = "interrupt_and_inject"
Clock = Callable[[], datetime]
UnsettledEffects = Callable[[], Awaitable[tuple[str, ...]]]


@dataclass(frozen=True)
class InjectionSettings:
    """Watch and settlement bounds; measured latency is evidence, never a promise (ADR-0008)."""

    poll_seconds: float = 2.0
    settle_grace_seconds: float = 30.0
    settle_poll_seconds: float = 0.5


class InjectionParked(Exception):
    """The interrupted turn left effects whose outcome is unknown: park `in_doubt`."""

    def __init__(self, pending_effect_ids: tuple[str, ...]) -> None:
        super().__init__(f"uncertain effects did not settle: {', '.join(pending_effect_ids)}")
        self.pending_effect_ids = pending_effect_ids


@runtime_checkable
class CooperativeInjectLane(Protocol):
    """A lane that steers a running turn natively (`cooperative_inject`)."""

    async def inject(self, invocation: RuntimeInvocation, content: PromptSegment) -> None: ...


@dataclass
class TurnRecord:
    """What the turns of one operation attempt did with injected Commands."""

    delivery_keys: list[str] = field(default_factory=list)
    turns: int = 1


def interrupt_semantics(describe: LaneDescribe) -> str:
    return describe.delivery_semantics.get(INJECT_KIND, "unsupported")


def unit_boundary(request: OperationExecutionRequest) -> tuple[MailboxFamily, str] | None:
    """The mailbox boundary (family, node) of the unit an operation attempt runs."""

    unit = request.runtime_unit
    if unit is None:
        return None
    location = unit.location
    if unit.family == "stage_graph":
        return "StageGraph", str(getattr(location, "stage_id", unit.semantic_operation_id))
    return "GoalDirected", f"goal/{getattr(location, 'operation_role', 'executor')}"


class InterruptAndInjectService:
    def __init__(
        self,
        mailbox: MailboxDeliveryService,
        *,
        packs: ContextPackService | None = None,
        settings: InjectionSettings | None = None,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._mailbox = mailbox
        self._packs = packs
        self._settings = settings or InjectionSettings()
        self._clock = clock

    @property
    def settings(self) -> InjectionSettings:
        return self._settings

    async def pending(
        self, request: OperationExecutionRequest, node: tuple[MailboxFamily, str]
    ) -> MailboxEntry | None:
        """The first queued `interrupt_and_inject` entry this unit's turn would take."""

        family, node_key = node
        for entry in await self._mailbox.list_entries(
            request.request_scope, request.identity.run_id
        ):
            if entry.kind != INJECT_KIND or entry.state != MailboxState.QUEUED:
                continue
            if entry.node_key is not None and entry.node_key != node_key:
                continue
            if entry.node_key is None and family == "GoalDirected" and node_key != "goal/executor":
                continue
            return entry
        return None

    async def run_turn(
        self,
        *,
        request: OperationExecutionRequest,
        lane: OperationLane,
        invocation: RuntimeInvocation,
        secrets: Mapping[str, str],
        unsettled: UnsettledEffects,
        record: TurnRecord,
    ) -> RuntimeResult:
        """Run the attempt's turn, interrupting and replacing it for every injected Command.

        Raises `InjectionParked` when an interrupted turn's effects did not settle.
        """

        node = unit_boundary(request)
        describe = lane.describe()
        semantics = interrupt_semantics(describe)
        current = invocation
        task = asyncio.create_task(lane.execute(current, secrets))
        try:
            while True:
                entry = await self._watch(task, request, node)
                if entry is None:
                    return task.result()
                if semantics == "unsupported" or node is None:
                    await self._mailbox.inject_unsupported(
                        request.request_scope, entry, lane_profile=describe.lane_profile
                    )
                    continue  # the running turn is never interrupted
                key = f"{request.idempotency_key}:inject:{entry.command_id}"
                cooperative = semantics == "cooperative_inject" and isinstance(
                    lane, CooperativeInjectLane
                )
                delivered = await self._mailbox.deliver(
                    request.request_scope,
                    request.identity.run_id,
                    delivery_key=key,
                    family=node[0],
                    node_key=node[1],
                    iteration_start=False,
                    lane_profile=describe.lane_profile,
                    kinds=(INJECT_KIND,),
                    cancelled_turn_ref=None if cooperative else f"turn:{record.turns}",
                )
                if not delivered:
                    continue  # another boundary took it first; keep watching this turn
                record.delivery_keys.append(key)
                follow_up = await self.follow_up_segment(request, delivered, key)
                if cooperative:
                    assert isinstance(lane, CooperativeInjectLane)
                    await lane.inject(current, follow_up)
                    await self._mailbox.inject_replaced(
                        request.request_scope,
                        request.identity.run_id,
                        delivery_key=key,
                        lane_profile=describe.lane_profile,
                        delivered_semantics="cooperative_inject",
                        cancelled_turn_ref=None,
                        replacement_turn_ref=f"turn:{record.turns}",
                    )
                    continue
                # cancel_and_replace: interrupt, settle, then the replacement turn.
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                cancelled_turn = f"turn:{record.turns}"
                uncertain = await unsettled()
                pending = await self._settle(unsettled)
                if pending:
                    await self._mailbox.inject_parked(
                        request.request_scope,
                        request.identity.run_id,
                        delivery_key=key,
                        lane_profile=describe.lane_profile,
                        cancelled_turn_ref=cancelled_turn,
                        pending_effect_ids=pending,
                    )
                    record.delivery_keys.remove(key)
                    raise InjectionParked(pending)
                record.turns += 1
                await self._mailbox.inject_replaced(
                    request.request_scope,
                    request.identity.run_id,
                    delivery_key=key,
                    lane_profile=describe.lane_profile,
                    delivered_semantics="cancel_and_replace",
                    cancelled_turn_ref=cancelled_turn,
                    replacement_turn_ref=f"turn:{record.turns}",
                    settled_effect_ids=uncertain,
                )
                current = current.model_copy(update={"follow_up": follow_up})
                task = asyncio.create_task(lane.execute(current, secrets))
        except asyncio.CancelledError:
            # The activity itself was cancelled (Temporal cancel, lease deadline): stop the
            # turn too and let the operation boundary settle exactly as before.
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def _watch(
        self,
        task: asyncio.Task[RuntimeResult],
        request: OperationExecutionRequest,
        node: tuple[MailboxFamily, str] | None,
    ) -> MailboxEntry | None:
        while True:
            done, _pending = await asyncio.wait({task}, timeout=self._settings.poll_seconds)
            if done:
                return None
            if node is None:
                continue
            entry = await self.pending(request, node)
            if entry is not None:
                return entry

    async def _settle(self, unsettled: UnsettledEffects) -> tuple[str, ...]:
        """Wait (bounded) for the interrupted turn's uncertain effects to settle."""

        deadline = asyncio.get_running_loop().time() + self._settings.settle_grace_seconds
        pending = await unsettled()
        while pending and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(self._settings.settle_poll_seconds)
            pending = await unsettled()
        return pending

    async def follow_up_segment(
        self,
        request: OperationExecutionRequest,
        entries: tuple[MailboxEntry, ...],
        delivery_key: str,
    ) -> PromptSegment:
        """The replacement turn's injected item: a sealed `follow_up_turn` Context Packet
        when the packer is composed, else the bounded admitted-input text itself."""

        if self._packs is not None:
            sealed = await self._packs.pack_follow_up(
                request,
                await self._packs.queued_candidates(entries, request_scope=request.request_scope),
                delivery_key=delivery_key,
                sealed_at=self._clock(),
            )
            return sealed.prompt_segment
        parts = []
        for entry in entries:
            body = (
                entry.content_inline
                if entry.content_inline is not None
                else f"(artifact {entry.content_ref}, {entry.content_digest})"
            )
            parts.append(f"Operator instruction (interrupt_and_inject {entry.command_id}):\n{body}")
        content = "\n\n".join(parts)
        return PromptSegment(
            source_ref=f"mailbox-inject:{delivery_key}",
            source_revision=1,
            trust_class=PromptTrustClass.ADMITTED_INPUT,
            content=content,
            rendered_digest=sha256_digest(content),
        )


# --- Session lanes (AgentHarness protocol): the path FT-G4 drives for Cursor ---------------


@dataclass(frozen=True)
class ReplacedTurn:
    handle: TurnHandle
    cancelled_turn_ref: str | None
    settled_effect_ids: tuple[str, ...]


async def cancel_and_replace_turn(
    lane: AgentHarness,
    *,
    cancel: CancelTurnRequest,
    replacement: SendTurnRequest,
    unsettled: UnsettledEffects,
    settings: InjectionSettings | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    after: str | None = None,
    on_frame: Callable[[LaneFrame], Awaitable[None]] | None = None,
    cancel_first: bool = True,
) -> ReplacedTurn:
    """`cancel_and_replace` over the harness protocol: cancel the running turn (idempotent),
    observe it to its terminal frame, settle uncertain effects (raise `InjectionParked` when
    they do not settle within the grace), then send the replacement turn whose
    `instruction_ref` names the injected mailbox item.

    FT-G4: `after` resumes the drain from the last persisted cursor and `on_frame` persists
    every drained frame (a tool call that completes during the cancel settles its effect);
    `cancel_first=False` replaces a turn already found terminal (a retried replacement).
    """

    settings = settings or InjectionSettings()
    if cancel_first:
        await lane.cancel_turn(cancel)
    observe = ObserveRequest(
        **cancel.model_dump(exclude={"turn", "reason", "urgency"}),
        turn=cancel.turn,
        after=after,
    )
    if cancel_first:
        async for frame in lane.observe(observe):
            if on_frame is not None:
                await on_frame(frame)
            if frame.terminal:
                break
    settled_before = await unsettled()
    waited = 0.0
    pending = settled_before
    while pending and waited < settings.settle_grace_seconds:
        await sleep(settings.settle_poll_seconds)
        waited += settings.settle_poll_seconds
        pending = await unsettled()
    if pending:
        raise InjectionParked(pending)
    handle = await lane.send_turn(replacement)
    return ReplacedTurn(
        handle=handle,
        cancelled_turn_ref=cancel.turn.native_turn_ref,
        settled_effect_ids=settled_before,
    )


__all__ = [
    "INJECT_KIND",
    "CooperativeInjectLane",
    "InjectionParked",
    "InjectionSettings",
    "InterruptAndInjectService",
    "ReplacedTurn",
    "TurnRecord",
    "cancel_and_replace_turn",
    "interrupt_semantics",
    "unit_boundary",
]
