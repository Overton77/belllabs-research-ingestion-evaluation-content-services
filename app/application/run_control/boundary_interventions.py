"""Governed boundary interventions: delivery, application and the operator facade (RRM-007).

`CON-CP-WORKFLOW-MESSAGE-V1` / REQ-CP-EXEC-006 and 007 (AMD-RRM-001):

* `BoundaryInterventionService` is the only public entry for pause, resume, wait release,
  cancel and `reconcile_unit`: it runs the command through run control (acceptance and the
  `accepted` receipt) and, when a transport is composed, delivers what is pending.
* `BoundaryCommandDeliveryService` delivers accepted commands to their exact target, in
  target-sequence order, through a transport whose Update return value is evidence of
  `delivered` and never of `applied`. Ordering stops at the first transport failure; a
  re-run (redelivery) is safe because the target de-duplicates and the receipt ledger
  never transitions twice.
* `BoundaryCommandApplicationService` serves the family boundaries' activities: it binds
  the current run version itself, so a family never fails on version drift caused by
  pending commands, and it records the `applied` (or `rejected`) receipt through run
  control atomically with the phase effect.

Nothing here is family-, company- or provider-specific.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from pydantic import TypeAdapter, ValidationError

from app.application.run_control.run_control_repository import pending_delivery
from app.application.run_control.service import RunControlService
from app.domain.run_control.boundary_commands import receipt
from app.domain.run_control.contracts import (
    BOUNDARY_FACT_KINDS,
    ActorContext,
    BoundaryCommandReceipt,
    BoundaryCommandStatus,
    CommandResult,
    CommandStatus,
    LifecycleAction,
    LifecycleCommand,
    ReceiptState,
    RunPhase,
)
from app.domain.run_control.errors import CommandRejected

logger = logging.getLogger(__name__)
LIFECYCLE_ACTION_ADAPTER: TypeAdapter[LifecycleAction] = TypeAdapter(LifecycleAction)
Clock = Callable[[], datetime]
DELIVERY_RECORDER = "boundary-delivery"


class BoundaryDeliveryAck(Protocol):
    """What a transport returns once the exact target acknowledged the command."""

    @property
    def status(self) -> str: ...

    @property
    def transport_ref(self) -> str: ...

    @property
    def detail(self) -> str: ...


class BoundaryCommandTransport(Protocol):
    """Delivers one accepted command to its exact target execution (root, then family).

    Returns an acknowledgement whose status is `delivered` or `duplicate` (both evidence of
    delivery), or `stale_generation` / `stale_target` (terminal rejections). Any transport
    failure raises; the caller keeps ordering by stopping at the first failure.
    """

    async def deliver(self, status: BoundaryCommandStatus) -> BoundaryDeliveryAck: ...


class BoundaryDeliveryResult:
    __slots__ = ("detail", "status", "transport_ref")

    def __init__(self, status: str, transport_ref: str, detail: str = "") -> None:
        self.status = status
        self.transport_ref = transport_ref
        self.detail = detail


class BoundaryCommandDeliveryService:
    def __init__(
        self,
        run_control: RunControlService,
        transport: BoundaryCommandTransport,
        *,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._run_control = run_control
        self._transport = transport
        self._clock = clock

    async def deliver_pending(
        self, request_scope: str, run_id: str
    ) -> tuple[BoundaryCommandStatus, ...]:
        """Deliver every accepted, undelivered command of the run in target-sequence order.

        Returns the statuses after delivery. The first transport failure ends the pass so
        that no later command overtakes an earlier one; the relay re-runs the pass.
        """

        delivered: list[BoundaryCommandStatus] = []
        run = await self._run_control.get_run(request_scope, run_id)
        if run.phase == RunPhase.TERMINAL:
            # F1: nothing is delivered to a terminal run; what run control did not close in
            # its terminalizing commit is closed here with the same terminal reason.
            return tuple(
                [
                    await self._run_control.record_boundary_receipt(
                        request_scope,
                        receipt(
                            status.command,
                            ordinal=1,
                            state=ReceiptState.REJECTED,
                            recorded_by=DELIVERY_RECORDER,
                            rejection_reason="terminal_run",
                            detail="the run is terminal; nothing is delivered",
                            recorded_at=self._clock(),
                        ),
                    )
                    for status in await self._run_control.list_boundary_commands(
                        request_scope, run_id
                    )
                    if pending_delivery(status)
                ]
            )
        pending = sorted(
            (
                status
                for status in await self._run_control.list_boundary_commands(
                    request_scope, run_id
                )
                if pending_delivery(status)
            ),
            key=lambda status: (
                status.command.target.sequence_space,
                status.command.target_sequence,
            ),
        )
        for status in pending:
            try:
                ack = await self._transport.deliver(status)
            except Exception:
                logger.exception(
                    "boundary command delivery failed; later commands wait for redelivery",
                    extra={"run_id": run_id, "command_id": status.command.command_id},
                )
                break
            delivered.append(await self._record(request_scope, status, ack))
        return tuple(delivered)

    async def _record(
        self, request_scope: str, status: BoundaryCommandStatus, ack: BoundaryDeliveryAck
    ) -> BoundaryCommandStatus:
        if ack.status in {"delivered", "duplicate"}:
            update = receipt(
                status.command,
                ordinal=1,
                state=ReceiptState.DELIVERED,
                recorded_by=DELIVERY_RECORDER,
                detail=ack.detail or ack.status,
                transport_ref=ack.transport_ref,
                recorded_at=self._clock(),
            )
        elif ack.status in {"stale_generation", "stale_target"}:
            update = receipt(
                status.command,
                ordinal=1,
                state=ReceiptState.REJECTED,
                recorded_by=DELIVERY_RECORDER,
                rejection_reason=ack.status,  # type: ignore[arg-type]
                detail=ack.detail or ack.status,
                transport_ref=ack.transport_ref,
                recorded_at=self._clock(),
            )
        else:
            raise ValueError(f"transport returned an unknown acknowledgement: {ack.status}")
        return await self._run_control.record_boundary_receipt(request_scope, update)


class BoundaryInterventionService:
    """The public facade for governed interventions: accept through run control, deliver."""

    def __init__(
        self,
        run_control: RunControlService,
        delivery: BoundaryCommandDeliveryService | None = None,
    ) -> None:
        self._run_control = run_control
        self._delivery = delivery

    async def execute(self, command: LifecycleCommand) -> CommandResult:
        result = await self._run_control.execute(command)
        if result.status == CommandStatus.ACCEPTED and self._delivery is not None:
            await self._delivery.deliver_pending(command.request_scope, command.run_id)
        return result

    async def redeliver(
        self, request_scope: str, run_id: str
    ) -> tuple[BoundaryCommandStatus, ...]:
        if self._delivery is None:
            return ()
        return await self._delivery.deliver_pending(request_scope, run_id)

    async def list_commands(
        self, request_scope: str, run_id: str
    ) -> tuple[BoundaryCommandStatus, ...]:
        return await self._run_control.list_boundary_commands(request_scope, run_id)


class BoundaryApplicationRejected(ValueError):
    """The boundary fact was refused for a reason that a retry cannot repair."""


class BoundaryFactStale(RuntimeError):
    """Authority kept moving under the fact; the activity retries (no result was stored)."""


class BoundaryCommandApplicationService:
    """Serves the family boundaries' lifecycle facts (`apply_boundary_command`, `set_wait`,
    `observe_quiescence`, a policy pause) with the boundary's own actor.

    The run version is bound at execution: the family's copy of the version is a hint
    (pending commands never move it, but operation settlements may). An exact replay of a
    fact returns its stored result (idempotent by command identity).
    """

    def __init__(
        self,
        run_control: RunControlService,
        actor: ActorContext,
        *,
        clock: Clock = lambda: datetime.now(UTC),
        attempts: int = 8,
    ) -> None:
        self._run_control = run_control
        self._actor = actor
        self._clock = clock
        self._attempts = attempts

    @property
    def run_control(self) -> RunControlService:
        return self._run_control

    async def execute(
        self,
        *,
        request_scope: str,
        run_id: str,
        command_id: str,
        idempotency_issuer: str,
        correlation_id: str,
        action: dict[str, object],
        reason: str,
        evidence_refs: tuple[str, ...] = (),
        occurred_at: datetime | None = None,
    ) -> CommandResult:
        try:
            typed = LIFECYCLE_ACTION_ADAPTER.validate_python(action)
        except ValidationError as error:
            raise BoundaryApplicationRejected(
                f"boundary fact is not a typed lifecycle action: {error}"
            ) from error
        if typed.kind not in BOUNDARY_FACT_KINDS and typed.kind != "pause":
            raise BoundaryApplicationRejected(
                f"{typed.kind} is not a family boundary fact"
            )
        when = occurred_at or self._clock()
        prior = await self._run_control.get_command_result(
            request_scope, run_id, idempotency_issuer, command_id
        )
        if prior is not None:
            # Boundary facts never store a STALE result (F2), so a stored result is final.
            return prior
        for _attempt in range(self._attempts):
            run = await self._run_control.get_run(request_scope, run_id)
            command = LifecycleCommand(
                command_id=command_id,
                idempotency_issuer=idempotency_issuer,
                request_scope=request_scope,
                run_id=run_id,
                expected_run_version=run.version,
                actor=self._actor,
                action=typed,
                reason=reason,
                evidence_refs=evidence_refs,
                occurred_at=when,
                correlation_id=correlation_id,
                causation_id=command_id,
            )
            try:
                # A `pause` here is the family's own policy pause: self-issued (N1).
                result = await self._run_control.execute(
                    command, self_issued=typed.kind == "pause"
                )
            except CommandRejected as error:
                raise BoundaryApplicationRejected(str(error)) from error
            if result.status != CommandStatus.STALE:
                return result
        raise BoundaryFactStale(
            f"boundary fact {command_id} remained stale after {self._attempts} attempts"
        )

    async def boundary_receipt_state(
        self, request_scope: str, run_id: str, idempotency_issuer: str, command_id: str
    ) -> tuple[str, int]:
        """The command's current receipt state and target sequence (`("", 0)` if unknown)."""

        status = await self._run_control.get_boundary_command(
            request_scope, run_id, idempotency_issuer, command_id
        )
        if status is None:
            return "", 0
        return status.state.value, status.command.target_sequence

    async def record_receipt(
        self, request_scope: str, update: BoundaryCommandReceipt
    ) -> BoundaryCommandStatus:
        return await self._run_control.record_boundary_receipt(request_scope, update)
