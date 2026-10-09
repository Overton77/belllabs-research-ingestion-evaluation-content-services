"""Fenced session ownership and the native dispatch journal (SPEC-01 runtime; MP-06).

A Session Lane's native side effects are the *create* of a provider session and the *send* of
a turn. Both are journaled on the harness execution before they are issued and acknowledged
with the native identity after it, so a worker that dies between the provider accepting the
call and the local receipt never repeats it blindly:

- `intended` without an acknowledgement is an ambiguous dispatch. A retried segment first asks
  the lane (`DispatchReconcilingLane`) by idempotency key / native identity. `found` records
  the acknowledgement and the turn is observed, never re-sent; `not_received` is the
  provider's authoritative statement that nothing was accepted, and only then may the same
  idempotency key be sent once more; anything else parks the unit `in_doubt` and blocks a
  competing replacement until an operator reconciles it.
- `declined` is a send the provider refused without accepting work (`busy`): it may be sent
  again at the next boundary.

Every journal write and the settlement are fenced by the *session owner*: one worker-owned
session manager per harness execution, identified by `owner_ref` and an `epoch` that
increases on each takeover. A live lease held by another owner refuses the claim
(`SessionOwnedElsewhere`); a takeover after expiry fences the previous owner, whose later
journal writes and settlement raise `StaleSessionOwner`. Takeover does not undo an external
tool the old owner already started.

Pure contracts and transition rules: the stores (in-memory, PostgreSQL) apply them under
their own row lock.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.application.execution.usage_admission import ProviderLimitSignal
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.lanes import SessionHandle, TurnHandle

DispatchKind = Literal["create", "send"]
DispatchPhase = Literal["intended", "acknowledged", "declined", "not_received", "in_doubt"]
DispatchOutcome = Literal["acknowledged", "declined", "not_received", "in_doubt"]
LookupOutcome = Literal["found", "not_received", "unknown"]
# Phases from which the same idempotency key may be dispatched again: nothing was accepted.
_REDISPATCHABLE: frozenset[str] = frozenset({"declined", "not_received"})


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SessionOwner(_Contract):
    """The worker-owned session manager holding one harness execution generation."""

    owner_ref: str = Field(min_length=1, max_length=512)
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    lease_expires_at: AwareDatetime
    claimed_at: AwareDatetime
    previous_owner_ref: str | None = Field(default=None, min_length=1, max_length=512)

    def expired(self, now: datetime) -> bool:
        return self.lease_expires_at <= now

    def same_holder(self, other: SessionOwner) -> bool:
        return (
            self.owner_ref == other.owner_ref
            and self.epoch == other.epoch
            and self.generation == other.generation
        )


class DispatchRecord(_Contract):
    """One journaled native create/send, keyed by kind and provider idempotency key."""

    kind: DispatchKind
    idempotency_key: str = Field(min_length=1, max_length=1_024)
    expected_generation: int = Field(ge=1)
    instruction_digest: str = Field(min_length=1, max_length=128)
    phase: DispatchPhase = "intended"
    attempts: int = Field(default=1, ge=1)
    owner_ref: str = Field(min_length=1, max_length=512)
    owner_epoch: int = Field(ge=1)
    intended_at: AwareDatetime
    native_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    resolved_at: AwareDatetime | None = None
    reason: str | None = Field(default=None, min_length=1, max_length=128)

    @property
    def key(self) -> str:
        return dispatch_key(self.kind, self.idempotency_key)

    @property
    def ambiguous(self) -> bool:
        return self.phase == "intended"


class DispatchClaim(_Contract):
    """What `intend_dispatch` returns: the stored record and whether this caller may send."""

    record: DispatchRecord
    fresh: bool


class StaleSessionOwner(RuntimeError):
    """A write or settlement names an owner (or epoch) that no longer holds the session."""


class SessionOwnedElsewhere(RuntimeError):
    """Another worker's session manager holds a live lease on the session."""

    def __init__(self, owner_ref: str, lease_expires_at: datetime) -> None:
        super().__init__(f"session is owned by {owner_ref} until {lease_expires_at.isoformat()}")
        self.owner_ref = owner_ref
        self.lease_expires_at = lease_expires_at


class DispatchConflict(ValueError):
    """The same idempotency key was journaled for a different instruction or native ref."""


class DispatchFenced(RuntimeError):
    """The run's Stop Fence denies a new native dispatch (a governed effect after the fence)."""

    def __init__(self, effect_ref: str, fence_command_id: str | None) -> None:
        super().__init__(f"{effect_ref} is stop-fenced by {fence_command_id}")
        self.effect_ref = effect_ref
        self.fence_command_id = fence_command_id


class ProviderCapacityLimited(RuntimeError):
    """A lane's create/send was refused by a provider limit before any work was accepted.

    The dispatch is journaled `declined`; the workflow decides the wait or rejection with
    MP-05 `plan_limit_response` (Temporal timers only, no provider retry loop)."""

    def __init__(self, signal: ProviderLimitSignal) -> None:
        super().__init__(f"{signal.lane_profile} {signal.kind} ({signal.source})")
        self.signal = signal


def dispatch_key(kind: DispatchKind, idempotency_key: str) -> str:
    return f"{kind}:{idempotency_key}"


def instruction_digest(
    *, kind: DispatchKind, instruction_ref: str | None, binding_digest: str, turn_no: int
) -> str:
    return sha256_digest(
        {
            "kind": kind,
            "instruction_ref": instruction_ref,
            "binding_digest": binding_digest,
            "turn_no": turn_no,
        }
    )


def claim_ownership(
    current: SessionOwner | None,
    *,
    owner_ref: str,
    generation: int,
    now: datetime,
    lease: timedelta,
) -> SessionOwner:
    """Claim (or renew) the session for `owner_ref`; a live foreign lease refuses it."""

    expires = now + lease
    if current is None:
        return SessionOwner(
            owner_ref=owner_ref,
            epoch=1,
            generation=generation,
            lease_expires_at=expires,
            claimed_at=now,
        )
    if generation < current.generation:
        raise StaleSessionOwner(
            f"generation {generation} is superseded by generation {current.generation}"
        )
    if current.owner_ref == owner_ref and current.generation == generation:
        return current.model_copy(
            update={"lease_expires_at": max(expires, current.lease_expires_at)}
        )
    if generation == current.generation and not current.expired(now):
        raise SessionOwnedElsewhere(current.owner_ref, current.lease_expires_at)
    return SessionOwner(
        owner_ref=owner_ref,
        epoch=current.epoch + 1,
        generation=generation,
        lease_expires_at=expires,
        claimed_at=now,
        previous_owner_ref=current.owner_ref,
    )


def assert_holder(current: SessionOwner | None, owner: SessionOwner) -> None:
    if current is None or not current.same_holder(owner):
        holder = "nobody" if current is None else f"{current.owner_ref}#{current.epoch}"
        raise StaleSessionOwner(f"{owner.owner_ref}#{owner.epoch} no longer holds it ({holder})")


def renew_ownership(
    current: SessionOwner | None, owner: SessionOwner, *, expires_at: datetime
) -> SessionOwner:
    """Extend the holder's lease; an owner fenced out by a takeover cannot renew."""

    assert_holder(current, owner)
    assert current is not None
    return current.model_copy(
        update={"lease_expires_at": max(expires_at, current.lease_expires_at)}
    )


def intend(
    current: DispatchRecord | None, incoming: DispatchRecord, owner: SessionOwner
) -> DispatchClaim:
    """Journal a dispatch before it is issued. `fresh` is True only when nothing the provider
    may have accepted is recorded for the key; otherwise the caller must reconcile."""

    if incoming.owner_ref != owner.owner_ref or incoming.owner_epoch != owner.epoch:
        raise StaleSessionOwner("a dispatch is journaled by the current owner only")
    if current is None:
        return DispatchClaim(record=incoming.model_copy(update={"attempts": 1}), fresh=True)
    if current.instruction_digest != incoming.instruction_digest:
        raise DispatchConflict(
            f"{current.key} was journaled for another instruction ({current.instruction_digest})"
        )
    if current.phase in _REDISPATCHABLE:
        return DispatchClaim(
            record=incoming.model_copy(
                update={"attempts": current.attempts + 1, "phase": "intended"}
            ),
            fresh=True,
        )
    return DispatchClaim(record=current, fresh=False)


def resolve(
    current: DispatchRecord | None,
    *,
    outcome: DispatchOutcome,
    owner: SessionOwner,
    at: datetime,
    native_ref: str | None = None,
    reason: str | None = None,
) -> DispatchRecord:
    """Record what became of a journaled dispatch (acknowledgement, refusal, ambiguity)."""

    if current is None:
        raise LookupError("a dispatch is resolved only after it was journaled")
    if outcome == "acknowledged":
        if native_ref is None:
            raise ValueError("an acknowledged dispatch names its native identity")
        if current.phase == "acknowledged":
            if current.native_ref != native_ref:
                raise DispatchConflict(
                    f"{current.key} is acknowledged as {current.native_ref}, not {native_ref}"
                )
            return current
    elif current.phase == "acknowledged":
        raise DispatchConflict(f"{current.key} is already acknowledged as {current.native_ref}")
    elif current.phase == "in_doubt" and outcome != "in_doubt":
        # Only an operator decision clears an in-doubt dispatch (not this journal).
        raise DispatchConflict(f"{current.key} is in doubt; an operator reconciles it")
    return current.model_copy(
        update={
            "phase": outcome,
            "native_ref": native_ref if native_ref is not None else current.native_ref,
            "resolved_at": at,
            "reason": reason,
            "owner_ref": owner.owner_ref,
            "owner_epoch": owner.epoch,
        }
    )


class DispatchLookup(_Contract):
    """A lane's answer about a journaled dispatch it may or may not have accepted."""

    outcome: LookupOutcome
    native_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    detail: str = Field(default="", max_length=512)


@runtime_checkable
class DispatchReconcilingLane(Protocol):
    """A lane that can tell, by idempotency key or native identity, whether a create/send
    was accepted. `not_received` must be the provider's authoritative answer; a lane that
    cannot be sure answers `unknown` (the unit parks `in_doubt`)."""

    async def reconcile_dispatch(
        self,
        record: DispatchRecord,
        *,
        session: SessionHandle | None,
    ) -> DispatchLookup: ...


SteerOutcome = Literal["applied", "stale_target"]


class SteerResult(_Contract):
    outcome: SteerOutcome
    target_turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    detail: str = Field(default="", max_length=512)


@runtime_checkable
class SteeringLane(Protocol):
    """A lane that steers the exact active native turn (`cooperative_inject`). A turn that
    completed concurrently returns `stale_target`; it never lands on another turn."""

    async def steer(self, turn: TurnHandle, *, instruction_ref: str) -> SteerResult: ...


__all__ = [
    "DispatchClaim",
    "DispatchConflict",
    "DispatchFenced",
    "DispatchKind",
    "DispatchLookup",
    "DispatchOutcome",
    "DispatchPhase",
    "DispatchReconcilingLane",
    "DispatchRecord",
    "ProviderCapacityLimited",
    "SessionOwnedElsewhere",
    "SessionOwner",
    "StaleSessionOwner",
    "SteerResult",
    "SteeringLane",
    "assert_holder",
    "claim_ownership",
    "dispatch_key",
    "instruction_digest",
    "intend",
    "renew_ownership",
    "resolve",
]
