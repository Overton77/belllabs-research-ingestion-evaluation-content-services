"""Continuation phase machine and the four independent progress counters (MP-12, ADR-0039).

SPEC-01 "Four independent progress mechanisms": continuation crash recovery is a persisted
phase machine ``requested -> frozen -> snapshotted -> sealed -> target_prepared -> hydrated
-> verified -> activated``. Each phase is idempotent, only one target generation activates
for one source generation of a logical execution, and the old generation stays fenced
(no new agent action is dispatched to it) from ``frozen`` until the activation is
recorded or the transfer ends without a target.

Everything here is pure: the application layer persists the phase on the
``ContinuationTransfer`` row (``application/context/phases.py``) and the lane turn service
reads the fence; neither needs anything else from this module than the rules below.

The second half records the rule that goal iteration, provider context compaction,
mission continuation and Temporal Continue-As-New are *separate* counters: advancing one
never moves, resets or implies another (``MechanismCounters.advanced``).
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContinuationPhase(StrEnum):
    REQUESTED = "requested"
    FROZEN = "frozen"
    SNAPSHOTTED = "snapshotted"
    SEALED = "sealed"
    TARGET_PREPARED = "target_prepared"
    HYDRATED = "hydrated"
    VERIFIED = "verified"
    ACTIVATED = "activated"


PHASE_ORDER: tuple[ContinuationPhase, ...] = (
    ContinuationPhase.REQUESTED,
    ContinuationPhase.FROZEN,
    ContinuationPhase.SNAPSHOTTED,
    ContinuationPhase.SEALED,
    ContinuationPhase.TARGET_PREPARED,
    ContinuationPhase.HYDRATED,
    ContinuationPhase.VERIFIED,
    ContinuationPhase.ACTIVATED,
)
# Between these two the source generation takes no new agent action: the mailbox is held,
# the lane refuses every create/send except the activated target's first turn.
FENCING_PHASES: frozenset[ContinuationPhase] = frozenset(PHASE_ORDER[1:-1])


def phase_index(phase: ContinuationPhase) -> int:
    return PHASE_ORDER.index(phase)


def next_phase(phase: ContinuationPhase) -> ContinuationPhase | None:
    index = phase_index(phase)
    return PHASE_ORDER[index + 1] if index + 1 < len(PHASE_ORDER) else None


class PhaseTransitionRejected(ValueError):
    """``continuation_phase_rejected``: a phase was entered out of order or twice."""

    code = "continuation_phase_rejected"


def advance_phase(current: ContinuationPhase, target: ContinuationPhase) -> ContinuationPhase:
    """The phase after one idempotent step.

    Entering the current phase again is a no-op (a retried activity after a crash that
    persisted the phase); entering the successor advances; anything else is rejected, so a
    stale worker cannot move a transfer backwards or skip the verification.
    """

    if target == current:
        return current
    if next_phase(current) == target:
        return target
    raise PhaseTransitionRejected(f"continuation phase {current.value} cannot enter {target.value}")


def phase_reached(current: ContinuationPhase, phase: ContinuationPhase) -> bool:
    return phase_index(current) >= phase_index(phase)


def fences_source(phase: ContinuationPhase, *, terminal: bool) -> bool:
    """Whether a transfer in ``phase`` still fences its source generation.

    A transfer that ended without activating (failed, governor exhausted, human review)
    releases the fence: the source session may act again (``terminal``)."""

    return phase in FENCING_PHASES and not terminal


class PhaseEntry(_Contract):
    """One phase entered, with the fact that entering it recorded."""

    phase: ContinuationPhase
    entered_at: AwareDatetime
    detail: str = Field(default="", max_length=512)


def phase_entries_consistent(entries: Iterable[PhaseEntry]) -> bool:
    """Entries are in phase order with no repeats (a persisted sequence that is not is
    evidence of a defect, never silently accepted)."""

    seen: list[int] = []
    for entry in entries:
        index = phase_index(entry.phase)
        if seen and index <= seen[-1]:
            return False
        seen.append(index)
    return True


# --------------------------------------------------------------------------------------
# Activation: exactly one target generation per source generation
# --------------------------------------------------------------------------------------


class ActivationCandidate(_Contract):
    transfer_id: str = Field(min_length=1)
    source_generation: int = Field(ge=1)
    target_generation: int | None = Field(default=None, ge=2)
    phase: ContinuationPhase


class ActivationVerdict(_Contract):
    allowed: bool
    reason: Literal["activated", "already_activated", "generation_taken", "not_verified"]
    winner_transfer_id: str | None = None


def decide_activation(
    candidate: ActivationCandidate, siblings: Iterable[ActivationCandidate]
) -> ActivationVerdict:
    """Only one transfer activates a target generation for a source generation.

    ``siblings`` are the other transfers of the same logical execution. A candidate that is
    already activated is reported as such (idempotent); a verified candidate whose source
    generation another transfer already superseded loses (``generation_taken``); a candidate
    that has not been verified cannot activate.
    """

    if candidate.phase == ContinuationPhase.ACTIVATED:
        return ActivationVerdict(
            allowed=True, reason="already_activated", winner_transfer_id=candidate.transfer_id
        )
    if candidate.phase != ContinuationPhase.VERIFIED:
        return ActivationVerdict(allowed=False, reason="not_verified")
    for sibling in siblings:
        if sibling.transfer_id == candidate.transfer_id:
            continue
        if (
            sibling.phase == ContinuationPhase.ACTIVATED
            and sibling.source_generation == candidate.source_generation
        ):
            return ActivationVerdict(
                allowed=False, reason="generation_taken", winner_transfer_id=sibling.transfer_id
            )
    return ActivationVerdict(
        allowed=True, reason="activated", winner_transfer_id=candidate.transfer_id
    )


def target_generation_for(source_generation: int) -> int:
    return source_generation + 1


# --------------------------------------------------------------------------------------
# Four independent progress mechanisms (ADR-0039)
# --------------------------------------------------------------------------------------


class ProgressMechanism(StrEnum):
    GOAL_ITERATION = "goal_iteration"
    PROVIDER_COMPACTION = "provider_compaction"
    MISSION_CONTINUATION = "mission_continuation"
    TEMPORAL_CONTINUE_AS_NEW = "temporal_continue_as_new"


class MechanismCounters(_Contract):
    """One counter per mechanism, each advanced only by its own trigger.

    ``budget_resets`` and ``retry_resets`` exist to make the rule explicit in tests and
    records: no mechanism may increment them (a compaction or a transfer never resets a
    budget or a retry counter; only the budget ledger and the retry classifier do).
    """

    goal_iteration: int = Field(default=0, ge=0)
    provider_compaction_epoch: int = Field(default=0, ge=0)
    continuation_transfers: int = Field(default=0, ge=0)
    temporal_segment: int = Field(default=1, ge=1)
    budget_resets: int = Field(default=0, ge=0)
    retry_resets: int = Field(default=0, ge=0)

    def advanced(self, mechanism: ProgressMechanism) -> MechanismCounters:
        """The counters after one occurrence of ``mechanism``; every other field is
        unchanged by construction."""

        if mechanism == ProgressMechanism.GOAL_ITERATION:
            return self.model_copy(update={"goal_iteration": self.goal_iteration + 1})
        if mechanism == ProgressMechanism.PROVIDER_COMPACTION:
            return self.model_copy(
                update={"provider_compaction_epoch": self.provider_compaction_epoch + 1}
            )
        if mechanism == ProgressMechanism.MISSION_CONTINUATION:
            return self.model_copy(
                update={"continuation_transfers": self.continuation_transfers + 1}
            )
        return self.model_copy(update={"temporal_segment": self.temporal_segment + 1})

    def differs_only_in(self, other: MechanismCounters, mechanism: ProgressMechanism) -> bool:
        """True when ``other`` is this record with exactly the mechanism's counter moved."""

        field = _COUNTER_FIELD[mechanism]
        for name in type(self).model_fields:
            mine, theirs = getattr(self, name), getattr(other, name)
            if name == field:
                if theirs != mine + 1:
                    return False
            elif theirs != mine:
                return False
        return True


_COUNTER_FIELD: dict[ProgressMechanism, str] = {
    ProgressMechanism.GOAL_ITERATION: "goal_iteration",
    ProgressMechanism.PROVIDER_COMPACTION: "provider_compaction_epoch",
    ProgressMechanism.MISSION_CONTINUATION: "continuation_transfers",
    ProgressMechanism.TEMPORAL_CONTINUE_AS_NEW: "temporal_segment",
}


__all__ = [
    "FENCING_PHASES",
    "PHASE_ORDER",
    "ActivationCandidate",
    "ActivationVerdict",
    "ContinuationPhase",
    "MechanismCounters",
    "PhaseEntry",
    "PhaseTransitionRejected",
    "ProgressMechanism",
    "advance_phase",
    "decide_activation",
    "fences_source",
    "next_phase",
    "phase_entries_consistent",
    "phase_index",
    "phase_reached",
    "target_generation_for",
]
