"""MP-12: the pure continuation phase machine and the four independent counters."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from mission_control.application.context.continuation import ContinuationTransfer, TransferStatus
from mission_control.domain.context.checkpoint import ContinuationTrigger, ContinuationTriggerKind
from mission_control.domain.context.phases import (
    FENCING_PHASES,
    PHASE_ORDER,
    ActivationCandidate,
    ContinuationPhase,
    MechanismCounters,
    PhaseEntry,
    PhaseTransitionRejected,
    ProgressMechanism,
    advance_phase,
    decide_activation,
    fences_source,
    next_phase,
    phase_entries_consistent,
    target_generation_for,
)

NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


def test_phases_advance_one_step_idempotently_and_never_backwards() -> None:
    current = ContinuationPhase.REQUESTED
    walked = [current]
    while (following := next_phase(current)) is not None:
        assert advance_phase(current, current) == current, "re-entering is a no-op"
        current = advance_phase(current, following)
        walked.append(current)
    assert tuple(walked) == PHASE_ORDER
    with pytest.raises(PhaseTransitionRejected):
        advance_phase(ContinuationPhase.SEALED, ContinuationPhase.FROZEN)
    with pytest.raises(PhaseTransitionRejected):
        advance_phase(ContinuationPhase.SEALED, ContinuationPhase.HYDRATED)
    assert next_phase(ContinuationPhase.ACTIVATED) is None


def test_the_source_is_fenced_from_frozen_until_activation_or_an_ended_transfer() -> None:
    assert set(PHASE_ORDER[1:-1]) == FENCING_PHASES
    assert not fences_source(ContinuationPhase.REQUESTED, terminal=False)
    assert fences_source(ContinuationPhase.FROZEN, terminal=False)
    assert fences_source(ContinuationPhase.VERIFIED, terminal=False)
    assert not fences_source(ContinuationPhase.ACTIVATED, terminal=False)
    assert not fences_source(ContinuationPhase.HYDRATED, terminal=True), "ended: released"


def test_exactly_one_target_generation_activates_per_source_generation() -> None:
    first = ActivationCandidate(
        transfer_id="t1", source_generation=1, target_generation=2, phase=ContinuationPhase.VERIFIED
    )
    second = first.model_copy(update={"transfer_id": "t2"})
    assert decide_activation(first, (second,)).reason == "activated"
    winner = first.model_copy(update={"phase": ContinuationPhase.ACTIVATED})
    taken = decide_activation(second, (winner,))
    assert (taken.allowed, taken.reason, taken.winner_transfer_id) == (
        False,
        "generation_taken",
        "t1",
    )
    again = decide_activation(winner, (second,))
    assert (again.allowed, again.reason) == (True, "already_activated")
    unverified = second.model_copy(update={"phase": ContinuationPhase.HYDRATED})
    assert decide_activation(unverified, ()).reason == "not_verified"
    later = second.model_copy(update={"source_generation": 2, "target_generation": 3})
    assert decide_activation(later, (winner,)).allowed, "a new source generation may move on"
    assert target_generation_for(1) == 2


def test_phase_entries_must_be_ordered_without_repeats() -> None:
    entries = [
        PhaseEntry(phase=ContinuationPhase.FROZEN, entered_at=NOW),
        PhaseEntry(phase=ContinuationPhase.SNAPSHOTTED, entered_at=NOW),
    ]
    assert phase_entries_consistent(entries)
    assert not phase_entries_consistent([*entries, entries[0]])
    assert not phase_entries_consistent(list(reversed(entries)))


def test_each_mechanism_moves_only_its_own_counter() -> None:
    counters = MechanismCounters()
    for mechanism in ProgressMechanism:
        moved = counters.advanced(mechanism)
        assert counters.differs_only_in(moved, mechanism), mechanism
        assert moved.budget_resets == 0 and moved.retry_resets == 0
        for other in ProgressMechanism:
            if other != mechanism:
                assert not counters.differs_only_in(moved, other)
    after_compaction = counters.advanced(ProgressMechanism.PROVIDER_COMPACTION)
    assert after_compaction.goal_iteration == counters.goal_iteration
    assert after_compaction.continuation_transfers == counters.continuation_transfers
    assert after_compaction.temporal_segment == counters.temporal_segment


def _transfer(**changes: object) -> ContinuationTransfer:
    values: dict[str, object] = {
        "transfer_id": "transfer-1",
        "request_scope": "tenant-1",
        "run_key": "run-1",
        "activation_key": "unit-1",
        "logical_execution_id": "unit-1",
        "lane_profile": "cursor_local",
        "trigger": ContinuationTrigger(
            kind=ContinuationTriggerKind.REQUEST_CONTINUATION, ref="command://c1", observed_at=NOW
        ),
        "delivery": "wait_then_send",
        "source_session_ref": "agent-1",
        "requested_at": NOW,
        "updated_at": NOW,
    }
    values.update(changes)
    return ContinuationTransfer.model_validate(values)


def test_a_pre_mp12_row_lifts_its_phase_from_its_status() -> None:
    assert _transfer().phase == ContinuationPhase.REQUESTED
    sealed = _transfer(status=TransferStatus.SEALED, checkpoint_id="cp-1")
    assert sealed.phase == ContinuationPhase.SEALED and sealed.open
    transferred = _transfer(
        status=TransferStatus.TRANSFERRED, checkpoint_id="cp-1", target_session_ref="agent-2"
    )
    assert transferred.phase == ContinuationPhase.VERIFIED and transferred.fencing
    released = transferred.model_copy(update={"released": True})
    assert ContinuationTransfer.model_validate(released.model_dump(mode="python")).activated
    failed = _transfer(status=TransferStatus.FAILED, failure_reason="x")
    assert failed.ended and not failed.fencing and not failed.open
    review = _transfer(status=TransferStatus.HUMAN_REVIEW, failure_reason="review")
    assert review.ended and not review.fencing


def test_a_recorded_phase_is_never_lowered_by_the_legacy_lift() -> None:
    entries = (PhaseEntry(phase=ContinuationPhase.FROZEN, entered_at=NOW),)
    frozen = _transfer(phase=ContinuationPhase.FROZEN, phases=entries)
    assert frozen.phase == ContinuationPhase.FROZEN and frozen.fencing
    with pytest.raises(ValueError, match="out of order"):
        _transfer(
            phase=ContinuationPhase.SNAPSHOTTED,
            phases=(
                PhaseEntry(phase=ContinuationPhase.SNAPSHOTTED, entered_at=NOW),
                PhaseEntry(phase=ContinuationPhase.FROZEN, entered_at=NOW),
            ),
        )
