"""Recovery 2026-10-09 (MP-20): a linked child family that concluded `failed` returns
normally; the linked-run workflow must observe it as a failed child, not a completed one."""

from __future__ import annotations

from mission_control.adapters.temporal.linked_run_workflow import _concluded_failed


def test_failed_children_are_recognised_in_both_family_shapes() -> None:
    assert _concluded_failed({"completion_proposal": {"failed": True}})
    assert _concluded_failed({"terminalization_proposal": {"proposed_outcome": "fail"}})


def test_accepted_or_partial_children_are_not_failed() -> None:
    assert not _concluded_failed({"completion_proposal": {"failed": False}, "output_refs": {}})
    assert not _concluded_failed({"terminalization_proposal": {"proposed_outcome": "complete"}})
    assert not _concluded_failed(
        {"terminalization_proposal": {"proposed_outcome": "partial_or_fail"}}
    )
    assert not _concluded_failed(None)
