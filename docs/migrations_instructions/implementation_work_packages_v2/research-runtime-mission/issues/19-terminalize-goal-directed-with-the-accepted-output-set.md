# RRM-019 — GoalDirected terminalization names exactly the accepted output set

**What to build:** A GoalDirected run whose iterations produce different output refs terminalizes `complete`: the outputs the family promotes as accepted output evidence and the outputs its terminalization proposal names are the same set.

**Blocked by:** None.
**Blocks:** Any completing GoalDirected run whose iterations produce distinct output refs, including the GoalDirected company fixture (RRM-011). Whether it blocks RRM-010 is the coordinator's decision.
**Status:** ready-for-agent (found by RRM-016, 2026-10-02)
**Branch:** `wp/rrm-019-goal-directed-terminal-outputs`
**Authority:** REQ-BP-GD-010 (stopping produces a proposal, not terminality), REQ-CP-RUN-005 (terminality follows accepted evidence; the reducer rejects `terminal_output_mismatch` when terminal outputs differ from the accepted outputs)

## Diagnosis (RRM-016, 2026-10-02)

- `GoalDirectedInterpreter.apply_execution_result` accumulates `state.output_refs` as the ordered union of every iteration's executor output refs, and `result(state)` returns that union (`app/domain/orchestration/goal_directed.py`).
- `GoalDirectedWorkflow.run` records `record_output_evidence` for every ref in `result.output_refs` (the union) when the outcome is `complete`, then submits `terminalize` with `valid_output_refs = proposal.output_refs` (`app/temporal/workflows/goal_directed.py`).
- The terminalization proposal's `output_refs` is only the final executor's `execution.output_refs` (`_terminalization_proposal` in the interpreter).
- The reducer requires `set(valid_output_refs) == accepted output evidence` and rejects `terminal_output_mismatch`, so the family fails non-retryably at the end of an otherwise complete run.
- Every earlier fixture reused one output ref across iterations (the WP-BP-020 live instructions even tell iteration 2 to "preserve" iteration 1's ref), which hid it.
- Reproduction: `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py::test_iterations_with_distinct_output_refs_terminalize`, marked `xfail(strict=True, raises=WorkflowFailureError)` citing this ticket; it re-raises only when the cause is `terminal_output_mismatch`. Remove the marker when fixed.

## Acceptance

- [ ] A decided rule (spec-cited) for which outputs a completed GoalDirected run promotes: the verified final outputs, or every iteration's outputs named by the proposal.
- [ ] Promotion and the terminalization proposal use the same set; replay-safe for existing histories (`workflow.patched` if commands change).
- [ ] The RRM-019 reproduction passes without its marker.
