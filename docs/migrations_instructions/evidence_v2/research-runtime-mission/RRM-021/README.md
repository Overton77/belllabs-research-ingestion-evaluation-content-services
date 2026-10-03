# RRM-021 implementation evidence

Disposition: implemented; independent review pending (2026-10-02)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-021 settles the admitted baseline reservation of a StageGraph run. Requirements: REQ-CP-RUN-006 (reservations are released or settled before terminalization), the reducer's terminalization rule (`budget_not_settled`), REQ-CP-EXEC-008 step 7 (cancellation also terminalizes).
Base revision and head revision: base integration `0d0c184`; code commit `c30c9f8` (branch `wp/rrm-021-stagegraph-baseline-settlement`). Not merged (the coordinator owns review and merge).
Framework/package baseline: unchanged `uv.lock`; CPython 3.12; temporalio time-skipping test server and `start_local` (RRM-009 qualification).

## Implemented contracts and seams

- **Diagnosis confirmed.** Admission records `reservations["baseline"]`; StageGraph stages reserve and settle their own operation reservations, so the baseline is never consumed. `StageGraphDecisionService.complete` then reported `budget_settled=False` and the reducer rejected with `budget_not_settled`.
- **Fix: release before terminalizing.** `StageGraphDecisionService.settle_baseline` (`app/application/orchestration/service.py`) records a `RecordUsageAction` against reservation `baseline` with `actual_amounts={}` and `release_amounts` = the remaining baseline (the same shape GoalDirected uses at its closing boundary, `_settle_operation(..., "baseline", ...)`). The usage is zero, so nothing is consumed.
- **Activity.** `stagegraph.settle_baseline` (`StageGraphActivities.settle_baseline`, registered in `coordinator_activities("StageGraph", ...)`) with new contracts `StageGraphBaselineSettlementRequest` / `StageGraphBaselineSettlementResult` (`app/domain/orchestration/contracts.py`).
- **Workflow.** `StageGraphWorkflow` calls the activity once, immediately before `stagegraph.complete`, on every terminal outcome (completed, failed, cancelled; the cancellation saga reaches the same point). A local `baseline_settled` flag stops the cancelling liability-retry loop from re-issuing it. The call is gated by `workflow.patched("rrm-021-settle-stagegraph-baseline")` and made only when the family input carries a non-empty `baseline_reservation`, so an empty-baseline history is unchanged and records no marker.
- **Idempotent, no double counting.** The authoritative budget is the source of truth: a baseline already released (Activity retry, a continued segment, a replayed family) makes the call a no-op that returns the current run version. A stale version (an outside command such as a cancel moved it) is retried once at the reported version under a new command identity, like `initialize`. A request baseline that differs from the remaining admitted reservation raises `ValueError` and releases nothing.
- **Governed launch unchanged.** `RunLaunchService` already refuses a family input whose baseline differs from the admitted one (`budget_mismatch`, RRM-009). It is now also proven in the production acceptance test.

## Requirement-to-evidence map

| Requirement / acceptance | Test | Observed assertion |
|---|---|---|
| Completes with a non-empty baseline, budget settled, nothing reserved | `test_stagegraph_with_a_baseline_completes_with_the_budget_settled` | run `terminal` / `completed`; `reserved` all zero; no `baseline` reservation; `consumed["tokens.total"] == 0`; the terminal fixture refuses while `baseline` is reserved, and saw it released; marker `rrm-021-settle-stagegraph-baseline` in the history; a second settlement is a no-op and the budget is unchanged |
| Cancelled run releases the baseline and terminalizes `cancelled` | `test_cancelled_stagegraph_with_a_baseline_releases_it_and_terminalizes_cancelled` | root result `cancelled`; run `terminal` / `cancelled`; nothing reserved; no `baseline`; zero consumed; one accepted settlement; history replays |
| Differing baseline refused at settlement | `test_a_baseline_that_differs_from_the_admitted_reservation_is_refused` | `ValueError`; budget unchanged |
| Replay of captured pre-patch histories | `test_pre_change_stagegraph_history_replays_without_the_baseline_settlement` (2 histories) | no `rrm-021` marker, no `stagegraph.settle_baseline` scheduled, `Replayer` accepts both |
| Production path with a non-empty baseline | `test_stagegraph_runs_through_the_production_composition_with_fork_relay_and_inspection` (RRM-009, under the lock) | source and derived runs admit `{"tokens.total": 20}` (the fork carries the same), both end `completed` with nothing reserved and no `baseline` reservation |
| Governed launch refuses another baseline | same RRM-009 test; `tests/unit/run_control/test_run_launch.py` | HTTP 422 `budget_mismatch` for a baseline of 21 |
| Failed outcome | covered by construction, not by a dedicated run | settlement precedes `stagegraph.complete` unconditionally; the fixture harnesses have no failing stage. The settlement takes no outcome input |

## Changed paths and migrations

- `app/application/orchestration/service.py`, `app/domain/orchestration/contracts.py`, `app/temporal/orchestration_activities.py`, `app/temporal/workflows/stagegraph.py`.
- `app/temporal/registration/activities.py`: one line (coordinator-owned seam), adds `settle_baseline` to the StageGraph activity surface.
- Tests: `tests/integration/temporal/test_rrm_021_stagegraph_baseline.py`, `tests/integration/temporal/test_rrm_021_replay_pre_change_histories.py`, histories `tests/fixtures/histories/rrm021_pre_change/stagegraph_baseline_{completed,cancelled}.run1.json`, `tests/acceptance/control_plane/test_rrm_009_production_composition.py`, `tests/fixtures/rrm009_production_stack.py` (`baseline_reservation()` is `{"tokens.total": 20}` for both families; the StageGraph workaround is removed).
- No migration, no schema change.

## Deterministic verification

All commands ran in the worktree after `unset VIRTUAL_ENV`, with `uv run --no-sync`.

| Gate | Result |
|---|---|
| `ruff check app tests scripts` | All checks passed |
| `mypy app` and the two new test modules | no issues |
| Owning suites hermetic (`tests/integration/temporal`, `tests/unit/orchestration`, `tests/unit/run_control`, `tests/unit/operations/test_rrm_009_cancellation_composition.py`) | 313 passed, 2 skipped |
| Replay (RRM-007 pre-change, RRM-021 pre-change, RRM-008 family cancellation tests) | passed (7 history replays; the RRM-008 tests replay their own histories) |
| Full hermetic `pytest` (DSNs unset) | 1069 passed, 90 skipped, 3 xfailed (baseline 1064 / 90 / 3; +5 new tests) |
| Full DSN `pytest`, chunk `--ignore=tests/acceptance`, under the lock | 1061 passed, 27 skipped, 3 xfailed |
| Full DSN `pytest tests/acceptance`, under the lock | 62 passed, 9 skipped (chunks total 1123 passed, 36 skipped, 3 xfailed; baseline 1118 / 36 / 3) |
| `git diff --check` | clean |

## Live runtime qualification

RRM-009 technical StageGraph qualification, under the stack lock, disposable PostgreSQL `127.0.0.1:55432` and MongoDB `127.0.0.1:27017`, Temporal `start_local`: `pytest tests/acceptance/control_plane/test_rrm_009_production_composition.py -k stagegraph` passed twice (155.7 s before the budget assertions, 146.5 s after). The run now admits a non-empty baseline on the production path (launch, fork, relay, terminalization). No container was started, stopped or pruned. No company fixtures.

## Replay and recovery artifacts

- `tests/fixtures/histories/rrm021_pre_change/` was captured on the unpatched `app/temporal/workflows/stagegraph.py` (base `0d0c184`, the file stashed during capture) with a non-empty baseline in the family input: one completed run and one cancelled run (RRM-008 saga). Neither holds the new marker. Replaying them on the patched code proves `workflow.patched` returns false on replay and the recorded command sequence is intact.
- The RRM-007 pre-change histories and the RRM-008 family cancellation tests replay green.
- The new tests replay their own post-change histories (`Replayer`), including the marker.

## Replacement and deletion checks

- The RRM-009 harness workaround (`baseline_reservations={}` for StageGraph, and the matching `{}` in the fork request) is deleted.
- The fixture `terminalize_through_run_control` in the RRM-008 test still releases a leftover baseline itself, for tests whose family input carries none; the new tests use a `complete` that fails if the baseline is still reserved.
- `tests/acceptance/control_plane/test_rrm_006_semantic_forks.py` and `test_wp_bp_010_live.py` still admit `baseline_reservations={}` (their own fixtures, not RRM-009); they are unchanged.

## Unresolved risks and drift checks

- Failed outcome has no dedicated drill (see the map); the code path is shared with the other outcomes.
- If the family fails before it reaches the terminal proposal (for example `stagegraph_blocked`), no terminalization happens and the baseline is not released; that is a pre-existing non-terminal failure, not changed here.
- Settlement uses `release` against zero usage. If a later ticket makes the baseline cover recorded usage, `settle_baseline` is the one place to change.
- `app/temporal/registration/activities.py` is coordinator-owned; review that one line. CR-4 and RRM-020 may touch neighbouring lines (the StageGraph tuple), so expect a trivial merge conflict at most.
- The RRM-015 digest guard passes (included in the full runs).

## Final disposition

Implemented; independent review pending. Nothing merged, pushed or amended.
