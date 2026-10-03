# RRM-010 readiness gate evidence

Disposition: accepted 2026-10-02 (coordinator review; combined live smoke passed; repeated-run stability in STABILITY.md; main merge delegated to the peer session biotech-61 at the user's request; tested head `7e0b77e`; merged into integration at `9da2919`)
Recorded date: 2026-10-03 (America/New_York)

This README indexes the RRM-010 evidence:

- [READY_FOR_SEPARATE_FIXTURE_SESSION.md](READY_FOR_SEPARATE_FIXTURE_SESSION.md): the readiness manifest (status, revisions, ticket dispositions, deployment and Agent Server instructions, budget estimate, unresolved gates, main-merge commands).
- [STABILITY.md](STABILITY.md): test-stability fixes and repeated-run results.
- Combined technical smoke: `tests/acceptance/control_plane/test_rrm_010_combined_smoke.py`, behind `BELLABS_RUN_RRM_010_LIVE=1`.
  - **Coverage:** inspection, historical checkpoint read, StageGraph and GoalDirected safe forks, StageGraph wait release and GoalDirected pause/resume, and running cancellation with an active real async child on the Agent Server.
  - **Result:** 1 passed (161 s).
  - **Run IDs:** in the manifest.
  - **Spend:** under USD 0.10.
- Fixture-session prompt: `docs/RESEARCH_RUNTIME_FIXTURE_SESSION_PROMPT.md`. Held for a separate, user-started session. RRM-011 has not been started.
- Capability summary for the next phase: `docs/RESEARCH_RUNTIME_CAPABILITY_SUMMARY.md`.

## Integration merge gates (coordinator, merge commit `9da2919`)

Tested head `7e0b77e` merged `--no-ff` into `integration/research-runtime-mission` at `9da2919`.

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | no issues, 384 files |
| `hermetic full pytest on 8bd5203` | 1091 passed, 94 skipped, 2 xfailed, 0 failed |
| `hermetic on 7e0b77e, seeds 1 / 4242 / 90210 (implementer)` | 1091 passed, 94 skipped, 2 xfailed each time |
| `DSN pass 1 on 7e0b77e, seed 2024, LANGSMITH_TRACING not forced (implementer)` | 1148 passed, 37 skipped, 2 xfailed |
| `DSN pass 2 on 7e0b77e, seed 31337, chunk A (implementer)` | timed out at 595 s with one failure (test_goal_directed_policy_pause_is_durable_across_forced_continue_as_new) under heavy host load: about 133 tests in 595 s against 1,114 in 315 s in pass 1 |
| `DSN rerun on 8bd5203, same seed 31337, LANGSMITH_TRACING not forced (coordinator), chunks A + B` | 1085/27/2 + 63/10/0 = 1148 passed, 37 skipped, 2 xfailed, 0 failed |
| `git diff --check` | clean |

## Final disposition

accepted
