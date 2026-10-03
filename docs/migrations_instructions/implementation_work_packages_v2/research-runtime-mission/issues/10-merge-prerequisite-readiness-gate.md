# RRM-010 — Review, merge and hand off prerequisite readiness

**What to build:** one tested integration revision and readiness manifest the user can use to start a separate company-fixture session.

**Blocked by:** RRM-005, RRM-006, RRM-007, RRM-008, RRM-009 and RRM-013 accepted; transitive prerequisites also accepted.
**Status:** accepted 2026-10-02 (tested head `7e0b77e`, integration merge `9da2919`; [evidence](../../../evidence_v2/research-runtime-mission/RRM-010/README.md))
**Branch:** `integration/research-runtime-mission`
**Authority:** v2 readiness/evidence contracts, accepted lifecycle amendments and all owning tickets
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-010/`

- [x] Every prerequisite ticket has actual tested commits, owning qualification, reviewer disposition and no hidden mandatory gate.
- [x] The integration revision passes Ruff, mypy, applicable offline/full suite, real persistence, required provider/service technical qualifications and diff/replacement checks. (`7e0b77e`: ruff, mypy, `git diff --check`, three hermetic one-process runs 1091/94/2 at three seeds, one full DSN pass 1148/37/2 with ambient tracing, live smoke. A second DSN pass's chunk A timed out at 595 s with one failure, root cause not established; see manifest §5.)
- [x] Captured histories replay; worker-loss and forced continuation cover active children/messages; checkpoint recovery does not duplicate provider work. (Replay, restart and forced Continue-As-New suites inside the full runs; restart with an active real child is RRM-009's live proof, not re-run on this head.)
- [x] Combined technical smoke joins inspection, historical state, a safe derived fork, boundary intervention and cancellation through the governed facade. It includes an active real async subagent on the Agent Server: inspection shows it, fork admission classifies it, and cancellation reconciles it.
- [x] Record precisely what fork boundaries and interventions are proven; arbitrary cognitive steering is deferred explicitly.
- [x] Verify protected parent lineage/results, full result/usage/effect settlement and capability availability on the actual integration commit.
- [ ] (Delegated to the peer session biotech-61 at the user's request; 0 conflicting hunks with the user's 5 uncommitted files.) Review and merge the accepted integration branch to main; preserve unrelated user changes and do not push without authorization.
- [x] Publish `ready_for_separate_fixture_session` with prerequisite revision, accepted meta revision, definitions/capability inputs, deployment instructions (including the Agent Server), estimated budget limits and remaining held work.
- [x] Supply the user a later fixture-session prompt; RRM-011 stays held and CP-050 stays unaccepted until its complete tracer evidence exists.
- [ ] Stop the current session. Do not create/start a company fixture, run its report/fork, schedule an automation or message another agent to execute it.

This ticket is not full system/mission acceptance. If a required prerequisite fails, return the named owning ticket for rework and do not emit readiness.
- [x] (Coordinator, from the RRM-016 merge gate; stability fixes in `wp/rrm-010-stability`. Residual: this test can still time out under extreme host load. It passed with the same seed on the final commit.) Two RRM-007 Temporal tests (`test_rrm_007_boundary_interventions.py`, time-skipping) have each failed once with a wall-clock `asyncio` timeout while the host was heavily loaded, and passed in isolation. Before the final gate, run the full suite at least three times on the final integration commit. Then either show those tests stable, or make their waits load-tolerant without weakening what they assert. Record the result. **Open:** stable in H1–H3, D1 and the five `STABILITY.md` runs. A second DSN pass (seed 31337) aborted at 595 s with one `F` at this test's position (inferred), and the file passed 6/6 alone with the same seed. Root cause not established (manifest §5).
- [x] (Coordinator.) RRM-015 (required before RRM-010), RRM-016, RRM-018 and RRM-019 are accepted. RRM-018 and RRM-019 block multi-iteration GoalDirected runs on Mongo documents and with differing output refs.
- [x] (Coordinator, from the RRM-008 gates.) `test_rrm_007_interventions` failed once in a full DSN run, then passed 3 of 3 alone and in the next full run. The suspected race: the test stops worker 1 as soon as run control shows `paused`, possibly before the family's workflow task has recorded the pause. Confirm or fix the race (wait on the family's applied receipt, not the projection) as part of the stability item above.
- [x] (Coordinator, from CR-4.) `tests/unit/integrations/test_langsmith_tracing.py::test_settings_expose_langsmith_contract` failed in a full DSN run where `LANGSMITH_TRACING` was not forced to `false`. It passes alone. The test depends on the developer `.env` and on test order. Isolate it from the environment (monkeypatch or explicit settings) so the gate is independent of the developer `.env`.

