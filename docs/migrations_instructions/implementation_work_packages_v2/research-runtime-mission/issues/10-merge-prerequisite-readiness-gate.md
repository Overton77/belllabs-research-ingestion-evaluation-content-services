# RRM-010 — Review, merge and hand off prerequisite readiness

**What to build:** one tested integration revision and readiness manifest the user can use to start a separate company-fixture session.

**Blocked by:** RRM-005, RRM-006, RRM-007, RRM-008, RRM-009 and RRM-013 accepted; transitive prerequisites also accepted.
**Status:** blocked
**Branch:** `integration/research-runtime-mission`
**Authority:** v2 readiness/evidence contracts, accepted lifecycle amendments and all owning tickets
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-010/`

- [ ] Every prerequisite ticket has actual tested commits, owning qualification, reviewer disposition and no hidden mandatory gate.
- [ ] The integration revision passes Ruff, mypy, applicable offline/full suite, real persistence, required provider/service technical qualifications and diff/replacement checks.
- [ ] Captured histories replay; worker-loss and forced continuation cover active children/messages; checkpoint recovery does not duplicate provider work.
- [ ] Combined technical smoke joins inspection, historical state, a safe derived fork, boundary intervention and cancellation through the governed facade. It includes an active real async subagent on the Agent Server: inspection shows it, fork admission classifies it, and cancellation reconciles it.
- [ ] Record precisely what fork boundaries and interventions are proven; arbitrary cognitive steering is deferred explicitly.
- [ ] Verify protected parent lineage/results, full result/usage/effect settlement and capability availability on the actual integration commit.
- [ ] Review and merge the accepted integration branch to main; preserve unrelated user changes and do not push without authorization.
- [ ] Publish `ready_for_separate_fixture_session` with prerequisite revision, accepted meta revision, definitions/capability inputs, deployment instructions (including the Agent Server), estimated budget limits and remaining held work.
- [ ] Supply the user a later fixture-session prompt; RRM-011 stays held and CP-050 stays unaccepted until its complete tracer evidence exists.
- [ ] Stop the current session. Do not create/start a company fixture, run its report/fork, schedule an automation or message another agent to execute it.

This ticket is not full system/mission acceptance. If a required prerequisite fails, return the named owning ticket for rework and do not emit readiness.
