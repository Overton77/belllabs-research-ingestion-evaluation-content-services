# RRM-002 — Restore a reproducible verification baseline

**What to build:** a trustworthy owning/shared verification gate so new lifecycle changes can be assessed against real assertions rather than stale paths and time-dependent tests.

**Blocked by:** None — next code issue; existing accepted package authority applies.
**Status:** ready-for-agent
**Branch:** `wp/rrm-002-baseline`
**Authority:** accepted CP/BP package checks and readiness requirements; no new lifecycle semantics
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-002/`

The audit records mypy success, 104 Ruff errors, 44 full-suite failures and one focused failure. Six causes were reproduced: moved authority-guard paths, erroneous repository-root calculations for migrations/reviewed payloads/scanner, expired snapshot retention in tests, and strict API payload rejection. Classify remaining failures individually before deciding repairs.

- [ ] Record dirty/untracked ownership; work from a reviewed commit/worktree and preserve existing user changes.
- [ ] Repair moved test/source/migration references without removing architecture, provenance or authority assertions.
- [ ] Make snapshot test time deterministic; retain production retention/compatibility/tamper validation.
- [ ] Diagnose the 422 publish response and correct the test/request or implementation according to the accepted contract; never weaken strict validation just to recover 201.
- [ ] Classify every full-suite failure by cause, owner and required environment; fix regressions and mission-blocking stale tests. Deliberately retired tests require an accepted replacement/deletion rationale.
- [ ] Restore Ruff and the focused/shared offline gates; mypy remains green. No blanket skip/xfail or undocumented test deselection.
- [ ] Real-service/provider gates stay explicit and separately reproducible; unavailable infrastructure is not silently reported as a pass.
- [ ] Record exact commands, tests, sanitized results and any genuinely external unresolved gate. Final combined acceptance cannot hide an unresolved mandatory gate.
- [ ] Review and merge the completed repair into the mission integration branch; update the issue/index with actual tested commits.

Out of scope: running company missions, consuming unreviewed agentic-component work, introducing checkpoint APIs or changing lifecycle semantics. If a genuine unrelated functional defect exceeds this issue, make a concrete dependent repair ticket instead of disabling its test.
