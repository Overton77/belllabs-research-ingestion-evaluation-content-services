# RRM-002 — Restore a reproducible verification baseline

**What to build:** a trustworthy owning/shared verification gate so new lifecycle changes can be assessed against real assertions rather than stale paths and time-dependent tests.

**Blocked by:** None — next code issue; existing accepted package authority applies.
**Status:** accepted 2026-10-01; merged into `integration/research-runtime-mission` (tested head `ea0f529`; see evidence)
**Branch:** `wp/rrm-002-baseline`
**Authority:** accepted CP/BP package checks and readiness requirements; no new lifecycle semantics
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-002/`

The audit records mypy success, 104 Ruff errors, 44 full-suite failures and one focused failure. Six causes were reproduced: moved authority-guard paths, erroneous repository-root calculations for migrations/reviewed payloads/scanner, expired snapshot retention in tests, and strict API payload rejection. Classify remaining failures individually before deciding repairs.

- [x] Record dirty/untracked ownership; work from a reviewed commit/worktree and preserve existing user changes.
- [x] Repair moved test/source/migration references without removing architecture, provenance or authority assertions.
- [x] Make snapshot test time deterministic; retain production retention/compatibility/tamper validation.
- [x] Diagnose the 422 publish response and correct the test/request or implementation according to the accepted contract; never weaken strict validation just to recover 201.
- [x] Classify every full-suite failure by cause, owner and required environment; fix regressions and mission-blocking stale tests. Deliberately retired tests require an accepted replacement/deletion rationale.
- [x] Restore Ruff and the focused/shared offline gates; mypy remains green. No blanket skip/xfail or undocumented test deselection.
- [x] Real-service/provider gates stay explicit and separately reproducible; unavailable infrastructure is not silently reported as a pass.
- [x] Record exact commands, tests, sanitized results and any genuinely external unresolved gate. Final combined acceptance cannot hide an unresolved mandatory gate.
- [x] Review and merge the completed repair into the mission integration branch; update the issue/index with actual tested commits.

Out of scope: running company missions, consuming unreviewed agentic-component work, introducing checkpoint APIs or changing lifecycle semantics. If a genuine unrelated functional defect exceeds this issue, make a concrete dependent repair ticket instead of disabling its test.

## Disposition (2026-10-01)

Accepted. Commits on `wp/rrm-002-baseline`: `858c721` (Ruff style only) and `ea0f529` (baseline repairs), plus the evidence/ticket commit. Results:

- Shared offline gate: 678 passed, 46 skipped, 2 xfailed, 0 failed. This holds both with the developer `.env` and hermetically without one.
- Focused owning suites: 142 passed.
- Ruff: clean. mypy: 332 files clean.
- Disposable opted-in Postgres/Mongo suites: 38 passed.

One production regression was fixed (`DEFAULT_SEMANTIC_OVERLAY`). One out-of-scope defect was split into RRM-012, with two cases strict-xfailed. Unrun external gates: Agent Server endpoint, live providers and WSL. Full evidence: [`evidence_v2/research-runtime-mission/RRM-002/README.md`](../../../evidence_v2/research-runtime-mission/RRM-002/README.md).
