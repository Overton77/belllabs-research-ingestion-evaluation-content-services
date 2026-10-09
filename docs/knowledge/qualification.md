---
type: Verification Reference
title: Evidence and qualification gates
description: What deterministic local tests establish and what remains unproved, including the per-profile status of the seven lane profiles after the multi-provider packet's waves 0 to 2.
tags: [mission-control, implementation]
---

# Evidence and qualification gates

Use layered evidence: pure contract/reducer tests, real PostgreSQL constraints and
RLS, Temporal histories/replay, and authenticated HTTP-to-runtime acceptance.
Mocks can isolate a transport failure; they cannot prove database privileges,
durable replay or provider behavior.

Local proofs exercise StageGraph, GoalDirected, lifecycle delivery, snapshot/fork
reuse, artifact promotion and bounded subordinate execution with deterministic
models. The implementation status records exact selections and dates. Source moves
require renewed lint, typing, import and acceptance verification; earlier results
are not automatically proof of the reorganized package.

The common component is qualified on two local disposable databases with different
protected domain fixtures (`tests/qualification/two_project/` is the independent
reviewer suite) and was installed into both Supabase projects on 2026-10-03 with
identical fingerprints and clean protected-object comparisons. Remaining gates include
live application traffic (runtime login roles), a verified recovery point, Agent Server
topology/license, live storage policies, operating-system enforced executable mounts
and provider-specific qualification. These are explicit limits, not default
skips hidden behind a green headline.

The fast-track packet added layers to the same ladder. `common_db`-marked tests
(`tests/integration/postgres`, `tests/qualification`, the manifest, chain and
Postgres-parity acceptance suites) need a disposable PostgreSQL 17 with pgvector through
`MISSION_CONTROL_TEST_ADMIN_DSN` and fail, never skip, without it. Temporal replay suites
(`tests/integration/temporal/test_replay_histories.py`, `test_lane_replay_histories.py`) replay
captured histories on the current worker. `make lane-qualify` runs the offline lane suites, and
`LIVE=1` adds the paid Cursor drill, which has not run. `scripts/fast_track_dry_run.py` installs
release 1.1.0 and the seeds on scratch databases and compiles and submits the three fixture
manifests without Temporal. None of these is a live mission; see
[release and qualification](release-and-qualification.md).

## Multi-provider profiles (2026-10-09, waves 0 to 2 integrated)

The packet adds fixture lanes (`tests/fixtures/mp06_lanes.py`), recorded or synthetic provider
fixtures (`tests/unit/frames/fixtures/`, `tests/fixtures/projections/discovery/`) and real-service
modules for the dispatch journal, Human Gates, the socket, the 0032 release and the cluster
guard. Fixtures prove Mission Control's own rules, never provider behavior. Status per profile
(full statement with evidence and blocked requirements:
[release statement](../qualification/release/multi-provider-2026-10.md)):

| Profile | Code on the integrated base | Live-qualified | Account-qualified |
| --- | --- | --- | --- |
| `deep_agents` | lane, production launch, chains, Human Gates; DB/Temporal-tested with deterministic cognition | flag `qualified=True` (WP-CP-040); no live run in this packet | no |
| `cursor_local`, `cursor_cloud` | lanes; offline and fixture DB/Temporal suites | no (drill unrun) | no |
| `claude_agent_sdk`, `codex` | describe stub, projection, frame mapping, auth routes; no harness (MP-07, MP-08 in flight) | no | no |
| `claude_cloud`, `codex_cloud` | describe stub with refusals everywhere; Outcome 3 | no | no |

No `qualified` flag changed. All-provider completion stays open until hosted parity (MP-21),
which is blocked.

Broad Knowledge Services generalization is deferred (the shared contracts are required scope; see [knowledge-services](knowledge-services.md)). Removing a legacy adapter is
a clean-break source decision, not a migration of historical data or a claim that
every future general Mission Control workflow type is implemented.

# Citations

- [PostgreSQL runtime acceptance](../../tests/acceptance/mission_control/test_postgres_runtime_parity.py).
- [Authenticated scoped runtime acceptance](../../tests/acceptance/mission_control/test_authenticated_scoped_runtime.py).
- [Independent two-project qualification](../../tests/qualification/two_project/conftest.py).
- Exact suite results: `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`.
- [Lane qualification](../qualification/lanes/README.md); [dry run](../../scripts/fast_track_dry_run.py).
- [Multi-provider release statement](../qualification/release/multi-provider-2026-10.md);
  [local profile readiness](../qualification/local-profiles/README.md);
  [VALIDATION](../specs/multi-provider-2026-10/VALIDATION.md).
- Changed paths, recovery and removals: `docs/REMOVAL_GUIDE.md`.
