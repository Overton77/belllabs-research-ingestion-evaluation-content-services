---
type: Verification Reference
title: Evidence and qualification gates
description: What deterministic local tests establish and what remains unproved.
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

Remaining gates include the released common schema and compatible production
adapters, live Supabase/storage policies, operating-system enforced executable
mounts and provider-specific qualification. These are explicit limits, not default
skips hidden behind a green headline.

Broad KnowledgeServices generalization is deferred. Removing a legacy adapter is
a clean-break source decision, not a migration of historical data or a claim that
every future general Mission Control workflow type is implemented.

# Citations

- [PostgreSQL runtime acceptance](../../tests/acceptance/mission_control/test_postgres_runtime_parity.py).
- [Authenticated scoped runtime acceptance](../../tests/acceptance/mission_control/test_authenticated_scoped_runtime.py).
- Exact suite results: `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`.
- Changed paths, recovery and removals: `docs/REMOVAL_GUIDE.md`.
