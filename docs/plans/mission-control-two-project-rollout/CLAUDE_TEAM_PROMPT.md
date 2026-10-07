# Copy-paste Claude Code agent team launch prompt

Paste the following instruction into the next Claude Code session. This file is
a handoff; creating it did not start the team or authorize a live change.

---

Work in `C:\Users\Pinda\Proyectos\Biotech\mission-control`. Use an agent team to
implement modular Mission Control migrations/seeding, qualify the existing
StageGraph and GoalDirected write paths, finish REMOVAL_GUIDE, then clean code and
sync documentation. Execute working code and tests, not another planning-only pass.

Read `AGENTS.md`, relevant scoped AGENTS and
`docs/plans/mission-control-two-project-rollout/IMPLEMENTATION_PLAN.md` completely.
Its source inventory is `source-inventory.json` alongside it. Baseline commit is
`f6521c128738b4962f22fb7646882950c0b63d7c`; verify current HEAD/status and refresh
changed facts. Preserve dirty work, `.tmp-cleanup-unit/`,
`.tmp-worker-poll-20261003/`, ignored `app/personal_code/`, unique evidence and data.
Do not restore old user deletions or run git reset/clean.

Critical owner correction: `ai-engineer-db-contract` owns AI Engineer DOMAIN ENTITY
tables. It is not the owner or prerequisite for the reusable Mission Control
component. Create the independent component at
`packages/mission-control-db-contract/`, with one SQL source, generated MC-only
contract, immutable release manifest/digests and a shared tested installer. Convert
`../biotech-postgres-db-contract` into a thin consumer; keep app manifests/bindings
under `deployments/biotech/` and `deployments/ai-engineer/`. Do not run or modify
the AI Engineer entity migration chain. Amend the bounded obsolete ownership
statements in the general specification/issue packet before implementing.

Install identical common `mission_control` and `mission_control_search` schema
releases in `biotech-research-ingestion` (app `biotech`) and `supabase-blue-ocean`
(app `ai-engineer`). Verify project refs, endpoint routes, environments and approved
secret references first: project names are not identity proof. Separate private
`mission_control_runtime` saver/store setup and qualified native Agent Server
persistence from business authority. Reserve `mission_control_agent_server` only
if the pinned server supports it; prove support rather than inventing configuration.
Keep optional `biotech_mission_adapters` separate and Biotech-only. Temporal remains
the sole macro scheduler, outside both application databases.

Use a lead plus at most five teammates with explicit disjoint file ownership:
database/installer/deployment; runtime repositories; catalog/artifacts/seeds;
native persistence/Agent Server; independent qualification/docs. The lead owns
shared contracts, bootstrap, manifests and integration. Exactly ONE deployment
owner mutates databases/storage, sequentially. Do not race on SQL numbering,
shared fixtures or composition files. No commit/push/deploy without authorization.

Follow gates: G0 baseline/inventory -> G1 schema/port/role/seed/transaction contract
freeze -> G2 two-disposable component and adapter proofs -> G3 actual parity ->
G4 approved concrete live plan and sequential project promotion -> G5 cleanup/docs.
Parallelize independent inventory first and disjoint implementations after contracts
freeze. The detailed plan contains the complete write-path matrix and acceptance
tests; produce a method/table/scope/transaction/test map with zero unmapped paths.

Do not just replace `belllabs_control` strings or remove production guards. Map
mission/revision/program/run/activation/attempt, definitions/bindings, commands,
outbox/events, effects/usage, async children, artifacts, checkpoints/forks and
inspection into canonical tables with composite installation/app/tenant scope.
Preserve atomic family admission, immutable digests, generation fencing and
accepted/delivered/applied distinctions. Prove API/worker/callback/relay paths use
the common component with legacy schemas absent and poisoned. No memory, Mongo,
old-schema, filesystem or provider fallback in production.

Reuse checksum-prefix, advisory-lock and fingerprint protections from the existing
installer, removing its wrong source-owner and single-project restrictions. Seed
versioned dependency-closed catalog/binding bundles with atomic seed receipts:
equal digest replays, changed digest conflicts, revocations remain revoked. No
fake usage, evidence, memberships or provider support. Skills are whole immutable
directory bundles. Keep business SQL transactions separate from vendor setup
(pinned LangGraph includes concurrent indexes) and storage reconciliation.

Prove both disposable installs preserve unrelated schema/data fingerprints, roles
and domain fixtures. Then prepare a concrete live plan: exact verified target,
release/seed/plan digests, affected namespaces/roles/policies, protected before
snapshots, recovery point, allowed differences and validation. Obtain approval
before live mutation. Explicit separate approval is required for destructive work,
persistent access expansion, paid runs or unsupported data changes. Never reset
either populated project, copy Auth users, alter entity tables or assume a backup.
Use a quiescent or attributable window for protected-data comparisons. Promote
Biotech, verify fully, then Blue Ocean; stop the second target if the first fails.

Run real PostgreSQL/Temporal tests, both workflow families, pause/resume/waits,
cancellation windows, restart/replay, semantic fork, child admission/settlement,
artifact promotion and native persistence recovery. Re-run lint/typechecks/full
relevant tests, wheel/resource checks and exact schema/seed retry/drift/role tests.
Local deterministic proof is not live two-project or paid-provider proof. No default
skips for required gates. Bound test counts, payloads, retries and query timeouts;
zero metered calls until an explicit finite budget is approved.

Save per-target evidence under `docs/qualification/two-project/<target>/<run-id>/`
as specified by the plan; redact secrets and retain unique recovery evidence.
Complete REMOVAL_GUIDE with exact already-removed, conditional-removal and keep
paths plus evidence/recovery conditions. Sync OKF/AGENTS, README, operator/status
docs and ownership references; validate links and generated contracts. Do not
undertake broad Knowledge Services generalization or the entire expanded backlog.

Continue independent local work when live identity/access is missing. Stop on
unknown target/schema ownership, drift, unsupported native topology, data changes,
access expansion, ambiguous effects or unapproved spend. Finish with exact changes,
applied/not-applied versions and seeds for EACH project, protected-data comparisons,
test results/exclusions, remaining blockers and tested startup instructions. Never
claim production parity while a mandatory live gate remains blocked.
