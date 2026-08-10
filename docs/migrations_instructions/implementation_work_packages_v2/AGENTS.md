# v2 work-package agent instructions

These instructions apply to work under this directory and to implementations authorized by its
active `WP-*` documents.

## Read before editing

1. Read the owning canonical `SPEC-*`, `REQ-*`, and `CON-*` in `../../../../biotech-meta`.
2. Read `README.md`, `IMPLEMENTATION_READINESS.md`, `SUPPLEMENT_CODEBASE_ORGANIZATION.md`, and the
   complete active WP plus every dependency.
3. Read accepted dependency evidence under `../evidence_v2/`.
4. For parallel blueprint work, read `PARALLEL_WORKTREE_PROTOCOL.md` and record the kickoff fields
   it requires.
5. Inspect as-built code and tests last. Historical Stage 0–8 packages and experiments are prior
   art only and cannot authorize behavior.

## WP-BP-010 / WP-BP-020 kickoff

- Both branches must name the same accepted `WP-CP-045` base revision.
- Work only inside the active WP's exclusive paths and named shared-file regions.
- Treat run control, generic operation execution, async delegation, Deep Agent materialization,
  root workflow, and registries as frozen integrator-owned foundation.
- Do not implement sibling-family semantics, opportunistic cleanup, bulk file moves, or whole-file
  formatting in shared contract modules.
- If a required foundation change appears, stop and publish an amendment/handoff; do not hide it in
  a family branch.

## Runtime proof is mandatory

Deterministic unit/property, contract, API integration, Temporal replay/recovery, and offline
regression tests are mandatory. Each BP also requires its credential-gated real-LLM acceptance
vertical through:

```text
BellLabs API -> transactional admission -> BellLabsRunWorkflow ->
family workflow -> OperationWorkflow -> Deep Agents adapter -> real LLM
```

StageGraph evidence must prove branching incremental release. GoalDirected evidence must prove
separate executor/verifier operations and convergence or revision behavior. Provider success alone
is not semantic proof. Record the reducer-authorized terminal outcome.

Use a minimal exact binding. Do not expand these WPs to materialize the complete Skills, MCP,
sandbox, snapshot, or combined capability vertical; that is `WP-CP-050`.

## Handoff and acceptance

Preserve unrelated dirty changes. Record base/head revisions, owned and changed paths, test
commands, sanitized outputs, replay artifacts, live run/workflow/binding identities, risks, deletion
checks, and an explicit disposition. Follow `../evidence_v2/README.md`.
