# Stage 0 — architecture acceptance, baseline reconciliation, and ecosystem qualification

Status: not started  
Mission type: decisions, evidence, disposable spikes, and accepted contracts; do not build production abstractions from unqualified APIs  
Depends on: none

## 1. Mission

Convert the proposed architecture into an accepted, version-qualified implementation contract. Reconcile the current repository baseline, settle or amend D-01 through D-16, and run architecture-invalidating spikes before production contracts or database migrations depend on the ecosystem.

This stage deliberately front-loads facts that could invalidate later work: graph-factory introspection behavior, Agent Server auth/persistence/limits, PostgreSQL operation-journal atomicity, checkpoint compatibility, context reconstruction, async cancellation, reducers, MCP, QuickJS, async subagents, Store isolation, Sandboxes, and middleware composition.

## 2. Permission to clarify or interview

The agent may ask questions or run a structured owner interview before implementation. Recommended interview subjects:

1. Accept standard Agent Server as primary and Managed Deep Agents as non-primary?
2. Confirm Standard/Serverless staging versus Dedicated evaluation posture and intended region/workspace.
3. Choose deployment ownership: CLI-managed or GitHub/UI-managed.
4. Confirm PostgreSQL authority migration for operation claims/attempts/settlements and rollback window.
5. Confirm thread/epoch/fork identity and blue/green compatibility policy.
6. Confirm cross-thread Store posture for scientific versus procedural memory.
7. Confirm PHI/data classification, trace masking, checkpoint encryption, retention, and deletion requirements.
8. Confirm sandbox provider-first choice, egress posture, snapshot retention, and cloud-credential restrictions.
9. Confirm which beta/preview features are desired for initial migration: QuickJS pure transforms, PTC, dynamic subagents, async subagents.
10. Confirm acceptance baseline workflows/datasets and quality, latency, cost, and concurrency thresholds.
11. Confirm whether `biotech-meta` ADR/spec updates are authorized in this stage or will be separately reviewed.

Record decisions durably. Do not infer a platform purchase, destructive reset, production deployment, or preview-feature enablement.

## 3. Required reading and inspection

Read the main index, global handoff rules, traceability matrix, all four architectural documents, nested `AGENTS.md`, and relevant local skills. Inspect at minimum:

- `pyproject.toml`, `uv.lock`, `.env.example`, Docker/Compose files, CI workflows;
- `app/domain/control_plane/`, `app/domain/run_control/`, `app/domain/orchestration/`, `app/domain/operation_execution/`;
- `app/application/orchestration.py`, `coordinator_launch.py`, `coordinator_facade.py`, `operation_execution.py`, and repositories;
- `app/temporal/stagegraph_workflow.py`, `goal_directed_workflow.py`, worker composition, and legacy runtime adapters;
- `app/server.py`, existing API routers, MCP server, tracing, settings, and readiness;
- database migrations and Mongo/Beanie operation models;
- current tests and expected integration-service skips;
- `docs/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md` and `docs/WORKFLOW_IMPLEMENTATION_BINDINGS_PROTOTYPE.md`;
- current official Agent Server, graph rebuild, auth, deployment, Deep Agents, MCP, Sandbox, and evaluation docs.

Reconcile stale documentation against the current code. For example, determine the actual current GoalDirected/StageGraph runtime wiring rather than trusting older caveats.

## 4. Deliverables

### 4.1 Accepted decision package

- Decision record for D-01 through D-16: `accepted`, `amended`, `rejected`, or `deferred`.
- ADRs or accepted decision notes for:
  - domain authority versus Agent Server state;
  - thread, Agent Server run, checkpoint, epoch, fork, and deployment identity;
  - generic StageGraph first;
  - GoalDirected outer graph and independent verifier;
  - standard Agent Server versus Managed Deep Agents;
  - standalone FastAPI/Agent Server coexistence topology;
  - managed checkpointer/Store boundary;
  - graph assembly/rebuild and compatibility;
  - PostgreSQL operation journal;
  - async I/O policy;
  - context and Store policy;
  - delegation modes and feature maturity;
  - canonical naming/identifier grammar;
  - deployment ownership and blue/green rollback.

### 4.2 Reconciled baseline report

Record:

- exact base revision and dirty worktree status;
- Python/runtime/toolchain versions;
- current dependency and lock state;
- full lint/type/test baseline with every failure and skip classified;
- required PostgreSQL, MongoDB, Redis, Temporal, Neo4j, S3, and external-service test topology;
- current domain/application/runtime/API connection map;
- current auth composition and readiness behavior;
- current operation-claim/settlement storage and transactional limitations;
- current trace hooks and possible double-registration risk;
- current deployable package/build context and excluded assets;
- current model/default mismatch and other known baseline drift;
- current secrets/PHI/data-classification exposure review.

Do not “clean up” unrelated failures without scoping them into an accepted baseline issue.

### 4.3 Exact compatibility matrix and lock proposal

Qualify and record exact compatible versions for at least:

- Python;
- `langchain`, `langchain-core`, `langgraph`;
- `deepagents`;
- `langgraph-sdk`, `langgraph-cli[inmem]`, Agent Server/base image API;
- `langchain-mcp-adapters`;
- `langchain-quickjs` / required Deep Agents extra;
- `langsmith[sandbox]`, `langsmith[pytest]` or selected eval packages;
- async PostgreSQL checkpoint/store packages for standalone integration only;
- model-provider integrations enabled by policy.

For each capability record:

```text
package/version
feature/API exercised
stable/beta/preview/private-beta status
known default behavior
required Python/runtime version
Serverless/Dedicated/local support
feature flag
fallback
evidence
```

Do not yet remove legacy dependencies.

### 4.4 Disposable spike suite

Spikes must be isolated, small, and throwaway unless explicitly promoted after review. Produce evidence for all mandatory spikes below.

## 5. Mandatory qualification spikes

### S0-Q01 — graph factory access contexts and cleanup

Prove with the exact pinned packages:

- supported factory signatures and current `ServerRuntime` fields;
- calls for `threads.create_run`, `threads.update`, `threads.read`, and `assistants.read`;
- `execution_runtime` returns the execution variant for `threads.create_run` and
  `None` for read/update/assistant contexts; only the execution variant exposes
  execution-only `context`;
- read/schema/update construction does not create sandboxes, MCP sessions, secrets, reservations, DB mutations, or traces with sensitive payloads;
- async context-manager cleanup occurs on success, interrupt, failure, and cancellation;
- immutable compiled-structure cache contains no secrets, sessions, handles, or tenant data.

Architecture-invalidating failure: no safe way to produce compatible introspection graph shape without resource side effects.

### S0-Q02 — Agent Server application, auth, resources, and platform limits

Prove:

- current `langgraph.json` schema and exact custom auth/http configuration;
- graph import/load and Studio inspection;
- custom route auth, native threads/runs/assistants/Store/crons filters, default-deny gaps, and route collision behavior;
- background runs, stream reconnect, interrupts, state/history, and concurrent-run strategies used by the plan;
- managed checkpointer/Store injection behavior;
- custom Postgres feasibility only as a separate experiment, not assumed topology;
- actual workspace entitlements, Serverless/Dedicated availability, regions, concurrency, cold start, maximum practical run/wait behavior, sandbox access, and quota/limit evidence;
- deployment revision semantics and whether updates preserve/resume old checkpoints as assumed.

### S0-Q03 — PostgreSQL operation transaction and Mongo migration

Prototype one transaction that coordinates:

- operation binding/semantic attempt;
- budget/concurrency reservation;
- effect claim;
- attempt result/usage;
- settlement;
- lifecycle/outbox update.

Inject crashes before and after the external-effect boundary. Prove unique/idempotent identities and reconciliation. Design digest-verified Mongo backfill, source-document lineage, single-authority writes, rollback, RLS, and grants. Reject unrecoverable dual-write.

### S0-Q04 — checkpoint/state compatibility and blue/green routing

Change node/state/reducer/topology versions intentionally. Prove and document:

- what resumes safely;
- what fails and how it fails;
- compatibility manifest checks;
- N threads resume on deployment N after N+1 is live via separate endpoint/ID;
- fork/migrate/fail-safe choices;
- revision metadata alone is not treated as an execution router.

### S0-Q05 — context compaction and deterministic reconstruction

Run repeated compaction/rollover cycles. Measure preservation of:

- protected goal and exact instructions;
- citations and claim/evidence links;
- unresolved contradictions;
- approvals and attempt/budget identities;
- artifact/source digests;
- deletion/retraction tombstones;
- reconstruction assembly digest and drift.

Define thresholds for Stage 5/6. Model-written summaries must remain derived manifests, not replacements.

### S0-Q06 — end-to-end async policy

Prove cancellation, deadline propagation, backpressure, bounded fan-out, event-loop non-blocking, and resource closure across representative DB, model, MCP, sandbox, artifact, Store, and streaming boundaries. Reject hidden event loops, unbounded `gather`, request-owned fire-and-forget tasks, and blocking calls on the loop.

### S0-Q07 — frontier, `Send`, reducers, and parallel subgraphs

Use two roots, a join, bounded concurrency, duplicate replay, and conflicting duplicate results. Prove reducer laws under randomized merge order. Separately test invocation-scoped parallel subgraphs, distinct stateful subgraphs, and repeated use of one stateful subgraph; document namespace constraints.

### S0-Q08 — interrupts, state edits, forks, and concurrent-run strategies

Prove:

- interrupt/resume after process restart;
- node re-execution from the beginning and idempotent pre-interrupt effects;
- parallel interrupt ID maps;
- durable decision lookup rather than trust in resume payload;
- typed `update_state` and privileged `Overwrite` behavior;
- fork as new BellLabs run/thread;
- accepted behavior for `reject`, `enqueue`, and typed interrupt strategies;
- why rollback is not used for governed external effects.

### S0-Q09 — MCP transport and interceptor behavior

Prove Streamable HTTP auth, tool discovery, exact schema comparison, interceptor/wrapper order, timeouts, cancellation, progress/log events, elicitation, explicit persistent sessions, and cleanup. Verify stdio limitations in deployment. Demonstrate fail-closed behavior for missing/extra/drifted tools.

### S0-Q10 — QuickJS and dynamic delegation

Prove exact package/API, `call`/`turn`/`thread` modes, serialization/snapshot size, resource limits, cancellation, traces, PTC, dynamic `task()`, and Serverless support. Attempt approval/authorization/idempotency bypass through PTC and dynamic delegation. Keep PTC/dynamic dispatch disabled if independent wrappers cannot enforce the BellLabs contract.

### S0-Q11 — async subagents

Using the exact preview version, prove ASGI and optional HTTP topology, launch/check/update/cancel/list behavior, dedicated `async_tasks` state, task/thread/run identities, crash/orphan recovery, stale status handling, full task IDs, capacity requirements, parent wait/resume, cancellation settlement, tenant isolation, and feature disablement. Record that preview APIs may change.

### S0-Q12 — Store memory safety

Prove tenant/environment/purpose namespaces, contamination resistance, expiry, deletion, contradiction/retraction behavior, and default denial for scientific claims. Demonstrate that Store data cannot authorize or terminalize.

### S0-Q13 — Sandbox lifecycle and snapshots

Prove create, upload/download, execute/reconnect, limits, egress, secret flow, snapshot/restore, thread scope, timeout, idempotent cleanup, orphan cleanup, usage reporting, and entitlement. Restore must clone and reacquire live resources. Verify no ambient cloud credential or cross-tenant mount.

### S0-Q14 — middleware surface and order

Inspect the actual Deep Agents default stack and prebuilt tools. Prove wrapper nesting/after-hook order, async hooks, call-limit scope, failure propagation, duplicate middleware detection, summarization conflicts, filesystem/search tool collision detection, and the predicted exposure of `task`, async-task tools, `eval`, QuickJS `task()`, and `tools.*`.

### S0-Q15 — tracing, redaction, and evaluation shape

Run representative current and spike flows. Inspect actual root/child trace shapes before writing evaluators. Prove metadata correlation and masking with sentinel secrets and synthetic PHI. Identify offline datasets and one-metric evaluators for later stages without attaching production run rules yet.

### S0-Q16 — build and deployment ownership

Validate local `langgraph dev`; when Docker is available, test minimal `build/up`. Compare CLI and GitHub/UI deployment paths, current beta flags, revision rules, environment/secret handling, and artifact exclusions. Select one ownership path for Stage 8.

## 6. Baseline issue decomposition

Create implementation issues/spec rows, not time-box estimates. At minimum:

- baseline correctness/static-check cleanup;
- dependency qualification and lock;
- each failed or amended spike;
- each accepted ADR/contract publication;
- environment/entitlement blockers;
- deferred optional capability tracks;
- required `biotech-meta` changes, if authorized.

## 7. Verification and evidence gate

Stage 0 passes only when:

- D-01 through D-16 have accepted dispositions;
- the current baseline is reproducible and unexplained skips are eliminated or accepted;
- exact-version import/minimal-execution matrix exists;
- S0-Q01 through S0-Q09 and S0-Q14 through S0-Q16 pass mandatory criteria;
- S0-Q03 transaction/migration direction is accepted;
- context preservation and async cleanup thresholds are accepted;
- each beta/preview capability has a flag and fallback;
- S0-Q10/S0-Q11 may be deferred only as disabled optional tracks;
- no production schema/runtime abstraction relies on an unqualified API;
- no destructive database action occurred;
- the outgoing handoff is accepted.

## 8. Explicit non-goals

- Do not implement the production graph packages.
- Do not apply production database migrations.
- Do not deploy or purchase production resources without explicit authorization.
- Do not remove or freeze the legacy runtime yet.
- Do not convert spike code into permanent architecture without review.

## 9. Outgoing handoff additions

In addition to the global template, include:

- D-01–D-16 disposition table;
- qualified package/runtime/platform matrix;
- spike-by-spike evidence and architecture impact;
- exact enabled/disabled capability matrix;
- accepted deployment ownership and environment topology;
- accepted operation-journal migration/backfill direction;
- accepted context, Store, trace, sandbox, and retention thresholds;
- list of configuration keys that remain “verify against pinned CLI”;
- next-stage schema/contract design inputs.

Stage 1 may start only when mandatory decisions and architecture-invalidating spikes are accepted.

