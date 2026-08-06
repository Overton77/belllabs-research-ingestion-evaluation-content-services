# Stage 7 — API/coordinator convergence, observability, evaluation, security, and production-like validation

Status: not started  
Mission type: complete product/control surface and release evidence before managed staging  
Depends on: accepted Stages 3–6

## 1. Mission

Make the migrated runtime usable as a coherent BellLabs backend service. Converge v2 control-plane, run-control, schema-grounding, coordinator REST/MCP, and Agent Server surfaces on shared domain/application services and identity. Add complete tracing, offline/online evaluation foundations, security/adversarial coverage, health/operations, fault injection, load/cost measurements, and a reproducible production-like build.

This stage is the final local/pre-staging release gate. It does not cut over production.

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- v2 public/MCP surface and v1 compatibility commitments;
- dashboard/operator needs versus coordinator-only scope;
- Supabase JWT/role/tenant mapping and custom auth policy;
- which coordinator prompts/resources/tools are actually published;
- trace projects/workspaces, retention, PHI masking, and access;
- accepted datasets, quality dimensions, evaluators, thresholds, sampling, and human review;
- family-specific latency, cost, concurrency, checkpoint-size, and recovery SLOs;
- security review scope and remediation severity gate;
- local Docker availability and external test credentials/services;
- whether online evaluators may be attached now or only in staging.

Evaluation criteria requiring product judgment must be owner-selected. Inspect actual traces before implementing field extraction or online rules.

## 3. Required inputs

- shared principal mapper/auth and custom HTTP app;
- Stage 3 runtime/intervention/event services;
- Stage 4/5 typed results and graph runtimes;
- Stage 6 capability/maturity/readiness manifest;
- current v1 APIs, coordinator facade/MCP/resources/prompts, schemas, and dashboard/event behavior;
- existing evaluation fixtures and accepted live proofs;
- accepted security/data/retention policies.

## 4. Deliverables

### 4.1 One shared composition and identity model

REST, coordinator MCP, and Agent Server custom routes must:

- authenticate through one principal-to-BellLabs-authority mapper;
- use the same application services/facades;
- enforce request scope/tenant/role consistently;
- use one BellLabs v2 success/error envelope for BellLabs routes/MCP methods;
- preserve native Agent Server endpoint schemas;
- share correlation/idempotency semantics;
- default-deny missing resource policies.

Fix current deployment principal injection gaps without embedding auth in domain functions.

### 4.2 Control-plane v2

Add read/write/compile surfaces backed by existing services:

- current draft/publish/alias/compile/retire behavior;
- Workflow Type and Implementation search/description/comparison;
- exact published-definition lookup;
- graph/harness/middleware/context/delegation/MCP/interpreter/sandbox/evaluation definitions;
- compact capability/maturity discovery;
- per-stage capability requirement discovery and authoring schemas that expose stable BellLabs capability IDs, constraints, maturity, incompatibilities, resource needs, and fallbacks without exposing provider credentials or runtime handles;
- Workflow Implementation validation that requires complete stage/variant execution bindings and predicts each stage's model-visible/runtime-visible surface;
- complete schema bundle/OpenAPI;
- compile/launch preview showing redacted exact runtime assembly, differences, degradations, approvals, incompatibilities, and digests;
- deployed graph/assembly compatibility validation.

Do not model assistants as Workflow Types or add a second compiler.

### 4.3 Run-control v2

Implement accepted forms of:

- `POST /run-control/v2/run-requests`;
- `POST /runs/{belllabs_run_id}/executions` through transactional outbox;
- `GET /runs/{belllabs_run_id}/runtime` combined projection;
- typed intervention endpoint;
- pending interrupt list and response endpoint;
- resumable event endpoint by BellLabs outbox cursor;
- redacted operator checkpoint summaries;
- typed result endpoint.

Every execution, intervention, and response reuses Stage 1/3 services. No request calls Agent Server directly before BellLabs authority/transaction.

### 4.4 Schema-grounding v2

Preserve governed read-model behavior and existing application services:

- v2 envelope/schema completeness;
- exact execution/operation binding lineage;
- deterministic latest ordering by timestamp plus stable identity;
- run-scoped grounding status/evidence;
- bounded operator diagnostics without raw data/query leakage;
- initiation only through shared workflow prepare/launch path;
- no generic memory mutation or arbitrary Cypher API.

### 4.5 Coordinator facade and MCP convergence

Expose shared REST wrappers and MCP methods for:

- bootstrap/readiness/capabilities;
- Workflow Type-first discovery and exact rehydration;
- design validation;
- prepare immutable launch ticket;
- authorized idempotent launch;
- combined runtime projection and resumable events;
- pending interrupts and allowed interventions;
- typed result/evidence retrieval.

The coordinator graph/agent reasons and proposes; the deterministic facade verifies/effects. Preserve internal-first discovery and quarantine external candidates. Freeze exact refs, maturity snapshot, and assembly digest at preparation. The coordinator never needs raw Agent Server thread/run mechanics, sandbox credentials, MCP auth, reducers, or provider SDK details.

The coordinator and owner-facing authoring flow may intentionally choose different exact models, tools, skills, MCP servers, context policies, specialists, sandboxes, and verifiers for different stages. It must do so through `StageCapabilityRequirement` and published Workflow Implementation definitions; it cannot attach arbitrary runtime capabilities. Preparation compiles the proposal into complete `StageExecutionBinding`/`OperationAssemblySpec` records, shows conflicts/degradations/cost/concurrency projections, and fails closed before launch when any stage is incomplete or unavailable.

### 4.6 v1 coexistence and deprecation evidence

- keep required v1 routes callable;
- bridge current approval/event paths to the single new authority;
- replace Temporal dispatcher only for selected exact implementation bindings;
- record deprecation headers/docs as accepted;
- snapshot request/response behavior and schema compatibility;
- no client is silently routed to a new runtime globally.

### 4.7 Trace taxonomy

Adopt nested traces/spans for:

- BellLabs run or stateless admin operation root;
- workflow cycle/goal iteration;
- stage/operation;
- model;
- tool/MCP;
- synchronous/dynamic/async subagent;
- interpreter;
- sandbox command;
- independent verifier/evaluator.

Safe metadata includes pseudonymous scope, BellLabs run/epoch, Workflow Type/Implementation refs, assembly/binding digests, provider-qualified thread/run IDs, deployment endpoint/ID/revision, semantic attempt, retry layer, snapshot ref, and usage summary.

Mask secrets, headers, tokens, signed URLs, environment, PHI, raw private corpora, sandbox files, and unrestricted tool output. Traces are evidence, not authority.

### 4.8 Dataset and offline evaluation program

Create versioned datasets from actual inspected outputs/traces for:

1. coordinator retrieval/selection/prepare/launch;
2. deterministic contract/routing/schema cases;
3. StageGraph schedule/replay/idempotency trajectories;
   include heterogeneous per-stage capability compilation, measured concurrency, isolation, async wait/resume, and full-lineage cases from `09A`;
4. GoalDirected convergence/revision/handoff/verifier cases;
5. schema-grounding and web-research accepted/rejected cases;
6. MCP schema/auth/error/selection;
7. context compaction/evidence preservation;
8. sandbox/QuickJS containment if enabled;
9. adversarial tenant/injection/approval/tool escalation;
10. citation/scientific-grounding/output quality.

Follow these rules:

- run and inspect actual output/trace shapes before extraction;
- match run-function outputs to dataset schemas;
- one metric per evaluator;
- deterministic code evaluators for exact invariants;
- model judges only for semantic dimensions;
- pin judge prompt/model/schema;
- validate on known good/bad examples;
- inspect raw judge output and uncertainty;
- promotion decisions remain BellLabs policy.

### 4.9 Online evaluation preparation

Design project-attached evaluators only after inspecting production-shaped root traces and interviewing the owner about quality concerns. Record:

- evaluator name/prefix;
- one quality dimension;
- code versus LLM judge;
- trace fields and variable mapping;
- score meaning;
- sampling rate chosen by owner;
- historical test results;
- empty/error/unexpected-shape behavior;
- run rule/attachment procedure and rollback.

Do not silently attach a sampling rule or automatically publish an authoritative definition from a score. If attached in this stage, use authorized non-production project traffic and verify the run rule.

### 4.10 Security review and adversarial suite

Review/test:

- custom auth and every native/custom resource filter;
- PostgreSQL RLS and Mongo scope filters;
- capability compiler and mutable alias denial;
- coordinator attempts to omit a stage binding, select an unqualified model/tool/MCP/skill/child, exceed hierarchical resources, or smuggle provider configuration outside the exact definition;
- prompt/skill/MCP schema injection and tool escalation;
- approval bypass including QuickJS PTC/dynamic paths if enabled;
- effect idempotency and shadow claims;
- secret flow and trace/stream/error redaction;
- Store/context/filesystem/sandbox cross-tenant isolation;
- sandbox egress, mounts, packages, cleanup, malware posture;
- SSRF/endpoint handling for MCP/remote subagents/artifacts;
- checkpoint serialization/encryption/retention/deletion as accepted;
- custom route collision/shadowing;
- supply-chain/lock/build artifact contents.

Zero unresolved critical/high authority, cross-tenant, secret, PHI, approval-bypass, or duplicate-effect findings at gate unless the owner explicitly accepts a risk exception and blocks production promotion as appropriate.

### 4.11 Fault injection, load, cost, and recovery

Inject failures for:

- BellLabs PostgreSQL, MongoDB, artifact service;
- Agent Server transport and stream disconnect;
- MCP timeout/schema drift/session loss;
- sandbox create/execute/snapshot/delete;
- model timeout/rate limit/malformed output;
- checkpointer/Store unavailability where testable;
- process cancellation and deployment restart.

Measure by workflow/operation class:

- cold/warm start;
- max practical duration/wait;
- concurrency/fairness/queue age/backpressure;
- checkpoint/context/trace size;
- model/tool/sandbox usage and cost;
- recovery/reconciliation latency;
- stream reconnect and event lag.

Compare against accepted baselines/thresholds.

### 4.12 Production-like local build

When Docker is available, run accepted equivalents of:

```powershell
uv run langgraph build --config langgraph.json -t belllabs-agent-server:qualification
uv run langgraph up --config langgraph.json --recreate --wait --port 8123
```

Verify:

- reproducible build from lockfile;
- authenticated E2E against containerized server;
- runtime artifact has no tests/scratch/personal code/secrets/host `.tools` unless deliberately included;
- no localhost-only or local AWS profile dependency;
- release migrations are separate;
- non-owner runtime DB roles;
- production-like persistence/interrupt/stream/sandbox/MCP behavior.

If Docker is unavailable, the stage remains blocked on the production-like gate; `langgraph dev` is insufficient for Stage 8 entry.

### 4.13 Operations/runbooks

Produce runbooks for:

- deployment/revision health;
- stuck run and reconciliation;
- provider/API outage;
- DB failover;
- lost stream;
- stale interrupt;
- orphan async task/sandbox;
- incompatible checkpoint;
- trace/evaluator outage;
- backup/restore and managed thread export/fork/recovery;
- known-good rollback.

Alerts cover queue age, run errors/timeouts, checkpoint failures, interrupt age, retry storms, sandbox leaks, DB saturation, outbox lag, reconciliation backlog, redaction/evaluator failure, and cost thresholds.

## 5. Gate

Stage 7 passes when:

- discover/prepare/launch/stream/interrupt/resume/steer/cancel/fork/result succeeds end to end;
- REST and MCP use equivalent data/error semantics and one facade/identity implementation;
- v1 coexistence is verified and no launch bypasses compilation/admission/binding;
- all route/resource/RLS/Store/sandbox tenant tests pass;
- trace taxonomy and sentinel redaction pass;
- offline datasets/evaluators meet accepted deterministic and quality thresholds;
- online evaluator plans/rules are owner-approved and verified where attached;
- fault, load, cost, checkpoint-size, and recovery thresholds pass;
- security review has no disallowed unresolved findings;
- production-like lockfile build and authenticated E2E pass;
- backup/restore and runtime recovery/blue-green drills required before staging are complete;
- outgoing handoff is accepted.

## 6. Explicit non-goals

- Do not route broad production traffic.
- Do not let evaluators publish definitions or terminalize runs.
- Do not remove v1/legacy paths yet.
- Do not deploy staging until Stage 8 authorization.
- Do not waive production-like Docker/build evidence silently.

## 7. Outgoing handoff additions

Include:

- final v1/v2/native/MCP route and schema inventory;
- auth/resource/RLS coverage;
- coordinator capability and launch-ticket schema/digest evidence;
- E2E operator/coordinator transcript with safe refs;
- trace taxonomy/projects/masking tests;
- dataset/evaluator registry, thresholds, experiment results, and online sampling decisions;
- security findings/remediations/exceptions;
- fault/load/cost/SLO measurements;
- production-like image/config/artifact manifest and E2E evidence;
- operational/backup/recovery/rollback runbooks;
- exact staging environment and secret-name requirements.
