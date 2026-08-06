# Stage 8 — managed deployment, shadow, canary, cutover, legacy drain, and decommission

Status: not started  
Mission type: authorized external deployment and controlled production migration  
Depends on: accepted Stage 7 and explicit owner authorization for each rollout transition

## 1. Mission

Deploy the exact qualified Agent Server application to the selected LangSmith staging environment, prove platform behavior and rollback, run shadow and canary by exact Workflow Implementation binding, make LangGraph the default for accepted new admissions, drain legacy executions, and remove Temporal/OpenAI Agents execution only after the full rollback window and reconciliation gate.

This stage contains consequential external changes. Owner approvals are required at the staging deployment, production canary, default-routing, legacy-admission stop, and decommission gates.

## 2. Permission to clarify or interview

The agent should conduct a pre-deployment owner interview unless every answer exists in the accepted Stage 7 handoff. Confirm:

- target LangSmith organization/workspaces/projects/regions;
- Serverless versus Dedicated topology and plan entitlement;
- CLI versus GitHub/UI deployment ownership;
- staging/production secret names and data classification;
- Agent Server managed database versus any accepted custom database;
- exact canary Workflow Types/Implementations/tenants and progression;
- shadow effect/cost/data policy;
- SLO/quality/cost/security thresholds and observation windows expressed as evidence requirements rather than implementation estimates;
- rollback endpoint and compatibility strategy;
- backup/restore, retention, legal, and historical evidence requirements;
- who may approve each rollout/decommission transition;
- whether any optional beta/preview capability is allowed in production.

Do not create/update deployments, attach production evaluator rules, route production traffic, stop legacy admission, or delete resources without the corresponding authorization.

## 3. Required inputs

- accepted Stage 7 image/build/config/runbooks and staging requirements;
- exact lock, assembly/state schema digests, route/auth/capability manifests;
- accepted data/secret/retention policies;
- known-good legacy and Agent Server rollback endpoints;
- blue/green compatibility routing implementation;
- shadow/canary comparison tools and effect-claim protection;
- backup/restore and decommission plans.

## 4. Deliverables and controlled transitions

### 4.1 Pre-deployment release gate

Before external mutation:

- reproduce full lint/type/unit/integration/schema/eval/security suite from clean checkout/lock;
- rebuild exact image/artifact and verify manifest/SBOM as accepted;
- apply BellLabs forward migrations through release-job owner credentials in staging only;
- verify runtime uses non-owner credentials;
- confirm no broad `.env`, secret value, local profile, localhost, or host `.tools` dependency;
- confirm enabled feature maturity/fallback matrix;
- confirm database backups and restore identifiers for BellLabs-owned authorities;
- record exact approval and planned rollback.

### 4.2 Staging deployment

Using the one accepted ownership path:

1. create/update staging deployment;
2. wait for deployed status;
3. record endpoint, deployment ID, revision, image/build identity, graph IDs, assembly/schema digests, and secret/config snapshot refs;
4. do not assume revision metadata pins old thread execution;
5. create a separate blue/green endpoint for checkpoint-incompatible code;
6. verify deployment logs/metrics/health before E2E.

Current CLI and platform flags must be rechecked immediately before use. Do not mix UI/GitHub-created deployment update ownership with CLI unless current platform rules explicitly support it and the owner accepts the change.

### 4.3 Authenticated staging qualification

Prove with synthetic/approved data:

- all graph imports and assistants;
- native/custom auth and cross-tenant denial;
- control/run/schema/coordinator APIs and MCP facade;
- prepare/admit/dispatch/stream/reconnect/interrupt/resume/steer/cancel/fork/result;
- managed checkpointer/Store behavior and namespaces;
- longest supported wait/run and scale-to-zero reconnect;
- sandbox/MCP/remote services and egress;
- enabled delegation modes and capacity;
- traces/redaction/offline and authorized online evaluations;
- health/readiness/degradation/alerts;
- backup/restore or export/fork/recovery appropriate to managed and BellLabs-owned data;
- cold start, concurrency, queue, cost, and checkpoint limits against SLOs.

Platform entitlement and capacity must be observed, not assumed from pricing/docs.

### 4.4 Known-good rollback drill

Rehearse:

- routing new admissions to legacy or known-good Agent Server endpoint;
- keeping existing threads on their original blue/green endpoint/ID;
- N-on-N resume after N+1 is live;
- disabling optional features without invalidating stable workflows;
- interrupting/allowing active runs according to policy;
- preserving lifecycle, claims, budgets, decisions, outbox, artifacts, and traces;
- redeploying known-good code when compatible;
- reconciliation after partial deployment/transport failure.

Do not use runtime rollback semantics that delete run/checkpoint evidence for governed work.

### 4.5 Shadow execution

Run identical immutable input/bindings through legacy and LangGraph paths.

Rules:

- one runtime only holds consequential provider-effect claim;
- shadow is passive/captured when a provider cannot guarantee safe duplicate read/cost/effect behavior;
- compare lifecycle schedule, budgets, obligations, result/evidence/artifact refs, usage/cost, errors, waits, interventions, and evaluation;
- account for stochastic semantic tolerance explicitly;
- trace ordering/model retry incidentals are not required to match;
- failures/rejections are part of parity;
- record every mismatch and disposition.

Shadow gate requires accepted parity/security/recovery/cost evidence and zero duplicate consequential effects.

### 4.6 Canary progression

Progress only with explicit gate record:

1. internal deterministic StageGraph implementations;
2. selected schema-grounding StageGraph implementation;
3. agentic StageGraph implementations;
4. bounded GoalDirected stable harness;
5. broader tenant/workflow canary;
6. LangGraph default for accepted new implementation bindings.

Beta/preview QuickJS dynamic or async-subagent tracks need their own production authorization and canary; they are not enabled automatically with GoalDirected.

Each canary records:

- exact Workflow Implementation refs and traffic selection rule;
- endpoint/deployment/assembly binding;
- active run inventory;
- SLO/quality/cost/security/reconciliation evidence;
- incident/mismatch count;
- rollback decision and result;
- next progression approval.

Never use one global runtime flag when exact implementation routing is available.

### 4.7 Default routing and coexistence

When approved:

- new admissions for selected exact bindings use graph outbox dispatcher;
- existing legacy runs stay on legacy runtime;
- existing Agent Server threads stay on their bound endpoint;
- v1 clients remain supported per compatibility commitment;
- coordinator uses shared facade without vendor details;
- deployment revision/assembly facts are persisted in each binding;
- runbooks/alerts/on-call posture are active.

### 4.8 Stop new legacy admissions

After sustained accepted canary evidence and rollback readiness:

- stop new Temporal/OpenAI Agents execution admissions by exact routing policy;
- retain workers/services required to drain existing runs;
- inventory every active/waiting/paused/cancelling legacy run;
- preserve restart/decision/result paths;
- reconcile budgets, effects, decisions, artifacts, and terminal state;
- keep historical traces/evidence read-only.

This is a separate owner-approved transition from making LangGraph default.

### 4.9 Legacy drain ledger

Maintain per-run ledger:

```text
belllabs_run_id
legacy_runtime_identity
phase/wait/decision
budgets_and_pending_usage
effect_claims_and_settlements
artifacts/results
terminal/reconciliation status
retention refs
owner/next action
```

Zero active legacy executions is required for decommission. Do not force-terminalize merely to clear the ledger.

### 4.10 Decommission package

Only after rollback-window, retention, legal, and operations approval:

- remove Temporal worker/service topology used only for agent execution;
- remove OpenAI Agents runtime plugins and legacy trace bridge after proving no other consumers;
- remove legacy-only settings/tests/dependencies in a separately reviewable change;
- keep historical evidence readers for accepted retention;
- update current docs/runbooks without rewriting historical records;
- remove old database schemas/resources only through separately approved backed-up literal-target runbook;
- verify no current client, schedule, linked run, or incident procedure depends on removed behavior.

Do not broadly delete Supabase projects, BellLabs control schemas, Mongo records, artifacts, or audit evidence.

### 4.11 Final operational readiness

Confirm:

- alerts/dashboards and on-call ownership;
- reconciliation jobs and orphan cleanup;
- checkpoint/Store/thread TTL and governed cleanup;
- sandbox cleanup and quota/cost monitoring;
- evaluator/run-rule monitoring and false-positive process;
- dependency/security upgrade and compatibility process;
- periodic backup/restore and N-on-N+1 drills;
- incident communications and rollback authority;
- coordinator-agent MCP readiness handoff.

## 5. Final gate

The migration goal is complete only when:

- staging and production-selected deployments are healthy and exact bindings recorded;
- authenticated/cross-tenant, persistence, longest-run, sandbox/MCP, trace, stream, eval, cost, and cold-start gates pass;
- rollback to known-good endpoint/revision is rehearsed;
- shadow/canary show accepted parity and zero duplicate consequential effects;
- StageGraph and stable GoalDirected become default only by exact accepted implementation binding;
- optional beta/preview capabilities are either separately accepted or remain disabled;
- coordinator can discover, prepare, launch, observe/intervene, and get typed results through shared facade/API/MCP;
- zero active legacy executions remain before decommission;
- all budgets/effects/decisions/artifacts/results/terminal states are reconciled;
- retention/legal/operations approve legacy removal;
- no current client depends on removed behavior;
- historical evidence remains accessible;
- final handoff/runbooks and owner acceptance are recorded.

## 6. Rollback rules

Rollback always means:

- route new admissions to legacy or a known-good Agent Server endpoint;
- keep in-flight runs routed to their original compatible endpoint;
- interrupt/inspect/allow completion per policy;
- disable optional capability flags if implicated;
- reconcile runtime and BellLabs facts;
- preserve every authoritative record and audit/evidence artifact.

Rollback never means deleting BellLabs lifecycle history, claims, budgets, decisions, checkpoints needed for recovery, artifacts, or audit evidence.

## 7. Explicit non-goals

- Do not enable optional features merely because core migration is live.
- Do not move in-flight checkpoints implicitly between incompatible deployments.
- Do not delete legacy data/resources as part of traffic cutover.
- Do not treat a deployment revision value as an execution router.
- Do not declare success while active legacy runs or unsettled effects remain.

## 8. Final outgoing handoff additions

Include:

- deployed environment/endpoint/ID/revision/assembly matrix;
- secret/config snapshot refs without values;
- staging and production smoke/eval/security/SLO evidence;
- shadow and canary progression records;
- exact traffic-routing rules and active-run endpoint bindings;
- rollback drill transcript and known-good refs;
- legacy drain ledger and final zero-active proof;
- decommission changes and retained historical readers;
- backup/restore/retention/legal/operations approvals;
- alerts/runbooks/on-call/reconciliation ownership;
- final accepted capability/maturity/feature-flag matrix;
- owner acceptance that the migration goal is complete.

