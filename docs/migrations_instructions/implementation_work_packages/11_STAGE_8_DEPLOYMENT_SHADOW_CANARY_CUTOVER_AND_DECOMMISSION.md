# Stage 8 — AWS deployment, shadow, canary, cutover, drain, and evidence-based decommission

Status: `NOT_STARTED`
Document role: normative Stage 8 deployment, migration, and decommission package
Mission type: authorized production-shaped deployment and controlled production migration
Depends on: accepted Stage 7 and explicit approval at every consequential transition

## 1. Mission and fixed production direction

The initial production path uses self-hosted Temporal on AWS. This is the current cost decision and is the baseline that Stage 8 must deploy and qualify.

Stage 8 selects the exact AWS compute topology—ECS, EKS, EC2, or an explicitly justified combination—from measured Stage 7 and production-shaped Stage 8 evidence. Kubernetes is not a prerequisite and must not be mandated before the measurements and operational trade-offs support it.

Deploy and qualify:

- the modular BellLabs API/control service as the sole governed external REST/MCP facade;
- a self-hosted Temporal service;
- five isolated Temporal worker pools;
- BellLabs application PostgreSQL as application authority;
- a separate Temporal persistence database and credentials;
- the existing Mongo/content-addressed catalog where the accepted design assigns it authority;
- object storage for artifacts/evidence and large payloads;
- LangSmith tracing, evaluation, sandboxes, and selected graph deployments;
- provider-neutral sandbox adapters for LangSmith, Daytona, and custom containers.

Then compare the legacy direct-activity/OpenAI execution paths with the new Temporal child-workflow plus bounded agent-operation architecture, canary exact implementations, cut over only through the BellLabs API, drain obsolete paths, and decommission only what the evidence proves is no longer required.

This stage contains external and potentially irreversible changes. Separate owner approvals are required for production-shaped infrastructure creation, shadow with production data/effects, production canary, default routing, stopping legacy admission, and each decommission package.

## 2. Non-negotiable boundaries

### 2.1 Sole external facade

All production admissions, commands, status/reconnect, interventions, results, evidence, and catalog/compile operations traverse the BellLabs API/control REST/MCP facade.

Do not:

- expose Temporal frontend/API credentials or arbitrary workflow operations to product clients;
- expose Agent Server native thread/run/checkpoint APIs as a product/control surface;
- route coordinators, callbacks, operators, or legacy clients directly to providers;
- permit deployment routing to bypass compile, admission, authority, or transactional outbox;
- use a global runtime flag when exact Workflow Implementation bindings can select a path.

### 2.2 Separate persistence authorities

Temporal persistence and BellLabs application PostgreSQL must use separate databases or separately isolated clusters, separate credentials, separate migration ownership, and separate backup/restore procedures.

The Temporal service account has no read/write access to BellLabs application schemas. BellLabs runtime accounts have no direct write access to Temporal persistence tables. Neither application code nor operators may query or mutate Temporal persistence as a substitute for supported Temporal APIs.

BellLabs application PostgreSQL remains authority for admission, lifecycle facts, inbox/outbox, commands, callbacks, projections, budgets, claims, and reconciliation. Temporal history is durable execution state, not BellLabs product authority.

### 2.3 Internal operation runtimes

Retain internal Agent Server/LangGraph for accepted bounded agent operations. LangSmith remains the selected tracing/evaluation service and may provide sandboxes and selected graph deployments where the compiled binding requires them.

Agent Server is not a duplicate macro scheduler. Temporal owns durable macro orchestration; BellLabs owns governance and product state.

## 3. Required inputs and approvals

Required inputs:

- accepted Stage 7 images, SBOMs, contracts, security report, evaluation results, failure evidence, and runbooks;
- exact workflow, activity, event, assembly, state, and artifact schema digests;
- five worker-pool task-queue, privilege, scaling, timeout, retry, heartbeat, and compatibility requirements;
- measured CPU, memory, I/O, connection, queue, latency, throughput, and cost envelopes;
- accepted RPO/RTO, multi-AZ, backup, retention, encryption, and legal requirements;
- legacy direct-activity and OpenAI path inventory;
- shadow comparison and single-effect-claim mechanism;
- replay corpus and worker-build compatibility matrix;
- known-good old-worker images and rollback routing.

Before infrastructure mutation, record:

- AWS account/region/VPC/subnet/security-boundary ownership;
- target availability model and accepted failure domains;
- exact staging/production data classification and secret names;
- topology selection criteria and decision owner;
- LangSmith projects, selected graph deployments, evaluator rules, and retention;
- sandbox providers allowed per data class and their egress/storage policy;
- shadow/canary populations, observation windows, SLO/quality/cost/security thresholds, and stop conditions;
- backup/restore and disaster-recovery approval owners;
- rollback window and decommission approvers.

Do not infer production entitlement, quotas, capacity, or cost from documentation alone. Measure them.

## 4. Evidence-based AWS topology selection

### 4.1 Candidate evaluation

Evaluate ECS, EKS, EC2, and reasonable combinations against measured requirements for:

- Temporal service topology and supported operational model;
- API/control service scaling and deployment isolation;
- long-polling Temporal worker behavior;
- five worker pools with independent CPU/memory/GPU/network profiles;
- predictable task-queue pollers and graceful worker drain;
- Agent Server and selected graph-deployment connectivity;
- sandbox adapter and custom-container isolation needs;
- multi-AZ placement, recovery, upgrades, observability, and on-call complexity;
- connection counts and latency to both PostgreSQL authorities, Mongo, object storage, and providers;
- steady-state, burst, data-transfer, NAT, load-balancer, database, observability, and operator cost.

Use load and failure measurements, not platform preference. A mixed result is acceptable when it has a simpler operational proof than forcing all workloads onto one orchestrator.

### 4.2 Topology decision record gate

Before production-shaped deployment, publish an approved decision record containing:

- candidate configurations and comparable measurements;
- selected topology and rejected alternatives;
- capacity and scaling assumptions with confidence ranges;
- failure domains, network paths, service identities, and data stores;
- deployment, drain, upgrade, rollback, and disaster-recovery procedures;
- monthly baseline and tested-load cost;
- unresolved risks and explicit triggers for later topology change.

Gate passes only when the selected topology satisfies accepted SLO, security, recovery, operability, and cost thresholds. No Kubernetes-specific deliverable is required unless EKS wins this gate.

## 5. Deployment topology and isolation

### 5.1 Modular BellLabs API/control service

Deploy one governed external service boundary with the Stage 7 modules:

- catalog/compile;
- run control/admission;
- command/message inbox/outbox;
- evidence/artifacts;
- projections/events;
- Temporal launch/command bridge;
- provider callback ingress;
- provider gateways.

Modules may scale as separate processes where measured need justifies it, but they use the same public contract, authorization/policy layer, BellLabs authority, and no-bypass invariant.

### 5.2 Self-hosted Temporal

Deploy supported self-hosted Temporal components with:

- private network access;
- separate persistence DB/cluster, credentials, encryption, backups, and migrations;
- namespace, retention, archival, search-attribute, and visibility decisions recorded;
- authenticated service identities and restricted operator access;
- health, service, persistence, namespace, task-queue, poller, history, and archival observability;
- controlled version upgrade and rollback procedure;
- multi-AZ/DR posture that meets the accepted RPO/RTO.

Do not treat an API health endpoint as sufficient readiness. Required Temporal services, persistence, namespaces, task queues, and pollers must all be verified.

### 5.3 Five worker pools

Deploy independently scalable pools for:

1. coordinator workflows/activities;
2. bounded agent operations;
3. ingestion and I/O;
4. sandbox control;
5. verification and reconciliation.

Each pool has dedicated task queues, least-privilege service identity, resource and concurrency limits, build identity, deployment/drain policy, alerts, and cost attribution. One saturated, failed, or credential-revoked pool must not starve another.

### 5.4 Data and evidence services

Deploy or connect:

- BellLabs application PostgreSQL with non-owner runtime roles and separate release migration role;
- existing Mongo/content-addressed catalog only for responsibilities already assigned by accepted design;
- object storage for immutable artifacts, evidence, snapshots, and large payloads;
- digest verification, scoped access, retention, legal hold, cleanup, backup, and restore;
- reconciliation that detects missing metadata, missing objects, digest mismatch, and orphaned content.

Do not duplicate an authority merely because another store is deployed.

### 5.5 LangSmith and graph deployments

Configure LangSmith tracing and evaluation for every canary and production path. Configure selected LangSmith graph deployments only for exact compiled operation bindings that need them.

Record workspace/project/deployment/revision/graph identities in diagnostic metadata, while keeping BellLabs events authoritative. Trace/evaluator outage must degrade diagnostics in a controlled way and must not create an ungoverned admission path.

### 5.6 Provider-neutral sandbox adapters

Expose one internal sandbox contract with adapters for:

- LangSmith sandboxes;
- Daytona;
- custom containers on the selected AWS compute topology.

The compiled binding chooses the allowed provider. The adapter contract must normalize create, readiness, execute/job, stream, artifact/snapshot, cancel, timeout, delete, usage, and error behavior.

Every adapter enforces tenant isolation, data class, image/package policy, egress, mount/secret policy, quotas, idempotency, cleanup, retention, and correlated sandbox/job IDs. Provider-specific credentials and handles remain internal.

## 6. Pre-deployment release gate

Before creating or updating production-shaped resources:

- reproduce lint, type, unit, integration, contract, replay, evaluation, and security suites from a clean lock;
- rebuild and verify exact images, manifests, signatures, and SBOMs;
- verify no broad `.env`, secret values, local profiles, localhost dependencies, or undeclared host tools;
- verify runtime uses non-owner and least-privilege credentials;
- verify BellLabs and Temporal database migrations are separate release operations with separate credentials;
- verify backup locations, encryption keys, restore procedures, and test identifiers;
- verify internal-only network policy for Temporal and Agent Server;
- verify all public REST/MCP launch paths enter the BellLabs facade;
- record approved topology, capacity, cost budget, rollout plan, and rollback plan.

Failure of any item blocks deployment.

## 7. Production-shaped qualification

### 7.1 Functional and security qualification

With synthetic or explicitly approved data, prove:

- catalog/compile/prepare/admit/launch through BellLabs REST and MCP;
- command inbox/outbox and Temporal launch/Signal bridge;
- callback authentication, persist/dedupe-before-Signal, duplicate handling, and delayed delivery;
- BellLabs durable status/event stream, cursor reconnect, projection rebuild, and diagnostic links;
- pause/resume/steer/cancel/fork/decision/result/evidence paths;
- cross-tenant denial and internal Temporal/Agent Server access denial;
- all five worker-pool queue and privilege boundaries;
- Agent Server bounded operation and checkpoint behavior;
- all sandbox adapters, egress rules, cleanup, and artifacts;
- LangSmith traces, offline evaluators, authorized online evaluators, and redaction;
- application DB, Temporal DB, Mongo, and object-store backup/restore;
- reconciliation of callbacks, outbox records, workflows, agent runs, sandboxes/jobs, artifacts, and BellLabs runs.

### 7.2 Performance, queue, and cost qualification

Measure against accepted thresholds:

- cold/warm API admission and projection reads;
- schedule-to-start and queue age for every worker pool;
- Temporal service and persistence latency, throughput, connections, and storage growth;
- longest supported run, wait, heartbeat interval, history growth, continue-as-new, and reconnect;
- worker concurrency, fairness, backpressure, scaling, and graceful drain;
- Agent Server checkpoint size and operation latency;
- sandbox startup, execution, cleanup, leak rate, and provider-specific cost;
- model/tool/MCP usage and rate-limit behavior;
- BellLabs event and callback persistence-to-Signal lag;
- tracing/evaluation ingestion lag and cost;
- full AWS and third-party cost by service, workflow family, operation class, and tenant where required.

## 8. Replay and worker compatibility gates

### 8.1 Replay corpus

Replay representative histories for:

- every root workflow family and operation child-workflow family;
- success, rejection, cancellation, timeout, callback wait, intervention, retry, and continue-as-new;
- all five worker pools where workflow code is present;
- histories containing legacy direct activities and new child-workflow/bounded-operation paths;
- long-lived histories spanning at least one worker deployment change.

Any nondeterminism blocks rollout until the workflow code or versioning strategy is corrected and the corpus passes.

### 8.2 N/N+1 compatibility

Prove:

- N histories replay under compatible N+1 workers;
- N workers can continue their assigned in-flight histories while N+1 accepts selected new work;
- N+1 does not poll task queues/build sets that would strand incompatible histories;
- worker build/version routing, task queues, and deployment metadata preserve the correct code for each history;
- rollback to N routes new compatible admissions appropriately while existing N+1 histories remain on compatible workers or follow an explicitly rehearsed recovery;
- old compatible worker images, configuration, and secrets remain available for the full rollback/history window.

Do not remove old workers merely because canary traffic is healthy.

## 9. Hours-long injected-failure qualification

Run repeated, hours-long fault campaigns in production-shaped infrastructure. A short smoke fault is insufficient.

Each campaign includes realistic mixed workload, long waits, callbacks, agent operations, sandbox jobs, projection consumers, and deploy/drain events. Repeatedly inject:

- individual and combined Temporal service restarts;
- Temporal persistence latency, failover, connection exhaustion, and restore rehearsal;
- missing pollers and increasing backlog for each of the five pools;
- worker crash, network partition, deployment drain, incompatible build, and heartbeat loss;
- API/control, inbox/outbox dispatcher, callback ingress, and projection-consumer restarts;
- callback duplicates, delay, reordering, invalid authentication, and Signal outage after fact persistence;
- Agent Server transport/checkpoint failures;
- provider timeout, throttling, malformed response, and uncertain effect;
- LangSmith trace/evaluator outage;
- sandbox create/execute/snapshot/delete failure and leaked job/container;
- BellLabs PostgreSQL, Mongo, and object-store degradation;
- orphan workflows, operations, callbacks, sandboxes/jobs, artifacts, and BellLabs nonterminal runs.

For every campaign, record workload, fault timing, detection, alerts, product-visible behavior, unaffected pools, duplicate effects, data loss, queue/event lag, operator actions, automatic reconciliation, recovery time, RPO/RTO result, and cost.

The gate requires:

- zero unauthorized or duplicate consequential effects;
- zero loss of committed BellLabs facts;
- successful product reconnect from BellLabs event cursor;
- no starvation outside the targeted failure domain beyond threshold;
- all orphans repaired or explicitly escalated within threshold;
- accepted RPO/RTO, backlog recovery, SLO, and cost results;
- at least one successful repeat after the last material remediation.

## 10. Shadow comparison

Run the same immutable Workflow Implementation, inputs, policy, and assembly digests through:

- legacy direct-activity and/or OpenAI execution path;
- new Temporal child-workflow plus bounded agent-operation path.

Only one path may hold a consequential provider-effect claim. Use passive/captured shadowing when duplicate reads, cost, external writes, emails, tickets, mutations, or other effects cannot be made safe.

Compare:

- compile and admission decision;
- lifecycle stages/segments and durable event sequence;
- child-workflow and bounded-operation outcomes;
- waits, callbacks, interventions, retries, cancellation, and recovery;
- budgets, usage, cost, effect claims, and settlements;
- typed result, evidence, artifact refs/digests, citations, and verifier/evaluator scores;
- failure/rejection behavior, projection/reconnect behavior, and orphan reconciliation;
- latency, queue age, worker/sandbox utilization, and operational load.

Define semantic tolerance for stochastic output before the run. Trace ordering, provider IDs, and incidental retry timing need not match unless contractually relevant.

Shadow gate passes only when:

- the approved sample and duration are complete;
- deterministic invariants match exactly;
- semantic quality, latency, recovery, and cost meet thresholds;
- every mismatch has an owner and accepted disposition;
- security/redaction checks pass;
- duplicate consequential effect count is zero.

## 11. Canary and cutover gates

### 11.1 Canary progression

Progress by exact Workflow Implementation binding, tenant/population rule, and immutable assembly digest:

1. internal deterministic workflows;
2. selected ingestion/schema-grounding workflows;
3. bounded agent-operation workflows;
4. representative callback, sandbox, and long-wait workflows;
5. broader tenant/workflow canary;
6. new architecture as default for approved exact bindings.

Each step has a recorded sample, duration, SLO/quality/security/cost thresholds, active-run inventory, incidents, reconciliation state, rollback result, and explicit approval for the next step.

Optional beta/preview capabilities require separate authorization and canary. They do not become enabled because the core path passes.

### 11.2 BellLabs-API-only cutover

Cutover changes routing inside governed BellLabs compile/admission and outbox dispatch. Clients continue using the same BellLabs REST/MCP facade.

The cutover gate requires tests proving:

- selected new admissions resolve to the new Temporal child-workflow/bounded-operation binding;
- unselected bindings remain on their approved path;
- no direct Temporal, Agent Server, OpenAI, provider, callback, or operator admission endpoint exists;
- in-flight runs stay bound to compatible workflow/worker/operation implementations;
- BellLabs events remain the status/reconnect source;
- rollback routing is ready and old compatible workers are healthy;
- alerts, on-call, reconciliation, backups, and cost controls are active.

### 11.3 Stop obsolete admissions

Stopping new legacy admissions is a separate approval after sustained canary evidence. Disable only the obsolete direct-activity/OpenAI/duplicate-macro-scheduler bindings by exact policy.

Retain all services and compatible workers required to finish or recover existing histories. Do not stop Temporal; Temporal is the new durable macro-orchestration foundation.

## 12. Rollback

Rollback is binding- and history-aware:

- route new admissions through BellLabs API to the last known-good compatible binding;
- keep each in-flight Temporal history on a compatible worker build/task queue;
- keep each bounded Agent Server operation on its compatible deployment/checkpoint path;
- retain N workers while N+1 histories and the rollback window require them;
- disable implicated optional capabilities without mutating unrelated histories;
- preserve BellLabs lifecycle facts, inbox/outbox, callbacks, claims, budgets, decisions, artifacts, results, traces, Temporal histories, and required checkpoints;
- reconcile partial launch/command/callback/provider outcomes before retrying effects.

Rollback never means:

- directing clients around the BellLabs API;
- deleting or editing Temporal persistence;
- moving incompatible histories/checkpoints to arbitrary old code;
- force-terminalizing runs to simplify operations;
- deleting evidence or authority records.

A rollback drill must demonstrate both N-on-N and mixed N/N+1 operation before production default routing.

## 13. Drain ledger

Maintain a durable ledger for every run on an obsolete execution path:

```text
belllabs_run_id and epoch
exact Workflow Implementation and assembly digest
legacy direct-activity/OpenAI/macro-scheduler identity
Temporal workflow/run and compatible worker build
Agent Server thread/run/checkpoint where applicable
phase, wait, callback, command, and decision state
budgets, usage, pending effects, and settlements
artifacts, evidence, and typed result
terminal and reconciliation state
retention and rollback dependencies
owner and next action
```

Do not force-terminalize to clear the ledger. Every active, waiting, paused, cancelling, callback-waiting, or uncertain-effect run must finish, be safely recovered, or receive an explicit retained-runtime plan.

## 14. Evidence-based decommission

### 14.1 Remove only obsolete frontiers

After the approved rollback window, zero-required-consumer proof, drain evidence, backup, retention/legal review, and operations approval, decommission:

- obsolete direct-activity execution frontiers superseded by child workflows and bounded operations;
- obsolete OpenAI-specific adapters/plugins and legacy trace bridges with no remaining consumer;
- duplicate Agent Server macro schedulers, public admission surfaces, and product streaming/control routes;
- legacy-only queues, settings, dependencies, tests, schemas, and resources through separately reviewable packages.

### 14.2 Explicitly retain

Retain:

- self-hosted Temporal service and its persistence, workers, histories, visibility, backup, and recovery capability;
- modular BellLabs API/control service and BellLabs application authority;
- internal Agent Server/LangGraph bounded-operation runtime;
- LangSmith tracing, evaluation, sandboxes, and accepted selected graph deployments;
- provider-neutral sandbox adapter contract and approved providers;
- historical evidence readers and records for accepted retention;
- old compatible worker images/configuration as long as any history or rollback requirement depends on them.

### 14.3 Testable decommission gate

Each removal package must prove:

- zero active or retained history requires the target;
- zero catalog binding, admission rule, task queue, callback route, schedule, client, runbook, alert, or incident procedure references it;
- all claims, budgets, callbacks, decisions, artifacts, results, and terminal facts are reconciled;
- required data is backed up and restore-tested;
- replacement SLO/security/cost gates remain green after removal;
- rollback no longer depends on the target, or the target remains retained;
- post-removal synthetic admission, long-run, callback, reconnect, recovery, and replay tests pass.

Do not broadly delete BellLabs PostgreSQL data, Temporal persistence, Mongo records, object storage, LangSmith evidence, checkpoints required by live histories, or audit records.

## 15. Final production gate

The migration is complete only when all of the following are evidenced and approved:

- the AWS topology decision is based on measured ECS/EKS/EC2 evidence and meets SLO, security, recovery, operability, and cost thresholds;
- self-hosted Temporal and its separate persistence DB/credentials pass health, backup/restore, DR, upgrade, and restricted-access tests;
- the modular BellLabs API/control service is the only governed external REST/MCP facade;
- all five worker pools pass isolation, saturation, poller-loss, drain, build-compatibility, and recovery tests;
- BellLabs application PostgreSQL, accepted Mongo/catalog, object storage, LangSmith, Agent Server, and every enabled sandbox adapter pass their authority and integration gates;
- replay corpus and N/N+1 worker compatibility pass;
- repeated hours-long injected-failure campaigns pass after the final material remediation;
- shadow comparison of legacy direct-activity/OpenAI and new child-workflow/bounded-operation paths meets deterministic, semantic, security, recovery, and cost thresholds with zero duplicate consequential effects;
- canary progression and BellLabs-API-only cutover gates pass for each exact binding;
- old workers remain available for every history and rollback dependency;
- obsolete-path admissions are stopped only after separate approval;
- drain ledger has no unresolved run/effect requiring a removed component;
- each decommission package passes its zero-consumer and post-removal tests;
- internal Agent Server/LangGraph operation runtime and LangSmith services remain available for accepted roles;
- final runbooks, on-call ownership, dashboards/alerts, cost controls, backups, reconciliation, security acceptance, and owner acceptance are recorded.

Any failed criterion leaves the relevant rollout or decommission transition blocked. Traffic percentage, elapsed time, or lack of observed incidents cannot substitute for the required test evidence.

## 16. Explicit non-goals

- Do not prematurely mandate Kubernetes.
- Do not combine Temporal persistence credentials or schemas with BellLabs application PostgreSQL authority.
- Do not expose Temporal or Agent Server as a second public facade.
- Do not cut over by giving clients a new provider/runtime endpoint.
- Do not decommission Temporal; it is the accepted durable orchestration foundation.
- Do not remove internal Agent Server/LangGraph bounded-operation runtime or required LangSmith services.
- Do not move incompatible histories/checkpoints between worker or Agent Server builds.
- Do not delete legacy data merely because new admissions have moved.
- Do not declare success while unresolved effects, active dependencies, failed replay, missing old workers, or untested rollback remain.

## 17. Final outgoing handoff

Include:

- approved AWS topology decision and measured ECS/EKS/EC2 comparison;
- deployed service/network/identity/data-flow diagram and environment manifest;
- BellLabs and Temporal database isolation, migration, backup/restore, and DR evidence;
- modular API, Temporal service, five worker-pool, Agent Server, LangSmith, Mongo/catalog, object-store, and sandbox-adapter deployment matrix;
- exact image/build/task-queue/worker-version/assembly/schema digests;
- replay corpus and N/N+1 compatibility results;
- hours-long failure campaign reports and final repeat evidence;
- shadow datasets, mismatch dispositions, effect-claim proof, and accepted thresholds;
- canary records, exact routing rules, BellLabs-API-only cutover proof, and rollback drills;
- active/legacy drain ledger and zero-required-consumer evidence;
- decommission packages and retained-component inventory;
- security/redaction, evaluation, SLO, queue, recovery, and cost evidence;
- alerts, dashboards, on-call, reconciliation, backup, retention/legal, and owner approvals.

