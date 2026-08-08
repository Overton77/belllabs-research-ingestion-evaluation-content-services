# Stage 7 — governed API/control facade, observability, evaluation, security, and production-shaped qualification

Status: `NOT_STARTED`
Document role: normative Stage 7 implementation and production-qualification package
Mission type: finish the governed product/control boundary and produce Stage 8 deployment evidence
Depends on: accepted Stages 3–5 and aggregate Stage 6 acceptance recorded only after internal `09A` proof passes

## 1. Mission and architectural invariant

Implement and qualify one modular BellLabs API/control service as the sole governed external REST and MCP facade. All product clients, operators, coordinators, and automation enter through this service for catalog operations, compilation, admission, launch, command, observation, evidence, and results.

The API/control service owns the public contract and delegates durable execution to Temporal. Temporal APIs and LangGraph Agent Server APIs remain internal/restricted implementation surfaces. They are not alternative product entry points.

The non-negotiable admission path is:

```text
external REST/MCP caller
  -> BellLabs authentication and authorization
  -> catalog resolution and deterministic compile
  -> policy/admission and idempotency transaction
  -> BellLabs run facts plus transactional outbox
  -> internal Temporal launch/command bridge
  -> Temporal workflow and bounded operation runtimes
```

No client, coordinator, callback handler, provider gateway, Agent Server route, or operational tool may bypass BellLabs compilation, admission, authority, or outbox dispatch.

This stage is the final pre-production qualification gate. It does not route broad production traffic.

## 2. Required inputs and owner decisions

Required inputs:

- accepted BellLabs authority, run/event, intervention, artifact, and projection services from earlier stages;
- accepted Workflow Type, Workflow Implementation, `StageExecutionBinding`, and `OperationAssemblySpec` contracts;
- accepted Temporal workflow families, task queues, retry/timeout policies, and reconciliation design;
- existing REST/MCP compatibility commitments and product reconnect behavior;
- accepted data classification, tenancy, retention, encryption, and redaction policies;
- representative workflows and known-good/known-bad evaluation fixtures.

Before implementation, record owner decisions for:

- external REST/MCP compatibility and deprecation commitments;
- principal, tenant, role, service-account, and operator authorization policy;
- public event retention and reconnect limits;
- SLOs for admission, launch, command acknowledgement, event lag, recovery, queue age, and cost;
- LangSmith workspaces/projects, evaluator thresholds, sampling, retention, and human-review policy;
- security severity gate and any explicit risk-exception process.

Quality criteria that require product judgment must be owner-selected. Inspect real production-shaped traces and outputs before fixing evaluator field mappings or thresholds.

## 3. Modular BellLabs API/control service

The service is one deployable control boundary with explicit internal modules. Modules may have separate packages and scaling characteristics, but they share one public facade, identity model, policy model, authority, and error semantics.

### 3.1 Catalog and compile module

Provide governed REST/MCP operations for:

- Workflow Type and Workflow Implementation discovery, exact-version retrieval, comparison, draft, publish, alias, retire, and compatibility checks;
- capability/maturity discovery without exposing provider credentials or runtime handles;
- complete per-stage execution-binding validation;
- deterministic compilation of exact graph, harness, model, tool, skill, MCP, context, delegation, sandbox, verifier, and evaluation choices;
- redacted launch preview with assembly/binding digests, differences, degradations, incompatibilities, approvals, resource needs, and cost/concurrency projections;
- schema bundles and OpenAPI/MCP schemas generated from the same contracts.

There is one compiler. Assistants, Agent Server deployments, and Temporal workflows are runtime implementations, not alternative Workflow Types or authorities.

### 3.2 Run control and admission module

Provide versioned REST/MCP operations for:

- prepare immutable launch ticket;
- authorize and idempotently admit a run;
- inspect run/runtime projection;
- request typed interventions including pause, resume, steer, cancel, fork, and accepted domain-specific commands;
- list and answer pending decisions;
- retrieve typed result and terminal status.

Admission must atomically persist the accepted launch facts and an outbox record. Command acceptance must atomically persist the command/message fact and an outbox record. HTTP success never implies that a direct provider or Temporal call completed.

Every path fails closed when compilation, exact bindings, policy, budget, authority, idempotency, or required capability evidence is incomplete. There is no direct provider admission bypass.

### 3.3 Command/message inbox and outbox module

Implement durable:

- inbound command/message records with stable idempotency keys;
- transactional outbox records for Temporal launch, Temporal Signal/Update, provider operations, and downstream publication;
- dispatcher leases, bounded retries, poison-message handling, and dead-letter/reconciliation states;
- causation/correlation fields linking every command to the resulting events and runtime operation;
- the canonical receipt projection from `06C`, preserving distinct `accepted`, `routed`,
  `runtime-observed`, `model-visible`, `applied`, `rejected`, `expired`, and `superseded` states.

`applied` is projected only after the checkpoint containing the injected model-visible batch
commits. Routing, runtime observation, model visibility, or later reconciliation must never be
collapsed into `applied`.

An API may add transport-facing projection states such as queued, dispatch-retrying, dead-lettered,
or reconciling, but the projection must remain lossless: every response/event retains the canonical
receipt state and maps each added state explicitly without merging, renaming, or skipping canonical
transitions.

The inbox/outbox is the boundary between authoritative BellLabs transactions and fallible transports. Repeated delivery must not duplicate launch, command, provider effect, artifact, or terminalization.

### 3.4 Evidence and artifacts module

Provide authorized, redacted access to:

- typed results and evidence manifests;
- immutable artifact metadata and content-addressed references;
- checksums, provenance, assembly digests, verifier outcomes, and retention state;
- bounded download/upload flows using short-lived scoped references;
- artifact availability and reconciliation status.

Artifact bytes belong in the selected object/content store, not Temporal history, event payloads, logs, or unrestricted traces.

### 3.5 Projections and durable events module

Build product status, timelines, and reconnectable streams from BellLabs durable events and projections. Required behavior:

- stable monotonic BellLabs event cursor;
- authorized replay from cursor with documented retention/compaction behavior;
- snapshot-plus-tail reconnect contract;
- deterministic lifecycle projection and typed event schemas;
- event links to relevant Temporal workflow/run and LangSmith trace/run diagnostics;
- explicit lag, degraded, stale, and reconciliation indicators.

Temporal histories and LangSmith traces are diagnostic evidence. They are not the product status stream and are not product authority. A browser or MCP client must not reconnect by reading Temporal history or Agent Server streaming APIs.

### 3.6 Temporal launch and command bridge

The bridge is internal and is the only module allowed to launch or command Temporal for admitted BellLabs runs. It must:

- consume committed outbox records;
- map exact Workflow Implementation and assembly digests to workflow type, task queues, worker-build constraints, and search attributes;
- use deterministic workflow IDs and idempotent launch/Signal/Update semantics;
- persist dispatch observations without claiming completion before BellLabs observes the corresponding fact;
- expose no public Temporal credentials, namespace access, workflow handles, or arbitrary Signal surface;
- reconcile unknown, delayed, duplicated, rejected, and already-completed dispatch outcomes.

Temporal Web/API access is restricted to service identities and authorized operations staff. It is not exposed through generic pass-through REST or MCP methods.

### 3.7 Provider callback ingress

Every asynchronous provider callback follows this order:

```text
authenticate and validate callback
  -> normalize provider identity and idempotency key
  -> persist/deduplicate the callback fact in BellLabs authority
  -> commit a transactional outbox record
  -> asynchronously Signal/Update the owning Temporal workflow
```

Requirements:

- verify signature, timestamp/nonce, endpoint audience, tenant/run binding, and allowed payload size before accepting;
- retain a redacted/verifiable receipt sufficient for audit and replay;
- return success for a valid duplicate only after confirming the original fact is durable;
- quarantine invalid, unbound, stale, conflicting, or oversized callbacks;
- never Signal Temporal first and persist later;
- never let callback payloads directly mutate a projection or terminalize a run;
- reconcile a durable callback fact whose Temporal delivery is delayed or lost.

### 3.8 Provider gateways

All outbound model, tool, MCP, data, and sandbox-provider effects use governed provider gateways or provider-neutral adapters. Gateways must enforce:

- exact compiled binding and admitted operation scope;
- tenant and data-classification policy;
- budget, timeout, retry, concurrency, and rate limits;
- stable effect/idempotency claim;
- request/response schema validation and redaction;
- normalized usage, cost, provider IDs, callback bindings, and error taxonomy.

Neither API handlers nor coordinators may call provider SDKs directly. Gateway access alone does not authorize a new run or operation.

### 3.9 Internal Agent Server boundary

LangGraph Agent Server remains an internal operation runtime for accepted bounded agent operations. Its native thread/run/checkpoint APIs:

- are reachable only by the owning worker/gateway service identities and restricted operators;
- do not perform BellLabs macro scheduling, admission, product lifecycle authority, or public streaming;
- cannot accept arbitrary public assistants, graph IDs, configuration, tools, or provider credentials;
- return operation facts/results to the owning Temporal activity/child workflow for BellLabs persistence;
- may be replaced by another compatible bounded-operation runtime without changing the public BellLabs API.

## 4. External contract and compatibility

REST and MCP must use the same application services and enforce:

- one principal-to-BellLabs-authority mapper;
- identical tenant, role, resource, and action policy;
- one versioned BellLabs success/error model;
- equivalent idempotency, pagination, event cursor, and redaction semantics;
- default-deny policy when ownership or scope is absent;
- no route or MCP method that accepts raw Temporal workflow IDs, arbitrary Signals, Agent Server thread/run commands, provider credentials, or uncompiled runtime configuration.

Required end-to-end contract coverage includes discover, compare, compile, prepare, admit, launch observation, stream/reconnect, decision, pause, resume, steer, cancel, fork, result, evidence, and failure/reconciliation.

Required compatibility routes may coexist temporarily, but each must delegate to the same BellLabs services. No legacy route may silently choose a runtime or call a provider before BellLabs admission.

## 5. Correlation and observability

### 5.1 Required identity taxonomy

Logs, metrics, BellLabs events, Temporal search attributes, LangSmith traces, artifacts, and reconciliation records must correlate, where applicable:

- BellLabs run ID and epoch;
- stage/segment ID;
- root workflow type, workflow ID, and workflow run ID;
- workflow family and family version;
- operation workflow type, workflow ID, and workflow run ID;
- activity type and activity ID;
- semantic attempt and execution generation, kept distinct from transport/activity retry count;
- Agent Server deployment/graph, thread ID, run ID, and checkpoint ID;
- sandbox provider, sandbox ID, and job/command ID;
- Workflow Type/Implementation refs;
- `StageExecutionBinding`, `OperationAssemblySpec`, input, state/schema, artifact, and result digests;
- causation ID, correlation ID, inbox message ID, outbox message ID, callback receipt ID, and provider effect ID.

IDs must be structured fields, not parsed from log text. Public responses expose only safe BellLabs identities and authorized diagnostic links.

### 5.2 Metrics, health, and alerts

At minimum, measure:

- admission latency and rejection reason;
- inbox/outbox age, dispatch latency, retries, poison records, and reconciliation backlog;
- Temporal schedule-to-start, task-queue backlog, poller presence, activity/workflow failures, retries, timeouts, heartbeat age, and continue-as-new behavior;
- per-pool concurrency, saturation, fairness, and queue age;
- callback authentication failures, duplicate rate, persistence-to-Signal lag, and orphan count;
- BellLabs event projection lag and reconnect success;
- Agent Server operation latency, checkpoint size, and transport failures;
- sandbox create/start/execute/snapshot/delete latency, leaks, and quota;
- provider usage, cost, throttling, and malformed results;
- LangSmith trace/evaluator ingestion lag and failure;
- redaction failures and security-denial anomalies.

Readiness must distinguish API availability, write authority, Temporal connectivity, worker-poller health, projection freshness, and degraded optional dependencies. A missing required poller is not healthy merely because the API process responds.

### 5.3 Product diagnostics

Authorized product status includes BellLabs lifecycle state, projection freshness, pending decisions, accepted commands, evidence, and typed failure. It may include deep links to:

- the exact Temporal workflow/run for operational diagnosis;
- the exact LangSmith root trace or evaluation record;
- the exact Agent Server operation or sandbox/job when policy allows.

Links are enrichment only. Histories, traces, and provider dashboards never override BellLabs durable facts.

## 6. LangSmith tracing and evaluation

LangSmith tracing and evaluation are required release capabilities, not optional follow-up work.

### 6.1 Trace structure

Trace at least:

- one root trace for the BellLabs run or stateless control operation;
- epoch/cycle, stage/segment, child-workflow/operation, activity, bounded agent run, model, tool/MCP, sandbox command, verifier, and evaluator spans;
- exact correlation taxonomy and assembly digests from Section 5;
- normalized latency, usage, cost, retries, outcome, and error class.

Temporal retries and semantic attempts must be distinguishable. A replayed workflow must not emit misleading duplicate business spans without replay-safe handling.

### 6.2 Offline evaluation

Create versioned datasets containing known-good and known-bad cases for:

1. catalog discovery, deterministic compile, preparation, and admission denial;
2. schedule, child-workflow, command, replay, idempotency, and callback trajectories;
3. bounded agent operation convergence, checkpoint/resume, handoff, and verifier behavior;
4. ingestion, schema-grounding, evidence, citation, and scientific-quality outcomes;
5. reconnect/projection correctness and diagnostic-link integrity;
6. MCP auth/schema/error equivalence with REST;
7. sandbox containment and provider-gateway policy;
8. cross-tenant, injection, approval, capability, and budget attacks;
9. orphan and partial-failure reconciliation.

Use deterministic evaluators for exact invariants and pinned model judges only for semantic dimensions. Each evaluator has one named dimension, defined score meaning, pinned inputs/prompt/model/schema, known-good/known-bad validation, empty/error behavior, threshold, and owner.

### 6.3 Online evaluation preparation

After inspecting production-shaped root traces, record for every proposed online evaluator:

- project, evaluator version, trace selector, field mapping, and sampling rule;
- score interpretation, alert threshold, review workflow, cost limit, and rollback;
- historical validation and behavior on missing/redacted/malformed traces.

Evaluation never publishes definitions, admits work, changes authority, or terminalizes a run. Any online attachment requires explicit authorization and verification in a non-production project before Stage 8.

## 7. Security and redaction

Implement and test:

- external JWT/service identity validation and consistent tenant/resource/action authorization;
- separate internal service identities for API, dispatchers, five worker pools, projection/reconciliation, callbacks, Agent Server, sandboxes, and operations;
- restricted network access to Temporal, Agent Server, databases, object storage, and provider callback administration;
- least-privilege database roles and row/resource filters;
- capability and immutable-binding enforcement, including mutable-alias denial at admission;
- callback signature/replay protection and outbound SSRF/endpoint allowlists;
- command, callback, provider, MCP, artifact, and event schema/size limits;
- effect claims and idempotency under retries, callbacks, replay, and shadow execution;
- sandbox tenant isolation, egress, mounts, package policy, cleanup, snapshot access, and malware posture;
- encryption, retention, deletion, and legal-hold behavior for authority, events, artifacts, checkpoints, histories, and traces;
- supply-chain lock, image/SBOM, migration ownership, and secret delivery.

Redact before persistence or export to logs, events, errors, traces, evaluator inputs, and diagnostic links. Sentinel tests must cover secrets, authorization headers, cookies, tokens, signed URLs, connection strings, environment values, PHI, raw private corpora, prompts where classified, unrestricted tool output, sandbox files, and provider payloads.

No unresolved critical/high authority, cross-tenant, secret/PHI, approval-bypass, arbitrary-signal, SSRF, sandbox-escape, or duplicate-effect finding may pass the gate. A documented exception must identify compensating controls and explicitly block production promotion when appropriate.

## 8. Five worker-pool isolation qualification

Test these five independently deployed and independently scalable Temporal worker classes:

1. coordinator workers;
2. agent-operation workers;
3. ingestion-I/O workers;
4. sandbox-control workers;
5. verification/reconciliation workers.

For each class, prove:

- dedicated task queues and least-privilege credentials;
- no polling or execution of another class's task queue;
- explicit concurrency, rate, timeout, retry, heartbeat, and resource limits;
- safe deployment/build identity and compatibility behavior;
- queue-specific metrics, alerts, drain, and rollback;
- tenant and workload fairness;
- failure/saturation does not starve unrelated classes.

Run cross-pool isolation scenarios: saturate each pool in turn, stop all pollers, deploy an incompatible worker build, inject poison work, revoke one pool's credentials, and recover it. Record schedule-to-start, backlog growth, API behavior, unaffected-pool behavior, recovery time, and reconciliation result.

## 9. Self-hosted Temporal and failure qualification

Use production-shaped self-hosted Temporal configuration for qualification. Inject and recover from:

- Temporal frontend/service restart and temporary unavailability;
- persistence latency, connection exhaustion, failover, and restart;
- missing pollers for every required task queue;
- task-queue backlog and uneven worker capacity;
- workflow-task and activity failures/timeouts/retries;
- lost or delayed activity heartbeats and cancellation;
- duplicate/delayed/out-of-order authenticated callbacks;
- callback fact persisted while outbox-to-Signal delivery is unavailable;
- API/outbox dispatcher crash before and after transport acknowledgement;
- provider timeout, rate limit, malformed response, and uncertain effect;
- Agent Server transport/checkpoint failure;
- sandbox create/execute/snapshot/delete failure and leaked resources;
- projection/event consumer lag and reconnect during compaction;
- orphan Temporal workflow, Agent Server run, sandbox/job, callback, outbox record, artifact, and BellLabs nonterminal run.

Verification/reconciliation workers must detect, classify, and safely repair or escalate orphans without inventing terminal facts or duplicating effects. Temporal history may be used to diagnose and reconcile, but any recovered product fact is persisted through BellLabs authority.

Measure failure-detection time, recovery time, event lag, queue age, duplicate-effect count, orphan age, and operator actions against accepted thresholds.

## 10. Production-shaped build and runbooks

Produce reproducible images/configuration from the lockfile for:

- modular API/control service;
- all five worker pools;
- projection/outbox/reconciliation processes where separately deployed;
- internal bounded Agent Server runtime used by accepted operations.

Verify no image contains secrets, personal/scratch code, broad `.env` files, host profiles, or undeclared host tools. Runtime services use non-owner credentials; schema migrations use separate release credentials.

Provide runbooks for:

- API admission degradation and no-bypass behavior;
- Temporal service/persistence health;
- missing pollers, queue backlog, heartbeat loss, and worker drain;
- callback authentication, delayed Signal, and replay;
- stuck run, uncertain provider effect, and orphan reconciliation;
- projection lag and product reconnect;
- Agent Server/checkpoint and sandbox failures;
- LangSmith tracing/evaluation outage;
- database/object-store backup and restore;
- compatible worker rollback and evidence-preserving recovery.

## 11. Stage 7 gate

Stage 7 passes only when all of the following have durable evidence:

- every external REST/MCP launch and command traverses the modular BellLabs API/control service, deterministic compile, admission, authority, and outbox;
- tests prove Temporal and Agent Server APIs are internal/restricted and no provider admission bypass exists;
- catalog/compile, run control/admission, inbox/outbox, evidence/artifacts, projections/events, Temporal bridge, callback ingress, and provider gateway modules pass contract and authorization tests;
- valid callbacks are durably persisted/deduplicated before outbox Signal, including duplicate, delayed, and delivery-outage cases;
- product reconnect uses BellLabs durable event cursors and succeeds without Temporal history or Agent Server streams;
- correlation fields join BellLabs, Temporal, LangSmith, Agent Server, sandbox, artifact, and message records for representative runs;
- LangSmith tracing is complete and redaction sentinels do not leak;
- required offline evaluators meet accepted deterministic and semantic thresholds;
- all five worker-pool isolation and starvation tests pass;
- self-hosted Temporal missing-poller, backlog, heartbeat, callback, restart, persistence, and orphan-reconciliation scenarios meet thresholds;
- security review has no disallowed unresolved findings;
- load, cost, event-lag, queue-age, recovery, and reconciliation thresholds pass;
- production-shaped images, authenticated end-to-end tests, backup/restore evidence, and runbooks are accepted.

Any missing item is a Stage 8 blocker; it may not be converted into an undocumented deployment assumption.

## 12. Explicit non-goals

- Do not route broad production traffic.
- Do not expose Temporal or Agent Server as a second public/control API.
- Do not create a second compiler or macro scheduler.
- Do not let callbacks, traces, evaluators, or provider gateways mutate BellLabs authority directly.
- Do not remove legacy paths before Stage 8 shadow, canary, drain, and rollback gates.
- Do not choose the final ECS/EKS/EC2 topology in this stage.

## 13. Outgoing handoff

Include:

- final external REST/MCP contract and internal Temporal/Agent Server boundary inventory;
- module ownership, dependency, data-flow, and deployable manifests;
- principal/resource/action matrix and service-identity/network policies;
- compile/admission/no-bypass and callback persist-before-Signal evidence;
- product event/reconnect contract and projection-lag evidence;
- complete correlation dictionary and representative joined diagnostic record;
- LangSmith projects, trace taxonomy, redaction tests, datasets, evaluator registry, thresholds, and results;
- five worker-pool queue/config/isolation/load evidence;
- self-hosted Temporal failure, callback, orphan, recovery, and reconciliation reports;
- security findings, remediations, and explicit exceptions;
- production-shaped image/SBOM/config manifests;
- Stage 8 SLO, cost, topology-measurement, secret-name, backup/restore, rollback, and runbook requirements.
