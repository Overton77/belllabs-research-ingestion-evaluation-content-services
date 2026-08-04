# Coordinator MCP Phase 1 specification

Date: 2026-07-27  
Status: Implementation plan  
Depends on: `COORDINATOR_SPEC_INDEX.md`

## Objective

Make the existing coordinator lifecycle truthful and production-complete. An authorized MCP
client must be able to bootstrap, select an existing published Workflow Type, prepare it without
creating a run, launch it through real admission and Temporal execution, and retrieve exactly one
durable typed result through every URI returned by the server.

This phase hardens the existing path. It does not add broad catalog UX, composition planning,
progressive reports, or the coordinator sandbox.

## Current-state problems

- `app/mcp/__main__.py` can advertise launch while composing only search, readiness, audit, and
  optional external discovery.
- `app/application/coordinator_composition.py` contains the fuller production dependency shape,
  but it is not yet the single composition path for mounted and standalone MCP deployments.
- Launch returns a result URI that differs from the resource registered in
  `app/mcp/coordinator_resources.py`.
- `CoordinatorResultService` correctly rejects a terminal run without a typed result, but worker
  completion does not yet guarantee idempotent result materialization.
- Bootstrap feature flags, prompt registration, and actual providers can drift.
- External discovery writes candidate/evidence records, so a read-only annotation is inaccurate.
- The main FastAPI deployment does not yet mount the coordinator Streamable HTTP MCP endpoint.

## Required deliverables

### 1. One production composition path

- Mount the coordinator MCP application in the main FastAPI deployment behind an explicit
  setting.
- Build it through `build_production_coordinator_facade(...)`.
- Reuse lifespan-managed MongoDB, application PostgreSQL, catalog PostgreSQL, Temporal, run
  control, binding, and repository instances.
- Keep `app/mcp/coordinator_server.py` dependent only on the facade protocol.
- Give the mounted MCP path explicit principal mapping, tenant/request scoping, request limits,
  rate limits, and correlation propagation.

The mounted path must not make loopback HTTP calls to the same backend.

### 2. Truthful standalone mode

Choose and test one of these explicit modes:

- fully composed launch mode using the same production composition builder; or
- read-only mode that omits preparation, launch, result, and unavailable prompt/resource
  capabilities.

Do not use a launch-enabled flag with missing providers. Startup validation must reject any
impossible enabled configuration.

### 3. Effective capability bootstrap

Derive bootstrap capabilities from both policy/configuration and composed provider availability.
For every tool, prompt, and resource:

- advertise and register it when a usable provider exists;
- omit it when optional and unavailable; or
- fail startup when configuration requires it but no provider exists.

Registered prompts must have exact bindings. Do not advertise placeholder bindings.

### 4. Canonical run resource URI

Standardize launch and result responses on the registered run URI:

```text
belllabs://runs/{run_id}/result
```

Every returned URI must be immediately readable by the same authorized principal and must enforce
tenant and request scope server-side.

### 5. Terminal typed-result materialization

Add an application-owned, idempotent completion seam used by the production worker path:

1. receive the authoritative terminal execution outcome and family-specific details;
2. construct `WorkflowResultRecord`;
3. persist it immutably through the workflow-result repository;
4. reconcile or complete run terminality according to an explicitly tested ordering;
5. return the existing record on a same-payload retry;
6. reject a conflicting payload for the same run identity.

Required recovery cases:

- result write succeeds and completion is retried;
- run reaches terminal projection before result write and recovery resumes;
- Temporal activity retries after an ambiguous persistence response;
- duplicate completion carries a conflicting digest or family.

Temporal history is execution evidence, not the typed-result read model.

### 6. Protocol correctness

- Correct side-effect annotations for external discovery.
- Preserve stable success/error envelopes and correlation IDs.
- Enforce payload limits and reject payload tenant/request-scope overrides.
- Audit consequential preparation and launch events with exact actor and scope.
- Keep preparation non-consequential: no run is admitted before `launch_workflow`.

### 7. Core Viome tracer

Implement the **core tracer gate** from
`COORDINATOR_VIOME_STAGEGRAPH_ACCEPTANCE_SPEC.md` in this phase. It must use the existing
published web-research StageGraph path and real coordinator/application/Temporal contracts. Phase
1 does not need Phase 5 report collections or Phase 6 sandbox bundles to pass this gate.

## Application ownership

Primary files likely to change:

- `app/server.py`
- `app/config.py`
- `app/mcp/__main__.py`
- `app/mcp/coordinator_server.py`
- `app/mcp/coordinator_resources.py`
- `app/mcp/coordinator_prompts.py`
- `app/application/coordinator_composition.py`
- `app/application/coordinator_facade.py`
- `app/application/coordinator_results.py`
- `app/application/postgres_workflow_result_repository.py`
- `app/temporal/coordinator_runtime.py`
- `app/temporal/orchestration_activities.py`
- `app/temporal/worker.py`

Do not move terminal-result authority into `app/mcp/`, a live experiment script, or a Temporal
workflow implementation.

## Verification

### Contract and startup tests

- Every advertised operation has a provider.
- Required provider absence fails startup.
- Optional provider absence removes or marks only that capability unavailable.
- Prompt advertisement equals actual binding registration.
- Discovery annotations reflect candidate/evidence writes.
- Principal payloads cannot override authenticated scope.

### Launch and result tests

- Prepare freezes exact refs, digests, policy/environment snapshots, semantic plan, and
  idempotency identity.
- Prepare does not admit a run.
- Expired, consumed, caller-mismatched, or context-mismatched tickets cannot launch.
- Launch reaches the registered StageGraph or GoalDirected Temporal family.
- Duplicate launch identity returns the prior handle or rejects a conflicting request.
- Exactly one immutable typed result is materialized.
- Terminal/result boundary retries converge.
- The launch result URI is registered and readable.

### Deployment acceptance

Exercise the mounted Streamable HTTP MCP endpoint against the deployed application composition,
not a direct facade call. Evidence must include:

- bootstrap response;
- selected exact Workflow Type/Implementation refs;
- prepared ticket identity and launchability;
- run, workflow, and Temporal run identities;
- terminal typed result;
- successful reads of all returned URIs.

## Exit criteria

Phase 1 is accepted only when:

1. the mounted production endpoint and supported standalone mode report truthful capabilities;
2. an authorized MCP client completes prepare → launch → Temporal → durable typed result;
3. exactly one result survives retries and process boundaries;
4. every returned URI is registered, authorized, and readable;
5. the core Viome tracer passes without the test harness directly constructing the final launch
   or calling Temporal.

## Outgoing handoff to Phases 2-3

The Phase 1 handoff must include:

- effective MCP tool/prompt/resource inventory and feature defaults;
- mounted and standalone composition diagrams;
- canonical URI list and authorization behavior;
- launch/result contract versions;
- terminal-result ordering and retry semantics;
- migrations and deployment order;
- acceptance-test evidence;
- any provider that remains conditional or environment-specific.

Phases 2-3 may begin implementation earlier, but cannot integrate caller-facing discovery or
schema claims until the canonical Phase 1 facade, bootstrap, URI, and result contracts are
accepted.

## Explicit non-goals

- Cursor-paginated decision cards and implementation comparison.
- Migration of every opaque contract string to JSON Schema.
- Snapshot search, compatibility APIs, or linked-run composition planning.
- Official report/evidence/artifact collection resources.
- Coordinator base snapshot, Agent Skill, or workspace bundle materialization.
