# BellLabs Coordinator MCP: Architecture, Execution Trace, and Plan

Date: 2026-07-27

Status: Interview summary and implementation-alignment plan. This document explains the
current code and records the decisions reached in the coordinator architecture interview. It is
not a product specification and does not supersede accepted authority in `biotech-meta`.

Primary architecture references:

- `../../../biotech-meta/docs/workflow-catalog-configuration-composition-and-agent-entry.md`
- `../../docs/workflow-control-plane-current-state-and-next-slices.md`
- `../../../biotech-meta/docs/checkpoints/schema_schema_workspaces_efficient_db_interaction/2026-07-16-large-schema-workspaces-selection-and-report-splitting-special-checkpoint.md`

## Executive decision

Build a robust coordinator MCP server first, then place a coordinator agent in a reviewed
sandbox workspace that uses that server.

The two layers have different responsibilities:

- The coordinator MCP server is the authoritative agent-facing control surface for discovery,
  exact retrieval, compatibility assessment, validation, preparation, launch, run status, and
  result retrieval.
- The coordinator sandbox, skill, and helper executables provide an efficient reasoning and
  authoring environment for large contracts and multi-file workflow plans.
- The sandbox never becomes catalog, policy, launch, or workflow state authority.
- The coordinator agent proposes. BellLabs application services resolve, validate, admit, and
  execute.

Initially, mount the MCP endpoint in the deployed FastAPI backend and give it the same
application-service composition, repositories, and Temporal client as the existing backend.
Preserve the facade and port boundaries so the MCP deployment can be extracted into a separate
service later without changing its external contracts.

Do not add loopback HTTP calls merely to make MCP execution appear more real. The current
in-process path already reaches the actual compiler, run-control service, repositories, semantic
binding service, and Temporal submitter. A future separately deployed coordinator may call a
dedicated internal service API, but that is a deployment boundary rather than a domain rule.

## The current system in one picture

```text
Coordinator agent
  |
  | Streamable HTTP / MCP
  v
app/mcp/coordinator_server.py
  |
  | authenticated, typed facade calls
  v
app/application/coordinator_facade.py
  |
  +-- catalog search and exact definition retrieval
  +-- external candidate discovery and inspection
  +-- design validation
  +-- launch preparation
  +-- launch
  +-- result retrieval
  |
  v
BellLabs application services
  |
  +-- ControlPlaneService.compile(...)
  +-- RunControlService.admit(...)
  +-- semantic binding authoring
  +-- WorkflowLaunchDispatcher.prepare_bound(...)
  +-- TemporalWorkflowSubmitter.submit(...)
  |
  v
StageGraphWorkflow or GoalDirectedWorkflow
```

The important boundary is not “MCP versus REST.” It is:

```text
protocol adapter
  -> project-owned application use case
  -> deterministic domain validation and authority
  -> durable execution mechanics
```

Both MCP and REST should delegate to project-owned use cases. Neither should reproduce domain
logic.

## How execution works today

### 1. MCP transport

`coordinator_server.py` defines a narrow `CoordinatorFacade` protocol and registers the
agent-facing tools. Each tool:

1. resolves the authenticated `CoordinatorPrincipal`;
2. delegates to the facade;
3. serializes the result into the common versioned envelope;
4. converts domain and argument failures into stable error envelopes.

The envelope is:

```json
{
  "ok": true,
  "schema_version": "1",
  "correlation_id": "...",
  "data": {}
}
```

The MCP layer correctly avoids owning compilation, run admission, Temporal behavior, or
persistence.

### 2. Shared application facade

`ProductionCoordinatorFacade` is the application-owned coordinator surface shared by MCP,
tests, and a possible future HTTP adapter. It owns cross-cutting coordinator concerns such as:

- permissions and tenant scope;
- feature flags;
- request limits and timeouts;
- catalog authorization;
- audit records;
- dependency availability;
- translation between agent-facing DTOs and application services.

This is the correct reusable boundary. MCP should remain a protocol adapter over it.

### 3. Launch preparation

`CoordinatorLaunchPreparationService.prepare(...)` performs the non-consequential but
authoritative launch preview:

1. validate the proposal against the authenticated launch context;
2. compile the requested Workflow Type and Implementation into an immutable Effective Run
   Configuration;
3. resolve the exact Workflow Type and Blueprint references;
4. validate the StageGraph or GoalDirected initial input;
5. prepare the exact semantic binding plan;
6. construct the frozen `RunRequest`;
7. execute admission preview;
8. persist a short-lived prepared launch ticket.

The ticket freezes the exact configuration, request digest, assets, authority decisions,
availability decisions, approvals, policy/environment snapshots, and semantic binding plan.

This separation is valuable: planning and preview do not silently create a run.

### 4. Launch and Temporal submission

`CoordinatorWorkflowLaunchService.launch(...)`:

1. reloads the prepared ticket;
2. revalidates caller, tenant, scope, approvals, policy, environment, ticket state, and expiry;
3. calls `RunControlService.admit(...)` through the admission port;
4. authors and persists the exact operation semantic binding;
5. creates the bound StageGraph or GoalDirected input;
6. submits it through `TemporalWorkflowSubmitter`;
7. consumes the prepared ticket;
8. returns the run, workflow, and Temporal run identities.

The Temporal workflow is therefore not bypassed by MCP. MCP reaches the same actual execution
engine through application services.

### 5. Result retrieval

`CoordinatorResultService` joins:

- the authoritative run projection, which supplies lifecycle phase; and
- an immutable typed `WorkflowResultRecord`.

It intentionally rejects a terminal run that has no durable typed result. This exposes a real
current gap: live experiment scripts persist typed results after awaiting Temporal, but the
production worker path does not yet guarantee that terminal execution materializes the typed
result record.

## Why MCP should not call the backend's own REST endpoints today

Calling the current FastAPI endpoints from the MCP process would add:

- another serialization and network hop;
- a second authentication translation;
- duplicated correlation and error handling;
- potential schema drift between REST and MCP;
- failure modes where the backend calls itself;
- no additional domain validation or Temporal durability.

The current REST surface also does not expose a complete coordinator API. `app/server.py`
currently mounts control-plane, run-control, and schema-grounding routers. It does not mount a
coordinator REST router, and the MCP server provides higher-level coordinator operations that
do not map one-to-one to those routes.

The recommended initial deployment is:

```text
one cloud backend deployment
  +-- FastAPI operator/application APIs
  +-- mounted Streamable HTTP coordinator MCP endpoint
  +-- shared lifespan-managed repositories and clients
  +-- shared application services
```

The recommended future extraction path is:

```text
coordinator MCP service
  -> dedicated internal coordinator/control-plane client
  -> backend execution service
```

Extraction becomes justified when independent scaling, network isolation, release cadence,
fault containment, or security policy requires it. The facade ports should make the transition
possible, but the first cloud deployment should not pay the distributed-system cost in advance.

## Current MCP surface

The server currently exposes these tool families:

### Bootstrap

- `coordinator_bootstrap`

Returns server capabilities, runtime families, recommended operation order, and resource
templates.

### Catalog

- `search_capabilities`
- `get_capability`

Search combines catalog projection retrieval with authoritative definition rehydration and
selection policy. Exact retrieval is the final authority; search rank is evidence, not
permission.

### External candidate discovery

- `discover_mcp_servers`
- `discover_agent_skills`
- `inspect_external_candidate`

External results are candidate-only. Discovery or installation does not authorize use.
Inspection and governed promotion must precede selection.

### Workflow authoring and execution

- `validate_workflow_design`
- `prepare_workflow_launch`
- `launch_workflow`
- `get_workflow_result`

These represent the correct high-level lifecycle, but some implementations and read models are
not yet complete enough for the target coordinator experience.

### Resources

The server currently registers resources for:

- Workflow Type contract;
- Workflow Type input schema;
- Workflow Type output contracts;
- exact catalog asset;
- exact catalog manifest;
- run launch;
- run bindings;
- run result.

This is a good progressive-disclosure foundation. It needs schema-bearing contracts, more
decision-oriented views, and progressive run result resources.

### Prompts

The MCP package registers:

- `propose_workflow`;
- `review_workflow_design`;
- `explain_launch_blocker`;
- `summarize_workflow_result`.

Only `propose_workflow` is normally bound in current composition. Advertised prompts must either
have real bindings or be omitted from bootstrap and registration.

## Current gaps and defects

### P0: the standalone server advertises launch without composing launch

`app/mcp/__main__.py` builds search, readiness, audit, and optional external discovery directly.
It passes the `coordinator_launch_enabled` feature flag, but it does not supply:

- launch preparation;
- launch service;
- launch context provider;
- run projection/result service;
- run resource reader;
- semantic binding and Temporal submission dependencies.

Consequently, the local HTTP MCP server can advertise permissions and feature state that its
provider graph cannot fulfill.

The production composition root in `app/application/coordinator_composition.py` already defines
the right dependency shape. The mounted cloud endpoint and local executable should use one
truthful composition path or fail startup when an enabled capability has no provider.

### P0: the launch result URI is not registered

Launch currently returns:

```text
belllabs://workflow-results/{request_scope}/{run_id}
```

The MCP server registers:

```text
belllabs://runs/{run_id}/result
```

Every URI returned by an MCP operation must be immediately readable by the same authorized
principal. Standardize the URI and add an end-to-end contract test.

### P0: typed result persistence is not part of terminal execution

`CoordinatorResultService` correctly requires a durable typed result for terminal runs, but the
Temporal worker path does not yet guarantee that record is written. Experiment harnesses should
not be the production result writer.

Materialize the immutable result through an application-owned terminal activity or completion
handler with idempotent persistence. Run lifecycle terminality and typed result persistence need
a defined ordering and recovery rule.

### P1: generic capability search is insufficient for a large catalog

`CapabilitySearchRequest` provides `limit` but no opaque cursor, stable continuation identity,
facets, coverage metadata, or result count. Large Workflow Type, Prompt, Skill, MCP, Agent
Profile, snapshot, and implementation catalogs need decision-specific read models.

The current search path also risks repeated authoritative reads. Search projections should
produce compact cards, then selected records should be batch-rehydrated once and checked against
current authority.

### P1: Workflow Type contracts are not truly schema-bearing

`WorkflowDesignDraft.input_contract`, Workflow Type input admission, and output contract fields
still contain opaque strings in important paths. The resource named `input-schema` therefore
cannot yet reliably provide a versioned JSON Schema that an agent or local executable can use.

Promote input, admission, operation, output, evaluation, and decision contracts to exact,
versioned, schema-bearing definitions, with a compatibility adapter for existing records.

### P1: design validation is structurally shallow

The present validation accepts a `WorkflowDesignDraft`, resolves requested assets, and reports
publication requirements. It is not yet the complete deterministic validator implied by its
name.

Target validation phases should include:

- schema validity;
- Workflow Type contract conformance;
- StageGraph or GoalDirected structural validity;
- obligation and output realization;
- linked-run slot legality;
- authority intersection;
- workspace compatibility;
- asset fit and promotion state;
- environment availability;
- publication requirements.

Findings should carry stable codes, severity, JSON paths, evidence references, and suggested
repairs.

### P1: workflow composition is not coordinator-accessible

The control plane already models linked-run slot constraints and the application implements
linked-run behavior. The coordinator does not yet have first-class composition-template search,
composition-plan validation, or composition preview.

Composition must connect independently admitted Workflow Runs. It must never concatenate or
silently rewrite their StageGraph definitions.

### P1: snapshots lack coordinator discovery and compatibility views

Snapshot creation, clone-on-restore, and compatibility enforcement exist in
`app/application/sandbox_snapshots.py`, but the coordinator cannot efficiently:

- search snapshot metadata;
- inspect lineage and retention;
- compare a snapshot with a proposed Workflow Implementation or operation;
- receive typed mismatch reasons.

Snapshot bytes and credentials must not be exposed through catalog search. Return metadata,
content identities, compatibility decisions, and the live resources that must be reacquired.

### P2: result retrieval is too monolithic for reports and evidence

Large GoalDirected checkpoints, reports, evidence collections, artifacts, and execution details
will exceed practical MCP response and context limits. `get_workflow_result` should not become a
large object dump.

Return a compact typed summary and immutable resource links. Retrieve reports, artifacts,
evidence, bindings, and paginated details independently.

### P2: discovery annotations are inaccurate

External MCP and skill discovery currently carry `readOnlyHint`, but discovery persists
candidate and evidence records. Mark these operations as non-destructive candidate-producing
commands, with idempotency semantics where the underlying behavior supports them.

### P2: prompt registration and capability advertisement can drift

Three registered prompts commonly have no bindings. The same class of drift exists between
feature flags and provider availability.

Bootstrap should describe effective capabilities derived from both policy and composed
providers. Startup should fail for impossible enabled configurations; optional capabilities
should be omitted rather than fail only after the agent selects them.

## Target coordinator architecture

### Layer 1: MCP protocol adapter

Owns only:

- MCP tool, resource, and prompt registration;
- transport authentication integration;
- principal resolution;
- strict input parsing;
- stable MCP response envelopes;
- protocol annotations;
- protocol-level size limits.

It must not own catalog ranking, compilation, policy, admission, result construction, or
Temporal submission.

### Layer 2: coordinator application facade

Owns:

- coordinator authorization;
- tenant and request scope;
- coordinator query and command use cases;
- rate, concurrency, and payload limits;
- feature/provider capability reporting;
- audit and correlation;
- stable agent-facing DTOs;
- mapping domain failures to coordinator errors.

The facade must remain usable from MCP, tests, and a future internal-service adapter.

### Layer 3: decision-oriented application services

Add application use cases for:

- Workflow Type search and description;
- launch contract generation;
- Workflow Implementation comparison;
- agentic asset fit;
- snapshot search and compatibility;
- composition template search and preview;
- deterministic workflow draft validation;
- launch preparation and submission;
- compact run status and result summary;
- report, artifact, evidence, and binding retrieval.

These services re-resolve exact authoritative records before consequential decisions.

### Layer 4: domain authority and persistence

Continue to treat:

- immutable definitions and exact references;
- compiler rules;
- run-control reducer and admission;
- linked-run constraints;
- operation execution bindings;
- workspace and snapshot compatibility;
- immutable result records;

as BellLabs domain/application truth. Search indexes, MCP responses, prompts, skills, sandbox
files, and Temporal history do not replace that truth.

### Layer 5: execution mechanics

Temporal owns durable execution mechanics: workflow lifecycle, retries, timers, recovery, and
worker coordination. It does not decide Workflow Type semantics or grant authority.

## Information architecture for a capable coordinator agent

Large contracts should use progressive disclosure:

```text
compact navigation
  -> high-recall search cards
  -> exact decision-oriented description
  -> exact contract sections and schemas
  -> optional content-addressed workspace bundle
  -> deterministic validation and preparation
```

### Compact search cards

A Workflow Type search result should be small enough to compare many candidates. It should
include:

- exact current/default reference;
- purpose and non-goals;
- accepted input and output summaries;
- StageGraph or GoalDirected family;
- maturity and lifecycle;
- implementation aliases;
- required capability summary;
- authority and sensitivity summary;
- default budget/duration bands;
- standalone/composition use;
- environment availability;
- launchability and exclusion reasons.

Equivalent compact cards should exist for implementations, agentic assets, snapshots, and
composition templates.

### Exact resources

After selection, MCP resources should expose exact immutable sections rather than one enormous
definition:

```text
belllabs://workflow-types/{id}/{revision}/summary
belllabs://workflow-types/{id}/{revision}/contract
belllabs://workflow-types/{id}/{revision}/launch-contract
belllabs://workflow-types/{id}/{revision}/implementations
belllabs://contracts/{id}/{revision}/schema
belllabs://catalog/{kind}/{id}/{revision}
belllabs://catalog/{kind}/{id}/{revision}/manifest
```

Resources should include source revision and digest so the agent can detect stale material.

### Optional workspace bundle

When a contract set is too large or cross-referenced for efficient MCP-only reasoning, the
coordinator may request a content-addressed materialization manifest. The sandbox adapter
materializes only the selected bundle:

```text
coordinator/
  README.md
  task/
    objective.md
    constraints.json
  catalog/
    workflow-types/
    implementations/
    contracts/
    assets/
    snapshots/
  drafts/
    workflow-design.json
    composition-plan.json
  validation/
  results/
  bin/
  skills/
    belllabs-workflow-coordinator/
```

Every authoritative file records its exact source URI, revision, digest, and retrieval time.
Workspace drafts are proposals. Editing a materialized contract never changes the catalog.

## Target MCP query and command surface

Avoid exposing every internal repository method. Expose intent-specific coordinator use cases
plus exact low-level retrieval.

### Always-visible navigation

- `coordinator_bootstrap`
- `search_workflow_types`
- `describe_workflow_type`
- `get_workflow_launch_contract`
- `compare_workflow_implementations`
- `search_agentic_assets`
- `get_capability`

### Design and composition

- `validate_workflow_design`
- `search_composition_templates`
- `preview_workflow_composition`
- `validate_workflow_composition`

### Snapshot and workspace planning

- `search_sandbox_snapshots`
- `assess_snapshot_compatibility`
- `prepare_coordinator_workspace_bundle`

### Governed execution

- `prepare_workflow_launch`
- `launch_workflow`
- `prepare_workflow_composition`
- `launch_workflow_composition`

### Run and result queries

- `get_workflow_status`
- `get_workflow_result`
- `search_run_reports`
- `get_run_report`
- `list_run_artifacts`
- `list_run_evidence`

### External discovery, revealed only when needed and permitted

- `discover_mcp_servers`
- `discover_agent_skills`
- `inspect_external_candidate`

Do not remove the generic `search_capabilities` tool immediately. Keep it as a compatibility
and advanced escape hatch while intent-specific services mature. The coordinator skill should
prefer the intent-specific path.

## Two workflow authorship paths

### Path A: governed launch of an existing Workflow Type

This is the normal path:

```text
search Workflow Types
  -> inspect launch contract and default implementation
  -> compare approved implementations if needed
  -> provide admitted inputs
  -> propose bounded overlays
  -> preview/prepare
  -> launch
```

The backend resolves aliases, exact refs, authority, environment state, and digests. The agent
must not fabricate these values.

### Path B: novel workflow authoring

A materially novel StageGraph, GoalDirected contract, or Workflow Type is a draft:

```text
author draft
  -> deterministic validation
  -> independent review where required
  -> governed publication of exact definitions
  -> compile
  -> prepare
  -> launch
```

Validation alone does not turn a draft into executable authority. “On the fly” may be fast and
run-scoped, but it must still be recorded, validated, and published through an accepted
authoring seam.

## Multi-stage workflows versus multi-workflow composition

Use these terms precisely:

- A StageGraph or GoalDirected Blueprint defines execution inside one Workflow Implementation.
- A Workflow Run executes exactly one Effective Run Configuration.
- A composition connects independently admitted Workflow Runs through typed links.

The coordinator may select or author a multi-stage implementation, but it must not merge the
internals of several Workflow Types into one generated graph. Cross-Workflow-Type plans use
linked runs with explicit:

- parent slot and purpose;
- dependency class;
- input/output artifact mappings;
- delegated authority ceiling;
- budget reservation;
- timeout and cancellation behavior;
- result admission policy;
- provenance and invalidation.

This preserves separate lifecycle, control, evaluation, and reusable outputs.

## Progressive result model

`get_workflow_result` should return:

- run identity and exact Workflow Type/Implementation;
- lifecycle phase and terminal outcome, if any;
- compact result summary;
- output readiness summary;
- warnings and blockers;
- resource links;
- stable result revision/digest.

Detailed resources should include:

```text
belllabs://runs/{run_id}/status
belllabs://runs/{run_id}/launch
belllabs://runs/{run_id}/bindings
belllabs://runs/{run_id}/result-summary
belllabs://runs/{run_id}/reports
belllabs://runs/{run_id}/reports/{report_id}
belllabs://runs/{run_id}/artifacts
belllabs://runs/{run_id}/evidence
belllabs://runs/{run_id}/events
```

Collections use opaque cursors and stable ordering. Large payloads remain immutable artifact or
object-store references with authorized retrieval.

## Coordinator sandbox, skill, and snapshot

Build this only after the MCP lifecycle and decision-oriented query surface are reliable.

### Base environment

Maintain a reviewed, versioned base sandbox snapshot containing:

- the coordinator Agent Skill;
- JSON Schemas for local draft files;
- helper executables;
- workspace layout and read order;
- examples that reference exact MCP workflows;
- no credentials;
- no mutable catalog copy;
- no tenant-specific data.

Each coordinator session clones the base snapshot into a fresh workspace. Authentication and
fresh authoritative records arrive through MCP at runtime.

### Skill responsibilities

The skill teaches the agent:

- bootstrap and capability negotiation;
- internal-first catalog search;
- progressive contract retrieval;
- the distinction between Workflow Type, Implementation, Run, and Composition;
- the existing-launch and novel-draft paths;
- how to create local design and composition drafts;
- when to materialize a bundle instead of expanding model context;
- how to run local deterministic helpers;
- how to submit server-side validation;
- how to interpret preparation blockers;
- how to launch only after explicit preparation;
- how to retrieve progressive results.

The skill does not contain policy decisions, current aliases, catalog authority, credentials, or
hard-coded environment availability.

### Helper executables

Useful deterministic helpers include:

- validate a local workflow draft against its JSON Schema;
- validate a local composition-plan shape;
- verify materialized file digests against a manifest;
- estimate context/token footprint before loading a bundle;
- produce a concise diff between draft revisions;
- render server validation findings near the affected JSON paths;
- package a draft for an MCP validation call.

Local success is advisory. Server validation is authoritative.

## Deployment evolution

### Phase 1 deployment: co-hosted

Mount the MCP application in the main cloud backend:

- one process or deployment unit;
- one FastAPI lifespan;
- shared Mongo and PostgreSQL clients;
- shared run-control and control-plane services;
- shared Temporal client;
- MCP-specific auth principal mapping and rate limits.

The standalone `python -m app.mcp` entry remains useful for development, but it must use the same
composition root or explicitly advertise read-only mode.

### Possible later deployment: extracted coordinator service

Extract only when operational evidence requires it. Preserve:

- the MCP schemas and resource URIs;
- the coordinator facade contracts;
- application-owned validation and authority;
- end-to-end correlation and idempotency.

Replace in-process ports with authenticated internal clients. Do not copy domain logic into the
MCP service.

## Phased implementation plan

### Phase 1: make the current lifecycle truthful and production-complete

1. Mount the coordinator MCP endpoint in the main FastAPI deployment behind configuration.
2. Compose it from `build_production_coordinator_facade(...)`.
3. Share lifespan-managed backend resources instead of rebuilding a partial provider graph.
4. Add startup validation for every enabled/advertised tool, prompt, and resource provider.
5. Make standalone local mode explicitly `read-only` or fully compose launch dependencies.
6. Standardize the launch result URI on the registered run resource.
7. Guarantee idempotent typed result persistence as part of terminal execution.
8. Bind all advertised prompts or omit unavailable prompts.
9. Correct external-discovery MCP annotations.
10. Add one full MCP-to-Temporal-to-result acceptance test.

Exit condition:

An authorized MCP client can bootstrap, select an existing published Workflow Type, prepare,
launch through actual Temporal execution, and retrieve a durable typed result using every URI
returned by the server.

### Phase 2: add decision-oriented workflow discovery

1. Add cursor-paginated Workflow Type cards.
2. Add exact Workflow Type descriptions.
3. Add launch-contract generation with real JSON Schemas.
4. Add approved Workflow Implementation summaries and semantic comparison.
5. Batch authoritative rehydration and eliminate repeated per-hit reads.
6. Add facets and stable continuation metadata.
7. Return explicit inclusion/exclusion and launchability reasons.

Exit condition:

An agent can choose a Workflow Type and approved implementation without loading raw catalog
definitions or guessing input shape.

### Phase 3: make contracts schema-bearing

1. Introduce exact versioned contract definitions.
2. Cover input, admission, operation, output, evaluation, and decision roles.
3. Add migration readers for current opaque strings.
4. Make MCP schema resources return actual JSON Schema with revision and digest.
5. Validate launch inputs with the same contract used by admission.
6. Add examples and compatibility metadata.

Exit condition:

The agent and helper executables can construct and validate caller-facing requests without
reimplementing admission rules.

### Phase 4: add agentic asset fit, snapshots, and composition planning

1. Add intent-specific asset-fit search over Workflow Type, implementation, operation class,
   authority, environment, and promotion state.
2. Add snapshot summary search and typed compatibility assessment.
3. Add composition-template cards.
4. Add composition-plan validation and preview over linked-run slots.
5. Add exact blocking, degradation, budget, authority, and result-admission findings.
6. Keep every child run separately prepared and admitted.

Exit condition:

The coordinator can construct a governed multi-run plan and explain why each component is
selectable, incompatible, unavailable, or forbidden.

### Phase 5: add progressive reports and result exploration

1. Split compact run status from typed result summary.
2. Add report, artifact, evidence, binding, and event resources.
3. Add cursor pagination and immutable resource identities.
4. Add result summarization prompts only when their bindings are available.
5. Preserve output readiness separately from execution outcome.

Exit condition:

The coordinator can navigate large completed runs without exceeding MCP or model context limits.

### Phase 6: build the coordinator sandbox package

1. Define the versioned base snapshot contract.
2. Create the coordinator Agent Skill.
3. Add deterministic helper executables and local schemas.
4. Add content-addressed workspace bundle materialization.
5. Add manifest verification and stale-resource detection.
6. Test clean clone, concurrent sessions, snapshot upgrade, and credential isolation.

Exit condition:

A fresh coordinator session can clone the reviewed base environment, retrieve current authority
through MCP, construct and validate a workflow or composition draft, launch it through the
governed path, and retrieve results.

## Verification strategy

### MCP contract tests

- every advertised operation has a composed provider;
- unavailable optional operations are omitted or return an explicit bootstrap capability state;
- every returned resource URI is registered and readable;
- tool annotations match side effects;
- tenant and request scope cannot be overridden by payloads;
- envelopes preserve stable error codes and correlation.

### Catalog tests

- cursor pagination is stable under fixed projection generation;
- compact search never makes projection data executable authority;
- selected records are re-resolved by exact ref and digest;
- candidate-only external assets cannot enter preparation;
- implementation comparison reports typed differences.

### Launch tests

- prepare freezes exact refs, digests, policy/environment context, and semantic plan;
- expired or context-mismatched tickets cannot launch;
- duplicate idempotency identities return the prior result or reject conflicting payloads;
- launch reaches the real Temporal workflow family;
- terminal execution writes exactly one immutable typed result;
- retries recover from submission/result-materialization boundaries.

### Composition tests

- a plan cannot target a child outside the declared linked-run slot;
- child authority is the required intersection;
- budget and dependency classes are explicit;
- child output cannot satisfy a parent obligation without result admission;
- composition never rewrites child or parent blueprints.

### Sandbox tests

- a base snapshot contains no secrets or tenant data;
- every session receives a fresh clone;
- materialized contracts verify against exact MCP digests;
- local edits cannot mutate authority;
- stale bundles are detected before preparation;
- server validation can reject a locally valid draft.

## Official end-to-end acceptance task: Viome flagship offering

The official tracer-bullet task for the coordinator system is:

> Find the flagship product or service sold by the biotechnology company Viome.

This is intentionally a small web-research task, not a rigorous Research Mission. Its purpose is
to prove that the coordinator agent can discover, construct, prepare, launch, and inspect a real
StageGraph workflow through the complete governed path.

### Required behavior

The test begins with only:

- the natural-language objective;
- the authenticated coordinator principal and tenant/request scope;
- permission to query the internal catalog and launch the accepted test run;
- configured Tavily access supplied through the normal secret/capability boundary.

The test must not begin with:

- a hard-coded answer about Viome;
- a preassembled run proposal containing all exact asset references;
- a direct call from the test harness to Temporal;
- an external candidate treated as already authorized;
- a workspace containing previously retrieved Viome research.

The coordinator agent must:

1. call `coordinator_bootstrap`;
2. search for an appropriate published general web-research Workflow Type and StageGraph
   implementation;
3. inspect its launch contract and relevant exact resources;
4. find the reviewed, promoted Tavily Agent Skill and its required MCP/tool bindings through the
   internal catalog;
5. select or request a compatible workspace template and fresh workspace;
6. construct the workflow launch proposal from the task, exact selected definitions, admitted
   inputs, and bounded controls;
7. submit server-side validation and repair any permitted proposal errors;
8. prepare the launch and inspect the frozen ticket;
9. launch the accepted run through the coordinator MCP server;
10. allow the StageGraph workflow to execute through Temporal and the real Tavily-backed
    operation binding;
11. poll status and retrieve the durable typed result;
12. follow the returned report, evidence, artifact, and binding resources;
13. provide a concise answer identifying Viome's flagship offering, with the supporting source
    references and uncertainty visible.

“Implemented by the coordinator agent” means the agent performs catalog selection and proposal
construction through MCP. The test harness supplies the objective, identity, policy fixtures,
and runtime dependencies, but it must not silently construct the final workflow on the agent's
behalf.

### Minimum StageGraph shape

The coordinator may select an approved equivalent graph, but the execution must contain the
following semantic work:

```text
frame objective and search plan
  -> retrieve current Viome product/service evidence with Tavily
  -> extract and compare flagship-offering candidates
  -> verify the conclusion against the retrieved sources
  -> emit a concise result and evidence references
```

This graph is deliberately bounded:

- no broad company dossier;
- no biomedical efficacy assessment;
- no medical recommendation;
- no graph ingestion;
- no external mutation;
- no hidden linked Research Mission;
- no claim that marketing evidence establishes scientific validity.

### Workspace requirement

The run must use a governed workspace resolved from an exact Workspace Template. The workspace
should contain:

- a read-only admitted task brief;
- operation-specific working directories;
- Tavily skill instructions and any allowed helper files;
- retrieved source observations or references;
- a candidate-comparison artifact;
- a final concise report;
- a Workspace Materialization Manifest linking governed files to durable records.

The workspace must be fresh for the run. Credentials remain provider-managed and must not be
written into the workspace or snapshot.

### Capability requirement

The Tavily capability used by the run must be:

- an exact promoted catalog asset, not a candidate-only discovery result;
- permitted by the selected Workflow Type and implementation;
- available in the execution environment;
- included in the prepared semantic binding plan;
- recorded in the actual Operation Execution Binding;
- restricted to the operation classes that need web retrieval.

If Tavily is unavailable, forbidden, unpromoted, or incompatible, preparation must fail or return
an explicit non-launchable ticket. The coordinator must not silently replace it when this
specific acceptance task requires Tavily.

### Acceptance assertions

The test passes only when all of the following are demonstrated:

- the coordinator agent selected a published StageGraph path through catalog queries;
- the selected Workflow Type, Implementation, Blueprint, Tavily assets, prompts, models,
  workspace template, and evaluation bindings are exact and digest-pinned;
- the server validated and prepared the proposal before launch;
- the prepared ticket was launchable and later consumed;
- run admission occurred through `RunControlService`;
- Temporal executed the StageGraph workflow rather than a test stub;
- at least one bound operation used the required Tavily capability;
- the run used a fresh governed workspace;
- the final answer was produced from live retrieved evidence rather than a fixture;
- source locators and retrieval observations are preserved;
- the conclusion distinguishes a commercial flagship offering from scientific validation;
- the run reached a valid terminal outcome;
- exactly one immutable typed result was persisted;
- every URI returned by launch and result retrieval was readable;
- the coordinator could retrieve the final report, evidence summary, workspace/binding metadata,
  and result through MCP.

The semantic answer may change as Viome changes its offerings. The test should therefore assert
evidence-backed completion and contract validity, not compare the final answer with a permanent
hard-coded product string.

### Test modes

Maintain two related modes:

1. A deterministic contract test using recorded Tavily responses to verify proposal, binding,
   StageGraph, workspace, result, and URI behavior without network variance.
2. A live acceptance test using current Tavily retrieval to prove the deployed integration and
   produce the current evidence-backed answer.

The live test is the official product demonstration. The recorded test is the repeatable CI
guardrail. Both must exercise the same coordinator, application, and Temporal contracts; only
the external retrieval adapter differs.

## Decisions recorded by the interview

1. Use an authoritative MCP control surface plus an optional sandbox/skill planning environment.
2. Initially co-host the MCP endpoint in the deployed FastAPI backend.
3. Call shared application services in-process in that deployment.
4. Preserve deployable ports for later service extraction.
5. Deliver large contracts through cards, exact resources, then optional workspace bundles.
6. Separate governed existing-workflow launch from novel draft authoring/publication.
7. Compose substantial cross-Workflow-Type work as linked Workflow Runs.
8. Add intent-specific coordinator queries while retaining exact generic retrieval.
9. Return compact results with progressive report, artifact, evidence, and binding resources.
10. Clone coordinator sessions from a versioned base sandbox snapshot and fetch fresh authority
    through MCP.
11. Harden the full lifecycle first, expand coordinator queries second, and build the sandbox
    skill package third.

## Immediate next slice

The first implementation slice should remain narrow:

```text
production composition + mounted endpoint
  -> truthful bootstrap/provider checks
  -> URI correction
  -> durable result materialization
  -> Viome StageGraph coordinator acceptance task
```

Do not begin the sandbox skill or broad composition authoring surface until this slice proves
that the MCP server can reliably execute and report one existing published Workflow Type through
the real backend. The Viome task is the official proof: the coordinator agent must select the
StageGraph implementation, Tavily assets, and workspace; prepare and launch the run; and retrieve
its evidence-backed result through MCP.
