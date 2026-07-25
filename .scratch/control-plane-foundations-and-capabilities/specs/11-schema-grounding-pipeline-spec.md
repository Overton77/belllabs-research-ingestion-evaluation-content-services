# Schema Grounding Pipeline and Canonical Control-Plane Integration

Date: 2026-07-24  
Status: Proposed implementation specification  
Issue: [Control Plane 11](../issues/11-schema-catalogs-and-selections.md)  
Handoff: [Implementation handoff](../handoffs/11-schema-grounding-pipeline-handoff.md)

## 1. Purpose

This specification defines how the successful schema-context-selection experiment becomes a
canonical Bell Labs backend capability rather than a direct CLI experiment.

The target capability:

1. deterministically compiles one authoritative Neo4j GraphQL directive SDL and one governed
   semantic overlay into a reusable Schema Catalog Build;
2. gives agents compact, purpose-appropriate, read-only schema workspaces;
3. admits semantic schema membership only after structural validation and independent review;
4. deterministically expands closure and produces a purpose-bound operation projection;
5. gates graph access on exact deployment compatibility and independent graph authority; and
6. executes bounded, typed read intents while preserving immutable evidence.

The collective name is **Schema Grounding Pipeline**.

The pipeline is composable into Supporting Graph Reconciliation, Knowledge Preflight, ingestion
planning, validation, and other Workflow Types. It does not make schema context authoritative
knowledge and does not grant graph capability.

## 2. Governing vocabulary and boundary with Knowledge Preflight

The following terms are normative:

### 2.1 Schema Grounding Pipeline

A reusable sequence of deterministic operations, one optional/linked semantic-selection workflow,
workspace bindings, graph admission decisions, and bounded graph-reconciliation execution. It is a
capability composition, not a Workflow Type and not a Workflow Run.

### 2.2 Schema Catalog Build

An immutable, content-addressed result of parsing one exact Schema Definition with one exact
semantic-overlay revision under one generator version. It contains a manifest and generated
resources suitable for progressive schema navigation.

### 2.3 Schema Context Selection Workflow

A first-class, independently runnable or linked Workflow Type that chooses semantic schema
membership for a declared purpose. It includes deterministic structural validation and independent
semantic review. Only an accepted selection may become execution context.

### 2.4 Schema Context Derivation

A deterministic operation that expands accepted membership into complete structural closure and
produces a purpose-bound Schema Operation Projection. It cannot add semantic membership.

### 2.5 Schema Workspace Materialization

The shared operation defined by the schema capability specification and owned for implementation by
Issue 13. It verifies exact resources and manifests, writes only to declared read-only workspace
slots, and emits an immutable Schema Workspace Binding.

### 2.6 Supporting Graph Reconciliation Workflow

A bounded observational Workflow Type, or a declared stage inside another Workflow Type, that
answers one admitted matching/reconciliation question through typed, bounded graph read intents.
It may emit Graph Match Candidates and reconciliation evidence. It cannot resolve identity, mutate
the graph, or claim broad Knowledge Preflight coverage.

This is the canonical target name for the experiment class currently called
`ReportGraphReconciliationWorkflow`.

### 2.7 Knowledge Preflight Workflow

The broader Workflow Type already defined in `biotech-meta/docs/CONTEXT.md`. It performs broad,
purpose-bound observational discovery and owns:

- the Knowledge Preflight Brief;
- Coverage Matrix and Query Plan revisions;
- multimodal Knowledge Retrieval Observations;
- graph/prior-work candidates;
- contradiction candidates and gap hypotheses;
- the immutable Knowledge Preflight Snapshot;
- purpose-bound Freshness Assessment; and
- Decision Report.

Knowledge Preflight composes the Schema Grounding Pipeline. A Supporting Graph Reconciliation
result may be one input to Knowledge Preflight, but it is not a preflight snapshot.

## 3. Proven baseline

The implementation is based on a completed live experiment, not a speculative design.

### 3.1 Successful candidate

```text
.scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-3
```

### 3.2 Baseline

```text
.scratch/schema-context-selection-runs/live-windows-bind-9
```

### 3.3 Accepted comparison

```text
.scratch/schema-context-selection-runs/comparisons/
  official-catalog-v1-live-20260723-3-vs-live-windows-bind-9/
```

Comparison digest:

```text
sha256:c06ab6c47efa3cf6ade04b440d2ec07c4cfba512e65dea3552a7b09a1cf6595c
```

All nine gates passed:

1. identical workload input digests;
2. completed candidate;
3. independently accepted selection;
4. required core semantic membership;
5. exact `IMPLEMENTS` product/platform relationship;
6. preserved 100% oracle recall;
7. all offered products recovered;
8. five successful, zero rejected, zero failed bounded queries; and
9. exact deployed-schema compatibility.

Resource and usage changes:

| Metric | Baseline | Candidate | Change |
|---|---:|---:|---:|
| Catalog resources | 1,046 | 67 | -93.6% |
| Catalog bytes | 7,837,446 | 1,559,712 | -80.1% |
| Input tokens | 394,797 | 235,409 | -40.4% |
| Total tokens | 406,001 | 258,035 | -36.4% |
| Oracle recall | 1.0 | 1.0 | preserved |
| Query records | 32 | 32 | preserved |
| Elapsed time | 234,192 ms | 291,395 ms | +24.4% |

The elapsed-time result is a single-run observation and must remain visible without being treated
as a stable regression until repeated runs establish a distribution.

## 4. Authority and persistence model

### 4.1 Authorities

- The versioned Neo4j GraphQL directive SDL is the Schema Definition authority.
- The governed semantic overlay is authored policy input, not agent output.
- A Schema Catalog Build is deterministic derived context.
- The graph-schema deployment process is the only issuer of a production Schema Deployment
  Manifest.
- PostgreSQL owns Workflow Run admission, lifecycle, commands, budget ledgers, linked runs, and
  durable outbox effects.
- MongoDB/Beanie owns immutable document-shaped catalog, selection, derivation, binding,
  reconciliation, and evaluation metadata.
- Object storage owns large immutable schema bundles, captured query payloads, reports, and
  promoted files.
- Temporal owns durable execution mechanics, not domain truth or persistence authority.
- Neo4j owns approved canonical graph knowledge and is read-only to this pipeline.

### 4.2 Non-authoritative material

The following can inform a decision but cannot create authority:

- schema descriptions, module names, aliases, navigation skills, or candidate rankings;
- sandbox files or workspace paths;
- agent selections or reviews before application admission;
- live Neo4j introspection;
- a provider run ID or transcript;
- a retrieved result or similarity score;
- possession of a Schema Workspace Binding.

### 4.3 Durable identity

Historical records must bind immutable IDs, revisions, digests, environment identities, and durable
object references. They must not depend on:

- host absolute paths;
- current mutable aliases;
- sandbox/container IDs;
- timestamps included in logical content digests;
- provider request IDs as semantic identity; or
- Temporal activity attempt numbers as semantic revisions.

## 5. Pipeline invariants

1. There is one published Schema Definition identity for one SDL content hash.
2. Identical normalized source, overlay, and generator inputs produce one logical catalog digest.
3. Source location does not affect the physical-schema digest.
4. Every generated resource proves lineage to the exact Schema Definition and Catalog Build.
5. Governed module membership is input policy; an agent cannot redefine it during a run.
6. Semantic selection is separate from structural validation, independent review, closure,
   projection, materialization, and graph authority.
7. The selecting agent cannot approve its own selection.
8. A reviewer must bind the exact selection identity. The host never repairs reviewer output.
9. Deterministic expansion cannot add semantic membership.
10. A purpose-bound projection cannot be silently reused for another purpose.
11. Workspace resources are read-only and bounded by an exact manifest profile.
12. Schema compatibility requires exact deployed-SDL hash equality, not inferred similarity.
13. Graph authority is checked separately from schema compatibility.
14. Agents submit typed query intents, never arbitrary Cypher.
15. All report-derived values remain query parameters.
16. Reads are bounded and embedding values are removed from persisted results.
17. Every attempted query intent has exactly one persisted typed result.
18. Successful-zero, successful-nonzero, rejected, and failed results remain distinct.
19. Supporting Graph Reconciliation is observational and cannot mutate or resolve identity.
20. Knowledge Preflight consumes this pipeline but owns its broader coverage/snapshot semantics.

## 6. Component 1 — SchemaCatalogBuild

### 6.1 Inputs

A `SchemaCatalogBuildRequest` must contain:

- request and idempotency identities;
- exact Schema Definition durable reference, SHA-256 digest, media type, and optional S3 version ID;
- exact governed semantic-overlay revision and digest;
- catalog schema version;
- parser/generator version;
- normalization-policy version;
- requested publication target;
- actor/authority and request scope; and
- occurrence time excluded from logical content identity.

### 6.2 Deterministic stages

```text
load exact bytes
-> verify declared digest
-> parse physical schema
-> validate directive/type/relationship references
-> load governed overlay
-> validate overlay against physical element IDs
-> derive semantic catalog
-> render Tier 0 and resource profiles
-> build canonical resource manifest
-> rebuild and compare logical digest
-> publish immutable bundle
-> persist accepted build metadata
```

### 6.3 Required outputs

The build record must include:

- build ID and status;
- Schema Definition reference and content digest;
- semantic-overlay revision/digest;
- parser/generator/catalog/normalization versions;
- physical and derived catalog digests;
- resource-manifest digest;
- object/bundle digest and durable reference;
- resource count and total bytes;
- Tier 0 size;
- profile names and path manifests;
- validation decision and diagnostics;
- predecessor/successor lineage where applicable; and
- publication evidence.

### 6.4 Official v1 profile

The accepted v1 baseline contains:

- generator version `typed-schema-catalog-v1`;
- Tier 0 at 49,911 bytes;
- 91 selectable node types;
- 22 relationship-property types represented separately;
- 206 relationship types;
- 14 governed TruDiagnostic-relevant overlay elements across five modules;
- `selection-tier0`; and
- `selection-candidates`.

Tier 0 must stay at or below 50 KiB for the authoritative acceptance fixture.

### 6.5 Profile semantics

`selection-tier0` provides:

- catalog/resource manifest;
- Tier 0 orientation;
- its own profile declaration; and
- the governed schema-navigation skill.

`selection-candidates` adds:

- deterministic report-derived candidate manifest; and
- one canonical JSON detail resource per admitted node/relationship candidate.

Profiles must not contain byte-identical Markdown and JSON twins or a second drill-down
representation. The full catalog bundle may remain available to auditors/services but is not
mounted to selector/reviewer agents by default.

## 7. Component 2 — SchemaContextSelectionWorkflow

### 7.1 Run identity and launch modes

The workflow is a published Workflow Type. It may be:

- submitted directly through a Run Request;
- launched through a declared linked-run slot;
- invoked as an inline bounded operation only when the consuming Workflow Type explicitly permits
  that shortcut and accepts the same output contract.

A broad, ambiguous, expensive, reusable, or review-heavy selection should use a distinct linked
Workflow Run.

### 7.2 Inputs

The admitted request binds:

- purpose and intended operation classes;
- exact Schema Definition and Catalog Build;
- report/artifact/input references and digests;
- coverage obligations;
- allowed semantic modules/profile;
- workspace binding;
- selection/review policy and bounded revisions;
- model/runtime/agent/prompt revisions;
- authority, budgets, and sensitive-data policy; and
- optional admitted prior selection.

### 7.3 StageGraph blueprint

```text
Materialize selection-tier0 and selection-candidates
-> Semantic Selector
-> Deterministic Structural Validation
-> Independent Semantic Reviewer
-> Accept
   or one bounded Semantic Revision
-> Persist terminal selection outcome
```

The selector and reviewer are separate agent operations with separate immutable Operation Execution
Bindings. They may use the same provider/model profile but must not share reviewer authority.

### 7.4 Structural validation

Validation must reject:

- unknown node/relationship names;
- property names presented as semantic members;
- duplicate or noncanonical membership;
- schema/catalog lineage mismatch;
- missing required endpoints under declared policy;
- unsupported purpose/operation classes;
- nonselectable relationship-property types; and
- stale or cross-purpose admitted context.

### 7.5 Independent review

Review assesses:

- purpose and obligation coverage;
- overbroad and unjustified selections;
- temporal identity/snapshot distinctions;
- provenance boundaries;
- near misses and unresolved mappings;
- exact relationship discrimination; and
- structural diagnostic findings.

The review must return the exact selection ID. If it does not:

1. persist the mismatched review as a discarded attempt;
2. provide a bounded retry reason with the expected and observed identities;
3. rerun the reviewer against the same immutable draft;
4. never rewrite or stamp the reviewer’s output; and
5. fail closed if the retry still does not bind.

Reviewer binding retries are infrastructure/operation attempts, not semantic selection revisions.

### 7.6 Outputs

Terminal records are:

- selection draft revision(s);
- deterministic validation diagnostic(s);
- independent review(s);
- accepted selection, rejection, or bounded-exhaustion decision;
- exact usage and timing;
- immutable transcript/output references under the data policy; and
- evaluation metrics.

## 8. Component 3 — SchemaContextDerivation

### 8.1 Expansion

Expansion starts only from an accepted selection and deterministically includes:

- complete selected node definitions;
- physical relationship endpoints and relationship fields;
- relationship-property types;
- referenced enums and unions;
- implemented interfaces;
- relevant directives and indexes;
- identity/search candidates; and
- selected SDL sufficient for the purpose.

Closure diagnostics must distinguish selected semantic members from structurally included elements.

### 8.2 Operation projection

The projection binds:

- projection ID/version/digest;
- purpose;
- accepted selection and expanded-slice digests;
- source Schema Definition/Catalog Build;
- permitted query kinds;
- allowed labels, relationships, fields, traversals, and maximum depth;
- exact identity fields;
- eligible live full-text/vector capabilities;
- denied capabilities;
- projection policy version; and
- admission decision.

Live index availability may narrow an already permitted capability. It cannot invent or broaden
schema authority.

## 9. Component 4 — SchemaWorkspaceMaterialization and graph gate

Issue 13 owns the production implementation. This specification defines the integration contract.

### 9.1 Stage-scoped bindings

Prefer separate bindings rather than one universal workspace:

| Stage | Logical profile/binding |
|---|---|
| Selector orientation | `selection-tier0` |
| Selector/reviewer detail | `selection-candidates` |
| Derivation | accepted selection plus catalog service access |
| Query planner/executor | accepted selection, expanded slice, operation projection, compatibility decision, and bounded live-capability snapshot |

The runtime query profile may be introduced as `graph-query-runtime`; until published, it is a
typed request over exact resources, not an informal folder copy.

### 9.2 Compatibility

For live graph access:

```text
Catalog Build Schema Definition digest
  == active, non-revoked Schema Deployment Manifest deployed SDL digest
```

The environment, database, deployment identity, issuer authority, and manifest lineage must also
match the admitted target. Introspection is diagnostic only.

### 9.3 Capability

After compatibility, a separate authority check must admit:

- graph read capability;
- exact database/environment;
- allowed query kinds and bounds;
- permitted labels/relationships/indexes;
- credentials by secret reference;
- budget reservation; and
- sensitive-data/result policy.

No credentials, Docker socket, or graph client are mounted into selector/reviewer sandboxes.

## 10. Component 5 — SupportingGraphReconciliationWorkflow

### 10.1 Purpose

This workflow answers one admitted, bounded graph matching or reconciliation question. The
TruDiagnostic fixture asks which existing organization, products, lab tests, panels, biomarkers,
metrics, and platforms are already represented.

### 10.2 StageGraph blueprint

```text
Admission
-> accepted Schema Context or linked selection
-> deterministic derivation
-> workspace binding + compatibility/capability gate
-> capability snapshot
-> query brief
-> bounded query planning
-> host validation and execution
-> evidence-reference verification
-> evaluation
-> result promotion and terminalization
```

### 10.3 Query authority

The planner may call only a host-provided typed `execute_read_intent` operation. The host:

- validates lineage and purpose against the projection;
- compiles from an allowlisted query kind;
- applies parameters separately from query structure;
- enforces label/relationship/field/index/depth/limit bounds;
- strips embedding fields and truncates bounded collections;
- executes with a read-only Neo4j principal where possible; and
- persists the intent and result before returning the bounded result to the planner.

Custom Cypher is prohibited.

### 10.4 Required evidence

The result records:

- reconciliation question and query goals;
- exact intent/result references in execution order;
- matched existing entities;
- observed relationships;
- aliases used;
- match method and bounded confidence class;
- unresolved candidates;
- schema mismatches and legacy mappings;
- query failures;
- stopping rationale;
- compatibility/workspace/projection references;
- usage, timing, and evaluation; and
- limitations.

The agent’s final evidence references must exactly equal the persisted host intent/result sequence.

## 11. Knowledge Preflight composition

`KnowledgePreflightWorkflow` must compose, not duplicate, this pipeline:

```text
Run admission and exact inputs
-> Schema Grounding Pipeline
-> subject/intent normalization
-> Knowledge Preflight Coverage Matrix
-> multimodal Knowledge Preflight Query Plan
-> graph/prior-work/search retrieval observations
-> candidate, contradiction, gap, and coverage evaluation
-> optional bounded cycle or linked work
-> immutable Knowledge Preflight Snapshot
-> Freshness Assessment
-> Decision Report
```

Two blueprint variants remain valid:

- `StageGraphPreflight` for explicit stages/branches/cycles; and
- `GoalDirectedPreflight` for adaptive, independently evaluated iteration.

Both use identical pipeline contracts and emit the same preflight output contract.

## 12. Canonical application package plan

### 12.1 Keep

```text
app/domain/schema_catalog/
app/domain/schema_context/
app/integrations/neo4j_read_executor.py
```

These already contain the core typed behavior. Refactor names only through explicit contract
versions.

### 12.2 Replace experiment dependency

The current file:

```text
app/application/schema_context_selection.py
```

imports its service from `app.experiments`. This dependency direction is temporary and forbidden in
the canonical state.

Create application-owned modules such as:

```text
app/application/schema_catalog_build.py
app/application/schema_catalog_repository.py
app/application/schema_context_selection.py
app/application/schema_context_derivation.py
app/application/schema_workspace_binding.py
app/application/supporting_graph_reconciliation.py
```

Application modules depend on domain contracts and ports, never on experiment runners or provider
SDKs.

### 12.3 Provider adapters

Move provider-specific behavior to:

```text
app/integrations/schema_catalog_payloads.py
app/integrations/schema_agent_runtime.py
app/integrations/neo4j_read_executor.py
app/integrations/schema_deployment_manifests.py
```

The existing OpenAI Agents/Docker harness is prior art for the adapter. Canonical execution must
flow through `OperationExecutionService`, durable sandbox materialization, secret references,
runtime policy, and budget settlement.

### 12.4 Temporal

Add workflow-specific activity adapters and worker registration:

```text
app/temporal/schema_grounding_activities.py
app/temporal/schema_context_selection_workflow.py      # only if generic StageGraph is insufficient
app/temporal/supporting_graph_reconciliation_workflow.py # same rule
```

Prefer published StageGraph definitions interpreted by the existing `StageGraphWorkflow`. Add a
dedicated Temporal workflow only for workflow-specific durable signals, queries, or history
behavior that the interpreter cannot express.

Worker startup must register:

- StageGraph orchestration;
- Operation Execution Workflow/activities;
- workspace/artifact/linked-run activities;
- schema catalog/derivation/reconciliation activities; and
- the published task queues selected by the Effective Run Configuration.

### 12.5 API

Add an authenticated query router, for example:

```text
app/api/schema_grounding.py
```

Commands continue through:

```text
POST /run-control/run-requests
POST /run-control/runs/{run_id}/commands
POST /run-control/runs/{run_id}/operations
```

The new router exposes reads such as:

```text
GET /schema-grounding/catalog-builds/{build_id}
GET /schema-grounding/catalog-builds/{build_id}/resources
GET /schema-grounding/selections/{selection_id}
GET /schema-grounding/projections/{projection_id}
GET /schema-grounding/runs/{run_id}/binding
GET /schema-grounding/runs/{run_id}/reconciliation
GET /schema-grounding/runs/{run_id}/evaluation
```

Exact route spelling may follow existing API conventions, but there must be no alternate ungoverned
start endpoint.

## 13. Control-plane definitions and execution bindings

Publish immutable definitions for:

- Workflow Type `schema-context-selection`;
- StageGraph blueprint `schema-context-selection-v1`;
- Workflow Type `supporting-graph-reconciliation`;
- StageGraph blueprint `supporting-graph-reconciliation-v1`;
- control profiles for revision bounds, query limits, compatibility requirements, and authority;
- runtime profiles for deterministic/Python, OpenAI agent, and Neo4j read operations;
- workspace templates with exact read-only and exclusive-write slots;
- evaluation profiles containing the nine proven acceptance gates plus runtime metrics;
- workflow configurations for the official catalog/overlay and TruDiagnostic fixture.

Operation contracts are already represented on `OperationExecutionRequest.operation_contract_ref`.
Until the control-plane catalog has a first-class operation-contract definition kind, bind
pre-provisioned immutable contract references through the existing validated extension boundary.
Do not overload an unrelated `DefinitionKind` or leave an unversioned free-form name.

The Effective Run Configuration freezes:

- exact Workflow Type/blueprint/profile revisions;
- operation contract and output schema refs;
- schema definition/catalog/overlay refs;
- model, tools, skills, prompts, guardrails, and delegation ceiling;
- workspace template and slot contract;
- authority and capability ceilings;
- secret references, never secret values;
- stage/run budget reservations;
- evaluation and sensitive-data policies; and
- permitted linked-run slots.

## 14. Run-control and projection model

### 14.1 Commands

Use existing Run Requests and lifecycle commands for:

- admission/start;
- wait/pause/resume/cancel;
- continuation or added-budget decisions;
- linked selection/reconciliation requests;
- linked-result admission/rejection/deferment; and
- terminalization/finalization.

### 14.2 Durable events

At minimum publish:

- catalog build requested/validated/published/rejected;
- workspace binding materialized/rejected;
- selection revision created/validated/reviewed/accepted/rejected;
- reviewer binding mismatch discarded;
- derivation completed/rejected;
- compatibility accepted/rejected;
- graph capability admitted/rejected;
- query intent accepted/rejected/executed/failed;
- evidence assembled/invalid;
- evaluation accepted/rejected;
- artifact promoted; and
- run completed/partially completed/failed/cancelled.

Events carry durable record references and digests, not raw large payloads.

### 14.3 Query projections

Reconnectable clients must be able to query:

- current run/stage/revision state;
- accepted selection and review outcome;
- workspace and compatibility status;
- query counts/status without secrets;
- budget/usage and timing;
- output/evidence references;
- evaluation gates; and
- failure code plus safe diagnostic.

## 15. Failure model

Use stable typed failure codes. Required classes include:

- `schema_source_digest_mismatch`;
- `schema_parse_failed`;
- `semantic_overlay_invalid`;
- `catalog_nondeterministic`;
- `catalog_publication_conflict`;
- `workspace_profile_invalid`;
- `selection_structurally_invalid`;
- `selection_review_rejected`;
- `selection_review_binding_mismatch`;
- `selection_revision_exhausted`;
- `derivation_lineage_mismatch`;
- `projection_purpose_mismatch`;
- `deployment_manifest_missing`;
- `deployment_manifest_revoked`;
- `schema_deployment_mismatch`;
- `graph_capability_denied`;
- `query_intent_rejected`;
- `query_execution_failed`;
- `query_evidence_mismatch`;
- `budget_exhausted`; and
- `sensitive_data_policy_denied`.

Preparation failures must occur before external semantic side effects where possible. Error records
must suppress credentials, raw provider exceptions, and restricted input content.

## 16. Security and data handling

- Schema resources are context, not capability.
- OpenAI transmission of nonpublic reports, structured candidates, schema workspaces, or bounded
  graph results requires an admitted sensitive-data policy and caller authority.
- Secrets resolve just in time from references and never enter Temporal payloads, prompts,
  artifacts, workspaces, traces, events, snapshots, or errors.
- Selector/reviewer sandboxes receive only allowlisted read-only files and no Docker socket or
  Neo4j credentials.
- Neo4j execution uses the narrowest read principal available.
- Query values are parameters; query structure comes from host compilers.
- Large or sensitive results are externalized by digest and exposed through authorized query
  surfaces.
- Object storage buckets block public access, enforce bucket ownership, enable versioning, and use
  server-side encryption.

## 17. Testing and evaluation

### 17.1 Deterministic tests

- golden parse/build/rebuild;
- storage-path relocation;
- `IN`/`OUT` endpoint direction;
- malformed SDL and overlay drift;
- content-addressed successor rules;
- Tier 0 size;
- profile uniqueness and stale-destination rejection;
- structural closure;
- projection purpose/authority;
- canonical serialization.

### 17.2 Workflow tests

- selector then independent reviewer;
- exact reviewer binding and bounded retry;
- semantic revision versus operation retry identity;
- rejected selection prevents graph driver creation;
- compatibility failure prevents graph driver creation;
- accepted selection produces one result per intent;
- evidence references exactly match persisted intents/results;
- cancellation, wait/pause/resume, and linked-run result admission;
- budget reconciliation and idempotency.

### 17.3 Integration tests

- MongoDB immutable document lineage;
- S3 bundle digest/version/control verification;
- PostgreSQL run-control lifecycle and outbox;
- Temporal workflow/activity retry;
- real workspace read-only materialization;
- exact production deployment-manifest fixture;
- Neo4j read-only fixture with known zero/success/failure outcomes;
- API authorization and tenant scoping;
- durable realtime replay.

### 17.4 Acceptance workload

The TruDiagnostic replay remains the mandatory regression fixture. It must compare against
`live-windows-bind-9` with:

- identical schema/report/structured-candidate digests;
- candidate completion;
- independent acceptance;
- core semantic membership;
- exact `IMPLEMENTS`;
- oracle recall no lower than 1.0;
- all three offered products;
- zero rejected/failed required queries;
- schema compatibility;
- resource/byte/token/request/timing metrics; and
- normalized selection stability over repeated runs.

Latency must be measured over repeated candidates because the first optimized run was slower despite
substantial context/token reduction.

## 18. Migration and rollout plan

### Phase 0 — Freeze the proof

- Preserve baseline, successful candidate, failed reviewer-binding run, and comparison artifacts.
- Preserve the current 136-passed repository regression result.
- Treat experiment paths as reference fixtures during promotion.

### Phase 1 — Canonical pure services

- Remove canonical application imports from `app.experiments`.
- Introduce application ports/services and immutable repository contracts.
- Keep CLI behavior as a thin adapter over the new service.
- Prove byte-for-byte/gate-equivalent outputs.

### Phase 2 — Durable catalog and selection

- Publish catalog bundles to object storage by digest.
- Persist build/selection/review/derivation metadata through MongoDB adapters.
- Publish control-plane definitions and compile a real Effective Run Configuration.
- Execute agent stages through Operation Execution Bindings.

### Phase 3 — Run-control and Temporal

- Admit standalone and linked selection/reconciliation runs through Run Requests.
- Register StageGraph/activity workers and exact task queues.
- Expose authenticated query projections and durable events.
- Prove retry, cancellation, budget, and idempotency behavior.

### Phase 4 — Production graph gate

- Consume Issue 12’s real Schema Deployment Manifest.
- Consume Issue 13’s durable Schema Workspace Binding and graph capability gate.
- Delete test-attestation use from canonical live execution.
- Prove no Neo4j client/query exists before both gates pass.

### Phase 5 — Knowledge Preflight composition

- Bind the pipeline into `StageGraphPreflight`.
- Reuse the same contracts from `GoalDirectedPreflight`.
- Add Coverage Matrix, multimodal observations, snapshot, freshness, and Decision Report through the
  existing Knowledge Preflight specifications.

### Phase 6 — Default and cleanup

- Run repeated A/B/stability/latency suites.
- Make the canonical service path the default.
- Keep experiments as evaluation fixtures only.
- Remove duplicated legacy catalog materializers only after no canonical consumer remains.

## 19. Completion definition

Issue 11 is complete when:

1. the catalog, selection, and derivation behavior is application-owned and durable;
2. canonical execution uses published control-plane definitions, Run Request admission, Operation
   Execution Bindings, Temporal orchestration, budgets, workspaces, and public query projections;
3. no canonical `app/` module imports `app.experiments`;
4. large payloads are content-addressed in object storage and document metadata is queryable;
5. selection/review/closure/projection contracts preserve the successful fixture behavior;
6. production graph access is integrated only through Issues 12 and 13;
7. Supporting Graph Reconciliation is correctly distinguished from Knowledge Preflight;
8. all nine TruDiagnostic comparison gates still pass;
9. repeated-run resource, token, stability, and timing evidence is recorded; and
10. repository-wide lint, type, unit, integration, and production-shaped acceptance tests pass.

## 20. Out of scope

- Schema authoring or domain-model changes.
- Replacing the directive SDL with introspection.
- Graph mutation, identity resolution, ingestion, or repair.
- Full Knowledge Preflight implementation inside Issue 11.
- Final vector retrieval tuning.
- A dashboard before command/query/event contracts.
- Treating local experiment directories as production persistence.
