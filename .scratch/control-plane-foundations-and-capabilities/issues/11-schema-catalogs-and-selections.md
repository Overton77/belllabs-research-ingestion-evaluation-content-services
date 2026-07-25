# 11 — Promote the proven Schema Grounding Pipeline into canonical backend services

**What to build:** Promote the successful schema-catalog and graph-reconciliation experiment into
application-owned, durable, reusable backend capabilities. The implementation must build one
content-addressed catalog from the authoritative Neo4j GraphQL directive SDL; select, independently
review, validate, expand, and project purpose-bound schema context; materialize only the admitted
workspace profile; enforce deployment compatibility and graph authority before Neo4j access; and
persist bounded reconciliation evidence through the Bell Labs control plane and run-control
surfaces.

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/11

**Replaces:** the original Issue 11 body, which described catalog generation but explicitly left
the standalone selection workflow and production integration out of scope. The experiment has now
proved those seams together and established a better baseline.

**Companion documents:**

- [Normative specification](../specs/11-schema-grounding-pipeline-spec.md)
- [Implementation handoff](../handoffs/11-schema-grounding-pipeline-handoff.md)

**Read first:**

1. [Local control-plane ticket index](INDEX.md)
2. `biotech-meta/docs/CONTEXT.md`
3. `biotech-meta/docs/specs/pre-research/control-plane-capabilities/01-schema-catalog-deployment-manifest-and-workspace-materialization.md`
4. `biotech-meta/docs/spec_synthesis/2026-07-18-knowledge-preflight-workflow-synthesis.md`
5. `tests/SCHEMA_CONTEXT_SELECTION_WORKFLOW_HANDOFF.md`

## Why this replacement exists

The prior issue was written before the complete experiment existed. The current repository now
contains a typed deterministic catalog, governed semantic overlay, compact workspace profiles,
independent schema-context review, deterministic closure and operation projection, a compatibility
gate, guarded Neo4j read intents, persisted evidence, and an accepted live A/B comparison.

The successful candidate is:

```text
.scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-3
```

The accepted comparison is:

```text
.scratch/schema-context-selection-runs/comparisons/
  official-catalog-v1-live-20260723-3-vs-live-windows-bind-9/
```

All nine acceptance gates passed. Relative to `live-windows-bind-9`, the candidate:

- reduced catalog resources from 1,046 to 67 (`-93.6%`);
- reduced catalog bytes from 7,837,446 to 1,559,712 (`-80.1%`);
- reduced OpenAI input tokens from 394,797 to 235,409 (`-40.4%`);
- reduced total tokens from 406,001 to 258,035 (`-36.4%`);
- preserved 100% oracle recall and all 32 observed records;
- executed five of five bounded queries with zero rejection or failure;
- preserved all three offered products and exact `IMPLEMENTS` discrimination;
- passed exact deployed-schema compatibility and independent semantic acceptance.

One run is not a latency benchmark: candidate elapsed time was 24.4% higher. Production rollout
must retain repeated-run latency and selection-stability evaluation.

## Canonical name and vocabulary

Use **Schema Grounding Pipeline** for the reusable capability sequence. It is not itself a Workflow
Type and does not create one additional Workflow Run merely by being named.

The five components are:

| # | Canonical component | Kind | Responsibility |
|---|---|---|---|
| 1 | `SchemaCatalogBuild` | shared deterministic operation | Parse and validate the exact Schema Definition plus governed semantic overlay; derive and publish a content-addressed catalog and compact profiles. |
| 2 | `SchemaContextSelectionWorkflow` | independently runnable or linked Workflow Type | Select semantic membership, perform deterministic structural validation, obtain independent semantic review, and emit an accepted selection. |
| 3 | `SchemaContextDerivation` | shared deterministic operation | Expand structural closure and create a purpose-bound Schema Operation Projection without adding semantic membership. |
| 4 | `SchemaWorkspaceMaterialization` | shared governed operation | Bind exact resources to read-only workspace slots and, for graph work, require strict schema deployment compatibility plus separate graph authority. Issue 13 owns the durable production binding and gate. |
| 5 | `SupportingGraphReconciliationWorkflow` | bounded Workflow Type or declared stage | Compile and execute allowlisted, bounded read intents and persist evidence for one declared matching/reconciliation question. It is the canonical successor to the experiment name `ReportGraphReconciliationWorkflow`. |

`KnowledgePreflightWorkflow` remains the broader first-class observational Workflow Type defined in
`CONTEXT.md`. It composes the Schema Grounding Pipeline, but additionally owns a
`KnowledgePreflightBrief`, coverage matrix, multimodal query plan, retrieval observations,
candidate/contradiction/gap assessment, immutable Knowledge Preflight Snapshot, purpose-bound
freshness assessment, and Decision Report.

A bounded `SupportingGraphReconciliationWorkflow` must not claim broad Knowledge Preflight
coverage. Conversely, Knowledge Preflight must not reimplement catalog parsing, selection,
derivation, materialization, compatibility, or guarded Neo4j execution.

## Proven experiment source

Keep these paths as the executable reference until promotion is complete:

```text
app/domain/schema_catalog/
app/domain/schema_context/
app/application/schema_catalog.py
app/application/schema_workspace.py
app/experiments/schema_context_selection/
app/integrations/neo4j_read_executor.py
schema-catalog/semantic-overlay.v1.json
schema-catalog/source-reference.v1.json
scripts/run_schema_context_selection.py
scripts/compare_schema_context_runs.py
tests/test_schema_catalog_core.py
tests/test_schema_catalog.py
tests/test_schema_workspace.py
tests/test_schema_context_selection.py
tests/test_schema_expansion.py
tests/test_schema_operation_projection.py
tests/test_graph_query_intents.py
tests/test_neo4j_read_executor.py
tests/test_report_graph_reconciliation_workflow.py
tests/test_report_graph_reconciliation_gates.py
tests/test_schema_context_run_comparison.py
```

The authoritative Schema Definition is:

```text
biotech-kg/src/schema/neo4jbiotechschema.graphql
sha256:86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112
```

The immutable private S3 reference is recorded in
`schema-catalog/source-reference.v1.json`. The experiment’s test-only deployment attestation is not
a production Schema Deployment Manifest.

## Target execution flow

```text
Exact Schema Definition + governed Semantic Overlay
  -> SchemaCatalogBuild
  -> content-addressed Catalog Build + Tier 0 + selection profiles
  -> SchemaWorkspaceMaterialization(selection-tier0 / selection-candidates)
  -> SchemaContextSelectionWorkflow
       selector
       -> deterministic structural validation
       -> independent reviewer
       -> bounded revision or exact acceptance
  -> SchemaContextDerivation
       accepted selection
       -> deterministic closure
       -> purpose-bound operation projection
  -> SchemaWorkspaceMaterialization(runtime projection)
       exact Schema Deployment Manifest comparison
       + independent graph capability admission
  -> SupportingGraphReconciliationWorkflow
       host-compiled/validated read intents
       -> guarded Neo4j executor
       -> immutable intent/result evidence
       -> reconciliation result
```

Knowledge Preflight may consume the same flow before its broader coverage and retrieval stages.

## Required application integration

### 1. Canonical package boundary

- Keep framework-independent catalog and schema-context rules under `app/domain/`.
- Replace `app/application/schema_context_selection.py`’s import from `app.experiments` with an
  application-owned service and ports.
- Move reusable orchestration from `app/experiments/schema_context_selection/` into
  `app/application/`, `app/integrations/`, and `app/temporal/` according to responsibility.
- Leave only fixture runners, A/B comparison code, experiment prompts, and historical evaluation
  harnesses under `app/experiments/`.
- Do not allow `app/domain` or canonical `app/application` modules to import `app.experiments`.

### 2. Durable records and object storage

- Persist Schema Catalog Build metadata, resource manifests, semantic-overlay revision,
  selection/validation/review/acceptance records, expanded slices, operation projections,
  compatibility decisions, workspace bindings, query intents/results, and reconciliation metadata
  as immutable application documents.
- Store large catalog bundles, captured query payloads, reports, and promoted summaries in object
  storage by digest.
- Keep PostgreSQL authoritative for run admission, lifecycle, budgets, command idempotency, linked
  runs, and outbox events.
- Keep MongoDB/Beanie authoritative for document-shaped schema and reconciliation records.
- Keep Neo4j read-only to these workflows.
- Never persist host-only absolute paths as durable identity.

### 3. Control-plane definitions

Publish exact immutable revisions for:

- Workflow Type `schema-context-selection`;
- Workflow Type `supporting-graph-reconciliation`;
- operation contracts `schema-catalog-build`, `schema-context-derive`,
  `schema-workspace-materialize`, and `neo4j-bounded-read`;
- StageGraph blueprints for selection and reconciliation;
- runtime, control, workspace, agent, prompt, evaluation, and sensitive-data profiles;
- the semantic-overlay revision and catalog-generator version.

The Effective Run Configuration must freeze every exact reference, authority ceiling, budget,
workspace slot, model/runtime choice, output schema, and data-handling policy before execution.

### 4. Run-control and Temporal execution

- Start standalone or linked workflows through the existing typed Run Request admission boundary.
- Use the existing StageGraph interpreter for the first canonical blueprints unless a genuinely
  workflow-specific durable signal/query requires a dedicated Temporal workflow.
- Execute model calls, catalog publication, workspace materialization, Neo4j snapshots, queries,
  and persistence as activities/application services, never inside deterministic Temporal code.
- Execute agent stages through the canonical Operation Execution Binding and immutable budget
  reservation.
- Preserve semantic revision identities separately from Temporal activity attempts and provider
  request IDs.
- Publish durable events and queryable projections for every stage, selection revision, gate,
  query result, and terminal outcome.

### 5. Public control surfaces

- Retain `/run-control/run-requests` as the command authority; do not add a bypassing “run now”
  endpoint.
- Add authenticated query surfaces for catalog builds/resources, accepted selections, projections,
  compatibility/workspace bindings, reconciliation observations, and evaluation summaries.
- Expose typed JSON Schemas for the new commands and query documents.
- Publish authorized Socket.IO projection changes with durable cursors; raw model deltas may remain
  ephemeral.
- Allow cancellation, wait/pause/resume, continuation budget decisions, and linked-result admission
  only through existing run-control commands.

## Dependency and ownership boundaries

- GitHub Issues 1 and 8 are closed and no longer block this work.
- Issue 12 remains the sole owner of production Schema Deployment Manifest issuance, revocation,
  supersession, and strict environment identity.
- Issue 13 remains the owner of durable Schema Workspace Binding, slot-safe materialization, and the
  production graph-access gate.
- This issue owns catalog/selection/derivation promotion and the canonical workflow/control-surface
  integration. It must integrate with Issues 12 and 13 rather than duplicating them.
- The Knowledge Preflight specification owns broad coverage, multimodal retrieval, snapshots,
  freshness, and Decision Reports. This issue supplies its upstream schema grounding and bounded
  graph-reconciliation capability.

## Acceptance criteria

### Catalog build and publication

- [ ] One application-owned service consumes the exact directive SDL plus one governed semantic
  overlay revision and emits a deterministic Schema Catalog Build record.
- [ ] The physical digest excludes source path/storage identity while every build retains exact
  source content lineage.
- [ ] Relationship field direction preserves physical start/end nodes for `IN` and `OUT`.
- [ ] Malformed SDL, unknown overlay elements, duplicate identities, unsupported directives,
  unresolved endpoints, or nondeterministic output fail closed before publication.
- [ ] The official TruDiagnostic overlay fixture contains 10–20 governed important types; the
  current accepted fixture contains 14 types across five modules.
- [ ] Tier 0 remains at or below 50 KiB for the authoritative fixture; the accepted baseline is
  49,911 bytes.
- [ ] `selection-tier0` and `selection-candidates` profiles are manifest-declared, digest-bound, and
  contain no byte-identical Markdown/JSON/drill-down duplicates.
- [ ] Identical canonical input and generator versions converge on one logical catalog digest;
  meaningful input or generator changes produce a successor.
- [ ] Build metadata and immutable bundle publication are idempotent and concurrency-safe.

### Selection and derivation

- [ ] `SchemaContextSelectionWorkflow` is application-owned and runnable standalone or through a
  declared linked-run slot.
- [ ] An agent-produced selection cannot be accepted without deterministic structural validation
  and an independent semantic reviewer.
- [ ] Reviewer output must bind the exact selection identity. A mismatch is persisted, rejected,
  and retried only within the declared bound; the host never rewrites reviewer output.
- [ ] Semantic revisions are bounded and distinct from infrastructure retries.
- [ ] Deterministic expansion adds endpoint, enum, union, interface, directive, property, and
  relationship-property closure without adding semantic membership.
- [ ] Schema Operation Projections are purpose-bound and require new admission before cross-purpose
  reuse.
- [ ] Existing selection, review, expansion, and operation-projection wire contracts remain
  backward-compatible for the first production slice or receive an explicit versioned migration.

### Control plane, runtime, and persistence

- [ ] Published Workflow/operation/profile definitions compile into an immutable Effective Run
  Configuration through the existing control plane.
- [ ] Every model/tool/skill/workspace/secret/budget binding executes through the existing
  Operation Execution Binding; no experiment-only direct SDK path remains in canonical execution.
- [ ] Run admission, lifecycle, budgets, commands, linked runs, and outbox effects use the existing
  PostgreSQL run-control authority.
- [ ] Document-shaped records use application repositories; large immutable payloads use object
  storage; historical records contain durable references rather than host paths.
- [ ] A Temporal worker registers the required workflow/activity implementations on explicit task
  queues with bounded timeouts and idempotency identities.
- [ ] Authenticated APIs expose start-through-admission and read/query surfaces without bypassing
  the generic control plane.
- [ ] Durable projection events expose stage, revision, gate, query, usage, failure, and terminal
  state to reconnecting clients.

### Graph safety and evidence

- [ ] Production Neo4j access remains impossible until Issue 12’s exact deployment attestation and
  Issue 13’s independent graph-capability gate both succeed.
- [ ] Agents cannot submit arbitrary Cypher. They submit typed intents admitted against the exact
  purpose-bound projection; all report values remain parameters.
- [ ] Reads are bounded, embeddings are stripped, every attempted intent receives one persisted
  typed result, and zero/success/rejected/failed remain distinct.
- [ ] Possessing catalog files or a Schema Workspace Binding grants no graph credentials, mutation
  authority, semantic truth, or approval.
- [ ] Supporting reconciliation emits immutable intent/result references and cannot claim broad
  Knowledge Preflight coverage.

### Behavioral verification

- [ ] Golden, malformed-input, relocation, direction, closure, purpose-reuse, reviewer-binding,
  concurrency/idempotency, security, and failure-gate tests pass through public services.
- [ ] A production-shaped fixture proves no Neo4j driver/query is created for every incompatible
  deployment or unauthorized graph-capability outcome.
- [ ] The TruDiagnostic workload continues to pass all nine comparison gates against
  `live-windows-bind-9`.
- [ ] The accepted thresholds preserve 100% oracle recall, all 32 baseline records, all three
  products, exact `IMPLEMENTS`, independent acceptance, and zero rejected/failed required queries.
- [ ] Resource, byte, token, request, selection-stability, and elapsed-time metrics are persisted
  per stage. Repeated-run timing is reported rather than inferred from one run.
- [ ] Repository-wide Ruff, mypy, and pytest checks pass.

## Explicit non-goals

- Authoring or changing the canonical Neo4j schema.
- Replacing directive-SDL identity with live introspection.
- Implementing production Schema Deployment Manifest issuance inside this issue.
- Granting schema resources graph credentials or mutation capability.
- Calling bounded Supporting Graph Reconciliation “Knowledge Preflight.”
- Implementing the full Knowledge Preflight Coverage Matrix, multimodal retrieval suite, Snapshot,
  Freshness Assessment, or Decision Report in this issue.
- Identity resolution, graph repair, ingestion, or canonical graph mutation.
- Selecting final embedding models, vector dimensions, or retrieval weights.
- Building a dashboard before typed command/query/event surfaces exist.

## Verification commands

```powershell
Set-Location C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system

.\.venv\Scripts\ruff.exe check app tests scripts
.\.venv\Scripts\mypy.exe app
.\.venv\Scripts\python.exe -m pytest -q --basetemp .scratch\pytest-schema-grounding

.\.venv\Scripts\python.exe scripts\compare_schema_context_runs.py `
  --baseline .scratch\schema-context-selection-runs\live-windows-bind-9 `
  --candidate .scratch\schema-context-selection-runs\official-catalog-v1-live-20260723-3 `
  --output .scratch\schema-context-selection-runs\comparisons\issue-11-acceptance
```

## Source basis

- Human Upgrade System Context
- Reusable Schema Catalog, Deployment Manifest, and Schema Workspace Materialization
- Knowledge Preflight Workflow Synthesis and specifications
- System Workflow Execution and Control Plane synthesis
- Successful deterministic-catalog live experiment and A/B comparison

This issue is self-contained enough for a fresh implementation agent, but the normative companion
specification governs any ambiguity.
