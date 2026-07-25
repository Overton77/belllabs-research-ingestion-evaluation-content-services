# Goal: Python Schema Context Selection and Graph Reconciliation Sandbox

## Goal statement

Implement and run a fully Python, typed, OpenAI Agents SDK sandbox workflow that:

1. deterministically transforms the versioned Neo4j GraphQL directive SDL into a rich, navigable Schema Workspace;
2. runs a filesystem-enabled `SchemaContextSelectionWorkflow` using `gpt-5-mini`;
3. independently reviews and accepts or rejects the semantic selection;
4. deterministically expands an accepted selection and creates a query-purpose `SchemaOperationProjection`;
5. runs the selection workflow as a child of `ReportGraphReconciliationWorkflow`;
6. creates typed `QueryExecutionIntent` objects;
7. validates, compiles, and actually executes bounded read-only Neo4j queries;
8. records typed `QueryExecutionResult` objects and a final reconciliation result; and
9. writes a complete, reproducible real-run artifact tree for tuning.

This implementation must live entirely in:

```text
biotech-research-ingestion-evaluation-system/
```

The TypeScript Cursor SDK workflow in `biotech-kg` is prior art and a behavioral reference only. The finished implementation must not invoke Node, TypeScript, pnpm, the Cursor SDK, or generated TypeScript executables at runtime. Reimplement the required schema parsing, catalog generation, deterministic expansion, query projection, and workflow orchestration in Python.

The first real run must use the existing TruDiagnostic report and live Neo4j data.

---

## Why this goal exists

Before ingesting a report, the system must determine what relevant entities and relationships already exist in Neo4j. That requires two different kinds of reasoning:

- semantic reasoning about which schema surface is relevant to the report; and
- operational reasoning about which safe queries should be executed against that surface.

These must remain separate. A filesystem-enabled agent may navigate the schema workspace, but it must not silently turn schema selection into unrestricted query execution.

The intended boundary is:

```text
versioned SDL
  -> deterministic Schema Catalog
  -> read-only Schema Workspace
  -> semantic Schema Context Selection
  -> independent review
  -> deterministic structural expansion
  -> query-purpose Schema Operation Projection
  -> typed Query Execution Intent
  -> guarded host-side execution
  -> typed Query Execution Result
  -> graph reconciliation evidence
```

The sandbox should be easy to tune now and straightforward to place behind the official control plane and Temporal workflows later.

---

## Required names

Use these names unless an existing Python convention makes a minor spelling change necessary:

- `SchemaContextSelectionWorkflow`
- `ReportGraphReconciliationWorkflow`
- `SchemaContextSelectionRequest`
- `SchemaContextSelection`
- `SchemaSelectionReview`
- `AcceptedSchemaContextSelection`
- `ExpandedSchemaSlice`
- `SchemaOperationProjection`
- `SchemaDeploymentAttestation`
- `SchemaCompatibilityDecision`
- `QueryExecutionIntent`
- `QueryExecutionResult`
- `GraphReconciliationEvidence`
- `ReportGraphReconciliationResult`

Do not call the child workflow merely `SchemaSelectionWorkflow`. Its output is semantic, immutable, purpose-bound schema context.

---

## Repository inputs

### Authoritative versioned schema

```text
../biotech-kg/src/schema/neo4jbiotechschema.graphql
```

This Neo4j GraphQL directive SDL is the schema authority for the experiment. It is approximately 2,882 lines and contains roughly:

- 83 node types;
- 303 relationship triplets;
- 36 enums;
- 29 unions;
- 4 interfaces;
- rich `@relationship`, `@relationshipProperties`, `@fulltext`, and `@vector` metadata.

The Python parser must operate directly on this SDL. Do not consume TypeScript-generated compact schemas or schema cards as runtime inputs.

### First report

```text
../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/reports/products-labtests-biomarkers.md
```

### Structured candidates for evaluation

These are evaluation/reference inputs, not replacements for the report:

```text
../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/output/structured-extract-products-biomarkers.json
../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/output/structured-extract-company-fundamentals.json
```

The extracts contain legacy `OrganizationState` and `ProductState` names. The current SDL uses `OrganizationSnapshot` and `ProductSnapshot`. The workflow must surface this as an explicit resolved mapping, exclusion, or unresolved mapping. It must not silently invent legacy schema types.

### Existing Python infrastructure to reuse

Reuse patterns and components where appropriate:

```text
app/integrations/openai_agents_runtime.py
app/integrations/neo4j.py
app/integrations/filesystem_workspace.py
app/application/workspace_materialization.py
app/domain/operation_execution/contracts.py
app/temporal/run_operation_probe.py
tests/test_openai_agents_runtime.py
tests/test_workspace_materialization.py
```

Do not force the experiment through `OperationExecutionBinding`, the full control plane, or Temporal yet.

### Normative design references

```text
../biotech-meta/docs/specs/pre-research/control-plane-capabilities/01-schema-catalog-deployment-manifest-and-workspace-materialization.md
../biotech-meta/docs/checkpoints/schema_schema_workspaces_efficient_db_interaction/2026-07-16-large-schema-workspaces-selection-and-report-splitting-special-checkpoint.md
.scratch/control-plane-foundations-and-capabilities/issues/11-schema-catalogs-and-selections.md
.scratch/control-plane-foundations-and-capabilities/issues/12-schema-deployment-manifest-attestation.md
.scratch/control-plane-foundations-and-capabilities/issues/13-schema-workspace-materialization-and-graph-gate.md
```

The sandbox may simplify persistence and orchestration, but it must preserve the semantic-selection, deterministic-expansion, independent-review, purpose-binding, compatibility-gate, and graph-authority boundaries.

---

## Important API and billing note

Use the OpenAI Agents SDK model identifier:

```text
gpt-5-mini
```

Make it the default through a constant and allow an environment override:

```text
SCHEMA_SELECTION_MODEL=gpt-5-mini
```

The real sandbox run requires `OPENAI_API_KEY`. ChatGPT/Codex subscription access and OpenAI API billing are separate. The implementation agent may write and test deterministic components without an API call, but it must clearly report if a real `gpt-5-mini` run cannot be executed because API credentials or API quota are unavailable. Never print or persist the API key.

Keep the repository's current compatible OpenAI package constraints unless the installed SDK demonstrably lacks required `SandboxAgent` behavior. Do not casually upgrade OpenAI packages.

---

## Architectural decision

### Parent workflow

`ReportGraphReconciliationWorkflow` is a plain asynchronous Python orchestrator for this experiment.

Its responsibilities:

1. freeze and hash input files;
2. build the Python Schema Catalog;
3. capture a live Neo4j capability snapshot;
4. create and verify a test deployment attestation;
5. materialize the read-only Schema Workspace;
6. invoke `SchemaContextSelectionWorkflow` as an explicit child;
7. stop if selection review is not accepted;
8. deterministically expand the accepted selection;
9. build the query-purpose operation projection;
10. invoke the query-planning agent;
11. service guarded `QueryExecutionIntent` calls;
12. persist every intent and result;
13. produce `GraphReconciliationEvidence`;
14. produce `ReportGraphReconciliationResult`; and
15. write metrics and a concise human-readable summary.

This is an explicit Python method call with typed input and output. Do not implement it as an OpenAI agent handoff. Do not use an agent-as-tool to hide the child workflow boundary.

### Child workflow

`SchemaContextSelectionWorkflow` owns semantic selection and review only.

Its responsibilities:

1. run the selector `SandboxAgent`;
2. receive a typed draft `SchemaContextSelection`;
3. run deterministic structural validation;
4. run a separate reviewer agent;
5. combine validation and review into an acceptance decision; and
6. return the draft, review, validation diagnostics, and optional accepted selection.

The selecting agent must not approve itself.

### Deterministic host code

Host code, not an LLM:

- parses and normalizes the SDL;
- computes source and catalog digests;
- validates selected names;
- validates relationship endpoint topology;
- expands structural closure;
- builds the operation projection;
- verifies test attestation equality;
- validates query intent fields;
- compiles baseline structured intents into Cypher;
- enforces bounds;
- executes Neo4j calls; and
- writes immutable run artifacts.

### Query-planning agent

The query-planning agent receives:

- the report;
- the accepted selection;
- the expanded slice;
- the query-purpose projection;
- the live index snapshot;
- prior query results as the bounded loop progresses; and
- instructions for the reconciliation question.

It may call one host-side tool:

```text
execute_read_intent
```

The tool accepts a typed `QueryExecutionIntent`. The host validates it, compiles or admits it, executes it through the async Neo4j driver, persists both intent and result, and returns a bounded typed result.

The sandbox must never receive Neo4j credentials.

---

## Python package layout

Use production-shaped domain and application modules for reusable logic, while keeping experimental orchestration isolated:

```text
app/
  domain/
    schema_context/
      __init__.py
      contracts.py
      errors.py
      canonicalization.py
      validation.py
      expansion.py
      projection.py
  application/
    schema_catalog.py
    schema_context_selection.py
    graph_query.py
  integrations/
    neo4j_read_executor.py
  experiments/
    schema_context_selection/
      __init__.py
      catalog_builder.py
      workspace.py
      selection_workflow.py
      reconciliation_workflow.py
      agents.py
      prompts.py
      evaluation.py

scripts/
  run_schema_context_selection.py

tests/
  test_schema_catalog.py
  test_schema_context_selection.py
  test_schema_expansion.py
  test_schema_operation_projection.py
  test_graph_query_intents.py
  test_report_graph_reconciliation_workflow.py
  fixtures/
    schema_context_selection/
      trudiagnostic/
```

Avoid duplicating contracts under `experiments/`. Domain contracts belong under `app/domain/schema_context/`.

If a smaller file decomposition is clearly better, preserve these ownership boundaries even if individual filenames differ.

---

## Python SDL parser and deterministic Schema Catalog

### Dependency

Prefer an explicit Python GraphQL AST dependency such as:

```text
graphql-core>=3.2,<4
```

Add it to `pyproject.toml` if it is not already a direct dependency. Do not use regex as the primary SDL parser.

### Parsing requirements

Parse and preserve:

- schema source path, byte length, and SHA-256 digest;
- object types and descriptions;
- interfaces and implemented interfaces;
- fields, descriptions, type references, nullability, and list structure;
- enums and values;
- unions and members;
- `@node`;
- `@id`;
- `@alias`;
- `@timestamp`;
- `@relationship`;
- `@relationshipProperties`;
- `@fulltext`;
- `@vector`;
- relationship direction, type, endpoint, and relationship-properties type;
- full-text index name, fields, and GraphQL query name;
- vector index name, provider, embedding property, and GraphQL query name;
- identity candidates;
- search candidates;
- immediate topology.

Unknown directives must be retained in a generic, canonical form rather than discarded.

### Determinism

Canonical JSON must:

- use stable field names;
- sort dictionary keys;
- sort logically unordered collections;
- preserve declared field order where it aids human navigation;
- exclude generated timestamps from logical digests;
- use UTF-8;
- end files with a newline; and
- serialize with stable separators/indentation.

Two builds from identical source bytes and generator version must produce the same logical catalog digest and resource manifest.

Changing the SDL or generator version must change the catalog identity.

### Catalog resources

Generate both machine-readable JSON and human-readable Markdown:

```text
schema/
  manifest.json
  source/
    neo4jbiotechschema.graphql
  global/
    compact-schema.json
    compact-schema.md
    module-index.json
    module-index.md
    topology-index.json
    topology-index.md
    search-index.json
    identity-index.json
  modules/
    organizations-and-people.json
    products-and-commerce.json
    diagnostics-and-biomarkers.json
    studies-and-evidence.json
    documents-and-provenance.json
    remaining-schema.json
  cards/
    nodes/
      Organization.json
      Organization.md
      ...
    relationships/
      OFFERS.json
      OFFERS.md
      ...
    enums/
    unions/
    interfaces/
  drilldown/
    nodes/
    relationships/
  indexes/
    fulltext.json
    vector.json
    aliases.json
    lexical-terms.json
  skills/
    schema-navigation/
      SKILL.md
```

### Modules

For the experiment, modules may be deterministic, code-owned definitions. They are overlapping conceptual views, not ownership partitions.

At minimum define:

- organizations-and-people;
- products-and-commerce;
- diagnostics-and-biomarkers;
- studies-and-evidence;
- documents-and-provenance; and
- remaining-schema.

Every schema element must be reachable through the global indexes even if it belongs only to `remaining-schema`.

### Node cards

Each node card should contain:

- name and description;
- interfaces;
- aliases;
- all properties with type and description;
- identity candidates;
- exact/range-search candidates;
- full-text indexes;
- vector indexes;
- outgoing relationships;
- incoming relationship topology inferred from the complete schema;
- related enums/unions;
- source schema digest;
- catalog digest or build reference; and
- recommended drill-down paths.

### Relationship cards

Each relationship card should contain:

- Neo4j relationship type;
- all valid source/target endpoint combinations;
- GraphQL field names;
- directions;
- relationship property type and fields;
- immediate neighboring types;
- source schema digest; and
- example read patterns that are generated deterministically.

### Navigation skill

Create a concise schema-navigation skill mounted read-only in the sandbox. It must instruct the agent to:

1. read the manifest;
2. read the global compact and module indexes;
3. identify candidate modules;
4. inspect only relevant node and relationship cards;
5. use drill-down files when a card is insufficient;
6. never invent names;
7. distinguish semantic selection from deterministic expansion;
8. record exclusions, near misses, and unresolved mappings;
9. treat schema files as context, not graph authority; and
10. avoid reading the complete parsed schema unless necessary.

---

## Workspace materialization

### Real-run root

Write each run under:

```text
.scratch/schema-context-selection-runs/<run-id>/
```

Use a sortable UTC run ID, for example:

```text
trudiagnostic-products-20260722T015800Z
```

### Complete run layout

```text
<run-id>/
  run.json
  inputs/
    report.md
    structured-extract-products-biomarkers.json
    source-manifest.json
  schema/
    manifest.json
    source/
    global/
    modules/
    cards/
    drilldown/
    indexes/
    skills/
    runtime/
      live-schema.json
      live-indexes.json
      index-options.json
      deployment-attestation.json
      compatibility-decision.json
  selection/
    request.json
    selector-prompt.md
    draft.json
    deterministic-validation.json
    reviewer-prompt.md
    review.json
    accepted.json
    expanded-slice.json
    operation-projection.json
  queries/
    planner-prompt.md
    001-intent.json
    001-result.json
    002-intent.json
    002-result.json
    final-evidence.json
  traces/
    events.ndjson
    usage.json
  metrics.json
  result.json
  summary.md
```

### Read-only sandbox view

Mount these inputs read-only:

- report;
- schema manifest;
- global indexes;
- module files;
- cards and drill-down resources;
- schema-navigation skill;
- accepted selection for the query stage;
- expanded slice;
- operation projection;
- live capability snapshots.

Provide a separate writable output directory only when required by `SandboxAgent`. The host must treat model-written files as candidates until validated.

Every governed file in the workspace manifest must include:

- logical path;
- content digest;
- source schema digest where applicable;
- catalog digest;
- media type;
- resource kind; and
- read-only status.

---

## Typed contracts

Implement strict Pydantic v2 models. Use `extra="forbid"` on externally produced contracts unless a concrete reason requires otherwise.

### `SchemaContextSelectionRequest`

Required fields:

- `request_id`
- `purpose`
- `intended_operations`
- `schema_definition_ref`
- `schema_definition_digest`
- `catalog_digest`
- `report_ref`
- `report_digest`
- `coverage_obligations`
- `workspace_ref`
- `created_at`

The first purpose should be:

```text
pre_ingestion_graph_reconciliation
```

### `SchemaContextSelection`

Required fields:

- `selection_id`
- `revision`
- `purpose`
- `schema_definition_ref`
- `schema_definition_digest`
- `catalog_digest`
- `report_ref`
- `report_digest`
- `selected_node_types`
- `selected_relationship_types`
- `property_intent_hints`
- `coverage_obligations`
- `rationale`
- `evidence_locators`
- `explicit_exclusions`
- `unresolved_mappings`
- `near_miss_candidates`
- `parent_selection_id`
- `created_at`

Constraints:

- names must be unique;
- ordering must be canonical;
- selections must not contain properties as semantic membership;
- property hints must not prune deterministic expansion;
- all rationales and locators must be bounded in length;
- every selected relationship must have selected or deterministically required endpoints.

### `SchemaSelectionReview`

Required fields:

- `review_id`
- `selection_id`
- `reviewer_role`
- `decision`: `accepted | rejected | revision_required`
- `structural_valid`
- `coverage_findings`
- `missing_concepts`
- `overbroad_selections`
- `unjustified_selections`
- `temporal_coverage`
- `identity_coverage`
- `provenance_coverage`
- `near_miss_assessment`
- `required_revisions`
- `rationale`
- `created_at`

The reviewer must not receive instructions allowing it to mutate the draft. It reviews and returns findings.

### `AcceptedSchemaContextSelection`

This binds:

- exact draft selection;
- deterministic validation digest;
- independent review digest;
- acceptance decision;
- accepted selection digest; and
- acceptance timestamp.

It must not be constructible when review is rejected or revision is required.

### `ExpandedSchemaSlice`

Generated deterministically from the accepted selection.

Required content:

- complete selected node definitions;
- complete properties for selected nodes;
- relationship endpoints;
- complete relationship fields;
- relationship property types;
- required enums;
- required unions and members;
- implemented interfaces;
- relevant directives;
- full-text/vector declarations;
- identity candidates;
- complete selected SDL;
- closure diagnostics;
- accepted selection digest;
- source schema digest;
- expansion policy version;
- expanded slice digest.

### `SchemaOperationProjection`

The first projection purpose is:

```text
read_query_reconciliation
```

Required content:

- projection ID and version;
- source and selection lineage;
- allowed node labels;
- allowed relationship types;
- allowed properties by label;
- allowed relationship properties;
- allowed traversals with direction;
- identity fields by label;
- exact/range-search capabilities;
- full-text capabilities;
- vector capabilities;
- permitted query kinds;
- procedure allowlist;
- default and maximum limits;
- maximum traversal depth;
- timeout;
- result redaction/size policy;
- projection digest.

### `QueryExecutionIntent`

Required fields:

- `intent_id`
- `sequence`
- `purpose`
- `query_kind`
- `projection_id`
- `projection_digest`
- `schema_definition_digest`
- `selection_digest`
- `goal`
- `coverage_obligation_ids`
- `labels`
- `relationship_types`
- `parameters`
- `requested_fields`
- `limit`
- `max_depth`
- `stopping_evidence`
- `semantic_query_text`
- `proposed_cypher`
- `created_at`

`query_kind` must initially be restricted to:

- `exact_identity`
- `fulltext_search`
- `bounded_neighborhood`
- `entity_details`
- `vector_search`

Do not enable arbitrary custom Cypher for the first completed run.

`proposed_cypher` may be recorded for tuning, but the executor must run deterministic host-compiled Cypher for these baseline query kinds. Persist both proposed and compiled forms when they differ.

The intent must never contain:

- credentials;
- database URI;
- a raw embedding vector;
- unbounded result requests;
- write intent;
- schema/admin intent; or
- labels, relationships, properties, or indexes absent from the admitted projection.

### `QueryExecutionResult`

Required fields:

- `result_id`
- `intent_id`
- `intent_digest`
- `query_kind`
- `status`: `succeeded | rejected | failed`
- `compiled_cypher`
- `redacted_parameters`
- `columns`
- `records`
- `record_count`
- `truncated`
- `elapsed_ms`
- `database`
- `server_info`
- `diagnostics`
- `error_type`
- `started_at`
- `finished_at`
- `result_digest`

Rules:

- never persist secret-bearing error strings;
- omit embeddings from records;
- cap strings, lists, nested maps, and total result bytes;
- distinguish rejection, execution failure, and a valid zero-record result;
- preserve Neo4j IDs and labels needed for reconciliation;
- do not infer “no entity exists” from a failed query.

### `GraphReconciliationEvidence`

Required content:

- reconciliation question;
- query goals;
- all intent/result references;
- matched existing entities;
- existing relationships;
- aliases used;
- match method;
- confidence;
- unresolved candidates;
- schema mismatches;
- legacy-name mappings;
- query failures;
- stopping rationale.

### `ReportGraphReconciliationResult`

Required content:

- run ID and status;
- input and schema digests;
- model;
- selection/review/expanded/projection references;
- compatibility decision;
- query result references;
- reconciliation evidence;
- usage;
- timings;
- evaluation metrics;
- artifact root;
- warnings.

---

## Test deployment attestation and graph gate

Do not use the earlier `unattested_read_only_experiment` shortcut.

Create a typed, explicitly test-only `SchemaDeploymentAttestation` whose deployed SDL digest equals the exact SDL digest used to build the catalog.

Requirements:

- `attestation_kind="test_only"`
- exact environment/database identifier;
- schema definition reference;
- deployed SDL SHA-256 digest;
- issuer such as `schema-context-selection-sandbox`;
- issued timestamp;
- attestation digest;
- `production_usable=false`.

Require an explicit opt-in for a live query:

```text
SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1
```

The compatibility decision must verify exact digest equality before obtaining or using the Neo4j driver for query execution.

Be precise: this test attestation proves the gate mechanics and lineage consistency of the experiment. It does not prove that a real deployment service attested the live database. Record that limitation in `summary.md` and the typed result.

Live introspection remains diagnostic and must not mint or replace the attestation.

---

## Live Neo4j capability snapshot

Before selection/query execution, capture bounded read-only snapshots using the Python async driver:

1. `SHOW INDEXES` including:
   - name;
   - type;
   - state;
   - entity type;
   - labels or relationship types;
   - properties;
   - population percentage if available;
   - options needed to understand vector dimensions/provider.
2. A bounded live schema inventory:
   - labels with observed scalar property keys/types;
   - relationship types and observed endpoints;
   - no large property payloads;
   - no embeddings.

Never generically run `toString()` over all node properties. Neo4j LIST properties such as `searchFields`, `searchEmbedding`, `categories`, and `valueChainStages` cause a scalar conversion type error. Search only schema-confirmed scalar fields or use type-aware query templates.

Keep authority separate:

- SDL/catalog: what the versioned schema defines;
- live snapshot: what the current database reports as executable/observed;
- test attestation: sandbox gate fixture;
- operation projection: what this run is allowed to query.

---

## Expected first selection

The selector may reasonably vary, but the expected core is:

### Nodes

- `Organization`
- `Product`
- `LabTest`
- `Biomarker`
- `PanelDefinition`
- `TechnologyPlatform`

### Relationships

- `OFFERS`
- `DELIVERS_LABTEST`
- `MEASURES`
- `IMPLEMENTS`
- `IMPLEMENTS_PANEL`
- `INCLUDES_BIOMARKER`
- `INCLUDES_LABTEST`
- `USES_PLATFORM`
- `DEVELOPS_PLATFORM`

Do not hard-code this as the selector output. Use it as an evaluation expectation. The selector must justify additions and exclusions.

The reviewer must specifically check:

- product aliases and trademarks;
- temporal snapshot requirements;
- `OrganizationState` -> `OrganizationSnapshot`;
- `ProductState` -> `ProductSnapshot`;
- OMICmAge/SYMPHONYAge/DunedinPACE as `TechnologyPlatform` versus possible `Metric` or `Biomarker` near misses;
- evidence/provenance needs;
- whether `Document` is required for the selected purpose;
- whether 122 biomarkers require broad selection or can be queried under the selected `Biomarker` type without expanding semantic membership.

---

## Expected live-graph oracle

The first run should recover, at minimum:

### Organization

```text
name: TruDiagnostic
id: 64720458-3328-5439-b6de-1624bd5b60ae
```

### Products connected through `OFFERS`

- TruAge
- TruHealth
- TruAge + TruHealth

### Existing product neighborhood examples

- `DELIVERS_LABTEST`
  - TruAge Epigenetic Biological Age Test
  - TruHealth Epigenetic Biomarker Proxy Test
  - TruDiagnostic Immune Cell Deconvolution Test
- `IMPLEMENTS_PANEL`
  - TruAge Aging Panel
  - SymphonyAge Organ Systems Panel
  - TruHealth Epigenetic Biomarker Proxy Panel
  - Immune Cell Composition Panel
- `IMPLEMENTS`
  - Methylation Screening Array
  - OMICmAge
  - SymphonyAge
  - DunedinPACE
  - Epigenetic Biomarker Proxies
  - 19-cell Immune Deconvolution

Do not hard-code these as returned workflow results. Use them as assertions/evaluation oracles proving that real queries ran and useful existing graph state was found.

---

## Query strategy for the first run

Use a bounded sequence:

1. exact/scalar identity lookup for `TruDiagnostic`;
2. full-text fallback through `OrganizationName` if exact lookup is insufficient;
3. bounded one-hop organization neighborhood restricted to selected relationships;
4. fetch the three offered products;
5. bounded product neighborhoods for selected relationships;
6. selected lab-test/panel/platform details;
7. a small sample of report-mentioned biomarkers using exact/full-text lookup.

Baseline ordering:

```text
exact identity
  -> full-text fallback
  -> bounded topology traversal
  -> entity details
```

Vector search is not required to pass the first baseline. Implement and test its contract/compiler path if practical, but enable it in a later tuning variant.

For vector intent:

- the intent contains semantic query text, not an embedding;
- the host creates the embedding;
- the executor verifies index availability and dimensions;
- the raw vector is never persisted;
- vector results preserve native scores;
- exact/topological evidence outranks vector similarity for identity reconciliation.

---

## Guarded Neo4j executor

Create a dedicated adapter rather than placing query behavior in `app/integrations/neo4j.py`.

Suggested path:

```text
app/integrations/neo4j_read_executor.py
```

Requirements:

- reuse `create_neo4j(settings)` or a supplied `AsyncDriver`;
- use a read-only Neo4j principal when configured;
- execute only admitted `QueryExecutionIntent`;
- compile baseline query kinds deterministically;
- use parameters for all report-derived values;
- enforce default and maximum limits;
- enforce maximum traversal depth;
- enforce timeout;
- allowlist procedures, initially:
  - `db.index.fulltext.queryNodes`;
  - `db.index.vector.queryNodes` only for vector variant;
- reject writes, schema changes, admin operations, dynamic procedure names, and unbounded variable-length traversals;
- return bounded typed results;
- close sessions/driver reliably;
- sanitize diagnostics and exceptions;
- record intent and result even when rejected or failed.

Defense in depth:

1. typed intent restrictions;
2. operation projection allowlist;
3. deterministic query compiler;
4. procedure allowlist;
5. query bounds;
6. read session/access mode;
7. read-only database principal;
8. result bounds and redaction.

Do not claim keyword scanning alone makes arbitrary Cypher safe.

---

## OpenAI Agents SDK implementation

### Selector

Use `SandboxAgent` with:

- model `gpt-5-mini`;
- filesystem capability;
- shell capability only for safe workspace navigation commands;
- read-only schema/report mounts;
- schema-navigation skill;
- structured `output_type=SchemaContextSelection`;
- bounded turns;
- tracing with sensitive data excluded;
- no Neo4j credentials;
- no Neo4j execution tool.

The selector should begin with global indexes and progressively disclose cards. It should not receive the entire 108 KB SDL inline unless it chooses to read the mounted source.

### Reviewer

Use a separate agent instance and fresh instructions.

It receives:

- request;
- report or report locators;
- draft selection;
- deterministic validation;
- compact schema/module resources; and
- relevant cards.

Use `output_type=SchemaSelectionReview`.

The reviewer must assess semantic coverage, near misses, temporal needs, identity needs, provenance, and overbreadth.

### Query planner

Use a separate `SandboxAgent` or typed `Agent` with:

- model `gpt-5-mini`;
- accepted selection;
- expanded slice;
- operation projection;
- live capability snapshot;
- read-only report;
- `execute_read_intent` host tool;
- structured final evidence/result output;
- bounded query-call count;
- sequential tool execution for reproducibility.

The host tool must accept and validate the Pydantic contract. Tool arguments are not trusted merely because the SDK parsed them.

### Usage and traces

Capture:

- model;
- input/output/total tokens where available;
- model turns;
- tool-call count;
- sandbox filesystem/shell calls;
- query-intent count;
- query success/rejection/failure counts;
- elapsed time by stage;
- provider response/run IDs when available;
- prompts and structured outputs.

Do not include secrets or complete raw embeddings in traces.

---

## Prompt requirements

Prompts should be versioned Python constants or template files and copied into the run artifacts.

### Selector prompt

Must emphasize:

- purpose is pre-ingestion graph reconciliation;
- select semantic node/relationship membership only;
- inspect global indexes first;
- drill down progressively;
- never invent schema names;
- include explicit exclusions and near misses;
- resolve or flag legacy State/Snapshot names;
- property hints do not limit expansion;
- schema context grants no graph authority;
- return only the typed output.

### Reviewer prompt

Must emphasize:

- reviewer is independent;
- do not rewrite the draft;
- find false negatives and unjustified breadth;
- check temporal, identity, provenance, document, test, panel, biomarker, and platform coverage;
- return `revision_required` when substantive changes are needed;
- return only the typed review.

### Query-planner prompt

Must emphasize:

- use only the admitted operation projection;
- create typed intents;
- prefer exact and topology evidence before semantic similarity;
- distinguish query failure from zero matches;
- stop when required reconciliation questions have bounded evidence;
- never request writes;
- never place credentials or raw embeddings in intents;
- return final typed evidence.

---

## Selection revision behavior

For the first implementation, allow at most one revision round:

1. selector emits draft;
2. validator and reviewer assess it;
3. if `revision_required`, selector receives bounded findings and emits revision 2;
4. validator and a fresh review run assess revision 2;
5. accept or terminate as not accepted.

Do not allow unbounded agent loops.

Only semantic node/relationship membership changes create a new selection revision. Reading more cards or expanding properties does not.

---

## Testing plan

### Unit tests: catalog

Prove:

- SDL parses successfully;
- required known types and relationships exist;
- full-text/vector metadata parses correctly;
- aliases and identity fields are preserved;
- deterministic rebuilds produce identical digests;
- source changes alter digest;
- malformed SDL fails with a typed error;
- unknown directives are retained;
- generated paths are safe and deterministic.

### Unit tests: selection validation

Cover:

- valid core selection;
- unknown node;
- unknown relationship;
- invalid endpoint topology;
- duplicate names;
- wrong schema digest;
- wrong purpose;
- relationship endpoint closure;
- explicit legacy-name mapping;
- selector cannot self-approve.

### Unit tests: expansion

Prove:

- complete selected properties are included;
- relationship endpoints are included;
- relationship-property types are included;
- enums/unions/interfaces/directives close correctly;
- selected SDL parses;
- expansion does not mutate semantic membership;
- identical inputs produce identical digest.

### Unit tests: projection

Prove:

- only selected/closed labels and relationships are allowed;
- index declarations are represented;
- live offline/missing indexes are diagnostic;
- LabTest/Biomarker do not gain vector capability merely because they have embedding properties without an admitted vector index;
- purpose and digest are bound.

### Unit tests: query intent/executor

Cover:

- exact identity compilation;
- full-text compilation;
- neighborhood compilation;
- entity-details compilation;
- vector intent validation;
- unknown label/property/relationship/index rejection;
- excessive limit rejection;
- excessive depth rejection;
- raw embedding rejection;
- credentials/URI rejection;
- write/admin/custom query rejection;
- result truncation and embedding stripping;
- list-valued properties do not trigger generic scalar conversion;
- failed query is not represented as no match.

### Workflow tests with fakes

Use fake selector/reviewer/query agents and a fake Neo4j executor to prove:

- parent invokes child selection workflow;
- rejected selection prevents all Neo4j calls;
- compatibility failure prevents all Neo4j calls;
- accepted selection leads to expansion/projection/query;
- every intent has one result artifact;
- query failures are recorded;
- final evidence references exact results;
- rerunning deterministic stages is stable.

### Real integration run

With explicit environment opt-ins:

```text
OPENAI_API_KEY
SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1
```

Run against configured Neo4j Aura and produce a complete artifact directory.

Never make graph writes.

---

## CLI

Implement:

```bash
uv run python scripts/run_schema_context_selection.py \
  --schema ../biotech-kg/src/schema/neo4jbiotechschema.graphql \
  --report ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/reports/products-labtests-biomarkers.md \
  --structured-candidates ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/output/structured-extract-products-biomarkers.json \
  --model gpt-5-mini \
  --output-root .scratch/schema-context-selection-runs
```

Useful options:

- `--build-only`
- `--offline`
- `--skip-vector`
- `--max-query-intents`
- `--run-id`
- `--model`
- `--output-root`

Behavior:

- `--build-only`: parse/build/materialize and run deterministic tests only;
- `--offline`: selection may run, but no Neo4j execution; result explicitly records no live execution;
- normal mode: requires exact test-attestation opt-in and configured Neo4j credentials;
- failures return nonzero exit status;
- stdout prints only concise status and artifact paths;
- detailed diagnostics go to sanitized artifacts.

---

## Evaluation and tuning metrics

Write `metrics.json` containing:

### Workspace quality

- catalog resource count;
- node/relationship/card counts;
- total bytes;
- selected bytes versus full catalog bytes;
- files read by selector when observable;
- deterministic digest stability.

### Selection quality

- selected node count;
- selected relationship count;
- expected-core recall;
- unjustified additions;
- invalid-name count;
- unresolved mapping count;
- near-miss count;
- review decision;
- revision count.

### Query quality

- intent count;
- successful/rejected/failed counts;
- exact/fulltext/neighborhood/vector counts;
- total records;
- duplicate records;
- truncated results;
- known-oracle entity recall;
- unnecessary labels/relationships queried;
- stopping reason.

### Runtime

- tokens by agent/stage;
- turns;
- tool calls;
- elapsed milliseconds by stage;
- total elapsed time;
- repeated-run output stability excluding timestamps/provider IDs.

### First tuning variants

After the baseline succeeds, support separately named runs for:

1. filesystem navigation with full report;
2. filesystem navigation plus generated Schema Selection Brief;
3. typed schema lookup helpers versus direct filesystem drill-down;
4. exact/full-text/topology only;
5. vector-assisted fallback;
6. repeated baseline runs for reproducibility.

Do not combine variants in one artifact directory.

---

## Security and data handling

- Never persist secrets.
- Never pass Neo4j credentials into the sandbox.
- Never execute writes.
- Never commit PHI.
- Treat research reports as research inputs, not medical advice.
- Sanitize exception output because driver errors can contain connection information.
- Strip embeddings and oversized values from results.
- Record only bounded graph neighborhoods.
- Use explicit opt-ins for live API and Neo4j calls.
- Make test-only attestation impossible to confuse with production attestation.

---

## Explicit non-goals

Do not implement during this goal:

- Temporal workflow definitions;
- official control-plane registration;
- MongoDB/S3 persistence of schema catalogs;
- production deployment-manifest issuance;
- graph mutation or ingestion execution;
- a general arbitrary-Cypher agent;
- canonical report segmentation;
- a schema MCP server;
- final semantic/vector retrieval tuning;
- UI/API endpoints;
- migration of TypeScript code;
- runtime dependence on `biotech-kg` TypeScript artifacts.

The only permitted cross-repository runtime inputs are source data files such as the SDL, report, and structured extract.

---

## Implementation order

Follow this order so deterministic behavior is established before paid agent runs:

1. Add domain contracts and typed errors.
2. Add GraphQL AST dependency if needed.
3. Implement Python SDL parsing and canonicalization.
4. Implement deterministic catalog/resource generation.
5. Implement workspace materialization and manifests.
6. Implement selection validation.
7. Implement deterministic expansion.
8. Implement query-purpose projection.
9. Implement test attestation and compatibility decision.
10. Implement query intent validation and deterministic compilers.
11. Implement bounded Neo4j read executor.
12. Implement selector and reviewer agents.
13. Implement `SchemaContextSelectionWorkflow`.
14. Implement query planner and `execute_read_intent`.
15. Implement `ReportGraphReconciliationWorkflow`.
16. Implement CLI.
17. Add deterministic/unit/workflow tests.
18. Run formatting, linting, type checking, and tests.
19. Run build-only workflow and inspect workspace.
20. Run real `gpt-5-mini` + Neo4j workflow if credentials and explicit opt-ins are available.
21. Evaluate against TruDiagnostic oracle.
22. Fix defects and repeat until all completion criteria pass.

Do not begin with an API endpoint or Temporal wrapper.

---

## Verification commands

Use repository-standard tooling:

```bash
uv sync
uv run ruff check app tests scripts
uv run mypy app
uv run pytest
```

Then:

```bash
uv run python scripts/run_schema_context_selection.py \
  --schema ../biotech-kg/src/schema/neo4jbiotechschema.graphql \
  --report ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/reports/products-labtests-biomarkers.md \
  --structured-candidates ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/output/structured-extract-products-biomarkers.json \
  --model gpt-5-mini \
  --output-root .scratch/schema-context-selection-runs \
  --build-only
```

Finally, when authorized by environment:

```bash
SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1 \
uv run python scripts/run_schema_context_selection.py \
  --schema ../biotech-kg/src/schema/neo4jbiotechschema.graphql \
  --report ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/reports/products-labtests-biomarkers.md \
  --structured-candidates ../biotech-kg/research/trudiagnostic-20260330-203619-research-mission/output/structured-extract-products-biomarkers.json \
  --model gpt-5-mini \
  --output-root .scratch/schema-context-selection-runs \
  --skip-vector
```

On Windows shells, set the environment variable using the shell-appropriate syntax if the inline form is unsupported.

If an existing unrelated lint/test failure is present, document it separately. Do not weaken checks or delete unrelated tests.

---

## Completion criteria

The goal is complete only when all applicable items are true:

### Python ownership

- [ ] All runtime implementation is Python.
- [ ] No Node/TypeScript/Cursor SDK command is invoked.
- [ ] The SDL is parsed directly in Python.
- [ ] TypeScript artifacts are not runtime dependencies.

### Workspace

- [ ] The full schema catalog is generated from the versioned SDL.
- [ ] The workspace contains global indexes, modules, cards, drill-down resources, search/index metadata, and navigation skill.
- [ ] Every governed file has digest lineage.
- [ ] Repeated builds are deterministic.
- [ ] Sandbox inputs are read-only.

### Selection

- [ ] `SchemaContextSelectionWorkflow` runs as an explicit child workflow.
- [ ] Selector uses `gpt-5-mini`.
- [ ] Selector returns typed `SchemaContextSelection`.
- [ ] Deterministic validation runs.
- [ ] Independent reviewer returns typed `SchemaSelectionReview`.
- [ ] Selector cannot self-approve.
- [ ] Legacy State/Snapshot naming is explicitly handled.
- [ ] Accepted selection is purpose- and digest-bound.

### Expansion and projection

- [ ] Expansion is deterministic and structurally complete.
- [ ] Query-purpose projection is typed and bounded.
- [ ] Full-text/vector capability comes from SDL plus diagnostic live state.
- [ ] Unsupported live capabilities are not invented.

### Graph gate

- [ ] Test-only attestation is typed and explicit.
- [ ] Exact SDL digest equality is checked.
- [ ] Graph authority is separate from workspace presence.
- [ ] Compatibility failure results in zero Neo4j calls.

### Query execution

- [ ] Query planner produces typed `QueryExecutionIntent`.
- [ ] Every intent is validated against the projection.
- [ ] Baseline intents are deterministically compiled.
- [ ] The async Neo4j driver actually executes admitted read intents.
- [ ] Every attempted intent produces a persisted typed `QueryExecutionResult`.
- [ ] Credentials never enter sandbox or artifacts.
- [ ] Results are bounded and embeddings removed.
- [ ] No writes occur.

### TruDiagnostic result

- [ ] Real run finds the known TruDiagnostic organization ID.
- [ ] Real run finds the three known offered products.
- [ ] Real run finds relevant existing lab tests/panels/platforms.
- [ ] Result explains which existing IDs/relationships downstream ingestion should preserve.
- [ ] Zero-result, rejected, and failed queries remain distinguishable.

### Quality

- [ ] Unit and workflow tests pass.
- [ ] Ruff passes for changed code.
- [ ] Mypy passes for changed code or documented repository-wide pre-existing failures remain.
- [ ] A complete real-run artifact tree exists when credentials are available.
- [ ] `summary.md` explains outcome, limitations, warnings, and next tuning steps.

---

## Required final report from the implementation agent

When finished, report:

1. files added/changed;
2. architecture implemented;
3. exact model used;
4. tests and commands run;
5. whether a real OpenAI run occurred;
6. whether a real Neo4j query occurred;
7. artifact directory;
8. selected nodes/relationships;
9. selection review outcome;
10. `QueryExecutionIntent` count and kinds;
11. `QueryExecutionResult` statuses;
12. TruDiagnostic entities/relationships recovered;
13. token/tool/timing metrics;
14. deviations from this plan;
15. remaining blockers or recommended tuning experiments.

Do not claim completion if only mocks ran. Distinguish:

- deterministic implementation complete;
- sandbox agent run complete;
- live Neo4j reconciliation complete.

---

## Final implementation principle

The filesystem is a navigable projection, not schema authority. The selecting agent chooses semantic schema membership, not executable graph authority. Deterministic code expands and projects that membership. The query planner creates typed intents. A guarded Python host validates, compiles, executes, and records each query. This separation is the main result the experiment must preserve while tuning quality and cost.
