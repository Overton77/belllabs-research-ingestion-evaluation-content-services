# Schema Context Selection and Graph Reconciliation Handoff

## Purpose

This document explains the experimental Python workflow that selects a purpose-bound subset of
the authoritative Neo4j GraphQL directive SDL, independently reviews that selection, converts it
into a bounded Neo4j read projection, and reconciles a research report against the live graph.

The implementation is deliberately pre-production:

- it uses real OpenAI Agents SDK `SandboxAgent` sessions with `gpt-5-mini`;
- it executes real, parameterized, read-only Neo4j queries through a host-owned tool;
- it persists complete lineage, agent transcripts, query intents, query results, and evaluation
  evidence;
- it does not register a production service, issue a production schema attestation, or mutate the
  graph.

The first tuned workload uses the TruDiagnostic products/lab-tests/biomarkers report and the SDL
from the sibling `biotech-kg` repository.

## Where the implementation lives

The primary components are:

| Area | File | Responsibility |
|---|---|---|
| CLI | `scripts/run_schema_context_selection.py` | Parses run options, invokes the parent workflow, and writes sanitized failure artifacts. |
| Parent workflow | `app/experiments/schema_context_selection/reconciliation_workflow.py` | Owns the end-to-end run, child workflow, graph gate, query tool, persistence, and metrics. |
| Child workflow | `app/experiments/schema_context_selection/selection_workflow.py` | Runs the bounded selector/reviewer revision loop and prevents selector self-approval. |
| Agent harness | `app/experiments/schema_context_selection/agents.py` | Creates real SandboxAgents and read-only, allowlisted Windows Docker workspace views. |
| Prompts | `app/experiments/schema_context_selection/prompts.py` | Selector, reviewer, and bounded query-planner instructions. |
| Catalog | `app/application/schema_catalog.py` and `app/experiments/schema_context_selection/catalog_builder.py` | Parse the SDL directly in Python and materialize the schema workspace. |
| Contracts | `app/domain/schema_context/contracts.py` | Strict typed request, selection, review, projection, intent, result, and evidence contracts. |
| Validation | `app/domain/schema_context/validation.py` | Structural selection validation, acceptance binding, and exact schema-digest compatibility checks. |
| Expansion | `app/domain/schema_context/expansion.py` | Deterministic structural closure, including union/interface endpoint resolution. |
| Projection | `app/domain/schema_context/projection.py` | Produces the least-authority operation projection used for query admission. |
| Query compiler | `app/application/graph_query.py` | Validates typed intents and compiles the admitted baseline query kinds to Cypher. |
| Neo4j executor | `app/integrations/neo4j_read_executor.py` | Executes async read-only queries and bounds/sanitizes returned records. |
| Evaluation | `app/experiments/schema_context_selection/evaluation.py` | Selection and live-graph oracle metrics. |

## How the workflow works

### 1. Freeze inputs and build the schema workspace

The parent reads the SDL and report as source data, records their SHA-256 digests, and copies the
report into the run directory. The structured candidate extract is retained as a host-side
evaluation/reference input; it is not mounted into agent sandboxes.

The SDL is parsed directly with Python. No TypeScript, Node, Cursor SDK, GraphQL API, or schema MCP
server is involved. The generated workspace includes:

- compact schema representations;
- module and topology indexes;
- identity and search indexes;
- node, relationship, enum, interface, union, and property-type cards;
- drill-down resources;
- a schema-navigation `SKILL.md`;
- a manifest containing content and source-schema digest lineage.

The catalog is built twice during a run and the logical digests are compared to detect
nondeterminism.

### 2. Run the semantic selector in a sandbox

The selector receives the report, request, schema navigation resources, and relevant catalog
files through a read-only allowlist. It returns a typed semantic selection containing node types,
relationship types, rationale, explicit exclusions, near misses, and legacy-name mappings.

The sandbox does not receive:

- the OpenAI API key;
- Neo4j URI, username, password, or driver;
- a Neo4j execution tool;
- the Docker socket;
- the structured-candidate evaluation JSON.

The trusted host adds request lineage, revision metadata, selection identity, and timestamps.

### 3. Validate and independently review

The host deterministically checks schema names, request lineage, purpose, digests, and structural
validity. A separate reviewer SandboxAgent reads the draft and validation result but cannot mutate
the draft.

The reviewer either accepts or requests one bounded revision. The live tuned run required a second
revision because the first draft omitted `LabTest`. The revision also correctly distinguished the
SDL relationship `IMPLEMENTS` used by `Product.implementsPlatforms` from the separate
`IMPLEMENTS_PLATFORM` relationship used elsewhere.

Only an independently accepted, structurally valid selection becomes an
`AcceptedSchemaContextSelection` bound to the schema/report digests and purpose.

### 4. Expand and construct a least-authority query projection

Expansion resolves the structural definitions needed to understand the selected semantic surface.
The query projection deliberately narrows authority back to the accepted semantic node membership
and selected relationships. Structural closure is diagnostic context, not automatic query
authority.

The projection records:

- admitted node labels, relationships, properties, and traversals;
- identity fields;
- exact, full-text, and optional vector capabilities;
- live index availability diagnostics;
- permitted query kinds;
- maximum limit, depth, timeout, result bytes, string sizes, and collection sizes;
- the exact selection, expansion, catalog, and SDL lineage.

Full-text/vector capabilities require both an SDL declaration and compatible live state. Offline or
missing indexes are not invented.

### 5. Apply the graph compatibility gate

The current experiment creates an explicitly test-only deployment attestation. Before obtaining a
Neo4j driver, the host checks exact equality between the authoritative SDL digest and the attested
deployed SDL digest.

Live reads additionally require:

```text
SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1
```

This attestation is intentionally impossible to mistake for a production attestation. A production
backend integration must replace it with an independently issued deployment manifest/attestation.

### 6. Run the query-planner SandboxAgent

The query planner receives a much smaller allowlisted view than the selection agents. It sees the
report, accepted selection, expanded slice, operation projection, live schema/index diagnostics,
and `selection/query-brief.json`.

The host compiles a deterministic bounded seed sequence for the TruDiagnostic baseline:

1. exact `Organization{name: "TruDiagnostic"}` identity;
2. one-hop `OFFERS` neighborhood;
3. one-hop TruAge product neighborhood;
4. one-hop TruHealth product neighborhood;
5. one-hop TruAge + TruHealth product neighborhood.

The agent must call the host-owned `execute_read_intent` tool for every required intent. It cannot
submit arbitrary Cypher. The host validates lineage and authority, compiles the intent to
parameterized Cypher, opens a Neo4j read session, executes it, and returns a bounded typed result.

The workflow fails rather than reporting completion when required intents do not successfully run.
Final agent evidence must reference the exact persisted intent/result pairs.

### 7. Persist and evaluate

Each run directory contains approximately this structure:

```text
<output-root>/<run-id>/
  run.json
  result.json
  metrics.json
  summary.md
  inputs/
  schema/
    manifest.json
    global/
    modules/
    cards/
    skills/
    runtime/
  selection/
    draft.json
    deterministic-validation.json
    review.json
    accepted.json
    expanded-slice.json
    operation-projection.json
    query-brief.json
  queries/
    001-intent.json
    001-result.json
    ...
    final-evidence.json
  agent-runs/
  traces/
  .sandbox-views/
```

The last successful tuning artifact is:

```text
.scratch/schema-context-selection-runs/live-windows-bind-9
```

It executed five real read-only Neo4j queries, returned 32 records, had zero rejected/failed
intents, and recovered 100% of the specified TruDiagnostic oracle.

The first official deterministic-catalog candidate is:

```text
.scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-3
```

Its accepted A/B comparison against `live-windows-bind-9` is:

```text
.scratch/schema-context-selection-runs/comparisons/
  official-catalog-v1-live-20260723-3-vs-live-windows-bind-9/
```

All nine acceptance gates passed. The candidate preserved 100% oracle recall, all 32 records,
all three offered products, exact `IMPLEMENTS` discrimination, schema compatibility, independent
acceptance, and five successful queries with zero rejected or failed intents. Relative to the
baseline it reduced catalog resources by 93.6%, catalog bytes by 80.1%, OpenAI input tokens by
40.4%, and total tokens by 36.4%. This single candidate run took 24.4% longer wall-clock time, so
latency needs repeated-run measurement before drawing a performance conclusion.

The preceding live attempt exposed an opaque `selection_id` transcription error in an otherwise
accepted independent review. The workflow now supplies the authoritative identifier explicitly and
retries one mismatched review against the same immutable draft. It persists the rejected attempt,
never rewrites the reviewer output, and accepts only an exactly bound review.

## Tests and what each test file proves

| Test file | Coverage |
|---|---|
| `tests/test_schema_catalog.py` | SDL directives, aliases, indexes, cards, manifests, digest determinism, and typed parse errors. |
| `tests/test_schema_context_selection.py` | Unknown-name/lineage rejection, selector cannot self-approve, and child selector/reviewer sequencing. |
| `tests/test_schema_expansion.py` | Deterministic closure, relationship endpoints, and union/interface endpoint resolution. |
| `tests/test_schema_operation_projection.py` | Least-authority labels and live-index admission without invented vector capability. |
| `tests/test_graph_query_intents.py` | Deterministic compilation and rejection of unknown labels/properties/relationships/indexes, excessive bounds, credentials, URIs, embeddings, writes, and custom Cypher. |
| `tests/test_neo4j_read_executor.py` | Record/list truncation, embedding removal, list-property snapshot safety, and failed-query versus valid-zero-result semantics. |
| `tests/test_report_graph_reconciliation_workflow.py` | Parent-to-child invocation and offline artifact behavior. |
| `tests/test_report_graph_reconciliation_gates.py` | Selection and schema-digest gates prevent all Neo4j calls; accepted selection produces one result per intent and exact evidence references. |
| `tests/schema_context_helpers.py` | Small deterministic SDL and typed fixtures shared by the tests. |

## Running the tests

From the repository root:

```powershell
Set-Location C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system
$env:UV_CACHE_DIR = "$PWD\.scratch\uv-cache"
uv sync --frozen
uv run ruff check app tests scripts
uv run mypy app
uv run pytest -q --basetemp .scratch/pytest-handoff
```

Run only this workflow's tests:

```powershell
uv run pytest -q `
  tests/test_schema_catalog.py `
  tests/test_schema_context_selection.py `
  tests/test_schema_expansion.py `
  tests/test_schema_operation_projection.py `
  tests/test_graph_query_intents.py `
  tests/test_neo4j_read_executor.py `
  tests/test_report_graph_reconciliation_workflow.py `
  tests/test_report_graph_reconciliation_gates.py `
  --basetemp .scratch/pytest-schema-reconciliation
```

The final verified repository-wide result was 136 passed and 7 skipped. There is one unrelated
Starlette/httpx deprecation warning.

## Running deterministic build-only mode

Build-only mode does not call OpenAI or Neo4j:

```powershell
uv run python scripts/run_schema_context_selection.py `
  --schema ..\biotech-kg\src\schema\neo4jbiotechschema.graphql `
  --report ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\reports\products-labtests-biomarkers.md `
  --structured-candidates ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\output\structured-extract-products-biomarkers.json `
  --model gpt-5-mini `
  --output-root .scratch\schema-context-selection-runs `
  --run-id handoff-build-only `
  --build-only
```

Use this first after changing catalog parsing, canonicalization, or workspace generation.

## Running selection without Neo4j

Offline mode still runs the real OpenAI selector and reviewer but does not obtain a Neo4j driver or
execute graph queries:

```powershell
uv run python scripts/run_schema_context_selection.py `
  --schema ..\biotech-kg\src\schema\neo4jbiotechschema.graphql `
  --report ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\reports\products-labtests-biomarkers.md `
  --structured-candidates ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\output\structured-extract-products-biomarkers.json `
  --model gpt-5-mini `
  --output-root .scratch\schema-context-selection-runs `
  --run-id handoff-offline `
  --offline `
  --skip-vector
```

`OPENAI_API_KEY` must be configured in `.env`. The report and selected schema resources are sent to
OpenAI during a real agent run, so authorization for that data handling must be explicit.

## Running the real OpenAI + Neo4j workflow

Prerequisites:

- Docker is running and can start `python:3.12-slim` containers;
- `OPENAI_API_KEY` is configured;
- Neo4j URI, username, password, and database configuration are available through `Settings`;
- the configured Neo4j principal is read-only where possible;
- the user has approved sending the report/schema and bounded query results to OpenAI;
- test-attestation use is explicitly enabled.

Run:

```powershell
$env:SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION = "1"

uv run python scripts/run_schema_context_selection.py `
  --schema ..\biotech-kg\src\schema\neo4jbiotechschema.graphql `
  --report ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\reports\products-labtests-biomarkers.md `
  --structured-candidates ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\output\structured-extract-products-biomarkers.json `
  --model gpt-5-mini `
  --output-root .scratch\schema-context-selection-runs `
  --run-id handoff-live-baseline `
  --max-query-intents 12 `
  --database neo4j `
  --skip-vector
```

Use a unique run ID. The workflow resets the directory for that specific run ID before starting.
Do not point `--output-root` at a directory containing non-run data.

The CLI prints only status and artifact location. Inspect `failure.json` for sanitized failures,
`summary.md` for the short outcome, `result.json` for the complete typed result, and `queries/` for
proof of actual Neo4j execution.

## Current sandbox/workspace footprint

The latest successful run confirms the optimization concern raised during review:

- `schema/global/compact-schema.json`: approximately 980 KB;
- `schema/global/compact-schema.md`: approximately 980 KB and largely duplicative;
- topology JSON/Markdown: approximately 156 KB each;
- each selector/reviewer sandbox view: about 3.68 MB across roughly 80 files;
- query-planner sandbox view: about 406 KB across 7 files.

The security boundary is sound—the views are allowlisted and read-only—but the selection workspace
is larger than it needs to be for efficient agent reasoning. “Compact” currently means compact
relative to the complete generated catalog, not compact in an agent/token sense.

## Recommended optimization direction

### 1. Introduce a physical Neo4j schema projection

The current catalog faithfully preserves much of the GraphQL directive schema. That is useful for
schema governance, but the selection/retrieval agents do not need most GraphQL API concerns.

Generate a separate, digest-bound `neo4j-physical-schema` projection containing only:

- concrete Neo4j labels;
- scalar stored properties and their types/nullability/list shape;
- relationship type, direction, concrete source labels, and concrete target labels;
- relationship-property types;
- identity candidates and constraints;
- full-text/vector/range index declarations;
- interface/union resolution only where required to map a directive endpoint to concrete labels;
- the original SDL locator and digest for every projected item.

Exclude or move to drill-down resources:

- GraphQL resolver/query names;
- API-only field arguments and connection types;
- duplicate Markdown renderings of machine-readable JSON;
- directives with no effect on Neo4j storage or query admission;
- descriptions and metadata irrelevant to the current purpose.

Do not parse `SHOW SCHEMA` as the authority. The versioned SDL remains authoritative; this is a
purpose-built physical projection derived from it.

### 2. Replace the 980 KB compact schema with tiers

Suggested layers:

```text
Tier 0: schema vocabulary
  node label names
  relationship type names
  index names
  module assignments
  one-line endpoint signatures

Tier 1: purpose shortlist
  report-matched labels/relationships
  neighboring endpoint candidates
  identity/search capabilities

Tier 2: exact cards
  properties, directives, aliases, endpoints, and evidence

Tier 3: full SDL slice
  only for ambiguity resolution or audit
```

The selector should normally solve the task from Tiers 0–2 without mounting the full compact schema.

### 3. Build a deterministic report-to-schema candidate brief

Before the semantic agent runs, use host-side deterministic extraction to create a small selection
brief from:

- report headings and high-frequency entities;
- exact schema vocabulary matches;
- aliases/trademark-normalized matches;
- relationship phrases such as offers, delivers, implements, measures, and uses;
- required purpose obligations;
- one-hop schema topology around matched types.

This brief should rank candidates but must not make the final semantic decision. The selector still
owns inclusion/exclusion rationale and the reviewer still independently approves it.

The existing structured candidate JSON should remain evaluation/reference data unless a future
variant explicitly tests it as an agent input.

### 4. Prefer typed lookup tools over a large static mount

Evaluate a selector variant with host-owned, read-only schema tools such as:

```text
search_schema_names(query, kinds, limit)
get_node_surface(label)
get_relationship_surface(type)
get_neighbors(label, direction, limit)
get_index_capabilities(label)
```

Every response should be digest-bound and bounded. This can reduce mounted files and tokens while
preserving deterministic authority. Keep a filesystem-navigation baseline so the two strategies can
be compared honestly.

### 5. Separate selection context from reviewer context

The reviewer currently receives nearly the same multi-megabyte schema view as the selector. A
reviewer generally needs:

- the request and report brief;
- the draft and deterministic validation;
- exact cards for selected items;
- exact cards for top excluded/near-miss candidates;
- endpoint closure and purpose-critical core checks.

Generate that review packet after selection instead of remounting the broad selector workspace.

### 6. Make the query planner even more evidence-focused

The tuned query planner is already much smaller than the selector. Further reduce it by mounting:

- `query-brief.json`;
- operation projection;
- a concise report entity list;
- live capability summary rather than the full live schema snapshot;
- prior typed query results as tool messages rather than duplicated files when retries occur.

The host should continue to own baseline compilation, safety admission, execution, and persistence.
Use the agent primarily for ambiguity handling, fallback selection, stopping decisions, and evidence
synthesis.

### 7. Cache immutable catalog artifacts by SDL digest

The schema workspace is rebuilt for every run. Cache the immutable catalog under its SDL/catalog
digest and create a lightweight per-run manifest referencing it. Preserve a fully self-contained
export option for audit and portability.

This should reduce disk churn without weakening lineage or reproducibility.

### 8. Measure optimization variants separately

Do not overwrite the successful baseline. Use distinct run IDs and compare:

- mounted file count and bytes by agent stage;
- files actually opened;
- OpenAI input/output tokens and request count by stage;
- selection core recall and unjustified additions;
- reviewer revision rate;
- query success/rejection rate;
- live-oracle recall;
- elapsed time;
- repeated-run selection stability after removing timestamps/provider IDs.

Recommended first variants:

1. current filesystem baseline;
2. physical-schema Tier 0 + exact cards;
3. Tier 0 + deterministic report candidate brief;
4. typed schema lookup tools without the broad compact schema;
5. optimized reviewer packet;
6. exact/full-text/topology baseline versus vector fallback only when necessary.

## Important invariants to preserve while optimizing

Optimization must not weaken these properties:

- the versioned SDL remains the schema authority;
- every derived resource retains exact digest lineage;
- selection remains semantic and independently reviewed;
- structural closure does not silently become query authority;
- graph authority remains separate from sandbox workspace access;
- compatibility is checked before creating/using the Neo4j driver;
- sandboxes receive no Neo4j credentials or Docker socket;
- agents cannot submit arbitrary Cypher;
- all report-derived values remain query parameters;
- all reads remain bounded and embeddings are stripped;
- every attempted intent receives a typed persisted result;
- failed, rejected, successful-zero, and successful-nonzero results remain distinct;
- a run cannot report completion without successful required Neo4j executions;
- no graph writes occur.

## Suggested next implementation slice

The highest-value next change is a deterministic `Neo4jPhysicalSchemaProjection` plus a Tier 0
vocabulary/topology file. Keep the existing catalog as the audit source, then change only the
selector/reviewer mount builders to consume the smaller projection and exact shortlisted cards.

Run it as a new named variant against TruDiagnostic and accept it only if it preserves:

- independently accepted semantic selection;
- exact `IMPLEMENTS` versus `IMPLEMENTS_PLATFORM` discrimination;
- all three offered products;
- relevant lab-test/panel/platform neighborhoods;
- 100% current oracle recall;
- zero unsafe or rejected baseline queries.

That gives a clean A/B comparison against `live-windows-bind-9` without discarding the proven
baseline strategy.
