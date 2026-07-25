# Issue 11 Schema Grounding Pipeline — implementation handoff

Date: 2026-07-24  
Audience: implementation agent or engineer promoting the experiment into canonical backend services  
Issue: [replacement Issue 11](../issues/11-schema-catalogs-and-selections.md)  
Spec: [Schema Grounding Pipeline specification](../specs/11-schema-grounding-pipeline-spec.md)

## 1. Executive state

The experiment succeeded and is now a real behavioral baseline.

Do not restart the design from the original Issue 11 assumptions. The repository already proves:

- typed deterministic parsing and derivation from the authoritative Neo4j GraphQL SDL;
- governed semantic overlay validation;
- compact Tier 0 and report-derived candidate workspace profiles;
- semantic selection with deterministic validation and independent review;
- deterministic closure and purpose-bound operation projection;
- fail-closed schema compatibility before graph access;
- bounded, typed, parameterized Neo4j read intents;
- persisted intent/result/evidence lineage;
- a successful live TruDiagnostic replay; and
- an accepted A/B comparison with large resource and token reductions.

What remains is promotion: durable repositories and object storage, published control-plane
definitions, Run Request admission, Operation Execution Bindings, Temporal execution, public query
projections, Issue 12’s production deployment attestation, and Issue 13’s production workspace
binding/graph gate.

## 2. Naming decision

Use **Schema Grounding Pipeline** for the five-component capability:

1. `SchemaCatalogBuild` — deterministic shared operation.
2. `SchemaContextSelectionWorkflow` — independently runnable or linked Workflow Type.
3. `SchemaContextDerivation` — deterministic expansion and projection operation.
4. `SchemaWorkspaceMaterialization` — shared binding/compatibility/capability operation.
5. `SupportingGraphReconciliationWorkflow` — bounded observational Workflow Type or declared stage.

The current experiment parent `ReportGraphReconciliationWorkflow` should become
`SupportingGraphReconciliationWorkflow` when promoted.

Do not rename this whole sequence `KnowledgePreflightWorkflow`.

`KnowledgePreflightWorkflow` is broader and already canonically defined. It consumes the Schema
Grounding Pipeline but adds a Brief, Coverage Matrix, multimodal Query Plan, retrieval observations,
candidates/contradictions/gaps, immutable Snapshot, Freshness Assessment, and Decision Report.

## 3. Governing sources

Read in this order:

1. `biotech-meta/docs/CONTEXT.md`
2. `biotech-meta/docs/specs/pre-research/README.md`
3. `biotech-meta/docs/specs/pre-research/control-plane-capabilities/01-schema-catalog-deployment-manifest-and-workspace-materialization.md`
4. `biotech-meta/docs/spec_synthesis/2026-07-18-knowledge-preflight-workflow-synthesis.md`
5. the companion Issue 11 and specification linked above
6. `tests/SCHEMA_CONTEXT_SELECTION_WORKFLOW_HANDOFF.md`

If a local implementation detail conflicts with those authority/persistence/workflow distinctions,
preserve the governing distinction and migrate the implementation explicitly.

## 4. Experiment code map

### 4.1 Typed catalog core

```text
app/domain/schema_catalog/models.py
app/domain/schema_catalog/parser.py
app/domain/schema_catalog/overlay.py
app/domain/schema_catalog/validation.py
app/domain/schema_catalog/derivation.py
app/domain/schema_catalog/renderer.py
app/domain/schema_catalog/errors.py
```

Important behavior:

- the physical digest excludes the source path;
- `IN` and `OUT` relationship fields retain correct physical start/end nodes;
- relationship-property types are distinct from selectable node types;
- semantic overlay drift fails closed;
- Tier 0 rendering is typed and deterministic.

### 4.2 Compatibility adapter and workspace renderer

```text
app/application/schema_catalog.py
app/application/schema_workspace.py
schema-catalog/semantic-overlay.v1.json
schema-catalog/source-reference.v1.json
```

`app/application/schema_catalog.py` keeps the legacy catalog contract used downstream while sourcing
physical truth from the typed parser.

`app/application/schema_workspace.py` emits:

```text
schema/manifest.json
schema/overview/tier0.json
schema/profiles/selection-tier0.json
schema/profiles/selection-candidates.json
schema/elements/nodes/<name>/detail.json
schema/elements/relationships/<type>/detail.json
schema/skills/schema-navigation/SKILL.md
```

There are no Markdown twins or separate drill-down copies in the optimized profiles.

### 4.3 Selection and reconciliation experiment

```text
app/experiments/schema_context_selection/agents.py
app/experiments/schema_context_selection/selection_workflow.py
app/experiments/schema_context_selection/reconciliation_workflow.py
app/experiments/schema_context_selection/evaluation.py
app/experiments/schema_context_selection/comparison.py
app/experiments/schema_context_selection/prompts.py
app/experiments/schema_context_selection/workspace.py
```

The parent experiment:

1. freezes input files and digests;
2. builds/rebuilds the catalog to prove deterministic digest stability;
3. materializes the optimized schema workspace;
4. invokes `SchemaContextSelectionWorkflow`;
5. expands accepted context and builds an operation projection;
6. applies a test-only schema deployment attestation;
7. captures live Neo4j schema/index capability;
8. gives the query planner only a typed `execute_read_intent` tool;
9. persists every intent and result;
10. verifies final evidence references exactly match the persisted sequence; and
11. writes metrics, result, summary, usage, and trace artifacts.

### 4.4 Guarded graph integration

```text
app/application/graph_query.py
app/integrations/neo4j_read_executor.py
app/domain/schema_context/contracts.py
app/domain/schema_context/validation.py
app/domain/schema_context/expansion.py
app/domain/schema_context/projection.py
```

The query planner cannot submit arbitrary Cypher. Host compilers admit exact/bounded intent kinds,
apply values as parameters, enforce the projection, truncate bounded results, and remove embedding
fields.

### 4.5 CLI and comparison

```text
scripts/run_schema_context_selection.py
scripts/compare_schema_context_runs.py
```

These remain useful adapters and acceptance tools. They must not remain the production control
surface.

## 5. Authoritative schema publication

Schema source:

```text
biotech-kg/src/schema/neo4jbiotechschema.graphql
```

SHA-256:

```text
86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112
```

Private immutable object:

```text
s3://belllabs-biotech-schema-298199649527-us-east-1/
  schemas/neo4jbiotechschema/sha256/
  86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112/
  neo4jbiotechschema.graphql
```

Version ID:

```text
J6d_lL6g2TEtTL9Imi6hwIMBNSTSahEB
```

Local durable reference:

```text
schema-catalog/source-reference.v1.json
```

The bucket was verified with public access blocked, `BucketOwnerEnforced`, versioning enabled, and
AES-256 default encryption. `scripts/publish_schema_to_s3.ps1` now reads back all four controls.

This object is a durable Schema Definition reference. It is not a production Schema Deployment
Manifest; Issue 12 owns that attestation.

## 6. Successful artifacts

### 6.1 Optimized live run

```text
.scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-3
```

Important files:

```text
run.json
inputs/source-manifest.json
schema/manifest.json
schema/overview/tier0.json
schema/profiles/selection-tier0.json
schema/profiles/selection-candidates.json
selection/draft.json
selection/deterministic-validation.json
selection/review.json
selection/accepted.json
selection/expanded-slice.json
selection/operation-projection.json
selection/query-brief.json
schema/runtime/deployment-attestation.json
schema/runtime/compatibility-decision.json
schema/runtime/live-schema.json
schema/runtime/live-indexes.json
queries/001-intent.json ... 005-intent.json
queries/001-result.json ... 005-result.json
queries/final-evidence.json
metrics.json
result.json
summary.md
traces/
agent-runs/
```

### 6.2 Baseline

```text
.scratch/schema-context-selection-runs/live-windows-bind-9
```

Do not overwrite it.

### 6.3 Comparison

```text
.scratch/schema-context-selection-runs/comparisons/
  official-catalog-v1-live-20260723-3-vs-live-windows-bind-9/
    comparison.json
    comparison.md
```

Comparison digest:

```text
sha256:c06ab6c47efa3cf6ade04b440d2ec07c4cfba512e65dea3552a7b09a1cf6595c
```

### 6.4 Failed run worth preserving

```text
.scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-2
```

This run produced a structurally valid selection and an “accepted” review, but the reviewer
mistyped one character in the opaque `selection_id`. The host correctly refused acceptance.

The live-discovered fix:

- explicitly gives the reviewer the authoritative ID;
- persists a mismatched review;
- retries the reviewer once against the same draft;
- never rewrites reviewer output; and
- accepts only an exact binding.

The dedicated regression test is in `tests/test_schema_context_selection.py`.

## 7. Proven metrics

Official catalog:

- catalog digest:
  `sha256:94cd791e4daa058a5135b50a31641ca1476a7f820d7f0e3294105d42726a267e`;
- generator: `typed-schema-catalog-v1`;
- Tier 0: 49,911 bytes;
- 91 selectable node types;
- 22 relationship-property types;
- 206 relationship types;
- 14 governed overlay elements;
- five governed modules;
- 67 optimized catalog resources.

A/B result:

| Metric | `live-windows-bind-9` | Optimized | Delta |
|---|---:|---:|---:|
| Catalog resources | 1,046 | 67 | -93.6% |
| Catalog bytes | 7,837,446 | 1,559,712 | -80.1% |
| Input tokens | 394,797 | 235,409 | -40.4% |
| Total tokens | 406,001 | 258,035 | -36.4% |
| Selection revisions | 2 | 2 | no change |
| Oracle recall | 1.0 | 1.0 | preserved |
| Query records | 32 | 32 | preserved |
| Elapsed time | 234,192 ms | 291,395 ms | +24.4% |

Query outcome:

- five intents;
- five successes;
- zero rejections;
- zero failures;
- zero truncations;
- 32 records;
- all three products;
- exact `IMPLEMENTS`;
- 100% workload oracle recall.

The elapsed-time increase is a visible unresolved measurement. Run repeated candidates before
changing timeout/concurrency/model policy based on one sample.

## 8. Current verification

Final repository-wide result:

```text
Ruff: clean
mypy: clean across 121 source files
pytest: 136 passed, 7 skipped
```

There is one unrelated Starlette/httpx deprecation warning.

## 9. Commands

From the repository root:

### 9.1 Build only

```powershell
.\.venv\Scripts\python.exe scripts\run_schema_context_selection.py `
  --schema ..\biotech-kg\src\schema\neo4jbiotechschema.graphql `
  --report ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\reports\products-labtests-biomarkers.md `
  --structured-candidates ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\output\structured-extract-products-biomarkers.json `
  --model gpt-5-mini `
  --output-root .scratch\schema-context-selection-runs `
  --run-id issue-11-build-check `
  --build-only
```

### 9.2 Live replay

This sends the admitted report, structured candidates, optimized schema workspace, and bounded
query results to OpenAI. It also performs read-only Neo4j queries. Future production execution must
authorize this through the compiled sensitive-data policy rather than a shell flag.

```powershell
$env:SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION = "1"

.\.venv\Scripts\python.exe scripts\run_schema_context_selection.py `
  --schema ..\biotech-kg\src\schema\neo4jbiotechschema.graphql `
  --report ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\reports\products-labtests-biomarkers.md `
  --structured-candidates ..\biotech-kg\research\trudiagnostic-20260330-203619-research-mission\output\structured-extract-products-biomarkers.json `
  --model gpt-5-mini `
  --output-root .scratch\schema-context-selection-runs `
  --run-id <unique-run-id> `
  --max-query-intents 12 `
  --database neo4j `
  --skip-vector
```

Never treat `SCHEMA_EXPERIMENT_ALLOW_TEST_ATTESTATION=1` as production authority.

### 9.3 Compare

```powershell
.\.venv\Scripts\python.exe scripts\compare_schema_context_runs.py `
  --baseline .scratch\schema-context-selection-runs\live-windows-bind-9 `
  --candidate .scratch\schema-context-selection-runs\<candidate-run-id> `
  --output .scratch\schema-context-selection-runs\comparisons\<comparison-id>
```

### 9.4 Repository checks

```powershell
.\.venv\Scripts\ruff.exe check app tests scripts
.\.venv\Scripts\mypy.exe app
.\.venv\Scripts\python.exe -m pytest -q --basetemp .scratch\pytest-schema-grounding
```

## 10. Promotion sequence

### Step 1 — Correct the package dependency

Current temporary problem:

```text
app/application/schema_context_selection.py
  -> imports app.experiments.schema_context_selection.selection_workflow
```

Move the service and ports into `app/application`. Canonical app code must never import experiments.

Keep the experiment runner as a thin adapter over the new service and prove identical artifacts and
comparison gates.

### Step 2 — Add immutable repository contracts

Create application ports for:

- catalog build metadata and resource lookup;
- catalog bundle payload publication/retrieval;
- semantic-overlay revisions;
- selection/validation/review/acceptance;
- expanded slices and projections;
- compatibility decisions and workspace bindings;
- query intents/results and reconciliation evidence;
- evaluation/comparison records.

Use Beanie/MongoDB adapters for document-shaped records and S3 for large payloads. Do not make local
folders the production repository.

### Step 3 — Publish control-plane definitions

Create exact revisions for:

- `schema-context-selection` Workflow Type and StageGraph blueprint;
- `supporting-graph-reconciliation` Workflow Type and StageGraph blueprint;
- runtime/control/workspace/evaluation profiles;
- pre-provisioned operation contract refs;
- agent/prompt/output schemas;
- semantic overlay and catalog generator;
- sensitive-data and graph-read policy.

Compile them through `ControlPlaneService`; do not hard-code model/prompt/workspace/limit choices in
Temporal workflow code.

### Step 4 — Execute through canonical operations

Replace direct `SandboxAgentHarness` calls with `OperationExecutionService` requests. Each selector,
reviewer, and planner attempt must have:

- semantic execution identity;
- immutable binding;
- exact prompt/agent/model/tool/output-schema refs;
- workspace binding;
- authority and secret refs;
- budget reservation;
- side-effect/idempotency key;
- settlement and usage.

### Step 5 — Integrate Run Control and Temporal

- Admit standalone/linked runs through `/run-control/run-requests`.
- Use the generic StageGraph workflow when sufficient.
- Register required activities/workers/task queues.
- Use existing lifecycle commands for waits, pause/resume, cancellation, continuation, linked
  results, and finalization.
- Persist durable outbox events and query projections.

Do not add a CLI-shaped FastAPI “run workflow now” bypass.

### Step 6 — Add read/query surfaces

Add an authenticated `app/api/schema_grounding.py`-style router for:

- catalog build/resource lookup;
- accepted selection/review;
- expanded slice/projection;
- workspace/compatibility status;
- reconciliation evidence and evaluation;
- stage/revision/query/usage summaries.

Include schemas in the existing schema discovery surfaces and publish authorized durable Socket.IO
projection changes.

### Step 7 — Replace test authority

Integrate:

- Issue 12’s production Schema Deployment Manifest;
- Issue 13’s durable Schema Workspace Binding;
- a separately admitted Neo4j read capability and secret ref.

Prove no driver or query is created for missing/revoked/wrong-environment/hash-mismatch/unauthorized
outcomes.

### Step 8 — Compose Knowledge Preflight

Only after the pipeline is canonical, compose it into `StageGraphPreflight` and
`GoalDirectedPreflight`. Add the Knowledge Preflight-specific domain records rather than relabeling
Supporting Graph Reconciliation.

## 11. Known gaps and traps

1. **Test-only attestation:** the experiment compatibility gate proves mechanics, not production
   deployment authority.
2. **Local persistence:** `.scratch` artifacts are excellent acceptance evidence but not production
   repositories.
3. **Direct CLI/SDK path:** the experiment bypasses Run Request admission, Operation Execution
   Binding, Temporal, and public query projections.
4. **Reverse dependency:** `app/application/schema_context_selection.py` imports experiments.
5. **Dual catalog surface:** the compatibility adapter still supports the larger legacy catalog;
   do not delete it until canonical consumers use the optimized profiles.
6. **Latency:** context reduction did not reduce wall time in the first live candidate.
7. **Agent identity transcription:** preserve the fail-closed reviewer-binding retry.
8. **Data authorization:** previous user authorization does not become a universal production
   sensitive-data policy.
9. **Graph authority:** a schema workspace never implies credentials or graph access.
10. **Terminology:** bounded reconciliation is not broad Knowledge Preflight and does not resolve
    identity.
11. **Temporal determinism:** database, filesystem, SDK, Docker, S3, and Neo4j work belongs in
    activities/services.
12. **Retry identity:** Temporal attempts, provider requests, semantic operation attempts, selection
    revisions, StageGraph cycles, and GoalDirected iterations are different axes.

## 12. First implementation checkpoint

A strong first pull request should:

1. introduce canonical application ports/services for catalog build, selection, and derivation;
2. remove the canonical import from `app.experiments`;
3. keep the CLI as an adapter to the new service;
4. add in-memory conformance repositories;
5. prove the successful run’s catalog/selection/derivation artifacts remain equivalent;
6. retain all nine comparison gates; and
7. keep the full repository suite green.

Do not combine the first checkpoint with production deployment-manifest issuance, final API design,
or full Knowledge Preflight. Those are separate seams with separate authorities.

## 13. Handoff completion test

Before declaring Issue 11 complete, an implementation agent must be able to answer “yes” with
durable evidence:

- Is all canonical schema-grounding behavior under `app/domain`, `app/application`,
  `app/integrations`, and `app/temporal`, with no canonical import from experiments?
- Can a published Workflow Type compile into an Effective Run Configuration and be admitted as a
  standalone or linked run?
- Does every semantic agent operation have an immutable Operation Execution Binding and budget
  settlement?
- Are catalog bundles and large evidence content-addressed in object storage?
- Are build/selection/derivation/reconciliation records queryable and immutable?
- Does live graph work require both a real deployment attestation and graph capability?
- Can an authenticated client reconnect and query durable stage/revision/gate/result state?
- Does the TruDiagnostic workload still pass all nine gates?
- Are repeated-run stability and latency reported?
- Do Ruff, mypy, unit, integration, and production-shaped acceptance tests pass?
