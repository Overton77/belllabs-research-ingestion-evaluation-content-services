# Schema grounding pipeline

Issue 11 promotes the successful schema-context experiment into application-owned
capabilities. Experiment folders remain diagnostic adapters; production code does not
import them.

## Canonical components

| Component | Runtime form | Application owner |
| --- | --- | --- |
| `SchemaCatalogBuild` | deterministic shared operation | `app/application/schema_catalog_build.py` |
| `SchemaContextSelectionWorkflow` | independently runnable or linked Workflow Type | `app/application/schema_context_selection.py` |
| `SchemaContextDerivation` | deterministic shared operation | `app/application/schema_context_derivation.py` |
| `SchemaWorkspaceMaterialization` | governed operation whose durable contract is owned by Issue 13 | `app/application/schema_workspace_binding.py` consumes its result |
| `SupportingGraphReconciliationWorkflow` | bounded Workflow Type/stage | `app/application/supporting_graph_reconciliation.py` |

The control-plane fixtures in `app/domain/schema_grounding/definitions.py` publish both
Workflow Types, their StageGraph blueprints, control/runtime/workspace/evaluation
profiles, configurations, and the linked selection slot. They use the existing
`POST /control-plane/v1/definitions` publication boundary and compile through the normal
Effective Run Configuration path.

## Durable identity and authority

`SchemaCatalogBuild` accepts exact SDL, semantic-overlay, and optional candidate-seed
references plus SHA-256 digests. It authenticates all supplied bytes, parses the same
inputs twice to detect nondeterminism, and publishes one canonical, content-addressed
bundle. MongoDB stores immutable metadata and manifests; configured S3 stores the bundle
and preserves its object version ID when available.

Graph access is default-deny. Before a Neo4j driver can be created, the application
requires:

1. an active Issue 12 deployment manifest whose environment, database, SDL reference,
   and deployed digest exactly match the admitted catalog;
2. an Issue 13 read-only, run-scoped workspace binding whose catalog and resource
   manifest digests exactly match;
3. a separate graph-authority grant bound to the same run, purpose, environment,
   database, secret, and budget reservation; and
4. a purpose-bound projection with exact schema and selection lineage.

Agents produce typed read intents only. The host validates the allowlisted query kind,
projection lineage, limit, traversal depth, and absence of arbitrary Cypher. Every intent
and result is persisted, including rejected, failed, and successful-zero outcomes.
Reconciliation evidence is observational and explicitly does not claim broad Knowledge
Preflight coverage.

## Runtime and API

The main Temporal worker composes schema-grounding activities on
`<TEMPORAL_TASK_QUEUE>-schema-grounding`. Agent-backed selector, reviewer, and planner
operations execute through immutable Operation Execution Bindings. Schema Context
Derivation and catalog construction stay deterministic application services.

Authenticated, tenant-scoped query routes are under `/schema-grounding/v1`:

- catalog build and resource manifest;
- accepted selection and operation projection;
- per-run workspace binding and compatibility decision;
- per-run reconciliation and evaluation; and
- JSON Schemas for the public contracts.

There is intentionally no workflow start route here. Runs enter through the existing run
admission and Effective Run Configuration flow.

## Verification

```powershell
uv run ruff check app tests
uv run mypy app
uv run pytest -q
uv run python scripts/compare_schema_context_runs.py `
  --baseline .scratch/schema-context-selection-runs/live-windows-bind-9 `
  --candidate .scratch/schema-context-selection-runs/official-catalog-v1-live-20260723-3 `
  --output .scratch/schema-context-selection-runs/comparisons/issue-11-acceptance
```

The comparison is accepted only when all nine Issue 11 gates pass. The experiment CLI
also supports `--build-only` for a deterministic catalog smoke test without OpenAI or
Neo4j.
