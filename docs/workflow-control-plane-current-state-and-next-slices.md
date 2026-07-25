# Workflow Control Plane: Current State and Next Slices

Date: 2026-07-25

Status: Implementation alignment note. Product authority remains in `biotech-meta`.

Primary architecture note:
`../../biotech-meta/docs/workflow-catalog-configuration-composition-and-agent-entry.md`

Companion code trace:
`CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`

## What the app currently proves

The current app is not merely a Temporal workflow prototype. Its control-plane domain already
contains a coherent compilation boundary:

- `app/domain/control_plane/contracts.py`
  - immutable exact definition references;
  - Workflow Type, StageGraph, GoalDirected, control, runtime, workspace, evaluation,
    workflow-specific configuration, and Workflow Implementation definitions;
  - typed run overlays;
  - immutable Effective Run Configuration.
- `app/domain/control_plane/compiler.py`
  - pure compilation;
  - authority intersection;
  - overlay validation;
  - environment availability decisions;
  - canonical digest generation.
- `app/application/control_plane.py`
  - draft/publication/alias resolution;
  - default implementation resolution through
    `{workflow_type.logical_id}.implementation:default`;
  - exact component loading and implementation conformance checks;
  - Effective Run Configuration persistence.
- `app/domain/composition` and `app/application/linked_runs.py`
  - linked-run identities, dependency semantics, child compilation, and result admission.
- `app/domain/operation_execution/contracts.py`
  - exact prompt, model, tool, MCP, skill, plugin, agent, delegation, workspace, authority,
    and snapshot bindings.
- `app/application/sandbox_snapshots.py`
  - immutable snapshot creation;
  - clone-on-restore;
  - digest/contract/capability compatibility;
  - live resource reacquisition.

The existing design therefore supports the recommended meaning of a domain default: an approved
Workflow Implementation revision selected through a conventional `default` alias.

## Drift in the project-organization rule

`.cursor/rules/project-organization.mdc` is directionally correct about layering and authority,
but its phase warning—“do not invent Workflow Type behavior ahead of accepted tickets”—predates
the implemented Workflow Implementation binding, default alias resolution, compiled workspace
contracts, linked-run execution, operation bindings, and sandbox snapshots.

Continue to respect its hard rules:

- domain truth in `app/domain` and application use cases in `app/application`;
- Temporal and vendor adapters are mechanics;
- `.scratch`, local `docs`, and experiments are not product authority;
- vocabulary comes from `biotech-meta`.

Interpret the phase focus against the newer accepted foundation/capability specifications and the
2026-07-25 architecture note rather than as evidence that Workflow Types are still wholly
unmodeled.

## Gaps in the present public surface

`app/api/control_plane.py` currently supports:

- publish definition;
- save/get/publish draft;
- move/resolve alias;
- compile;
- retrieve Effective Run Configuration;
- retire;
- export schemas.

It does not yet support:

- list/search workflow types;
- describe a Workflow Type as a caller-facing launch contract;
- list or compare approved implementations;
- semantic diff from the default implementation;
- preview compilation/launch without admission;
- first-class input contract retrieval;
- search compatible skills, MCP servers, or Agent Profiles;
- search snapshots or assess compatibility;
- composition plan preview.

The repository interface similarly provides point lookup and mutation but no catalog query
methods. This is the largest gap between the internal architecture and a usable agent/operator
experience.

## Recommended local slices

### Slice A: workflow discovery

Add application read models, not direct Beanie documents:

```text
WorkflowTypeCard
WorkflowTypeDescription
WorkflowImplementationSummary
WorkflowImplementationComparison
ConfigurationAffordance
LaunchabilityAssessment
```

Add repository queries with cursor pagination and exact filters. Start with indexed lexical and
structured filtering over kind, logical ID, lifecycle, blueprint family, input/output kinds,
capabilities, and implementation aliases. A rebuildable enriched search projection can follow.

### Slice B: launch contract and preview

Add a small caller-facing `WorkflowLaunchRequest`. An application service should:

1. resolve Workflow Type and optional implementation aliases;
2. load/generate the input schema and configuration affordances;
3. convert admitted caller inputs into a Run Input Manifest reference;
4. construct the internal `CompileInvocation` using authoritative identity, authority, and
   environment data;
5. return a preview with exact refs, overlay decisions, availability, approvals, and differences
   from default.

Admission must repeat authoritative checks; preview is not a reservation or permission.

### Slice C: schema-bearing contracts

Replace opaque workflow fields such as `input_admission_contract: str` and string-only output/gate
refs with exact refs to versioned contract definitions. A contract revision should expose:

- stable ID, revision, digest, and lifecycle;
- JSON Schema;
- semantic role: input, admission, operation, output, evaluation, or decision;
- examples and concise agent-facing description;
- compatibility and migration metadata.

Use a migration layer so current fixtures and published records can be read while definitions are
upgraded.

### Slice D: agentic catalog resolution

Keep catalog definitions separate from `OperationExecutionBinding`. The catalog owns reviewed
candidates and exact immutable revisions; the binding records what one operation actually used.

Add operation-class attachment rules to Workflow Type/Implementation configuration. Resolve
requirements by intersecting workflow allowlists, caller authority, permissions, environment
availability, and promotion/health state. Materialize only exact digests.

### Slice E: snapshot discovery

The snapshot contract already contains most compatibility inputs. Add:

```text
SnapshotSearchFilter
SnapshotSummary
SnapshotCompatibilityRequest
SnapshotCompatibilityAssessment
```

Compatibility results should report precise mismatch codes for runtime, image, package,
environment, workflow contract, mount manifest, capability shape, retention, and payload
integrity. Never expose snapshot capability history as current authority.

### Slice F: composition authoring

Build composition templates and plans over existing linked-run slots and request contracts.
Do not concatenate StageGraph definitions. Each child remains separately admitted, configured,
budgeted, controlled, and evaluated.

## Packaging guidance

Prefer extending existing packages:

```text
app/domain/control_plane/       discovery and launch contracts
app/application/control_plane*  query, compare, preview use cases
app/api/control_plane.py        read/preview endpoints
app/domain/operation_execution/ snapshot compatibility read contracts
app/application/sandbox_snapshots.py
```

Create a new domain package only when it owns a distinct lifecycle. Governed agentic asset
catalogs qualify; a collection of control-plane query DTOs does not.

## Tests to add with the slices

- default implementation alias resolves to an exact revision and later alias movement does not
  alter previews already compiled;
- search returns compact projections but launch re-resolves authoritative exact records;
- implementation comparison reports a typed semantic diff;
- launch schema exposes only contract-permitted configuration fields;
- preview rejects unknown, authority-expanding, and invariant-weakening overlays;
- first-class input contract schemas validate exactly as the admission service does;
- catalog search never returns quarantined/revoked assets as selectable;
- snapshot compatibility explains every digest/capability mismatch;
- restoring a compatible snapshot still creates a new workspace and reacquires live resources;
- composition preview cannot create a child outside the parent's declared linked-run slot.
