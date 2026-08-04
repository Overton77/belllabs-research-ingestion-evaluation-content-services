# Coordinator MCP Phases 2-3 specification

Date: 2026-07-27  
Status: Implementation plan  
Depends on: accepted Phase 1 handoff  
Companion: `COORDINATOR_PHASES_2_3_WORKFLOW_CATALOG.md` — current Workflow Types, acceptance
path, contracts, implementations, and file locators for discovery work

## Objective

Give the coordinator decision-oriented Workflow Type discovery backed by exact, versioned,
schema-bearing contracts. An agent must be able to choose a published Workflow Type and approved
implementation, construct admitted inputs, and understand exclusions without loading raw catalog
records or reproducing admission rules.

Phase 2 supplies compact discovery and comparison. Phase 3 supplies the contract authority that
makes those views machine-usable. They share one caller-facing acceptance gate.

## Delivery sequence

### Slice A: exact contract model

Define exact, versioned contract records for these roles:

- workflow input;
- admission;
- operation input/output;
- workflow output;
- evaluation;
- decision.

Each record must carry:

- stable logical identity and revision;
- canonical digest;
- JSON Schema dialect/version;
- role and owning Workflow Type/Implementation where applicable;
- examples separated from normative schema;
- compatibility metadata;
- lifecycle/publication state.

The domain model owns contract identity and invariants. Persistence and catalog projection remain
adapters/read models.

### Slice B: compatibility reads

Current records contain opaque strings in paths such as `WorkflowDesignDraft.input_contract`.
Provide migration readers/adapters that:

- can read existing published definitions;
- label legacy opaque values as legacy rather than presenting them as JSON Schema;
- resolve migrated schema-bearing records by exact ref;
- prevent a legacy string from being returned through a URI claiming to be an exact schema;
- support a controlled publication/migration path without mutating immutable old revisions.

Do not infer a normative schema from prose at read time.

### Slice C: decision-oriented read models

Add cursor-paginated Workflow Type cards containing:

- exact current/default reference;
- purpose and non-goals;
- input/output summaries;
- StageGraph or GoalDirected family;
- lifecycle and maturity;
- approved implementation aliases;
- required capability and authority summaries;
- sensitivity and environment availability;
- budget/duration bands;
- standalone/composition use;
- launchability and typed exclusion reasons.

Add exact Workflow Type descriptions, approved implementation summaries, and semantic
implementation comparison.

Search results remain compact projections. Before returning a consequentially useful detail,
batch rehydrate selected authoritative definitions and verify revision/digest/current authority.

### Slice D: exact launch contract

Add an intent-specific launch-contract use case and MCP resource/tool view that returns:

- exact Workflow Type and selected/default implementation refs;
- exact admitted-input schema ref and schema URI;
- bounded overlay schema, if overlays are allowed;
- required/optional assets and capability constraints;
- workspace requirements;
- authority, approval, budget, and environment requirements;
- output contract refs;
- examples and compatibility notes;
- launchability and exclusion findings.

The launch input validator and admission path must use the same exact contract revision. Avoid
duplicated hand-written validation in MCP.

### Slice E: deterministic workflow-design validation

Expand validation findings to include:

- stable code and severity;
- JSON path;
- human-readable message;
- evidence and exact-definition references;
- suggested repair when safe;
- blocking/non-blocking classification.

Validation phases cover schema validity, contract conformance, StageGraph/GoalDirected structure,
obligations, outputs, linked-run slots, authority, workspace, asset promotion/fit, environment,
and publication requirements. A validated draft remains a draft.

## Target MCP surface

Add or mature:

- `search_workflow_types`
- `describe_workflow_type`
- `get_workflow_launch_contract`
- `compare_workflow_implementations`
- `validate_workflow_design`

Retain `search_capabilities` and `get_capability` as compatibility/advanced exact-retrieval
surfaces. Coordinator guidance prefers intent-specific operations.

Target resources include:

```text
belllabs://workflow-types/{id}/{revision}/summary
belllabs://workflow-types/{id}/{revision}/contract
belllabs://workflow-types/{id}/{revision}/launch-contract
belllabs://workflow-types/{id}/{revision}/implementations
belllabs://contracts/{id}/{revision}/schema
```

Every resource includes source revision and digest. Cursor tokens are opaque and bind to a stable
projection generation and ordering.

## Application ownership

Primary areas:

- `app/domain/control_plane/contracts.py`
- `app/domain/coordinator/contracts.py`
- `app/application/control_plane.py`
- `app/application/capability_search.py`
- `app/application/catalog_projection.py`
- `app/application/catalog_projection_generation.py`
- `app/application/catalog_projection_metadata.py`
- `app/application/postgres_capability_search_repository.py`
- `app/application/coordinator_facade.py`
- `app/mcp/coordinator_server.py`
- `app/mcp/coordinator_resources.py`

Add query DTOs and use cases to existing coordinator/control-plane packages unless a genuinely
distinct lifecycle is accepted.

## Pagination and authority rules

- Cursor continuation must preserve filter, tenant scope, projection generation, sort, and page
  boundary.
- Fixed-generation pagination cannot duplicate or skip records.
- Coverage/count metadata must state whether it is exact, bounded, or unavailable.
- Projection rank is never permission or launchability.
- Candidate-only external records cannot appear as approved implementations.
- Alias resolution occurs server-side and returns exact refs.
- Environment availability is observed state and must include observation time or snapshot
  identity.

## Verification

### Contract tests

- Valid schemas identify their dialect and canonical digest.
- Invalid schemas cannot publish.
- Legacy opaque contracts are readable only through explicit compatibility views.
- Exact schema resources never return prose disguised as JSON Schema.
- Launch and admission validation use the same contract ref/digest.
- New immutable revisions preserve old exact reads.

### Discovery tests

- Cursor pagination is stable under a fixed projection generation.
- Cursor reuse with changed filters/scope fails.
- Selected cards are batch rehydrated and digest-checked.
- Launchability reasons distinguish forbidden, incompatible, unavailable, candidate-only, and
  deprecated/retired states.
- Implementation comparison reports typed differences rather than prose-only summaries.
- Large catalogs do not cause one authoritative read per search hit.

### Agent acceptance

Starting only with an objective and authenticated scope, an agent can:

1. search Workflow Types;
2. compare appropriate approved implementations;
3. retrieve the exact launch contract and schema;
4. construct a schema-valid request;
5. receive deterministic validation findings;
6. prepare through the unchanged Phase 1 governed path.

## Exit criteria

Phases 2-3 are accepted when:

1. all caller-facing launch contract roles are exact and schema-bearing, or explicitly identified
   as unsupported legacy records;
2. discovery cards and comparisons are paginated, compact, and authority-safe;
3. an agent can choose and prepare without guessing input shape;
4. admission and coordinator validation agree on the exact contract;
5. no new read model can become executable authority.

## Incoming Phase 1 checks

Before integration, confirm:

- canonical facade and MCP envelope versions;
- effective provider/bootstrap logic;
- canonical run result URI;
- exact launch/result contract compatibility;
- mounted endpoint authorization and scope behavior.

If Phase 1 changed one of these after handoff, rebase the public resource and test contracts
before acceptance.

## Outgoing handoff to Phase 4

Include:

- exact contract taxonomy and schema dialect;
- resource URI inventory;
- legacy compatibility/migration matrix;
- pagination token semantics and projection generation rules;
- Workflow Type card and implementation-comparison schemas;
- validation finding codes;
- accepted launch-contract examples;
- known catalog records that remain legacy or non-launchable.

Phase 4 must consume exact asset/workspace/linked-slot constraints from these contracts. It must
not parse human summaries to infer compatibility.

## Explicit non-goals

- Governing external candidate promotion.
- Snapshot bytes or credentials through search.
- Launching a composition as one merged StageGraph.
- Progressive report/artifact/evidence navigation.
- Sandbox bundle materialization.
