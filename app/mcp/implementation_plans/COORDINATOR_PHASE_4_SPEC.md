# Coordinator MCP Phase 4 specification

Date: 2026-07-27  
Status: Implementation plan  
Depends on: accepted Phases 2-3 handoff

## Objective

Enable the coordinator to assess agentic asset fit, find compatible sandbox snapshots, and
construct a governed multi-run composition plan. Every selection or rejection must be explained
with exact evidence and typed reasons.

This phase exposes existing snapshot and linked-run authority through decision-oriented
application use cases. It does not make MCP, search projections, or the coordinator agent the
authority for compatibility or run admission.

## Scope A: agentic asset fit

Add intent-specific search and assessment over:

- Workflow Type and exact revision;
- Workflow Implementation and operation class;
- Agent Profile, Skill, Prompt, MCP Server, MCP Tool, model/runtime profile, and workspace
  template;
- requested versus allowed authority;
- required capabilities and secrets by reference;
- environment/runtime availability;
- lifecycle, inspection, review, and promotion state.

Return compact cards plus a typed decision:

- selectable;
- candidate-only;
- incompatible;
- unavailable;
- forbidden.

Each decision includes stable reason codes, exact refs/digests, the tested operation/workflow
context, and current environment observation identity. External discovery results remain
candidate-only until inspected and governed promotion creates exact catalog authority.

## Scope B: snapshot discovery and compatibility

Build coordinator use cases over `app/application/sandbox_snapshots.py` for:

- metadata search with opaque cursor;
- exact snapshot summary;
- lineage, base snapshot, retention, and lifecycle;
- content/manifests by digest without returning raw bytes;
- compatibility assessment against an exact implementation, operation, runtime, and workspace
  contract;
- typed mismatches and live resources that must be reacquired.

Never return credentials, snapshot bytes, tenant secrets, or unrestricted filesystem paths.
Restoration remains clone-based. An incompatible restore requires an authored migration/new
snapshot; the coordinator cannot waive compatibility.

## Scope C: composition templates and plans

Add:

- `search_composition_templates`
- `preview_workflow_composition`
- `validate_workflow_composition`

A composition plan connects independently admitted Workflow Runs. For each parent/child relation
it identifies:

- parent linked-run slot and purpose;
- child exact Workflow Type/Implementation;
- dependency class;
- typed input/output artifact mapping;
- delegated authority ceiling/intersection;
- budget reservation;
- timeout, cancellation, and failure propagation;
- result admission policy;
- provenance and invalidation behavior.

Validation must prove slot legality and contract compatibility without admitting a run.
Preparation later creates separately frozen/admitted child requests. Composition must never
concatenate or rewrite parent or child StageGraph/GoalDirected blueprints.

## Scope D: decision findings

Use one finding shape across fit, snapshot, and composition assessments:

- code;
- severity;
- JSON path or plan component;
- decision state;
- message;
- exact evidence refs/digests;
- current environment/policy observation;
- blocking or degradation classification;
- suggested repair when the server can state one safely.

At minimum, findings distinguish:

- authority intersection failure;
- missing/unpromoted capability;
- runtime unavailable versus incompatible;
- snapshot content/runtime mismatch;
- workspace contract mismatch;
- undeclared linked-run slot;
- artifact mapping mismatch;
- budget reservation failure;
- forbidden result admission;
- stale projection or exact-ref digest.

## Target MCP surface

Add or mature:

- `search_agentic_assets`
- `search_sandbox_snapshots`
- `assess_snapshot_compatibility`
- `search_composition_templates`
- `preview_workflow_composition`
- `validate_workflow_composition`

Exact retrieval remains available through catalog/snapshot resources. Search responses contain
metadata and decisions, not secret material or executable authority.

## Application ownership

Primary areas:

- `app/domain/coordinator/contracts.py`
- `app/domain/composition/contracts.py`
- `app/domain/control_plane/contracts.py`
- `app/domain/operation_execution/contracts.py`
- `app/application/capability_search.py`
- `app/application/sandbox_snapshots.py`
- `app/application/workspace_materialization.py`
- `app/application/linked_runs.py`
- `app/application/orchestration.py`
- `app/application/operation_execution.py`
- `app/application/coordinator_facade.py`
- `app/mcp/coordinator_server.py`
- `app/mcp/coordinator_resources.py`

Use existing Mongo/PostgreSQL repositories where their lifecycle matches. Add query projections
without changing snapshot or linked-run authority.

## Verification

### Asset-fit tests

- Search context binds Workflow Type, implementation, operation, tenant, and environment.
- Candidate-only assets cannot become selectable through ranking.
- Revoked/retired or digest-mismatched records fail closed.
- Agent Profile dependency closure resolves exact assets once.
- Authority and capability decisions explain every exclusion.

### Snapshot tests

- Search exposes metadata, lineage, digests, and retention only.
- Cross-tenant snapshot access is denied.
- Compatibility returns stable typed mismatch codes.
- Restore creates a fresh clone and preserves immutable source snapshot.
- Credentials and live resource handles are reacquired rather than restored.

### Composition tests

- Undeclared slots and incompatible artifact mappings fail validation.
- Child authority is the required intersection, never the union.
- Budget, timeout, dependency class, cancellation, and result admission are explicit.
- Child output cannot satisfy a parent obligation before result admission.
- Parent and child blueprints remain byte/digest identical through planning.
- Preview/validation creates no run.
- Any later preparation produces a distinct request/ticket for each run.

## Exit criteria

Phase 4 is accepted when:

1. the coordinator can explain why each required asset and snapshot is selectable, incompatible,
   unavailable, candidate-only, or forbidden;
2. snapshot discovery and assessment expose no bytes or credentials;
3. a valid multi-run plan can be previewed against exact linked-run slots;
4. invalid authority, budget, mapping, workspace, and result-admission cases fail with typed
   findings;
5. no composition code rewrites Workflow Implementation internals.

## Incoming Phases 2-3 checks

Confirm:

- exact contract and launch-contract resource versions;
- asset and implementation card identity/digest semantics;
- linked-run slot schema;
- workspace and operation contract refs;
- validation finding conventions;
- cursor/projection generation behavior.

Phase 4 must stop and request a contract revision if required fit data exists only in prose.

## Outgoing handoff to Phase 5

Include:

- asset-fit request/decision/finding schemas;
- snapshot card, lineage, and compatibility schemas;
- composition template/plan schemas;
- linked-run result-admission rules;
- exact resource URI additions;
- pagination semantics;
- environment observation and stale-data rules;
- test fixtures for selectable, incompatible, unavailable, forbidden, and candidate-only cases.

Phase 5 uses these identities to link result summaries to bindings, admitted child results, and
workspace/artifact provenance. It must not flatten a composition into one result record.

## Explicit non-goals

- Raw snapshot download through search.
- Automatic external asset installation or promotion.
- Consequential composition launch unless separately specified and prepared through governed
  per-run admission.
- Canonical report documents or per-artifact S3 bucket policy.
- Coordinator sandbox packaging.
