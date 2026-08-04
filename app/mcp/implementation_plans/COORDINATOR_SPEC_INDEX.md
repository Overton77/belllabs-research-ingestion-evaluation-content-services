# Coordinator MCP specification index

Date: 2026-07-27  
Status: Working implementation specifications derived from
`COORDINATOR_ARCHITECTURE_AND_PLAN.md`. These documents guide delivery but do not supersede
accepted authority in `biotech-meta`.

## Purpose

This index is the shared starting point for every agent implementing or reviewing the BellLabs
Coordinator MCP. It divides the architecture plan into bounded specifications, defines the
cross-phase handoff protocol, and identifies the application files that currently carry
authority or execution mechanics.

Read this file before opening a phase specification. Then read only the source and application
areas referenced by that phase.

## Authority and supporting references

Use this precedence order when documents disagree:

1. accepted specifications and vocabulary in `biotech-meta`;
2. domain and application contracts in this repository;
3. these phase implementation specifications;
4. tests, examples, local skills, and experiment/live harnesses.

Primary references:

- `../../../biotech-meta/docs/workflow-catalog-configuration-composition-and-agent-entry.md`
- `../../docs/workflow-control-plane-current-state-and-next-slices.md`
- `../../../biotech-meta/docs/checkpoints/schema_schema_workspaces_efficient_db_interaction/2026-07-16-large-schema-workspaces-selection-and-report-splitting-special-checkpoint.md`
- `../../docs/SCHEMA_S3_REFERENCE.md` for existing S3 behavior; it does not establish the missing
  canonical storage policy identified by Phase 5.

## Specification set and order

1. `COORDINATOR_PHASE_1_SPEC.md` — truthful, production-complete lifecycle.
   Implementation handoff: `COORDINATOR_PHASE_1_HANDOFF.md`.
2. `COORDINATOR_PHASES_2_3_SPEC.md` — decision-oriented discovery and schema-bearing contracts.
   Workflow inventory companion: `COORDINATOR_PHASES_2_3_WORKFLOW_CATALOG.md`.
3. `COORDINATOR_PHASE_4_SPEC.md` — agentic asset fit, snapshots, and linked-run composition.
4. `COORDINATOR_PHASE_5_SPEC.md` — progressive result navigation using current artifact and
   reporting reality.
5. `COORDINATOR_PHASE_6_SPEC.md` — reviewed coordinator sandbox package and workspace bundles.
6. `COORDINATOR_VIOME_STAGEGRAPH_ACCEPTANCE_SPEC.md` — official StageGraph acceptance flow for
   the Viome flagship-product mission.

The Viome specification has two execution gates:

- **Core tracer gate:** begins as part of Phase 1 and proves the real launch/result path.
- **Full product gate:** runs after Phase 6 and exercises all capabilities that have actually
  shipped. Missing optional infrastructure must be reported, never simulated.

## Shared architecture rules

1. MCP is a protocol adapter. It parses, authenticates, delegates, envelopes, and applies
   protocol limits; it does not own compilation, policy, admission, persistence, or Temporal
   semantics.
2. The coordinator agent proposes. Application services resolve exact definitions, validate,
   authorize, prepare, admit, launch, and persist.
3. Domain authority belongs in `app/domain/`; application use cases and ports belong in
   `app/application/`; vendor and persistence adapters belong in `app/integrations/`; durable
   workflow mechanics belong in `app/temporal/`.
4. Search cards and projections are selection evidence, not executable authority. Consequential
   operations rehydrate immutable exact references and verify their digests.
5. Existing Workflow Type launch and novel Workflow Type authoring are different paths.
   Validation of a draft does not publish it or make it executable.
6. A StageGraph is a multi-stage implementation of one Workflow Run. Cross-Workflow-Type work is
   composition of separately admitted linked runs; never merge their blueprints.
7. Every advertised tool, prompt, resource, and URI must have a composed provider and be usable
   by the authorized principal. Otherwise omit it or advertise an explicit unavailable state.
8. Terminality and immutable typed result persistence must have a defined, idempotent ordering
   and recovery rule.
9. Credentials, PHI, mutable authority, and tenant data must not enter coordinator base snapshots
   or documentation fixtures.
10. Research output is evidence-backed research, not medical advice or proof of scientific
    validity.

## Shared agent operating procedure

Every implementation agent must:

1. Read this index, its assigned specification, and the source architecture plan.
2. Confirm the preceding phase handoff is accepted before depending on its outputs.
3. Inspect current code before adding packages or contracts; extend existing packages where
   ownership already exists.
4. Check `biotech-meta` before inventing durable vocabulary or caller-facing authority.
5. Preserve tenant scope, request scope, correlation identity, idempotency identity, exact
   references, and digests across protocol and application boundaries.
6. Add tests at the lowest authoritative layer and at the MCP boundary for changed behavior.
7. Record deviations and unavailable dependencies in the outgoing handoff.
8. Never mark a phase complete using mocks alone when its exit condition requires PostgreSQL,
   MongoDB, Temporal, S3, or another real adapter.

## Cross-phase handoff contract

Each phase produces a handoff record with these sections:

```yaml
phase: "phase-1"
revision: "<git revision or working-tree identifier>"
status: "accepted | conditional | blocked"
completed_scope: []
deferred_scope: []
contract_changes: []
schema_or_migration_changes: []
new_tools_resources_prompts: []
feature_flags_and_defaults: {}
provider_readiness: {}
test_evidence: []
known_gaps: []
security_and_data_notes: []
next_phase_prerequisites: []
rollback_or_disable_path: []
```

Handoff rules:

- `accepted` means every mandatory exit criterion has evidence.
- `conditional` is allowed only for explicitly optional capabilities. The next phase must not
  treat conditional outputs as authority.
- `blocked` prevents dependent work from being integrated.
- Deferred work remains assigned to its originating phase unless the receiving specification
  explicitly owns it.
- Contract changes include version, compatibility behavior, migration/read strategy, and exact
  resource URI changes.
- Test evidence names the test, mode, dependencies, and observed result; “tests added” is not
  sufficient.
- The receiving agent rechecks provider availability and contract versions. A handoff is not a
  permanent runtime readiness assertion.

## Phase dependency graph

```text
Phase 1: truthful lifecycle
  |-- core Viome tracer may run here
  v
Phases 2-3: discovery + exact schemas
  v
Phase 4: fit + snapshots + composition
  v
Phase 5: progressive result exploration
  v
Phase 6: sandbox + workspace bundle
  v
Full Viome StageGraph acceptance
```

Phase 2 and Phase 3 are one delivery plan because their public value is coupled: launch contracts
must not claim to be machine-usable until the underlying contracts are exact and schema-bearing.
They may be implemented in internal slices, but they share one external acceptance gate.

## Application file map

### Coordinator protocol surface

- `app/mcp/coordinator_server.py` — current MCP tools and envelope/error boundary.
- `app/mcp/coordinator_resources.py` — registered exact-definition and run resources.
- `app/mcp/coordinator_prompts.py` — prompt registration and binding behavior.
- `app/mcp/coordinator_bootstrap.py` — bootstrap serialization plus existing HTTP deployment and
  mount helpers; application startup still needs to compose and use them truthfully.
- `app/mcp/coordinator_auth.py` — MCP principal resolution boundary.
- `app/mcp/__main__.py` — standalone development composition; currently a known source of
  capability/provider drift.

### Coordinator application boundary

- `app/application/coordinator_facade.py` — shared coordinator authorization, limits, DTOs,
  auditing, and delegation.
- `app/application/coordinator_composition.py` — production facade/provider composition.
- `app/application/coordinator_launch.py` — preparation tickets and consequential launch.
- `app/application/coordinator_results.py` — current run projection plus typed-result join.
- `app/application/capability_search.py` — generic internal capability search.
- `app/application/catalog_projection*.py` — projection generation and metadata.
- `app/application/external_capability_discovery.py` — candidate-only discovery.
- `app/application/external_candidate_inspection.py` — inspection before promotion.

### Domain authority

- `app/domain/coordinator/contracts.py` — search, authorization, draft, and error contracts.
- `app/domain/coordinator/launch.py` — launch proposal/ticket/handle and typed workflow result.
- `app/domain/control_plane/contracts.py` — definitions, Workflow Types, implementations,
  StageGraph blueprints, workspaces, and exact references.
- `app/domain/control_plane/compiler.py` — Effective Run Configuration compilation.
- `app/domain/run_control/contracts.py` and `app/domain/run_control/reducer.py` — run lifecycle,
  commands, outcomes, and invariants.
- `app/domain/composition/contracts.py` — linked-run composition contracts.
- `app/domain/orchestration/contracts.py` — StageGraph and GoalDirected runtime contracts.
- `app/domain/operation_execution/contracts.py` — exact operation execution bindings.

### Launch and execution mechanics

- `app/application/run_control.py` — run admission and lifecycle service.
- `app/application/orchestration.py` — launch dispatcher and family-specific launch services.
- `app/application/orchestration_binding_repository.py` — semantic run-input bindings.
- `app/application/operation_execution.py` — operation execution use cases.
- `app/integrations/temporal_workflow_submission.py` — Temporal submission adapter.
- `app/temporal/stagegraph_workflow.py` — StageGraph durable workflow.
- `app/temporal/orchestration_activities.py` — StageGraph activities.
- `app/temporal/coordinator_runtime.py` — routed coordinator workers and readiness.
- `app/temporal/worker.py` — deployed worker registration/composition.

### Persistence, artifacts, workspaces, and snapshots

- `app/application/postgres_launch_ticket_repository.py` — durable prepared tickets.
- `app/application/postgres_workflow_result_repository.py` — immutable typed result records.
- `app/application/postgres_run_control_repository.py` — run-control persistence/projections.
- `app/application/sandbox_snapshots.py` — snapshot create/restore and compatibility checks.
- `app/application/workspace_materialization.py` — governed workspace materialization.
- `app/application/artifact_promotion.py` — artifact promotion lifecycle.
- `app/integrations/artifact_payloads.py` — current S3 artifact payload adapter.
- `app/integrations/control_plane_payloads.py` — content-addressed control-plane payload
  mechanics.
- `app/integrations/snapshot_payloads.py` and `app/integrations/s3.py` — existing snapshot and S3
  mechanics that remain adapters rather than canonical storage policy.
- `app/config.py` — typed environment contract, including the current single optional
  `s3_bucket`; this is not yet a canonical per-artifact storage policy.

### Existing coordinator skill assets

- `.agents/skills/belllabs-workflow-coordinator/SKILL.md` — partial coordinator guidance already
  exists and should be reviewed/evolved rather than recreated blindly.
- `.agents/skills/belllabs-workflow-coordinator/schemas/` — advisory local workflow-design and
  launch-proposal schemas; they are not authoritative Phase 3 catalog contracts.
- `.agents/skills/belllabs-workflow-coordinator/scripts/` — current local validators; Phase 6
  must version, harden, and add the remaining deterministic helpers.
- `.agents/skills/belllabs-workflow-coordinator/references/` — protocol, authority, selection, and
  examples that require reconciliation with the accepted phase contracts.

### Existing web-research tracer assets

- `app/domain/coordinator/web_capability_fixtures.py` — reviewed deterministic web capability
  definitions used by bootstrap/tests.
- `app/application/web_research_coordinator_live.py` — existing live coordinator composition
  and launch harness; useful evidence, not the final MCP acceptance harness.
- `app/application/web_research_semantic_binding.py` — exact web-research binding plan.
- `app/application/web_research_semantic_handlers.py` — StageGraph operation handlers.
- `app/integrations/web_research_runtime.py` — real web retrieval/browser adapters.
- `app/temporal/web_research_smoke.py` — web-research StageGraph worker composition.
- `scripts/run_web_research_coordinator_live.py` — operator script for the current live path.

### High-value existing tests

- `tests/test_coordinator_mcp_read_surface.py`
- `tests/test_coordinator_facade.py`
- `tests/test_coordinator_launch_preparation.py`
- `tests/test_coordinator_launch_idempotency.py`
- `tests/test_coordinator_temporal_runtime.py`
- `tests/test_coordinator_semantic_binding_integration.py`
- `tests/test_coordinator_surface_promotion.py`
- `tests/test_stagegraph_orchestration.py`
- `tests/test_run_web_research_coordinator_live.py`

## Documentation authority and update rule

The source architecture interview remains `COORDINATOR_ARCHITECTURE_AND_PLAN.md`. If
implementation evidence contradicts that document, record the discrepancy in the phase handoff
and update the relevant specification. Do not silently “fix” accepted product vocabulary in
these plans.

Durable accepted specifications belong in `biotech-meta`. These files remain implementation
plans colocated with the MCP package until reviewed and promoted.
