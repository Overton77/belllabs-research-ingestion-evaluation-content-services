# Coordinator Phases 2–3 — Workflow Type catalog (supplementary)

Date: 2026-07-29  
Status: Implementation companion to `COORDINATOR_PHASES_2_3_SPEC.md`  
Audience: agents implementing discovery, launch-contract, and validation surfaces

## Purpose

This file answers three questions the Phase 2–3 spec assumes but does not spell out:

1. **Where** are Workflow Types, implementations, and their contracts defined in code?
2. **How** does a caller go from “I want workflow X” to an admitted run?
3. **What** product Workflow Types exist today, with what alternatives and composition?

Contracts are currently **large and string-referenced** (`input_admission_contract`, `invariants`, `obligations`, `output_contracts` are opaque logical ids, not JSON Schema). Phase 2–3 adds schema-bearing exact contract records; this catalog documents **today's authority** faithfully so discovery can project it without guessing.

---

## 1. Acceptance path (specific vs general)

There are **two HTTP surfaces** and **one coordinator path**. All admitted runs end at the same gate.

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ CONTROL PLANE  /control-plane/v1                                        │
│   publish definitions → compile(CompileInvocation) → ERC (digest)         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ effective_configuration_digest
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ RUN CONTROL  /run-control/v1                                            │
│   POST /run-requests  body: RunRequest  →  admit  →  RunProjection      │
└─────────────────────────────────────────────────────────────────────────┘

Coordinator (MCP / facade) does NOT bypass this:

  WorkflowLaunchProposal
    ├─ compilation: CompileInvocation     ──► compile ──► ERC
    ├─ admission: RunAdmissionSpec        ──► frozen into RunRequest
    └─ selected_asset_refs, policy/env digests
         │
         ▼
  SemanticBindingPlan (workflow-specific payload, post-compile)
         │
         ▼
  PreparedLaunchTicket → admit(RunRequest) → launch Temporal
```

### What “general” vs “specific” means

| Mode | What the caller supplies | What the compiler resolves |
|------|--------------------------|---------------------------|
| **General (default)** | `workflow_type` only (+ manifest, authority, environment, context) | `{workflow_type.logical_id}.implementation` alias **`default`** |
| **Specific implementation** | `workflow_type` + `implementation` (exact or alias) | Blueprint, control/runtime/workspace/evaluation profiles, optional `workflow_configuration` from the binding |
| **Legacy component pick** | `workflow_type` + all five component selectors (+ optional `workflow_configuration`) | No implementation binding; caller names blueprint and every profile explicitly |

Rules live in `CompileInvocation.select_one_compilation_mode` (`app/domain/control_plane/contracts.py`) and `ControlPlaneService.compile` (`app/application/control_plane.py`).

**Key insight:** `RunRequest` does **not** carry arbitrary workflow JSON. It carries:

- `effective_configuration_digest` — binds the compiled ERC
- `workflow_type_ref` — exact Workflow Type identity (must match ERC)
- `input_manifest` — `RunInputManifestRef` (content-addressed admitted-input pointer)
- `budget_envelope`, `admission_evidence_refs`, sponsorship/approval/delegation

Semantic run input (goal text, schema refs, query intents, etc.) is authored **after compile** via workflow-specific `SemanticBindingProvider` implementations and stored as `RunSemanticInputBinding`. See `app/application/coordinator_semantic_bindings.py`.

### Run-control admission chain

`POST /run-control/v1/run-requests` (`app/api/run_control.py`) → `RunControlService.admit` (`app/application/run_control.py`):

1. Authorize actor, scope, sponsorship, approvals
2. `F1RunConfigurationVerifier.verify` — load ERC by digest; extract workflow ref, manifest, admission contract, invariants, obligations, budget ceilings
3. Bind request fields to verified configuration (digest, workflow type, manifest must match)
4. Validate budget envelope against ERC authority
5. `AdmissionPolicyRegistry.validate` — **deny-by-default**; every `input_admission_contract` and `invariant` string must have a registered Python validator

Registered today (in `run_control.py` lifespan):

- `register_schema_grounding_admission_policies`
- `register_web_research_admission_policies`

No validator registration ⇒ admission rejected even if ERC compiles.

---

## 2. Definition composition (what a Workflow Type is made of)

```text
WorkflowTypeDefinition
  ├─ input_admission_contract          (opaque string → executable validator)
  ├─ invariants[]                      (opaque strings → executable validators)
  ├─ obligations[]                     (semantic requirements; realized by implementation)
  ├─ output_contracts[]                (opaque schema ids)
  ├─ allowed_*                         (frozensets of ExactDefinitionRef per family)
  ├─ authority_ceiling, workspace_contract
  └─ linked_run_slots[]                (composition: child Workflow Types)

WorkflowImplementationBindingDefinition   (one approved way to run the type)
  ├─ workflow_type_ref
  ├─ blueprint_ref                       StageGraph | GoalDirected
  ├─ control_profile_ref
  ├─ runtime_profile_ref
  ├─ workspace_template_ref
  ├─ evaluation_profile_ref
  ├─ workflow_configuration_ref?         (optional pinned extensions)
  ├─ obligation_realizations[]
  └─ output_contract_realizations[]

CompileInvocation + overlay → EffectiveRunConfiguration (immutable, digest-addressed)
```

Blueprint families (`app/domain/control_plane/contracts.py`):

| Family | Class | Temporal workflow name |
|--------|-------|------------------------|
| `StageGraph` | `StageGraphBlueprint` | `belllabs.stagegraph` |
| `GoalDirected` | `GoalDirectedBlueprint` | `belllabs.goal-directed` |

Generic **contract fixtures** (not product Workflow Types): `app/domain/control_plane/fixtures.py` (`fixture.generic-stage-graph`, `fixture.generic-goal-directed`).

---

## 3. Product Workflow Type inventory

Three Workflow Types are fully defined for production/coordinator paths. All revision-1 unless catalog promotion has advanced revisions.

### 3.1 `schema-context-selection`

| Field | Value |
|-------|-------|
| **Authority source** | `app/domain/schema_grounding/definitions.py` → `schema_grounding_definitions()` |
| **Blueprint family** | StageGraph only |
| **Default implementation alias** | `schema-context-selection.implementation` → **`default`** |
| **Implementations** | 1 (staged selector → validator → reviewer → accept) |
| **Composition** | Standalone; **child of** `supporting-graph-reconciliation` linked slot `schema_context_selection` |
| **Semantic binding** | `app/application/schema_context_stage_handlers.py` (routed via `coordinator_semantic_bindings.py`) |
| **Admission module** | `app/application/schema_grounding_admission.py` |

**Contracts (opaque today):**

| Role | Ref |
|------|-----|
| Admission | `admission:schema-context-selection:v1` |
| Invariants | `invariant:schema-selection-independent-review:v1`, `invariant:schema-selection-exact-lineage:v1` |
| Obligations | `obligation:semantic-selection:v1`, `obligation:structural-validation:v1`, `obligation:independent-review:v1` |
| Output | `schema:accepted-schema-context-selection:v1` |

**Pinned configuration:** `schema-context-selection-official-v1` — extension `belllabs.schema-grounding/operation-contracts` pins operation contract refs, overlay, catalog generator.

**Blueprint:** `schema-context-selection-v1` — stages: `materialize_selection_context` → `semantic_selector` → `structural_validation` → `independent_reviewer` → `accept_selection`.

**Required admission evidence prefixes** (`schema_grounding_admission.py`):

`schema-definition:`, `schema-catalog-build:`, `semantic-overlay:`, `sensitive-data-policy:`

---

### 3.2 `supporting-graph-reconciliation`

| Field | Value |
|-------|-------|
| **Authority source** | `app/domain/schema_grounding/definitions.py` |
| **Blueprint families** | **StageGraph** (default) and **GoalDirected** (alternative) |
| **Implementation aliases** | `supporting-graph-reconciliation.implementation` → **`default`** (StageGraph), **`goal-directed`** (GoalDirected) |
| **Implementations** | 2 bindings, **same** `logical_id`, different revisions/blueprints |
| **Composition** | Parent workflow; linked slot admits child `schema-context-selection` |
| **Semantic binding** | `app/application/schema_grounding_semantic_handlers.py` |
| **Admission module** | `app/application/schema_grounding_admission.py` |

**Contracts:**

| Role | Ref |
|------|-----|
| Admission | `admission:supporting-graph-reconciliation:v1` |
| Invariants | `invariant:exact-schema-deployment-compatibility:v1`, `invariant:independent-graph-capability:v1`, `invariant:no-arbitrary-cypher:v1`, `invariant:observational-no-graph-mutation:v1` |
| Obligations | `obligation:schema-context-derived:v1`, `obligation:graph-gate-admitted:v1`, `obligation:bounded-query-evidence:v1` |
| Output | `schema:supporting-graph-reconciliation-record:v1` |
| Linked-result policy | `linked-result:schema-selection-exact-purpose:v1` (child slot) |

**Pinned configuration:** `supporting-graph-reconciliation-official-v1`.

**Blueprints:**

| logical_id | family | Notes |
|------------|--------|-------|
| `supporting-graph-reconciliation-v1` | StageGraph | Linear 9-stage pipeline (admission → derive → gate → execute → promote) |
| `supporting-graph-reconciliation-goal-directed-v1` | GoalDirected | `max_iterations=12`, variant `required-seed-intents` |

**Required admission evidence prefixes:**

`schema-definition:`, `schema-catalog-build:`, `schema-deployment-manifest:`, `schema-workspace-binding:`, `graph-capability:`, `sensitive-data-policy:`

**Choosing the alternative:** compile with `implementation` selector alias `goal-directed` (see `tests/test_schema_grounding_control_plane.py`). Default compile uses StageGraph.

---

### 3.3 `web-research-browser-verification`

| Field | Value |
|-------|-------|
| **Authority source** | `app/domain/coordinator/web_capability_fixtures.py` → `web_capability_definitions()` |
| **Blueprint family** | StageGraph only |
| **Default implementation alias** | `web-research-browser-verification.implementation` → **`default`** |
| **Implementations** | 1 |
| **Composition** | Standalone (no linked-run slots) |
| **Semantic binding** | `app/application/web_research_semantic_binding.py` |
| **Admission module** | `app/application/web_research_admission.py` |
| **Live harness** | `app/application/web_research_coordinator_live.py` |

**Contracts:**

| Role | Ref |
|------|-----|
| Admission | `admission:web-research-public-goal:v1` |
| Invariants | `invariant:two-provider-identity-preserved:v1`, `invariant:untrusted-web-content-is-not-instruction:v1`, `invariant:search-tools-only:v1`, `invariant:browser-authority-explicit:v1` |
| Obligations | `obligation:firecrawl-search-evidence:v1`, `obligation:tavily-search-evidence:v1`, `obligation:browser-verification:v1`, `obligation:cited-synthesis:v1` |
| Output | `schema:web-research-browser-verification-result:v1` |

**Blueprint:** `web-research-browser-verification-v1` — parallel search branches, synthesis, browser verify, promote.

**Required admission evidence prefixes** (each invariant may require additional refs):

`public-goal:`, `capability-selection:`, `catalog://mcp_server/mcp.firecrawl/`, `catalog://mcp_server/mcp.tavily/`, `tool-allowlist:firecrawl_search:`, `tool-allowlist:tavily_search:`, `catalog://skill/skill.agent-browser/`, `browser-authority:`, `policy:untrusted-web-content-is-data:`

**Selected assets** (enforced in semantic binding): Firecrawl + Tavily MCP servers/tools, search skills, `skill.agent-browser`, `agent-profile.web-research-browser-verification`.

---

## 4. File locator map (read this before adding discovery code)

| Concern | Path |
|---------|------|
| Workflow Type / implementation / blueprint contracts | `app/domain/control_plane/contracts.py` |
| ERC compiler (pure) | `app/domain/control_plane/compiler.py` |
| Schema-grounding Workflow Types + implementations | `app/domain/schema_grounding/definitions.py` |
| Web-research Workflow Type + capability fixtures | `app/domain/coordinator/web_capability_fixtures.py` |
| Generic blueprint fixtures (tests only) | `app/domain/control_plane/fixtures.py` |
| Compile / publish / retrieve ERC | `app/application/control_plane.py`, `app/api/control_plane.py` |
| Run admission + lifecycle | `app/application/run_control.py`, `app/api/run_control.py` |
| Coordinator launch (compile → bind → ticket → admit) | `app/application/coordinator_launch.py`, `app/domain/coordinator/launch.py` |
| Semantic binding router | `app/application/coordinator_semantic_bindings.py` |
| Capability search (internal, pre–Phase 2–3 discovery) | `app/application/capability_search.py`, `app/application/postgres_capability_search_repository.py` |
| MCP resources (stubs / early) | `app/mcp/coordinator_resources.py` |
| Composition / linked runs | `app/domain/composition/contracts.py`, `app/application/linked_runs.py` |
| StageGraph / GoalDirected runtime contracts | `app/domain/orchestration/contracts.py` |
| Operation execution bindings | `app/domain/operation_execution/contracts.py` |
| Domain workflow guide (longer narrative) | `docs/interview_and_research_result_documentation/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md` |

**JSON Schemas exported today:**

- Control plane: `GET /control-plane/v1/schemas`
- Run control: `GET /run-control/v1/schemas`

These export **transport contracts** (`RunRequest`, `CompileInvocation`, `EffectiveRunConfiguration`, etc.), not per–Workflow Type input JSON Schemas.

---

## 5. Coordinator launch shape (what discovery must eventually explain)

`WorkflowLaunchProposal` (`app/domain/coordinator/launch.py`) bundles:

```yaml
request_scope / tenant_scope
compilation:          # CompileInvocation
  workflow_type:      # DefinitionSelector (exact or alias)
  implementation:     # optional; else default alias
  input_manifest:     # RunInputManifestRef — digest often covers semantic input
  caller_authority / environment / context
  overlay:            # optional capability/budget/variant/extensions overlay
admission:            # RunAdmissionSpec → becomes RunRequest fields
  actor, budget_envelope, admission_evidence_refs, sponsorship, approvals, ...
selected_asset_refs:  # exact catalog assets (web research)
policy_snapshot_digest / environment_snapshot_digest
idempotency_issuer / idempotency_key
initial_goal:         # GoalDirected only; optional for StageGraph
```

After prepare: `PreparedLaunchTicket` carries `effective_configuration_digest`, `blueprint_family`, `semantic_binding_plan_digest`, frozen `run_request_digest`.

**Phase 2–3 deliverable:** `get_workflow_launch_contract` must expose the same refs/digests an agent needs to build this proposal without reading live harness code.

---

## 6. Contract size note (for implementers)

Current pain points (do not “fix” in discovery slice; document and project):

| Today | Phase 2–3 target |
|-------|------------------|
| `WorkflowTypeDefinition` embeds full allowed-ref frozensets | Discovery cards: compact summaries + exact refs for rehydration |
| Contract roles are string ids (`admission:…`, `schema:…`) | Exact versioned JSON Schema records + digest |
| `RunInputManifestRef` points at opaque manifest bytes | Launch contract returns admitted-input schema URI |
| Admission validators are hand-written per workflow | Launch validator and admission share same contract revision |
| `WorkflowConfigurationDefinition.extensions` carry domain payloads | Remain extensions; discovery lists namespace/discriminator |

Slice B of `COORDINATOR_PHASES_2_3_SPEC.md` explicitly labels legacy opaque strings; discovery must not present them as JSON Schema.

---

## 7. Quick reference — compilation examples

**Default (general):**

```python
CompileInvocation(
    workflow_type=DefinitionSelector(exact=workflow_type_ref),
    input_manifest=RunInputManifestRef(...),
    caller_authority=...,
    environment=...,
    context=CompilationContext(...),
)
# Resolves implementation alias: {logical_id}.implementation / default
```

**Explicit GoalDirected reconciliation:**

```python
CompileInvocation(
    workflow_type=DefinitionSelector(exact=reconciliation_type_ref),
    implementation=DefinitionSelector(
        alias=AliasRef(
            kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
            logical_id="supporting-graph-reconciliation.implementation",
            alias="goal-directed",
        )
    ),
    input_manifest=...,
    ...
)
```

**Run request (after compile):**

```python
RunRequest(
    effective_configuration_digest=erc.digest,
    workflow_type_ref=workflow_type_ref,  # must match ERC source_refs
    input_manifest=erc.input_manifest,    # must match exactly
    budget_envelope=...,                  # must respect ERC ceilings
    admission_evidence_refs=...,          # workflow-specific prefixes
    ...
)
```

---

## 8. Tests as ground truth

| Scenario | Test file |
|----------|-----------|
| Compile modes + reconciliation aliases | `tests/test_schema_grounding_control_plane.py` |
| Schema grounding admission evidence | `tests/test_schema_grounding_admission.py` (if present) / `schema_grounding_admission.py` |
| Web research admission | `tests/test_web_research_admission.py` |
| Coordinator launch + semantic binding | `tests/test_coordinator_launch_preparation.py`, `tests/test_coordinator_semantic_binding_integration.py` |
| StageGraph / GoalDirected orchestration | `tests/test_stagegraph_orchestration.py`, `tests/test_goal_directed_interpreter.py` |
| Live coordinator harnesses | `tests/test_run_web_research_coordinator_live.py`, schema grounding live scripts |

---

## 9. Relation to Phase 2–3 spec

Use this catalog to implement:

- **Slice C** — Workflow Type cards (family, implementations, aliases, composition, launchability)
- **Slice D** — launch contract (ERC roles, manifest, evidence, output refs)
- **Slice B** — mark which contract strings are legacy opaque vs migrated schema

Do **not** treat search hits or this markdown as executable authority. Rehydrate `ExactDefinitionRef` + digest from Mongo/control plane before any consequential operation.
