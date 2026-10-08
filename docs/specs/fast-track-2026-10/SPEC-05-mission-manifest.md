---
type: Specification
title: "SPEC-05: Mission Manifest v1 — the YAML authoring surface that compiles into the Mission Definition"
description: "Full specification of mc.mission_manifest.v1: file shape, goals and criteria, the Environment bundle and its per-node inheritance, capability search-or-pin entries, agents, hooks and plugins blocks, the program and its behaviors, input bindings with expansion tiers, controls; the compile algorithm that resolves searches to Capability Pins and validates lane support; the mapping onto the existing compiler; CLI, HTTP and MCP verbs compile, submit and start; provenance and error codes; tickets E1 to E3."
tags: [mission-control, spec, fast-track, authoring]
---

# SPEC-05: Mission Manifest v1

Decisions: ADR-0022 (the manifest is an authoring surface that compiles into the typed Mission Definition), ADR-0034 (v1 settled), ADR-0023 (capability kinds and host support), ADR-0029 (chains), ADR-0027 (Context Packet). Companion: [SPEC-04 Mission Chains](SPEC-04-mission-chains.md), [SPEC-01 Capabilities](SPEC-01-capabilities-catalog.md), [SPEC-02 Context Packet](SPEC-02-context-packet.md). Codebase facts: [research/codebase-map.md](research/codebase-map.md) section 8 and gap (f).

## Problem Statement

The owner authors missions by describing computational and agent environments at the granularity people think in: which lane, which model, which sandbox, which skills and servers each piece of work gets, with mission-wide defaults and per-node exceptions, and sometimes several workflows chained together. Today nothing accepts that description. The only authoring paths are the coordinator's `WorkflowDesignDraft` (a structural draft that is never launchable) and a `CompileInvocation` of exact catalog selectors that a human cannot reasonably write. There is no `MissionDefinition` class, no YAML loader, no search-to-pin resolution, and no CLI verb that turns a file into an admitted run. The three owner missions therefore cannot be written down, reviewed, versioned or started.

## Solution

A `mission.yml` file, the Mission Manifest, is the one authoring surface for humans and coordinators. It is compiled by a deterministic service into the typed `MissionDefinition@1`, a Compiled Program and a Validation Report, with every Capability Search resolved to an exact Capability Pin and every inheritance decision recorded in a `mc.manifest_resolution.v1` document. The committed Revision is the typed definition, never the YAML; the YAML and its resolution are stored beside the Revision as authoring provenance. Three verbs cover the lifecycle: `compile` (validate and report, nothing persisted), `submit` (commit the Revision and admit the Run or Mission Chain without starting), `start` (the separate authorized call). The same handlers serve `missionctl`, HTTP and the coordinator MCP server.

## User Stories

1. As an owner, I want to write a mission as a YAML file with goals, criteria and a program, so that the mission is reviewable and versionable before any agent runs.
2. As an owner, I want a mission-level `environment` that every node inherits, so that I state the lane, model, sandbox, skills and capabilities once.
3. As an owner, I want a node to overlay the environment (a cheaper model, a smaller budget, an extra skill), so that exceptions are local and visible.
4. As an owner, I want the compiler to reject a node that widens budget, governors or side-effect allowances beyond the mission, so that overlays can only narrow.
5. As a coordinator agent, I want to name capabilities by search (`search: "pubmed literature retrieval"`) and let compile resolve them to exact pins, so that I need not memorize catalog identifiers.
6. As a coordinator agent, I want a search with zero admitted hits to be a typed blocker in the Validation Report, so that no mission silently runs without a capability it asked for.
7. As an owner, I want to pin a capability exactly (`pin: mcp.pubmed@2.10.20#sha256:…`) when I know it, so that a search result cannot drift under me.
8. As an owner, I want to declare subagent profiles, hook scripts and plugins in the manifest, so that an agent's delegates, lifecycle guards and bundles are part of the mission, not of a host's local configuration.
9. As an owner, I want the compiler to tell me when a hook event or a subagent field is unsupported on the lane I chose, so that I learn it at compile time rather than from a silent no-op.
10. As an owner, I want to declare input bindings between nodes with an expansion tier, so that I decide whether a producer's output is inlined into the consumer's prompt, referenced, or materialized as a file.
11. As an owner, I want to declare several missions and links in one file, so that a research mission can supply an ingestion mission as a Mission Chain.
12. As an owner, I want `missionctl mission compile mission.yml` to print the Validation Report without persisting anything, so that I iterate quickly.
13. As an owner, I want `missionctl mission submit mission.yml` to commit the Revision and admit the Run or chain without starting it, so that start remains a separate deliberate action.
14. As an owner, I want `missionctl mission start RUN_ID` to be the only thing that launches agents, so that commit never implies spend.
15. As a coordinator agent, I want MCP tools `mission_manifest_compile`, `mission_manifest_submit` and `mission_run_start` with the same semantics, so that a Claude Code or Cursor host can author and launch missions.
16. As an operator, I want the manifest and its resolution stored with the Revision, so that I can later see which search produced which pin.
17. As an owner, I want a JSON Schema for the manifest exported next to the other contracts, so that editors validate the file as I type.
18. As an owner, I want the compiler to validate lane support per behavior, so that a Parallel Swarm on a Cursor lane fails compile rather than at run time.
19. As an owner, I want goal and criterion coverage checked (every goal has a criterion, every required criterion is served by a node), so that unreachable goals are caught before admission.
20. As an owner, I want budgets and governors validated for feasibility (node budgets sum within the mission budget, iteration caps present on every Goal Loop), so that governed outcomes are decided at compile time.
21. As an owner, I want the manifest to accept aliases like `@latest` only in searches and pins at authoring time, so that the committed Revision carries exact digests only.
22. As an owner, I want a `controls` block that names the commands the mission admits and the subscriptions to register at start, so that intervention and callbacks are configured where the mission is.
23. As an owner, I want to declare the application in the file but have compile resolve it against my authenticated scope, so that a file cannot grant itself another application.
24. As a reviewer, I want compile errors to carry JSON pointers into the YAML, so that I find the offending line.
25. As an owner, I want the three owner missions shipped as example manifests that compile, so that they are the acceptance fixtures for this feature.
26. As an owner, I want `expand: auto` on a binding to let the Context Packer decide by budget, so that I only choose a tier when I care.
27. As an owner, I want human gates declared inline as nodes (`behavior: human_gate`), so that reviews are explicit program nodes with dependencies.
28. As an owner, I want each Goal Loop node to declare its action space from the resolved capabilities and executors, so that the loop controller can choose only admitted actions.
29. As a coordinator agent, I want compile to return the resolved environment per node, so that I can show the human what each node will actually run with.
30. As an owner, I want the `mission.yml` of a chain to compile each mission independently and the links as a chain, so that one broken mission does not hide errors in the other.

## Contracts

### `mc.mission_manifest.v1` (file shape)

The file is YAML 1.2, parsed with `yaml.safe_load`, keys `lower_snake`, unknown keys rejected (`extra = "forbid"`). The top level is either `mission:` (one mission) or `missions:` plus `links:` (a Mission Chain). Both forms share `manifest: mission/v1`.

```yaml
manifest: mission/v1              # required literal
mission:                          # OR missions: [...] with links: [...]
  key: <slug>                     # required; stable within the file; becomes the mission title key
  title: <string>                 # required
  application: biotech | ai-engineer   # required; must equal the authenticated scope's application
  domain_pack: <ref>              # optional; app-owned templates, rubrics, prompts
  description: <string>           # optional
  goals: [Goal...]                # required, 1..n
  environment: Environment        # required at mission level
  program: ProgramNode            # required; the root node
  controls: Controls              # optional
```

#### Goal, Success Criterion

```yaml
goals:
  - key: evidence_map             # required slug, unique in the mission
    description: <string>         # required
    importance: primary | secondary | optional    # default primary
    objectives:                   # optional; nested steps
      - key: <slug>
        description: <string>
        parent: <objective key>   # optional
    criteria:                     # required, 1..n
      - key: coverage
        description: <string>
        evidence: [<output name or artifact schema>]      # required
        acceptance:               # exactly one of
          assessment: { capability: <search|pin entry>, threshold: 0.8 }
          human: review_accept | approved
          schema: <schema ref>    # deterministic schema check
          all: [acceptance...]    # closed AST (ADR-0010)
          any: [acceptance...]
```

`acceptance` is the authoring form of the Completion Contract's closed typed AST; the compiler emits the typed expression and rejects any string that looks like code or a natural-language predicate.

#### Environment

```yaml
environment:
  lane: deep_agents | cursor_local | cursor_cloud          # claude_agent_sdk | codex reserved, rejected in v1
  model: { profile: <model profile id or search entry>, settings: {...} }
  sandbox: { profile: <sandbox profile id>, egress: [<allowlist names>] }
  workspace:
    repo: { url: <git url>, ref: <ref>, path: <local path> }   # cursor lanes; path for cursor_local
    context: [ <path or search entry> ]                         # read-only files from catalog context bundles or artifacts
    skills: [ <pin or search entry> ]                           # skill_bundle capabilities
  capabilities: [ CapabilityEntry... ]                          # mcp_server, mcp_tool, deterministic_executor, assessment, ...
  agents: [ AgentEntry... ]                                     # subagent_profile capabilities
  hooks: [ HookEntry... ]                                       # hook_script capabilities
  plugins: [ <pin or search entry> ]                            # plugin capabilities; members expand at compile
  budget: { usd: <decimal>, tokens: <int>, wall_clock: <duration>, tool_calls: <int> }
  governors: { depth: <int>, fan_out: <int>, iterations: <int>, rounds: <int>, patience: <int>, transfers: <int> }
  side_effects: [read_only, workspace_write, external_write_reversible, external_write_irreversible, spend]
```

`CapabilityEntry` is exactly one of:

```yaml
- { pin: "<capability_id>@<version>#sha256:<64 hex>", as: <alias> }
- { search: "<query>", kind: <capability kind>, as: <alias>, require: [<lane profile>...], tools: [<tool names>] }
```

`as` names the alias other blocks use (action spaces, bindings). `kind` is a catalog Capability Kind (ADR-0020, ADR-0023): `mcp_server`, `mcp_tool`, `skill_bundle`, `hook_script`, `subagent_profile`, `plugin`, `deterministic_executor`, `assessment`, `model_profile`, `sandbox_profile`, `context_bundle`. `require` defaults to the node's lane; the search filters on `host_support`. `tools` narrows an `mcp_server` to an explicit tool allowlist (selecting a server never selects every sibling tool).

`AgentEntry`:

```yaml
- { pin | search, as: verifier, overlay: { readonly: true, is_background: false, model: inherit } }
```

`HookEntry`:

```yaml
- { pin | search, as: citation_check, events: [after_tool, stop], matcher: "<tool name glob>", fail_closed: true }
```

`events` are `mc.hook_event` values (ADR-0026). Kernel hooks are never declared; they are always present.

#### Inheritance

A node's `environment` is a partial `Environment`. The effective environment of a node is `merge(parent_effective, node_overlay)` where:

- mappings merge key by key, recursively;
- lists replace the inherited list entirely (`skills: []` removes all skills; to add one, restate the list);
- scalars replace;
- `lane` may change per node (a Stage Graph on `deep_agents` may hold a `cursor_local` stage); each lane change is recorded in the resolution;
- **narrowing only**: `budget.*` ≤ parent, `governors.*` ≤ parent, `side_effects` ⊆ parent, `capabilities` after plugin expansion need not be a subset (a node may add a capability) but any added capability's side-effect class must be within `side_effects`. Violations are `INVALID_DEFINITION` with pointer and reason `widens_authority`.

The resolution document records, per node, the effective environment and the provenance of each field (`inherited | overlay | plugin:<alias>`).

#### Program Node

```yaml
program:
  key: root
  behavior: stage_graph | goal_loop | parallel_swarm | evaluator_optimizer
          | agent_executor | deterministic_executor
          | event_wait | timer | human_gate | proof_gate | child_mission_invocation
  objectives: [<goal key or objective key>]       # required for executors and workflow systems
  environment: Environment                        # optional overlay
  inputs: [ InputBinding... ]
  outputs: [ { name: <slug>, schema: <schema ref>, required: true } ]
  completion: { acceptance: <acceptance AST> }    # optional; defaults from criteria the node serves
  <behavior body>                                 # discriminated by behavior
```

Behavior bodies (v1):

| Behavior | Body |
| --- | --- |
| `stage_graph` | `nodes: [ProgramNode...]`, each with `depends_on: [keys]` (default expression `all`; `any` and `quorum: n` allowed), `fail_fast: bool`, `concurrency: int`; `revisit` regions deferred |
| `goal_loop` | `objective: <goal or objective key>`, `action_space: [<alias>...]` (capability aliases and nested node keys), `verifier: { independent: true, environment: ... }`, `stop: <acceptance AST>` optional; governors `iterations` and `patience` required (inherited or overlay) |
| `agent_executor` | `instruction: <string or context path>`, `operating_contract: <ref>` optional, `missing_output_policy: follow_up_turn{max_turns} | not_accepted` |
| `deterministic_executor` | `kind: verification_dispatch | artifact_register | git_snapshot | schema_validate | test_run | coalesce | agreement_check | noop_echo`, `params: {...}` typed per kind |
| `human_gate` | `task: { kind: APPROVAL|QUESTION|SELECTION|REVIEW|POLICY_OVERRIDE, prompt, reviewers: [...], packet: [<node.output>...], timeout, on_timeout }` |
| `event_wait` | `event_type`, `match: {...}`, `timeout`, `on_timeout`, `mode: once | rearm{...}` |
| `timer` | `duration` or `until` |
| `proof_gate` | `evidence: <node.output>`, `required_disposition`, `on_reject` |
| `child_mission_invocation` | `mode: spawn | attach`, `child: { inline: <mission block> | template: <ref> | existing: <mission id> }`, `await: {until}`, `on_parent_cancel`, `portal: peek | peek_and_command` |
| `parallel_swarm`, `evaluator_optimizer` | accepted by the schema; compile rejects them on any lane but `deep_agents` with `UNSUPPORTED_BEHAVIOR` |

#### Input binding

```yaml
inputs:
  - { name: sources, from: collect.sources, expand: inline | reference | materialize | auto, required: true }
  - { name: brief, artifact: "artifact://...", expand: materialize }
  - { name: question, value: "NAD+ precursors and skeletal muscle" }        # literal admitted input
```

`from` is `<node key>.<output name>` within the same mission, or `<mission key>.<output name>` for a chain link (SPEC-04). `expand` is the Expansion Tier the Context Packer applies (ADR-0027); `auto` is the packer's budget rule.

#### Controls

```yaml
controls:
  commands_allowed: [pause, resume, cancel, queue_instruction, add_context, interrupt_and_inject, fork, request_continuation]
  subscriptions:
    - { events: [human_task.opened, run.completed, chain_link.released], channel: { webhook: <url ref> | stream | mcp } }
  notifications: { on: [human_task, completion], channel: inbox }
```

`commands_allowed` intersects with grants; it can only narrow. Subscriptions are created at `start` (SPEC-06).

#### Chain form

```yaml
manifest: mission/v1
missions:
  - key: research
    ... (a full mission block)
  - key: ingestion
    ...
links:
  - { from: research, to: ingestion, kind: supplies, outputs: [evidence_map], on: goal_accepted, on_upstream_cancel: cancel_downstream }
  - { from: research, to: ingestion, kind: depends_on, on: execution_complete }
```

Links are specified in [SPEC-04](SPEC-04-mission-chains.md); the manifest validates that `from` and `to` name missions in the file, that `outputs` exist on the supplier's program root projection, and that the consumer declares a matching input `from: research.evidence_map`.

### `mc.manifest_resolution.v1`

```text
ManifestResolution {
  schema_version: "mc.manifest_resolution.v1"
  manifest_digest                       // sha256 of the canonical YAML bytes
  application_id, tenant_id, actor_ref  // from scope, never from the file
  resolved_at
  capabilities[]: { pointer, alias, request: {pin|search,...}, result: {capability_id, version, digest, kind, host_support[]} | blocker }
  plugins[]: { pointer, alias, members[]: capability pins }
  nodes[]: { node_key, lane, effective_environment, field_provenance{field: inherited|overlay|plugin:<alias>} }
  lane_support[]: { node_key, behavior, lane, supported: bool, reason? }
  hook_events[]: { node_key, hook alias, event, native_event | unsupported_on_lane }
  blockers[]: { code, pointer, message }
  warnings[]: { code, pointer, message }
}
```

Immutable; stored with the Revision; returned by `compile`.

### `MissionDefinition@1`, Compiled Program, Validation Report

Unchanged from the specification ("Definition and compiler contracts"). The compiler for the manifest emits `MissionDefinition@1` with `goals[]`, `objectives[]`, `criteria[]`, `inputs[]`, `program` (typed `ProgramNode@1` tree with exact Capability Pins), `policies`, `budget`, `completion_contract`; the Validation Report carries errors with JSON pointers into the manifest, missing capabilities, policy conflicts, cost and capacity warnings.

## Implementation Decisions

### Compile algorithm

Deterministic, no model judgment, no agent execution, no domain writes.

1. **Parse and shape-validate.** `yaml.safe_load` → Pydantic `MissionManifest` (`extra="forbid"`). Errors carry JSON pointers (`/mission/program/nodes/0/environment/budget/usd`). Compute `manifest_digest` over canonical YAML (sorted keys, no comments).
2. **Scope.** Take `application_id`, `tenant_id`, `actor_ref` from the authenticated request. If `mission.application` differs from the scope, fail `APPLICATION_FORBIDDEN`. The file never selects scope.
3. **Expand plugins.** For each `plugins` entry (pin or search), resolve the plugin row and append its member pins to the owning environment's `capabilities`, `skills`, `agents`, `hooks` with provenance `plugin:<alias>`. Duplicate pins collapse; conflicting versions of one capability id are `INVALID_DEFINITION`.
4. **Resolve capabilities.** For each `search` entry: call the Hybrid Search service (SPEC-01) with `query`, `kind`, scope, `host_support ⊇ require` (default: the node's lane), `max_results` 5. Take the top hit whose `host_support` covers every lane that will use the alias; zero admitted hits is blocker `CAPABILITY_UNAVAILABLE` (`search_no_admitted_hits`); a top hit whose score is within 0.05 of the second is a warning `ambiguous_search` listing both. For each `pin` entry: resolve the exact row; a missing or revoked row is `CAPABILITY_UNAVAILABLE`; a digest mismatch is `CAPABILITY_DRIFT`. `@latest` in a pin resolves to the current admitted version at compile and is recorded as such.
5. **Flatten environments.** Walk the program; compute each node's effective environment by the inheritance rules; record provenance; enforce narrowing (`INVALID_DEFINITION`, `widens_authority`).
6. **Lane support.** For each node, check `(behavior, lane)` against the lane registry's `describe` (SPEC-07): `deep_agents` supports all behaviors; `cursor_local` and `cursor_cloud` support `stage_graph`, `goal_loop`, `agent_executor`, `deterministic_executor`, `human_gate`, `event_wait`, `timer`, `proof_gate`, `child_mission_invocation`; anything else is `UNSUPPORTED_BEHAVIOR`. Check each hook alias's events against the lane's hook vocabulary (`unsupported_on_lane` warning, or blocker when `fail_closed: true`). Check each subagent overlay field against the lane (`readonly`/`is_background` unsupported inline on cloud → warning: projected as file).
7. **Coverage.** Every goal has ≥ 1 criterion; every criterion's `evidence` names an output some node declares; every `objectives` reference resolves; every `from` binding names an existing producer output with a compatible schema; every `action_space` alias resolves to a capability or nested node; Goal Loops have `iterations` and `patience`.
8. **Budgets and governors.** Node budgets never exceed the parent; the sum of sibling budgets in a Stage Graph may exceed the parent only with warning `oversubscribed_budget` (release is governed at run time); each Goal Loop and each `child_mission_invocation` has governors; depth ≤ platform cap.
9. **Emit.** Build `MissionDefinition@1`, call the existing compiler path to produce the Compiled Program (section "Mapping"), compute canonical digests, assemble the Validation Report and `ManifestResolution`. `compile` returns all four and persists nothing.

### Mapping onto the existing compiler

The code has no `MissionDefinition`; it has definition kinds, `CompileInvocation` and `compile_effective_run_configuration` producing an `EffectiveRunConfiguration` (the glossary's Compiled Program). The manifest service bridges in two steps:

1. `domain/authoring/manifest.py` defines `MissionManifest` (Pydantic), `MissionDefinition` (`MissionDefinition@1`, new, Pydantic, in `domain/authoring/mission_definition.py`), and `manifest_to_definition()` (pure).
2. `application/authoring/manifest_service.py::ManifestCompileService.compile()` performs steps 1 to 8 above, then **lowers** the definition into the existing inputs: a transient `StageGraphBlueprint` (via `build_stagegraph_v2`) or `GoalDirectedBlueprint`, a `RuntimeProfileDefinition` whose `operation_assemblies` carry the per-node effective environments as `OperationAssemblyDefinition` entries (lane, model, sandbox, capability requirements by exact pin), a `WorkflowConfigurationDefinition`, and a `CompileInvocation` with `input_manifest`, `caller_authority` from grants and `environment` availability from the deployment. It calls `ControlPlaneService.compile(invocation)` and attaches the resulting `EffectiveRunConfiguration` as the Compiled Program. Lowering is deterministic and digest-stable; it is recorded in the resolution (`lowering: {blueprint_digest, runtime_profile_digest}`). Until a later ticket makes the compiler consume `MissionDefinition@1` natively, this lowering is the only path; no second compiler is written.

Lane-specific execution bindings: for `deep_agents` nodes the assembly produces the existing `DeepAgentExecutionBinding`; for `cursor_*` nodes it produces the `CursorExecutionBinding` from SPEC-07. The manifest never names a binding; the lowering does.

### Submit and start

`submit`: compile; refuse if blockers; in one application transaction write `definition_snapshot` (the `MissionDefinition@1`), `mission_revision` (compiled digests), `compiled_program`, the manifest bytes and resolution as `authoring_provenance` rows, and admit the Run (existing admission path `mc.runtime_admission.v1` built from the Compiled Program) or, for a chain, the `mission_chain` and `chain_link` rows plus the first mission's Run (SPEC-04). Returns `mission_id`, `revision_id`, `run_id` (or `chain_id` and the list). Idempotent on `request_id`; re-submitting an identical manifest digest with a new `request_id` creates a new Revision only if the previous one is not the scheduling head with equal digests (then returns the existing head with `unchanged: true`).

`start`: the existing launch path (`POST /runs/{id}/launch`); registers the manifest's `controls.subscriptions` (SPEC-06) in the same transaction as the launch intent; never implied by submit.

### Persistence

No new tables in the T3 region for the manifest itself; reuse `definition_snapshot`, `mission_revision`, `compiled_program` (written by `canonical.py` today) and the hitherto unused `mission_draft`, `goal`, `objective`, `success_criterion`, `program_node` tables from `mig/0002` to persist the typed definition rows. Add in migration `0028_mission_chains.sql` (shared with SPEC-04) one table `authoring_provenance(revision_id, manifest_digest, manifest_yaml text, resolution jsonb, created_at, actor_ref)`, RLS-forced, immutable.

### Interfaces

| Surface | Verb | Behavior |
| --- | --- | --- |
| CLI | `missionctl mission compile FILE [--json]` | prints the Validation Report and resolution; exit 0 when no blockers, 2 on blockers |
| CLI | `missionctl mission submit FILE [--json] [--request-id UUID]` | compiles, commits, admits; prints ids; exit 2 on blockers, 4 on idempotency conflict |
| CLI | `missionctl mission start RUN_ID [--wait SECONDS]` | the existing launch; `--wait` as today |
| CLI | `missionctl mission schema [--out PATH]` | prints or writes the JSON Schema |
| HTTP | `POST /v1/applications/{app}/missions:compile` body `{manifest_yaml | manifest}` | 200 with report and resolution; 422 with report when blockers |
| HTTP | `POST /v1/applications/{app}/missions:submit` | 201 with ids; 200 on exact replay; 409 on conflict; 422 on blockers |
| HTTP | `POST /v1/applications/{app}/missions/{mission_id}/runs` | alias of launch for the admitted run of the head revision |
| MCP | `mission_manifest_compile(manifest_yaml)` | read-only tool; returns report and resolution |
| MCP | `mission_manifest_submit(manifest_yaml, request_id)` | consequential; requires `mission.author` |
| MCP | `mission_run_start(run_id)` | consequential; requires `mission.start` |
| Schema | `src/mission_control/contracts/schemas/mc.mission_manifest.v1.json` | exported by the existing schema export command; referenced by `# yaml-language-server: $schema=` in the examples |

Grants: compile needs `mission.read` and `catalog.read`; submit needs `mission.author`; start needs `mission.start`.

### Error codes

| Code | When |
| --- | --- |
| `INVALID_DEFINITION` | shape errors, unknown keys, widening overlays, coverage failures, unresolved `from`, missing governors |
| `CAPABILITY_UNAVAILABLE` | search with zero admitted hits; pin not found or revoked |
| `CAPABILITY_DRIFT` | pin digest differs from the admitted row |
| `UNSUPPORTED_BEHAVIOR` | behavior not supported on the node's lane; `claude_agent_sdk`/`codex` lanes in v1 |
| `AUTHORITY_DENIED` | side effects or commands outside the caller's grants |
| `APPLICATION_FORBIDDEN` | file application differs from scope |
| `BUDGET_EXHAUSTED` | mission budget exceeds the tenant's admitted ceiling at submit |
| `IDEMPOTENCY_CONFLICT` | same `request_id`, different manifest digest |

Every error carries `pointer` (JSON pointer into the manifest) and `message`.

### Insertion points

- New: `src/mission_control/domain/authoring/manifest.py` (schema, inheritance, pointers), `domain/authoring/mission_definition.py` (`MissionDefinition@1`), `application/authoring/manifest_service.py` (compile, submit), `interfaces/http/missions.py` (router), CLI `mission` group in `interfaces/cli/main.py` (T3 may add the group; the integrator owns the file), MCP tools in `interfaces/mcp/coordinator_server.py`.
- Reuse: `ControlPlaneService.compile`, `compile_effective_run_configuration`, `build_stagegraph_v2`, `canonical.py` writers, the admission path used by `CoordinatorWorkflowLaunchService`, `CapabilitySearchService` (SPEC-01 A3 adds host-support filtering and lexical fallback).
- Schema export: the command that writes `src/mission_control/contracts/schemas/*.json` today gains the manifest.

## Testing Decisions

A good test exercises the public verb (compile, submit, start) on a manifest fixture and asserts on the report, the persisted rows, or the admitted run, never on internal lowering structures except through their digests.

- Unit (`tests/unit/authoring/test_manifest_schema.py`): shape errors with pointers; inheritance (deep merge, list replace, narrowing violations); plugin expansion and duplicate collapse; `from` resolution; coverage; chain form validation.
- Unit (`test_manifest_compile.py`): with an in-memory catalog and a stub Hybrid Search, each error code; `ambiguous_search` warning; lane support matrix; deterministic digests (compile twice → identical `manifest_digest`, definition digest and resolution).
- Golden: the three manifests under `missions/` compile with zero blockers against the seeded catalog fixture (`tests/fixtures/catalog/fast_track_seeds.json`), and their resolutions are committed as golden files.
- Integration (`tests/integration/postgres/test_manifest_submit.py`, `common_db`): submit writes `definition_snapshot`, `mission_revision`, `compiled_program`, `authoring_provenance`, and admits a run; replay with the same `request_id` returns the same ids; `IDEMPOTENCY_CONFLICT` on a changed digest.
- Acceptance (`tests/acceptance/mission_control/test_manifest_lifecycle.py`): `missionctl mission compile|submit|start` against the real local stack on mission 1's manifest reaches a running Stage Graph (ties to ticket I1).
- Prior art: `tests/unit/control_plane/test_control_plane.py` (compile), `tests/unit/coordinator/test_coordinator_launch_preparation.py` (prepare and launch), `tests/acceptance/control_plane/test_rrm_007_api.py` (CLI against the API).

## Out of Scope

`templates:` and `extends:` (v2); authoring review Human Tasks for proposals (the existing governed proposal flow remains available but is not wired to the manifest in v1); `claude_agent_sdk` and `codex` lanes; manifest-driven Revision Proposals against a running mission (`request_revision` stays a command); a graphical editor; schema migration of manifests between versions.

## Further Notes

The manifest is where the owner's "proprietary mission contract" lives, but the word contract is avoided (Completion Contract and Operating Contract already exist). A manifest may carry `# yaml-language-server: $schema=../../../src/mission_control/contracts/schemas/mc.mission_manifest.v1.json` as its first line for editor validation; the loader ignores comments.

Capability `search` entries are the one place stochastic ranking enters authoring; the resolution freezes the result, so a committed Revision is deterministic regardless of later catalog changes.

## Tickets

- [FT-E1](issues/E1-manifest-schema-inheritance-json-schema.md) Manifest schema, inheritance rules and JSON Schema export.
- [FT-E2](issues/E2-manifest-compile-resolve-searches-validate-lanes.md) Compile resolves searches to pins and validates lanes.
- [FT-E3](issues/E3-manifest-submit-and-start-cli-mcp-provenance.md) Submit and start on CLI and MCP; provenance stored with the revision.

## Example manifest (annotated)

```yaml
# yaml-language-server: $schema=../../../src/mission_control/contracts/schemas/mc.mission_manifest.v1.json
manifest: mission/v1
mission:
  key: nad-muscle-sweep
  title: Literature sweep on NAD+ precursors and skeletal muscle
  application: biotech                    # must equal the authenticated application
  domain_pack: biotech.research           # app-owned templates and rubrics

  goals:
    - key: evidence_map
      description: Map the human evidence for NMN and NR effects on muscle function
      importance: primary
      criteria:
        - key: coverage
          description: The claim table covers every included trial
          evidence: [source_manifest, claim_table]
          acceptance: { assessment: { capability: { search: "evidence coverage assessment", kind: assessment, as: coverage_check }, threshold: 0.8 } }
        - key: reviewed
          description: A reviewer accepted the claim table
          evidence: [claim_table]
          acceptance: { human: review_accept }

  environment:                            # mission-level defaults; every node inherits
    lane: deep_agents
    model: { profile: frontier.default }
    sandbox: { profile: research.standard, egress: [pubmed, biorxiv, tavily, firecrawl] }
    workspace:
      skills: [ { pin: "skill.mission-control-observe@0.2.0#sha256:0000000000000000000000000000000000000000000000000000000000000000" } ]
    capabilities:
      - { search: "pubmed literature retrieval", kind: mcp_server, as: pubmed, tools: [search_articles, get_article_metadata] }
      - { search: "web search and extract", kind: mcp_server, as: tavily }
    budget: { usd: 25, tokens: 2000000, wall_clock: 4h }
    governors: { depth: 2, fan_out: 6, iterations: 8, patience: 2, transfers: 3 }
    side_effects: [read_only, workspace_write]

  program:
    key: root
    behavior: stage_graph
    objectives: [evidence_map]
    nodes:
      - key: collect
        behavior: goal_loop
        objective: evidence_map
        action_space: [pubmed, tavily]
        environment: { budget: { usd: 10 }, governors: { iterations: 5 } }   # narrows only
        outputs: [ { name: sources, schema: source_manifest@1, required: true } ]
      - key: synthesize
        behavior: agent_executor
        objectives: [evidence_map]
        depends_on: [collect]
        inputs: [ { name: sources, from: collect.sources, expand: materialize } ]
        instruction: .mission/instructions/synthesize.md
        environment: { model: { profile: frontier.long_context } }
        outputs: [ { name: claims, schema: claim_table@1, required: true } ]
      - key: review
        behavior: human_gate
        depends_on: [synthesize]
        task: { kind: REVIEW, prompt: "Accept the claim table?", reviewers: [owner], packet: [synthesize.claims], on_timeout: keep_waiting }

  controls:
    commands_allowed: [pause, resume, cancel, queue_instruction, add_context]
    subscriptions:
      - { events: [human_task.opened, run.completed], channel: stream }
```

## Field reference

| Pointer | Type | Required | Notes |
| --- | --- | --- | --- |
| `/manifest` | literal `mission/v1` | yes | |
| `/mission` or `/missions[]` | mission block | one of | `missions` requires `links` |
| `/links[]` | link | with `missions` | SPEC-04 |
| `mission/key` | slug | yes | unique in file |
| `mission/title` | string | yes | |
| `mission/application` | `biotech` or `ai-engineer` | yes | must equal scope |
| `mission/domain_pack` | ref | no | |
| `mission/goals[]` | Goal | yes, ≥1 | |
| `goal/key`, `goal/description` | slug, string | yes | |
| `goal/importance` | enum | no | default `primary` |
| `goal/objectives[]` | Objective | no | `parent` optional |
| `goal/criteria[]` | SuccessCriterion | yes, ≥1 | |
| `criterion/evidence[]` | output names or schema refs | yes | |
| `criterion/acceptance` | acceptance AST | yes | one of assessment, human, schema, all, any |
| `mission/environment` | Environment | yes | full at mission level |
| `environment/lane` | enum | yes at mission | `deep_agents`, `cursor_local`, `cursor_cloud` |
| `environment/model` | `{profile, settings}` | yes at mission | profile is id or search entry |
| `environment/sandbox` | `{profile, egress[]}` | yes for `deep_agents` | |
| `environment/workspace` | `{repo, context[], skills[]}` | `repo` required for cursor lanes | |
| `environment/capabilities[]` | CapabilityEntry | no | pin or search |
| `environment/agents[]` | AgentEntry | no | subagent profiles |
| `environment/hooks[]` | HookEntry | no | hook scripts |
| `environment/plugins[]` | pin or search | no | expanded at compile |
| `environment/budget` | `{usd, tokens, wall_clock, tool_calls}` | yes at mission | decimals as strings or numbers; never floats internally |
| `environment/governors` | `{depth, fan_out, iterations, rounds, patience, transfers}` | yes at mission | Goal Loops need `iterations`, `patience` |
| `environment/side_effects[]` | enum | no | default `[read_only, workspace_write]` |
| `mission/program` | ProgramNode | yes | root |
| `node/key` | slug | yes | unique in mission |
| `node/behavior` | enum | yes | discriminator |
| `node/objectives[]` | keys | executors and systems | |
| `node/environment` | partial Environment | no | overlay |
| `node/inputs[]` | InputBinding | no | `from`, `artifact` or `value`; `expand` |
| `node/outputs[]` | `{name, schema, required}` | executors | |
| `node/completion` | `{acceptance}` | no | |
| `node/depends_on[]` | keys | inside `stage_graph` | |
| `node/action_space[]` | aliases or node keys | `goal_loop` | |
| `node/task` | HumanTask | `human_gate` | |
| `mission/controls` | Controls | no | |
| `controls/commands_allowed[]` | command kinds | no | narrows grants |
| `controls/subscriptions[]` | Subscription | no | SPEC-06 |

# Citations

- ADR-0022, ADR-0034, ADR-0023, ADR-0027, ADR-0029, ADR-0010, ADR-0020 (`docs/adr/`).
- `.scratch/mission-manifest/spec.md` (draft the owner accepted as the shape).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Coordinator interaction and authoring", "Definition and compiler contracts", "Public skill CLI MCP and dashboard contract").
- `../mission-control-general/workflow-types/00`, `01`, `02`, `05`, `06`.
- [research/codebase-map.md](research/codebase-map.md) section 8 (`CompileInvocation`, `ControlPlaneService.compile`, `build_stagegraph_v2`, unused `mig/0002` tables, `WorkflowDesignDraft`) and gap (f).
- `src/mission_control/domain/authoring/contracts.py` (`CompileInvocation`, `EffectiveRunConfiguration`, `RuntimeProfileDefinition`, `StageGraphBlueprint`, `GoalDirectedBlueprint`).
- `GLOSSARY.md`.
