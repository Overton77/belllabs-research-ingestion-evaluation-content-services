# Mission Control — Final Specification

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

> **Superseded (2026-09-07).** The architectural authority is now [`MISSION_CONTROL_ARCHITECTURE.md`](./MISSION_CONTROL_ARCHITECTURE.md), with behavior in [`workflow-types/`](./workflow-types/index.md), language in [`../../CONTEXT.md`](../../CONTEXT.md), and decisions in [`../../adr/`](../../adr/). This document remains a source for later-phase material (capabilities §9, additional lanes §10.4–10.5, prompt cache and file editing §12.2–12.3, proof verticals and goal families §13–16, A2A and packaging §17.6–17.7, other stores §19.3). Its `NodeSpec.strategy` union, `MissionDefinition.graph`, `MissionEdge`, singular `goal`, session-reuse assumptions, single-status enums, and `orchestration.*` schema extension are retired; see the architecture §20 for the mapping.

**Status:** superseded candidate (was: canonical lock)  
**Date:** 2026-09-07  
**Supersedes:** [`MISSION_CONTROL_PRESPEC.md`](./MISSION_CONTROL_PRESPEC.md) and [`MISSION_CONTROL_SUPPLEMENT.md`](./MISSION_CONTROL_SUPPLEMENT.md) (both remain as sources; new decisions land here)  
**Companion:** [`MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md`](./MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md)  
**Service:** `ai-engineer-mission-control` · **Schema owner:** `ai-engineer-db-contract` · **Proof/knowledge owner:** `ai-engineer-knowledge-services`

This document is written from the position of the **coordinator agent**: the agent that knows more about current agent and workflow strategy than the agents it directs, that must be able to draft a mission of any complexity, must be able to ask the system what it can compose from, must be able to run codebase work, deep research, ingestion, and evaluation on one plane, and must be able to look inside a running mission and change its trajectory when policy allows. Everything below exists to make that coordinator effective and safe.

Mission Control serves two customers on the same kernel:

1. **AI Engineer app intelligence** — research, ingestion, verification, promotion, curriculum projection into the proprietary KB.
2. **AI Engineer app services** — building, testing, deploying, and improving the actual product software (web, mobile, desktop, services), including Mission Control itself.

---

## 0. What is locked here that was open before

| Pre-spec open item | Lock |
|---|---|
| Hard spawn-depth default | Soft cap **3** per mission unless granted; hard cap **16**, platform-configurable |
| Cyclic `STAGE_GRAPH` timing | Compiler accepts `cycles: allowed` from the first compiler release; runtime cycle execution ships with the M4-cluster workflow-type milestone (sequence M6), together with `GOAL_LOOP`, `PARALLEL_SWARM`, and `EVALUATOR_OPTIMIZER`. First demos may use strategy loops |
| Package registry / scope | `@aiengineer/*` on GitHub Packages (matches existing `@aiengineer/database-contract`, `@aiengineer/mission-contracts`). The pre-spec's `@ai-engineer/*` is retired |
| Temporal hosting | Temporal Cloud (ADR 0001 in the service repo). Workers stay portable |
| MCP UI vs web debugger | Same contract. Web four-zone layout first; **MCP Apps** resources for peek/status ship in the same milestone as the web debugger; the MCP Apps command composer follows |
| Wallets | Schema reserved (`WALLET` capability kind, `CREDENTIAL` human task, `credit_account`), enablement deferred to north-star phase 4 |
| Official vs personal branding | One product, "Mission Control". Official AI Engineer is tenant + module pack, not a different kernel |

### Dated amendments

**2026-09-07b — Workflow-type layering and spawn.** Workflow-type lifecycles (`STAGE_GRAPH`, `GOAL_LOOP`, `PARALLEL_SWARM`, `EVALUATOR_OPTIMIZER`) complete on a simple harness in the M4 cluster, before the dashboard control plane and before capability, compaction, and catalog-choice work. `MISSION_SPAWN` names the system as an invocable Agent Skill / plugin: any granted agent starts a mission. Nodes in other workflow types (for example a `STAGE_GRAPH` stage) may optionally carry a spawn grant for a sub-objective. It is not a seventh workflow engine sequenced after evolution. See [`MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md`](./MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md) principles 2–4 and M4–M8.

Corrections to the pre-spec and supplement, based on verified 2026-09-07 facts:

| Was written | Now |
|---|---|
| `DurableAgent` from `@workflow/ai` as the Eve/Vercel inner agent | **`WorkflowAgent` from `@ai-sdk/workflow`** on Workflow SDK 5 (`workflow@beta`). `DurableAgent` is deprecated |
| Temporal sees a Vercel run as an "external durable operation" (implied Nexus) | **Temporal Activity** + async activity completion (task token) with poll fallback. Nexus is Temporal↔Temporal cross-namespace only |
| "MCP UI" | **MCP Apps** (official extension): `ui://` resources, `text/html;profile=mcp-app`, `_meta.ui.resourceUri`. `@mcp-ui/client` may be used as a renderer |
| A2A "north-star surface" without version | **A2A 1.0**: `TASK_STATE_*` lifecycle, Agent Card `supportedInterfaces[]`, SSE + push notifications |
| MCP without version | Target **MCP `2026-07-28`** (stateless), serve **`2025-11-25` dual-era** until hosts finish migrating |
| Store classes `official_canonical / internal_exploratory / user_managed` everywhere | API vocabulary stays. Persistence maps to db-contract literals `official / exploratory / user_managed`. The kernel never invents a third vocabulary |
| Interrupt semantics "adapter-specific" | Now an explicit **delivery-semantics enum** reported per command per runtime (§11.2). Cursor Cloud cannot inject mid-run; it is `cancel_and_replace` or `wait_then_send` |
| One "plugin" | **Three manifests, one payload**: Agent Plugin (portable `plugin.json` + `mcp.json`), Cursor plugin (`.cursor-plugin/`), Claude Code plugin (`.claude-plugin/`), sharing one `SKILL.md` set and one MCP server |

---

## 1. Product bet and invariants

### 1.1 Bet

Extreme decomposition with guardrails. A coordinator compiles a real execution program from strategies, harnesses, capabilities, workspaces, and proof gates. Humans and authorized agents inspect and steer live work through one durable command vocabulary. Nothing trusted reaches search, curriculum, or a merged codebase without independent proof. The same plane later runs personal intelligence spaces and is shareable as a platform.

### 1.2 Invariants (binding on every milestone)

1. **Sessions and provider runs are disposable. Missions are durable.** Chat text is never a receipt.
2. **One domain, many clients.** HTTP API, CLI, MCP, MCP Apps, Agent Skill, A2A, web, mobile, desktop share one application service. None schedule independently.
3. **Postgres is the domain ledger. Orchestrators recover execution.** Temporal and Vercel Workflow history carry identifiers and compact outcomes only.
4. **New shared schema lives only in `ai-engineer-db-contract`.** Mission Control consumes the pinned contract; no app-local `Database` types, no second migration tree.
5. **Capabilities are admitted, versioned, discoverable.** Discovery never grants authority; admission and materialization do.
6. **Spawn is a grantable capability, not a raw provider key.** Ordinary agents never hold `CURSOR_API_KEY` or equivalents. They call Mission Control.
7. **Proof is a required stage of completion.** Code: tests. Research: `verification.v1`. Curriculum and retrieval consume admitted records only.
8. **Persist immediately, promote atomically.** The verified output, not the mission, is the transaction boundary.
9. **Producer and verifier are different deployments.** Self-verification is rejected.
10. **Design order is API → CLI → MCP → MCP Apps → Skill → A2A → web/mobile/desktop.**
11. **Do not start a second flywheel, learner schema, notes/KB platform, or mission ledger.**
12. **Delivery semantics are reported, never assumed.** Every command result names how it was delivered on that runtime.
13. **Contracts before adapters.** Types and schemas are published and compiled against before any harness is wired. Simple agent bindings run on the full kernel before rich ones.
14. **Introspection before composition.** A coordinator can always ask the system what strategies, runtimes, models, profiles, capabilities, templates, budgets, and limits exist before drafting.

---

## 2. Architecture

### 2.1 Layers

```text
Coordinator (human, dashboard chat, or any agent that installed the plugin)
   │  draft / validate / commit / start / peek / command / reconcile / spawn
   ▼
Mission Control API  ── discovery (system.describe, catalog, profiles, runtimes, schemas)
   │
   ├── Compiler + revision ledger (deterministic)
   ├── Policy (grants, budgets, depth, credit lanes, admission)
   ├── Command router (durable commands → orchestrator updates)
   └── Event gateway (Postgres events + outbox → WS / SSE / MCP)
   │
   ▼
Temporal (mission kernel: MissionWorkflow → NodeWorkflow → HarnessExecutionWorkflow)
   │                                      │
   │  Activities                          │  Activity + async completion
   ▼                                      ▼
Harness adapters                     Vercel Workflow lane (Eve / WorkflowAgent)
  cursor_cloud · cursor_local
  claude_agent_sdk · codex_app_server
  eve · workflow_agent · deep_agents
  direct_model · deterministic_executor
   │
   ▼
Workspaces · sandboxes · repos · browsers · tests · deployments
   │
   ▼
Artifacts + provenance → Postgres (db-contract) + object storage + git
   │
   ▼
Knowledge Services: retrieval · verification.v1 · ingestion · promotion (by ID, never embedded)
```

### 2.2 Ownership

| Layer | Owns | Does not own |
|---|---|---|
| Mission Control | Mission lifecycle, compiler, policy, spawn, commands, events, discovery, dashboard contract | Verification algorithms, embeddings, canonical fact physics |
| Temporal | Durability of mission / node / harness executions across all lanes; timers; cancellation; human waits | Vendor step graphs; chat tokens; UI archives |
| Vercel Workflow | Durable steps inside the Eve / WorkflowAgent lane | Canonical mission state |
| Harness adapters | Vendor launch, follow-up, interrupt, snapshot, continuation, cancel | Graph mutation, admission, budgets |
| Knowledge Services | Capture → chunk → embed → retrieve → evidence packet; `verification.v1`; promotion decisions; publication | Mission scheduling |
| db-contract | Shared schema, RLS, generated types | Runtime orchestration |
| Clients | Render and steer | Orchestration rules |

### 2.3 Three-layer neutrality

Kernel nouns are provider-neutral (mission, revision, node, strategy, artifact, command, budget, proof, spawn, peek). Execution binding is dimensional (§4.2). Adapters are vendor-faithful and observational: native status never becomes canonical state without a kernel transition.

---

## 3. Coordinator model

The coordinator is a compiler, not a task lister. Its loop is:

```text
describe → explore → draft → validate → (dry-run review) → commit → start
        → peek → steer (queue / interrupt / pause / resume) → reconcile (mutate graph)
        → spawn (child missions) → accept proof → publish / merge / promote
```

### 3.1 What a coordinator may always do

- Query the **Composition Manifest** (§8) for strategies, runtimes, models, credit accounts, sandboxes, capability and environment profiles, templates, exemplars, verification kinds, policy limits, and per-runtime command delivery semantics.
- Draft a `MissionDefinition` of arbitrary size: any number of nodes, mixed strategies, nested subgraphs, cycles under budget, per-node harness/model/skills/workspace/hooks/output contracts, event triggers, and child-mission portals.
- Validate and compile deterministically without spending agent budget.
- Peek any mission it is scoped to, at bounded fidelity.
- Issue commands within its grant, and propose graph mutations that become new immutable revisions.

### 3.2 What a coordinator may never do by prose

Raise hard caps, add authority to a capability profile, approve its own proof, publish to `official_canonical`, obtain provider launch keys, or mutate committed history. Those require a policy grant, a `POLICY_OVERRIDE` / `APPROVAL` human task, or a new revision.

### 3.3 Agent binding levels

To let lifecycle and control-plane work land before capability work, every agent node declares a binding level. The kernel is identical at every level; only materialization differs.

| Level | Node receives | Purpose |
|---|---|---|
| `simple` | objective prompt, repo/workspace ref, model, output contract, budget | First lanes; lifecycle proofs; dashboard proofs |
| `standard` | `simple` + admitted skills, MCP allowlist, workspace template, admitted hooks, verification gates | Real coding/research work |
| `full` | `standard` + sandbox config, HITL tool middleware, subagents, continuation policy, prompt-cache policy, memory policy, source-intelligence policy, spawn grant, Mission Control skill | Deep missions, recursive spawn |

---

## 4. Domain model

### 4.1 Entities and their storage

`exists` = row/table already in db-contract; `extend` = additive columns; `new` = new table in `ai-engineer-db-contract` (§19).

| Entity | Meaning | Storage |
|---|---|---|
| `GoalSpec` | Objective, success criteria, non-goals, constraints, evidence and verification requirements, output contracts | `orchestration.mission.goal` + `acceptance_criteria` (extend with `goal_spec jsonb`) |
| `Mission` | Durable objective + policy envelope + lineage + status | `orchestration.mission` (exists; extend: `root_mission_id`, `parent_mission_id`, `parent_node_id`, `depth`, `active_revision_id`, `goal_spec`, `policy jsonb`) |
| `MissionRevision` | Append-only authored `MissionDefinition`; exactly one `active` per execution epoch | `orchestration.mission_revision` (new) |
| `CompiledGraph` | Immutable, fully resolved program for one revision; digest | `orchestration.compiled_graph` (new) |
| `MissionNode` | One compiled logic unit with a strategy and execution binding | `orchestration.mission_node` (new) |
| `MissionEdge` | `requires` · `supplies` · `gates` · `evaluates` · `supersedes` · `derives`, with predicate | `orchestration.mission_edge` (new); `work_item_dependency` remains the runtime release edge |
| `WorkItem` | Runtime instance of a compiled node for one epoch | `orchestration.work_item` (exists; extend: `node_id`, `revision_id`, `epoch`, `strategy`) |
| `Attempt` | One retry epoch of a work item | `orchestration.attempt` (exists; extend: execution dimensions) |
| `HarnessExecution` | Attachment of a harness conversation to an attempt; native IDs | `orchestration.harness_execution` (new) |
| `ProviderRun` | One prompt / CLI / SDK invocation; tokens, cache hit/miss, cost, credit account | `orchestration.provider_run` (new) |
| `Session` | Harness-native context window; disposable | `orchestration.agent_session` (exists; generalize beyond Eve) |
| `ContinuationCheckpoint` | Compact handoff at compaction | `orchestration.continuation_checkpoint` (exists) |
| `Artifact` | Immutable content or manifest with digest and lineage | `orchestration.artifact`, `artifact_manifest`, `artifact_lineage` (exist) |
| `OperationIntent` / `Receipt` | Canonical write proposal and executor receipt | `orchestration.operation_intent`, `operation_receipt` (exist) |
| `PromotionProposal` | Atomic candidate for canonical admission | `retrieval.content_promotion_proposal` (exists) + research-scoped proposal (new, §19) |
| `HumanTask` | `APPROVAL` · `QUESTION` · `SELECTION` · `REVIEW` · `CREDENTIAL` · `POLICY_OVERRIDE` | `orchestration.human_task` (new; links to `evaluation.review_task` / `knowledge_service.review_subject` when knowledge-scoped) |
| `Command` | Durable operator or agent intervention | `orchestration.mission_command` (new) |
| `MissionEvent` | Normalized, attributable, cursor-ordered event | `orchestration.mission_event` (exists; extend: `seq`, `family`, `work_item_id`, `attempt_id`, `actor`, `payload`) |
| `CapabilityDescriptor` / `CapabilityProfile` | Versioned capability; allowlisted set | `orchestration.capability`, `capability_version`, `capability_profile`, `capability_profile_item` (exist; extend `capability_kind` seed) |
| `EnvironmentProfile` | Repo, workspace template, sandbox config, secret refs, MCP config digest | `orchestration.environment_profile` (new) |
| `CreditAccount` / `CreditLedgerEntry` | Per-lane spend ledger | `orchestration.credit_account`, `credit_ledger_entry` (new) |
| `MissionInputSnapshot` | Immutable canonical-state snapshot at planning | `orchestration.mission_input_snapshot` (new; from PoC proposal) |
| `SpawnGrant` | Scoped token record: tenant, mission, remaining depth, budget, ops | `orchestration.spawn_grant` (new) |
| `SourceEncounter` | What we asked the world and what came back | `evidence.source_query` / `source_retrieval` (exist) + `evidence.source_encounter` (new, later) |
| `MemoryItem` | Curated typed memory | `orchestration.memory_item` (new, later) |

Keep `session` for harness context windows only. Knowledge Services already binds `missionId`, `workItemId`, `attemptId` on every operation.

### 4.2 Execution dimensions

Never collapse into a single `provider` field. Every attempt and every compiled agent node resolves all five.

| Dimension | Values |
|---|---|
| `agent_runtime_kind` | `cursor_cloud` · `cursor_local` · `claude_agent_sdk` · `codex_app_server` · `codex_sdk` · `eve` · `workflow_agent` · `deep_agents` · `direct_model` · `deterministic_executor` |
| `orchestrator_kind` | `temporal` · `vercel_workflow` · `langgraph_checkpoint` (inner only) |
| `model_access_kind` | `cursor_credits` · `vercel_ai_gateway` · `direct_openai` · `direct_anthropic` · `direct_other` · `none` |
| `compute_environment_kind` | `cursor_cloud_vm` · `local_trusted_fs` · `local_sandbox` · `vercel_function` · `vercel_sandbox` · `aws_worker` · `browser_runner` · `deep_agents_sandbox` |
| `credit_account_kind` | `cursor` · `vercel_ai_gateway` · `openai_direct` · `anthropic_direct` · `other_direct` · `aws_compute` · `temporal_cloud` · `none` |

Compiler rule: a node's `credit_account_kind` must be consistent with its `model_access_kind` and `agent_runtime_kind`; a Cursor node cannot draw Gateway tokens and an Eve node cannot draw Cursor credits. Fallback orders name a credit account per fallback.

### 4.3 GoalSpec

```text
GoalSpec@1 {
  objective: string
  success_criteria[]: { id, statement, check: machine | evaluator | human, ref? }
  non_goals[]
  constraints[]
  evidence_requirements[]: { kind, min_count?, locator_required: boolean }
  verification_policy: VerificationPolicy        // §13
  output_contracts[]: { name, schema_ref, required: boolean, sensitivity }
}
```

A goal completes when the verification policy accepts the output contracts, not when an agent says so.

### 4.4 State machines

**Mission** (`orchestration.mission_status`, existing enum, extended): `created → planning → committed → running ⇄ paused → blocked → succeeded | failed | cancelled | superseded`. `blocked` means waiting on a human task or event trigger. Additive enum value: `committed`.

**WorkItem** (existing): `pending → ready → running → succeeded | failed | cancelled | skipped`, with `blocked` from any pre-terminal state. `running` requires a lease (existing constraint).

**Attempt** (existing): `succeeded | failed | timeout | cancelled | rejected`. `rejected` is the proof-rejection outcome and is not retried as infrastructure failure.

**Command** (new `command_state`): `accepted → queued → delivered → observed → completed | failed | rejected | expired`. Idempotency key is `command_id`. Delivery semantics recorded at `delivered`.

**HumanTask** (new): `open → claimed → answered | approved | denied | expired`. Answers are artifacts.

**Revision** (new `revision_status`): `draft → validated → committed → active → superseded`. Only one `active` per mission.

---

## 5. Workflow types and strategies

Strategies are per-node execution semantics. A mission may compile a fifty-node program mixing all seven. No strategy is the architecture.

| Strategy | Use when | Compiled runtime semantics | Governors |
|---|---|---|---|
| `STAGE_GRAPH` | Dependencies are known | Nodes release when incoming `requires` / `gates` predicates pass. Default acyclic. `cycles: allowed` admits revisits | `max_visits_per_node`, `termination_predicate`, remaining budget |
| `GOAL_LOOP` | Route uncertain | Observe → act → evaluate → continue until success criteria or stop | `max_iterations`, tokens, dollars, wall-clock, `stop_predicate` |
| `PARALLEL_SWARM` | Breadth or competing hypotheses | N bounded workers; fan-in through `evaluator_fan_in` or `coalesce` node | `max_workers`, `quorum`, `speculative_race`, `fail_fast` |
| `DETERMINISTIC` | Mechanical work must not be delegated to a model | Executor with typed inputs/outputs, retries, receipts; no LLM | retry policy, timeout, idempotency key |
| `EVALUATOR_OPTIMIZER` | Quality must rise | Producer ↔ evaluator alternate; accept at threshold or cap | `max_rounds`, `threshold`, rubric version, distinct evaluator identity |
| `MISSION_SPAWN` | Subtree needs own owner/lifecycle/schedule/authorization | Emits a child mission from an artifact set; awaits or tracks | depth, children per node, descendants, remaining budget, justification |
| `EVENT_TRIGGER` | Work should resume when the world changes | Node arms a durable wait on typed events (commit, failed proof, new source, revalidation due, external webhook, timer) | `max_firings`, cooldown, budget remaining, expiry |

`STAGE_GRAPH`, `GOAL_LOOP`, `PARALLEL_SWARM`, and `EVALUATOR_OPTIMIZER` are workflow-type systems: each has its own runtime, governors, and lifecycle proof. They complete on a simple harness before capabilities, compaction, or coordinator catalog choice layer on. `DETERMINISTIC` is the mechanical executor. `EVENT_TRIGGER` is a durable wait primitive.

`MISSION_SPAWN` is invocability, not a peer of those four systems. The product meaning is Mission Control as an Agent Skill / plugin: a granted agent drafts and starts missions. A node of another type may optionally carry `spawn_grant` and `mission_control_access: peek_command_spawn` and emit a child for an objective or sub-objective. A compiled `MISSION_SPAWN` portal is that grant, not a fifth workflow engine.

### 5.1 Two kinds of cycles

1. **Strategy loops** — `GOAL_LOOP`, `EVALUATOR_OPTIMIZER`, `EVENT_TRIGGER`. Bounded by iteration/token/dollar/wall-clock governors; the compiled edge list stays acyclic.
2. **Cyclic stage graphs** — a `STAGE_GRAPH` declares `cycles: allowed`. The compiler admits it only with `max_visits_per_node`, a `termination_predicate`, and a budget-remaining check. Each revisit is a new `WorkItem` epoch under the same node; history is append-only.

### 5.2 Parallelism and failure declarations

Per node or subgraph: `serial` · `bounded_parallel(n)` · `fully_parallel` · `quorum(k of n)` · `map_reduce` · `speculative_race` · `evaluator_fan_in`.

Importance: `required` · `optional` · `best_effort`. Independent branches continue unless `fail_fast`.

### 5.3 Edges and predicates

```text
MissionEdge@1 {
  from, to
  kind: requires | supplies | gates | evaluates | supersedes | derives
  predicate?: { all?: [...], any?: [...], artifact_present?, disposition_in?, score_gte?, human_task_state?, event_seen? }
  payload_binding?: { from_output: string, to_input: string }
}
```

`supplies` carries artifacts between nodes. `gates` holds release until a proof or human task resolves. `evaluates` binds an evaluator to a producer. `supersedes` and `derives` are lineage edges written by evolution (§7).

### 5.4 Subgraphs, templates, dynamic fan-out

- A `NodeSpec` may contain `subgraph: NodeSpec[]` with its own edges and concurrency; compiled flat with namespaced keys.
- Templates expand into ordinary `MissionDefinition` drafts; exemplars are immutable tested expansions used in CI.
- Dynamic fan-out is declared: a `PARALLEL_SWARM` or `STAGE_GRAPH` node may set `fan_out_from: <artifact list output>` with `max_children`; the compiler emits a bounded template child per list item at runtime under the same revision. Anything beyond the declared bound requires a new revision.

### 5.5 Research templates are catalog, not kernel

Time-context (publication / historical / interval / current), entity identity, entity descriptor, metric probe, technical thread, artifact assessment, course/challenge/project projection, application build → test → preview → improve, harness comparison experiment, RSI after-commit optimizer, blank N-node stage graph.

---

## 6. `MissionDefinition@1`

```text
MissionDefinition@1 {
  schema_version: "mission-definition@1"
  goal: GoalSpec@1
  graph: { nodes: NodeSpec[], edges: MissionEdge[], cycles: forbidden | allowed, concurrency: ConcurrencyPolicy }
  inputs: ArtifactBinding[]                    // immutable refs; USED_AS_INPUT_TO lineage
  input_snapshot_policy: { required: boolean, scope[] }
  template_ref?: { template_id, version, params_digest }
  workspace_templates: WorkspaceTemplate[]
  sandbox: SandboxConfig                       // mission default; nodes may override
  capability_bindings: CapabilityBinding[]     // exact versions from an admitted profile
  environment_profile_ref?: string
  hooks: HookSpec[]                            // coordinator-authored; admitted as artifacts
  tool_middleware: ToolMiddleware[]            // HITL gates on tools
  file_edit: FileEditPolicy
  memory: MemoryPolicy
  source_intelligence: SourceIntelligencePolicy
  experiments?: ExperimentPolicy
  prompt_cache: PromptCachePolicy
  continuation: ContinuationPolicy
  budgets: BudgetEnvelope                      // per credit account + tokens + wall-clock + storage + browser minutes
  verification: VerificationPolicy
  human_controls: HumanControlPolicy
  spawn_policy: SpawnPolicy
  event_policy: EventPolicy                    // which world events may arm EVENT_TRIGGER nodes
  observability: ObservabilityPolicy
}

NodeSpec@1 {
  key
  strategy: STAGE_GRAPH | GOAL_LOOP | PARALLEL_SWARM | DETERMINISTIC | EVALUATOR_OPTIMIZER | MISSION_SPAWN | EVENT_TRIGGER
  objective, success_criteria[]
  importance: required | optional | best_effort
  binding_level: simple | standard | full
  execution: { agent_runtime_kind, orchestrator_kind, model_access_kind, compute_environment_kind, credit_account_kind,
               model: { id, params }, fallbacks[] }
  workspace_ref?, sandbox_ref?
  capabilities: { skills[], mcp_servers[], clis[], custom_tools[], subagents[] }   // versions, all admitted
  hooks[], tool_middleware[]
  inputs[]: { name, from: artifact | node_output | mission_input }
  output_contract: { name, schema_ref, required }
  verification_gates[]
  parallelism?, loop_governors?, cycle_governors?, trigger?: EventTriggerSpec
  mission_control_access: none | peek | peek_and_command | peek_command_spawn
  spawn_grant?: { max_depth, max_children, budget_share, ops[] }
  continuation?, prompt_cache?, memory?, file_edit?                     // per-node overrides
  subgraph?: { nodes[], edges[], concurrency }
}
```

Per-node overrides win over mission defaults. Anything omitted at `simple` binding level is materialized from the profile's baseline.

---

## 7. Compiler, revisions, and evolution

### 7.1 Intake

1. **Describe / explore** — coordinator reads the Composition Manifest, existing missions, KB state, artifacts.
2. **Draft** — durable draft revision (append-only).
3. **Validate** — deterministic, no remote agents: schema, references, strategy governors, cycle admission, capability admission, credit-lane consistency, budget feasibility, depth grants, output contracts, verification policy completeness.
4. **Dry-run review** — required when cost/risk policy says so; produces a `REVIEW` human task with the compiled graph.
5. **Commit** — canonicalize, hash (`definition_digest`, `graph_digest`), store revision + compiled graph + work items in one Postgres transaction; snapshot inputs.
6. **Start** — outbox → Temporal `client.workflow.start` with `workflowId = mission:<id>` and `REJECT_DUPLICATE`, or Eve/Vercel binding for Eve-only missions.

Planning is probabilistic. Validation and compilation are deterministic. Execution starts only from a committed digest.

### 7.2 Evolution law

Workflows are evolvable; every accepted program is immutable.

A `GraphMutationSet` is proposed by a coordinator, a reconciler, or an authorized human. Mission Control validates policy, budget, depth, grants, and structural invariants, then appends a new `MissionRevision` and `CompiledGraph`. The new epoch starts; in-flight work finishes under the old epoch or is cancelled/superseded with a recorded reason.

| Mutation | Meaning |
|---|---|
| `ADD_NODE` / `ADD_EDGE` | Append future work |
| `FAN_OUT` / `FAN_IN` | Add parallel siblings or an aggregator |
| `SUPERSEDE` | Mark a not-yet-accepted node obsolete; keep history |
| `REWIRE_FUTURE` | Change edges among nodes not yet started |
| `FORK_BRANCH` | Alternate strategy for the same objective |
| `SPAWN_MISSION` | Promote a node into a child mission |
| `CLOSE_BRANCH` | Stop a branch with terminal evidence |
| `SET_GOVERNOR` | Tighten a loop/cycle/budget governor (loosening requires `POLICY_OVERRIDE`) |
| `ARM_TRIGGER` / `DISARM_TRIGGER` | Add or remove an `EVENT_TRIGGER` node |

Completed history never changes. Lineage edges (`supersedes`, `derives`, `USED_AS_INPUT_TO`) bind epochs.

**Agent-initiated mutation** is allowed when `mission_control_access ≥ peek_and_command` and `human_controls.agent_mutation = allowed | allowed_below_cost(X)`. Otherwise the proposal becomes a `REVIEW` human task.

### 7.3 Reconciliation triggers

Stage boundaries, budget thresholds, repeated failure, evaluator rejection, operator request, material new evidence, compaction, agent-declared uncertainty or decomposition threshold. The reconciler receives compact peek state, never raw transcripts.

---

## 8. Discovery and introspection (the Composition Manifest)

The coordinator must be able to ask, before drafting, exactly what it can compose from. This is a read surface; it grants nothing.

```text
GET /v1/system/describe  →  CompositionManifest@1 {
  contract_versions: { mission_definition, compiled_graph, command, event, peek_view, capability_descriptor }
  strategies[]: { kind, governors_schema_ref }
  commands[]: { kind, target_scopes[], delivery_semantics_by_runtime: { <agent_runtime_kind>: <DeliverySemantics> } }
  runtimes[]: { agent_runtime_kind, available: boolean, orchestrators[], compute[], model_access[], credit_account,
                supports: { queue, interrupt, pause, snapshot, inspect, continuation, subagents, hooks, webhook } }
  models[]: { id, provider, credit_account_kind, context_window, cache_support, availability }
  compute_environments[], sandboxes[]
  credit_accounts[]: { kind, remaining, reserved, currency }
  capability_profiles[], environment_profiles[]
  capability_summary: { counts_by_kind, search_endpoint }
  templates[], exemplars[]
  verification_kinds_admitted[]                  // from Knowledge Services catalog, not assumed
  policy_limits: { soft_depth, hard_depth, max_children_per_node, max_descendants, max_active_missions, max_parallel_per_harness }
  human_controls_available[]
}
```

Companion endpoints: `POST /v1/catalog/search` (semantic + tags + interface + cost), `GET /v1/catalog/capabilities/{id}`, `GET /v1/profiles/capability`, `GET /v1/profiles/environment`, `GET /v1/runtimes`, `GET /v1/schemas/{name}`, `GET /v1/templates`, `POST /v1/templates/{id}/expand`, `GET /v1/exemplars`.

### 8.1 Authority rule

Discovery is open. Authority is closed.

- Catalog search and `npx skills find <query>` produce **candidates**.
- Mission Control **admits** exact versions into a profile for that revision.
- A specialization may tighten a profile. Adding authority requires policy or human approval.
- Runtime MCP discovery cannot expand the committed allowlist.
- Coordinator-written hooks and middleware are artifacts: scanned, hashed, admitted, then materialized.

---

## 9. Capabilities and the harness

The harness is the unit of agent power: **MCP + agent skills + CLIs + prompts + sandbox + workspace + hooks + models**.

```text
CapabilityDescriptor@1 {
  capability_id, version
  kind: AGENT_SKILL | MCP_SERVER | MCP_TOOL | CLI | CUSTOM_TOOL | BROWSER | FILESYSTEM | SANDBOX | DATABASE |
        MODEL | DETERMINISTIC_EXECUTOR | TOOL_MIDDLEWARE | SUBAGENT | MEMORY | CONTINUATION | PROMPT_CACHE |
        FILE_EDIT | MISSION_CONTROL | WALLET
  interface_schema, requirements[], scopes[]
  secrets: SecretRef[]                  // references only
  cost_model?, policy_tags[]
  compatibility: { runtimes[], orchestrators[], compute[] }
  source: catalog | skills_find | authored
  lifecycle: draft | candidate | active | deprecated | retired      // existing capability_version.lifecycle
}
```

Lifecycle: **Discover → Resolve → Admit → Materialize → Bind → Observe → Revoke.**

Materialization writes exact skill directories (`.agents/skills/`, plus host overlays), MCP config, CLI binaries, instruction bundles, hooks files (`.cursor/hooks.json`, `.claude/settings.json` hooks, Eve `hooks/`), and frozen inputs into the workspace. Digests are verified before launch. Auto-discovered subagents or skills not in the profile fail admission.

### 9.1 Vendor middleware is projected, not hidden

Deep Agents `FilesystemMiddleware`, `SubAgentMiddleware`, `HumanInTheLoopMiddleware`, summarization, backends (`StateBackend`, `StoreBackend`, `FilesystemBackend`, `CompositeBackend`, sandbox backends); Eve tools/skills/connections/subagents; Cursor hooks and subagents; OpenAI `apply_patch` — each is a catalog row with a kernel concept beside it and an adapter behind it. Admission still applies.

### 9.2 Wallets and service auth (reserved)

`WALLET` capability kind, `credit_account`, `CREDENTIAL` human tasks, spend limits, revocation at next safe boundary. No secret values in prompts, configs, events, or artifacts. Enabled in north-star phase 4.

---

## 10. Execution lanes and the harness contract

### 10.1 `AgentHarness`

```text
AgentHarness {
  prepare(spec, workspace, capabilities) -> HarnessExecutionRef
  start(ref, initial_context) -> Stream<HarnessEvent>
  queue_instruction(ref, instruction) -> DeliveryReport
  interrupt_and_instruct(ref, instruction, policy) -> DeliveryReport
  pause(ref, safe_point_policy) -> DeliveryReport
  resume(ref, instruction?) -> DeliveryReport
  snapshot(ref) -> WorkspaceSnapshot
  inspect(ref, query) -> InspectionResult
  request_continuation(ref) -> ContinuationCheckpoint
  cancel(ref, reason) -> DeliveryReport
  observe(ref, cursor) -> Stream<HarnessEvent>       // recovery path; never the only path
}
```

### 10.2 Delivery semantics (verified 2026-09-07)

Every `DeliveryReport` carries one of:

`turn_boundary_guaranteed` · `cooperative_inject` · `cancel_and_replace` · `wait_then_send` · `pause_at_tool_gate` · `unsupported`

| Runtime | `queue_instruction` | `interrupt_and_instruct` | `pause` | `cancel` | `resume` | `snapshot` | hooks | subagents | completion signal |
|---|---|---|---|---|---|---|---|---|---|
| `cursor_cloud` | `wait_then_send` (active run → `409 agent_busy`; adapter waits or cancels) | `cancel_and_replace` (`run.cancel` → `agent.send`) | `unsupported` (emulated: cancel + checkpoint) | native `run.cancel` / REST cancel | native `Agent.resume(bc-…)`; re-pass MCP | git branch/PR state per agent (not per run) | project `.cursor/hooks.json` (command hooks only; no `sessionStart/End`, no MCP hooks) | native, depth 2 | poll `GET run`; v0 webhook `statusChange` (`FINISHED` / `ERROR`); v1 webhooks pending |
| `cursor_local` | `wait_then_send` | `cooperative_inject` via `run.steer` (`complete_delivered` or `revert_to_followup`) | `unsupported` (emulated) | native | native | filesystem + git | full `.cursor/hooks.json` incl. `preCompact`, `stop`, subagent events | native | stream + `run.wait()` |
| `claude_agent_sdk` | `turn_boundary_guaranteed` (streaming-input queue) | `cancel_and_replace` (`interrupt()` + raw `cancel_queued` where CLI ≥ 2.1.219; otherwise queue survives) | `pause_at_tool_gate` (`PreToolUse` → `defer`) | emulated (interrupt + process abort) | native `resume` / `forkSession` / `resumeSessionAt` | `rewindFiles` with file checkpointing; fork | SDK hooks (`PreToolUse`, `PostToolUse`, `Stop`, `PreCompact`, `SubagentStart`…), `canUseTool`, settings hooks | native `agents` | stream `result` |
| `codex_app_server` | `turn_boundary_guaranteed` (next `turn/start`) | `cooperative_inject` via `turn/steer` (same turn); `cancel_and_replace` via `turn/interrupt` + `turn/start` | `pause_at_tool_gate` (approval policy) | native `turn/interrupt` | native `thread/resume` / `thread/fork` | `thread/compact`, `thread/fork`, `thread/revert` | `hooks/list` session hooks; approvals | native Multi-Agent (children reject direct steer) | notifications `turn/completed` |
| `codex_sdk` (TS) | `turn_boundary_guaranteed` after completion only | `unsupported` | `unsupported` | `unsupported` (process kill) | native `resumeThread` | — | config only | — | JSONL events |
| `eve` | `turn_boundary_guaranteed` (`turnPolicy: "queue"`) | `cancel_and_replace` (`turnPolicy: "steer"`, new `turnId`) | `pause_at_tool_gate` (approval / Workflow `createHook`) | native session cancel (`turnId`, `tasks`) | native durable `sessionId`; `compact` / `clear` | Workflow event log + sandbox FS | observe-only `defineHook`; context via `defineInstructions` / `defineDynamic` | native declared + built-in `agent` | stream `turn.completed` / `session.waiting`; Agent Runs observability |
| `workflow_agent` | `turn_boundary_guaranteed` (next `stream()` after park) | `pause_at_tool_gate` + `resumeHook` | native `createHook` park | native run cancel | native run replay | event log | `needsApproval`, hooks | composed | `run.returnValue`; custom completion step |
| `deep_agents` | `turn_boundary_guaranteed` (next invoke when idle) | `unsupported` | `pause_at_tool_gate` (`interrupt_on`) | emulated (LangGraph cancel) | native checkpointer + `thread_id` | backend state | middleware `before_tool` / `after_tool` | native `task` | graph terminal |
| `direct_model` | `turn_boundary_guaranteed` | `cancel_and_replace` | n/a | native abort | n/a (stateless; kernel holds messages) | n/a | kernel-side | kernel-side | response |
| `deterministic_executor` | n/a | n/a | n/a | native | n/a | receipts | n/a | n/a | receipt |

The dashboard and the skill always show the delivered semantics. "Best-effort interrupt" and "guaranteed turn-boundary delivery" are never conflated.

### 10.3 Temporal kernel

```text
MissionWorkflow (workflowId = mission:<id>)
  ├─ Update  `command(Command)`           validator: grant + scope + state; returns { accepted, queued, delivery_hint }
  ├─ Update  `applyRevision(revision_id)` new epoch; supersede in-flight per policy
  ├─ Signal  `worldEvent(Event)`          for EVENT_TRIGGER nodes and child-mission notifications
  ├─ Query   `peek(PeekQuery)`            compact: head revision, active nodes, blockers, budget, last N events cursor
  └─ children: NodeWorkflow per released node (ABANDON parent-close policy for MISSION_SPAWN portals)
       └─ HarnessExecutionWorkflow per attempt
            activities: prepare, start, deliver(command), observe(cursor, heartbeat), snapshot, continuation, cancel
```

Rules:

- Workflows perform no I/O; activities do. History carries IDs, digests, compact outcomes.
- Commands arrive as **Updates** with `updateId = command_id` (idempotent per run). The Postgres command ledger dedupes across Continue-As-New.
- `peek` is a Query for live state; the Postgres read model serves history and cross-mission views.
- Continue-As-New on `workflowInfo().continueAsNewSuggested` after `condition(allHandlersFinished)`. Never from a handler.
- Worker Versioning: `PINNED` for `MissionWorkflow`; upgrade at Continue-As-New. Patching (`patched` / `deprecatePatch`) is the fallback only.
- Cancellation: `CancellationScope.nonCancellable` for cleanup (cancel harness, write receipt).
- Task queues: `mission-control`, `cursor`, `claude-code`, `codex`, `eve`, `browser-test`, `ingestion`, `verification`, `artifact`, `notifications`.
- `signalWithStart` is used for `worldEvent` on a mission that may be dormant; Update-with-Start is not relied on for atomic init.

### 10.4 Eve + Vercel Workflow lane

Temporal remains the top-level orchestrator when a mission spans lanes. Inside the lane:

- `start(missionNodeWorkflow, [args])` from an Activity; `runId` persisted on `harness_execution`.
- Completion: the Vercel workflow's final `"use step"` calls `POST /v1/internal/async-complete` with the Temporal task token (`CompleteAsyncError` pattern). Fallback: a heartbeating Activity awaits `getRun(runId).returnValue`. `getRun` is by `runId` only; Mission Control owns the business-key mapping.
- Commands into the lane: `resumeHook(token)` for parked agents; Eve `turnPolicy` for live sessions.
- Retry ownership: Temporal retries start/observe/complete-callback; Vercel retries inner steps; deterministic effects use receipts and are never double-retried.
- Eve-only missions (no Temporal) are permitted for the dashboard coordinator chat and short pipelines; they still write the same Postgres ledger and are reconciled by Mission Control.

### 10.5 Claude Code and Codex lanes

First-class. Adapter over Claude Agent SDK (streaming input mode) and Codex app-server JSON-RPC (not the TS SDK, which lacks steer/interrupt). Each wrapped in `HarnessExecutionWorkflow`. If a deployment cannot run Temporal around a CLI, the CLI still speaks the Mission Control protocol (hydrate, heartbeat, finish, peek) and Mission Control reconciles — degraded lane, not the architecture.

### 10.6 Continuation vocabulary

`CONTINUE` (same execution, new session via `ContinuationCheckpoint`) · `RETRY` (same node, new attempt) · `RERUN` (new execution from chosen inputs) · `FORK` (new branch from checkpoint/artifacts) · `RECONCILE` (new revision).

---

## 11. Intervention, peek, human tasks

### 11.1 Commands

| Command | Target scopes | Semantics |
|---|---|---|
| `QUEUE_INSTRUCTION` | node, execution, session | Durably enqueue for the next safe turn boundary; must enter the next context before the agent continues |
| `INTERRUPT_AND_INJECT` | execution, session, turn | Stop/steer the current run per runtime support; inject context; attach artifacts (failed E2E trace, evidence packet) |
| `PAUSE` | mission, branch, node, execution | Stop scheduling; pause after the configured safe boundary |
| `HARD_PAUSE` | mission, branch, node, execution | Aggressive stop; confirm when side effects may be partial; snapshot |
| `CANCEL` | mission, branch, node, execution | Terminal with reason; cleanup in non-cancellable scope |
| `RESUME` | mission, branch, node, execution | Continue, optionally with instruction, capability, or budget change (budget increase requires grant) |
| `RETRY` / `RERUN` / `FORK` | node, execution | As §10.6 |
| `RECONCILE` | mission, branch | Ask coordinator/reconciler for a `GraphMutationSet` |
| `PEEK` | any | Read-only bounded view (§11.3) |
| `SPAWN` | node | Create child mission from output artifacts within grant |

Command state machine is durable (`accepted → queued → delivered → observed → completed | failed | rejected | expired`). Browser or MCP disconnect never loses a command. Idempotency key `command_id`.

### 11.2 Delivery report

```text
DeliveryReport@1 { command_id, runtime, semantics: DeliverySemantics, delivered_at?, observed_at?, native_ref?, note }
```

Shown in dashboard, returned by `missionctl intervene`, and included in the skill's tool result.

### 11.3 Peek view

```text
PeekView@1 {
  mission: { id, status, depth, root, parent?, active_revision, budgets_by_account }
  graph_head: { nodes: [{ key, strategy, status, attempt_no, runtime, blockers[] }], edges: compact }
  active_executions[]: { node, attempt, harness_execution, native_ref, last_event_at, delivery_capabilities }
  blockers[]: human_tasks, awaiting_events, budget_holds
  recent_events[]: last N with cursor
  artifacts: inventory (ids, kinds, digests, sizes) — never bodies
  children[]: portal summaries
  verification: dispositions by node
}
```

Peek returns pointers and diffs, never file bodies or transcripts. `subtree`, `filters`, `after_cursor` bound the response.

### 11.4 Human tasks

`APPROVAL` (merge, publish, DB mutation, expensive spawn, capability escalation) · `QUESTION` · `SELECTION` · `REVIEW` (report, diff, preview, evidence sample, dry-run graph) · `CREDENTIAL` · `POLICY_OVERRIDE` (depth, budget, blocked action).

Approval ≠ human input ≠ intervention. A queued message cannot smuggle a policy change.

### 11.5 Inter-agent communication law

No child→parent token stream. Children write artifacts and status; parents peek. Pointers by default. Scoped tokens see their subtree. Commands are the only writes. Peek is bounded and coalesced. The spawning agent is the local coordinator at every depth. No second message bus; shared intermediates are artifacts.

---

## 12. Continuation, compaction, prompt cache

### 12.1 `ContinuationPolicy`

```text
ContinuationPolicy@1 {
  triggers: { context_ratio: 0.60, token_budget_remaining, tool_payload_bytes, subagent_return, turn_count, operator_or_reconcile, cache_breakpoint }
  algorithms: [session_compact, tool_offload, subagent_pointers, graph_digest, memory_promotion, cache_preserving_compact]
  compacting_agent: { runtime, model, credit_account }     // read session + workspace; write only the checkpoint
  checkpoint_schema: ContinuationCheckpoint@1
  preserve_cache_prefix: boolean
  max_continuations_per_execution
  fail_if_checkpoint_invalid: true
}
```

`ContinuationCheckpoint@1`: objective state, decisions, completed artifact refs, workspace snapshot / sandbox ref, open questions, pending tool or human actions, event cursor, next actions, invariants, verification state, compact summary. Stored in `orchestration.continuation_checkpoint` with `verification_status`.

Vendor-native compaction (Claude auto-compaction + `PreCompact` / `PostCompact`, Codex `thread/compact`, Eve `compact`, Cursor `preCompact`, Deep Agents summarization) is observed and recorded as a `CONTINUE` when it fires, and may be requested by the adapter when supported.

### 12.2 `PromptCachePolicy`

Stable prefix (system, tools, skill list, workspace header, mission IDs) byte-identical across turns of an epoch; explicit breakpoints; queued instructions append after the breakpoint; adapters translate (Anthropic `cache_control` with `ttl: "5m" | "1h"`, OpenAI prompt-cache keys, AI SDK `providerOptions.anthropic.cacheControl`); cache-hit/miss tokens recorded per `provider_run`; no secrets in cached prefixes.

### 12.3 File editing outside Cursor

`FILE_EDIT` capability with strategies `apply_patch` (OpenAI V4A `create_file` / `update_file` / `delete_file`, `apply_patch_call_output`), `cursor_native`, `search_replace`, `whole_file` (last resort). Sandbox applies the structured edit; result returns as a tool result; conversation stores the patch artifact digest.

---

## 13. Proof and verification

Two families, one law: **no trusted downstream use without an independent check.**

### 13.1 Code — tests as proof

`VerificationPolicy` options, composable per node: unit, typecheck/lint, E2E (Playwright) against a preview, mobile/desktop launch smoke, evaluator score against a rubric version, human review of a preview. Artifacts: test report, trace, screenshot, deployment URL, coverage, evaluator result. A failed E2E artifact is attachable to `INTERRUPT_AND_INJECT`. Explicit `waived_proof` is recorded, visible, and requires `APPROVAL`.

### 13.2 Research — `verification.v1`

Knowledge Services owns `@aiengineer/knowledge-verification`. Mission Control dispatches; agents never copy the engine.

Use cases admitted in the KS contract: `captureSource`, `parseArtifact`, `extractStructuredData`, `verifyExtraction`, `verifyClaims`, `verifyReport`, `verifyMetricObservation`, `runBenchmark`, `compareBenchmarkRuns`, `replayRun`, `inspectAuditBundle`, `requestAdjudication`. These are **catalog-admitted capabilities**, present only when the KS deployment admits them; the Composition Manifest reports the live set.

Five ordered questions: capture integrity → selector integrity → mechanical correctness → semantic support → policy admission. Later stages cannot reverse an earlier deterministic failure. Orthogonal properties are stored separately.

Outcome mapping (already implemented in the service repo's `verification-dispatch`): KS policy `pass | pass_with_warnings` → `succeeded`; `review | abstain` → `review_required`; `fail` → `quality_rejected`; forged/unmatched → `reconciliation_unresolved`. Quality rejection is **execution success with a rejecting disposition** and is never retried as infrastructure failure.

Dispatch rules (proved locally in WS-10): workflow performs no I/O; activities submit and poll; credentials, bytes, and full receipts stay out of history; cancellation reaches the KS operation; producer and verifier deployments are distinct; `OperationContext` carries `tenantId`, `missionId`, `workItemId`, `attemptId`, `idempotencyKey`, `externalExecution.runtime = "mission_control"`.

### 13.3 Attribution

`Source → Capture → Selector → Fragment → EvidenceEdge → Assertion / Field`. Machine selectors, not display excerpts. Missing or ambiguous locators fail closed. Semantic judges see only mechanically authorized fragments.

### 13.4 Experiments

Fourth goal family: `harness_comparison`, `model_comparison`, `adversarial`, `compaction_comparison`, `prompt_cache_ablation`, `eval_benchmark`. Fan-out arms as `PARALLEL_SWARM`, freeze inputs, run, verify, compare, seal via `evaluation.*`. LLM-as-judge is one typed arm with rubric version and recorded judge identity; never the sole verifier of code or extraction.

---

## 14. Research and ingestion module

One-sentence rule: persist every research artifact immediately into its immutable or provisional layer; promote only independently verified atomic outputs into canonical knowledge, retrieval, ranking, and curriculum.

**Six zones** (0 capture → 1 starter/proposal → 2 mission ledger → 3 canonical identity → 4 verified facts → 5 product projection). Exploratory retrieval may index zones 1–2 only as `internal_exploratory`, structurally excluded from public answers.

**Four clocks:** publication, valid/effective, observation/capture, system/admission. **Perspectives:** `speaker_time`, `historical_as_of`, `interval_development`, `current_state`. New facts supersede; they never mutate.

**Gap planning:** every research mission starts from an immutable `mission_input_snapshot`. Idempotency includes subject, question hash, window, snapshot hash, policy version. If another mission filled the gap, admission is `no_op_duplicate` or merge.

**Promotion:** ordered, not score-bypassable — `lineage → identity → mechanical → semantic → policy → snapshot freshness`. Outcomes `admitted | review_required | held | quarantined | no_op_duplicate | superseded`. Mission Control computes the proposal disposition; the KS `promotion_decision` (human or `knowledge_api`, never a model, never the author) remains the authority for `official_canonical` publication. Agents author ingestion **intent**; only the `ingestion_executor` identity (a Mission Control deterministic executor) invokes admitted adapters via intent → approval → idempotent receipt.

Split the words: **execution success** (every required output has a disposition) ≠ **admission success** (each atom cleared promotion) ≠ **product publication**.

**Downstream projections:** retrieval returns evidence packets, never naked hits; ranking policies are separate and "missing is not zero"; curriculum units publish when their dependency graph is admitted; snippet assurance `discovered → inspected → runnable → curriculum_ready`.

---

## 15. Knowledge plane integration

- Retrieval: `POST /v1/retrieval-runs` with `RetrievalPlan`; client embeddings rejected; `EvidencePacket` returned. Honest production limits: no graph expansion / anchors / soft boosts / upper temporal bounds / context expansion (fail closed); requested spaces must have an active publication or retrieval abstains. Mission Control calls through `@aiengineer/knowledge-client` or KS MCP (`retrieval.search`, `retrieval.explain_run`, `retrieval.build_evidence_packet`) with `missionId` / `workItemId` / `externalExecution`.
- Ingestion chain: source discovery/resolution, capture, transformation, chunking, promotion proposal/decision, embedding, space publication/rollback, vector-store create/documents/ingestion, evaluation. Processing success never grants publication authority.
- A2A: KS already exposes `POST /v1/a2a/tasks` for `document_preparation`, `vector_store_ingestion`, `retrieval`, `evidence_packet_construction`, with HMAC callbacks. Mission Control adds its own A2A task kinds (§17.6) and does not invent a second bus.
- Dashboard Knowledge search is federated: missions/revisions/nodes/commands (Mission Control), entities/sources/captures/reports/claims (db-contract), evidence packets and vector hits (KS retrieval), verification runs/cases/findings (KS reads), artifacts and lineage (shared). Any result → **Use as Mission Input** with `USED_AS_INPUT_TO` lineage.

---

## 16. Goal families

**Application creation** — `specify → scaffold (cursor | claude | codex) → implement → unit + typecheck → preview deploy → E2E / mobile / desktop smoke → evaluator or human review → EVENT_TRIGGER on commit / failed proof → improve`. Real repos; agents test the project; handshake from Cursor / Claude Code / Codex / Cowork back into Mission Control is part of done.

**Course / challenge / project** — `gap snapshot → research / verify → admit facts → compose unit → pin examples at runnable+ → evidence-packet gate → publish versioned unit`. User-authored units use `user_managed` authority on the same pipeline.

**Experiment** — §13.4.

**Recursive self-improvement** — event policy: after a major commit, merged PR, failed verification, new upstream release, or `revalidation_due`, if budget and an open improvement objective remain, arm/release an optimizer node; require proof before accepting the new head.

**Personal intelligence platforms** — tenancy and store-class expansion: `user_managed` spaces, user budgets / future wallet, user-granted verifier profiles, user-authored curriculum. Same kernel, same promotion rules.

---

## 17. Public contract

### 17.1 HTTP API (canonical)

```text
# discovery
GET    /v1/system/describe
POST   /v1/catalog/search
GET    /v1/catalog/capabilities/{id}
GET    /v1/profiles/capability | /v1/profiles/environment
GET    /v1/runtimes
GET    /v1/schemas/{name}
GET    /v1/templates · POST /v1/templates/{id}/expand · GET /v1/exemplars

# authoring
POST   /v1/missions                                  (draft)
POST   /v1/missions/{id}/revisions
POST   /v1/missions/{id}/revisions/{rev}:validate | :compile | :commit
POST   /v1/missions/{id}:start
POST   /v1/missions/{id}/reconcile                   (GraphMutationSet → new revision)
POST   /v1/artifacts/{id}/compose-mission

# steering
POST   /v1/missions/{id}/commands                    (idempotent by command_id)
GET    /v1/missions/{id}/commands/{command_id}
POST   /v1/missions/{id}/spawn
GET    /v1/human-tasks · POST /v1/human-tasks/{id}:answer

# reading
GET    /v1/missions · GET /v1/missions/{id} · /summary · /graph?revision=&subtree= · /events?after= · /artifacts · /peek · /view
GET    /v1/executions/{id}/inspect
GET    /v1/artifacts/{id} · /lineage
GET    /v1/credit-accounts

# streams and internal
WS     /v1/stream
POST   /v1/internal/async-complete                   (Vercel lane → Temporal task token)
POST   /v1/internal/harness-events                   (CLI-lane handshake: hydrate, heartbeat, finish)
```

Mutations require an idempotency key. Errors are Problem+JSON. Knowledge and verification stay on KS `/v1/*`; Mission Control links by ID.

### 17.2 `missionctl`

JSON on stdout, human text on stderr, `--json` default when non-TTY, `--apply` for mutations, idempotency keys required.

Groups: `describe`, `catalog`, `profiles`, `runtimes`, `schema`, `template`, `exemplar`, `draft`, `validate`, `compile`, `commit`, `run`, `status`, `graph`, `peek`, `view`, `events`, `artifact`, `approval`, `input`, `intervene` (`queue` | `interrupt` | `pause` | `hard-pause` | `cancel` | `resume` | `retry` | `rerun` | `fork`), `reconcile`, `spawn`, `skills-find`, `test`.

### 17.3 MCP server

Protocol `2026-07-28` with `2025-11-25` dual-era. Stateless Streamable HTTP. Tools are task-oriented and scoped by token:

`mission_describe_system`, `mission_catalog_search`, `mission_draft`, `mission_validate`, `mission_commit`, `mission_start`, `mission_peek`, `mission_graph`, `mission_events`, `mission_artifacts`, `mission_artifact_get`, `mission_command` (queue | interrupt | pause | resume | cancel | retry | rerun | fork), `mission_reconcile`, `mission_spawn` (when granted), `mission_human_tasks`, `mission_compose_from_artifact`. Existing `mission_service_status` remains.

Ordinary agent tokens cannot approve, admit capabilities, execute ingestion, or raise hard caps.

### 17.4 MCP Apps

Predeclared `ui://mission-control/...` resources with `mimeType: text/html;profile=mcp-app`; tools reference them via `_meta.ui.resourceUri`; host capability `extensions["io.modelcontextprotocol/ui"]`. Regions: topology, event tail, command composer, output rail, human-task cards, compose-from-artifact. Every tool also returns plain `content` for non-UI hosts. A Cursor / Claude / Eve session that adopted the skill operates a mission without opening the website.

### 17.5 Agent Skill

`SKILL.md` per the agentskills.io spec (`name`, `description`, optional `license`, `compatibility`, `metadata`, `allowed-tools`). Host-specific flags (`disable-model-invocation`, Codex `agents/openai.yaml`) live in overlays. Teaches when to activate (decomposition, durability, spawn, proof), how to call `missionctl` / MCP, and how to read delivery semantics. Versioned and tested against the API contract. Prose cannot override policy.

### 17.6 A2A 1.0

Agent Card with `supportedInterfaces[]`, `capabilities.streaming` and `pushNotifications`, signed card. Task kinds: `mission_start`, `mission_peek`, `mission_command`, `compose_mission_from_artifacts`, `mission_spawn`. Lifecycle `TASK_STATE_SUBMITTED → WORKING → (INPUT_REQUIRED | AUTH_REQUIRED) → COMPLETED | FAILED | CANCELED | REJECTED` mapped from mission/command state. Same application services as HTTP.

### 17.7 Plugin packaging

One repository, one payload, three manifests:

| Target | Manifest | Ships |
|---|---|---|
| Agent Plugin (portable) | `plugin.json` + `mcp.json` | skills (`.agents/skills/mission-control/…`) + MCP server config |
| Cursor | `.cursor-plugin/plugin.json` (+ `marketplace.json`) | skills, MCP, rules, `hooks/hooks.json` (handshake hooks: `stop`, `afterFileEdit`, `subagentStop` → Mission Control events), variables (`MISSION_CONTROL_URL`, `MISSION_CONTROL_TOKEN`) |
| Claude Code | `.claude-plugin/plugin.json` (+ `marketplace.json`) | skills, MCP, `hooks/hooks.json` (`Stop`, `PostToolUse`, `SubagentStop`) |
| Codex | skills + `agents/openai.yaml` sidecar; MCP via `config.toml` | — |
| Eve | `agent/skills/`, `connections/` (MCP) | — |

Published as `@aiengineer/mission-contracts`, `@aiengineer/missionctl`, `@aiengineer/mission-control-skill`, `@aiengineer/mission-control-plugin`. Installing the plugin endows any agent system with describe / draft / run / peek / steer / spawn within its token's grant.

---

## 18. Dashboard — debugger and coordinator chat

### 18.1 Information architecture

Missions · Live Mission · Coordinator · Outputs · Human Tasks · Knowledge (federated) · Capabilities · Workers (Temporal queues, Vercel workflow health, adapter health) · Budgets (per credit account) · Audit / Provenance.

### 18.2 Live Mission

Health bar over four zones: topology (tree / DAG / outline with revision overlays and portal nodes for children), live coalesced events (families: temporal, vercel_workflow, harness, tool/MCP/CLI, git, test/browser, research/ingestion, verification, human, graph_mutation, artifact), inspector + command composer (with delivery semantics preview per runtime), output rail (artifacts → compose mission). Breadcrumbs `Root > Node 4 > Child Mission > Node 2`. Coalescing is presentation only.

### 18.3 Coordinator chat

Vercel AI SDK chat (`WorkflowAgent` or AI SDK agent) bound to a mission or "new mission". Tool calls are Mission Control and Knowledge Services tools; tool results are the same event fabric. Attach artifacts, evidence packets, verification runs, failed tests. Approve or reject dry-run graphs. Speak during a run; the coordinator peeks and issues commands through the API.

### 18.4 Transport

```text
Client → WS Gateway: subscribe { mission_id, subtree?, filters?, after_cursor? }
Gateway → Client:    MissionEvent | EventAggregate | ArtifactNotification | StatePatch | ChatPart | DeliveryReport
Client → Mission API: command | coordinator message
```

Commands never live only in WebSocket state. Reconnect replays from cursor. Presence shows other operators.

### 18.5 Roles

`Viewer` < `Operator` < `Mission Admin` < `Platform Admin`. High-impact actions confirm and may require second approval.

### 18.6 Proof of lifecycle (the acceptance scenario)

The control plane is proven when, on a running mission built and started by a coordinator agent, an operator can from the dashboard and an agent can from the skill (within policy):

1. `PEEK` the graph, active executions, blockers, budgets, and last events.
2. `QUEUE_INSTRUCTION` and see `turn_boundary_guaranteed` or `wait_then_send` reported and later `observed`.
3. `INTERRUPT_AND_INJECT` with an attached artifact and see the reported semantics (`cooperative_inject` or `cancel_and_replace`).
4. `PAUSE`, then `RESUME` with an instruction.
5. `HARD_PAUSE` / `CANCEL` with confirmation and a workspace snapshot.
6. `RETRY` a failed node, `RERUN` from chosen inputs, `FORK` a branch.
7. `RECONCILE`: propose `ADD_NODE` + `ADD_EDGE`, see a new revision, and see the old epoch preserved.
8. Answer a `HumanTask` and see the gated node release.
9. Watch a `MISSION_SPAWN` portal open a child and peek into it.
10. See proof dispositions gate acceptance and an `EVENT_TRIGGER` arm an improvement round.

---

## 19. Storage and schema plan

### 19.1 Reuse (no duplicates)

`orchestration.mission`, `work_item`, `work_item_dependency`, `work_item_event`, `attempt`, `agent_session`, `continuation_checkpoint`, `artifact`, `artifact_manifest`, `artifact_lineage`, `capability`, `capability_version`, `capability_profile`, `capability_profile_item`, `provider_route`, `operation_intent`, `operation_receipt`, `outbox_event`; `evaluation.review_task` / `review_decision`; `retrieval.content_promotion_proposal` / `content_promotion_decision`; `knowledge_service.operation` with `ownership_mode = 'mission_control'`; `observability.trace` / `usage_rollup`.

### 19.2 Additive gaps to land in `ai-engineer-db-contract`

Enums: `agent_runtime_kind`, `orchestrator_kind`, `model_access_kind`, `compute_environment_kind`, `credit_account_kind`, `node_strategy`, `edge_kind`, `command_kind`, `command_state`, `delivery_semantics`, `revision_status`, `human_task_kind`, `human_task_state`, `binding_level`.

Tables (schema `orchestration` unless noted): `mission_revision`, `compiled_graph`, `mission_node`, `mission_edge`, `mission_command`, `human_task`, `harness_execution`, `provider_run`, `environment_profile`, `credit_account`, `credit_ledger_entry`, `mission_input_snapshot`, `spawn_grant`; later `memory_item`, `evidence.source_encounter`, `research.promotion_proposal`, `research.temporal_context_window`, `research.report_assertion`, `evidence.claim_temporal_scope`, `ranking.computation_ledger`, `research.study`.

Column extensions: `mission` (`root_mission_id`, `parent_mission_id`, `parent_node_id`, `depth`, `active_revision_id`, `goal_spec`, `policy`), `work_item` (`node_id`, `revision_id`, `epoch`, `strategy`), `attempt` (five execution dimensions, `harness_execution_id`), `mission_event` (`seq`, `family`, `work_item_id`, `attempt_id`, `actor`, `payload`), `capability_kind` seed rows for the new kinds, `knowledge_service.operation.mission_id` FK.

Type exports: add hand-written `@aiengineer/database-contract/orchestration` (row aliases + enum unions). Apps import types only.

### 19.3 Other stores

Object storage: reports, traces, workspace snapshots, raw event archives, capability bundles. Git: code history. KS Postgres: captures, chunks, vectors, packets, verification runs. Temporal / Vercel Workflow: recovery logs only. Neo4j: optional projection, never a source of truth.

Event plane: Postgres `mission_event` + `outbox_event` + stream gateway first; dedicated stream/analytics store only when volume requires it.

---

## 20. Governors and safety

| Governor | Examples |
|---|---|
| Depth | Mission depth (soft 3 / hard 16), node nesting, native subagent depth, spawn depth |
| Concurrency | Active missions, branches, executions per harness, browser sessions, queue fairness |
| Budget | Tokens, dollars, per-credit-account remaining, wall-clock, storage, browser minutes |
| Spawn | Who may spawn, children per node, descendants, remaining budget, justification artifact |
| Capability | Allow/deny by mission, node, repo, data domain, network, secret, wallet |
| Verification | Required checks before accept / merge / publish / retrieve / teach |
| Credit lane | Cursor-credit work cannot silently spend Gateway credits, and the reverse |
| Cycle / loop | `max_visits_per_node`, `max_iterations`, `max_rounds`, `max_firings`, termination predicates |

Fail closed on broken provenance, missing published spaces, mismatched orchestration identity, quality rejection, unadmitted capability, and unknown delivery semantics.

---

## 21. Definition of done (product)

1. A coordinator can `describe` the system, then compile a multi-strategy mission (all seven strategies, subgraphs, cycles under governors) with per-node harness, skills, workspace, hooks, HITL middleware, output contracts, and binding levels.
2. Cursor Cloud/Local, Claude Agent SDK, Codex app-server, Eve, WorkflowAgent, Deep Agents, direct models, and deterministic executors run behind `AgentHarness` with normalized events and reported delivery semantics.
3. An authorized child can peek and spawn within depth and budget; children never hold provider keys.
4. A human or agent can execute the full §18.6 lifecycle and see semantics honestly.
5. A long run compacts and resumes without losing lineage or workspace identity.
6. The graph evolves by append-only revision while history stays immutable.
7. Code goals do not complete without unit and E2E (or an explicit, approved waiver).
8. Research goals do not enter canonical retrieval or curriculum without `verification.v1` + promotion decision.
9. Dashboard and MCP Apps show live topology, events, outputs, human tasks, and coordinator chat.
10. Any artifact can start a new mission with provenance.
11. Scale-out is multiple workers and queues, not one process.
12. Official and personal store classes exist on one contract; the plugin installs into Cursor, Claude Code, Codex, and Eve.

---

## 22. Explicitly rejected

- "Missions cannot compose child missions" as permanent law.
- "No runtime graph mutation" as permanent law (replaced by immutable revisions + legal mutations).
- A single `provider` field; a second mission or knowledge database; chat transcripts as receipts.
- Pretending Cursor Cloud can inject mid-run; pretending Nexus bridges to Vercel; naming "MCP-UI" as the protocol.
- Dashboard as a late optional approval inbox.
- Local Temporal as a production fallback.

---

## 23. Source map

| Source | Kept |
|---|---|
| `MISSION_CONTROL_PRESPEC.md` (2026-09-05) | Bet, invariants, entities, strategies, evolution, spawn, capabilities, lanes, commands, proof, zones, projections, public contract, dashboard, governors, gates |
| `MISSION_CONTROL_SUPPLEMENT.md` | Three-layer neutrality, credit accounts, Deep Agents projection, sandbox vs workspace, experiments, continuation algorithms, prompt cache, file edit, communication law, source intelligence, memory, shareable platform |
| Conversation 2026-09-07 | Coordinator-first framing, Composition Manifest, binding levels, contracts-first sequencing, dashboard lifecycle proof, agent-initiated mutation by policy, plugin for any agent system |
| Conversation 2026-09-07b | Workflow-type layering in the M4 cluster; `MISSION_SPAWN` as skill/plugin invocability plus optional per-node spawn grant |
| `ai-engineer-mission-control` inventory | Existing verification dispatch, `verificationWorkflow`, dispositions, API/MCP/CLI baseline, ADR 0001 |
| `ai-engineer-db-contract` inventory | Existing `orchestration.*` and inbound FKs; store-class literals; gaps |
| Knowledge Services inventory | `OperationContext`, 12 verification use cases, policy outcomes, A2A kinds, retrieval limits, promotion authority, WS-10 proofs |
| Vendor research 2026-09-07 | Cursor SDK 1.0.31 / Cloud API semantics and hooks; Claude Agent SDK streaming input / interrupt / hooks; Codex app-server `turn/steer`; Eve `turnPolicy`; Deep Agents middleware; `apply_patch`; Anthropic cache TTL; AI SDK `providerOptions` |
| Orchestrator/protocol research 2026-09-07 | Temporal TS 1.23 Update/Signal/Query, Update-with-Start caveat, Nexus scope, async completion, CAN limits, Worker Versioning; Workflow SDK 4.8.5 / 5 beta, `WorkflowAgent`; MCP 2026-07-28 + MCP Apps; A2A 1.0; Agent Skills spec; Cursor/Claude plugin manifests |
| `notes/*` and root `mission_control_*.md` | Postgres ledger, outbox, intent vs executor, approval/input/intervention split, typed failure, dimensional execution, stream gateway, command lifecycle |
| `docs/product/00-vision.md`, `11-north-star-path.md` | MCP-first surfaces, handshake, db-contract rule, phase order |
