# Mission Control Pre-Specification

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

> **Superseded 2026-09-07.** The canonical lock is [`MISSION_CONTROL_SPEC.md`](./MISSION_CONTROL_SPEC.md); the delivery order is [`MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md`](./MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md). This file is kept as a source. Do not add decisions here.

**Status:** superseded source (was: pre-specification — consolidation draft toward one canonical lock)  
**Date:** 2026-09-05  
**Product name:** Mission Control  
**Service target:** `ai-engineer-mission-control`  
**Database owner:** `ai-engineer-db-contract`  
**Knowledge / proof owner:** `ai-engineer-knowledge-services`

This file is the merged working specification. It is written at full product size on purpose. Sequencing is a delivery order, not a permission to shrink the system.

Companion (providers, credits, compaction, cache, file-edit, memory, experiments, shareable platform): [`MISSION_CONTROL_SUPPLEMENT.md`](./MISSION_CONTROL_SUPPLEMENT.md).

Until a later document is marked **canonical lock**, new Mission Control decisions land here. Do not add competing architecture to the older notes listed in §23.

---

## 0. Why this exists

The prior documents drifted across three generations:

1. An August research-ingestion operating model (when facts may become trusted knowledge).
2. A Cursor Cloud + Temporal plan-then-execute control plane (immutable DAG, no child missions in Phase 1).
3. A September general substrate (recursive spawn, evolving graphs, multi-harness, debugger dashboard).

Those are one product. This pre-spec keeps the ambitious substrate, keeps the research admission physics, and binds both to the Knowledge Services verification and retrieval planes that already exist.

The product bet is extreme decomposition with guardrails. A coordinator compiles a real execution program. Child agents may receive the Mission Control skill and spawn further work. Humans and agents can inspect and steer live work. Nothing trusted reaches search, curriculum, or a merged codebase without proof.

---

## 1. Product bet

Mission Control is the durable control plane for long-running agentic work that produces and maintains an educational / informational enterprise.

That enterprise is two nested products:

1. **AI Engineer** — official flywheel knowledge, courses, challenges, projects, search, and app services that make a person engineer-ready or entrepreneur-ready.
2. **Personal intelligence platforms** — the same contract, later offered so a user can run research, ingestion, course creation, and app-building workflows against their own knowledge spaces (`user_managed` stores, notes, and missions), not only the official KB.

Do not build a second platform. Personal spaces are a store class and tenancy/authority dimension on the same mission, artifact, retrieval, and verification contracts.

Workflows must be able to complete any of:

| Goal family | What “done” means | Proof |
|---|---|---|
| Research and ingestion | Atomic admitted facts, identities, reports, and dispositions | Knowledge Services verification + promotion protocol |
| Course / challenge / project creation | Versioned educational units whose every factual or executable dependency is admitted | Evidence packets + curriculum projection rules |
| Application creation and improvement | Running web, mobile, and desktop software that was built, tested, changed, and built again | Unit tests, E2E tests, deployment previews, evaluator loops |

Every family is the same kernel: a goal, a compiled program of work, harnessed agents, typed artifacts, proof gates, and a live debugger.

---

## 2. Design invariants

These are not optional for v1 of the *product*. A first vertical slice may implement a subset of adapters. It may not invent a weaker kernel.

1. **Sessions and provider runs are disposable. Missions are durable.** Chat text is never a receipt.
2. **One domain, many clients.** HTTP API, CLI, MCP, MCP UI, Agent Skill, A2A, web, mobile, and desktop share one application service. None of them schedule independently.
3. **Postgres is the domain ledger. Orchestrators recover execution.** Temporal history and Vercel Workflow history carry identifiers and compact outcomes, never corpora, source bytes, or secrets.
4. **New shared schema lives only in `ai-engineer-db-contract`.** Knowledge Services and Mission Control consume the pinned contract. No app-local `Database` types or second migration tree.
5. **Capabilities are admitted, versioned, and discoverable.** Agents may search semantically and may run `npx skills find <query>`. Discovery never grants authority. Admission and materialization do.
6. **Spawn is a grantable capability, not a raw provider key.** Ordinary agents never receive `CURSOR_API_KEY` or equivalent launch credentials. They call Mission Control.
7. **Proof is a required stage of goal completion.** Code uses tests. Research uses verification. Curriculum and retrieval consume admitted records only.
8. **Persist immediately. Promote atomically.** The mission is not the transaction boundary. The verified output is.
9. **Producer and verifier are different deployments.** Self-verification is rejected.
10. **Web and mobile are clients of the contract.** MCP / MCP UI / CLI / Agent Skill / A2A are designed first. Desktop is a first-class client of the same contract, not a later rewrite.
11. **Do not start a second flywheel, learner schema, or notes/KB platform.** Official and personal intelligence share the plane.

---

## 3. Architectural layers

```text
Coordinator (human or agent)     Dashboard / MCP UI / CLI / Skill / A2A
        |                                    |
        v                                    v
                 Mission Control API
        |                                    |
        +-- Compiler / policy / catalog -----+
        |                                    |
        v                                    v
   Temporal (mission kernel)          Vercel Workflow (Eve lane)
        |                                    |
        v                                    v
   Harness adapters                   Knowledge Services
   Cursor Local / Cloud               retrieval + vector stores
   Claude Code                        verification.v1
   Codex                              ingestion / publication
   Eve                                evidence packets
   Deterministic executors            promotion / receipts
   Frontier model adapters
        |
        v
   Workspaces, sandboxes, repos, browsers, tests, deployments
        |
        v
   Artifacts + provenance  -->  Postgres + object storage + Git
```

| Layer | Owns | Does not own |
|---|---|---|
| Mission Control | Mission lifecycle, compiler, policy, spawn, intervention, event gateway, dashboard contract | Verification algorithms, embedding, canonical fact physics |
| Temporal | Mission/node/harness durability for the kernel and Cursor/Codex/Claude lanes | Research corpora, UI event archives |
| Vercel Workflow | Durable steps inside the Eve / Vercel AI SDK lane | Canonical mission state |
| Harness adapters | Vendor launch, interrupt, snapshot, continuation | Graph mutation, admission, budgets |
| Knowledge Services | Capture → chunk → embed → retrieve → evidence packet; verification.v1; publication | Mission scheduling |
| db-contract | Shared schema, RLS, generated types | Runtime orchestration |
| Product clients | Render and steer | Orchestration rules |

---

## 4. Domain model

A **Mission** is the durable unit of intent. It owns the root goal, current graph revision, policies, budgets, capability grants, lineage, and status.

### 4.1 Core entities

| Entity | Meaning |
|---|---|
| `GoalSpec` | Objective, success criteria, non-goals, constraints, evidence requirements, verification requirements |
| `Mission` | Durable objective + policy envelope + execution history |
| `MissionRevision` | Append-only authored configuration. Exactly one revision is `active` for an execution epoch |
| `CompiledGraph` | Immutable, fully resolved execution program for one revision |
| `MissionNode` | One logic unit / stage / loop / swarm / spawn / evaluator. Has a strategy |
| `MissionEdge` | `requires`, `supplies`, `gates`, `evaluates`, `supersedes`, `derives` |
| `WorkItem` | Runtime instance of a compiled node |
| `Execution` / `Attempt` | One retry epoch for a work item |
| `ProviderBinding` | Attachment of a harness conversation to an attempt |
| `ProviderRun` | One prompt / CLI / SDK invocation |
| `Session` | Harness-native context window (Cursor local, Claude Code, Codex, Eve). Disposable. Used for continuation and interrupt targeting. Not a synonym for mission |
| `ContinuationCheckpoint` | Compact handoff when a session hits a context threshold |
| `Artifact` | Immutable content or manifest with digest, producer, schema, sensitivity |
| `PromotionProposal` | Atomic candidate for canonical admission |
| `Receipt` | Immutable evidence that a deterministic side effect was attempted |
| `HumanTask` | Approval, question, selection, review, credential request, policy override |
| `Command` | Durable operator or agent intervention |
| `CapabilityDescriptor` | Versioned skill, MCP, CLI, tool, browser, sandbox, model, executor, or Mission Control itself |
| `CapabilityProfile` | Allowlisted set of capability versions and limits |
| `EnvironmentProfile` | Repo, workspace, secrets-by-reference, MCP, materialization, network |
| `MissionEvent` | Normalized, attributable event |

Avoid using `session` as the mission runtime noun. Keep it for harness context windows. Knowledge Services already binds `missionId`, `workItemId`, `attemptId` on every operation.

### 4.2 Execution dimensions

Do not collapse the stack into one `provider` field.

| Dimension | Examples |
|---|---|
| `agent_runtime_kind` | `cursor_cloud`, `cursor_local`, `claude_code`, `codex`, `eve`, `direct_model`, `deterministic_executor` |
| `orchestrator_kind` | `temporal`, `vercel_workflow` |
| `model_access_kind` | `cursor_credits`, `vercel_ai_gateway`, `direct_openai`, `direct_anthropic`, `direct_other` |
| `compute_environment_kind` | `cursor_cloud_vm`, `local_sandbox`, `vercel_function`, `vercel_sandbox`, `aws_worker`, `browser_runner` |

Cursor Cloud + Temporal spends Cursor credits. Eve + Vercel Workflows + Vercel AI SDK spends frontier-provider credits. Claude Code and Codex are first-class runtimes even when Temporal wraps them for control flow.

### 4.3 GoalSpec

```text
GoalSpec {
  objective
  success_criteria[]          // machine-checkable where possible
  non_goals[]
  constraints[]
  evidence_requirements[]     // what must be pointed at
  verification_policy         // tests, verification.v1, human review
  output_contracts[]          // typed artifacts the goal must produce
}
```

A goal is not complete when an agent says so. It is complete when the verification policy accepts the output contracts.

---

## 5. Workflow types and strategies

No single strategy is the architecture. Strategies are per-node execution semantics. A coordinator may compile a twenty-node program where each node has its own strategy, harness, skills, workspace, hooks, HITL middleware, and output contract.

| Strategy | Use when | Runtime |
|---|---|---|
| `STAGE_GRAPH` | Dependencies are known | Nodes release when predicates pass. Default compiled form is a DAG. Cycles are an explicit, budgeted option |
| `GOAL_LOOP` | The route is uncertain | Observe → act → evaluate → continue until success or stop |
| `PARALLEL_SWARM` | Breadth or competing hypotheses | N bounded workers; fan-in through evaluator / coalescer |
| `DETERMINISTIC` | Mechanical work must not be delegated to a model | Explicit inputs/outputs, retries, receipts |
| `EVALUATOR_OPTIMIZER` | Quality must rise | Producer and evaluator alternate with caps and thresholds |
| `MISSION_SPAWN` | The node is itself a durable program | Emits a child mission and awaits or tracks it |
| `EVENT_TRIGGER` | Work should resume on the world changing | After major commit, failed E2E, new source, revalidation due |

`EVENT_TRIGGER` is the recursive self-improvement hook: after a qualifying commit, deployment, or verification failure, the mission schedules an optimization round instead of requiring a human to start over.

### 5.1 Cycles

Two different “cycles” exist. Do not mix them.

1. **Strategy loops** — `GOAL_LOOP`, `EVALUATOR_OPTIMIZER`, RSI `EVENT_TRIGGER`. These are bounded by iteration, token, dollar, and wall-clock governors. They do not require cycles in the compiled edge list.
2. **Cyclic stage graphs** — a coordinator may declare that a `STAGE_GRAPH` may revisit nodes. The compiler admits this only with an explicit `cycles: allowed` flag, a visit cap, a budget remaining check, and a termination predicate. Default admission remains acyclic.

This keeps Temporal and the compiler simple for the common case without forbidding the bet.

### 5.2 Parallelism declarations

Per node or subgraph: `serial`, `bounded_parallel`, `fully_parallel`, `quorum`, `map_reduce`, `speculative_race`, `evaluator_fan_in`.

Independent branches continue unless `fail_fast` is declared. Stage importance is `required`, `optional`, or `best_effort`.

### 5.3 Research templates are not the kernel

These are catalog templates that compile onto the strategies above:

- time-context (publication / historical / interval / current) — never named a “temporal mission”
- entity identity
- entity descriptor
- metric probe
- technical thread (`problem → constraints → approach → mechanism → implementation → evaluation → failure modes → alternatives → currency`)
- artifact assessment
- course / challenge / project projection
- application build → test → preview → improve

---

## 6. Evolution and immutability

**Proposed lock (was unresolved):** workflows are evolvable; every accepted program is immutable.

A running mission does not silently rewrite its graph. A coordinator, reconciler, or authorized human proposes a `GraphMutationSet`. Mission Control validates policy, budget, depth, capability grants, and structural invariants, then appends a new `MissionRevision` and a new immutable `CompiledGraph`.

| Mutation | Meaning |
|---|---|
| `ADD_NODE` / `ADD_EDGE` | Append future work |
| `FAN_OUT` / `FAN_IN` | Parallel siblings or aggregator |
| `SUPERSEDE` | Mark a not-yet-accepted node obsolete; keep history |
| `REWIRE_FUTURE` | Change edges among nodes that have not started |
| `FORK_BRANCH` | Alternate strategy for the same objective |
| `SPAWN_MISSION` | Promote a node into a child mission |
| `CLOSE_BRANCH` | Stop a branch with terminal evidence |

Completed history never changes. In-flight work either finishes under the old compiled epoch or is cancelled / superseded with a recorded reason. Lineage edges (`SUPERSEDES`, `DERIVED_FROM`, `USED_AS_INPUT_TO`) make the prior epoch part of the new one without mutating it.

This is how the system stays “truly immutable” while remaining alive.

Reconciliation triggers: stage boundaries, budget thresholds, repeated failure, evaluator rejection, operator request, material new evidence, compaction, or an agent-declared uncertainty / decomposition threshold. The reconciler receives compact state, not every token.

---

## 7. Coordinator as compiler

The coordinator is not a task lister. It writes an execution program.

```text
MissionDefinition {
  goal: GoalSpec
  graph: NodeSpec[]                  // strategies, edges, cycles flag
  inputs: ArtifactBinding[]
  workspace_templates: WorkspaceTemplate[]
  capability_bindings: CapabilityBinding[]
  hooks: HookSpec[]                  // coordinator-authored, admitted
  tool_middleware: ToolMiddleware[]  // including human-in-the-loop gates
  concurrency: ConcurrencyPolicy
  budgets: BudgetEnvelope
  continuation: ContinuationPolicy
  verification: VerificationPolicy
  human_controls: HumanControlPolicy
  spawn_policy: SpawnPolicy
  observability: ObservabilityPolicy
}
```

Per node the coordinator may set:

- harness and model, with fallback order
- agent skills, MCP servers, CLIs, custom tools
- workspace (repo, branch/worktree, seed artifacts, datasets, env, sandbox)
- subagents
- output contract and schema
- objective and success criteria
- HITL middleware on specific tools
- hooks (lifecycle, permission, compaction, test)
- whether the node receives the Mission Control skill / MCP (spawn + peek)
- verification gates
- depth / child-spawn grants

### 7.1 Authority rule

Discovery is open. Authority is closed.

- The coordinator may **query** the catalog semantically and may **propose** capabilities found via `npx skills find`.
- Mission Control **admits** exact versions into a profile for that revision.
- A specialization may tighten a profile. Adding authority requires policy or human approval.
- Runtime MCP discovery cannot expand the committed allowlist.
- Coordinator-written hooks are artifacts. They are scanned, hashed, and admitted before materialization. They are not an unrestricted shell.

### 7.2 Intake flow

1. Explore existing missions, KB state, schemas, and artifacts.
2. Draft a `MissionDefinition`.
3. Validate (deterministic; no remote agents).
4. Review when cost / risk policy requires a dry run.
5. Commit: canonicalize, hash, store, compile work items in one Postgres transaction.
6. Start: outbox → orchestrator with `workflowId = mission:<id>` or Eve/Vercel binding.

Planning is probabilistic. Validation and compilation are deterministic. Execution starts only from a committed revision hash.

---

## 8. Recursive spawn

This is the Alpha Centauri bet: large objectives are completed by agents that can spawn themselves for sub-objectives.

```text
Coordinator
  → Mission A
      → Node (GOAL_LOOP | STAGE_GRAPH | ...)
          → child Mission B          // if spawn granted
              → Node
                  → child Mission C  // depth-capped
```

### 8.1 Contract

Mission Control is itself a capability: `MISSION_CONTROL` / `workflow` skill + MCP + CLI.

Any authorized agent may:

- `mission.draft` / `validate` / `commit` / `start`
- `mission.peek` / `graph` / `events` / `artifacts`
- `mission.command` (queue, cancel-and-inject, pause, resume) within grant
- `mission.spawn` from an output artifact
- `mission.reconcile` when granted

Depth governors:

- default soft cap below a hard cap (hard cap is configurable; starting recommendation is 16, overrideable)
- max children per node
- max descendants
- remaining budget required to spawn
- required justification recorded on the spawn artifact

Children do not get provider launch keys. They get a scoped Mission Control token bound to tenant, mission, remaining depth, remaining budget, and allowed operations.

### 8.2 When to spawn vs enlarge the graph

Prefer a child mission when the subtree needs its own owner, lifecycle, schedule, retention, authorization, or reuse. Prefer a graph mutation when the work is still the same objective and should stay in one debugger breadcrumb trail.

Both are legal. The compiler and dashboard treat a child mission as a portal node.

---

## 9. Capability discovery and the harness

The harness is the unit of agent power: **MCP + agent skills + CLIs + prompts + sandbox + workspace + hooks + models**.

```text
CapabilityDescriptor {
  capability_id
  kind: AGENT_SKILL | MCP_SERVER | MCP_TOOL | CLI | CUSTOM_TOOL |
        BROWSER | FILESYSTEM | SANDBOX | DATABASE | MODEL |
        DETERMINISTIC_EXECUTOR | MISSION_CONTROL | WALLET   // wallet later
  version
  interface_schema
  requirements[]
  scopes[]
  secrets: SecretRef[]          // references only
  cost_model?
  policy_tags[]
  compatibility: { runtimes[], orchestrators[], compute[] }
}
```

Lifecycle: **Discover → Resolve → Admit → Materialize → Bind → Observe → Revoke**.

Discover paths:

1. Mission Control catalog search (semantic function, tags, interface, cost, constraints).
2. `npx skills find <query>` creates **candidates**. Candidates never confer admission.
3. Coordinator-authored hooks and tool middleware, submitted as artifacts for admission.

Materialization writes exact skill directories, MCP config, CLI binaries, instruction bundles, and frozen inputs into the workspace. Digests are verified before launch. Cursor auto-discovered subagents that are not in the profile must be kept out of the workspace or admission fails.

### 9.1 Wallets and service authentication (specified now, enabled later)

Agents will eventually hold wallets and permission to authenticate to third-party services. The kernel reserves:

- `WALLET` capability kind
- credential profiles that store **references**, never values
- scoped OAuth / API session grants as HumanTasks of type `CREDENTIAL`
- spend limits beside token/dollar budgets
- revocation at the next safe boundary

No secret values in prompts, configs, events, or artifacts. This lands with north-star phase 4 (credentials / sandboxes / harnesses). The schema must not preclude it.

---

## 10. Execution lanes

One mission domain. Multiple lanes. No “Cursor missions” vs “Eve missions.”

### 10.1 Cursor + Temporal (Cursor credits)

Best for repository-centered coding, investigation, implementation, and tests that want a full development VM.

- Temporal `MissionWorkflow` → `NodeWorkflow` → `HarnessExecutionWorkflow`
- Worker holds `CURSOR_API_KEY`
- Cursor Local SDK and Cursor Cloud SDK are both adapters behind `AgentHarness`
- Cloud: durable poll + later webhooks; SSE is observability, `GET run` is recovery
- Local: session, interrupt, and filesystem visibility are richer

### 10.2 Eve + Vercel Workflows + Vercel AI SDK (frontier credits)

Best for durable agent applications, structured research/ingestion pipelines, verification dispatch, and the dashboard coordinator chat.

- Temporal remains the top-level mission orchestrator when a mission spans lanes
- Vercel Workflow owns durable steps **inside** the Eve lane
- Temporal treats a Vercel run as one external durable operation
- Retry ownership is explicit: Temporal retries start/observe/communicate; Vercel retries internal steps; Mission Control decides whether to replace the external run; deterministic effects use receipts and are never double-retried
- Vercel AI SDK is the conversation runtime for coordinator agents in the control plane (see §18)

### 10.3 Claude Code and Codex

First-class codebase-creation platforms. They are not “nice to have later.”

The requirement is: **turn the Claude Code and Codex SDK/CLI into a program with control flow and durability**, so a coordinator can run them against a goal.

Recommended shape:

1. Implement the `AgentHarness` adapter (prepare, start, queue, interrupt, pause, snapshot, continuation, cancel).
2. Wrap the adapter in a Temporal `HarnessExecutionWorkflow` so crash, retry, cancel, and peek work the same as Cursor.
3. If a given deployment cannot run Temporal around that CLI, the CLI still speaks the Mission Control protocol (hydrate, heartbeat, finish, peek) and Mission Control reconciles. That is a degraded lane, not the architecture.

Dedicated task queues: `mission-control`, `cursor`, `claude-code`, `codex`, `eve`, `browser-test`, `ingestion`, `verification`, `artifact`, `notifications`.

### 10.4 AgentHarness

```text
AgentHarness {
  prepare_execution(spec, workspace, capabilities) -> HarnessExecutionRef
  start(ref, initial_context) -> Stream<Event>
  queue_instruction(ref, instruction)
  interrupt_and_instruct(ref, instruction, interrupt_policy)
  pause(ref, safe_point_policy)
  resume(ref)
  snapshot(ref) -> WorkspaceSnapshot
  inspect(ref, query) -> InspectionResult
  request_continuation(ref) -> ContinuationCheckpoint
  cancel(ref, reason)
}
```

Delivery semantics are adapter-specific. The command vocabulary is unified. The UI always distinguishes **best-effort interrupt** from **guaranteed turn-boundary delivery**. Cursor Cloud today is closer to cancel-and-send or after-current-run. Local Cursor, Claude Code, Codex, and Eve should expose the richer pair whenever the vendor allows it.

### 10.5 Continuation

Compaction is a session transition, not a mission retry.

`CONTINUE` — same execution, new session / context epoch, uses `ContinuationCheckpoint`.  
`RETRY` — same node, new attempt after failure.  
`RERUN` — intentional new execution from chosen inputs.  
`FORK` — new branch from a checkpoint or artifact set.  
`RECONCILE` — new graph revision.

---

## 11. Intervention and peek

Humans and authorized agents use the same durable commands. Every command has a target scope: mission, branch, node, execution, session, or (when the harness supports it) the current model/tool turn.

| Command | Semantics |
|---|---|
| `QUEUE_INSTRUCTION` | Durably enqueue for the next safe turn boundary. Must enter the next context before the agent continues |
| `INTERRUPT_AND_INJECT` | Cancel or stop the current run when supported, then inject context into the resumed or new run |
| `PAUSE` | Stop scheduling; pause after the configured safe boundary |
| `HARD_PAUSE` / `CANCEL` | Aggressive stop; confirm when side effects may be partial |
| `RESUME` | Continue, optionally with instruction, capability, or budget change |
| `RETRY` / `RERUN` / `FORK` | As in §10.5 |
| `RECONCILE` | Ask the coordinator / reconciler for a mutation set |
| `PEEK` | Read compiled graph, active executions, last N events, blockers, artifacts, budget, harness snapshot |

The Mission Control skill exposes `peek` as a first-class tool. Child agents should inspect before spawning or interrupting.

Command state machines are durable (`accepted → queued → delivered → observed`, and the interrupt/pause/reconcile variants). Browser or MCP disconnect never loses a command. Idempotency key is `command_id`.

Human tasks are separate from ad-hoc steering:

- `APPROVAL` — merge, publish, DB mutation, expensive spawn, capability escalation
- `QUESTION` — missing judgment
- `SELECTION` — choose among architectures, studies, course variants, branches
- `REVIEW` — report, diff, preview, evidence sample
- `CREDENTIAL` — scoped access / future wallet grant
- `POLICY_OVERRIDE` — depth, budget, otherwise blocked action

Approval ≠ human input ≠ intervention. Changing the committed graph, adding authority, or raising hard caps requires a new revision or an explicit override task. A queued message cannot smuggle those changes.

---

## 12. Proof and verification

Proof is how a goal becomes real. There are two proof families, one kernel idea: **no trusted downstream use without an independent check**.

### 12.1 Codebases — tests as proof

Application missions do not complete on “the agent said it works.”

Required verification policy options, composable per node:

- unit tests
- typecheck / lint
- E2E (Playwright or equivalent) against a preview
- mobile / desktop launch smoke
- evaluator score against a rubric
- human review of a preview

Artifacts: test report, Playwright trace, screenshot, deployment URL, coverage, evaluator result. A failed E2E artifact can be attached to `INTERRUPT_AND_INJECT` (“fix this failure”) with exact trace linkage.

The improvement loop is event-driven: qualifying commit or failed proof → `EVENT_TRIGGER` optimization round → new tests → new artifacts.

### 12.2 Research and ingestion — verification.v1

Knowledge Services owns the algorithm (`@aiengineer/knowledge-verification`). Mission Control dispatches it. Agents do not copy the engine.

Five questions, in order. Later stages cannot reverse an earlier deterministic failure:

1. Capture integrity — exact source bytes preserved
2. Selector integrity — locator deterministically selects the claimed evidence
3. Mechanical correctness — identity, type, value, unit, period, arithmetic
4. Semantic support — evidence supports the atomic claim
5. Policy admission — risk / use-case policy permits downstream use

Orthogonal properties are stored separately. No aggregate score may erase a failed property.

Operations Mission Control must be able to schedule:

| Use case | Proves |
|---|---|
| `captureSource` | Immutable acquisition |
| `verifyExtraction` | Field values match evidence-bound selectors |
| `verifyClaims` / `verifyReport` | Atomic claims and report-wide citation completeness |
| `verifyMetricObservation` | Metric identity, unit, period, optional calculation replay |
| `runBenchmark` / `compareBenchmarkRuns` | Frozen-case engineering labels; paired comparison |
| `replayRun` | Digests reproduce |

Dispatch rules already proved locally in Knowledge Services WS-10:

- Temporal workflow performs no I/O; activities submit a bounded capability and poll
- Credentials, source bytes, and full receipts stay out of workflow history
- Quality rejection is terminal (`disposition: quality_rejected`) and must not be retried as infrastructure failure
- Cancellation must reach the Knowledge Services operation
- Producer and verifier deployments are distinct
- `OperationContext` carries `tenantId`, `missionId`, `workItemId`, `attemptId`, `idempotencyKey`, `externalExecution.runtime = mission_control`

Current honesty: verification kinds exist in wire contracts and local Temporal proofs; they are not yet in the production capability matrix. Mission Control treats them as **catalog-admitted capabilities**, not as assumed production defaults.

### 12.3 Attribution is the research receipt

Agents must point at discoveries:

```text
Source → Capture → Selector → Fragment → EvidenceEdge → Assertion / Field
```

Machine selectors are not display excerpts. Missing or ambiguous locators fail closed. Semantic judges see only mechanically authorized fragments.

This is how reports become ingestible. A company/product report sitting in the dashboard output rail is still Zone 2 until promotion admits it.

---

## 13. Research and ingestion module

This module is the August 27 blueprint, restated as a Mission Control domain module. Time-context work is never called a “Temporal mission.”

### 13.1 One-sentence rule

Persist every research artifact immediately into its immutable or provisional layer. Promote only independently verified atomic outputs into canonical knowledge, retrieval, ranking, and curriculum.

### 13.2 Six zones

| Zone | Write | Trust | Consumers |
|---|---|---|---|
| 0 Capture | Source bytes | Unjudged | Intake, verification |
| 1 Starter / proposal | Packets, candidates | Provisional | Operator, planner |
| 2 Mission ledger | Plans, attempts, artifacts | Operational | Orchestration, debugger |
| 3 Canonical identity | Entity shell | Identity, not dossier | Internal systems |
| 4 Verified facts | Claims, metrics, examples | Trusted in declared scope/time | Reports, ranking, retrieval, curriculum |
| 5 Product projection | Vectors, lessons, leaderboards | Product-ready | End users, learner agents |

Exploratory retrieval may index zones 1–2 only if marked `provisional` and structurally excluded from public answers. Knowledge Services already has `official_canonical`, `internal_exploratory`, and `user_managed` store classes. Use them.

### 13.3 Four clocks and four perspectives

Publication time, valid/effective time, observation/capture time, system/admission time.

Perspectives: `speaker_time`, `historical_as_of`, `interval_development`, `current_state`.

A 2023 talk is not rewritten by a 2025 archive or a 2026 job title. New facts supersede; they do not mutate.

### 13.4 Gap planning

Every research mission starts from an immutable `mission_input_snapshot` of canonical state. The planner schedules only missing, stale, or disputed work. Idempotency includes subject, question hash, window, snapshot hash, and policy version. If another mission filled the gap, admission is `no_op_duplicate` or merge — never a double write.

### 13.5 Promotion

`decidePromotion` is ordered and not score-bypassable:

```text
lineage → identity → mechanical → semantic → policy → snapshot freshness
```

Outcomes: `admitted`, `review_required`, `held`, `quarantined`, `no_op_duplicate`, `superseded`.

A bundle of ten findings may promote seven, hold two, and keep one conflict. That is a successful execution with mixed admission, not a failed mission.

Split the words:

- **Execution success** — every required output reached a *disposition*
- **Admission success** — each atom independently cleared promotion
- **Product publication** — purpose-specific projection after admission

Agents author ingestion **intent**. Only `ingestion_executor` talks to target adapters. Intent + approval + idempotent receipt is the canonical-write path. Knowledge Services publication / promotion decisions remain the authority for vector spaces.

### 13.6 Downstream projections

Retrieval, ranking, and curriculum consume admitted records.

- Retrieval: filters first, then lexical + vector; return an evidence packet, never a naked hit. Modes: `as_of`, `current`, `compare`, `historical_then_current`, `executable_only`.
- Ranking: separate policies (research priority, curriculum inclusion, authority, adoption, reliability). Missing is not zero.
- Curriculum: a lesson or challenge publishes when *its* dependency graph is admitted. New evidence versions the unit. It does not silently change completed learner work.

Snippet assurance: `discovered` → `inspected` → `runnable` → `curriculum_ready`. Only the last two appear as working examples.

---

## 14. Knowledge plane

`ai-engineer-knowledge-services` is the product knowledge plane. Mission Control does not embed, index, or verify locally.

### 14.1 Retrieval and vector stores

Eight typed spaces exist: `engineering_claims`, `tool_capabilities`, `implementation_examples`, `paper_case_study_knowledge`, `entity_profiles`, `model_capabilities`, `benchmark_intelligence`, `source_native_sections`.

Production retrieval is `POST /v1/retrieval-runs` with a `RetrievalPlan`. Client-supplied embeddings are rejected. The API embeds, hybrid-searches (exact / FTS / trigram / ANN), fuses, and returns an immutable `EvidencePacket`.

Honest production limits today (fail closed, do not pretend otherwise):

- no graph expansion, anchors, or soft boosts on the API path
- upper temporal bounds not backed
- `vector_store_search` is not an admitted bypass
- published space versions must exist or retrieval abstains

Mission Control coordinators and dashboard search call Knowledge Services through `@aiengineer/knowledge-client` or MCP (`retrieval.search`, `retrieval.explain_run`, `retrieval.build_evidence_packet`). They pass `missionId` / `workItemId` / `externalExecution`.

### 14.2 Ingestion chain

Admitted worker operations already cover source discovery, capture, transformation, chunking, promotion proposal/decision, embedding, space publication, rollback, vector-store create/documents/ingestion, and evaluation.

Processing success never grants publication authority. Mission Control can trigger the chain. It cannot self-approve official_canonical publication.

### 14.3 What the dashboard searches

The control plane search box is a federated query, not a second index:

- missions, revisions, nodes, commands (Mission Control)
- entities, sources, captures, reports, claims (db-contract + research module)
- evidence packets and vector hits (Knowledge Services retrieval)
- verification runs, cases, findings (verification.v1 reads)
- artifacts and lineage (shared)

Selecting any result as **Use as Mission Input** opens the composer with immutable references and `USED_AS_INPUT_TO` edges.

---

## 15. Application, course, and RSI missions

These are first-class goal families on the same kernel.

### 15.1 Application creation

Typical compiled shape (coordinator may differ):

```text
specify → scaffold (Cursor | Claude Code | Codex)
       → implement
       → unit + typecheck
       → preview deploy
       → E2E / browser / mobile / desktop smoke
       → evaluator or human review
       → EVENT_TRIGGER on commit / failed proof → improve
```

Workspaces are real repos. Agents must be able to test the project. Browser-only progress is incomplete: handshake from Cursor / Claude Code / Codex / Cowork back into Mission Control is part of done.

### 15.2 Course / challenge / project creation

Typical compiled shape:

```text
gap snapshot → research / verify threads
            → admit facts
            → compose lesson / challenge / project
            → pin examples at runnable or curriculum_ready
            → evidence-packet gate
            → publish versioned unit
```

Official courses already include teaching users to create courses. User-authored units are the same projection pipeline with `user_managed` authority.

### 15.3 Recursive self-improvement

Not a hidden agent loop. An event policy:

- after a major commit, merged PR, failed verification, new upstream release, or revalidation_due
- if the mission (or parent) still has budget and an open improvement objective
- spawn or release an optimizer node
- require proof before accepting the new head

This is how a 10-billion-token objective stays a mission instead of a chat.

---

## 16. Personal intelligence platforms

Specified as a tenancy and store-class expansion of the same system, not a fork.

| Official AI Engineer | Personal platform |
|---|---|
| `official_canonical` spaces | `user_managed` spaces |
| Flywheel missions, platform budgets | User-owned missions, user budgets / future wallet |
| Platform verifier deployments | User-granted verifier profiles |
| Official curriculum | User-authored courses, notes, KB |

Notes and KB retrieval remain required on learning surfaces. Schema for notes / learner / KB stays in db-contract. Mission Control orchestrates work that writes into those schemas through the same promotion rules.

---

## 17. Public contract

Design order: **API → CLI → MCP → MCP UI → Agent Skill → A2A → web / mobile / desktop**.

### 17.1 HTTP API (canonical)

Representative resources:

```text
POST   /v1/missions
POST   /v1/missions/{id}/revisions
POST   /v1/missions/{id}/revisions/{rev}/validate|compile|commit
POST   /v1/missions/{id}/executions
POST   /v1/missions/{id}/commands
POST   /v1/missions/{id}/reconcile
GET    /v1/missions/{id}
GET    /v1/missions/{id}/summary
GET    /v1/missions/{id}/graph?revision=head&subtree=
GET    /v1/missions/{id}/events?after=
GET    /v1/missions/{id}/artifacts
GET    /v1/missions/{id}/view
GET    /v1/executions/{id}/inspect
GET    /v1/artifacts/{id}/lineage
POST   /v1/artifacts/{id}/compose-mission
POST   /v1/catalog/search
GET    /v1/schemas/mission
WS     /v1/stream
```

Knowledge and verification stay on Knowledge Services `/v1/retrieval-runs`, `/v1/verification/*`, `/v1/a2a/tasks`. Mission Control links them by ID.

### 17.2 CLI — `missionctl`

Machine-readable. JSON on stdout. Mutations require `--apply` and an idempotency key.

Command groups: `schema`, `template`, `exemplar`, `draft`, `validate`, `compile`, `commit`, `run`, `status`, `graph`, `peek`, `view`, `artifact`, `approval`, `input`, `intervene` (`queue` | `cancel-and-inject`), `events`, `catalog`, `skills-find`, `reconcile`, `test`.

### 17.3 MCP

Thin authenticated adapter. Task-oriented tools. Ordinary agent tokens cannot approve, admit capabilities, execute ingestion, or raise hard caps.

Required tools include draft/validate/commit/start, peek/graph/view, artifact list/get, command queue / interrupt, catalog search, and (when granted) spawn.

### 17.4 MCP UI

The debugger is not web-only. MCP UI exposes the same live-mission regions as interactive UI resources: topology, event tail, command composer, output rail, human-task cards, artifact-to-mission compose. A Cursor / Claude / Eve session that has adopted the skill should be able to operate a mission without opening the website.

### 17.5 Agent Skill

Teaches when to activate (decomposition, durability, spawn, proof) and how to call `missionctl` / MCP. Prose cannot override policy. Versioned and tested against the API contract.

Published as `@ai-engineer/mission-control-skill` alongside `@ai-engineer/mission-contracts` and `@ai-engineer/missionctl`.

### 17.6 A2A

North-star surface. Knowledge Services already has `POST /v1/a2a/tasks` for `document_preparation`, `vector_store_ingestion`, `retrieval`, `evidence_packet_construction`.

Mission Control adds A2A task kinds for `mission_start`, `mission_peek`, `mission_command`, and `compose_mission_from_artifacts`, mapping onto the same application services. Do not invent a second workflow bus.

---

## 18. Dashboard — debugger and coordinator chat

This is not a status page. It is the live operations console and the place a human talks to coordinator agents.

### 18.1 Information architecture

| Area | Purpose |
|---|---|
| Missions | Browse / filter by app, status, harness, depth, goal, tags |
| Live Mission | Topology + event stream + inspector/command + output rail |
| Coordinator | Vercel AI SDK chat with one or more coordinator agents, bound to the selected mission or to “new mission” |
| Outputs | Enterprise artifact catalog |
| Human Tasks | Cross-mission inbox |
| Knowledge | Federated search: entities, sources, reports, packets, verification runs |
| Capabilities | Catalog, health, grants |
| Workers | Temporal queues, Vercel workflow health, adapter health |
| Budgets | Tokens, dollars, credits (Cursor vs Gateway), concurrency |
| Audit / Provenance | Interventions, revisions, lineage |

### 18.2 Live Mission layout

Four zones under a health bar: topology (tree / DAG / outline), live coalesced events, inspector + command composer, output rail.

Recursive breadcrumbs: `Root > Node 4 > Child Mission > Node 2`. Graph revisions overlay mutations. Depth and spawn denials are visible.

Event families: Temporal, Vercel Workflow, harness, tool/MCP/CLI, git, test/browser, research/ingestion, verification, human, graph mutation, artifact.

Coalescing is presentation only.

### 18.3 Coordinator chat (Vercel AI SDK)

The dashboard hosts coordinator agents through the Vercel AI SDK. The human can:

- describe a goal and watch the coordinator draft / validate / compile
- attach artifacts, evidence packets, verification runs, failed tests
- approve or reject the dry-run graph
- speak while a mission runs; the coordinator peeks and issues `QUEUE` or `INTERRUPT_AND_INJECT` through the API
- ask the coordinator to spawn a follow-on mission from selected outputs

The chat is not a side channel around policy. Tool calls from the coordinator are Mission Control and Knowledge Services tools. The stream of tool results is the same event fabric the debugger shows.

### 18.4 Transport

```text
Client → WS Gateway: subscribe { mission_id, subtree?, filters?, after_cursor? }
Gateway → Client: MissionEvent | EventAggregate | ArtifactNotification | StatePatch | ChatPart
Client → Mission API: command | coordinator message
```

Commands never live only in WebSocket state. Reconnect replays from cursor. High-volume streams are backpressured. Presence shows other operators on the same mission.

### 18.5 Roles

`Viewer` < `Operator` < `Mission Admin` < `Platform Admin`. High-impact actions confirm and may require second approval.

---

## 19. Storage

| Store | Data |
|---|---|
| Postgres (db-contract) | Missions, revisions, compiled graphs, work items, attempts, capabilities, commands, artifact metadata, promotion proposals, approvals, snapshots, verification run links |
| Object storage | Reports, traces, workspace snapshots, raw event archives, capability bundles |
| GitHub / git | Code history, branches, PRs |
| Knowledge Services Postgres | Captures, chunks, vector items, evidence packets, verification runs/findings, publications |
| Neo4j (optional projection) | Domain graph views — not a second source of truth |
| Temporal / Vercel Workflow | Recovery logs only |

Reuse existing `orchestration.*`, `evidence.*`, `research.*`, `retrieval.*`, `evaluation.*`, `corpus.*`, `knowledge.*`, `curriculum.*`. Add bounded gaps already proposed: time-context windows, report assertions, claim temporal scope, mission input snapshots, promotion proposals, computation ledger, optional `research.study`.

Do not create a parallel mission ledger beside the existing orchestration tables Knowledge Services already foreign-keys (`evidence.verification_run` → `orchestration.mission` / `work_item` / `attempt`).

Event plane: start with Postgres events + outbox + a stream gateway. Introduce a dedicated stream/analytics store when dashboard volume requires it. Temporal queries stay compact.

---

## 20. Governors and safety

| Governor | Examples |
|---|---|
| Depth | Mission depth, node nesting, native subagent depth, spawn depth |
| Concurrency | Active missions, branches, executions per harness, browser sessions, queue fairness |
| Budget | Tokens, dollars, Cursor credits, Gateway credits, wall-clock, storage, browser minutes |
| Spawn | Who may spawn, children per node, descendants, remaining budget, justification |
| Capability | Allow/deny by mission, node, repo, data domain, network, secret, wallet |
| Verification | Required checks before accept / merge / publish / retrieve / teach |
| Credit lane | Cursor-credit work cannot silently spend Gateway credits, and the reverse |

Fail closed on broken provenance, missing published spaces, mismatched orchestration identity, and quality rejection.

---

## 21. Delivery sequence

This is a sequence of **completeness**, not a sequence that deletes later chapters.

North-star phases remain: 0 lock docs → 1 flywheel-to-KB → 2 agent-native core + editor + contract → 3 external-agent handshake → 4 credentials / sandboxes / harnesses → 5 interview + real products + user-authored → 6 hardware.

Mission Control tracks inside those phases as follows.

### Gate A — Lock this kernel

Accept this pre-spec (or its successor lock). Freeze entity names, strategies, command vocabulary, spawn rules, and the immutability model. Stop extending the four older Mission Control notes.

### Gate B — Contracts and compiler

Mission schemas, revision hashing, DAG + optional-cycle compile, capability catalog, `missionctl` draft/validate/compile. Six research exemplars plus one application-shaped exemplar compile deterministically.

### Gate C — Durable kernel + one coding lane

Temporal `MissionWorkflow` / `NodeWorkflow` / `HarnessExecutionWorkflow`. Cursor Cloud adapter first (credits lane already in use). Exact claim, frozen inputs, receipts. Crash, cancel, duplicate-launch, and missing-receipt proofs.

### Gate D — Proof plane

Wire Knowledge Services verification dispatch for extraction, claims, metrics, replay. Do not retry quality rejection. Dashboard and skill can peek a verification run. Promotion proposals write dispositions without leaking into canonical retrieval.

### Gate E — Debugger + coordinator chat

Live Mission four-zone UI, WS gateway, durable commands (`QUEUE` and `INTERRUPT_AND_INJECT`), human-task inbox, Vercel AI SDK coordinator chat against the same API. MCP UI exposes the same regions.

### Gate F — Research vertical

Time-context + identity + technical thread + atomic promotion on the existing flywheel. Retrieval and curriculum consume admitted records only. Gap snapshots prevent duplicate canonical writes.

### Gate G — Claude Code and Codex lanes

Adapters + Temporal wrappers + task queues. Coordinator can complete an application goal on either lane with unit + E2E proof.

### Gate H — Eve + Vercel Workflow lane

Dimensional records already in schema. Eve research/ingestion pipelines and dashboard coordinator runtime on frontier credits. Temporal still owns cross-lane missions.

### Gate I — Spawn and evolution

Grantable Mission Control skill on child nodes. Append-only graph revisions. Depth governors. Artifact → new mission from the output rail.

### Gate J — RSI, personal spaces, wallets

Event-triggered improvement loops. `user_managed` personal intelligence spaces. Wallet / service-auth capability. Desktop and mobile clients of the same contract. External-agent handshake back from Cowork / Cursor / Claude Code / Codex.

A first demo may be Cursor + Temporal + peek + one verification dispatch. That demo is not allowed to redefine the kernel as “immutable two-node research only.”

---

## 22. Definition of done for the product (not a slice)

The product is done enough to operate when all of the following are true:

1. A coordinator can compile a multi-strategy mission (sequential, parallel, goal-loop, deterministic, child-spawn) with per-node skills, workspace, hooks, HITL middleware, and output contracts.
2. Cursor Cloud/Local, Claude Code, Codex, and Eve can run behind `AgentHarness` with normalized events.
3. An authorized child can peek and spawn within depth and budget.
4. A human or agent can queue the next turn or cancel-and-inject, and observe delivery semantics.
5. A long run can compact and resume without losing lineage or workspace identity.
6. The graph can evolve by append-only revision while history stays immutable.
7. Code goals do not complete without unit and E2E (or an explicit waived proof).
8. Research goals do not enter canonical retrieval or curriculum without verification.v1 + promotion.
9. Dashboard and MCP UI show live topology, events, outputs, and coordinator chat.
10. Any artifact can start a new mission with provenance.
11. Scale-out is multiple workers and queues, not one process.
12. Personal / official store classes exist on one contract.

---

## 23. Source map

| Source | What was kept |
|---|---|
| This conversation (2026-09-05) | Extreme decomposition, configurable stages, spawn skill, two interrupt modes, peek, semantic + `npx skills find`, wallets later, personal intelligence, Cursor+Temporal vs Eve+Vercel credits, Claude Code/Codex durability, proof families, Vercel AI SDK coordinator chat |
| `mission_control_architecture_clarity.md` | Strategies, evolving graph, harness contract, continuation, governors, public API sketch |
| `mission_control_dashboard.md` | Debugger IA, commands, output rail, compose-from-artifact, WS gateway |
| `CURSOR_CLOUD_MISSION_ORCHESTRATION_ARCHITECTURE.md` | Postgres ledger, outbox, adapter, skill/CLI/MCP as clients, failure classes |
| `CURSOR_CLOUD_TEMPORAL_LOCAL_AND_AWS_IMPLEMENTATION_SPEC.md` | Compiler, catalog, materialization, frozen inputs, intent vs executor, Temporal workflow hygiene, approval/input/intervention split |
| `MISSION_CONTROL_MULTI_RUNTIME_EVOLUTION_NOTE.md` | One domain, dimensional execution, Eve/Vercel lane, retry ownership |
| Research-ingestion blueprint | Zones, four clocks, gap snapshots, `decidePromotion`, technical thread, projections |
| Knowledge Services verification | `verification.v1` five-stage proof, selectors, WS-10 dispatch rules |
| Knowledge Services retrieval | Spaces, store classes, `RetrievalPlan`, evidence packets, fail-closed API limits |
| `docs/product/00-vision.md` + `11-north-star-path.md` | MCP-first surfaces, agent-native courses, handshake, db-contract, phase order |

### Explicitly rejected as product law

- “Missions cannot compose child missions” as a permanent rule (it was a Phase 1 caution, not the bet).
- “No runtime graph mutation” as a permanent rule (replaced by immutable revisions + legal mutations).
- Dashboard as a late optional approval inbox.
- Temporal Cloud as the only hosting sentence (workers stay portable: local → AWS → Cloud).
- One `provider` field for the whole stack.
- Chat transcripts as receipts.
- A second knowledge or mission database.

---

## 24. Open questions for the lock pass

These are the only items still intentionally open. Everything else in this file is the proposed default.

1. Hard spawn-depth default (recommendation: 16, overrideable).
2. Whether cyclic `STAGE_GRAPH` ships in the first compiler or only strategy loops ship first (recommendation: compiler flag exists immediately; first demos use strategy loops).
3. Package registry for `missionctl` / skill.
4. Temporal Cloud vs self-hosted Temporal on AWS for production kernel.
5. When MCP UI ships relative to the web debugger (recommendation: same contract, web first for the four-zone layout, MCP UI in the same gate).
6. First wallet providers and the exact HumanTask grant UX.
7. How aggressively official vs personal intelligence are branded as one product in v1.

---

## 25. Immediate next action

Review this pre-spec as the merge. On acceptance, mark a successor file **canonical lock**, move leftover root `mission_control_*.md` files to an archive pointer, and implement Gate A–C without reopening the kernel in chat-only notes.
