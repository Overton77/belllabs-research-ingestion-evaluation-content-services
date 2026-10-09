# Mission Control Supplementary Architecture Note

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

> **Superseded 2026-09-07.** Folded into [`MISSION_CONTROL_SPEC.md`](./MISSION_CONTROL_SPEC.md) (§2.3, §4.2, §9, §12, §13.4, §16). Kept as a source. Do not add decisions here.

**Status:** superseded source (was: architectural guidance — companion to the pre-spec)  
**Date:** 2026-09-05  
**Companion:** [`MISSION_CONTROL_PRESPEC.md`](./MISSION_CONTROL_PRESPEC.md)

This note answers how Mission Control stays provider-neutral while still using vendor capabilities explicitly; how `MissionDefinition` grows; and how experiments, continuation, prompt caching, file editing, source intelligence, memory, and a shareable platform sit on the same kernel.

It is not a full adapter specification. Do not treat it as a competing control plane.

---

## 1. Provider-neutral kernel, explicit adapters

You will build **one stack at a time**. That is a delivery choice. The kernel must already be able to host the others without a rewrite.

The mistake to avoid: encoding Eve, Deep Agents, Cursor, or Temporal nouns into mission state. The mistake in the other direction: pretending Deep Agents `FilesystemMiddleware` or OpenAI `apply_patch` are generic “tools” with no native contract. Both are wrong.

Use three layers.

```text
Mission kernel          provider-neutral domain
  Mission, revision, node, strategy, artifact, command,
  budget, proof, spawn, peek

Execution binding       dimensional, not a single provider field
  agent_runtime_kind
  orchestrator_kind
  model_access_kind
  compute_environment_kind
  credit_account_kind

Adapter + projection    vendor-faithful, observational
  native objects, native IDs, native middleware
  projected into CapabilityDescriptor rows
```

The kernel decides *what work is allowed and whether it is done*. An adapter decides *how that work is invoked on a given stack*. Native status is observational. Mission Control applies canonical transitions.

### 1.1 The stacks you actually have

| Stack | Runtime | Orchestrator | Model / credit path | Why it exists in this company |
|---|---|---|---|---|
| Cursor SDK / CLI | `cursor_cloud`, `cursor_local` | Temporal | Cursor account credits | Repo-centered coding, already paid |
| Eve + AI SDK | `eve` | Vercel Workflow (inner), Temporal (cross-lane) | Vercel AI Gateway | Frontier credits, dashboard coordinator chat, research/ingestion apps |
| Anthropic / OpenAI direct | `direct_model` | Temporal or Vercel Workflow | Direct API balances | When Gateway routing is wrong, or a model feature is provider-native (caching, `apply_patch`) |
| LangGraph / Deep Agents / LangChain | `deep_agents` | LangGraph checkpoint **inside** a Temporal or Vercel step | Gateway or direct | When you want that library’s middleware (filesystem, subagents, memory, HITL, summarization) |
| Deterministic executors | `deterministic_executor` | Temporal activity or Vercel step | None | Ingestion, tests, apply_patch apply, promotion |

AWS is compute and later hosting, not a fourth agent brand. Temporal Cloud is the durable kernel host. Pro Vercel is the Eve / AI SDK / Workflow host. Those are **infrastructure accounts**, recorded on `compute_environment_kind` and `credit_account_kind`, not as mission types.

### 1.2 Credits are a ledger, not a vibe

Every node compile must resolve a **credit account**. Mixing them silently is a bug.

| `credit_account_kind` | Spends |
|---|---|
| `cursor` | Cursor agent / cloud runs |
| `vercel_ai_gateway` | Models reached through the Gateway |
| `openai_direct` | OpenAI API key / prepaid |
| `anthropic_direct` | Anthropic API key / prepaid |
| `other_direct` | Additional frontier keys |
| `aws_compute` | Workers, sandboxes, GPU-later |
| `temporal_cloud` | Workflow actions / storage |

Budgets already exist on the mission. Add **per-account remaining** and a compiler check: a Cursor node cannot draw Gateway tokens; an Eve node cannot draw Cursor credits. The dashboard budget rail shows each account separately.

A node may declare a fallback order (`cursor` → `claude_code` → `direct_model`) only if each fallback names its own credit account and the remaining budget on that account is enough.

### 1.3 Two orchestrators, one mission

| Orchestrator | Owns | Does not own |
|---|---|---|
| **Temporal Cloud** | Mission / node / harness durability across lanes; timers; cancel; Continue-As-New; human waits | Vendor step graphs, chat tokens, Deep Agents checkpoints |
| **Vercel Workflow** | Durable steps *inside* the Eve / AI SDK lane | Canonical mission state |

Rule already in the pre-spec, restated for credits: Temporal retries start / observe / communicate with an outer run. Vercel Workflow retries its inner steps. Deterministic side effects use receipts and are never retried by both.

If you later run Deep Agents, LangGraph’s checkpointer is a **third inner orchestrator**, same rule. Temporal sees one `HarnessExecution`. The Deep Agents graph is inside the adapter. Do not promote LangGraph nodes to Mission nodes unless the coordinator explicitly compiled them that way.

### 1.4 Capability projection (the Deep Agents case)

This is the important pattern. When a library has first-class middleware, **register those objects in the capability catalog**. Do not hide them behind a generic “run Deep Agents” blob, and do not fork Mission Control into a LangChain app.

Deep Agents today composes a deterministic middleware stack: `SkillsMiddleware`, `FilesystemMiddleware`, `SubAgentMiddleware`, summarization, `PatchToolCallsMiddleware`, prompt-caching middleware, `MemoryMiddleware`, `HumanInTheLoopMiddleware`. Backends include `StateBackend`, `StoreBackend`, `FilesystemBackend`, `SandboxBackend`, and `CompositeBackend` (path-prefix routing, e.g. `/memories/` → durable store).

Project each of those as catalog rows:

| Library object | Catalog kind | Kernel concept it binds to |
|---|---|---|
| `FilesystemMiddleware` | `TOOL_MIDDLEWARE` + backend | workspace + `SandboxRef` + file-edit policy |
| `FilesystemBackend` | `FILESYSTEM` | trusted local / CI only |
| `StateBackend` | `FILESYSTEM` | ephemeral in-memory workspace |
| `StoreBackend` | `FILESYSTEM` / `MEMORY` | durable path-prefixed store |
| `SandboxBackend` | `SANDBOX` | isolated compute |
| `CompositeBackend` | `FILESYSTEM` | routed mount table |
| `SubAgentMiddleware` | `SUBAGENT` | spawn / isolated context; subagent returns pointers |
| `SkillsMiddleware` | `AGENT_SKILL` | same skill catalog as Cursor / MCP |
| `MemoryMiddleware` | `MEMORY` | §10 memory manager |
| `HumanInTheLoopMiddleware` | `TOOL_MIDDLEWARE` | `tool_middleware` HITL |
| summarization middleware | `CONTINUATION` | §4 compaction |
| prompt-caching middleware | `PROMPT_CACHE` | §5 |
| `PatchToolCallsMiddleware` | `FILE_EDIT` | §7 |

Admission still applies. `npx skills find` or a Deep Agents skill source creates **candidates**. The coordinator selects versions. The compiler binds them to a node’s `agent_runtime_kind = deep_agents`. The adapter materializes the real LangChain objects. Mission peek shows the kernel view (files as artifacts, interrupts as HumanTasks), plus a native inspect handle when useful.

Eve, Cursor, and direct-API tools get the same treatment: native name in the catalog, kernel concept beside it, adapter behind it.

### 1.5 What “build one stack at a time” means

First vertical can be Cursor + Temporal Cloud. Schema and catalog already have the dimensions and kinds above. Eve / Gateway, direct APIs, and Deep Agents land as additional adapters and catalog backfills. No new mission table per stack.

---

## 2. `MissionDefinition` additions

The block in the pre-spec is the right shape. Keep it. Add the things that were implied and are now first-class.

**Sandbox is not only a workspace template.** A workspace says *what tree and seeds the agent sees*. A sandbox says *where that tree runs and what it is allowed to touch*. A Cursor Cloud VM, a Vercel Sandbox, an AWS worker, a Deep Agents `SandboxBackend`, and a local trusted `FilesystemBackend` are different sandboxes that may mount the same logical workspace.

Recommended additions (bold):

```text
MissionDefinition {
  goal: GoalSpec
  graph: NodeSpec[]
  inputs: ArtifactBinding[]
  template_ref?: TemplateRef              // §6
  workspace_templates: WorkspaceTemplate[]
  sandbox: SandboxConfig                  // NEW — default sandbox
  capability_bindings: CapabilityBinding[]
  hooks: HookSpec[]
  tool_middleware: ToolMiddleware[]
  file_edit: FileEditPolicy               // §7
  memory: MemoryPolicy                    // §10
  source_intelligence: SourceIntelligencePolicy  // §9
  experiments: ExperimentPolicy           // §3
  prompt_cache: PromptCachePolicy         // §5
  concurrency: ConcurrencyPolicy
  budgets: BudgetEnvelope                 // includes credit_account splits
  continuation: ContinuationPolicy        // §4 — load-bearing
  verification: VerificationPolicy
  human_controls: HumanControlPolicy
  spawn_policy: SpawnPolicy
  observability: ObservabilityPolicy
}
```

Per-node overrides still win over mission defaults. A node may set `sandbox_ref`, `file_edit`, `continuation`, `prompt_cache`, and `memory` independently.

### 2.1 `SandboxConfig` / `SandboxRef`

```text
SandboxConfig {
  kind: cursor_cloud_vm | vercel_sandbox | aws_worker |
        deep_agents_sandbox | local_trusted_fs | none
  image_or_template_ref?
  network_policy
  filesystem_policy          // roots, deny secrets, virtual_mode
  lifetime                   // per-run | per-attempt | per-mission
  mount_workspace: boolean
  credit_account_kind        // aws_compute, etc.
}

SandboxRef {
  sandbox_id                 // live instance
  config_digest              // exact admitted config
  workspace_snapshot_id?
}
```

`workspace_templates` continue to describe repo, branch, seed artifacts, env files, skill directories. The sandbox *materializes* that template. Do not collapse the two.

Deep Agents’ own warning applies here: raw `FilesystemBackend` is for trusted local/CI. Production and untrusted spawn use a sandbox backend. HITL middleware sits on destructive paths.

---

## 3. Experiments are a goal family

Add a fourth family beside research, course creation, and application creation:

| Goal family | What “done” means | Proof |
|---|---|---|
| **Experiment / benchmark** | A sealed comparison or adversarial run with frozen cases, declared arms, and a published result | Knowledge Services evaluation + verification.v1; optional LLM-as-judge **as one arm**, never as the only gate |

Experiment kinds the kernel must host:

| Kind | Question |
|---|---|
| `harness_comparison` | Same goal, different `agent_runtime_kind` / sandbox / file-edit policy |
| `model_comparison` | Same harness and prompt, different model / credit path |
| `adversarial` | Mutations that must not improve admission (verification module already requires monotonic degradation) |
| `compaction_comparison` | Same mission, different continuation algorithms / thresholds |
| `prompt_cache_ablation` | Cost and quality with cache on vs off |
| `eval_benchmark` | Frozen case set, engineering or gold labels |

This is not a side lab. It is a mission template: fan-out arms as `PARALLEL_SWARM` or sibling nodes, freeze inputs, run, verify, compare, seal. Knowledge Services already has `evaluation.*` (datasets, eval runs, experiments, verification benchmark runs and comparison). Mission Control schedules those operations; it does not grow a second eval store.

**LLM-as-judge** is a typed evaluator node with a rubric version, authorized fragments only, and a recorded judge identity. It cannot be the sole verifier of code (tests win) or of extraction (mechanical + selector integrity win). It is allowed as a quality arm and as semantic stage five of `verification.v1`.

Real E2E testing remains an application-mission proof and can also be an experiment arm (same Playwright artifact contract).

---

## 4. Continuation and compaction

This policy is load-bearing. Long missions die on context, not on Temporal.

A **session** is disposable. Compaction is a controlled session transition, not a mission retry. The pre-spec’s `CONTINUE` / `RETRY` / `RERUN` / `FORK` still apply. This section defines *how* CONTINUE is produced.

### 4.1 Triggers (not only 60%)

| Trigger | Typical use |
|---|---|
| `context_ratio` | e.g. 60% of the model’s usable window |
| `token_budget_remaining` | hard stop before the node budget is wasted on overflow |
| `tool_payload_bytes` | a single tool result or accumulated tool dump exceeds N |
| `subagent_return` | subagent finished; parent must not ingest the raw trace |
| `turn_count` | cheap periodic hygiene |
| `operator_or_reconcile` | human / coordinator asked for a clean session |
| `cache_breakpoint` | compact *below* a cached prefix so the next session can reuse cache |

The 60% example is the default for `GOAL_LOOP` coding and research nodes. Deterministic nodes rarely need it. Evaluator nodes may use a tighter ratio because the judge should see only authorized fragments.

### 4.2 Compaction kinds

These are algorithms the `ContinuationPolicy` may name. A node can enable several.

**1. Session compact (agent-authored handoff)**  
When the trigger fires, a *different* agent (or a deterministic summarizer plus a bounded model) reads the live session and writes a `ContinuationCheckpoint`:

- objective state
- decisions
- completed artifact refs
- workspace snapshot / sandbox ref
- open questions
- pending tool or human actions
- event cursor
- next actions
- invariants
- verification state
- compact summary

A fresh session starts with: checkpoint + mission peek + frozen input manifests. Not the old transcript.

**2. Tool-call offload**  
Huge tool results (HTML, PDFs, traces, `cat` of a file) are written to the workspace or object storage. The conversation keeps a pointer: path, digest, byte length, schema. The model is taught to `read` ranges, not to re-ask for the blob.

**3. Subagent pointer return**  
Deep Agents already isolates subagent context and asks for a final report. Mission Control goes further: the default subagent contract is **filesystem / artifact pointers**, not a pasted novel. The parent peeks or hydrates only what the next step needs. This is also how spawn stays cheap.

**4. Graph / mission digest**  
For deep spawn trees, do not stuff child transcripts into the parent. The parent receives node status, artifact IDs, verification dispositions, and a short digest. Full traces stay in the event store. Peek hydrates on demand.

**5. Memory promotion**  
Compaction may *write* episodic or procedural memories (§10) and then drop the raw turns. The next session retrieves those memories by policy, not by replaying history.

**6. Cache-preserving compact**  
When prompt cache is on, compact the *volatile tail* and keep the stable prefix (system, tools, skill list, mission header) byte-identical. See §5. Compaction that rewrites the prefix destroys the cache and looks like a cost regression.

### 4.3 `ContinuationPolicy` shape

```text
ContinuationPolicy {
  triggers: { context_ratio: 0.60, tool_payload_bytes, turn_count, ... }
  algorithms: [session_compact, tool_offload, subagent_pointers, ...]
  compacting_agent: { runtime, model, credit_account }  // often cheaper / faster
  checkpoint_schema: ContinuationCheckpoint@1
  preserve_cache_prefix: boolean
  max_continuations_per_execution
  fail_if_checkpoint_invalid: true
}
```

The compacting agent is a capability. It does not inherit the parent’s write authority. It may read the session and workspace and write only the checkpoint artifact.

---

## 5. Prompt caching

Caching is a cost governor, not an afterthought. Anthropic, OpenAI, and Gateway-routed models all have native cache primitives. The AI SDK exposes them as message-level `providerOptions` (for example Anthropic `cacheControl: { type: 'ephemeral' }`). Deep Agents already appends a prompt-caching middleware and can mark memory as cached.

Mission Control owns a **PromptCachePolicy** that adapters implement.

### 5.1 Rules that actually save money

1. **Stable prefix.** System prompt, tool schemas, admitted skill list, sandbox/workspace header, and mission IDs must be identical across turns of the same execution epoch. If the coordinator injects a queued instruction, append it *after* the breakpoint, or accept a cache miss.
2. **Breakpoints are explicit.** Mark the last stable block. Compaction must not shuffle bytes above that mark.
3. **Provider adapter translates.** Anthropic cache-control, OpenAI prompt-cache keys, Gateway equivalents — kernel policy is one; adapters emit the native field.
4. **Measure.** Every provider run records cache-hit tokens vs miss tokens on the credit ledger. Experiments of kind `prompt_cache_ablation` prove the policy.
5. **Do not cache secrets.** Cache prefixes contain references, never resolved credentials.

Tension with compaction: aggressive session-compact that rewrites the system prompt every time will zero out cache hits. Default for long coding nodes: tool-offload + cache-preserving compact first; full session-compact only when the tail is hopeless.

---

## 6. Workflow templates

Templates are parameterized, versioned authoring aids. They are not a second schema. The compiler expands a template into an ordinary `MissionDefinition`, then the coordinator may edit the draft before commit. That rule already exists in the implementation spec; keep it.

Catalog entries people will actually use:

- research flywheel (time-context → identity → thread → promote)
- application build → test → preview → improve
- course / challenge projection
- harness comparison experiment
- RSI after-commit optimizer
- “blank 20-node stage graph” (coordinator fills bindings)

Searchable like capabilities. `missionctl template expand` and the dashboard Advanced Composer both start from a template. Exemplars are immutable tested expansions used in CI.

---

## 7. File editing outside Cursor

Cursor already edits well. Everyone else must not fall back to rewriting whole files in the chat.

Introduce a **FileEdit harness** — a capability + sandbox-local apply engine — that non-Cursor runtimes share.

| Strategy | Where it comes from | Use |
|---|---|---|
| `apply_patch` / V4A diff | OpenAI Responses `apply_patch` tool (`create_file`, `update_file`, `delete_file`); report `apply_patch_call_output` on conflict / missing file | Direct OpenAI and any adapter that can emit the same operations |
| Cursor native edit | Cursor SDK / CLI | Cursor lane only |
| Search-replace / anchored hunk | Common coding-agent pattern | Fallback when the model cannot emit V4A |
| Whole-file write | Last resort | New files under size cap; never for large existing files |

The kernel contract is: the model proposes a **structured edit**; the sandbox applies it; the result (ok, conflict with context, file not found, hash mismatch) goes back as a tool result. The conversation stores the patch artifact (digest), not the whole new file.

This is also a Deep Agents / Eve / Claude Code gift: they get the same apply engine in the workspace. Catalog it as `FILE_EDIT` / `apply_patch@…`. Experiments of kind `harness_comparison` should include “Cursor native vs shared apply_patch” as an arm.

Do not send full file bodies through the coordinator peek channel. Peek returns paths and diffs.

---

## 8. Inter-agent and inter-node communication

Deep spawn makes shared chat impossible. The bus is **coalesced mission state** through the same three clients: Agent Skill, `missionctl`, MCP (and MCP UI).

That is already the pre-spec peek surface. This note only states the communication law:

1. **No child→parent token stream.** Children write artifacts and status. Parents peek.
2. **Pointers by default.** Subagents and child missions return artifact IDs, workspace paths, and verification dispositions.
3. **Scoped tokens.** A depth-3 agent can peek its subtree, not the sibling empire, unless granted.
4. **Commands are the only writes** (queue, interrupt, reconcile, spawn). Peek is read-only.
5. **Coalesce for the model.** The skill/MCP `mission.peek` returns a bounded view: graph head, active nodes, blockers, last N events, artifact inventory, budget, open human tasks. Raw event archives stay server-side.
6. **The spawning agent is the local coordinator.** At every depth the same tools exist. That is how a mission of unknown depth stays operable.

Do not add a second “agent message bus.” If two nodes must share a large intermediate, they share an artifact.

---

## 9. Source intelligence

Verification proves that a *claimed* extraction is bound to a locator. Source intelligence is the living map of **what we asked the world, what came back, and whether we should ask again**.

It is a first-class ledger beside artifacts, not a folder of bookmarks.

```text
SourceEncounter {
  source_id / capture_id
  query_or_reason            // why we touched it
  requested_at, observed_at
  method                     // search, browser, api, repo, pdf, interview
  result_digest              // what we got, immutable
  cost
  reuse_policy / freshness
  authority_vector           // from verification authority, when assessed
  downstream                 // claims, packets, missions that consumed it
}
```

What it enables:

- Gap planners skip redundant fetches (blueprint snapshot question: “which captures can be reused?”).
- Time-context missions enforce cutoffs (no post-T0 evidence in `historical_as_of`).
- Dashboard Knowledge search is this graph plus retrieval, not only vector hits.
- Verification binds claims to encounters; encounters without locators cannot support admission.
- Personal intelligence platforms inherit the same encounter log in `user_managed` space.

Knowledge Services already has sources, captures, locators, and evidence packets. Mission Control must **require an encounter record** whenever a harness searches, fetches, or scrapes — including Cursor and Deep Agents tool calls. The adapter emits a normalized encounter; the kernel stores it. That is “more than verification”: it is operational memory of the world’s interface.

---

## 10. Memory manager

Mission state is not memory. Transcripts are not memory. Memory is **curated, typed, retrieved on purpose**.

| Kind | What it holds | Written when | Retrieved when |
|---|---|---|---|
| **Semantic** | Durable facts, entity bindings, admitted claims, source encounters worth keeping | After admission or explicit memorize | Planner snapshots, research nodes, coordinator draft |
| **Procedural** | How we do things: file-edit strategy that worked, compaction settings, harness quirks, playbooks | After a successful pattern or an experiment seal | Node start, template expansion, adapter selection |
| **Episodic** | What happened in a mission: decisions, failures, interventions, continuations | On CONTINUE, reconcile, human interrupt, mission terminal | Fresh session start, RSI trigger, debugger “why are we here?” |

A **Memory Manager** is a kernel service (API + skill/MCP tools: `memory.write`, `memory.search`, `memory.pin`, `memory.forget`). It is not an unbounded second brain that every token is dumped into.

Rules:

- Writes are artifacts with kind + scope (`mission`, `tenant`, `user`, `official`).
- Retrieval is policy-gated and targeted (the `MemoryPolicy` on the node lists when to pull which kind, how many, and whether they enter the *cached* prefix).
- Deep Agents `MemoryMiddleware` is an adapter that reads/writes this service via `StoreBackend` routes such as `/memories/`. Eve and Cursor get the same tools.
- Semantic memory that is *knowledge* still goes through promotion before it can affect official retrieval or curriculum. Memory is not a back door around zones.
- Compaction may promote episodic → pointer and drop turns. That is the efficiency loop.

---

## 11. Shareable generalized platform

The end state is not “AI Engineer’s internal orchestrator.” It is a platform other people use to complete **big goals** with coordinator agents plus this control plane.

What must be separable from our company content:

| Portable (the product) | Ours (a module / catalog pack) |
|---|---|
| Mission kernel, compiler, peek, commands, spawn | Official flywheel templates |
| Capability catalog + admission | Our skill packs, MCP servers, research schemas |
| Adapters (Cursor, Eve, Deep Agents, direct APIs) | Our credit accounts and tenant branding |
| Continuation, cache, file-edit, memory, source encounters | Our `official_canonical` spaces |
| Verification and evaluation *interfaces* | Our gold sets and curriculum projections |
| Dashboard / MCP UI / skill / CLI / A2A | Biotech / AI Engineer filters as optional packs |

A third party should be able to: install `missionctl` + the skill, connect Temporal (or their orchestrator adapter), register *their* capabilities, pick a template, and run a coordinator against *their* goal. Official AI Engineer is the first tenant and the hardest module pack, not the ceiling.

That is why the kernel stays provider-neutral even while we handle Eve, Deep Agents, and Cursor explicitly. The explicit parts are **packs**. The mission is the product.

---

## 12. What this changes in the pre-spec (no rewrite)

When the lock pass happens, fold these into the kernel document as:

- execution dimension `credit_account_kind` and `deep_agents` runtime
- `SandboxConfig` / `SandboxRef` beside workspace templates
- `Experiment` as a goal family
- `ContinuationPolicy` algorithms and triggers
- `PromptCachePolicy`
- `FileEditPolicy` / shared apply engine
- `SourceIntelligencePolicy` + encounter ledger
- `MemoryPolicy` + memory manager
- templates as catalog, already implied
- shareable platform as the packaging bar

Until then, this file is the authority for those topics. Do not open a third architecture note for the same questions.
