# Mission Control — System Completion Proposal (v3 candidate)

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

**Status:** proposed for Coordinator sign-off — not architecture authority until accepted  
**Date:** 2026-09-28  
**Builds on:** [`MISSION_CONTROL_ARCHITECTURE.md`](./MISSION_CONTROL_ARCHITECTURE.md) (v2), [`workflow-types/`](./workflow-types/index.md) `00`–`09`, [`M0_M8_PROGRAM.md`](./M0_M8_PROGRAM.md), ADRs `0001`–`0005`, [`runtime-facts/`](./runtime-facts/), [`2026-09-19_CONTROL_PLANE_EXPERIENCE_AND_SANDBOX_SEED.md`](./2026-09-19_CONTROL_PLANE_EXPERIENCE_AND_SANDBOX_SEED.md)  
**Does not amend** any accepted document by itself. Every accepted decision (`D-xx`) becomes a dated amendment in the owning document, in `CONTEXT.md`, and — where hard to reverse — an ADR.  
**Rendered copy:** the Mission Control dashboard serves this document at `/specification` (`ai-engineer-mission-control/apps/dashboard`).

This document completes the Mission Control architecture where the accepted suite is silent or thin: naming law for every contract and piece of infrastructure, the full contract catalog, the event plane and its storage, the real-time streaming architecture, Mission Control as an Agent Skill with a CLI and MCP server, the Subagent tool, human-in-the-loop review of deliverables, and searchable, navigable Mission workspaces. It is written to be signed off section by section and sent back with review considerations.

---

## 0. Reading guide

### 0.1 The asks and where they are answered

| Ask | Part |
|---|---|
| Sign off the critical naming conventions for contracts, schemas, and infrastructure | **A** |
| Final recommendation for every contract of the system: purpose, features, assumptions | **B** |
| Use the software infrastructure we already have, optimally, hooking into library lifecycles | **C**, **D.7** |
| Store workflow event activity and agent event activity optimally (including Eve lifecycles) | **D.1–D.4** |
| WebSocket / real-time streaming architecture and every event handler, documented and reasoned | **D.5–D.6**, **H.2** |
| Mission Control as an Agent Skill with CLI, API, and MCP server; coordinator inspection and intervention; real-time tracing for agents; the custom Subagent tool distinct from Mission spawn | **E** (written by the dedicated subagent, integrated here) |
| Human-in-the-loop middleware for major completions: review, approve, promote, reject | **F** |
| Workflow spawning and structure searchable and retrievable by agents and consumers; navigable workspaces and schema context selection | **G** |
| The dashboard as a personal control plane, promotable to a sellable service, up as soon as possible | **H** |
| What this changes in the M0–M8 program | **I** |
| Everything to sign, with review considerations | **J** |

### 0.2 How decisions are presented

Every decision is numbered `D-<part><n>`. Each states a **Recommendation**, the **Why**, the **Alternative rejected**, and **Reversibility** (`cheap`, `moderate`, `hard`). Accepted vocabulary from `CONTEXT.md` is used verbatim and is not re-litigated; where this proposal changes an accepted literal it says so explicitly and proposes the dated amendment.

### 0.3 The honest baseline (surveyed 2026-09-28)

| Area | Reality today |
|---|---|
| `ai-engineer-mission-control` kernel | One real vertical: `verificationWorkflow` dispatching Knowledge Services operations via Temporal. `missionWorkflow` is a stub that always fails. No Signals, Queries, or Updates. No database client. No outbox. No Mission Events. |
| API | Fastify routes for verification executions only; bearer tokens from `MISSION_VERIFICATION_IDENTITIES_JSON`; scope via `x-tenant-id` / `x-mission-id` headers. No streaming route. |
| MCP server | One tool, `mission_service_status`; stateless Streamable HTTP; no auth. |
| `missionctl` | One command, `status`. |
| Contracts | `MissionStateSchema` still collapses lifecycle and outcome into one enum (`draft … succeeded`). Wire keys are camelCase (inherited from Knowledge Services). |
| Schema | No `mission_control` schema exists. `orchestration.mission / work_item / attempt` carry the old single-`status` model. IDs are `uuid` with `util.uuidv7()`. Outbox relays poll; no `LISTEN/NOTIFY`, no Realtime publication. |
| Knowledge Services boundary | Polled JSON `GET /v1/operations/{id}/events?after=` (no push). camelCase contracts, colon custom methods, `knowledge_*` MCP tools, `noun.verb_past` events. |
| Dashboard | Next.js 16.3.4, TanStack Query polling every 3 s, server-only proxy with a closed path allowlist, HMAC session cookie, no markdown renderer, no WebSocket. |
| Eve in the workspace | `research_ingestion_systems_agent` uses `defineHook` wildcard handlers observing `session.* turn.* step.* actions.requested action.result subagent.called subagent.completed`; no Mission Control adapter exists. |

Nothing above is a constraint on design (architecture invariant 11). It is the distance this proposal has to cover.

---

## Part A — Naming law for contracts, schemas, and infrastructure

The suite already fixes one vocabulary (`CONTEXT.md`, `00 §6`) and one law: contract fields and ledger columns carry the exact vocabulary names. Part A extends that law to every surface a name can appear on, so a term is spelled once and recognized everywhere — by the compiler, Postgres, Temporal, the API, the CLI, the MCP server, the dashboard, and the agent reading the specification.

### A.1 One vocabulary, eleven surfaces

| Surface | Casing | Example | Rule |
|---|---|---|---|
| Vocabulary term (prose) | Capitalized Words | Mission Run, Terminal Outcome | `CONTEXT.md` owns it |
| Vocabulary literal (value) | `lower_snake` | `not_accepted`, `turn_boundary_guaranteed` | identical in Zod, Postgres enum, JSON, UI label source |
| Contract field / JSON key | `lower_snake` | `terminal_outcome`, `after_seq` | **D-A1** |
| Zod schema constant / TS type | `PascalCase` + `Schema` / bare | `MissionEventSchema` / `MissionEvent` | one aggregate per file |
| TS function / variable | `camelCase` | `appendMissionEvents` | verb lexicon **A.10** |
| Postgres schema / table / column / enum | `lower_snake` | `mission_control.activation.terminal_outcome` | **A.6** |
| Temporal workflow type / activity | `camelCase` function names; string names `lower_snake` | `missionRunWorkflow`, update `"activate_revision"` | **A.7** |
| Temporal workflow id / task queue / search attribute | `kebab:` id prefix, `kebab-case` queue, `PascalCase` attribute | `mission-run:<run_id>`, `harness-eve`, `MissionRunId` | **A.7** |
| HTTP path | plural `kebab-case` nouns, `{lower_snake}` params | `/v1/missions/{mission_id}/runs/{run_id}/events` | **A.8** |
| CLI | `missionctl <noun> <verb>`; flags `--kebab-case` | `missionctl command queue-instruction --after-seq 42` | **A.8** |
| MCP tool / resource | `mission_<verb>`; `mc://` URIs | `mission_peek`, `mc://missions/{mission_id}/runs/{run_id}` | **A.8** |
| Mission Event / Command / Executor Kind | accepted | `activation.completed` / `queue_instruction` / `verification_dispatch` | **A.9** |
| Environment variable | `SCREAMING_SNAKE` | `MISSION_CONTROL_LEDGER_URL`, `TEMPORAL_ADDRESS` | **A.12** |
| npm package / directory | `@aiengineer/mission-<role>` / `packages/<role>` | `@aiengineer/mission-harness-eve` / `packages/harness-eve` | **A.11** |

### A.2 Wire casing

**D-A1 — Mission Control's own contracts are `lower_snake` on the wire, in the ledger, and in the specification; TypeScript identifiers stay `camelCase`.**

- **Recommendation.** Every JSON key produced or consumed by the Mission Control HTTP API, Mission Event stream, MCP tools and resources, `missionctl --json`, Agent Skill examples, Continuation Checkpoints, and dashboard DTOs is `lower_snake`, spelled exactly as in `workflow-types/`. Zod object keys are `lower_snake`; the TypeScript type inferred from them therefore carries `lower_snake` properties, which is accepted. Local variables, functions, classes, and non-contract objects remain `camelCase`.
- **Why.** The suite is written in `lower_snake` (`event_id`, `after_seq`, `node_key`, `terminal_outcome`). Postgres columns are `lower_snake`. A single spelling means zero casing transforms between the specification, the ledger, the wire, the CLI flag, and the label the dashboard renders, and the M1 contract fixture test compares literals without a mapping table. Agents read the specification and the JSON side by side; identical spellings remove a whole class of tool-call errors.
- **Alternative rejected.** `camelCase` to match Knowledge Services contracts and the current verification-dispatch code. Rejected because Mission Control is the product surface agents install, and the specification would then differ from every payload. Knowledge Services keeps its own conventions; the translation happens once, inside `@aiengineer/mission-knowledge` (the `KnowledgePort` adapter, A.11), never in kernel code or the dashboard.
- **Consequence.** The existing `/v1/verification/*` routes are a Knowledge Services projection and keep their camelCase shape until the `verification_dispatch` Executor Kind replaces them (M4). They are labelled `knowledge_projection` in `GET /v1/system/describe`.
- **Reversibility.** moderate (a codemod on Zod keys before M3; hard after clients exist).

### A.3 Identity and reference grammar

**D-A2 — Every aggregate identifier is a UUIDv7 named `<aggregate>_id` everywhere; there are no type-prefixed ids and no bare `id` columns.**

- Postgres primary key column is `mission_id`, `run_id`, `activation_id`, … (not `id`), default `util.uuidv7()`. The same name appears in JSON, events, search attributes, and CLI output, so `activation_id` is unambiguous in any join, payload, or log line. UUIDv7 gives creation ordering for free and matches the existing db-contract convention.
- Native identifiers from other systems are `text` and named `native_<thing>_ref` (`native_session_ref = "wrun_…"`, `native_turn_ref = "turn_3"`, `native_run_ref = "run-…"`). They are references, never identities.
- **Alternative rejected.** Prefixed string ids (`msn_01H…`). Readable in logs, but they create a second id format beside the db-contract's `uuid` columns and the KS `UuidSchema`. Readability is delivered by the `mc://` reference grammar instead (D-A3).

**D-A3 — One reference grammar (`mc://`) addresses every Mission Control resource identically on HTTP, MCP, the CLI, MissionFS, and inside Artifacts.**

```text
mc://missions/{mission_id}
mc://missions/{mission_id}/revisions/{revision_id}
mc://missions/{mission_id}/revisions/{revision_id}/program/{node_key}
mc://missions/{mission_id}/runs/{run_id}
mc://missions/{mission_id}/runs/{run_id}/activations/{activation_id}
mc://missions/{mission_id}/runs/{run_id}/activations/{activation_id}/attempts/{attempt_no}
mc://missions/{mission_id}/runs/{run_id}/events?after_seq={n}
mc://missions/{mission_id}/deliverables/{deliverable_id}
mc://artifacts/{artifact_id}
mc://human-tasks/{human_task_id}
mc://commands/{command_id}
mc://schemas/{schema_name}
mc://exemplars/{exemplar_name}
mc://patterns/goal/{pattern_name}@{version}
mc://profiles/subagent/{profile_name}@{version}
```

The path after `mc://` is byte-identical to the HTTP path after `/v1/`, to the MissionFS path after `/mission-control/`, and to the MCP resource URI. A field holding such a reference is named `<thing>_ref` (`artifact_ref`, `checkpoint_ref`, `portal_ref`). A field holding a same-store foreign key is `<thing>_id`. A field holding a content hash is `<thing>_digest` with value `sha256:<64 hex>` (matching the KS `Sha256DigestSchema`; the db-contract's unprefixed `sha256` columns are wrapped at the ledger adapter).

Suffix law: `_id` (identity in this ledger) · `_ref` (reference into any store, `mc://` or native) · `_digest` (content hash) · `_at` (instant) · `_no` (ordinal within a parent: `attempt_no`; `entry_seq` and `seq` are the accepted exceptions) · `_kind` (closed vocabulary that classifies) · `_policy` (declared behavior) · `_ceiling` / `_cap` (governor limit).

### A.4 Vocabulary literals and enum types

**D-A4 — Every state vocabulary is exported once as a `const` tuple in `@aiengineer/mission-contracts`, and the Postgres enum type carries the vocabulary's name.**

```ts
export const ACTIVATION_LIFECYCLE = ["pending", "ready", "running", "waiting", "completed"] as const;
export const ActivationLifecycleSchema = z.enum(ACTIVATION_LIFECYCLE);
export type ActivationLifecycle = (typeof ACTIVATION_LIFECYCLE)[number];
```

Postgres: `create type mission_control.activation_lifecycle as enum ('pending', …)`. The M1 fixture test reads `pg_enum` and asserts equality with the tuples. Enum type names are the vocabulary names from the architecture §19 index: `activation_lifecycle`, `terminal_outcome`, `cycle_decision`, `attempt_outcome`, `failure_class`, `side_effect_class`, `operation_receipt_outcome`, `human_task_kind`, `human_task_lifecycle`, `human_task_resolution`, `event_wait_mode`, `invocation_mode`, `attachment_sub_mode`, `await_policy`, `portal_access`, `mission_relationship_kind`, `mission_lifecycle`, `closure_outcome`, `run_lifecycle`, `proposal_lifecycle`, `proposal_resolution`, `revision_head_status`, `transition_impact`, `carry_forward_eligibility`, `command_kind`, `command_lifecycle`, `command_outcome`, `delivery_semantics`, `event_source_kind`, `authority_scope`, `agent_runtime_kind`, `orchestrator_kind`, `model_access_kind`, `compute_environment_kind`, `credit_account_kind`, `program_node_behavior`, plus the vocabularies introduced by Parts E–G (`subagent_invocation_origin`, `deliverable_kind`, `deliverable_review_state`, `stream_channel_kind`).

**D-A5 — Human Task kinds are normalized to `lower_snake`: `approval | question | selection | review | policy_override`.**

The accepted suite spells them `APPROVAL | QUESTION | …` (`05 §6.3`), the only upper-case vocabulary in the system. Normalizing removes the one exception to the literal law. Dated amendment to `05 §6.3`, `05 §7`, `09 §3`, and `CONTEXT.md`. Reversibility: cheap before M1.

### A.5 Zod, TypeScript, and versioning

- One aggregate per file, file named by the aggregate in `kebab-case`: `mission-event.ts` exports `MissionEventSchema`, `MissionEvent`, and its payload schemas `MissionEventPayloadSchemas` keyed by event type.
- Every envelope-level contract carries `contract_version: "<name>.v1"` (`"mission_definition.v1"`, `"mission_event.v1"`, `"command.v1"`, `"continuation_checkpoint.v1"`, `"peek_view.v1"`, `"trace_digest.v1"`, `"review_packet.v1"`, `"subagent_profile.v1"`, `"context_pack.v1"`). Nested contracts inherit the envelope version. A breaking change creates `<name>.v2` beside `v1`; both are exported until the deprecation date recorded in the file header. Schema constant names stay unversioned unless two versions coexist, in which case the older gains a suffix (`MissionEventSchemaV1`).
- `z.strictObject` at every contract boundary (no unknown keys reach the ledger). `z.uuid()` for ids; a shared `Sha256DigestSchema`, `McRefSchema` (`mc://…` grammar), and `InstantSchema` (RFC 3339 UTC, millisecond precision).
- JSON Schema is generated from Zod at build time into `catalog/schemas/<name>.v1.json`; `GET /v1/schemas/{schema_name}` and `mc://schemas/{schema_name}` serve those files. Nothing hand-writes JSON Schema.
- Type-only consumption of the database: `import type { Database } from "@aiengineer/database-contract"`; row types are never re-exported through the contracts package. The contracts package is the wire truth; the database contract is the storage truth; the ledger adapter is the only place both meet.

### A.6 Postgres

Schema `mission_control` (ADR 0005). Rules, matching the db-contract's own conventions where they exist:

| Rule | Form |
|---|---|
| Table | singular aggregate noun: `mission`, `mission_run`, `activation`, `attempt`, `harness_execution`, `agent_session`, `session_turn`, `mission_event`, `native_event`, `command`, `delivery_report`, `human_task`, `deliverable`, `outbox` |
| Primary key | `<table>_id uuid primary key default util.uuidv7()`; tenant tables also `unique (tenant_id, <table>_id)` |
| Foreign key | `<referenced_table>_id`, composite `(tenant_id, <x>_id)` on tenant-scoped rows |
| Ordinal | `seq bigint` (per-Mission event order), `attempt_no int`, `entry_seq int` |
| Three-field state | columns `lifecycle`, `phase`, `terminal_outcome` — never `status` |
| Instant | `<verb>_at timestamptz`: `occurred_at`, `recorded_at`, `sealed_at`, `resolved_at`, `expires_at`; `created_at` / `updated_at` only on mutable rows with the `util.set_updated_at` trigger |
| Immutable rows | `util.reject_mutation()` trigger on `mission_event`, `native_event`, `mission_revision`, `compiled_program`, `delivery_report`, `operation_receipt`, `continuation_checkpoint`, `journal_segment` |
| Enum | `mission_control.<vocabulary>` (A.4); vocabularies still moving (`executor_kind`, `deliverable_kind`) are `text` + lookup table with `code` primary key, the db-contract's own pattern |
| Constraint / index | `<table>_<purpose>_(pk|uq|idx|fk|ck)` |
| Digest column | `<thing>_digest text check (<thing>_digest ~ '^sha256:[0-9a-f]{64}$')` |
| Partition | `native_event` range-partitioned by `recorded_at` (monthly); retention by partition drop (D-D4) |
| Access | `control_plane` role writes; `app_reader` reads; RLS `tenant_id = util.current_tenant_id()` as the second lock, exactly like the other schemas |

### A.7 Temporal

| Element | Convention | M0–M8 names |
|---|---|---|
| Workflow function | `camelCase` noun ending in `Workflow` | `missionRunWorkflow`, `compositeActivationWorkflow`, `atomicActivationWorkflow` |
| Workflow id | `<aggregate-kebab>:<uuid>` | `mission-run:<run_id>`, `activation:<activation_id>` |
| Update / Signal / Query string name | `lower_snake`, identical to the spec | updates `command`, `activate_revision`; signal `world_event`; query `peek` |
| Activity | `camelCase` verb phrase from the lexicon (A.10); named for its effect | `appendLedgerRecords`, `prepareHarnessExecution`, `startSessionTurn`, `sendTurnInstruction`, `observeNativeEvents`, `snapshotWorkspace`, `sealContinuationCheckpoint`, `cancelSessionTurn`, `recordOperationReceipt`, `evaluateCompletionContract`, `openHumanTask`, `dispatchVerification`, `emitTraceDigest` |
| Task queue | `<role>-<lane>` kebab | `mission-kernel`, `harness-cursor-cloud`, `harness-eve`, `harness-direct-model`, `executor-verification`, `executor-artifact`, `executor-workspace` |
| Search attribute | `PascalCase` (Temporal convention) | `TenantId`, `MissionId`, `MissionRunId`, `RunLifecycle`, `HeadRevisionId` |
| Schedule id | `<purpose>:<scope>` | `mission-run-recurrence:<mission_id>`, `native-event-retention:<tenant_id>` |
| Memo keys | `lower_snake` | `mission_id`, `run_id`, `revision_id`, `input_snapshot_digest` |

**D-A6 — Task queues are role-prefixed (`harness-eve`, `executor-verification`) instead of the bare `eve`, `verification` listed in architecture §10.** Cheap amendment; the prefix tells an operator what a stuck queue is.

### A.8 HTTP, CLI, and MCP grammar

**D-A7 — HTTP: resources and sub-resources only; Commands are a resource; no colon custom methods; ids are `{<aggregate>_id}` path params.**

```text
GET  /v1/system/describe
GET  /v1/schemas · GET /v1/schemas/{schema_name}
GET  /v1/exemplars · GET /v1/exemplars/{exemplar_name}
GET  /v1/patterns/{pattern_kind} · GET /v1/profiles/subagent

POST /v1/missions                                    create Mission + first draft definition
GET  /v1/missions?lifecycle=&owner=&q=&after=          list / filter
GET  /v1/missions/{mission_id}                        summary
PUT  /v1/missions/{mission_id}/draft                  private authoring state (mission.author)
POST /v1/missions/{mission_id}/validations            ValidationReport for a definition
POST /v1/missions/{mission_id}/proposals · GET …/proposals/{proposal_id}
POST /v1/missions/{mission_id}/proposals/{proposal_id}/resolution   {resolution}
POST /v1/missions/{mission_id}/head                   {revision_id}   → activate_revision
GET  /v1/missions/{mission_id}/revisions/{revision_id} · …/program · …/transition-impact
POST /v1/missions/{mission_id}/runs                   start a Run {revision_id?, inputs}
GET  /v1/missions/{mission_id}/runs/{run_id} · …/program · …/traceability · …/artifacts · …/journal · …/peek
GET  /v1/missions/{mission_id}/events?after_seq=&types=&node_key=&wait_ms=&max=   (JSON page, or SSE via Accept)
POST /v1/missions/{mission_id}/commands · GET …/commands/{command_id}
POST /v1/missions/{mission_id}/children               Spawn (mission.invoke)
POST /v1/missions/{mission_id}/attachments            Attachment {sub_mode, existing_mission_ref}
POST /v1/missions/{mission_id}/closure                {closure_outcome}
GET  /v1/missions/{mission_id}/deliverables · GET /v1/deliverables/{deliverable_id}

GET  /v1/human-tasks?assignee=&lifecycle=&kind=  · GET /v1/human-tasks/{human_task_id}
POST /v1/human-tasks/{human_task_id}/claim · POST /v1/human-tasks/{human_task_id}/resolution

GET  /v1/native-events?harness_execution_id=&after=   non-canonical tail (mission.trace)
GET  /v1/fs/{path…}                                    MissionFS read (Part G)
POST /v1/searches                                      MissionSearchQuery → MissionSearchResult
POST /v1/context-packs                                 ContextPackRequest → ContextPack
GET  /v1/credit-accounts

POST /v1/stream-tokens                                 short-lived scoped token for the Stream Gateway
WS   /v1/stream                                        multiplexed subscriptions (Part D.5)
POST /v1/internal/harness-events                       hook ingress (Eve defineHook, Cursor/Claude hooks)
```

Errors are Problem+JSON (`application/problem+json`) with `code` in `SCREAMING_SNAKE` (`GRANT_SCOPE_MISSING`, `PROPOSAL_STALE`, `DELIVERY_UNSUPPORTED`). Mutations require `Idempotency-Key` or an embedded `command_id` / `proposal_id`. `Accept: text/event-stream` on the events route switches to SSE; the same handler serves both.

**D-A8 — `missionctl <noun> <verb>`; nouns are aggregates; JSON on stdout when `--json` or non-TTY; human text on stderr.**

Nouns: `system`, `schema`, `exemplar`, `mission`, `draft`, `proposal`, `revision`, `run`, `program`, `events`, `command`, `human-task`, `deliverable`, `artifact`, `subagent`, `fs`, `search`, `context`, `trace`. Verbs are the lexicon's imperative forms: `describe`, `get`, `list`, `create`, `validate`, `submit`, `resolve`, `activate`, `start`, `peek`, `tail`, `queue-instruction`, `interrupt`, `pause`, `resume`, `cancel`, `retry`, `rerun`, `fork`, `claim`, `ls`, `cat`, `find`, `select`, `emit`. Example: `missionctl events tail <mission_id> --after-seq 120 --follow`. The architecture's flat verb list (§13) is retired; the Skill teaches the noun-verb form.

**D-A9 — MCP tools are `mission_<verb>` or `mission_<aggregate>_<verb>`, at most 24, task-oriented; resources are `mc://` URIs; the server name is `ai-engineer-mission-control`.** The full tool set is fixed in Part E. Rationale: a host merges tools from many servers into one namespace; the `mission_` prefix is the namespace, and the verb-first tail reads as an action in a tool list.

### A.9 Events, Commands, Executor Kinds (accepted; codified)

- Mission Event: `<aggregate>.<past_tense_verb>` (`09 §3`). Families added by this proposal: `subagent` (E), `deliverable` (F), `search_index` (G). The rule "one aggregate per family" holds.
- Command: imperative `verb_object` (`queue_instruction`). No new Commands are proposed; Subagent invocation and Deliverable review are not Commands (they are a Program Node behavior and a Human Task respectively).
- Executor Kind: `object_verb` (`verification_dispatch`, `artifact_register`, `test_run`). New kinds: `trace_digest`, `review_packet_assemble`, `code_promote`, `report_promote`, `search_index_publish`, `context_pack_assemble`. Noun-first because kinds are catalog rows sorted by domain.
- Native event types keep their provider spelling (`turn.completed`, `FINISHED`) inside the Native Event Store and are never renamed; mapping to Mission Events is a function (`toAttemptOutcome`), not a rename.

### A.10 Functions and modules: names that carry their assumptions

**D-A10 — A verb lexicon governs every exported function; each verb fixes the function's side-effect class, error behavior, and return shape.**

| Verb | Side effects | On failure | Returns | Example |
|---|---|---|---|---|
| `validate` | none | never throws for domain errors | `ValidationReport` | `validateMissionDefinition(definition)` |
| `compile` | none, deterministic | throws `CompileError` only on input `validate` already rejected | `CompiledProgram` | `compileProgram(definition)` |
| `compute` | none | throws on programmer error only | value | `computeTransitionImpact(base, successor)` |
| `assert` | none | throws a typed error naming the broken invariant | `void` | `assertRevisionIsHead(revision)` |
| `require` | none | throws `GrantDenied` / `NotFound` | the required record | `requireGrant(principal, "mission.command")` |
| `resolve` | read | throws `NotFound` | the dereferenced record | `resolveArtifactRef(ref)` |
| `observe` | read of an external system | throws `ObservationFailed` (retryable) | observation with cursor | `observeNativeEvents(ref, cursor)` |
| `project` | read of the ledger | — | read model | `projectPeekView(runId, scope)` |
| `append` | ledger write with sequence | throws `SequenceConflict` | assigned `seq` range | `appendMissionEvents(events)` |
| `record` | exactly-once durable write (idempotent on its key) | throws `RecordConflict` on a differing duplicate | the recorded row | `recordOperationReceipt(intent, outcome)` |
| `seal` | finalize an immutable object | throws if already sealed | digest | `sealJournalSegment(segment)` |
| `open` / `resolve` (Human Task) | durable write | — | task / resolution row | `openHumanTask(spec)` |
| `start` / `cancel` / `send` | side effect on a harness or external system | throws `DeliveryFailed` with `failure_class` | native ref / `DeliveryReport` | `startSessionTurn(ref, instruction)` |
| `deliver` | Command delivery attempt | never throws; reports `unsupported` | `DeliveryReport` | `deliverCommand(command, harness)` |
| `ensure` | idempotent create-or-get | — | the record | `ensureMissionEveDeployment(tenant)` |
| `is` / `can` / `has` | none | — | `boolean` | `isRetryableFailureClass(cls)`, `canReleaseStage(stage, facts)` |
| `to` / `from` | none, pure mapping across a seam | throws on unknown literal | mapped value | `toAttemptOutcome(nativeStatus)` |
| `decide` | none | — | a decision object with `reason`, never a bare boolean | `decideActionAuthorization(proposal, grants)` |

Prohibited in exported names: `handle`, `process`, `manage`, `do`, `util`, `helper`, `data`, `info`, `item`, `status` (use `lifecycle`, `phase`, `terminal_outcome`), `check` (use `validate` or `assert`), `update` (name the transition), `get` for I/O (use `resolve`, `observe`, `project`), `run` and `execute` as verbs (they collide with Mission Run and Harness Execution; Temporal SDK names are exempt).

Modules: one aggregate per directory (`src/activation/`), the directory's `index.ts` exports the interface only (`codebase-design` skill: deep module, small interface). Ports are interfaces named `<Concern>Port` (`LedgerPort`, `HarnessPort`, `StreamPort`, `KnowledgePort`, `ObjectStorePort`); adapters are `<Technology><Concern>Adapter` (`PostgresLedgerAdapter`, `InMemoryLedgerAdapter`, `EveHarnessAdapter`, `CursorCloudHarnessAdapter`, `TemporalKernelAdapter`). Errors are `<Concern>Error` classes carrying `code: "<AREA>_<REASON>"` (`LEDGER_SEQUENCE_CONFLICT`, `HARNESS_DELIVERY_UNSUPPORTED`). A function whose name is in the lexicon needs no doc comment for its contract; one that is not is a review finding.

### A.11 Packages and directories

**D-A11 — Directory name equals the package suffix: `packages/<role>` publishes `@aiengineer/mission-<role>`; apps publish `@aiengineer/mission-<app>`.**

| Directory | Package | Responsibility |
|---|---|---|
| `packages/contracts` | `@aiengineer/mission-contracts` | Zod contracts, vocabularies, JSON Schema generation |
| `packages/compiler` | `@aiengineer/mission-compiler` | validate, compile, digests, Transition Impact |
| `packages/kernel` | `@aiengineer/mission-kernel` | application services, ports, acceptance evaluation, Command ledger |
| `packages/policy` | `@aiengineer/mission-policy` | grants, governors, Side-Effect Classes, Spawn Grants |
| `packages/ledger-postgres` | `@aiengineer/mission-ledger-postgres` | `LedgerPort` adapter, outbox, Native Event Store, read models |
| `packages/stream` | `@aiengineer/mission-stream` | Stream Gateway: subscriptions, SSE/WS/long-poll, replay |
| `packages/harness` | `@aiengineer/mission-harness` | `AgentHarness` contract, Harness Execution, status mapping, Native ingestion |
| `packages/harness-cursor-cloud` · `harness-eve` · `harness-direct-model` | `@aiengineer/mission-harness-<lane>` | one adapter per lane |
| `packages/executors` | `@aiengineer/mission-executors` | Executor Kind registry and implementations |
| `packages/knowledge` | `@aiengineer/mission-knowledge` | `KnowledgePort` adapter over `@aiengineer/knowledge-client` (the only camelCase seam) |
| `packages/missionfs` | `@aiengineer/mission-fs` | MissionFS projection and search index publication (Part G) |
| `packages/missionctl` | `@aiengineer/missionctl` | CLI |
| `packages/testkit` | `@aiengineer/mission-testkit` | fixtures, in-memory adapters, fixed clock |
| `apps/api` · `apps/worker` · `apps/mcp-server` · `apps/dashboard` | `@aiengineer/mission-api` · `-worker` · `-mcp` · `-dashboard` | processes |
| `skills/mission-control` | `@aiengineer/mission-control-skill` | Agent Skill and host overlays |
| `plugin/` | `@aiengineer/mission-control-plugin` | plugin manifests (Part E) |

The current `packages/mission-*` directories drop the redundant prefix; `packages/config` already fits (`@aiengineer/mission-config`).

### A.12 Environment and configuration

Own configuration is `MISSION_CONTROL_<AREA>_<NAME>` (`MISSION_CONTROL_LEDGER_URL`, `MISSION_CONTROL_STREAM_TOKEN_SECRET`, `MISSION_CONTROL_NATIVE_EVENT_RETENTION_DAYS`). Vendor SDK variables keep the vendor's expected names (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `CURSOR_API_KEY`, `AI_GATEWAY_API_KEY`). Knowledge Services client variables keep `KNOWLEDGE_API_URL` / `KNOWLEDGE_API_TOKEN`. JSON-valued variables end in `_JSON`. The dashboard keeps `DASHBOARD_*` for its own server-only settings and never exposes `NEXT_PUBLIC_*` secrets. `loadMissionControlConfig()` validates all of it with one Zod schema and fails closed in production.

### A.13 Enforcement

1. **Contract fixture test (M1):** vocabulary tuples ↔ `pg_enum` ↔ generated JSON Schema, byte-equal.
2. **Naming lint:** an ESLint rule set in `packages/config/eslint` enforcing the lexicon on exported function names, the prohibited-word list, the `Schema` suffix on Zod constants, and `lower_snake` keys inside `z.strictObject` literals in `packages/contracts`. The dashboard's current `eslint.config.mjs` ignores all TypeScript; it stops doing so.
3. **Route grammar test:** every registered Fastify route matches `^/v1/[a-z-]+(/\{[a-z_]+_id\}|/[a-z-]+)*$`, and every MCP tool name matches `^mission_[a-z_]+$`.
4. **Docs law:** a term used in code but absent from `CONTEXT.md` fails the `agent-docs` check once the vocabulary index is machine-readable (M1 deliverable: `catalog/vocabulary.json` generated from the contracts package).

---
## Part B — The contract catalog

Every contract of the system, in one place, with its purpose, the features it carries, and the assumptions a caller may rely on. A row says what the contract *is*; the owning suite document says how it *behaves*. Contracts marked **new** are introduced by this proposal (Parts D–G); the rest exist in the accepted suite and are catalogued here for completeness and sign-off of their final names.

Column law: **Assumes / guarantees** lists what a caller must supply and what the producer promises; when a function name in A.10 fully expresses it, the row says so.

### B.1 Authoring and program contracts (owner: `workflow-types/00–06`, contracts package)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `MissionDefinition` | Authored intent for one Mission | `contract_version`, `goals[]`, `program` (recursive `ProgramNode`), `inputs`, `policies`, `budgets`, `proof_policy`, `deliverables[]` (**new**, F) | Validated and stored before compile; never executed directly; digest = `definition_digest` | Coordinator, Skill, templates → compiler, ledger | M1 |
| `Goal` / `Objective` | Outcome contract and its operational tree | `goal_id`, `importance`, `success_criteria[]`, `evidence_requirements[]`, `objectives[]` (tree) | An Objective has exactly one parent; non-executable | authoring → compiler, traceability | M1 |
| `GoalPattern` / `ObjectivePattern` | Versioned reusable semantic patterns | `pattern_name`, `version`, `parameters`, `body` | Instantiation records source lineage | catalog → authoring | M1 (schema), M8+ (catalog) |
| `ProgramNode` | Universal typed unit with exactly one behavior | `node_key`, `objective_refs[]`, `inputs`, `outputs`, `importance`, `behavior` (discriminated: `stage_graph`, `goal_loop`, `parallel_swarm`, `evaluator_optimizer`, `agent_executor`, `deterministic_executor`, `subagent` (**new**, E), `event_wait`, `timer`, `human_gate`, `proof_gate`, `child_mission_invocation`), `completion_contract`, `budget_scope`, `policies` | `node_key` unique per Revision; behavior union is closed; nesting explicit | authoring → compiler | M1 |
| `StageGraphDeclaration` · `GoalLoopDeclaration` · `ParallelSwarmDeclaration` · `EvaluatorOptimizerDeclaration` | The four workflow systems | per `01`–`04` | Governors mandatory; phases fixed per system | authoring → compiler, kernel | M1 |
| `AgentExecutorBinding` | How one Agent Executor runs | `harness{5 dimensions}`, `instruction{objective_refs, operating_contract_ref, instruction_text}`, `workspace`, `output_contract`, `budget`, `retry_policy`, `session_policy`, `context_health_policy_ref`, `continuation_policy_ref`, `missing_output_policy`, `capability_binding`, `subagent_policy` (**new**), `trace_push_policy` (**new**), `context_seed_refs[]` (**new**, G) | Five dimensions mutually consistent (validated); `capability_binding: none` through M8 | authoring → compiler, harness | M1 |
| `OperatingContract` / `LoopOperatingContract` | Versioned rules given to an agent | `contract_ref`, `version`, sections per `05 §3.2` | Materialized per lane; never a prompt string alone | catalog → harness | M1, M5 |
| `CompletionContract` | How an activation is accepted | `required_outputs[]`, `validity_rules[]`, `verification_intents[]`, `tests[]`, `acceptable_dispositions[]`, `gates[]`, `acceptance_expression` | Mission Control evaluates (`evaluateCompletionContract`); never the agent | authoring → kernel | M1 |
| `ExecutorKindDescriptor` | Registered deterministic work | `executor_kind`, `version`, `input_schema_ref`, `output_schema_ref`, `side_effect_class` | Versioned; invocations idempotent on `(activation_id, attempt_no, executor_kind, input_digest)` | executors package → compiler, kernel | M1, M4 |
| `EventWait` · `Timer` · `HumanGate` · `ProofGate` | Durable Controls | per `05 §6` | Always present in the Compiled Program even when synthesized | authoring/compiler → kernel | M1, M4 |
| `ChildMissionInvocation` | Mission-boundary node | `mode`, `child_definition_source`, `input_bindings[]`, `await_policy`, `on_parent_cancel`, `spawn_grant_ref`, `portal{access}` | Requires a Spawn Grant; Portal never carries bodies | authoring → kernel | M1, M8 |
| `SpawnGrant` | Pinned authority to create Child Missions | `max_depth`, `max_children_per_node`, `max_descendants`, `budget_share`, `allowed_operations[]`, `justification_required` | Pinned by Revision; expansion is a proposal | authoring → policy | M1, M8 |
| **`SubagentProfile`** (new) | Admitted composition of a Subagent Invocation | per E.5.4 | Subset laws against the invoking binding; off by default | catalog → policy, harness | M1 (schema), M8 |
| **`DeliverableDeclaration`** (new) | Names a reviewable output of a Mission | `deliverable_key`, `deliverable_kind`, `produced_by_node_key`, `review_policy`, `promotion` | Compiler synthesizes the Review Gate and promotion node (F.2) | authoring → compiler | M1 (schema), M4 |
| **`ContextPackRequest`** (new) | What an agent wants in context | `members[]{kind, ref}`, `token_budget`, `render` | Deterministic assembly; digest-bound result | Skill, binding → `context_pack_assemble` | M3 |

### B.2 Revision and Run contracts (owner: `07`)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `ValidationReport` | Result of `validateMissionDefinition` | `ok`, `findings[]{code, path, severity, message}`, `definition_digest` | Pure; never throws for domain errors | compiler → everyone | M2 |
| `RevisionProposal` | Attributable proposed successor | `proposal_id`, `mission_id`, `base_revision_id`, `author{actor_ref, surface}`, `rationale`, `definition` (complete), `change_set` (semantic diff), `lifecycle`, `resolution?`, `transition_impacts[]` | Optimistic concurrency on the head; `stale` when overtaken | authoring → ledger, review | M2 |
| `MissionRevision` | Immutable committed version | `revision_id`, `revision_no`, `definition_digest`, `program_digest`, `head_status`, `committed_at`, `proposal_id` | Exactly one `head` per Mission | kernel → everyone | M2 |
| `CompiledProgram` | Deterministic executable | `program_digest`, `nodes[]` (flattened with structural path), `synthesized_nodes[]`, `fan_out_templates[]`, `node_definition_digests{}` | Compiles twice to identical digests | compiler → kernel, dashboard | M2 |
| `TransitionImpact` | Per-node classification of a proposal | `node_key`, `impact` (`unchanged … added`), `default_treatment` | Stored with the proposal | compiler → activation | M2, M8 |
| `CarryForwardEligibility` | Reuse decision by digests | `activation_id`, `eligibility`, `reason` | Never by inspection | compiler → kernel | M2, M8 |
| `MissionRun` | One execution of one Revision | `run_id`, `mission_id`, `revision_id`, `input_snapshot_digest`, `lifecycle`, `terminal_outcome?`, `started_at`, `completed_at?` | Pinned Revision; may outlive the head | kernel → everyone | M1, M3 |
| `InputSnapshot` | Immutable inputs of a Run | `input_snapshot_digest`, `inputs{name → artifact_ref}` | Content-addressed | authoring → run | M3 |

### B.3 Execution contracts (owner: `05`, `08`)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `Activation` | One instance of a node in one Run | `activation_id`, `run_id`, `node_key`, `behavior`, `parent_activation_id?` (**new**), `lifecycle`, `phase?`, `terminal_outcome?`, `reason?`, `governor_counters`, `budget_allocated`, `event_cursor` | Three fields never collapsed; only `accepted` projects outputs | kernel → everyone | M1, M4 |
| `Attempt` | One try at an activation | `activation_id`, `attempt_no`, `outcome?`, `failure_class?`, `started_at`, `ended_at?` | Outcome is execution only | kernel → harness, dashboard | M1, M4 |
| `HarnessExecution` | Adapter record binding an Attempt to native identity | `harness_execution_id`, `agent_runtime_kind`, `orchestrator_kind`, `model_access_kind`, `compute_environment_kind`, `credit_account_kind`, `native_session_ref`, `native_turn_refs[]`, `hook_grant_ref` (**new**, E) | Dimensions consistent; identity mismatch fails closed | harness → kernel, native store | M1, M5 |
| `AgentSession` / `SessionTurn` | Disposable context window and its native units | `agent_session_id`, `native_session_ref`, `session_policy`, `turn_no`, `native_turn_ref`, `usage` | A Session serves one activation lineage | harness → kernel | M1, M5 |
| `CompletionCandidate` | Agent's typed "I am done" | `activation_id`, `attempt_no`, `outputs[]{output_name, artifact_ref}`, `evidence_refs[]`, `criteria_mapping[]?`, `notes` | Never acceptance; evaluated by the Completion Contract | agent → kernel | M1, M5 |
| `OperationIntent` / `OperationReceipt` | Deterministic executor bookkeeping | `intent_id`, `executor_kind`, `input_digest`, `idempotency_key`; `outcome`, `changes_summary`, `affected_refs[]` | Exactly one receipt per intent | executors → ledger | M1, M4 |
| `ContinuationCheckpoint` | Typed handoff between Sessions | `checkpoint_id`, `contract_version`, `identities`, `acceptance_state`, `decisions[]`, `artifact_refs[]`, `workspace_snapshot_ref`, `work{completed, active, pending, blocked}`, `dispositions[]`, `human_tasks[]`, `queued_command_ids[]`, `event_cursor`, `governors`, `capability_versions`, `invariants[]`, `next_actions[]`, `digest`, `validator_ref` | Fail closed on invalid; no secrets or unbounded payloads | compaction → transfer | M1, M5 |
| `ContextHealthPolicyVersion` | Pinned thresholds | `policy_id`, `version`, key `(model, runtime, tool_profile, task_class)`, thresholds, `status` | Pinned by Revision | catalog → harness | M1, M6 |
| `JournalEntry` / `JournalSegment` | Goal Loop record | per `02 §4.1` | Append-only; segments chained by digest | loop → ledger, Artifacts | M1, M6 |
| `UsageRecord` (**new** name for budget consumption rows) | Tokens, cost, wall-clock per Turn or receipt | `usage_id`, `credit_account_kind`, `harness_execution_id?`, `receipt_id?`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, `cost_usd?`, `wall_clock_ms`, `native_ref` | Derived from native usage events (`step.completed.usage`, Cursor `usage`); feeds `budget.consumed` | harness → `budget_ledger` | M5 |

### B.4 Event, Command, and stream contracts (owner: `09`; Part D)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `MissionEvent` | Canonical sequenced record | `event_id` (UUIDv7), `mission_id`, `run_id?`, `revision_id?`, `seq`, `occurred_at`, `recorded_at`, `type`, `scope{}`, `source{kind, actor_ref?, native_event_ref?}`, `causation_id?`, `correlation_id?`, `payload` | References and digests only; at-least-once; gap-detectable on `seq` | kernel → everyone | M1, M3 |
| `MissionEventPayload.<type>` | Typed payload per event type | per family (`09 §3` + `subagent`, `deliverable`, `search_index`) | Literals from the vocabularies | kernel → consumers | M1 |
| `NativeEvent` | Raw provider event | `native_event_id`, `native_scope_ref` (harness execution or external operation), `provider`, `native_event_key` (dedupe), `native_type`, `occurred_at`, `recorded_at`, `payload_digest`, `payload_excerpt`, `payload_ref?`, `canonical: false` | Bounded retention; never a Mission Event | adapters → native store, dashboard tail | M3, M5 |
| `Command` | Durable write that changes execution | `command_id`, `mission_id`, `kind`, `target{run_id?, activation_id?, attempt_no?}`, `payload`, `actor_ref`, `grant_ref`, `lifecycle`, `outcome?`, `submitted_at` | Idempotent on `command_id`; never lost on disconnect | faces → kernel | M1, M3 |
| `DeliveryReport` | How a Command reached the agent | `command_id`, `agent_runtime_kind`, `delivery_semantics`, `delivered_at?`, `observed_at?`, `native_ref?`, `note` | Always one of the seven values; `emulated` names the emulation | harness → kernel, faces | M1, M4 |
| `PeekView` | Bounded live read | per E.3.1 | Pointers only; size-capped | kernel → faces | M1, M3 |
| **`StreamSubscription`** (new) | One channel on the Stream Gateway | `subscription_id`, `channel{kind, mission_id?, run_id?, harness_execution_id?, tenant scope}`, `after_seq?`, `types[]?`, `node_key?` | Replay-then-live; server assigns `subscription_id` echo | clients → gateway | M3 |
| **`StreamFrame`** (new) | Gateway → client message | `subscription_id`, `frame_kind` (`snapshot`, `event`, `native`, `gap`, `heartbeat`, `error`, `end`), body | Ordered per subscription; heartbeats every 15 s | gateway → clients | M3 |
| **`StreamToken`** (new) | Short-lived scoped credential for the gateway | JWT: `sub` (principal), `tenant_id`, `scopes[]`, `mission_ids[]?`, `native_tail`, `exp` (≤ 15 min) | Minted by `POST /v1/stream-tokens` from a real grant | api → browser/CLI | M3 |
| **`TraceDigest`** (new) | Bounded agent-facing trace | per E.4.4 | Deterministic; idempotent per window | `trace_digest` executor → agents | M7 |
| **`HarnessObservation`** (new) | Hook ingress envelope | `harness_execution_id`, `provider`, `native_event_key`, `native_type`, `occurred_at`, `payload_digest`, `payload_excerpt` | Observation only; Session-scoped token | hooks → native store | M5 |

### B.5 Human contracts (owner: `05 §6.3`; Part F)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `HumanTask` | Durable request for a human decision | `human_task_id`, `mission_id`, `activation_id?`, `kind` (lower_snake, D-A5), `prompt`, `context_refs[]`, `options[]?`, `assignee_policy`, `timeout?`, `on_timeout`, `lifecycle`, `resolution?`, `answer_artifact_ref?`, `claimed_by?`, `opened_at`, `resolved_at?` | Answer is an Artifact; approval ≠ input ≠ intervention | kernel → inbox, faces | M1, M3 |
| **`Deliverable`** (new) | Reviewable, promotable output bundle | `deliverable_id`, `mission_id`, `deliverable_key`, `deliverable_kind`, `review_state`, `review_packet_ref?`, `review_human_task_id?`, `promotion_state`, `promotion_receipt_ref?` | One per declaration per Run; state machine in F.4 | kernel → inbox, dashboard, faces | M4 |
| **`ReviewPacket`** (new) | Everything a reviewer needs, bounded | per F.3 | Assembled deterministically by `review_packet_assemble`; digest-bound | executor → Human Task `review` | M4 |
| **`ReviewResolution`** (new) | The human's decision on a Deliverable | `resolution` (`review_accept`, `review_reject`), `findings_ref?`, `promote: boolean`, `reviewer_ref`, `notes` | Human actor only; `promote` opens the promotion node | inbox → kernel | M4 |
| **`Notification`** (new) | Outbound notice of a Human Task or Deliverable | `notification_id`, `channel` (`dashboard_inbox`, `email`, `webhook`; later `slack`, `push`), `subject_ref`, `deep_link` (`mc://…`), `sent_at`, `delivery_state` | Best-effort; the inbox is the truth | kernel → notifier | M7 |

### B.6 Harness contracts (owner: architecture §9, `05`)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `AgentHarness` (port) | Per-runtime primitives | `prepare`, `start`, `sendTurn`, `cancelTurn`, `observe`, `snapshot`, `usage`, `endSession`, **`provisionSubagent`** (new, optional) | Pause/resume/continuation are kernel compositions | harness package → adapters | M5 |
| `HarnessCapabilityMatrix` | What each lane supports | `agent_runtime_kind`, `supports{queue, interrupt, pause, snapshot, observe_replay, subagents, hooks, webhook}`, `delivery_semantics_by_command` | Derived from `runtime-facts/`; served by `describe` | adapters → describe, dashboard | M5 |
| `NativeStatusMapping` | Provider status → Attempt outcome + Failure Class | table per `05 §3.5` | Pure function `toAttemptOutcome` | harness → kernel | M5 |
| `NativeIngestionCursor` | Where an adapter's observation stands | `harness_execution_id`, `provider_cursor` (Eve `startIndex`, Cursor `Last-Event-ID`, KS `after`), `last_native_event_key`, `updated_at` | Persisted; replay from it after crash | adapters → ledger | M5 |

### B.7 Proof-boundary contracts (owner: Knowledge Services; Mission Control links by id)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `VerificationDispatchRequest` / `Result` | The `verification_dispatch` Executor Kind's I/O | `operation` (12 KS use cases), `context`, `request_digest`, `request`; `disposition`, `operation_id`, `receipt_id` | camelCase inside the KS seam only; Mission Control stores the disposition as an Evidence Assessment | executor ↔ KS | M4 (exists) |
| `EvidenceAssessmentLink` | Immutable link to a KS result | `assessment_id`, `evidence_requirement_ref`, `artifact_ref`, `ks_operation_id`, `disposition`, `recorded_at` | Never re-derived here | kernel → traceability | M4 |
| `RubricRef` | Pinned Rubric by Artifact and version | `artifact_ref`, `rubric_version` | Schema owned by KS | authoring → EO | M6 |
| **`PromotionRequest`** (new) | Ask the owning domain to admit/publish | `deliverable_id`, `target_domain` (`knowledge_services`, `git_forge`), `artifact_refs[]`, `review_resolution_ref`, `side_effect_class: external_write_irreversible` | Only after `review_accept` with `promote: true`; executed by `report_promote` / `code_promote` | kernel → executor → KS / forge | M4+ |

### B.8 Discovery, search, and workspace contracts (Part G)

| Contract | Purpose | Key fields | Assumes / guarantees | Producer → Consumers | Milestone |
|---|---|---|---|---|---|
| `SystemDescription` (was `CompositionManifest`) | What can be composed from | `contract_versions{}`, `behaviors[]`, `commands[]{delivery_semantics_by_runtime}`, `runtimes[]`, `models[]`, `credit_accounts[]`, `executor_kinds[]`, `human_task_kinds[]`, `exemplars[]`, `schemas[]`, `policy_limits{}`, `knowledge_projection_routes[]` | Grants nothing | kernel → faces | M3 |
| `SchemaCatalogEntry` | One JSON Schema | `schema_name`, `contract_version`, `json_schema`, `digest` | Generated | contracts → faces | M1 |
| **`MissionFsNode`** (new) | One node of the navigable projection | `path`, `kind` (`directory`, `document`, `record`, `link`), `ref` (`mc://`), `size_bytes?`, `digest?`, `children[]?` | Read-only; paths follow D-A3 | missionfs → faces, workspace seed | M3 |
| **`MissionSearchQuery` / `MissionSearchResult`** (new) | Structured + full-text search | `q?`, `scope[]`, `filters{}`, `after?`, `max`; `hits[]{ref, kind, title, snippet, score, facts{}}`, `next` | Structured and full-text served here; semantic delegated to KS retrieval over published index documents | faces → missionfs | M3 |
| **`MissionIndexDocument`** (new Artifact kind) | Text rendering of a Mission for retrieval | `MISSION.md` content, `mission_id`, `revision_id`, `digest` | Published to KS as an Artifact on every head change | `search_index_publish` → KS | M7 |
| **`ContextPack`** (new) | Bounded bundle for an agent's context | `context_pack_id`, `digest`, `token_budget`, `members[]{kind, ref, rendered_bytes, truncated}`, `omitted[]` | Deterministic per request digest | `context_pack_assemble` → agents, bindings | M3 |

### B.9 Client contracts (Part E)

| Contract | Purpose | Assumes / guarantees | Milestone |
|---|---|---|---|
| HTTP API (`/v1`) | Canonical transport, D-A7 grammar | Problem+JSON errors; idempotency on mutations; SSE via `Accept` | M3 |
| `missionctl` | Operator and agent CLI, D-A8 grammar | JSON on stdout; exit codes `0` ok, `1` domain refusal (`rejected`, `not_accepted`, `denied`), `2` infrastructure | M2–M7 |
| MCP server | Tools per E.2.4 plus `mission_search`, `mission_fs_read`, `mission_context_select`, `mission_deliverable_review` (F); resources `mc://…`; `resources/subscribe` on `…/peek` and `…/deliverables` | Stateless Streamable HTTP; per-request grant; ≤ 28 tools | M7 |
| Agent Skill v1 | `skills/mission-control/SKILL.md` per E.6 | Prose never overrides policy; versioned against contracts | M7 |
| Plugin manifests | per E.7 | Hooks emit observations only | M7+ |
| Dashboard DTOs | `src/server/dto.ts` projections | Compact; never headers, tokens, bodies | M3+ |

### B.10 Storage contracts (schema `mission_control`, plus retained `orchestration.*`)

| Table | Holds | Immutable | Retention |
|---|---|---|---|
| `mission`, `goal`, `objective` | aggregates | no | forever |
| `mission_definition_snapshot`, `revision_proposal`, `mission_revision`, `compiled_program`, `program_node` | authoring lineage | snapshot/revision/program yes | forever |
| `mission_run`, `activation`, `attempt`, `harness_execution`, `agent_session`, `session_turn` | execution spine | no (state columns) | forever |
| `continuation_checkpoint`, `journal_segment`, `completion_candidate` | handoffs and candidates | yes | forever (bodies in object storage) |
| `command`, `delivery_report`, `human_task`, `event_receipt` | intervention and waits | report yes | forever |
| `mission_event` | canonical stream | yes | forever; archived to object storage 1 year after closure |
| `native_event` (partitioned) | provider events | yes | 30 days default (`MISSION_CONTROL_NATIVE_EVENT_RETENTION_DAYS`) |
| `native_ingestion_cursor` | adapter cursors | no | with the Harness Execution |
| `outbox` | fan-out queue with claims | no | published rows purged after 7 days |
| `usage_record`, `budget_ledger` | spend | yes | forever |
| `spawn_grant`, `mission_relationship`, `context_health_policy_version` | authority and catalog | no | forever |
| `deliverable`, `review_packet` (**new**) | review and promotion | packet yes | forever |
| `stream_token_revocation` (**new**) | revoked token ids | yes | until `exp` |
| `search_document` (**new**, `tsvector`) | full-text index | no | rebuilt from ledger |
| `orchestration.artifact*`, `capability*`, `operation_intent`, `operation_receipt` | retained until the second, KS-coordinated migration (ADR 0005) | — | — |

---
## Part C — Using the infrastructure we already have, optimally

The stack is fixed (`docs/architecture/0001-runtime-and-deployment.md`): Node 24, TypeScript 7, pnpm + Turborepo, Fastify 5.12, Zod 4.5, Temporal TypeScript SDK 1.23 on Temporal Cloud, MCP TypeScript SDK 1.30, Next.js 16.3 on Vercel with TanStack Query 5, Supabase Postgres 17 through `ai-engineer-db-contract`, Eve 0.52, `@cursor/sdk` 1.0.31, Vercel AI SDK 7 over AI Gateway, and Knowledge Services over its client. Part C says what each one is *for* in Mission Control, which advanced feature carries which responsibility, and what each is deliberately not used for.

### C.1 Temporal — the durable kernel, not the event store

| Feature (SDK 1.23, verified in `node_modules`) | Used for | Rule |
|---|---|---|
| **Updates** (`defineUpdate`, `setHandler` with validator) | Every Command (`command`) and `activate_revision`. The validator checks grant, scope, and state and rejects synchronously; the handler returns the `DeliveryReport`. `updateId = command_id` makes retries idempotent. | Commands never arrive as Signals; a Signal cannot reject or reply. |
| **Signals** (`defineSignal`) | `world_event` only: Event Wait firings, Human Task resolutions, child Mission notifications, harness observations that must wake a waiting activation. | `signalWithStart` only for `world_event` on dormant Runs (`09`, architecture §10). |
| **Queries** (`defineQuery`) | `peek` returns the bounded `PeekView` from workflow state without a ledger round trip. | Queries are consistent with history, so they are the fastest truthful read; the API uses the ledger projection by default and the Query when `consistency: strong` is asked. |
| **`condition` + `allHandlersFinished` + `continueAsNew`** | The `missionRunWorkflow` loop parks on `condition`, and Continue-As-New fires only after `allHandlersFinished()` so no Update is lost across the boundary. | History carries ids, digests, and compact outcomes only. |
| **Child workflows** | One `compositeActivationWorkflow` per workflow-system activation; one `atomicActivationWorkflow` per atomic node. `ParentClosePolicy.REQUEST_CANCEL` for `cancel_child`; `ABANDON` for `detach_child`. | Workflow id `activation:<activation_id>`; `REJECT_DUPLICATE`. |
| **Activities with heartbeat** | `observeNativeEvents` heartbeats with the native cursor as heartbeat detail, so a retried activity resumes from the last cursor instead of `startIndex=0`. | Heartbeat detail is the `NativeIngestionCursor`; today's `executeVerification` heartbeats a phase but does not read the detail back on retry — that is fixed. |
| **Non-cancellable scopes** | `CancellationScope.nonCancellable` around `sealContinuationCheckpoint`, `endSession`, `recordOperationReceipt`. | Cleanup always lands. |
| **Search attributes** (`upsertSearchAttributes`) | `TenantId`, `MissionId`, `MissionRunId`, `RunLifecycle`, `HeadRevisionId` on every Run workflow; the dashboard's Workers page and Temporal UI cross-link by these. | Visibility is for operators; it is never a read model for the product. |
| **Interceptors** (workflow inbound/outbound, activity inbound, client) | One `MissionControlInterceptor` set: stamps `correlation_id` / `causation_id` into headers, opens an OpenTelemetry span per activity, measures activity latency into `usage_record.wall_clock_ms`, and rejects any activity call whose input contains a forbidden key (`bytes`, `raw`, `secret`, `token`, `authorization` — the existing scan, generalized). | This is the "hook into the framework lifecycle" for Temporal. |
| **Sinks** (`proxySinks`) | Workflow-side structured logging routed to the worker logger with `mission_id` / `run_id`; never I/O. | — |
| **Worker Versioning** (`PINNED`) | A Run stays on the worker build that started it; a redeploy never replays history against changed code. | Configured per `apps/worker`; the deployment name is the git SHA. |
| **Schedules** | Mission Run recurrence (`mission-run-recurrence:<mission_id>`) and native-event retention jobs. | Recurrence is a Mission-level Run schedule, never a Timer node (`05 §6.2`). |
| **Nexus** (`@temporalio/nexus` 1.23, present) | *Candidate, later phase:* calling Knowledge Services' own durable workers across namespaces instead of HTTP polling, once KS confirms it runs Temporal in production. | Labelled candidate; nothing in M0–M8 depends on it. |

Not used for: storing Mission Events (the ledger is Postgres), serving reads to the dashboard (read models), or holding transcripts (object storage).

### C.2 Postgres via `ai-engineer-db-contract` — the Domain Ledger and the fan-out source

| Feature | Used for |
|---|---|
| Schema `mission_control` with `util.uuidv7()`, composite tenant keys, `util.reject_mutation()`, RLS on `app.tenant_id` | The whole ledger (A.6). |
| **Transactional outbox with claims** (copy the `knowledge_service.outbox` pattern: `claim_owner`, `claim_token`, `claimed_at`, `visibility_expires_at`, `max_delivery_attempts`) | `appendLedgerRecords` writes state rows, `mission_event` rows, and `outbox` rows in one transaction; the relay claims and publishes. Multiple relay instances are safe. |
| **`LISTEN/NOTIFY`** (not used anywhere in the workspace today) | A trigger on `outbox` insert runs `pg_notify('mission_control_outbox', mission_id::text)` so the relay wakes immediately instead of on its poll interval. The poll (1 s) remains the guarantee; the notification is the accelerator. |
| **Range partitioning** | `native_event` monthly by `recorded_at`; retention is `drop partition`. |
| **`tsvector` + GIN** | `search_document` for full-text search over Mission summaries, node instructions, Human Task prompts, and Artifact manifests (Part G). |
| **Advisory locks** | Per-Mission `seq` assignment: `pg_advisory_xact_lock(hashtext(mission_id))` inside `appendMissionEvents` guarantees a gap-free, monotonic `seq` under concurrent activities. |
| **Read models as views/tables** | `activation_state` (current three-field state per node per Run), `mission_summary`, `human_task_inbox`, `budget_by_account` — maintained in the same transaction as the events that change them, so the API never derives state from events at read time. |

Not used for: Supabase Realtime publications (they would bypass the grant model), the dashboard reading the database directly (the import boundary forbids it), or storing bodies (`payload_excerpt` ≤ 4 KiB; everything else is an Artifact in object storage).

**D-C1 — Fan-out is outbox + relay with `pg_notify` as a wake signal; Supabase Realtime is not used for Mission Events.** Reversibility: cheap.

### C.3 Fastify 5 — the Control API and the Stream Gateway host

- **Hooks:** `onRequest` resolves the principal and grant; `preHandler` requires scope per route (`requireGrant`); `onSend` strips upstream headers and stamps `x-mission-control-seq` (the latest `seq` the API has observed for the Mission) so clients can detect staleness; `onError` renders Problem+JSON.
- **Plugins:** `@fastify/websocket` for `/v1/stream`; SSE via a small `reply.raw` writer with `retry:` and `id:` lines (no extra dependency); `@fastify/rate-limit` on `POST /v1/internal/harness-events` and on long-poll; `@fastify/under-pressure` to shed stream connections before the event loop stalls.
- **Schema:** every route declares its Zod schema through `fastify-type-provider-zod`, and the same schemas produce the OpenAPI document served at `/v1/openapi.json` — the API's self-description for agents and for the dashboard proxy allowlist (which is generated from it rather than hand-maintained, H.3).
- **Process shape:** the API process hosts the Stream Gateway (long-lived connections need a container, not a serverless function). The worker process hosts the outbox relay. Both are the AWS containers the runtime ADR already names.

### C.4 MCP SDK 1.30 — tools, resources, subscriptions, elicitation

Verified present in the installed SDK: `SubscribeRequestSchema`, `ResourceUpdatedNotificationSchema`, `ResourceListChangedNotificationSchema`, `ProgressNotificationSchema`, `LoggingMessageNotificationSchema`, `ElicitRequestSchema`, `GetTaskRequestSchema` (experimental tasks).

| Feature | Used for |
|---|---|
| Tools | The `mission_*` surface (Part E). Every tool returns `structuredContent` validated by the same Zod schema as the HTTP route, plus plain `content` for non-structured hosts. |
| Resources with templates | `mc://missions/{mission_id}`, `…/runs/{run_id}/peek`, `…/program`, `mc://schemas/{schema_name}`, `mc://exemplars/{exemplar_name}`, `mc://fs/{path}`; `resources/list` enumerates the MissionFS root (Part G). |
| **`resources/subscribe` → `notifications/resources/updated`** | Hosts that keep a session open (Claude Code, Cursor, Eve connections) subscribe to `…/peek` and `…/deliverables`; the server pushes `updated` when the outbox relay publishes a change. This is MCP-native real-time and costs the agent nothing until it reads. Stateless hosts fall back to long-poll `mission_events`. | 
| **Elicitation** (`elicitation/create`) | When a `question` or `selection` Human Task is assigned to the principal behind the MCP session and the host advertises elicitation, the server raises it in-host; the answer is recorded through the same `resolveHumanTask` path with the human's `actor_ref`. Never for `approval`, `review`, `policy_override` (D-E14). |
| Progress notifications | `mission_start`, `mission_subagent_invoke {await}`, and `mission_context_select` report progress against a `progressToken` so hosts do not time out. |
| Logging notifications | Delivery Reports and denials are mirrored as `notifications/message` at `info`/`warning` so an agent's host log shows them even if the agent ignores the tool result. |
| Experimental tasks | *Candidate:* long `await` calls as MCP tasks when hosts support them; not relied on. |
| Auth | Bearer token per request (stateless) resolving to a grant, identical to HTTP. The server keeps a per-session subscription table keyed by the token's principal. |

**D-C2 — The MCP server supports `resources/subscribe` on peek and deliverable resources as the push channel for connected hosts; long-poll remains the universal path.** Reversibility: cheap.

### C.5 Next.js 16.3 + TanStack Query 5 — the dashboard

- Server Components render read models (mission list, program, inbox) from the API through the server-only proxy; client components own the live surfaces.
- **Live data path:** the browser opens `wss://<api>/v1/stream` *directly* with a `StreamToken` minted by the dashboard server route `POST /api/dashboard/stream-token` from the operator session cookie. Next route handlers on Vercel do not upgrade WebSockets and hold streaming responses only for the function's lifetime, so the gateway must be the API container (C.3). REST keeps going through the proxy.
- **Cache patching:** one `StreamClient` (H.2) dispatches every `StreamFrame` to TanStack Query: `queryClient.setQueryData(["activation_state", run_id], patch)` for state deltas and `queryClient.invalidateQueries` for coarse changes; polling (`refetchInterval`) is removed except as a 30 s safety net when the socket is down.
- **Static pages:** the specification page (`/specification`) and schema pages are prerendered from generated content modules (no runtime `fs`, no tracing surprises on Vercel; the app does not enable `cacheComponents`, so `force-static` per page is the tool).
- **Playwright** stays the E2E harness; the `FixtureStreamAdapter` (H.4) lets Live Mission be tested without Temporal.

### C.6 Agent runtimes and the AI SDK — where the lifecycle hooks are

| Runtime | Lifecycle hook Mission Control attaches to | What flows |
|---|---|---|
| **Eve** | `defineHook({ events: { "*": handler } })` in the Mission-Control-owned `mission-eve-agent` (`agent/hooks/mission-control.ts`), filtered to `session.*`, `turn.*`, `step.completed` (usage), `actions.requested`, `action.result`, `subagent.*`, `compaction.*`, `input.requested`, `authorization.required`; plus the NDJSON stream with `startIndex` replay as the source of truth. `defineDynamic` on `session.started` / `turn.started` injects the Operating Contract, the Context Pack, and the `.mission/` seed (Part G). `limits` carry the budget slice. | Native events → Native Event Store (hook = accelerator; stream = truth). Usage → `usage_record`. HITL `input.requested` → a `question` Human Task (E.3, F.7). |
| **Cursor cloud** | SDK `run.stream()` with `Last-Event-ID` resume and `GET run` fallback; `.cursor/hooks.json` command hooks (`stop`, `afterFileEdit`, `subagentStart`, `subagentStop`, `preCompact`, `afterShellExecution`) calling `missionctl trace emit`; v0 `statusChange` webhook as a completion accelerator. `usage` stream events → `usage_record`. | Same as Eve; `preCompact` gives `context_usage_percent`, the only context-health signal Cursor exposes, recorded as `session.compaction_observed`. |
| **Claude Code / Codex** (later lanes) | `.claude/settings.json` hooks (`Stop`, `PostToolUse`, `SubagentStart`, `SubagentStop`, `PreCompact`) → `missionctl trace emit`; Codex via launcher wrapper. | Observations only. |
| **`direct_model` (AI SDK 7 over AI Gateway)** | `generateText` / `streamText` callbacks `onStepFinish` (usage, tool calls) and `onFinish`; `experimental_telemetry` to OpenTelemetry with `mission_id` attributes. Every call is one `SessionTurn` with `native_turn_ref = response.id`. | Usage → `usage_record`; tool calls → `tool_call.completed` digests. |

### C.7 Knowledge Services — the proof seam

- The only camelCase seam (A.11): `@aiengineer/mission-knowledge` wraps `KnowledgeClient` and exposes `KnowledgePort { dispatch, observeOperationEvents, cancel, retry, publishIndexDocument, requestPromotion, search }`.
- KS operation events are polled (`GET /v1/operations/{id}/events?after=`); the executor's `observe` activity polls with the cursor in heartbeat detail and writes each KS event into the **Native Event Store** with `provider: knowledge_services`, `native_scope_ref: ks:operation/<operation_id>`, so the dashboard's agent tail shows verification progress the same way it shows an Eve turn. Mission Control never re-emits KS events as Mission Events; it records `disposition.recorded` when the terminal resource is read.
- KS `A2A` signed callbacks (`/v1/a2a/callbacks`, `hmac-sha256-v1`) are accepted as `world_event` accelerators in M8+; polling stays the guarantee.
- Retrieval, embeddings, and semantic search stay in KS; Mission Control publishes `MissionIndexDocument` Artifacts and calls `retrieval.search` (Part G). Promotion of reports goes through `promotion.submit`; Mission Control records the decision, never makes it.

### C.8 Object storage and Git

- Transcripts (sealed per Turn), workspace snapshots, Continuation Checkpoint bodies, Journal Segments, Review Packets, Trace Digests, and raw native payloads above the excerpt cap are Artifacts in object storage, addressed by `artifact_ref` and digest. The ledger holds manifests only.
- Git is the code deliverable's store: `git_snapshot` records refs; `code_promote` merges. Mission Control never holds a working tree.

### C.9 Deliberately not added

Redis / Kafka (the outbox and `pg_notify` cover M0–M8 volumes; revisit when a single Mission exceeds ~50 events/s sustained), Neo4j (Mission Graph is a table plus a recursive CTE), Supabase Realtime (grant model), a second message bus of any kind (architecture §17).

---

## Part D — The event plane: storing workflow and agent activity, streaming it, handling it

### D.1 Three stores and one set of read models

| Store | Holds | Written by | Read by | Canonical |
|---|---|---|---|---|
| **Domain Ledger** (`mission_control.*`) | state rows, `mission_event` (references and digests), `command`, `human_task`, `deliverable`, `usage_record`, `outbox` | `appendLedgerRecords` activity, API writes (drafts, proposals, Human Task claims) | everything | yes |
| **Native Event Store** (`native_event`, partitioned) | provider events from Eve, Cursor, AI SDK, Knowledge Services, and hook ingress; digests + excerpt; pointer to a body Artifact when retained | adapters' `observeNativeEvents`, `POST /v1/internal/harness-events` | dashboard agent tail, `mission_events {source: native}`, adapter audits | no (`canonical: false`) |
| **Artifact store** (object storage, manifests in `orchestration.artifact*` until re-specified) | transcript segments, checkpoints, journal segments, review packets, trace digests, workspace snapshots, native bodies | executors, harness at Turn end | via `artifact_ref` under grant | yes for content, by digest |
| **Read models** (`activation_state`, `mission_summary`, `human_task_inbox`, `budget_by_account`, `search_document`) | current state for lists and pages | same transaction as the events | API, dashboard, MCP resources | derived |

Temporal history is a recovery log and is not read by any product surface.

### D.2 Write path for workflow activity

1. A workflow computes the state transition deterministically and builds the list of `MissionEvent` envelopes with deterministic `event_id`s (UUIDv5 over `activation_id + logical_event_key`, so a replayed activity produces the same ids).
2. It calls one activity, `appendLedgerRecords({ state_patches[], events[], outbox_topics[] })`, on the `mission-kernel` queue.
3. The activity, in one transaction: takes the per-Mission advisory lock, assigns `seq` (max + 1 … n), inserts `mission_event` rows (`on conflict (event_id) do nothing` — retries are silent), applies the state patches to the aggregate tables and read models, inserts `outbox` rows, and commits. The trigger fires `pg_notify`.
4. It returns the assigned `seq` range; the workflow stores only that in history.

Guarantees: at-most-once state mutation per logical event (idempotent ids), gap-free `seq`, events visible to readers only after state is consistent, fan-out only after commit (outbox).

### D.3 Ingestion path for agent activity, per lane

| Lane | Truth | Accelerator | Dedupe key | Cursor persisted in |
|---|---|---|---|---|
| Eve | NDJSON stream `GET …/stream?startIndex=` (replay + live) | `defineHook` → `/v1/internal/harness-events` | `meta.id` **and** `(turnId, stepIndex, sequence)` (retried steps re-emit under new ids) | heartbeat detail + `native_ingestion_cursor` |
| Cursor cloud | SSE `…/runs/{runId}/stream` with `Last-Event-ID`; `GET run` after `410 stream_expired` | v0 `statusChange` webhook; `hooks.json` → `missionctl trace emit` | SSE `id:`; hook `(conversation_id, generation_id, hook_event_name, occurred_at)` | same |
| `direct_model` | AI SDK callbacks in-process | — | `response.id + step index` | in-activity |
| Knowledge Services | `GET /v1/operations/{id}/events?after=` | A2A callback (M8+) | `operation_id + sequence` | same |

Every native row is written to the Native Event Store first (`recordNativeEvents`, idempotent on `(native_scope_ref, native_event_key)`), then `toMissionEvents(nativeBatch, mapping)` derives the canonical events: `session.started`, `session.turn_started`, `session.turn_completed` (with a `transcript_segment` Artifact ref and usage summary), `tool_call.completed` (digests only), `session.compaction_observed`, `subagent.*`, `attempt.completed` (through `toAttemptOutcome`), `budget.consumed` (from `usage_record`). Text, thinking, and tool bodies never cross; the transcript Artifact holds them.

**D-D1 — Transcript segments are sealed as Artifacts at every Turn end; only digests and the segment reference enter the canonical stream.** At every `turn.completed` / `FINISHED` / `onFinish`, the adapter seals a `transcript_segment` Artifact (the native events of that Turn, verbatim, compressed) and references it from `session.turn_completed.transcript_ref`. This is how full agent activity is retained *optimally*: hot and bounded in the Native Event Store for 30 days, cold and complete in object storage for as long as the Mission exists, and only digests in the canonical stream. A reviewer, the dashboard, or a later audit resolves the Artifact under grant.

### D.4 Storage design details

- `mission_event`: `(tenant_id, mission_id, seq)` unique; B-tree on `(mission_id, seq)`; partial index on `(mission_id, type)`; `payload jsonb` capped at 8 KiB by check constraint; no partitioning through M8 (references only; volume is bounded by activations, not tokens).
- `native_event`: monthly partitions; columns `native_event_id uuid`, `tenant_id`, `native_scope_ref text`, `provider text`, `native_event_key text`, `native_type text`, `native_seq bigint?`, `occurred_at`, `recorded_at`, `payload_digest`, `payload_excerpt text` (≤ 4 KiB), `payload_ref` (Artifact, nullable), `ingested_via` (`stream`, `hook`, `webhook`, `poll`); unique `(native_scope_ref, native_event_key)`; index `(native_scope_ref, native_seq)`.
- `usage_record`: one row per Turn or receipt; `budget_ledger` aggregates per credit account and scope; `budget.threshold_crossed` is emitted by the same transaction.
- `outbox`: `topic` values `mission_event`, `native_event`, `human_task`, `deliverable`, `notification`; claims as in KS; `published_at`; purge after 7 days.
- Retention job: a Temporal Schedule per tenant drops `native_event` partitions older than the configured window and writes `search_index` refresh work.

**D-D2 — The Native Event Store is generalized to every external provider (agent harnesses and Knowledge Services operations), keyed by `native_scope_ref`, not only by `harness_execution_id`.** Amends `09 §4`. Reversibility: cheap.

**D-D3 — Mission Events never exceed 8 KiB and never carry bodies; bodies live in Artifacts referenced from the event.** Codifies `09 §5`.

**D-D4 — `native_event` is partitioned monthly with 30-day default retention; `mission_event` is unpartitioned and archived, never deleted.**

### D.5 The Stream Gateway

One module (`@aiengineer/mission-stream`) hosted in the API process serves three transports over one subscription contract.

```text
StreamSubscription {
  subscription_id                    // client-chosen, echoed
  channel:
      { kind: mission_events, mission_id, run_id?, after_seq?, types[]?, node_key? }
    | { kind: native_tail, harness_execution_id, after_native_seq? }          // requires native_tail
    | { kind: human_tasks, assignee_ref | tenant }                            // inbox
    | { kind: deliverables, mission_id? }
    | { kind: missions, tenant, filters? }                                    // list changes
    | { kind: workers }                                                       // Temporal queue health
    | { kind: budgets, credit_account_kind? }
}
StreamFrame { subscription_id, frame_kind: snapshot | event | native | gap | heartbeat | error | end, seq?, body }
```

| Transport | Who | How |
|---|---|---|
| **WebSocket** `WS /v1/stream?token=<StreamToken>` | Dashboard | One socket, many subscriptions (`{op: subscribe|unsubscribe, subscription}` frames). Replay from `after_seq`, then live. Heartbeat every 15 s; the client reconnects with the last `seq` per subscription. |
| **SSE** `GET /v1/missions/{mission_id}/events` with `Accept: text/event-stream` | CLI (`events tail --follow` may use it), scripts, browsers without WS | `id:` = `seq`; `Last-Event-ID` resumes; `retry: 3000`; heartbeat comment every 15 s. Single channel. |
| **Long-poll** `GET …/events?after_seq=&wait_ms=` and `mission_events` | Agents (Part E) | Server holds ≤ 25 s; returns `next_seq` always. |
| **MCP `resources/subscribe`** | Connected MCP hosts | `notifications/resources/updated` for `…/peek`, `…/deliverables`; the host then reads. |

Fan-out: the outbox relay (worker process) claims rows, publishes each to an in-process broadcaster keyed by `mission_id` and by channel, and — for multi-instance API — to a `pg_notify` channel carrying `(mission_id, seq)`; each API instance's gateway serves its subscribers from the ledger (`readEvents(after_seq)`), never from the notification payload. So a notification lost between processes costs latency, not correctness, and the WebSocket gateway is stateless beyond its subscription table.

Backpressure: per-connection send buffer cap (1 MiB); on overflow the gateway sends `gap` with the last delivered `seq` and drops to snapshot mode until the client re-subscribes with `after_seq`. `@fastify/under-pressure` refuses new subscriptions under event-loop delay. Limits: 32 subscriptions per socket, 8 sockets per principal.

Authorization: every subscription is checked against the token's scopes and Mission ids at subscribe time and again on every `revision`/`mission.closed` frame boundary (cheap re-check); `native_tail` requires the flag; `human_tasks` for another assignee requires `mission.admin`.

**D-D5 — One Stream Gateway, three transports (WebSocket multiplex for the dashboard, SSE and long-poll for everything else), one subscription contract, ledger-served replay.** The dashboard's WebSocket is a convenience over the same `seq` sequence; nothing exists only on the socket (Commands go over HTTP). Reversibility: moderate.

**D-D6 — Browsers connect to the gateway directly with a 15-minute `StreamToken`; the Next proxy never carries streams.**

### D.6 Event handler catalog

Handlers are named `on<Aggregate><PastTenseVerb>` and live next to the aggregate they mutate or render. A handler is registered once, in a table, never ad hoc.

**Kernel handlers (worker process; the reactions the ledger takes on its own events)**

| Event | Handler | Effect |
|---|---|---|
| `attempt.completed{outcome: succeeded}` | `onAttemptCompleted` | `evaluateCompletionContract` → `disposition.recorded` → `activation.completed{terminal_outcome}` |
| `attempt.completed{outcome: failed}` | `onAttemptFailed` | `decideRetry(failure_class, retry_policy)` → new Attempt or `activation.completed{execution_failed}` |
| `activation.completed` | `onActivationCompleted` | release evaluation in the enclosing system; Output Projection when `accepted`; Deliverable state update (F) |
| `human_task.resolved` | `onHumanTaskResolved` | Human Gate outcome; Goal Loop same-Iteration resume; Deliverable review transition |
| `disposition.recorded` | `onDispositionRecorded` | Proof Gate re-evaluation; traceability index update |
| `budget.threshold_crossed` / `budget.exhausted` | `onBudgetThresholdCrossed` | governor checks; `governor.exhausted`; Trace Digest trigger for bindings that declared it |
| `session.compaction_observed` | `onCompactionObserved` | Context Health assessment; schedule Compaction when soft threshold crossed |
| `command.accepted` | `onCommandAccepted` | `deliverCommand` through the harness → `command.delivered` / `command.observed` / `command.completed` |
| `subagent.completed` | `onSubagentCompleted` | budget slice release; `SubagentReport` Artifact; parent Trace Digest trigger |
| `child_mission.*` (from a child's ledger) | `onChildMissionChanged` | Portal recomputation → `child_mission.portal_updated` on the parent |
| `event_wait.fired` | `onEventWaitFired` | Event Receipt; dependent release; `rearm` new activation |
| `deliverable.reviewed{promote: true}` | `onDeliverableReviewed` | release the promotion node |
| any event with `trace_push_policy` match | `onTracePushTriggered` | coalesce, `trace_digest`, `queue_instruction` |
| `revision.head_activated` | `onRevisionHeadActivated` | `search_index_publish`; MissionFS invalidation; Transition Impact application (M8) |

**Stream Gateway handlers (API process)**

| Outbox topic | Handler | Channels fed |
|---|---|---|
| `mission_event` | `onOutboxMissionEvent` | `mission_events`, `missions` (list deltas on `mission.*`, `run.*`), `budgets` (on `budget.*`), `deliverables` (on `deliverable.*`), MCP `…/peek` resource update |
| `native_event` | `onOutboxNativeEvent` | `native_tail` |
| `human_task` | `onOutboxHumanTask` | `human_tasks`; MCP elicitation when eligible |
| `notification` | `onOutboxNotification` | notifier adapters (email/webhook) |
| worker heartbeat (Temporal `describeTaskQueue` poll, 30 s) | `onWorkerHealthSampled` | `workers` |

**Dashboard handlers (browser; `src/features/stream/handlers.ts`)**

| Frame | Handler | Cache effect |
|---|---|---|
| `snapshot` | `onSnapshotFrame` | `setQueryData` for the subscription's query key |
| `event` `activation.*` | `onActivationEvent` | patch `["activation_state", run_id]`; topology re-render |
| `event` `command.*` | `onCommandEvent` | update Command composer's Delivery Report panel |
| `event` `human_task.*` / `deliverable.*` | `onInboxEvent` | inbox counts and cards |
| `event` `artifact.*` | `onArtifactEvent` | output rail |
| `event` `journal.*` | `onJournalEvent` | Journal viewer append |
| `event` `budget.*` | `onBudgetEvent` | budget meters |
| `native` | `onNativeFrame` | agent tail (non-canonical badge) |
| `gap` | `onGapFrame` | invalidate the query, re-subscribe from last `seq` |
| `heartbeat` | `onHeartbeatFrame` | connection health indicator |
| `error` / `end` | `onStreamError` | fall back to 30 s polling; surface a banner |

**Agent-side handlers (Skill; what an agent does with each family)**

| Family | Agent reaction taught by the Skill |
|---|---|
| `activation` | update its plan; `not_accepted` → read the disposition, remediate within Action Space |
| `human_task` | if it may answer (`question`, `selection`) answer; else wait with long-poll |
| `command` | read `delivery_semantics`; never retry `rejected` |
| `budget` / `governor` | stop proposing spend; propose `request_revision` if blocked |
| `subagent` | read the report; never assume a sibling's internals |
| `deliverable` | `reviewed{review_reject}` → findings Artifact is the next input |

### D.7 Library lifecycle hooks, catalogued

| Library | Hook point | Mission Control attaches | Produces |
|---|---|---|---|
| Temporal SDK | `WorkflowInboundCallsInterceptor.execute/handleSignal/handleUpdate/handleQuery`, `WorkflowOutboundCallsInterceptor.scheduleActivity/startChildWorkflowExecution`, `ActivityInboundCallsInterceptor.execute`, client `WorkflowClientInterceptor.start/signal/update` | `MissionControlInterceptor` (C.1) | correlation headers, OTel spans, forbidden-key guard, activity latency |
| Temporal SDK | `Runtime.install({ telemetryOptions })`, `proxySinks` | metrics + logs export | worker health samples |
| Temporal Cloud | Schedules, search attributes | recurrence, retention, visibility | — |
| Fastify | `onRequest`, `preHandler`, `onSend`, `onError`, `onClose` | auth, scope, seq header, Problem+JSON, gateway drain | — |
| MCP SDK | `server.registerTool`, `registerResource` with `subscribe`, `server.notification`, `elicitInput`, `sendLoggingMessage` | tool bindings, resource subscriptions, elicitation for Human Tasks | `notifications/resources/updated`, `notifications/message` |
| Eve | `defineHook` (observe-only, at-least-once, same `meta.id`), `defineDynamic` on `session.started`/`turn.started`, `limits`, `onSession`, channel `auth` | observation hook, contract/context injection, budget slice, sandbox policy, Session-scoped auth | native events, usage, HITL requests |
| Cursor SDK | `run.stream()`, `send({ onDelta, onStep })`, `run.onDidChangeStatus`, `.cursor/hooks.json`, v0 webhook | observation, hooks → `trace emit`, completion accelerator | native events, usage, `preCompact` context percent |
| AI SDK | `onStepFinish`, `onFinish`, `experimental_telemetry`, tool `execute` wrappers | `direct_model` observation, usage, tool-call digests | native events, usage |
| Postgres | `after insert` trigger on `outbox` → `pg_notify`; `util.reject_mutation`; RLS | wake signal, immutability, tenant lock | — |
| Next.js | route handlers (`force-dynamic` proxy), server components, `generateStaticParams` for schema pages | proxy, prerender | — |
| TanStack Query | `QueryClient` cache API, `onlineManager`, `focusManager` | stream-driven cache patching, reconnect behaviour | — |
| Knowledge Services | operation events endpoint, A2A callbacks, `promotion.*`, `retrieval.search` | executor observation, promotion, semantic search | native events (`provider: knowledge_services`), dispositions |

### D.8 Observability across all of it

`correlation_id` is the OpenTelemetry trace id of the Run's root span; every Mission Event, native row, Command, Temporal header, KS request (`x-correlation-id`), and dashboard request carries it. `causation_id` is the `event_id` or `command_id` that caused a write. `eve traces` and the Temporal UI are reachable from the dashboard's inspector by these ids. Metrics: events appended/s, outbox lag, gateway subscriptions, long-poll wait time, native ingestion lag per lane, Delivery Semantics histogram, Human Task age.

---
## Part E — Mission Control as an Agent Skill, CLI, and MCP server: coordinator inspection, intervention, live tracing, and the Subagent tool

_This part was produced by the dedicated subagent the Coordinator asked for, then normalized to the naming law of Part A (noun-verb CLI, `mission_human_task_resolve`, lower-case Human Task kinds, UUIDv7 identifiers)._

### E.1 Purpose and position

Mission Control is consumed three ways by agents: as a **CLI** (`missionctl`), as an **MCP server**, and as an **Agent Skill** that teaches an agent when and how to use the first two. This part fixes how those three faces share one kernel and one authority model, how an agent holding the tool inspects a running Mission and changes its trajectory, how a turn-based agent receives live tracing without holding a socket, and how an agent fans out bounded work through a **Subagent Invocation** that is deliberately *not* a Mission.

Everything here derives from the accepted laws: Commands are the only writes; peek returns pointers, never bodies; no child-to-parent token stream; Delivery Semantics are reported, never assumed; an instruction can never widen authority, capability, or budget. Where this part introduces a new behavior (Subagent Invocation), a new event family (`subagent`), a new Executor Kind (`trace_digest`), and a new Artifact contract (`TraceDigest@1`), each is marked as a decision for sign-off.

Current implementation state, for honesty: the MCP server exposes exactly one tool (`mission_service_status`) and `missionctl` exactly one verb (`status`). `skills/` holds six Knowledge Services verification skills, not a Mission Control skill. Everything below is target contract for M7 (`F7.4`, `F7.5`) with additions for M8.

### E.2 Three faces, one kernel

### E.2.1 The rule

`missionctl`, the MCP server, and the Agent Skill are **faces**, not services. Each face maps one-to-one onto an application service exported by `packages/mission-kernel`; a face never contains domain logic, never reaches the Domain Ledger directly, and never talks to Temporal except through the kernel. The HTTP Control API is the fourth face and the transport the other three use when they run out of process.

```text
Agent Skill (SKILL.md)  ──teaches──▶  missionctl  ─┐
                                      MCP server  ─┼──▶  mission-kernel application services  ──▶  Temporal / Domain Ledger
Dashboard / HTTP clients ──────────▶  Control API ─┘
```

**D-E1.** One application-service catalogue in `mission-kernel`, named `MissionApplication`, with one method per public operation; every face is a generated or hand-written thin binding over it. *Recommendation: accept; any logic that appears twice across faces is a defect.*

### E.2.2 One authority model

Every face carries a **grant** with one or more authority scopes from `09 §9`: `mission.read | mission.author | mission.command | mission.invoke | mission.admin`. The grant is resolved once at the kernel seam and stamped on every Command (`actor_ref`, `grant_ref`) and every read. A face never enforces scope itself; it forwards the token and renders the kernel's refusal.

| Face | Where the grant lives | Typical scopes |
|---|---|---|
| `missionctl` | `MISSION_CONTROL_TOKEN` env, or `--token` | operator: all five; agent: `read, author, command` |
| MCP server | bearer token on Streamable HTTP; per-session grant cached | agent: `read, author, command`, optionally `invoke` |
| Agent Skill | none of its own; uses the host's `missionctl` / MCP configuration | inherits |
| Control API | bearer token | any |

**D-E2.** Scopes are the *only* authorization vocabulary across faces; roles (`Viewer < Operator < Mission Admin < Platform Admin` from the candidate §18.5) are dashboard presentation groupings that expand into scope sets and never appear in a tool contract. *Recommendation: accept.*

### E.2.3 Naming rule for tools and verbs

Every MCP tool is named `mission_<verb>` or `mission_<aggregate>_<verb>`; every `missionctl` command is the noun-verb form of the same name (D-A8) (`mission_human_task_resolve` ⇔ `missionctl human-task resolve`). The rule exists for three reasons: an agent's tool list is flat and multi-server, so the `mission_` prefix is the namespace; a verb-final name reads as an action in a tool call (`mission_peek`, not `peek_mission`); and the aggregate-in-the-middle form makes the eleven Commands, five Human Task kinds, and Subagent operations grep-able as families. Verbs are imperative present tense (`invoke`, `resolve`), never nouns (`mission_events` is the one accepted exception because it names the stream read, and it is kept for continuity with architecture §13).

**D-E3.** Adopt the naming rule above and rename the existing `mission_service_status` to `mission_describe_system` (it is a system read, not a Mission read). *Recommendation: accept; keep `mission_service_status` as a deprecated alias for one release.*

### E.2.4 The tool surface (M7 + M8)

Architecture §13 lists ten MCP tools. This part keeps them and adds the inspection, tracing, and Subagent tools that an intervening agent needs. Scope column is the minimum scope.

| MCP tool | `missionctl` | Kernel service | Scope | Purpose |
|---|---|---|---|---|
| `mission_describe_system` | `system describe` | `describeSystem` | `read` | Composition Manifest: runtimes, availability, commands with per-runtime Delivery Semantics, schemas, exemplars |
| `mission_schema_get` | `schema get <name>` | `getSchema` | `read` | JSON Schema by name |
| `mission_exemplar_get` | `exemplar get <name>` | `getExemplar` | `read` | Immutable exemplar definition |
| `mission_draft` | `draft create` | `createDraft` | `author` | Create a Mission in `drafting` from a Mission Definition |
| `mission_validate` | `draft validate` | `validateDefinition` | `author` | `ValidationReport`; no side effects |
| `mission_propose` | `proposal submit` | `proposeRevision` | `author` | Revision Proposal against `base_revision_id` |
| `mission_activate` | `revision activate` | `activateRevision` | `author` | Commit + Scheduling Head transition |
| `mission_start` | `run start` | `startRun` | `author` (+ `invoke` when called as Spawn from another Mission) | Start a Mission Run; **is** Spawn when it creates identity for a child |
| `mission_peek` | `run peek` | `peek` | `read` | Bounded `PeekView` |
| `mission_program` | `program get` | `getProgram` | `read` | Compiled Program with three-field state per activation |
| `mission_trace` | `trace get` | `getTraceability` | `read` | Goal → Objective → Activation → Artifact → Disposition walk |
| `mission_events` | `events tail` | `readEvents` | `read` | Cursor read with long-poll (E.4) |
| `mission_artifacts` | `artifact list` | `listArtifacts` | `read` | Artifact inventory: ids, kinds, digests, sizes |
| `mission_artifact_get` | `artifact get` | `getArtifactPointer` | `read` | Pointer + bounded excerpt; never full bodies through MCP |
| `mission_command` | `command <kind>` | `submitCommand` | `command` (`admin` for `hard_pause` on side-effecting work, budget/grant changes) | The eleven Commands; returns `DeliveryReport` |
| `mission_command_get` | `command get` | `getCommand` | `read` | Command lifecycle and outcome by `command_id` |
| `mission_human_tasks` | `human-task list` | `listHumanTasks` | `read` | Open/claimed tasks within grant |
| `mission_human_task_resolve` | `human-task answer` | `resolveHumanTask` | `command` | Resolution + answer Artifact |
| `mission_subagent_invoke` | `subagent invoke` | `invokeSubagent` | `command` | E.5 |
| `mission_subagent_report` | `subagent report` | `getSubagentReport` | `read` | E.5 |
| `mission_spawn` (M8) | `mission spawn` | `spawnMission` | `invoke` | Child Mission Invocation from a caller |
| `mission_attach` (M8) | `mission attach` | `attachMission` | `invoke` | Attachment sub-modes |
| `mission_close` | `mission close` | `closeMission` | `admin` | Closure Outcome |

`missionctl` additionally has `events tail --follow` (an in-process loop over `readEvents` long-poll printing NDJSON to stdout), `trace emit` (E.4.3, hook side), and `status` (service health, retained). Output is JSON on stdout, human text on stderr, `--json` default when non-TTY, idempotency keys required on every mutation (`--command-id`, `--proposal-id`), exactly as the candidate §17.2 laid down.

**D-E4.** Adopt the table as the M7/M8 tool surface; `mission_trace`, `mission_command_get`, `mission_artifact_get`, `mission_subagent_*`, and `mission_close` are additions to architecture §13; Parts F and G add `mission_search`, `mission_fs_read`, `mission_context_select`, and `mission_deliverable_review`, for 27 tools under the cap of 28 (D-A9). `request_revision` is issued through `mission_command`, not a separate tool. *Recommendation: accept; the additions are reads plus one bounded write family, none widens authority.*

**D-E5.** No `mission_subscribe` tool. MCP tools are request/response; a persistent subscription would tie correctness to the host's connection lifetime, which is exactly what `09 §7` forbids for Commands and what turn-based agents cannot honour. Long-poll `mission_events` (E.4) is the subscription. *Recommendation: accept.*

### E.3 How an agent inspects running state

### E.3.1 The three reads and what each answers

| Question the agent has | Tool | Bound |
|---|---|---|
| "Where is this Mission right now, what is blocked, what did it cost?" | `mission_peek` | `PeekView` size cap; `subtree`, `filters`, `after_cursor` |
| "What is the program, and what is each activation's `lifecycle` / `phase` / `terminal_outcome`?" | `mission_program` | one Revision (`revision_id` or head), optional `node_key` subtree |
| "Which Artifact and Disposition satisfied which Success Criterion?" | `mission_trace` | one Goal or Objective root |
| "What happened since I last looked?" | `mission_events {after_seq}` | `max` events, long-poll window |

`PeekView` is the candidate §11.3 shape re-expressed in accepted vocabulary and is the *only* read an agent needs on every turn:

```text
PeekView@1 {
  mission:   { mission_id, lifecycle, closure_outcome?, depth, root_mission_id, parent_mission_id?, head_revision_id, budgets_by_credit_account[] }
  run:       { run_id, lifecycle, terminal_outcome? }
  activations[]: { node_key, activation_id, behavior, lifecycle, phase?, terminal_outcome?, attempt_no, agent_runtime_kind?, blockers[] }
  active_attempts[]: { activation_id, attempt_no, harness_execution_id, native_session_ref?, native_turn_ref?, last_event_at,
                       delivery_semantics_by_command: { queue_instruction, interrupt_and_inject, pause, cancel } }
  blockers:  { human_tasks[], event_waits[], proof_gates[], budget_holds[] }
  recent_events[]: last N MissionEvent envelopes; next_seq
  artifacts: inventory { artifact_id, kind, digest, size_bytes, producer_activation_id }     // never bodies
  portals[]: child Portal summaries (06 §6)
  dispositions_by_node[]
  subagents[]: { subagent_activation_id, parent_activation_id, lifecycle, terminal_outcome?, report_artifact_ref? }   // E.5
}
```

`delivery_semantics_by_command` is included per active Attempt so the agent knows *before* issuing a Command what will happen (`wait_then_send` on Cursor cloud, `turn_boundary_guaranteed` on Eve, `emulated` for `pause` everywhere).

### E.3.2 What an agent can and cannot see

| Can see | Cannot see (and why) |
|---|---|
| Three-field state of every activation in its grant's subtree | Any activation outside the subtree the token was scoped to |
| Artifact pointers, digests, sizes, producer, bounded excerpts | Full Artifact bodies through MCP; bodies are fetched through the Artifact store URL returned in the pointer, under the same grant |
| Mission Events with `seq` cursor | Agent text, thinking, full tool bodies — those never become Mission Events (`09 §5`) |
| Child Portals (identity, lifecycle, outcome, projected outputs, blockers, budget) | Child transcripts or child events; a client wanting child detail opens the child's own reads within its grant (`09 §6`) |
| Native Event Store agent tail, **labeled `canonical: false`**, only with `mission.read` plus the explicit `native_tail` flag on the grant | Native tail on other Missions or beyond the bounded window; the tail is an audit aid, never a state source |
| Its own Subagent reports (E.5) | Sibling Subagent internals |

**D-E6.** The Native Event Store agent tail is exposed to agents only through `mission_events {source: native, harness_execution_id}` when the grant carries `native_tail: true`, and every returned row carries `canonical: false`. *Recommendation: accept; default grants for agents omit `native_tail`.*

### E.3.3 Intervention from the same seat

An agent with `mission.command` steers through `mission_command`. The call and its result:

```text
mission_command {
  mission_id, command_id            // UUIDv7 minted by the caller; idempotency key
  kind: queue_instruction | interrupt_and_inject | pause | hard_pause | resume | cancel | retry | rerun | fork | request_continuation | request_revision
  target: { run_id?, activation_id?, attempt_no? }
  payload: { instruction_text?, artifact_refs[]?, reason?, inputs_digest? }
}
→ { command_id, lifecycle: accepted | queued | delivered | observed | completed, outcome?: applied | failed | rejected | expired,
    delivery_report: { delivery_semantics, delivered_at?, observed_at?, native_ref?, note } }
```

The tool returns as soon as the Command is `accepted` with the *predicted* Delivery Semantics from `describeSystem`; the agent reads `mission_command_get` (or sees `command.delivered` / `command.observed` in `mission_events`) for the actual report. `rejected` with `reason: scope_insufficient` is the normal answer to an over-reaching agent; the kernel never escalates.

### E.4 Supplying a turn-based agent with real-time tracing

### E.4.1 The constraint

An agent runs in Turns. It cannot hold a WebSocket or SSE connection across Turns, and inside a Turn it can only act on what is in its context. Two channels therefore exist: the agent **pulls** when it decides to look, and Mission Control **pushes into the agent's next Turn** when something the agent must know has happened. Pull is primary; push is an accelerator. Neither channel ever carries an unbounded stream into a context window.

**D-E7.** Default tracing pattern for agents is cursor-based pull with server-side long-poll; push is delivered only as a bounded `TraceDigest` Artifact through `queue_instruction`. *Recommendation: accept.*

### E.4.2 Pull: `mission_events` with long-poll

```text
mission_events {
  mission_id
  after_seq                 // replay cursor; 0 for from-start, -1 for tail-only (returns next_seq without events)
  types[]?                  // event families or full types, e.g. ["activation", "human_task.opened"]
  node_key?                 // subtree filter
  wait_ms?                  // 0 = return immediately; max 25_000; server holds the request until ≥1 event or timeout
  max?                      // default 100, cap 500
  source?: mission | native // default mission; native requires native_tail grant (E.3.2)
}
→ { events[]: MissionEvent, next_seq, gap_detected: boolean, truncated: boolean, canonical: boolean }
```

Rules:

- `next_seq` is always returned, even on timeout, so the agent's cursor advances monotonically and never re-reads.
- `gap_detected` is true when `events[0].seq > after_seq + 1`; the agent must re-read from `after_seq` with a smaller `max` before trusting state (`09 §5`).
- The 25-second cap keeps one tool call inside every host's tool timeout; the skill teaches the agent to loop on `wait_ms: 20000` while waiting for a blocker to clear rather than sleeping.
- `missionctl events tail --follow --after-seq N` is this loop printed as NDJSON.

The long-poll is implemented at the kernel seam over the same outbox relay that feeds the dashboard's WebSocket gateway; the agent and the dashboard read the same `seq` sequence.

### E.4.3 Push: harness hooks into Mission Control, and Mission Control into the next Turn

Push has two directions and both are accelerators over the durable stream.

**Harness → Mission Control.** The agent's own runtime tells Mission Control that something happened, so the kernel does not wait for the observe activity's next poll.

| Runtime | Hook | What it emits | Delivery |
|---|---|---|---|
| Eve | `defineHook({ events: { "turn.completed", "action.result", "subagent.completed", "compaction.completed": handler } })` in `mission-eve-agent` | `POST /v1/internal/harness-events` with the native event's `meta.id` and `(turnId, stepIndex, sequence)` | at-least-once; kernel dedupes on native id |
| Cursor cloud / local | `.cursor/hooks.json` command hooks `stop`, `afterFileEdit`, `subagentStart`, `subagentStop`, `preCompact` | hook process runs `missionctl trace emit --hook <name> --stdin` which POSTs the same endpoint with `conversation_id`, `generation_id` | at-least-once |
| Claude Code | `.claude/settings.json` hooks `Stop`, `PostToolUse`, `SubagentStop`, `PreCompact` | same `missionctl trace emit` | at-least-once |
| Codex | none programmatic; `stop`-equivalent via wrapper | `missionctl trace emit --hook stop` from the launcher | best-effort |

`missionctl trace emit` is the *only* write a hook may perform; it carries `MISSION_CONTROL_TOKEN` with `mission.read` only, because a harness event is an observation, never a Command. The endpoint validates that `harness_execution_id` in the token matches the emitting Session; mismatch is `provenance` and quarantines the row in the Native Event Store.

**D-E8.** `POST /v1/internal/harness-events` accepts only observations (native event envelope + native ids) under a Session-scoped `mission.read` token; it can never create a Command, Artifact, or Disposition. *Recommendation: accept.*

**Mission Control → agent.** When a trace matters to the agent (a Human Task it is waiting on resolved, a Proof Gate released, a sibling activation `not_accepted`, a budget threshold crossed, a Subagent completed), the kernel produces a `TraceDigest` Artifact and delivers it with `queue_instruction` so it enters the agent's context at its next safe Turn boundary. Delivery Semantics apply exactly as for any Command: `turn_boundary_guaranteed` on Eve, `wait_then_send` on Cursor cloud, and the Delivery Report is recorded.

Push triggers are declared, not ad hoc, in the Agent Executor Binding:

```text
trace_push_policy {
  on: [ human_task.resolved, proof_gate.released, activation.completed{ node_key in watched[] }, budget.threshold_crossed, subagent.completed, command.observed ]
  min_interval_ms: 30_000          // coalesce; never more than one digest per interval per Attempt
  max_digest_bytes: 16_384
}
```

### E.4.4 `TraceDigest@1` and the `trace_digest` Executor Kind

An agent never receives raw events by push. It receives a **Trace Digest**: a bounded, digest-bound Artifact summarizing the canonical stream between two cursors, produced deterministically.

```text
TraceDigest@1 {
  mission_id, run_id, revision_id
  for_activation_id, for_attempt_no          // the consumer
  from_seq, to_seq                           // inclusive cursor window
  produced_at, producer: { executor_kind: trace_digest, version }
  counts_by_family: { activation: n, command: n, human_task: n, ... }
  state_delta[]:   { node_key, activation_id, lifecycle, phase?, terminal_outcome?, changed_at }
  blockers_now:    { human_tasks[], event_waits[], proof_gates[], budget_holds[] }
  blockers_cleared[]
  artifacts_registered[]: { artifact_id, kind, digest, producer_activation_id }
  dispositions_recorded[]: { source, value, activation_id }
  commands_observed[]: { command_id, kind, delivery_semantics, outcome? }
  subagents[]: { subagent_activation_id, lifecycle, terminal_outcome?, report_artifact_ref? }
  next_seq
  digest                                     // sha256 over canonical JSON of the above
}
```

`trace_digest` is a Deterministic Executor Kind (`05 §4.1` registry) with input `(mission_id, for_activation_id, from_seq, to_seq)` and output one `TraceDigest@1` Artifact; idempotent on the standard key, so a retried push never produces a second digest for the same window. Because it is an Executor Kind it writes an Operation Intent and Receipt, and because it is deterministic the dashboard can regenerate any digest an agent saw.

**D-E9.** Add `trace_digest` to the Executor Kind registry and `TraceDigest@1` to the Artifact contracts; agents receive traces only as this Artifact or through `mission_events` pull. *Recommendation: accept.*

### E.4.5 What "real-time" honestly means here

| Path | Latency bound | Guarantee |
|---|---|---|
| Pull, long-poll | outbox relay lag + 0–25 s | at-least-once, gap-detectable, replayable |
| Harness → MC hook | sub-second after native durable write | at-least-once, dedupe on native id; accelerates the observe activity only |
| MC → agent digest | next safe Turn boundary of the consumer | Delivery Semantics per runtime; `wait_then_send` may be minutes on a busy Cursor run |

The skill states these bounds verbatim so an agent never reasons as if it had a socket.

### E.5 The Subagent tool: bounded fan-out that is not a Mission

### E.5.1 Why a third thing

The accepted taxonomy gives an agent two ways to get more hands: a **nested workflow system** (compiled ahead of time, pursues existing Objectives) and a **Child Mission Invocation** (independent governance, its own Goals, a Portal, a Spawn Grant). Neither fits the everyday case where an Agent Executor, mid-Attempt, wants a *bounded helper*: "read these four Artifacts and produce a comparison table", "run the failing test in isolation and report", "draft the schema for this one node". That case needs a fresh context window with a narrower instruction, a subset of the parent's tools, and a result handed back as an Artifact. It does not need Goals, a Revision, a Portal, or Mission identity. Forcing it into a Child Mission makes every helper a governance event; forcing it into a nested system requires the author to have compiled the helper into the program before the run.

Frameworks already do this natively (Eve subagents, Cursor `customSubagents`, Claude Code subagents). Left unmodeled, those native fan-outs are invisible to Mission Control except as tool-call digests, which breaks budget accounting, Side-Effect Class enforcement, and traceability. So the kernel needs one concept that both *governs the fan-outs it provisions* and *records the fan-outs a framework performs on its own*.

**D-E10.** Introduce **Subagent Invocation** as a new atomic Program Node behavior in the executor family, alongside Agent Executor and Deterministic Executor; it is Attempt-level fan-out within one Mission and one Revision. *Recommendation: accept; this amends `00 §4.2` and `CONTEXT.md`.*

### E.5.2 Definition

> **Subagent Invocation**: an atomic behavior through which an active Agent Executor Attempt starts one bounded child Agent Session, composed from a Subagent Profile, that pursues the same Objectives under the same Revision, budget, and grants, and returns a Completion Candidate and Artifacts to the invoking Attempt. It shares nothing else with its parent and can never widen the parent's authority.
> _Avoid_: child mission, nested workflow system, worker, tool call.

> **Subagent Profile**: the versioned, admitted composition of a Subagent Invocation: instruction, Operating Contract reference, skills, MCP servers, allowed Executor Kinds, Side-Effect Class ceiling, budget slice, output contract, and context seed references.
> _Avoid_: prompt, agent config, capability profile.

> **Subagent Report**: the bounded Artifact returned to the invoking Attempt when a Subagent Invocation completes: Completion Candidate reference, registered Artifacts, usage, terminal outcome, and blockers. Never a transcript.
> _Avoid_: child stream, chat message.

### E.5.3 Subagent versus nested system versus Child Mission

| Concern | Subagent Invocation | Nested workflow system | Child Mission Invocation |
|---|---|---|---|
| Decided when | at run time by the acting agent, within the Action Space | at authoring time, compiled | at authoring time (`inline`, `template`) or run time within a Spawn Grant |
| Governance | parent's Revision, budget, grants, Objectives | same Mission | own Goals, authority, budget, owner, lifecycle |
| Identity | Activation of behavior `subagent` under the parent activation; own Harness Execution and Agent Session | Activation of the system | new Mission identity + Portal |
| Observation by parent | `SubagentReport` Artifact + `subagent.*` events; no token stream | activation state and projected outputs | Portal only |
| May widen authority | never | never | never; needs a Spawn Grant to exist at all |
| Depth | `max_subagent_depth` (default 2; matches Cursor's native limit) | static nesting | Spawn Grant depth (soft 3, hard 16) |
| Use it for | helpers, isolated checks, parallel reads, drafting one piece | known routes, loops, swarms, evaluation rounds | anything needing its own owner, schedule, or acceptance |

Rule of thumb the skill teaches: *if the helper's result is acceptable only because the parent accepts it, it is a Subagent; if the helper's result must be accepted on its own terms, it is a Mission; if the route was known before the run started, it is a nested system.*

### E.5.4 Contract

```text
SubagentProfile@1 {
  profile_id, version                         // catalog record; admitted like any capability version
  instruction: { operating_contract_ref, instruction_text }
  harness: { agent_runtime_kind?, model?, model_access_kind?, compute_environment_kind?, credit_account_kind? }   // absent ⇒ inherit parent's
  skills[]: { skill_ref, version }            // ⊆ parent's admitted skills
  mcp_servers[]: { server_ref, version, allowed_tools[]? }   // ⊆ parent's
  executor_kinds_allowed[]                    // ⊆ parent's Action Space
  side_effect_class_ceiling                   // ≤ parent's; default read_only
  budget_slice: { tokens?, money?, wall_clock_ms? }   // carved from the invoking Attempt's remaining budget
  output_contract: { outputs[]: { output_name, schema_ref } }
  context_seed_refs[]                         // immutable Artifact references (Metadata load only; bodies fetched by the child within grant)
  max_turns, max_transfers
}

SubagentInvocation {
  invoking_activation_id, invoking_attempt_no
  profile_ref | inline_profile                 // inline requires parent's grant to allow `subagent_inline_profiles`
  input_refs[]                                 // immutable Artifact references
  instruction_delta?                           // narrowing only; validated against the profile
  await: await | track                         // no `detach`: a Subagent never outlives its parent Attempt
}

SubagentReport@1 {
  subagent_activation_id, parent_activation_id, parent_attempt_no
  lifecycle, terminal_outcome, reason?
  completion_candidate_ref?
  outputs[]: { output_name, artifact_ref }
  usage: { tokens, cost, wall_clock_ms, turns, transfers }
  blockers[]
  native: { agent_runtime_kind, native_session_ref, native_turn_refs[] }
  origin: kernel_invoked | framework_observed  // E.5.6
}
```

Validation (at authoring for profiles, at invocation for the delta) enforces the subset laws: skills, MCP servers, executor kinds, and Side-Effect Class ceiling must each be a subset of, or at most equal to, the invoking Attempt's binding. Widening is `denied(outside_action_space)` and recorded, exactly as an Action Authorization denial in `02 §6.2`.

### E.5.5 Identity, lifecycle, and events

A Subagent Invocation is recorded as an **Activation** with `behavior: subagent`, `parent_activation_id` set to the invoking activation, and its own `attempt`, `harness_execution`, `agent_session`, and `session_turn` rows. It uses the shared three-field state with no phases. Terminal outcomes are the base vocabulary; `accepted` here means *the Subagent's own Completion Contract (its output contract) accepted its outputs*, which is necessary but never sufficient for the parent's acceptance.

Cancellation cascades: cancelling the parent Attempt cancels every `running | waiting` Subagent (`on_parent_cancel: cancel_child` is the only option). A Subagent's `not_accepted` does not fail the parent; the parent decides what to do with the report.

New event family, the seventeenth:

| Family | Events | Key payload fields |
|---|---|---|
| `subagent` | `invoked`, `started`, `completed`, `denied` | `subagent_activation_id`, `parent_activation_id`, `profile_ref`, `origin`, `terminal_outcome`, `report_artifact_ref`, `denial_reason` |

All four fit `<aggregate>.<past_tense_verb>`. `subagent.started` is distinct from `invoked` because provisioning a child Session may fail or wait (`capacity`), and the gap between the two is what the dashboard shows as "spawning". Session-level detail (`session.started`, `session.turn_completed`, `tool_call.completed`) is emitted for the child under the child's own `activation_id`, so the parent's event stream shows the child at activation granularity and the child's detail is filtered by `node_key`/`activation_id`. No new lifecycle or outcome literals are introduced.

**D-E11.** Add the `subagent` event family with `invoked | started | completed | denied` and record every Subagent Invocation as an Activation of behavior `subagent` under its parent activation. *Recommendation: accept; amends `09 §3` and the `activation` table with a nullable `parent_activation_id`.*

### E.5.6 Kernel-invoked versus framework-observed

Two origins, one record:

| Origin | How it starts | How Mission Control learns | What is enforced |
|---|---|---|---|
| `kernel_invoked` | the agent calls `mission_subagent_invoke` (or the equivalent `missionctl subagent invoke`); the kernel validates the profile subset laws, allocates the budget slice, and provisions the child Session through `AgentHarness.prepare/start` on the chosen lane | it made it | everything: profile admission, subset laws, budget, Side-Effect Class ceiling, depth, count |
| `framework_observed` | the runtime fans out on its own: Eve `subagent.called {childSessionId}` → `subagent.started` → `subagent.event` → `subagent.completed`; Cursor `subagentStart` / `subagentStop` hooks (`customSubagents`, depth 2), Claude Code `SubagentStart` / `SubagentStop` | the adapter ingests the native events and records a `subagent` Activation with `origin: framework_observed`, `native_session_ref` = child session id | usage is attributed and counted against the parent's budget; Side-Effect Class ceiling is enforced only through the parent's tool allowlist; depth/count caps are checked after the fact and produce `subagent.denied` + a `policy_denied` blocker on the parent if exceeded |

The distinction is recorded, not hidden, because the two differ in what Mission Control can promise. The skill teaches agents to prefer `mission_subagent_invoke` for anything with a Side-Effect Class above `read_only`, because only kernel-invoked Subagents get a ceiling enforced before the fact. Adapters map native subagent identity as: Eve `childSessionId` → `native_session_ref`, parent re-emitted `subagent.event` rows → Native Event Store only (never Mission Events); Cursor `subagentStart.conversation_id` → `native_session_ref`.

**D-E12.** Framework-native subagents are recorded as `origin: framework_observed` Subagent Invocations with after-the-fact governor checks; kernel-invoked Subagents are the only ones with before-the-fact Side-Effect Class enforcement, and the skill says so. *Recommendation: accept.*

### E.5.7 Tool surface

```text
mission_subagent_invoke {
  mission_id, activation_id, attempt_no          // the caller's own identity, verified against the token
  invocation_id                                  // UUIDv7; idempotency key
  profile_ref | inline_profile
  input_refs[], instruction_delta?, await
}
→ { subagent_activation_id, lifecycle, budget_slice_allocated, denied?: { reason } }
   // await=await: returns when the Subagent completes, bounded by the host tool timeout; on timeout returns lifecycle=running and the agent polls
   // await=track: returns immediately; completion arrives as subagent.completed in mission_events or as a TraceDigest push

mission_subagent_report { subagent_activation_id } → SubagentReport@1
```

Governors, all declared on the invoking Agent Executor Binding and pinned by the Revision:

```text
subagent_policy {
  allowed: boolean                               // default false through M7; true requires the binding to name profiles
  profiles_allowed[]: profile_ref                // catalog; `inline` permitted only with subagent_inline_profiles: true
  max_concurrent, max_total_per_attempt
  max_subagent_depth                             // default 2
  budget_share_max                               // fraction of the Attempt's remaining budget any one Subagent may take
  side_effect_class_ceiling                      // hard cap for every Subagent under this binding
}
```

Every denial is `subagent.denied` with its reason, mirroring Spawn Grant denials in `06 §7`.

**D-E13.** Subagent Invocation is off by default; a binding enables it by naming admitted profiles. *Recommendation: accept; keeps M6 `capability_binding: none` intact, with Subagent profiles arriving as the first catalog-bound capability in M8/M10.*

### E.5.8 Where it lands on the program

Recording `framework_observed` Subagents is adapter work and belongs in `F5.2` / `F5.3` (the adapters already ingest those native events). Kernel-invoked Subagents need profile admission, which is catalog work; the minimal form (profiles declared inline in the exemplar, no search) can ship in M8 alongside `F8.4`, with catalog-backed profiles in M10.

### E.6 The coordinator loop with the skill

`skills/mission-control/SKILL.md` teaches one loop. Every step names the tool, the scope it needs, and what the result means. Abbreviated:

1. **Describe.** `mission_describe_system {}` → runtimes with `available`, `commands[].delivery_semantics_by_runtime`, `schemas`, `exemplars`. The agent reads availability before choosing a lane.
2. **Draft.** `mission_draft { definition }` → `{ mission_id, lifecycle: drafting }`. Start from `mission_exemplar_get { name: "application_build_test_improve" }`, never from prose.
3. **Validate.** `mission_validate { mission_id, definition }` → `ValidationReport { ok, findings[] }`. Fix findings; do not submit a failing definition.
4. **Propose and activate.** `mission_propose { mission_id, base_revision_id: null, definition, proposal_id }` → `{ proposal_id, lifecycle: validated | awaiting_review }`; when `validated`, `mission_activate { mission_id, proposal_id }` → `{ revision_id, head_status: head }`. `awaiting_review` means a human decides; the agent polls `mission_events { types: ["revision"] }`.
5. **Start.** `mission_start { mission_id, revision_id, input_snapshot_refs[] }` → `{ run_id, lifecycle: running }`. Called from inside another Mission with `mission.invoke`, this is Spawn and records `parent_of`.
6. **Watch.** `mission_peek { mission_id }` once, then `mission_events { after_seq: next_seq, wait_ms: 20000, types: ["activation", "human_task", "command", "budget", "subagent"] }` in a loop. Handle `gap_detected` by re-reading.
7. **Steer.** `mission_command { command_id, kind: queue_instruction, target: { activation_id }, payload: { instruction_text, artifact_refs } }` → `{ lifecycle: accepted, delivery_report: { delivery_semantics: wait_then_send } }`. The agent reads the semantics and sets expectations: on `wait_then_send` it does not expect `observed` before the current Turn ends.
8. **Confirm delivery.** `mission_command_get { command_id }` or wait for `command.observed` in the loop. `outcome: rejected` with `reason` is final; the agent does not retry a rejected Command.
9. **Answer a Human Task it is entitled to.** `mission_human_tasks { mission_id }` → open tasks; `mission_human_task_resolve { human_task_id, resolution: answered, answer: { artifact_ref | text } }`. An agent may answer `question` and `selection` when its grant allows; it can never answer `approval` or `policy_override` (those require a human `actor_ref`; the kernel rejects agent actors).
10. **Fan out when needed.** `mission_subagent_invoke` per E.5; read `mission_subagent_report`.
11. **Close.** `mission_close { mission_id, closure_outcome }` requires `mission.admin`; ordinary agent grants stop at step 10 and leave closure to a human or an admin agent.

Without a provider key: the skill never needs `CURSOR_API_KEY`, an AI Gateway key, or an Eve deployment credential. It needs `MISSION_CONTROL_URL` and `MISSION_CONTROL_TOKEN` (a grant with `read, author, command`, optionally `invoke`). Provider credentials live in Mission Control's credit accounts and are resolved by the worker when it provisions a Session; the M7 exit proof "an agent that installed the skill starts a Mission from Artifacts without holding a provider key" is exactly this property.

**D-E14.** Agents can resolve `question` and `selection` Human Tasks when granted; `approval`, `review`, and `policy_override` reject non-human `actor_ref` at the kernel. *Recommendation: accept.*

### E.7 Packaging

One payload, host overlays, as the candidate §17.7 proposed; only the hook files change to point at the observation endpoint.

| Host | Manifest | Ships | Hook file → observation |
|---|---|---|---|
| Portable Agent Plugin | `plugin.json` + `mcp.json` | `skills/mission-control/SKILL.md`, MCP server config | none |
| Cursor | `.cursor-plugin/plugin.json` (+ `marketplace.json`) | skill, MCP, rules, `hooks/hooks.json` | `stop`, `afterFileEdit`, `subagentStart`, `subagentStop`, `preCompact` → `missionctl trace emit --hook <name>` |
| Claude Code | `.claude-plugin/plugin.json` (+ `marketplace.json`) | skill, MCP, `hooks/hooks.json` | `Stop`, `PostToolUse`, `SubagentStart`, `SubagentStop`, `PreCompact` → `missionctl trace emit --hook <name>` |
| Codex | skill + `agents/openai.yaml`; MCP via `config.toml` | skill, MCP | launcher wrapper emits `stop` |
| Eve | `agent/skills/mission-control/`, `connections/mission-control.json` | skill, MCP connection, `agent/hooks/mission-control.ts` (`defineHook`) | `turn.completed`, `action.result`, `subagent.*`, `compaction.*` → `POST /v1/internal/harness-events` |

Published packages: `@aiengineer/mission-contracts`, `@aiengineer/missionctl`, `@aiengineer/mission-control-skill`, `@aiengineer/mission-control-plugin`. Variables: `MISSION_CONTROL_URL`, `MISSION_CONTROL_TOKEN`. Hook tokens are Session-scoped `mission.read` grants minted by the worker at `prepare` and injected into the Session environment; they expire with the Session.

**D-E15.** Hook credentials are per-Session `mission.read` grants minted at `AgentHarness.prepare`, never the coordinator's own token. *Recommendation: accept.*

### E.8 Failure honesty

- Every `mission_command` result and every Command event carries `delivery_semantics`; the skill quotes the seven values and tells the agent what each implies for timing.
- `unsupported` is a valid, successful tool result (`outcome: rejected`, `delivery_semantics: unsupported`, `note` naming the runtime). The agent is taught to read `describe_system` first so it rarely hits it.
- `emulated` always carries `note` naming the emulation (`cancel_turn + checkpoint + transfer`).
- A `mission_subagent_invoke` on a runtime with no `AgentHarness` support for child provisioning returns `denied: { reason: runtime_unsupported }` and the skill says to fall back to `framework_observed` fan-out or a Child Mission.
- Agent prose never widens authority: `instruction_text` in any Command, `instruction_delta` in any Subagent Invocation, and any Human Task `answer` are content, not policy. Widening requires `mission_propose` → review, or `mission.admin`.
- Every refusal is recorded as an event (`command.completed{outcome: rejected}`, `subagent.denied`, `child_mission.spawn_denied`) so a reviewing human can see what an agent tried.

### E.9 Decisions for sign-off (Part E)

| # | Decision | Recommendation |
|---|---|---|
| D-E1 | One `MissionApplication` service catalogue in `mission-kernel`; CLI, MCP, Skill, and HTTP are thin faces | Accept |
| D-E2 | Authority scopes are the only authorization vocabulary in tool contracts; roles are dashboard groupings | Accept |
| D-E3 | Tool naming rule `mission_<verb>` / `mission_<aggregate>_<verb>`; rename `mission_service_status` → `mission_describe_system` with a one-release alias | Accept |
| D-E4 | M7/M8 tool surface per E.2.4, adding `mission_trace`, `mission_command_get`, `mission_artifact_get`, `mission_subagent_*`, `mission_close` | Accept |
| D-E5 | No `mission_subscribe`; long-poll `mission_events` is the subscription | Accept |
| D-E6 | Native Event Store tail exposed to agents only with `native_tail` grant, always `canonical: false` | Accept |
| D-E7 | Pull with long-poll is the default tracing pattern; push only as bounded `TraceDigest` via `queue_instruction` | Accept |
| D-E8 | `POST /v1/internal/harness-events` accepts observations only, under Session-scoped `mission.read` | Accept |
| D-E9 | Add `trace_digest` Executor Kind and `TraceDigest@1` Artifact contract | Accept |
| D-E10 | Introduce Subagent Invocation as an atomic executor-family behavior (amend `00 §4.2`, `CONTEXT.md`) | Accept |
| D-E11 | Add the `subagent` event family (`invoked, started, completed, denied`) and `activation.parent_activation_id` | Accept |
| D-E12 | Record framework-native subagents as `origin: framework_observed` with after-the-fact governors | Accept |
| D-E13 | Subagent Invocation off by default; enabled per binding by naming admitted profiles | Accept |
| D-E14 | Agents may resolve `question`/`selection`; `approval`/`review`/`policy_override` require a human actor (see also D-F3) | Accept |
| D-E15 | Hook credentials are per-Session `mission.read` grants minted at `prepare` | Accept |

---
## Part F — Human-in-the-loop middleware: the Deliverable Review Gate

### F.1 The gap

The suite already has the primitives for a human to be in the loop: Human Gate with five Human Task kinds (`05 §6.3`), Action Authorization with Side-Effect Classes (`02 §6.2`), Proof Gate, and the separation of Execution Completion, Goal Acceptance, Mission Acceptance, Downstream Admission, and Publication (ADR 0003). What it lacks is the thing an operator actually reviews: a **Deliverable** — a working codebase feature, a business report, a dataset — with its evidence attached, and the four verbs the operator wants on it: **review, approve, promote, reject**. Today "a Mission produced a PR" is a Stage that projected an Artifact; nothing names the PR as the unit of decision, assembles what a reviewer needs, or ties the decision to the promotion that follows.

**D-F1 — Introduce `Deliverable` as a new aggregate: a named, reviewable, promotable output bundle of a Mission, declared in the Mission Definition and materialized per Run.** Amends `CONTEXT.md`, `00 §5.2` (Output Projection may target a Deliverable), `09 §3` (new `deliverable` family), schema (`deliverable`, `review_packet`). Reversibility: moderate.

> **Deliverable**: a named output bundle of a Mission — such as a codebase feature or a business report — composed of projected Artifacts and their evidence, with a declared review policy and promotion path. Its review state and promotion state are separate facts from the acceptance of the activations that produced it.
> _Avoid_: artifact, output, result, PR.

> **Review Packet**: the bounded, digest-bound Artifact assembled for a human reviewer of a Deliverable: summary, Artifact references, diff or preview references, test and verification dispositions, evaluation reports, cost, and risk.
> _Avoid_: transcript, chat summary, dashboard page.

### F.2 Declaration and compiler synthesis (the "middleware")

The middleware is a compiler pass plus a kernel interceptor, not a runtime plugin:

```text
DeliverableDeclaration {
  deliverable_key                              // stable per Mission
  deliverable_kind: codebase_feature | business_report | research_report | dataset | design | policy_change | other
  produced_by_node_key                         // the node whose accepted Output Projection fills it
  outputs[]: { output_name }                   // subset of that node's projected outputs
  evidence: { test_receipts: required | optional | none, verification_intents[]?, evaluation_reports: required | optional | none }
  review_policy: {
    required: boolean                          // default true for every kind except `other`
    assignee_policy: any_operator | role { name } | user { id }
    quorum?: n                                 // default 1
    timeout?, on_timeout: keep_waiting | escalate | stop
    reject_to: remediation | stop              // what a review_reject releases
  }
  promotion?: {
    executor_kind: code_promote | report_promote | dataset_promote
    target: { forge_ref? | knowledge_domain? }
    requires_second_approval: boolean          // default true when side effects are irreversible
  }
}
```

Compilation synthesizes, per Deliverable, three Program Nodes that always appear explicitly in the Compiled Program (the "no hidden waits" law of `05 §6`):

1. `deliverable_review_packet:<key>` — Deterministic Executor `review_packet_assemble`, released when `produced_by_node_key` is `accepted`.
2. `deliverable_review:<key>` — Human Gate raising one `review` Human Task whose `context_refs[]` is the Review Packet; outcome per `05 §6.3`.
3. `deliverable_promotion:<key>` — Deterministic Executor from `promotion.executor_kind`, Side-Effect Class `external_write_irreversible`, gated on `review_accept` with `promote: true`; when `requires_second_approval`, an additional Human Gate `approval` is synthesized in front of it.

The kernel interceptor is `onActivationCompleted` (D.6): when the producing node is `accepted`, it creates the `deliverable` row (`review_state: awaiting_packet`), and the synthesized nodes carry it through. Nothing bypasses the Compiled Program; a Deliverable is therefore replayable, revisable, and visible in topology like any other node.

### F.3 Review Packet

```text
ReviewPacket@1 {
  contract_version, deliverable_id, mission_id, run_id, revision_id
  summary: { title, what_changed, why, risks[], open_questions[] }          // produced by the assembler from Completion Candidate notes and Progress Reviews; concise, no chain-of-thought
  artifacts[]: { output_name, artifact_ref, kind, digest, size_bytes, preview_ref? }
  diff_ref?, pr_ref?, branch_ref?                                            // codebase_feature
  document_ref?, rendered_ref?                                                // business_report / research_report
  tests[]:   { receipt_ref, outcome, summary }
  verification[]: { assessment_id, disposition, ks_operation_id }
  evaluations[]: { evaluation_report_ref, rubric_version, score, disposition }
  cost: { tokens, cost_usd, wall_clock_ms, by_credit_account[] }
  provenance: { producer_activation_ids[], agent_runtime_kinds[], models[] }
  promotion_preview: { executor_kind, side_effect_class, target, reversible: false }
  digest
}
```

Assembled deterministically by `review_packet_assemble`; idempotent on the standard executor key; sealed as an Artifact so the reviewer, the audit, and any later dispute see the same bytes.

### F.4 The four verbs and the state machines

Deliverable `review_state`: `awaiting_packet → open → claimed → reviewed | expired | cancelled`  
Deliverable `promotion_state`: `not_requested → requested → awaiting_approval → promoted | promotion_failed | declined`

| Operator verb | What it records | Vocabulary reused |
|---|---|---|
| **Review** | claims the `review` Human Task (`claimed`); reads the packet | `human_task.claimed` |
| **Approve** | resolves the task `review_accept`; Deliverable `reviewed`; promotion stays `not_requested` | `human_task.resolved{review_accept}`, `deliverable.reviewed` |
| **Promote** | `review_accept` with `promote: true` (or a later `promotion` request on an already-reviewed Deliverable) → `promotion_state: requested` → releases the promotion node; when second approval is required, opens the `approval` task | `deliverable.promotion_requested`, `human_task.opened{approval}`, `deliverable.promotion_recorded` |
| **Reject** | resolves `review_reject` with a `findings_ref` Artifact; per `reject_to`, releases Remediation (findings as typed input to the producing node's next activation) or stops | `human_task.resolved{review_reject}`, `deliverable.reviewed`, then `activation.*` of the remediation |

**D-F2 — Keep the accepted Human Task resolution vocabulary (`review_accept | review_reject`); "promote" is a flag on the resolution that releases a synthesized promotion node, and "request changes" is `review_reject` with findings.** No new resolution literals; the UI composes the four verbs from two resolutions and a flag. Reversibility: cheap.

New event family:

| Family | Events | Key payload fields |
|---|---|---|
| `deliverable` | `materialized`, `packet_assembled`, `review_opened`, `reviewed`, `promotion_requested`, `promotion_recorded`, `expired` | `deliverable_id`, `deliverable_kind`, `review_state`, `promotion_state`, `resolution?`, `promote?`, `findings_ref?`, `receipt_ref?` |

### F.5 Who may review, how they are told, and what they see

- **Scope.** `review`, `approval`, and `policy_override` tasks resolve only with a human `actor_ref` (D-E14). This proposal adds one authority scope, **`mission.review`**, granted to human principals, so that an operator who may steer (`mission.command`) is not automatically a reviewer of the work they steered. (D-F3, below).
- **Inbox.** `GET /v1/human-tasks` and the `human_tasks` stream channel feed the dashboard Inbox (H.1); cards show kind, Deliverable kind, age, cost, and the packet summary; actions are the four verbs plus "delegate" (re-assign within `assignee_policy`) and "ask" (open a `question` task back to the producing agent, delivered by `queue_instruction`).
- **Notification.** `human_task.opened` and `deliverable.review_opened` produce `Notification` rows on the `notification` outbox topic; adapters: `dashboard_inbox` (always), `email` and `webhook` (M7), `slack`/`push` later. Every notification carries the `mc://human-tasks/{id}` deep link; the inbox is the truth, notifications are best-effort.
- **SLA.** `timeout` / `on_timeout` per declaration; `escalate` re-assigns per an escalation list on the tenant; `human_task.expired` is visible in topology as a blocker.
- **Batch.** The inbox supports multi-select approve/reject for `question`/`selection` and approve for `review` when the packets' digests are unchanged since the list was loaded (optimistic concurrency on `review_packet.digest`).

**D-F3 — A new authority scope `mission.review`, granted to human principals only, is required to resolve `review`, `approval`, and `policy_override` tasks; `mission.command` alone never suffices.** Amends `09 §9`. Reversibility: cheap.

### F.6 Two worked flows

**Codebase feature.** `application_build_test_improve` declares `deliverable feature_pr { kind: codebase_feature, produced_by: implement_feature, evidence: { test_receipts: required }, promotion: { executor_kind: code_promote, target: { forge_ref }, requires_second_approval: false } }`. Run: the Stage `implement_feature` (Cursor cloud) is `accepted` once `test_run` receipts pass → `review_packet_assemble` builds the packet with `pr_ref`, `diff_ref`, test summaries, cost → `review` task opens; the operator reads the diff in the dashboard (rendered from the Artifact under grant), approves with `promote: true` → `code_promote` merges the PR and records an Operation Receipt → `deliverable.promotion_recorded`. A reject with findings releases Remediation on `implement_feature` with the findings Artifact as input.

**Business report.** `business_report_weekly` declares `deliverable report { kind: business_report, produced_by: write_report, evidence: { verification_intents: [verifyReport], evaluation_reports: required }, promotion: { executor_kind: report_promote, target: { knowledge_domain: "reports" }, requires_second_approval: true } }`. Run: the Evaluator Optimizer round is `accepted` at threshold, `verification_dispatch(verifyReport)` yields `admitted` → packet with `document_ref`, `rendered_ref`, evaluation scores, KS disposition → `review` task; approve with `promote: true` → `approval` task (second approval) → `report_promote` submits `promotion.submit` to Knowledge Services and records the decision KS returns. Publication remains KS's decision; Mission Control records it.

### F.7 Mid-execution human-in-the-loop (already accepted; how it surfaces)

Action Authorization `pending_human_task` (`approval`), Goal Loop `question` parking, Eve `input.requested` and `authorization.required` mapped to `question` / `approval` tasks, MCP elicitation for `question`/`selection` in connected hosts (C.4), and `policy_override` for governor and depth exceptions. All land in the same inbox and stream channel; the Deliverable Review Gate adds the end-of-work review without inventing a second inbox.

---

## Part G — Searchable, retrievable, navigable: MissionFS, search, and Context Packs

### G.1 Principle

Agent runtimes have made the file system the intellectual foundation of agent work: Eve is "filesystem-first", skills are directories, Deep Agents ship a filesystem middleware, Cursor and Claude Code read `.cursor/` and `.claude/` trees. Mission Control's structures — schemas, exemplars, patterns, Missions, Revisions, programs, Runs, activations, Artifacts, Human Tasks, Deliverables — must therefore be **navigable as a tree** and **searchable as a corpus**, for agents and for product consumers alike, with one address grammar (D-A3).

**D-G1 — MissionFS: a read-only, grant-scoped virtual file system over the ledger, with one canonical tree, exposed identically as MCP resources, `missionctl fs`, the `/v1/fs` route, a workspace seed, and the dashboard Explorer.** Reversibility: moderate (the tree shape becomes an agent-facing contract).

### G.2 The tree

```text
/mission-control/
  README.md                                   how to navigate; links to schemas and exemplars
  schemas/<schema_name>.v1.json
  exemplars/<exemplar_name>.mission.json
  patterns/goal/<name>@<version>.md · patterns/objective/<name>@<version>.md
  profiles/subagent/<name>@<version>.json
  missions/<mission_id>/
    MISSION.md                                human-readable: goals, objectives, head revision, lifecycle, deliverables, budgets
    definition.json                           head Revision's Mission Definition
    revisions/<revision_id>/definition.json · program.json · transition-impact.json
    runs/<run_id>/
      RUN.md                                  lifecycle, outcome, blockers, cost
      program-state.json                      three-field state per node
      events.ndjson                           bounded window (last N) with `next_seq`; full history via the events route
      activations/<node_key>/<activation_id>/
        ACTIVATION.md                         lifecycle/phase/outcome, attempts, native refs
        completion-candidate.json
        journal/segments/<n>.json             Goal Loop only
        subagents/<subagent_activation_id>/   report.json
      human-tasks/<human_task_id>.json
      commands/<command_id>.json
      deliverables/<deliverable_id>/DELIVERABLE.md · review-packet.json
    children/<child_mission_id>               link → /mission-control/missions/<child_mission_id> (Portal view only)
  artifacts/<artifact_id>/manifest.json       bodies via governed fetch, never inline
  human-tasks/<human_task_id>.json            tenant inbox (assignee-scoped)
```

Rules: every node is a `MissionFsNode` with a `ref` (`mc://…`); `*.md` documents are generated from read models by deterministic renderers (`renderMissionDocument`), so their digests are stable per state; the tree is *never* written to by clients (writes are Commands, proposals, and Human Task resolutions); directory listings are paged (`?after=`, 200 entries).

### G.3 Exposure

| Surface | Form |
|---|---|
| MCP | `resources/list` over the root; resource templates for every level; `mission_fs_read { path | ref, max_bytes }` returns a `MissionFsNode` with content for documents; `resources/subscribe` on a directory notifies on membership change |
| CLI | `missionctl fs ls <ref>`, `fs cat <ref>`, `fs find <ref> --name --kind --lifecycle`; output as NDJSON of `MissionFsNode` |
| HTTP | `GET /v1/fs/{path…}`; `Accept: text/markdown` returns the document, `application/json` the node |
| Workspace seed (`.mission/`) | At `AgentHarness.prepare`, the worker materializes the activation's own subtree as **Metadata load** (`2026-09-19` note §4.1): `.mission/MISSION.md`, `ACTIVATION.md`, `operating-contract.md`, `context-pack/…`, `artifacts/manifest.json`, `peek.json` — small, digest-listed files, no bodies, refreshed at every Turn boundary through `sendTurn` context or the Eve `defineDynamic` resolver. The agent navigates its Mission the way it navigates its repo. |
| Dashboard Explorer | a tree view over `/v1/fs` with the same paths, breadcrumbs `Mission > Run > Activation`, document preview, "open in inspector", and "copy `mc://` ref" |

### G.4 Search

**D-G2 — Structured and full-text search are served by Mission Control; semantic search is delegated to Knowledge Services over Mission Index Documents that Mission Control publishes.** Knowledge Services owns embeddings and vector stores (invariant 9); Mission Control owns its structured facts. Reversibility: cheap.

```text
MissionSearchQuery {
  q?                                   // full-text over search_document
  scope[]: missions | program_nodes | activations | artifacts | deliverables | human_tasks | events | exemplars | patterns | subagent_profiles
  filters: { tenant_id, lifecycle[]?, terminal_outcome[]?, behavior[]?, agent_runtime_kind[]?, deliverable_kind[]?, human_task_kind[]?, owner_ref?, labels[]?, time_range?, mission_ids[]? }
  semantic?: { enabled: boolean, top_k }    // routes q to KS retrieval over mission index documents; results merged by ref
  after?, max (≤ 200)
}
MissionSearchResult { hits[]: { ref, kind, title, snippet, score, facts{ lifecycle?, terminal_outcome?, deliverable_kind?, … } }, next?, sources: { structured: n, fulltext: n, semantic: n } }
```

- `search_document` (`tsvector`) is maintained in the ledger transaction for: Mission title/goals/objectives, node instruction text (never Artifact bodies), Human Task prompts, Deliverable summaries, Artifact manifests, Exemplar and Pattern descriptions.
- On every `revision.head_activated` and `mission.closed`, `search_index_publish` renders `MISSION.md` and submits it to Knowledge Services as a `mission_index_document` Artifact into a Mission-Control-owned vector store; `retrieval.search` with intent `implementation_lookup` answers "find me Missions like this one" and "which workflow structure did we use for X". `search_index.published` records the round trip.
- Faces: `POST /v1/searches`, `missionctl search "<q>" --scope missions --lifecycle running`, `mission_search`, the dashboard search bar (⌘K) and the Missions list filters.

### G.5 Schema context selection: Context Packs

An agent (or a human composing a Mission) needs the *right* subset of the specification in context: the schemas for the node behaviors it is using, one or two exemplars, the relevant patterns, and the subtree of the Mission it is working in — bounded to a token budget.

**D-G3 — `ContextPack`: a deterministic, digest-bound bundle assembled from MissionFS members under a token budget; the standard way agents and bindings load Mission Control context.**

```text
ContextPackRequest {
  members[]: { kind: schema | exemplar | pattern | subagent_profile | mission_subtree | artifact_summary | document, ref, priority: required | preferred | optional }
  token_budget                                  // required; the assembler counts with the binding's model tokenizer class
  render: markdown | json
}
ContextPack { context_pack_id, digest, token_budget, tokens_used, members[]: { kind, ref, rendered_bytes, truncated }, omitted[]: { ref, reason } }
```

- `context_pack_assemble` is a Deterministic Executor Kind, so packs are idempotent, receipted, and reproducible; a pack's `digest` is pinned in the Agent Executor Binding as `context_seed_refs[]` and recorded in the Continuation Checkpoint, so a transferred Session hydrates the same context.
- Faces: `POST /v1/context-packs`, `missionctl context select --schema program_node --exemplar goal_loop_basic --mission <id> --budget 12000`, `mission_context_select`, and the dashboard's "Compose" panel where the operator ticks schemas/exemplars and sees the token count before starting a Mission.
- The Skill teaches: call `mission_describe_system`, then `mission_context_select` with `required` schemas for the behaviors you intend to use, then draft — never paste the whole specification.

### G.6 Consumers, not only agents

Product tenants (learners, later customers) get the same surfaces with narrower grants: the Explorer over their own Missions, search within their tenant, Deliverables and Review Packets as the things they approve, and `mc://` links that open in the dashboard. Nothing in G is operator-only; scope decides visibility, and the tree shape is identical.

---
## Part H — The dashboard: personal control plane now, sellable service later

### H.1 Information architecture

| Area | Route | What it shows | Stream channels |
|---|---|---|---|
| Home | `/` | my Missions (running, blocked, dormant), inbox count, budgets by credit account, workers health | `missions`, `human_tasks`, `budgets`, `workers` |
| Missions | `/missions` | list with search and filters (G.4), create from exemplar / Context Pack composer | `missions` |
| Live Mission | `/missions/{mission_id}` and `/missions/{mission_id}/runs/{run_id}` | health bar; topology with three-field state per node (tree / outline); canonical event tail with `after_seq`; agent tail (non-canonical badge); inspector (activation, attempt, Harness Execution, native refs, Delivery Semantics preview); Command composer with Delivery Report; output rail (Artifacts, Deliverables); Journal viewer; Portals and Mission Graph breadcrumbs (M8) | `mission_events`, `native_tail` |
| Inbox | `/inbox` | Human Tasks by kind, Deliverable review cards with packet summary, the four verbs, delegate, ask, batch actions | `human_tasks`, `deliverables` |
| Deliverables | `/deliverables` | every Deliverable across Missions with review and promotion state, packet, receipts | `deliverables` |
| Explorer | `/explorer/{path…}` | MissionFS tree, document preview, `mc://` copy, open-in-inspector | `mission_events` (invalidation) |
| Catalog | `/catalog` | schemas, exemplars, patterns, Subagent Profiles, Executor Kinds, runtimes and their capability matrix (from `describe`) | — |
| Workers | `/workers` | Temporal task queues (pollers, backlog), adapter health per lane, outbox lag, gateway connections | `workers` |
| Budgets | `/budgets` | credit accounts, spend by Mission / lane / model, thresholds | `budgets` |
| Audit | `/audit` | Commands with Delivery Reports, denials, Human Task history, promotions with receipts, by actor | — |
| Specification | `/specification` | this document, rendered, with the decision register | — |
| Verification (existing) | `/verification/*` | retained as-is until the `verification_dispatch` executor and Live Mission absorb them | — |

Roles (`viewer < operator < mission_admin < platform_admin`) are presentation groupings that expand into scope sets (D-E2); the proxy already carries `role` in the session and gates mutations on `canControl`; it gains `canReview` (→ `mission.review`) and `canAdmin`.

### H.2 Client real-time architecture

```text
browser ── POST /api/dashboard/stream-token ──▶ Next server route (session cookie → MC API POST /v1/stream-tokens) ──▶ StreamToken (≤15 min)
browser ── WSS /v1/stream?token=… ─────────────▶ Stream Gateway (API container)
        ◀── StreamFrame{snapshot|event|native|gap|heartbeat|error|end}
browser ── REST via /api/mission-control/* (proxy, allowlist) ───────▶ Control API      (Commands, resolutions, drafts)
```

- `src/features/stream/stream-client.ts` — one `StreamClient` per tab: opens the socket, holds the subscription table with last `seq` per subscription, reconnects with backoff, renews the token before `exp`, exposes `subscribe(channel) → unsubscribe`. Deep module: the rest of the UI never sees frames.
- `src/features/stream/handlers.ts` — the handler table from D.6, each patching TanStack Query cache keys. Handlers are pure functions of `(queryClient, frame)`; tests assert cache state after a frame sequence.
- `src/features/stream/fixture-stream-adapter.ts` — replays a recorded `StreamFrame[]` fixture (from the exit-proof scripts) through the same handlers, for Playwright and for building the UI before the kernel exists (H.4).
- Queries keep a 30 s `refetchInterval` **only while the socket is down**; otherwise `staleTime: Infinity` and stream-driven patches. The current 3 s polling in `live-view.tsx` is removed.
- Presence and multi-operator awareness (who is looking at a Mission) ride on the same socket as a `presence` channel (later; not in M7).

**D-H1 — The dashboard consumes one WebSocket multiplex directly against the Stream Gateway with a short-lived token minted by its own server; REST stays behind the proxy allowlist.** Reversibility: moderate.

### H.3 Tenancy, auth, and the proxy

- Every ledger row is tenant-scoped; every token carries `tenant_id`; the API sets `app.tenant_id` per request so RLS is the second lock. The personal control plane is one tenant; a sellable service is many tenants with the same code and a billing layer over `credit_account` / `budget_ledger` (already per credit account).
- Dashboard operator login stays as implemented (token digest → HMAC session cookie, CSRF on mutations). The session maps to a Mission Control principal with scopes; the dashboard server mints per-request bearer tokens or holds one per-principal token — never a shared operator token for reviewers (D-F3 requires human `actor_ref`).
- The proxy allowlist stops being hand-written `if` chains: it is generated from `/v1/openapi.json` into `src/server/allowed-paths.generated.ts` with an explicit include list of route ids (`mission_control.missions.list`, …), so adding a page is adding a route id, and the boundary test still runs.
- Later: OAuth/OIDC login for external tenants; Vercel deployment unchanged.

**D-H2 — The proxy allowlist is generated from the API's OpenAPI document by route id; hand-written path chains are retired.** Reversibility: cheap.

### H.4 Getting the whole dashboard up as soon as possible

The kernel (M1–M6) is the long pole. The dashboard need not wait for it: every surface above is defined by contracts (`PeekView`, `MissionEvent`, `StreamFrame`, `HumanTask`, `Deliverable`, `MissionFsNode`) that M1 fixes and that fixture files can carry from day one.

| Phase | Ships | Backed by |
|---|---|---|
| **D0 — now (parallel with M1)** | shell, nav, Specification page, Catalog (schemas/exemplars from the contracts package), Explorer over a fixture MissionFS, Inbox and Deliverables UI, Live Mission layout with topology/event tail/inspector/composer bound to `FixtureStreamAdapter`, Playwright per page | contracts package + fixtures; no API |
| **D1 — with M3** | real `/v1/missions`, `/v1/fs`, `/v1/searches`, `/v1/human-tasks`, Stream Gateway over `mission_events` / `human_tasks` / `missions`; Commands parked in `accepted` | M3 API and event plane |
| **D2 — with M4** | Live Mission on `stage_graph_deterministic` and `stage_graph_mixed_gated`: three-field state, Proof Gate and Human Gate blockers, Deliverable Review Gate end-to-end on a deterministic Deliverable, Command composer for `pause/resume/cancel/retry/rerun` | M4 kernel |
| **D3 — with M5** | agent tail, Delivery Semantics preview and Delivery Reports on Cursor and Eve, usage and budgets, transcript Artifacts in the output rail | M5 lanes |
| **D4 — with M6/M7** | all four systems in topology, Journal viewer, Subagent sub-tree, Trace Digests visible, MCP resource parity, notifications | M6, M7 |

**D-H3 — The dashboard is built fixture-first from M1 contracts (phase D0) and bound to real endpoints milestone by milestone; the M7 "dashboard shell from M3 read models" is pulled forward to M1.** Reversibility: cheap.

### H.5 Toward generative UI (later, per the 2026-09-19 note)

The Live Mission surfaces are the substrate: tool calls, file edits, reasoning traces (from transcript Artifacts under grant), sources visited, sandbox filesystem state (from `.mission/` and workspace snapshots), spawned Subagents and Child Missions, and the Deliverable review. A generative-UI canvas binds to `mc://` refs and the same stream; it is a later feature specification, not part of M7's exit proof.

---

## Part I — Program impact (M0–M8)

No milestone is reordered and the critical path is unchanged. Additions are contracts in M1, read surfaces in M3, one synthesized gate in M4, adapter recording in M5, and control-plane features in M7/M8.

| Milestone | Amendment |
|---|---|
| M0 | Accept this proposal's decisions as dated amendments; `CONTEXT.md` gains Deliverable, Review Packet, Subagent Invocation, Subagent Profile, Subagent Report, Trace Digest, Context Pack, MissionFS, Stream Subscription; `05 §6.3` Human Task kinds lower-cased (D-A5); `09 §3` gains `subagent`, `deliverable`, `search_index` families; `09 §4` Native Event Store generalized (D-D2); `09 §9` gains `mission.review` (D-F3); architecture §10 task queue names (D-A6), §13 tool and CLI grammar (D-A8, D-A9). ADR 0006 "lower_snake wire contracts and the `mc://` reference grammar" (D-A1–D-A3). ADR 0007 "Deliverable as the unit of human review and promotion" (D-F1). |
| M1 | `F1.1` adds `SubagentProfile`, `DeliverableDeclaration`, `Deliverable`, `ReviewPacket`, `TraceDigest`, `ContextPack`, `StreamSubscription`, `StreamFrame`, `StreamToken`, `HarnessObservation`, `MissionFsNode`, `MissionSearchQuery/Result`, `UsageRecord`; `F1.2` adds `deliverable`, `review_packet`, `usage_record`, `native_ingestion_cursor`, `search_document`, `stream_token_revocation`, `activation.parent_activation_id`, generalized `native_event` with partitions, `outbox` with claims and the notify trigger; `F1.3` adds exemplars `business_report_weekly` and `subagent_helper_basic`; **new `F1.4 Naming law enforcement`** (fixture test, naming lint, route grammar test, `catalog/vocabulary.json`). Dashboard phase D0 starts. |
| M2 | `F2.1` compiles Deliverable declarations into the three synthesized nodes; `F2.3` uses the noun-verb CLI grammar. |
| M3 | `F3.2` becomes "Event plane, Native Event Store, and Stream Gateway (WebSocket multiplex, SSE, long-poll, `pg_notify` relay)"; **new `F3.5 MissionFS, search, and Context Packs`**; `F3.1` adds `/v1/deliverables`, `/v1/stream-tokens`, `/v1/searches`, `/v1/context-packs`, `/v1/fs`, OpenAPI at `/v1/openapi.json`; `F3.4` CLI reads include `fs`, `search`, `context`, `events tail --follow`. Dashboard phase D1. |
| M4 | **new `F4.6 Deliverable Review Gate`** (`review_packet_assemble`, `review` Human Gate, promotion node, `code_promote` on a fixture forge, `report_promote` stub); `F4.3` registry adds `trace_digest`, `context_pack_assemble`, `search_index_publish`. Exit proof adds: a deterministic Deliverable goes `awaiting_packet → open → reviewed` with `promote: true` and a receipt; a `review_reject` releases Remediation with the findings Artifact as input. Dashboard phase D2. |
| M5 | `F5.1` adds transcript sealing per Turn (D-D1), `usage_record`, hook-ingress tokens (D-E15), `HarnessObservation` ingestion; `F5.2`/`F5.3` record `framework_observed` Subagent Invocations; `F5.3` `mission-eve-agent` ships `agent/hooks/mission-control.ts` and the `.mission/` seed via `defineDynamic`. Dashboard phase D3. |
| M6 | unchanged; `F6.4` Goal Loop may declare `subagent_policy` (kernel-invoked Subagents still off until M8). |
| M7 | `F7.1` Live Mission per H.1 with the stream client; `F7.2` Inbox includes Deliverables and notifications (`dashboard_inbox`, `email`, `webhook`); `F7.4` MCP server per E.2.4 plus `mission_search`, `mission_fs_read`, `mission_context_select`, `mission_deliverable_review`, resources and `resources/subscribe`, elicitation; **new `F7.6 Trace Digests and agent-facing tracing`** (long-poll, `trace_push_policy`, `trace_digest`); `F7.5` CLI intervention in noun-verb form. Exit proof adds: an agent with the Skill watches a Mission by long-poll with zero gaps, receives one Trace Digest after a Human Task resolves, and a connected MCP host receives a `resources/updated` notification for `…/peek`. |
| M8 | `F8.4` adds kernel-invoked Subagent Invocation in minimal form (inline profiles, `provisionSubagent` on Eve and Cursor cloud, `subagent.*` events, denial governors); Mission Graph view in the Explorer. |
| After M8 | catalog-backed Subagent Profiles and search (M10); notification channels `slack`/`push`; presence; generative UI; Nexus to KS; multi-tenant billing. |

---

## Part J — Decisions for sign-off and review considerations

### J.1 Register

| # | Decision | Reversibility | Recommendation |
|---|---|---|---|
| D-A1 | Mission Control contracts are `lower_snake` on the wire and in the ledger; TS identifiers `camelCase`; KS translation only in `mission-knowledge` | moderate | accept |
| D-A2 | UUIDv7 ids named `<aggregate>_id` everywhere; no prefixed ids; native refs are `native_<thing>_ref` | moderate | accept |
| D-A3 | `mc://` reference grammar shared by HTTP, MCP, CLI, MissionFS, Artifacts; suffix law `_id/_ref/_digest/_at/_no/_kind/_policy` | moderate | accept |
| D-A4 | Vocabularies as `const` tuples; Postgres enum types named by vocabulary; fixture test | cheap | accept |
| D-A5 | Human Task kinds lower-cased (`approval`, …) | cheap | accept |
| D-A6 | Task queues role-prefixed (`harness-eve`, `executor-verification`) | cheap | accept |
| D-A7 | HTTP grammar: resources, Commands as a resource, no colon methods | moderate | accept |
| D-A8 | `missionctl <noun> <verb>` | cheap | accept |
| D-A9 | MCP tools `mission_<verb>` / `mission_<aggregate>_<verb>`, ≤ 28, `mc://` resources | cheap | accept |
| D-A10 | Verb lexicon for exported functions; prohibited words; ports/adapters/errors naming | cheap | accept |
| D-A11 | Directory = package suffix; package map | cheap | accept |
| D-C1 | Outbox + relay + `pg_notify`; no Supabase Realtime for Mission Events | cheap | accept |
| D-C2 | MCP `resources/subscribe` on peek/deliverables as push for connected hosts | cheap | accept |
| D-D1 | Transcript segments sealed as Artifacts per Turn; digests only in events | cheap | accept |
| D-D2 | Native Event Store generalized to all providers via `native_scope_ref` | cheap | accept |
| D-D3 | Mission Events ≤ 8 KiB, never bodies | cheap | accept |
| D-D4 | `native_event` monthly partitions, 30-day default; `mission_event` archived never deleted | cheap | accept |
| D-D5 | One Stream Gateway, three transports, one subscription contract, ledger-served replay | moderate | accept |
| D-D6 | Browser connects directly to the gateway with a 15-minute StreamToken | moderate | accept |
| D-E1 … D-E15 | Part E (faces, scopes, tool surface, long-poll tracing, hook ingress, Trace Digest, Subagent Invocation and its events, origins, defaults, human-only resolutions, per-Session hook grants) | cheap–moderate | accept |
| D-F1 | `Deliverable` aggregate and `deliverable` event family | moderate | accept |
| D-F2 | Keep `review_accept | review_reject`; `promote` flag; reject-with-findings → Remediation | cheap | accept |
| D-F3 | New authority scope `mission.review` for human reviewers | cheap | accept |
| D-G1 | MissionFS canonical tree, read-only, grant-scoped, five exposures | moderate | accept |
| D-G2 | Structured + full-text search here; semantic via KS over published index documents | cheap | accept |
| D-G3 | Context Packs as the standard bounded context loader | cheap | accept |
| D-H1 | Dashboard WebSocket multiplex direct to the gateway; REST via proxy | moderate | accept |
| D-H2 | Proxy allowlist generated from OpenAPI route ids | cheap | accept |
| D-H3 | Fixture-first dashboard from M1 (phase D0) | cheap | accept |

### J.2 Review considerations — where to push back

1. **`lower_snake` on the wire (D-A1)** is the single most consequential naming call. It diverges from Knowledge Services and from JavaScript habit. The payoff is spec-literal payloads; the cost is one translation seam and a codemod of today's verification-dispatch code. If you prefer camelCase, say so before M1; everything else in Part A survives either choice.
2. **`Deliverable` as an aggregate (D-F1)** adds a concept the suite did not have. The alternative is to treat Deliverables purely as Goal-level Output Projections plus a `review` Human Gate the author writes by hand. That is fewer concepts but every Mission author re-invents the packet and the promotion wiring; the compiler synthesis is what makes review a *middleware*.
3. **Subagent Invocation as a first-class behavior (D-E10)** versus "always a Child Mission". The proposal argues helpers must not be governance events. If you want every fan-out to be a Mission with a Portal, drop E.5 and keep only the `framework_observed` recording.
4. **Direct browser → gateway WebSocket (D-D6, D-H1)** means the API container is internet-facing for streams. The alternative is SSE through the Next proxy (works on Vercel fluid compute, single channel, reconnect-heavy). The proposal keeps SSE as a first-class transport so the fallback is one flag away.
5. **Semantic search delegated to KS (D-G2)** keeps invariant 9 but adds a publish step and a dependency for "find Missions like this". If KS is not ready, full-text alone ships and the semantic flag stays false.
6. **`mission.review` scope (D-F3)** separates steering from reviewing. If your control plane is single-operator for a long time, you may prefer to fold it into `mission.admin` and add the separation when a second human appears.
7. **Retention numbers** (30 days native, 8 KiB events, 4 KiB excerpts, 15-minute tokens, 25 s long-poll) are defaults chosen for a personal control plane; each is configuration, none is architecture.
8. **Package renames (D-A11)** touch every import path once. Doing it in M1 alongside the contracts rewrite is the cheapest moment; later is a chore.

### J.3 What happens after sign-off

For each accepted decision: dated amendment in the owning suite document and `CONTEXT.md`; ADRs 0006 and 0007; `M0_M8_PROGRAM.md` gains the feature specifications in Part I; `F1.1`/`F1.2`/`F1.4` are written next per `HANDOFF.md §15`; the dashboard's `/specification` page is regenerated from this file. Rejected or amended decisions are recorded in this document's header with the date and the reason, and the register is updated.
