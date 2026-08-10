# Supplement — application and domain contract enhancement compass

Status: compact implementation aid; enhancement register, not a substitute for an active package  
Audience: implementation agents working sequentially from Stage 3 through Stage 8  
Last reconciled: 2026-08-08

## 1. Contract objective

Enhance the existing contracts so every admitted BellLabs run can be compiled exactly, executed
durably, recovered safely, and explained end to end without giving Temporal, Deep Agents,
LangGraph, LangSmith, or another provider product authority.

```text
published definitions
  -> EffectiveRunConfiguration
  -> RunPlanV4 + exact StageExecutionBindingV2 / OperationAssemblySpecV3
  -> admitted BellLabs run + frozen execution epoch
  -> Temporal root / family / operation bindings
  -> bounded runtime attempt + evidence
  -> validation + effect/usage settlement
  -> authoritative projection / terminal result
```

The rule is: **contracts carry meaning; application services enforce and persist it; adapters
translate it; Temporal durably executes it; agent runtimes perform bounded cognition.**

Use Q/D from `00A` as continuous contract consumers. Every stage records the delta in blueprint,
API/control, persistence, workflow, Activity/adapter, worker, agent/provider, evaluation, and
security planes and executes that delta. If either reference requires a special-case DTO,
repository, command, or worker bypass, repair the reusable contract instead.

## 2. Existing contract ownership

Search these owners before introducing a new contract:

| Concern | Existing owner and anchors |
|---|---|
| workflow types, blueprints, profiles, ERC | `app/domain/control_plane/contracts.py` |
| StageGraph/GoalDirected inputs, state, results and pure interpreters | `app/domain/orchestration/` |
| runtime definitions, exact assemblies, bindings and `RunPlanV4` | `app/domain/graph_runtime/definitions.py` |
| runtime submissions, receipts, interventions, projections and provider facts | `app/domain/graph_runtime/contracts.py` |
| stable semantic and technical identities | `app/domain/graph_runtime/identities.py` |
| decisions, resources, lineage and cancellation kernel | `app/domain/graph_runtime/kernel.py` |
| operation request/binding/result, agent/tool/workspace contracts | `app/domain/operation_execution/contracts.py` |
| effect claims, technical attempts and settlement | `app/domain/operation_execution/journal.py` |
| admitted lifecycle and budgets | `app/domain/run_control/` |
| linked independently governed runs | `app/domain/composition/` |
| use cases, ports, repositories and reconciliation | `app/application/` |
| persistence representations | `app/models/` and `app/migrations/` |
| external DTOs | `app/api/` |

`...Document` is a persistence representation, `...Projection` is derived query state, and an API
schema is a transport representation. None should silently become the authoritative domain type.

## 3. Contract planes

Keep enhancements within one of these planes and translate explicitly between them.

| Plane | Answers | Representative contracts |
|---|---|---|
| semantic/catalog | what may run and what it means | `WorkflowTypeDefinition`, blueprint, profile, implementation binding, ERC |
| compiled execution | exactly how this admitted run will execute | `StageCapabilityRequirement`, `StageExecutionBinding`, `OperationAssemblySpec`, resource envelope, `RunPlanV3` |
| durable control | what is authorized, active, waiting, settled or terminal | run projection, command/message receipt, operation journal, effect claim, evidence and settlement records |
| runtime transport | how a specific executor/provider is invoked and observed | Temporal workflow/activity payload, runtime binding, provider handle/callback, agent thread/run/checkpoint facts |
| public/query | what a caller may command or observe | versioned API DTO, durable product event, redacted result/projection |

Provider handles and Temporal Run IDs are facts in runtime bindings. They are not BellLabs run,
operation, attempt, or settlement identities.

## 4. Stage 3 — freeze the durable spine

Stage 3 should freeze or extend contracts for:

- the stable `BellLabsRunWorkflow` root input/state/result and its BellLabs run, epoch, continuation
  generation, technical segment, and child-binding identities;
- family-child and generic `OperationWorkflow` inputs/results, with exact binding and operation
  attempt identity rather than mutable aliases or runtime discovery;
- deterministic Workflow ID grammar, task-queue compatibility, retry/timeout/heartbeat/cancellation
  profiles, and Continue-As-New carry-forward state;
- authoritative command/message envelope, target, monotonic per-target sequence, dedupe identity,
  inbox/outbox/lease claim, receipt state, expiry, authorization, and redaction metadata;
- non-disruptive steering and the disruptive intervention saga, including generation changes,
  ambiguous-effect reconciliation, stale-target outcomes, and orphan-overlap facts;
- resource leases and hierarchical reservations; semantic, technical, data, effect and settlement
  lineage; active-child reconciliation and terminality evidence.

Prefer extending the current `graph_runtime`, `operation_execution`, `run_control`, and application
services. Create a new type only when no existing owner can express the invariant without conflating
semantic authority with runtime transport.

Stage 3 contract freeze must prove: serialization compatibility, deterministic replay, duplicate
delivery, worker loss, cancellation, recovery, settlement-before-readiness, redaction, and N/N+1
worker compatibility. It does not require all Stage 4–6 behaviors to be enabled.

Stage 3 also runs Q/D durable skeleton implementations through the frozen spine; fixture execution
is required to prove that it is usable, not merely type-complete.

## 5. Stage 4 — StageGraph execution decisions

Enhance only the contracts needed to translate the existing pure interpreter into durable actions:

- interpreter input projection and decisions such as start operation, await, release join, skip,
  cancel, fail, complete, or Continue-As-New;
- durable stage/operation binding, dependency and join evidence for `all`, `any`, and `minimum(k)`;
- incremental settlement facts so accepted early siblings may release downstream work while slow
  siblings remain independently durable;
- cycle/reuse/invalidation generations and capacity reservations;
- heterogeneous operation results using the same generic `OperationWorkflow` envelope.

Do not place dependency evaluation inside Deep Agents or duplicate operation journal/effect
contracts in the StageGraph workflow.

Blueprint Q is the first production consumer and owns the first exact local safe-point receipt
promotion from runtime-observed to model-visible/applied.

## 6. Stage 5 — bounded Deep Agents contracts

The Deep Agents enhancement is an exact **operation assembly**, not a new macro-workflow authority.
Freeze or extend:

- `AgentProfileDefinition` for reviewed role/maximum authority and `AgentHarnessDefinition` for the
  exact reusable loop construction;
- `OperationAssemblySpec` bindings for model, prompts, tools, MCP, middleware, skills, context,
  filesystem/workspace, sandbox, structured output, guardrails, verifier and resource ceilings;
- bounded context slices/manifests, operation-local checkpoint identity, session rollover and goal
  handoff references;
- typed synchronous subagent request/result surfaces with aggregate capacity accounting;
- a governed delegation request/receipt that asks BellLabs/Temporal to create an independently
  durable `OperationWorkflow` child or linked run;
- independently bound verifier request/result/action and evidence-driven revision/convergence facts.

Built-in Deep Agents subagents remain synchronous, operation-local and non-addressable. If work
needs its own durable wait, message target, cancellation, capacity, lineage, settlement or reusable
result, it crosses the BellLabs delegation contract and becomes a Temporal child. Model turns,
middleware and checkpoints never decide goal convergence or terminality.

Blueprint D is the production GoalDirected consumer; Q reruns to prove the completed harness did
not fork StageGraph contracts. Local tool HITL and disruptive-restart contracts qualify here.

## 7. Stage 6 — provider-neutral remote execution

Add exact variants and compatibility evidence rather than provider-specific domain forks:

- local and remote assembly variants with immutable deployment/model/tool/skill/sandbox bindings;
- start → bind → wait/reconcile contracts for provider handles, callbacks, polling, expiry,
  cancellation, duplicate completion and ambiguous completion;
- provider-qualified lineage and capability-readiness/drift projections;
- sandbox materialization/snapshot compatibility and reacquired resource/credential facts;
- outbound MCP/tool effect claims, idempotency identities and accepted evidence;
- remote post-model/pre-tool command-injection certification.

Provider success is evidence. BellLabs validation and settlement still determine accepted success.

Both Q and D compile heterogeneous implementations and promote only compatibility-key-specific
capabilities with deterministic and bounded-live evidence.

## 8. Stage 7 — governed public and operational contracts

Stabilize the public facade around application authority:

- versioned catalog/compile, run-control, command/message, evidence/artifact, projection/event and
  callback DTOs;
- a stable launch handle keyed by BellLabs identity, never only by Temporal or provider identity;
- redacted durable product events and result projections distinct from Temporal Queries;
- consistent correlation across tenant, BellLabs run, epoch, continuation segment, family,
  operation, semantic attempt, execution generation, Temporal workflow/run, agent thread/run,
  provider job, effect claim, artifact and trace;
- authorization, tenancy, data classification, secret-reference, audit and trace-redaction policy;
- health/readiness contracts for the five logical worker-pool classes and dependent providers.

The coordinator and MCP surfaces call the same application contracts as REST/streaming clients.
They do not receive a privileged provider or Temporal bypass.

## 9. Stage 8 — compatibility and cutover contracts

Complete contracts needed for deployment without changing product meaning:

- worker build/version compatibility and replay-manifest records;
- topology/queue binding and capability-readiness evidence;
- shadow comparison and canary decision records;
- rollback intent/outcome, backlog recovery and reconciliation status;
- drain ledger for every admitted legacy and Temporal execution;
- explicit repurpose/remove/retain disposition with evidence for superseded Agent Server macro paths.

Infrastructure configuration implements these decisions; it does not redefine workflow or domain
contracts.

## 10. Contract change protocol

For each enhancement:

1. Name the owning bounded context and invariant before choosing a filename.
2. Search for an existing noun; extend it or compose it before adding `V2`, `new`, `config`, or a
   provider-specific duplicate.
3. Classify the change as additive-compatible, versioned-compatible, or breaking/migrated.
4. Freeze stable semantic identities separately from technical execution identities.
5. Define validation and failure behavior, including unknown enum/version and stale-generation cases.
6. Add explicit serializers/translators at API, Temporal, persistence and provider boundaries.
7. Add persistence migration/backfill/dual-read rules when authoritative stored state changes.
8. Test domain invariants, serialization, repositories, API/adapter conformance, replay, recovery,
   redaction, lineage and negative authority paths in proportion to the change.
9. Record the change and evidence in the active package matrix and outgoing handoff.

Never claim exactly-once provider execution. Use stable claims and idempotency identities, reconcile
ambiguous attempts, and guarantee exactly-once **BellLabs settlement**.

## 11. Minimal enhancement record

Use this compact block in an implementation session or stage handoff:

```text
Contract:
Owner / current path:
Invariant added or clarified:
Change class: additive | versioned | breaking+migrated
Authoritative store:
Boundary translators affected:
Stable identities / idempotency key:
Failure and stale-generation behavior:
Security / redaction classification:
Migration / backfill / compatibility window:
Tests and evidence paths:
Deferred follow-up and owning stage:
```

## 12. Review invariants

A contract enhancement is not ready if any answer is unclear:

- Can a model, provider, Temporal workflow, trace or checkpoint now grant authority? It must not.
- Can the run recover with only durable BellLabs state, Temporal history and referenced artifacts?
- Is every independently managed unit represented as a Temporal child or linked BellLabs run?
- Are commands, provider completions, effects and settlements idempotent and reconcilable?
- Does accepted evidence reach the journal/settlement before readiness or convergence changes?
- Are histories, payloads, traces and events bounded and free of secrets, PHI and large content?
- Can an old worker replay old histories and can N/N+1 coexist where required?
- Is the public result explainable through stable identities and complete lineage?

## 13. Deeper references

- [`06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md`](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md)
- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md`](../../interview_and_research_result_documentation/BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md)
- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md`](../../interview_and_research_result_documentation/BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md)
- [`CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`](../../interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md)
- [`SUPPLEMENT_CODEBASE_ORGANIZATION.md`](SUPPLEMENT_CODEBASE_ORGANIZATION.md)
