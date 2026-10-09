---
type: Workflow Specification
title: Mission Control — Executors and Durable Controls
description: "Scope: the atomic Program Node behaviors — Agent Executor, Deterministic Executor — and the Durable Controls: Event Wait, Timer, Human Gate, Proof Gate"
tags: [mission-control, spec, workflow]
---
# Mission Control — Executors and Durable Controls

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** the atomic Program Node behaviors — Agent Executor, Deterministic Executor — and the Durable Controls: Event Wait, Timer, Human Gate, Proof Gate

Runtime behavior is qualified per pinned Deep Agents/Agent Server, Cursor SDK Cloud or frontier-provider profile. Historical vendor fact sheets are not the contract.

## 1. Purpose

Workflow systems compose; executors *do*; durable controls *wait*. This document fixes the identity model, contracts, completion rules, and state vocabularies of the atomic behaviors so every workflow system builds on the same substrate.

## 2. Execution identity hierarchy (accepted Round 2)

| Term | Meaning | Native examples |
|---|---|---|
| **Activation** | One instance of a Program Node in one Mission Run (a Stage activation, a Swarm Member, an Iteration, a Round, a Durable Control wait). | — |
| **Attempt** | Mission Control's one try at an activation. May span several Agent Sessions through Continuation. | — |
| **Agent Session** | The harness-native context window (see the canonical specification). | Deep Agents graph thread; Cursor Cloud session; bounded provider session mapping |
| **Session Turn** | One native unit of work inside a Session. | Deep Agents invocation; Cursor Cloud run; provider request |
| **Harness Execution** | The adapter record binding one Attempt to its Sessions and Turns, carrying all five execution dimensions and native references. | `agent_id + run_id`; `sessionId + turnId` |

Cardinality and isolation:

- Attempt 1 → N Agent Sessions (Continuation Transfer creates the next).
- Agent Session 1 → N Session Turns.
- An Agent Session serves only consecutive Attempts within **one** workflow-system activation lineage (for example the Iterations of one Goal Loop). It never serves two concurrent Attempts and never crosses into another activation's lineage. Producer/evaluator independence follows from this rule.
- Native identity is provider-neutral: `agent_runtime_kind` plus `native_session_ref`, scoped to the admitted Harness Execution.

Native identity mapping recorded on every Harness Execution:

| Runtime | Session ref | Turn ref | Correlation |
| --- | --- | --- | --- |
| `deep_agents` | Scoped graph/thread identity | Server invocation/run identity | Persisted launch key, generation and native observation cursor |
| `cursor_cloud` | Qualified SDK native session identity | Qualified cloud run identity | Stable launch/operation key and native event identity |
| `direct_model` | Logical session mapping where supported | Provider request identity | Effect key and usage/observation identity |

Exact vendor field names belong to the adapter's pinned conformance fixtures. Agent Server is required for async Deep Agents subordinates in both applications; native identity is subordinate to PostgreSQL admission.

## 3. Agent Executor (accepted Round 2)

### 3.1 Agent Executor Binding

```text
AgentExecutorBinding {
  harness: {
    agent_runtime_kind          // deep_agents | cursor_cloud | direct_model
    model
    model_access_kind
    compute_environment_kind
    credit_account_kind
  }
  instruction: {
    objective_refs[]
    operating_contract_ref      // versioned Operating Contract
    instruction_text
  }
  workspace: { repo_ref?, base_ref?, snapshot_ref? }
  output_contract               // names and schemas of declared outputs
  budget
  retry_policy
  session_policy                // fresh | brief_reuse | compact_and_transfer (default for long-running)
  context_health_policy_ref
  continuation_policy_ref
  missing_output_policy         // §3.4
  capability_binding_ref        // exact admitted asset versions, grants and digests
}
```

The five `harness` dimensions must be mutually consistent. Deep Agents model-provider credentials and accounting are distinct from Cursor Cloud account/workspace bindings; no implicit cross-account fallback is allowed.

### 3.2 Operating Contract

Every Agent Executor receives a versioned **Operating Contract**: Objectives and Success Criteria, workspace conduct, evidence rules, the Completion Candidate protocol, Context Health and Compaction rules, budget and Side-Effect limits, and escalation/abstention rules. The **Loop Operating Contract** is its Goal Loop form, adding the Journal protocol, Progress Review protocol, and Action Space. This mirrors Completion Contract / Stage Completion Contract.

### 3.3 Turn semantics and qualification

The common harness exposes prepare/start/reattach/send_turn/cancel_turn/observe/snapshot/usage/end_session. These are Mission Control interfaces, not asserted vendor SDK method names.

| Concern | Required behavior |
| --- | --- |
| Start/reattach | Persist stable launch identity; reconcile lost launch before any duplicate submission |
| Follow-up | Qualified safe boundary; retain exact submitted context and generation |
| Mid-turn injection | Advertise only actual native or cancel-and-replace semantics; unsupported rejects |
| Pause/resume | Stop releases, reconcile current work, seal/validate checkpoint and hydrate if necessary |
| Cancel | Reconcile actual child/effect/usage state; acknowledgement alone is not terminal settlement |
| Completion | Native completion supplies execution outcome and candidate; kernel computes acceptance |
| Compaction | Observable runtime fact; canonical continuation requires its own validated manifest |
| Usage | Attributed settled/estimated/unknown dimensions; unknown is never zero |

Deep Agents plus its frontier model route and required Agent Server async lane are first priority. Cursor SDK Cloud and direct-provider execution follow. Each profile reports native/emulated/unsupported/unqualified control capabilities. The system does not promise universal native pause, fork or steer. A frontier single-turn profile may reject session operations.

### 3.4 Completion of an atomic Agent Executor

An Attempt `succeeded` when the Session Turn ends natively **and** the declared outputs are registered as Artifacts inside a **Completion Candidate** — the same term Goal Loop uses; there is no separate "completion report". Acceptance is then the activation's Completion Contract decision.

```text
CompletionCandidate {
  activation_id, attempt_no
  outputs[]: { output_name, artifact_ref }
  evidence_refs[]
  criteria_mapping[]?           // required for Goal Loop; optional for other executors
  notes                          // concise; no chain-of-thought
}
```

If the Turn ends without a Completion Candidate, or with required outputs missing:

`missing_output_policy: follow_up_turn { max_turns } | not_accepted` — default `follow_up_turn { max_turns: 1 }`. The follow-up Turn runs in the same Session, asks only for the declared outputs, and then the activation ends `not_accepted(outputs_missing)` if still incomplete. A native `FINISHED` / `turn.completed` is never, by itself, `accepted`.

### 3.5 Attempt outcome and Failure Class

Per `00 §6.5`. Native mapping:

| Native | Attempt outcome | `failure_class` |
|---|---|---|
| Qualified Deep Agents, Cursor Cloud or provider terminal success | `succeeded` (subject to §3.4) | — |
| Qualified terminal provider/graph error after bounded native recovery | `failed` | `provider_error` |
| Cursor run `EXPIRED` | `failed` | `timeout` |
| `409 agent_busy`, `429`, `SESSION_TOKEN_LIMIT_REACHED` | `failed` | `capacity` |
| Observed native cancellation caused by an admitted Command | `cancelled` | `cancelled_by_command` |
| Adapter cannot reconnect and native state is unknown | `failed` | `infrastructure` |

Provider-native status never becomes Mission state directly; the adapter reports an observation and Mission Control applies these rules.

## 4. Deterministic Executor (accepted Round 2)

### 4.1 Executor Kind registry

Deterministic work is performed by a registered, versioned **Executor Kind** with typed input and output schemas. Required registered kinds:

| Executor Kind | Purpose |
|---|---|
| `verification_dispatch` | Invoke a Knowledge Services verification operation and return its typed disposition. |
| `artifact_register` | Register a produced file or object as an immutable Artifact with digest and provenance. |
| `git_snapshot` | Record branch, commit, and PR references for a workspace. |
| `schema_validate` | Validate an Artifact against a schema reference. |
| `test_run` | Run a declared test command in a workspace and capture the report. |
| `coalesce` | Combine typed member outputs into one Artifact (Parallel Swarm Convergence). |
| `agreement_check` | Compute agreement among member outputs against a rule (quorum Convergence). |
| `noop_echo` | Test executor. |

### 4.2 Intent, receipt, idempotency

Every invocation records an **Operation Intent** before acting and exactly one **Operation Receipt** after. Receipt outcome vocabulary: `applied | rejected | noop | partial`. `partial` is a failure requiring reconciliation before any Retry. Idempotency key = `(activation_id, attempt_no, executor_kind, input_digest)`. The Attempt outcome derives from the receipt (`applied | noop → succeeded`; `rejected → failed(policy_denied)` or `not_accepted` per kind; `partial → failed(infrastructure)` pending reconciliation); acceptance still runs through the Completion Contract.

The canonical DATABASE contract owns `operation_intent` / `operation_receipt`; existing implementations are reuse inputs.

## 5. Retry Policy (accepted Round 2)

```text
RetryPolicy {
  max_attempts
  backoff: { initial, multiplier, max }
  retry_on: FailureClass[]      // subset of infrastructure | timeout | provider_error | capacity
}
```

- A Retry never changes inputs, instruction, capabilities, or policy.
- An Attempt that declared `external_write_irreversible` requires a receipt check proving the effect did not land before a Retry starts.
- `policy_denied` and `provenance` are never retried.

## 6. Durable Controls (accepted Round 2)

Durable Controls are Program Nodes. They have identity, the shared Activation Lifecycle, receipts, and outcomes, and other nodes may depend on them. Gate Conditions on Stages are predicates over the facts these nodes record; the compiler may synthesize a Durable Control node from an inline gate declaration, but the Compiled Program always contains it explicitly.

### 6.1 Event Wait

```text
EventWait {
  event_type
  match_predicate               // over the typed event payload and recorded facts
  timeout?
  on_timeout: skip | fail | keep_waiting | escalate
  mode: once | rearm { max_firings, cooldown, expiry }
}
```

- Each firing writes an **Event Receipt** Artifact: event id, payload digest, `matched_at`.
- `once`: the node completes `accepted` on the first match; `skipped` on `on_timeout: skip`; `execution_failed` on `fail`.
- `rearm`: each firing creates a new activation of the dependent subprogram under the same Revision using the fan-out identity rule; the node completes when `max_firings` or `expiry` is reached (`accepted` if ≥1 firing, else per `on_timeout`). The candidate specification's `EVENT_TRIGGER` is `EventWait { mode: rearm }`.

### 6.2 Timer

`Timer { duration | until }`. Completes `accepted` when the instant passes. Recurrence is a Mission-level Run schedule, never a Program Node — a Run must be able to complete.

### 6.3 Human Gate and Human Task

A **Human Gate** raises exactly one **Human Task** and waits on its resolution.

```text
HumanTask {
  kind: APPROVAL | QUESTION | SELECTION | REVIEW | POLICY_OVERRIDE     // CREDENTIAL deferred to phase 4
  prompt, context_refs[], options[]?                                   // options for SELECTION
  assignee_policy: any_operator | role { name } | user { id }
  timeout?, on_timeout: keep_waiting | escalate | default_answer | stop
}
```

Human Task lifecycle: `open → claimed → resolved | expired | cancelled`.  
Resolution: `approved | denied | answered | selected | review_accept | review_reject | overridden`.  
The answer is an Artifact. Approval ≠ input ≠ intervention: an answer cannot smuggle a policy change.

Human Gate outcome: `approved | answered | selected | review_accept | overridden → accepted`; `denied | review_reject → not_accepted`; `expired` per `on_timeout` (`stop → stopped_by_policy`, `default_answer → accepted` with the default recorded).

### 6.4 Proof Gate

A Completion Contract gates a *producer's acceptance*. A **Proof Gate** gates a *consumer's release* on evidence recorded elsewhere.

```text
ProofGate {
  evidence_ref | verification_intent_ref
  required_disposition
  on_reject: skip | fail | wait_for_remediation | escalate
}
```

Resolution sources: a Knowledge Services verification result, a Deterministic Executor check, or an Evaluation Report. Completes `accepted` when the disposition matches; otherwise per `on_reject` (`skip → skipped`, `fail → not_accepted`, `wait_for_remediation → waiting`, `escalate → Human Task REVIEW`).

## 7. State vocabularies in this document

| Vocabulary | Values |
|---|---|
| Attempt outcome | `succeeded \| failed \| cancelled` |
| Failure Class | `infrastructure \| timeout \| provider_error \| capacity \| policy_denied \| provenance \| cancelled_by_command` |
| Operation Receipt outcome | `applied \| rejected \| noop \| partial` |
| Human Task lifecycle | `open \| claimed \| resolved \| expired \| cancelled` |
| Human Task resolution | `approved \| denied \| answered \| selected \| review_accept \| review_reject \| overridden` |
| Event Wait mode | `once \| rearm` |
| Side-Effect Class | `read_only \| workspace_write \| external_write_reversible \| external_write_irreversible \| spend` |

## 8. Questions under interview

Resolved in Round 3: Mission Event vocabulary (`09 §3`); Rubric schema is owned by Knowledge Services (`04 §13`).

No Cursor local lane is required. Deep Agents/Agent Server, Cursor SDK Cloud and frontier-profile support must be qualified against the pinned dependencies before admission.
