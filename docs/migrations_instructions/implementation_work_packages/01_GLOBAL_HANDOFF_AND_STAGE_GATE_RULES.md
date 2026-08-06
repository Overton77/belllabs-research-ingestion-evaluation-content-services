# Global handoff and stage-gate rules

Status: mandatory execution protocol for every migration stage  
Applies to: all work packages in this directory and any formally accepted substages

## 1. Purpose

These rules make each large stage independently executable, reviewable, resumable, and safe to hand to another model or engineer. A stage handoff is an evidence-bearing contract, not a conversational summary.

Each stage document adds stage-specific entry conditions, exit evidence, and questions. If a stage rule conflicts with this global protocol, the stricter safety, authority, compatibility, or evidence requirement wins unless the owner explicitly decides otherwise.

Stages 3–6 must also read and satisfy [02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md](02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md) and [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md). Stage 6 must additionally complete [09A_STAGE_6_HETEROGENEOUS_STAGEGRAPH_COMPOSITION_PROOF.md](09A_STAGE_6_HETEROGENEOUS_STAGEGRAPH_COMPOSITION_PROOF.md).

The D-17–D-23 amendment and Stage 0–2 entry-critical evidence are closed by [05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md](05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md) in a separate task. When its compact `stage2_evidence/PRE_STAGE_3_ENTRY_HANDOFF.md` is `ACCEPTED`, the Stage 3 agent may treat that document as the accepted prior handoff and need not load the full Stage 0–2 evidence history unless it reports a contradiction.

## 2. Pre-stage clarification and interview are allowed

Before starting implementation, the agent may ask clarifying questions or conduct an interview with the owner. This permission is explicit in every stage.

An interview is recommended when the stage contains any of the following:

- an unsettled architecture decision;
- a public contract or accepted vocabulary change;
- a PostgreSQL/MongoDB authority or migration decision;
- platform purchase, region, deployment type, or ownership-path choice;
- data retention, PHI, trace, sandbox egress, or secret policy;
- preview/beta feature enablement;
- cutover, rollback, destructive reset, or decommission behavior;
- an acceptance threshold that cannot be derived from the existing baseline.

Use a short decision-oriented interview. For each question record:

```text
decision_id
question
why_now
options_and_tradeoffs
recommendation
owner_answer
effective_scope
follow_up_evidence
```

The agent may continue with non-blocking discovery while awaiting an answer. It must not implement a branch whose choice would materially change authority, compatibility, data safety, or rollout without an accepted answer or explicit assumption approval.

## 3. Stage status model

Use exactly these stage statuses in the handoff:

| Status | Meaning |
|---|---|
| `NOT_STARTED` | Entry material exists but no discovery has begun |
| `DISCOVERY` | Current code, decisions, versions, and evidence are being inspected |
| `IMPLEMENTING` | Accepted scope is being changed |
| `VERIFYING` | Required tests, inspections, drills, and evidence are being completed |
| `READY_FOR_REVIEW` | Implementer believes all mandatory exit criteria are met |
| `REWORK_REQUIRED` | Reviewer found missing or failed mandatory evidence |
| `BLOCKED_ON_DECISION` | A required owner/authority decision prevents safe progress |
| `BLOCKED_ON_EXTERNAL_STATE` | Entitlement, service, credential, environment, or third-party state prevents proof |
| `ACCEPTED` | Gate authority accepts the stage and next-stage entry |
| `ACCEPTED_WITH_DEFERRED_OPTIONAL_TRACKS` | Critical path is accepted; named optional tracks remain disabled/deferred |

Do not mark a stage `ACCEPTED` merely because tests pass. Required decision and operational evidence must also exist.

## 4. Incoming handoff requirements

Before editing, verify the incoming handoff contains:

1. stage and repository identity;
2. accepted scope and explicit non-goals;
3. accepted architecture/owner decisions and unresolved questions;
4. exact commit/worktree state or an explicit statement that the worktree is dirty;
5. files changed by the previous stage and files intentionally left unchanged;
6. dependency/version/configuration matrix when relevant;
7. schema and migration status;
8. commands and evidence for tests, lint, types, builds, deployments, evals, security, and recovery;
9. known failures, skips, risks, workarounds, and deferred optional tracks;
10. rollback/recovery posture;
11. next-stage entry criteria and recommended starting points.

If this information is absent, reconstruct it through read-only inspection before implementation. Do not assume the previous agent's narrative is complete.

## 5. Required stage artifacts

Every stage must maintain or create these artifacts. Their exact repository location may be selected in Stage 0, but the handoff must link them:

- `STAGE_HANDOFF.md` or an equivalently named stage-specific handoff;
- decision log with owner answers and assumptions;
- requirements-to-evidence matrix;
- implementation/change summary;
- exact verification command log and outcome summary;
- schema/migration manifest when applicable;
- feature flag and maturity manifest when applicable;
- known-risk/deferred-work register;
- rollback or recovery note proportional to the stage;
- links to traces, experiments, snapshots, build revisions, or deployment evidence when applicable.

Do not commit secrets, access tokens, raw private payloads, PHI, or unrestricted trace/sandbox output in these artifacts.

## 6. Requirements-to-evidence rule

At stage start, turn every deliverable and exit criterion into a matrix:

| Requirement ID | Requirement | Implementation location | Verification | Evidence | Status |
|---|---|---|---|---|---|
| `Sx-R01` | One atomic requirement | Exact files/components | Test, inspection, drill, or decision | Stable link/ref | pending/pass/fail/deferred |

Rules:

- Each row contains one testable proposition.
- Deterministic invariants use deterministic tests, not model scores.
- Security and tenant requirements include negative tests.
- Recovery requirements include failure injection or a documented operational drill.
- Evaluation requirements identify dataset/evaluator versions and thresholds.
- Optional capability rows may be deferred only with a disabled feature flag and fallback proof.
- A failing mandatory row prevents `READY_FOR_REVIEW`.

## 7. Change discipline

- Preserve unrelated user changes in a dirty worktree.
- Prefer existing project packages and ports; create a new domain package only for a distinct lifecycle/authority.
- Keep domain contracts strict, frozen, typed, and content-addressed where existing conventions require it.
- Keep vendor imports in runtime/integration/Agent Server packages, not pure domain reducers or compilers.
- Use forward-only database migrations and least-privilege runtime roles.
- Run database migrations in release jobs, never implicitly on every replica/cold start.
- Keep graph module import side-effect free: no network, DB, tracing startup, secret resolution, sandbox creation, or worker startup.
- Keep all I/O paths async; do not add `asyncio.run()` in application/runtime code, unbounded `gather`, request-owned fire-and-forget work, or blocking SDK calls on the event loop.
- Use typed identity builders; do not add ambiguous `run_id`, `checkpoint_id`, or `agent_id` fields.
- Update schemas, docs, tests, and runbooks in the same stage as the behavior they describe.

## 8. Verification hierarchy

Use proportionate evidence in this order:

1. strict contract parsing, canonical digest, and schema snapshot tests;
2. pure domain unit/property tests;
3. application and repository integration tests;
4. Agent Server local authenticated API/E2E tests with `langgraph dev`;
5. production-like container tests with `langgraph build`/`langgraph up`;
6. LangSmith datasets/experiments and trace inspection;
7. staging deployment tests and operational drills;
8. shadow/canary evidence.

Do not substitute a higher-level happy-path test for missing lower-level invariant tests.

Default project checks remain:

```powershell
uv run ruff check app tests
uv run mypy app
uv run pytest
```

Each stage may use a scoped subset during iteration, but the stage exit must run the full accepted suite or record an owner-approved exception with gate impact.

## 9. Handoff review roles

| Role | Responsibility |
|---|---|
| Implementer | Makes changes, produces evidence, recommends gate disposition |
| Gate reviewer | Re-runs/inspects critical evidence and checks scope/authority/compatibility |
| Owner/authority | Decides unsettled architecture, risk exceptions, destructive actions, and rollout |
| Next-stage implementer | Verifies entry conditions and refuses unsupported assumptions |

One agent may perform implementer and technical-review work in a small stage, but it cannot self-grant owner authority.

## 10. Mandatory outgoing handoff template

Use this structure verbatim or preserve every field in an equivalent machine-readable document:

```markdown
# Stage <N> handoff — <title>

Status: READY_FOR_REVIEW | REWORK_REQUIRED | BLOCKED_ON_DECISION | BLOCKED_ON_EXTERNAL_STATE
Prepared by:
Prepared at:
Repository/worktree:
Base revision:
Result revision or diff ref:

## Outcome
One concise statement of what is now true.

## Scope completed
- Requirement IDs and delivered outcomes.

## Explicitly not completed
- Non-goals, deferred optional tracks, and unaccepted work.

## Owner decisions and assumptions
| ID | Decision/assumption | Source/actor | Scope | Revisit trigger |

## Changes
| Area | Files/migrations/config | Behavioral effect |

## Contract and compatibility impact
- Schemas and digests changed.
- Backward/forward compatibility posture.
- State/checkpoint compatibility posture.
- Provider/runtime identity changes.

## Data and migration status
- Applied/not applied.
- Backfill/rollback status.
- RLS/grant verification.
- Destructive actions: none or exact approved action and recovery.

## Feature maturity and flags
| Capability | Version | stable/beta/preview | Flag/default | Fallback | Evidence |

## Verification evidence
| Command/drill/experiment | Environment | Result | Evidence ref |

## Failures, skips, and residual risks
| Item | Reason | Gate effect | Owner/follow-up |

## Security and data handling
- Tenant isolation, secrets, PHI/redaction, sandbox/network, Store/checkpoint findings.

## Operations and rollback
- How to disable/revert new behavior without destroying authority or evidence.

## Next-stage entry assessment
| Entry criterion | Met? | Evidence/blocker |

## Recommended first actions for next agent
1. Exact file/test/decision starting points.

## Gate recommendation
ACCEPT | ACCEPT_WITH_DEFERRED_OPTIONAL_TRACKS | REWORK | BLOCK
Reason:
```

## 11. Stage-specific handoff rules

Each stage mission contains its own outgoing handoff section. In addition to the global template:

- Stage 0 hands off accepted decisions, pinned qualification matrix, spike evidence, and enabled/disabled capability posture.
- Stage 1 hands off exact schemas, naming grammar, migrations, authority mapping, and transactional journal proof.
- Stage 2 hands off import-safe graph exports, `langgraph.json`, auth/resource filters, route collision evidence, and local server instructions.
- Stage 3 hands off runtime binding/attempt history, canonical lineage/query evidence, hierarchical resource-lease semantics, the shared operation-executor/outcome contract, interrupt/intervention protocols, recovery/reconciliation state machines, and checkpoint compatibility policy.
- Stage 4 hands off the exact per-stage requirement/execution-binding catalog, native/test adapter conformance, measured bounded concurrency, a parity matrix against the legacy StageGraph, and shadow-safe effect strategy; it must not hand off temporary Deep Agent/MCP mechanics.
- Stage 5 hands off the stable compiler and predicted/observed capability surfaces, reusable Deep Agent adapter, StageGraph composition proof, GoalDirected parity, exact harness/middleware/context manifests, sync-subagent construction, sandbox lifecycle, verifier evidence, and end-to-end lineage reports.
- Stage 6 hands off the required heterogeneous StageGraph proof, required/default-off async-subagent qualification, and a capability/maturity manifest for MCP, skills, optional QuickJS/dynamic delegation, Store, sandbox, and snapshots. Only explicitly optional tracks may be deferred.
- Stage 7 hands off public/MCP schemas, end-to-end operator path, trace taxonomy, dataset/evaluator registry, security findings, SLO thresholds, and production-like build evidence.
- Stage 8 hands off deployed endpoint/revision bindings, staging/canary evidence, rollback drill, in-flight routing, legacy drain ledger, and decommission approval.

## 12. Blocking and partial completion

If blocked:

1. complete all safe read-only discovery and independent work;
2. record the exact missing decision/external state and why assumptions are unsafe;
3. show which requirement rows are blocked and which are complete;
4. preserve a runnable worktree or clearly identify incomplete changes;
5. produce the outgoing handoff with `BLOCKED_ON_DECISION` or `BLOCKED_ON_EXTERNAL_STATE`;
6. do not weaken the gate to declare success.

If optional preview work fails qualification, disable and defer that track; do not block the stable critical path unless an accepted Workflow Implementation requires it.

## 13. Stage acceptance record

The gate reviewer/owner should append:

```text
gate_disposition: ACCEPTED | ACCEPTED_WITH_DEFERRED_OPTIONAL_TRACKS | REWORK_REQUIRED
accepted_by:
accepted_at:
accepted_requirement_matrix_digest:
deferred_tracks:
required_follow_ups:
next_stage_authorized: yes | no
```

This record is the incoming authority for the next stage. A chat acknowledgment should be copied into the durable handoff or decision log.
