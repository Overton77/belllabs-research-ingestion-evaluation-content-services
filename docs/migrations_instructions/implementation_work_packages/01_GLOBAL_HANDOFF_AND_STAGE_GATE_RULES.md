# Global handoff and stage-gate rules

Status: mandatory execution protocol for every migration package and accepted subpackage
Architecture: Temporal sole macro runtime; BellLabs semantic authority; bounded LangGraph/Deep Agents cognition
Applies to: all work packages in this directory

## 1. Purpose and precedence

A handoff is an evidence-bearing contract, not a narrative summary. It must let a new engineer or agent reproduce the claimed state, distinguish accepted facts from assumptions, and refuse unsafe downstream work.

Apply requirements in this order:

1. accepted owner decisions and explicit supersession in [00_MAIN_GOAL_AND_INDEX.md](00_MAIN_GOAL_AND_INDEX.md) and [02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md](02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md);
2. this global protocol;
3. package-specific requirements and direct-dependency handoffs;
4. earlier package text only where it is not superseded.

The stricter authority, safety, compatibility, or evidence requirement wins. A package may not silently revive the superseded Agent Server-primary architecture.

## 2. Global architecture guardrails

Every package gate must affirm all applicable guardrails:

- **No dual macro schedulers.** Temporal is the sole macro runtime for admitted production implementations. Agent Server/LangGraph macro graphs must be repurposed, qualification-only, or removed.
- **No provider bypass.** The BellLabs API and application ports are the only governed public command/query/result facade. Direct Temporal, Agent Server, sandbox, callback, or model-provider access cannot become an alternate product path.
- **No semantic-authority drift.** BellLabs stores/application services and pure StageGraph/GoalDirected interpreters retain lifecycle, readiness, convergence, budget, approval, effect, evidence, settlement, and terminality authority.
- **No unqualified intervention promise.** Pause, cancel, steering, message injection, fork, edited-state start, and disruptive intervention may be exposed only after their typed protocol, authorization, dedupe, quiescence, ambiguous-effect reconciliation, and recovery evidence passes.
- **No premature handoff.** A dependent package cannot begin merely because code exists or broad tests pass. Every direct package gate, evidence manifest, and acceptance record must exist.
- **No contract erosion.** Exact assemblies, capabilities, resource envelopes, canonical lineage, journals, effect claims, evidence, usage, and settlement identities must be preserved or deliberately versioned with compatibility evidence.
- **No secrets or PHI.** Never commit secrets, credentials, PHI, raw private payloads, unrestricted traces, sandbox dumps, or Temporal histories containing sensitive data.
- **No horizontal-only handoff.** Every package executes the applicable immutable Q/D reference
  blueprint increment from `00A`; schemas, mocks, imports, or unit tests alone do not prove an
  executable capability.
- **No demo fork.** Reference runs use the same application ports, compiler, stores, workers, and
  runtime path being prepared for production at that stage.
- **No live-web oracle.** Deterministic sanitized fixtures gate compatibility and replay. Bounded
  live canaries separately prove integrations and time-indexed research behavior.

Violation of a guardrail is `REWORK_REQUIRED`, not a deferrable documentation issue.

## 3. Package dependency and entry protocol

The normative dependency chain is:

```text
Stage 0 -> Stage 1 -> Stage 2 -> Pre-Stage 3 closure
                                  -> 06 + 06A contract sections
                                  -> 06-contract-frozen
                                  -> 06B Temporal foundation
                                  -> 06C communication/intervention
                                  -> aggregate Stage 3 acceptance
                                  -> Stage 4 Temporal StageGraph
                                  -> Stage 5 GoalDirected + Deep Agents
                                  -> Stage 6 advanced/remote candidate adapters
                                  -> 09A internal Stage 6 exit proof
                                  -> aggregate Stage 6 acceptance
                                  -> Stage 7 API/observability/security
                                  -> Stage 8 AWS deployment/cutover
```

After the contract-defining sections of `06` and `06A` are reviewed, versioned, internally
consistent, and recorded with their conformance evidence, the gate authority may record
`06-contract-frozen`. This gate authorizes `06B`; it is not acceptance of package `06` or Stage 3.
`06C` requires both `06-contract-frozen` and the passed `06B` implementation gate. Aggregate Stage 3
acceptance occurs only after the `06B` and `06C` gates pass and package `06` records the combined
handoff. Stage 4 depends on that aggregate acceptance.

Within Stage 6, `09A` depends on stable candidate adapters completed during `09`, not on accepted
Stage 6. Its evidence is the internal Stage 6 exit proof; only after `09A` passes may package `09`
record aggregate Stage 6 acceptance for Stage 7. The exact package table in `00` is normative if
shorthand here is ambiguous.

Before editing, verify:

1. repository, worktree, base revision, and dirty-state identity;
2. every direct dependency's durable acceptance record;
3. the accepted requirement-matrix digest and evidence-manifest path for each dependency;
4. explicit scope, non-goals, changed paths, migrations, versions, feature flags, failures, risks, and rollback posture;
5. applicable owner decisions and supersession;
6. exact current code/tests named by the package;
7. no contradiction between the handoff and current worktree.

Missing evidence must be reconstructed by read-only inspection or returned as an entry blocker. Do not substitute an earlier agent's chat summary.

## 4. Package status model

Use exactly:

| Status | Meaning |
|---|---|
| `NOT_STARTED` | Entry artifacts exist; discovery has not begun |
| `DISCOVERY` | Current code, decisions, versions, and evidence are being inspected |
| `IMPLEMENTING` | Accepted scope is changing |
| `VERIFYING` | Required tests, inspections, drills, and manifests are being completed |
| `READY_FOR_REVIEW` | Implementer believes every mandatory row passes |
| `REWORK_REQUIRED` | Mandatory evidence is missing or failed |
| `BLOCKED_ON_DECISION` | An owner/authority choice prevents safe work |
| `BLOCKED_ON_EXTERNAL_STATE` | Entitlement, credential, environment, or service state prevents proof |
| `ACCEPTED` | Gate authority accepts the package and its dependents' entry |
| `ACCEPTED_WITH_DEFERRED_OPTIONAL_TRACKS` | Critical path accepted; named optional features remain disabled |

Only the gate authority may append an accepted status. Passing tests is insufficient.

## 5. Required package artifacts and exact paths

At package start, select and record stable repository-relative paths for:

- package handoff;
- decision/supersession log;
- requirements-to-evidence matrix;
- evidence manifest;
- exact command/outcome log;
- implementation/change-path manifest;
- schema/migration manifest, if applicable;
- dependency/version/deployment-compatibility manifest;
- feature-maturity/flag manifest, if applicable;
- known-risk/deferred-work register;
- rollback/recovery runbook or note;
- trace, evaluation, replay-history, snapshot, build, and deployment references, if applicable.
- reference Q/D blueprint and implementation versions, fixture/live execution logs, comparison
  manifest, and capability-maturity delta.

The handoff must link the exact path to every artifact. “See tests,” “see logs,” glob-only references, uncommitted terminal scrollback, or links without environment/revision identity are not evidence manifests.

The evidence manifest must include:

```text
evidence_id
requirement_id
repository_relative_path_or_external_ref
revision_or_digest
environment
producer
created_at
redaction_class
reproduction_command_or_drill
result
```

External refs must be stable and access-controlled. Artifacts must contain references or sanitized fixtures, never secrets/PHI.

## 6. Requirements-to-evidence rule

Before substantive implementation, atomize every deliverable and exit criterion:

| Requirement ID | Atomic proposition | Implementation path | Verification | Evidence ID/path | Status |
|---|---|---|---|---|---|
| `Sx-R01` | One testable requirement | Exact file/component | Test/inspection/drill/decision | Stable ref | pending/pass/fail/deferred |

Rules:

- Deterministic invariants require deterministic tests, not model scores.
- Authority, tenant, provider-bypass, and intervention constraints require negative tests.
- Recovery requires failure injection or an accepted operational drill.
- Evaluation rows name dataset/evaluator versions and thresholds.
- Every changed contract maps to schema, migration/compatibility, producer, consumer, and test evidence.
- Optional features may defer only when disabled by default, with unsupported-capability behavior and fallback proof.
- Any failed or missing mandatory row prevents `READY_FOR_REVIEW`.
- Every capability row names the Q/D operation that exercises it, or explains why it is
  infrastructure-only and names the smallest executable fixture that proves it.
- Capability maturity attaches to an exact compatibility key; evidence for one adapter, provider,
  safe point, or implementation version cannot qualify another.

## 7. Cross-package contract preservation

Every affected package must map and test:

- BellLabs run, execution epoch, execution segment, family, semantic operation, attempt, and settlement identities;
- Temporal Workflow ID/Run ID, child, Activity ID/attempt, and worker/task-queue identity;
- agent thread/run/checkpoint, model, tool, MCP, sandbox, external-job, trace, and evaluation identities;
- exact Workflow Implementation, RunPlan, `StageCapabilityRequirement`, `StageExecutionBinding`, `OperationAssemblySpec`, prompts, schemas, policies, and compatibility digests;
- inbox, ledger, outbox, journal, claims, effects, evidence, usage, and typed result links.

Continue-As-New retains the BellLabs run and epoch and starts a new technical segment. A fork creates a new BellLabs run at epoch `1` and carries immutable parent/snapshot lineage. Temporal Reset is operational repair, never the product fork API.

A child or provider return is not sufficient settlement evidence. The parent must re-read authoritative records, validate identities/digests, and settle through BellLabs application services.

## 8. Communication and intervention gate

Any package touching communication must prove:

1. authoritative per-attempt inbox/ledger/outbox persistence and deduplication;
2. typed Signal/Update/callback/provider facts with authorization and stable command IDs;
3. product durable events emitted from authoritative BellLabs state, not inferred from Temporal history or traces;
4. no peer message affects StageGraph readiness or GoalDirected convergence before accepted settlement;
5. handler serialization/guards and quiescence before Continue-As-New;
6. disruptive-saga behavior across pause/cancel, reconciliation, mutation, rebind/resume, and failure compensation;
7. built-in synchronous subagents remain operation-local;
8. independent-lifecycle delegation uses custom Temporal workflows/children;
9. provider async is a subordinate adapter;
10. remote lifecycle follows start-bind-wait/reconcile, with optional callback completion converging on the same journal.

Qualification is incremental. Stage 3 certifies persisted command transport, deduplication,
target/version checks, durable waits, and runtime observation. Stage 4 certifies exact
post-model/pre-tool injection for its first local adapter; Stage 5 completes local disruptive
restart and reusable agent HITL/steering qualification; Stage 6 separately certifies selected
remote LangSmith placements. Until the applicable certification passes, the API reports the exact
capability as unsupported rather than promising best-effort intervention.

## 9. Change discipline

- Preserve unrelated user changes.
- Use existing pure domain services, interpreters, contracts, and ports before adding abstractions.
- Keep vendor imports out of pure domain compilers/reducers/interpreters.
- Keep workflow code deterministic and import-safe; no network, database, secret, tracing, sandbox, or worker startup at import/replay time.
- Put I/O in activities/application ports; use async I/O and bounded concurrency.
- Use forward-only migrations and least-privilege roles; never migrate implicitly on every worker start.
- Use typed identity builders; never add ambiguous `run_id`, `agent_id`, or `checkpoint_id`.
- Update schemas, tests, docs, runbooks, and compatibility manifests with behavior.
- A model/provider cannot select undeclared capabilities, queues, credentials, or fallback paths.
- Unfinished Agent Server macro graphs may be repurposed or removed only with explicit path inventory and preserved test/evidence disposition; they may not be production fallback work.

## 10. Verification hierarchy

Use proportionate evidence:

1. strict schema parsing, canonical digest, snapshot, and compatibility tests;
2. pure interpreter/domain unit and property tests;
3. repository/application/transaction integration tests;
4. self-hosted Temporal workflow replay, child/activity, message, restart, and Continue-As-New tests;
5. bounded local LangGraph/Deep Agents operation tests;
6. remote LangSmith deployment/sandbox/evaluation qualification;
7. BellLabs API authenticated E2E and negative-bypass tests;
8. production-like AWS staging, shadow/canary, rollback, and failure drills.
9. Q/D reference vertical comparison at the package's declared maturity, with deterministic fixture
   evidence and a bounded live canary or explicit skip reason.

Do not replace missing lower-level invariants with a happy-path E2E.

Default project checks remain:

```powershell
uv run ruff check app tests
uv run mypy app
uv run pytest
```

The package may iterate with scoped checks, but exit requires the accepted full suite or an owner-approved exception with explicit gate effect.

## 11. Package-specific mandatory gates

- **Stages 0–2 reconciliation:** retain decision history, identify still-valid evidence, and explicitly supersede Agent Server-primary assumptions.
- **Stage 3 / `06-contract-frozen`:** reviewed and versioned `06` root/family/operation, identity, command/fact, continuity, intervention, and recovery contract sections plus `06A` exact assembly, hierarchical capacity, canonical lineage, journal, effect, and settlement conformance. This gate authorizes implementation but does not accept Stage 3.
- **Stage 3 / 06B:** distinct `BellLabsRunWorkflow`, family children, generic `OperationWorkflow`, self-host Temporal replay/restart, five pool classes, independent-operation progress, and same-epoch/new-segment Continue-As-New.
- **Stage 3 / 06C:** inbox/ledger/outbox, typed command transport, durable wait/resume,
  settlement-before-readiness, dedupe, stale-target rejection, runtime observation, and explicit
  unsupported posture for adapter-level steering not yet qualified.
- **Stage 3 aggregate:** accepted only after `06-contract-frozen`, 06B, and 06C all pass and the combined `06` handoff is accepted.
- **Stage 4:** Q small heterogeneous Temporal-native StageGraph first; `all`, `any`, and
  `minimum(k)` incremental scheduling; slow-sibling policy; first local safe-point steering proof;
  no direct-gather frontier barrier.
- **Stage 5:** D GoalDirected research second; reusable Deep Agents operation harness; independent
  verifier; subgoals/revisions; context rollover; tool HITL and disruptive restart; Q remains green;
  no private macro scheduler.
- **Stage 6:** advanced capability, remote LangSmith start-bind-wait/reconcile, remote injection, sandbox, optional async completion, and stable candidate adapters, followed by the internal `09A` heterogeneous and hours-long injected-failure exit proof.
- **Stage 7:** modular BellLabs API as sole facade, coordinator integration, durable product events, auth/tenant/redaction, observability/evaluation, and provider-bypass negative tests.
- **Stage 8:** owner selection and proof of final AWS self-host topology, five isolated pool classes, hours-long topology failure/recovery, N/N+1 replay, canary, rollback, cutover, and drain.

No later package can waive an earlier mandatory gate by duplicating its implementation.

## 12. Mandatory outgoing handoff

Use this structure or a machine-readable equivalent preserving every field:

```markdown
# <Package> handoff — <title>

Status: READY_FOR_REVIEW | REWORK_REQUIRED | BLOCKED_ON_DECISION | BLOCKED_ON_EXTERNAL_STATE
Prepared by:
Prepared at:
Repository/worktree:
Base revision:
Result revision or diff ref:
Requirement matrix path and digest:
Evidence manifest path and digest:

## Outcome
## Scope completed
## Explicitly not completed
## Direct-dependency acceptance records
## Owner decisions, amendments, and supersession
## Exact changed paths
## Contract and compatibility impact
## Data and migration status
## Feature maturity and flags
## Verification evidence
## Failures, skips, and residual risks
## Security and data handling
## Runtime authority and bypass audit
## Operations, recovery, and rollback
## Reference blueprint increment
Q/D blueprint and implementation versions:
Deterministic commands and evidence:
Bounded live commands/evidence or skip reasons:
API/contracts/stores/workflows/Activities/workers/adapters traversed:
Capability promotions and unsupported surfaces:
Comparison with preceding accepted increment:
## Next-package entry assessment
## Recommended first actions
## Gate recommendation
ACCEPT | ACCEPT_WITH_DEFERRED_OPTIONAL_TRACKS | REWORK | BLOCK
Reason:
```

“Exact changed paths” lists every changed/added/deleted/renamed file and generated artifact. Security explicitly states that secret/PHI scans were run or why they were not applicable.

## 13. Review, blocking, and acceptance

The implementer produces evidence and recommends a disposition. The gate reviewer reproduces critical evidence and audits scope, authority, compatibility, and paths. The owner decides architecture exceptions, destructive actions, risk exceptions, topology, and rollout. No agent self-grants owner acceptance.

If blocked:

1. complete safe independent discovery;
2. identify the exact missing decision/external state;
3. mark affected requirement rows;
4. preserve a runnable worktree or enumerate incomplete paths;
5. produce a blocked handoff;
6. never weaken the gate.

The acceptance record is:

```text
gate_disposition: ACCEPTED | ACCEPTED_WITH_DEFERRED_OPTIONAL_TRACKS | REWORK_REQUIRED
accepted_by:
accepted_at:
accepted_requirement_matrix_digest:
accepted_evidence_manifest_digest:
accepted_direct_dependency_records:
deferred_tracks_and_disabled_flags:
required_follow_ups:
next_package_authorized: yes | no
```

An acceptance chat message is not durable authority until copied into the package handoff or decision log.
