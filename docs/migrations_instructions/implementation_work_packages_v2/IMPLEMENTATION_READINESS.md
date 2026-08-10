# Control-plane implementation readiness contract

Status: canonical implementation companion  
Applies to: every work package in `implementation_work_packages_v2/`  
Architecture authority: `ADR-0003` and the canonical `SPEC-CP-*` / `SPEC-BP-*` specifications

## 1. Implementation posture

BellLabs is pre-production. These packages implement the accepted control plane as a replacement,
not as a compatibility migration for a live product.

The following rules are therefore binding:

1. New canonical contracts receive new explicit schema identities and storage shapes.
2. No dual-write, dual-read, compatibility worker, rollback worker, schema translation layer, or
   old-execution drain is required for the OpenAI Agents SDK, Agent Server macro runtime, or the
   current prototype family workflows.
3. Existing code may be copied or refactored only when it already expresses an accepted invariant.
   Passing legacy tests is evidence about reusable behavior, not a requirement to preserve an old
   public or persistence contract.
4. Superseded runtime code is deleted from active composition, dependencies, worker registration,
   API launch paths, and required tests as soon as its replacement package supplies the same
   accepted responsibility.
5. Historical Markdown, experiments, and evidence may remain clearly marked non-normative. They
   must not be imported, registered, launched, or referenced by an active implementation path.
6. Rollback means reverting the new package before production adoption. It does not mean keeping
   the old runtime executable beside the replacement.
7. Existing local development data may be discarded and recreated by the new migrations. No
   production-data backfill or old-workflow replay requirement exists.

In these work packages, a **replacement inventory** is the small checklist that identifies the
current owner of a responsibility, its new owner, and the point at which the old executable path is
deleted. It is not a compatibility plan.

## 2. Canonical runtime and authority

```text
compile exact definitions -> admit run transactionally -> BellLabsRunWorkflow
    -> exactly one family workflow -> OperationWorkflow children
    -> native or Deep Agent adapter -> persisted facts/results
    -> family interpreter proposal -> lifecycle reducer decision
```

| Concern | Sole owner |
|---|---|
| Immutable definitions, compiled ERCs, detailed execution documents | MongoDB/Beanie application repositories |
| Admission, lifecycle, commands, budgets, effects, evidence, settlement, terminality | PostgreSQL application services |
| Durable scheduling, timers, child execution, cancellation delivery, Continue-As-New | Temporal |
| Stage readiness, joins, cycles, invalidation, reuse, completion proposal | `StageGraphInterpreter` |
| Goal revisions, verifier applicability, convergence, stopping proposal | `GoalDirectedInterpreter` |
| Bounded cognition and operation-local planning | Deep Agents through `OperationExecutor` |
| Large immutable payloads, artifacts, workspace snapshots | Object storage |
| Checkpoints and traces | LangGraph/LangSmith as subordinate evidence |

Temporal status, child closure, provider state, model text, a checkpoint, and a trace are never
authoritative BellLabs facts until an application service accepts them.

## 3. Frozen implementation paths

The v2 packages authorize the following target paths. Implementers may split a named module when
size demands it, but must not create a second semantic owner.

```text
app/domain/control_plane/
  contracts.py                 # definition refs, ERC and immutable authoring contracts
  compiler.py                  # pure deterministic compilation
app/domain/run_control/
  contracts.py                 # lifecycle, budget, effect and event contracts
  reducer.py                   # sole lifecycle transition authority
app/domain/orchestration/
  contracts.py                 # family-neutral and family decision contracts
  interpreter.py               # StageGraphInterpreter
  goal_directed.py             # GoalDirectedInterpreter
app/domain/operation_execution/
  contracts.py                 # OperationWorkflow request/outcome and runtime-neutral binding
app/application/
  control_plane.py             # definition publication and ERC compilation service
  run_control.py               # admission and lifecycle command service
  operation_execution.py       # provider-neutral operation execution service
  orchestration.py             # family launch preparation; no Temporal implementation
app/temporal/workflows/
  belllabs_run.py              # stable root
  operation.py                 # generic semantic operation child
  stagegraph.py                # StageGraph family mechanics
  goal_directed.py             # GoalDirected family mechanics
  linked_run.py                # observation/coordination only; child is its own root
app/temporal/activities/
  control_plane.py             # idempotent application-service calls
  operation.py                 # operation preparation, execution and reconciliation
app/temporal/registration/
  workflows.py                 # one workflow registry
  activities.py                # activity registry by worker pool
  task_queues.py               # stable logical queue names
app/integrations/agents/deep_agents/
  adapter.py                   # the only production `create_deep_agent` composition root
  materializer.py              # exact binding -> framework arguments
tests/unit/domain/
tests/contract/
tests/integration/temporal/
tests/replay/
tests/acceptance/control_plane/
```

Old flat modules may be edited in place during one package, but package acceptance requires new
imports and worker registration to use these owners. Do not add `v2`, `new`, or provider-specific
domain packages.

## 3.1 Canonical contract ownership matrix

| Contract | Domain/code owner | Durable persistence owner | Primary executable seam | Required evidence |
|---|---|---|---|---|
| `CON-CP-DEFINITION-REF-V1` | `domain/control_plane` | MongoDB | definition publication/resolution | strict schema, immutability, digest |
| `CON-CP-ERC-V1` | `domain/control_plane` | MongoDB; digest/ref in PostgreSQL | pure compiler and admission verifier | byte stability, alias independence |
| `CON-CP-RUN-REQUEST-V1` | `domain/run_control` | PostgreSQL | run admission API/service | atomic/idempotent admission |
| `CON-CP-LIFECYCLE-V1` | `domain/run_control` | PostgreSQL | lifecycle reducer | transition/CAS/terminality tables |
| `CON-CP-BUDGET-LEDGER-V1` | `domain/run_control` | PostgreSQL | budget service | concurrency and settlement |
| `CON-CP-DOMAIN-EVENT-V1` | `domain/run_control` | PostgreSQL outbox | relay/consumer ports | redelivery, gaps, ordering |
| `CON-CP-TEMPORAL-IDENTITY-V1` | `domain/orchestration` + `domain/operation_execution` | PostgreSQL runtime bindings; compact Temporal state | root/family/operation workflows | duplicate start, retry/generation lineage |
| `CON-CP-WORKFLOW-MESSAGE-V1` | `domain/operation_execution` | PostgreSQL inbox/ledger/outbox | message application service + Temporal transport | sequence and receipt progression |
| `CON-CP-LINKED-RUN-V1` | `domain/composition` | PostgreSQL | linked-run admission/result service | independent admission and late results |
| `CON-CP-CONTINUATION-V1` | `domain/orchestration` | PostgreSQL refs; Mongo/object snapshot metadata | Continue-As-New and fork services | active-child continuation and fork isolation |
| `CON-CP-DEEP-AGENT-PROFILE-V1` | `domain/operation_execution` | MongoDB | definition compiler/materializer | strict schema and composition |
| `CON-CP-DEEP-AGENT-PLACEMENT-V1` | `domain/operation_execution` | MongoDB | placement selector/adapter | compatibility and no fallback |
| `CON-CP-DEEP-AGENT-BINDING-V1` | `domain/operation_execution` | MongoDB; authority refs in PostgreSQL | operation preparation/materializer | attachment and runtime drift |
| `CON-CP-ASYNC-SUBAGENT-V1` | `domain/operation_execution` | MongoDB detail + PostgreSQL authority | parent-child service/adapter | lifecycle, messaging, admission |
| `CON-CP-WORKSPACE-MANIFEST-V1` | `domain/operation_execution` | MongoDB + object storage | workspace gateway | ownership and mount isolation |
| `CON-CP-ARTIFACT-PROMOTION-V1` | `domain/operation_execution` | PostgreSQL decision + Mongo metadata + object payload | artifact promotion service | retry/conflict/visibility |
| `CON-CP-SNAPSHOT-V1` | `domain/operation_execution` | MongoDB metadata + object payload | snapshot service | clone, tamper, reauthorization |
| `CON-BP-STAGEGRAPH-V1` | `domain/control_plane` | MongoDB | compiler + StageGraph interpreter | publication and join truth tables |
| `CON-BP-STAGE-DECISION-V1` | `domain/orchestration` | PostgreSQL accepted decisions; Mongo detail | interpreter/application decision service | determinism, fairness, invalidation |
| `CON-BP-GOAL-DIRECTED-V1` | `domain/control_plane` + `domain/orchestration` | MongoDB definitions/revisions + PostgreSQL accepted decisions | compiler + GoalDirected interpreter | envelope, revisions, convergence |
| `CON-BP-GOAL-HANDOFF-V1` | `domain/orchestration` | MongoDB metadata; object payload when large | handoff/context service | empty-session resume |
| `CON-BP-GOAL-VERIFICATION-V1` | `domain/orchestration` | MongoDB detail + PostgreSQL accepted decision | independent verifier operation | independence, applicability, stale rejection |

API DTOs and Temporal payloads adapt these contracts; neither becomes their semantic owner.

## 4. Temporal hierarchy and identity contract

The hierarchy is exact:

```text
BellLabsRunWorkflow                       workflow id: belllabs-run/{run_id}
  StageGraphWorkflow | GoalDirectedWorkflow
                                           child id: family/{run_id}/{epoch}
    OperationWorkflow                     child id: operation/{semantic_attempt_id}
      activities / provider tasks         effect and generation identities remain stable
```

- One admitted BellLabs run maps to one stable root Workflow ID.
- The root has exactly one family child selected from the ERC.
- A linked run is independently compiled and admitted, then starts its own root.
- Continue-As-New advances `technical_segment`; it does not increment `execution_epoch`.
- Disruptive recovery increments `execution_generation` inside the same semantic attempt.
- A semantic retry creates a new semantic attempt.
- A fork creates a new BellLabs run at epoch 1.
- Workflow code contains no database, network, model, MCP, sandbox, secret, object-store, or clock
  I/O other than Temporal deterministic APIs.

The root input contains only stable identities, schema versions, digests, immutable references,
the selected family discriminator, and compact accepted continuity state. It does not contain raw
corpora, secrets, mutable aliases, or an application database projection.

## 5. Shared run lifecycle

The only aggregate phases are:

```text
pending -> active -> waiting -> active
                  -> paused  -> active
active|waiting|paused -> cancelling -> terminal
active|waiting|paused ----------------> terminal
```

`waiting` is legal only when no currently admissible work can progress and at least one declared
condition can permit future progress. A scoped stage/operation wait does not make the run waiting
while unrelated work remains admissible. `paused` always requires an accepted resume command.

Terminal outcomes are `completed`, `partially_completed`, `failed`, and `cancelled`. They are
immutable. Family interpreters emit proposals; only `app/domain/run_control/reducer.py` assigns an
outcome after validating current versions, obligations, cancellation, effects, reservations, and
pending dependencies.

Every state mutation uses this sequence:

```text
typed command or observed fact
  -> authorization + idempotency fingerprint
  -> expected-version reducer decision
  -> transition + projection + ledgers + command result + outbox in one transaction
```

## 6. StageGraph executable contract

### 6.1 Authored blueprint

`CON-BP-STAGEGRAPH-V1` must be represented by versioned strict models containing:

- exact blueprint identity/revision/digest;
- unique `StageDefinition` records;
- typed `StageDependency {stage_id, dependency_class}` records;
- `JoinPolicy {mode: all|any|minimum, minimum?: int}`;
- declared input slots, output slots, obligation refs, and allowed operation variants;
- typed failure, skip, degradation, wait, cancellation, and late-result policies;
- exact stage-cycle and workflow-cycle policies and limits;
- a scheduler policy with fairness algorithm/version and `max_parallel_stages`;
- linked-run slots and completion/obligation policy.

Parallel `depends_on` and `dependency_classes` collections are not canonical. Every edge is one
typed dependency record. `concurrency_slots` is replaced by `stage_slot_weight`; operation, model,
tool, MCP, subagent, provider and resumption capacity belong to the exact operation resource
envelope, not to an ambiguous stage field.

### 6.2 Dependency and join decisions

An accepted dependency result is one whose applicability matches the current workflow/stage cycle
and whose evidence/result admission decision is current.

| Dependency class | Accepted success | Admitted degradation | Failed/skipped/unavailable | Blocks completion |
|---|---:|---:|---:|---:|
| `required` | satisfies | no | yes | yes |
| `degradable` | satisfies | satisfies with degradation | until policy admits degradation | until disposition |
| `optional` | satisfies | satisfies | does not block after disposition | no |
| `advisory` | may enrich | may enrich | never blocks | no |

Join calculation uses only dependencies with an accepted current disposition:

- `all`: every dependency has a satisfying disposition under its class;
- `any`: the first satisfying dependency releases the stage;
- `minimum(k)`: the kth satisfying dependency releases the stage;
- impossibility is detected when remaining undecided dependencies cannot satisfy the join;
- a late sibling is admitted, rejected, or quarantined by its frozen policy and never revokes an
  already accepted downstream result implicitly.

### 6.3 Deterministic frontier and fairness

The initial fairness algorithm is **weighted round-robin by authored fairness group**, with stable
ordering by `(priority, group_id, stage_id, mapped_instance_id)`. The accepted projection carries
the per-group cursor. One decision considers a stable sorted candidate set and admits a stage only
when every declared capacity and budget dimension can be reserved. A skipped candidate does not
consume the cursor. This algorithm is versioned in the blueprint/ERC.

The interpreter returns proposals only. Reservation and dispatch occur after the application
authority accepts the proposed frontier. After each accepted child result, the workflow applies
simultaneously available results in stable semantic-identity order, reruns the interpreter, and
launches newly admitted children without waiting for unrelated siblings.

### 6.4 Stage projection and decisions

Stage projection states are:

```text
unavailable | blocked | ready | reserved | running | waiting | paused |
completed | degraded | failed | cancelled | skipped | invalidated
```

`CON-BP-STAGE-DECISION-V1` is a discriminated union of:

```text
frontier_proposal | stage_result_proposal | wait_proposal | stage_cycle_proposal |
workflow_cycle_proposal | invalidation_proposal | reuse_proposal |
skip_proposal | degradation_proposal | completion_proposal | failure_proposal
```

Every decision carries decision ID, interpreter/schema version, blueprint and accepted-projection
digests, semantic identities, reason code, applicability/stale frontier, required reservations,
input/evidence refs, and proposed projection changes.

Completion requires every required obligation to have accepted current evidence and every required
dependency, child, reservation, effect, cancellation, and late-result liability to have a declared
settled disposition.

## 7. GoalDirected executable contract

### 7.1 Objective envelope and revisions

`CON-BP-GOAL-DIRECTED-V1` freezes:

- objective and acceptance contract;
- admitted input classes and exact input manifest ref;
- authority/capability ceiling and prohibited work;
- multidimensional budget and concurrency ceilings;
- required outputs and obligation matrix;
- allowed operation and async-subgoal classes;
- linked-run slots;
- verifier binding and rubric;
- session, handoff, rollover, workspace and snapshot policies;
- iteration, no-progress, blocker, rollover and stopping limits.

A `GoalRevision` contains revision identity/number, parent, canonical digest, tactical changes,
evidence, unmet obligations, proposer, deciding authority, applicability, and the unchanged envelope
digest. A revision can alter tactics, ordering, decomposition, coverage emphasis, or permitted
subgoals only. Any change to objective, acceptance, inputs, authority, budget, prohibited work,
required outputs, or linked-run permissions is rejected and routed to run control, linked-run
admission, fork, or a new run.

### 7.2 Iteration, verifier and handoff

Every significant executor iteration and every verifier invocation is a separately bound
`OperationWorkflow`. The verifier cannot share the executor's operation identity, agent session,
binding, or writable workspace. It may consume only admitted executor outputs and the exact rubric.

`CON-BP-GOAL-HANDOFF-V1` contains:

- handoff identity, schema version and digest;
- exact run, epoch, goal revision and source iteration;
- accepted facts, evidence and artifact refs;
- attempted and rejected tactics with reason codes;
- unresolved obligations and blockers;
- effect frontier and pending liabilities;
- consumed/reserved/remaining budgets and iteration limits;
- protected context facts and context selection/compaction decisions;
- workspace/snapshot refs permitted for the next session;
- source document and binding digests;
- redacted bounded continuation instructions.

A fresh empty session must be able to continue from this contract and exact referenced context
alone. Hidden chat history is never required.

`CON-BP-GOAL-VERIFICATION-V1` contains verifier operation/binding identity, exact revision and
iteration, accepted input/evidence refs, rubric and acceptance versions, decision, findings,
obligation applicability, stale frontier, usage/effects, and digest.

### 7.3 Convergence table

The interpreter evaluates accepted current facts in this strict order:

| Precedence | Condition | Proposal |
|---:|---|---|
| 1 | invariant or authority breach | `fail` or `escalate` according to frozen policy |
| 2 | hard budget exhausted | `partial_or_fail` |
| 3 | independent verifier accepts all required obligations | `complete` |
| 4 | irrecoverable failure | `partial_or_fail` |
| 5 | no-progress or repeated-blocker threshold | `pause`, `revise`, `escalate`, or `partial_or_fail` according to policy |
| 6 | iteration limit | `partial_or_fail` |
| 7 | authorized soft-budget response | `continue`, `reduce_effort`, or `skip_degradable` |
| 8 | accepted bounded tactical revision | `revise` |
| 9 | repair requested inside envelope | `repair` |
| 10 | otherwise | `continue` |

The interpreter state may be `ready`, `executing`, `awaiting_verification`, `waiting`, `paused`, or
`stopping`. It must not use `terminal`; its final output is a `GoalTerminalizationProposal`. The
run-control reducer maps that proposal and current authoritative evidence to a terminal outcome.

## 8. OperationWorkflow contract

`OperationWorkflow` represents exactly one semantic operation attempt. Its typed input includes:

- run/epoch/family/stage-or-goal/attempt/generation identities;
- ERC, control revision, operation assembly and execution binding refs/digests;
- exact objective, admitted inputs/context/artifact refs, authority and redaction ceilings;
- reservation/resource lease and workspace manifest refs;
- cancellation context, message cursor and effect frontier;
- retry, heartbeat, timeout, checkpoint and result policies.

Its result is a compact immutable manifest containing disposition, exact output/evidence/artifact
refs, usage, effect claims/settlement refs, checkpoint/handoff refs, async-child dispositions,
degradations/failures, binding/runtime lineage, and digest. Child or activity closure alone is not
the result.

The workflow follows `prepare -> bind -> execute/reconnect -> reconcile -> persist proposal ->
settle -> return manifest`. Technical retries reuse the semantic attempt and effect identities.

### 8.1 Initial async-subagent adapter

Deep Agents `0.7.5` exposes `AsyncSubAgent` and `AsyncSubAgentMiddleware`. The middleware uses the
LangGraph SDK against a remote Agent Protocol server and exposes `start_async_task`,
`check_async_task`, `update_async_task`, `cancel_async_task`, and `list_async_tasks`. It records a
provider thread ID, run ID, status, and timestamps in agent state.

WP-CP-045 wraps this exact mechanism; it does not treat the middleware state as the BellLabs
lifecycle. Before `start_async_task`, the adapter must persist the canonical contract, link and
reservation. The returned thread/run IDs are provider bindings on the existing child execution.
Checks and cancellation are reconciliation observations/requests. The adapter converts provider
output into a typed result manifest, after which parent application authority admits, rejects, or
defers it. Initial convergence uses polling through the middleware/SDK; callback optimization is
deferred and cannot change contract semantics.

## 9. Replacement and deletion rules

The packages delete or disconnect these active paths when their new owner lands:

| Old active responsibility | Replacement | Deletion gate |
|---|---|---|
| Direct StageGraph/GoalDirected Temporal submission | `BellLabsRunWorkflow` submission | WP-CP-030 acceptance |
| Direct family operation activities | `OperationWorkflow` children | WP-CP-030 acceptance |
| OpenAI Agents SDK operation runtime and Temporal plugin | Deep Agents adapter/materializer | WP-CP-040 acceptance |
| Agent Server StageGraph/GoalDirected macro graphs | Temporal family workflows | corresponding WP-BP acceptance |
| Parallel dependency maps and ambiguous stage concurrency fields | typed StageGraph V1 contracts above | WP-BP-010 contract tests |
| Provider/session completion authority | accepted facts + interpreter + reducer | owning package tests |
| Prototype async-child records tied to remote graphs | `CON-CP-ASYNC-SUBAGENT-V1` | WP-CP-045 acceptance |

Deletion includes imports, dependencies, settings, composition, worker registration, launch code,
live scripts, and tests whose only purpose is the superseded runtime. Historical evidence and
clearly isolated experiments may remain, but the default test suite must not require the OpenAI
Agents SDK or an Agent Server macro scheduler.

## 10. Package definition of ready and done

A package is **ready** when its requirements, contract fields, ownership, exact target paths,
replacement boundary, tests, and evidence are specified and all prerequisite packages are either
accepted or explicitly listed as blockers. `ready` does not mean dependencies are complete.

A package is **accepted** only when:

1. every listed atomic requirement maps to at least one exact test/evidence item;
2. new contracts have schema version, strict validation, canonical serialization and digest tests;
3. unit, contract, integration, replay and applicable acceptance tests pass;
4. the new path is registered in the actual application/worker composition;
5. the superseded active path and dependency are removed at its deletion gate;
6. searches prove absence of forbidden runtime imports/launches/authority writes;
7. exact changed paths, commands and sanitized outputs are recorded under `evidence_v2/WP-*/`;
8. documentation and traceability point to actual code, migrations, tests and evidence.

### 10.1 Parameters that do not block implementation

The remaining specification “open decisions” are owned parameters, not missing architecture:

| Parameter | Decided by | Constraint |
|---|---|---|
| PostgreSQL table/index and Mongo collection names | owning package migration/model | one writer/owner; names recorded in evidence |
| Retention durations and payload externalization threshold | deployment configuration | cannot change canonical content digest or historical authority |
| Workflow numeric budgets, cycle/iteration limits and verifier rubrics | published Workflow Type/blueprint | frozen in ERC before admission |
| Continue-As-New thresholds and message batch sizes | WP-CP-030 runtime configuration | forced-threshold tests must preserve semantics |
| Worker queue sizes and process topology | deployment configuration | exact logical queue is frozen in execution binding |
| Async timeout/orphan polling cadence | Deep Agent placement/profile | parent contract and settlement semantics remain unchanged |
| Remote LangSmith placement availability | separate qualification evidence | no silent fallback from local placement |

An implementer may choose physical names and safe test values inside the owning package without an
additional architecture interview. Any choice that changes authority, contract meaning, workflow
hierarchy, or dependency order requires an explicit specification amendment.

## 11. Required verification commands

Each package runs the narrowest relevant subset plus the final shared checks:

```powershell
.\.venv\Scripts\python.exe -m ruff check app tests
.\.venv\Scripts\python.exe -m mypy app
.\.venv\Scripts\python.exe -m pytest -q
```

Temporal packages additionally run captured-history replay, time-skipping, worker-loss,
redelivery, cancellation and forced Continue-As-New suites. PostgreSQL packages run against a
disposable application database with concurrency and transaction-failure injection.

Final replacement drift searches must show no required-runtime matches outside explicitly marked
historical/experiment paths:

```powershell
rg -n "from agents|import agents|OpenAIAgentsPlugin|openai_agents" app tests pyproject.toml
rg -n "start_workflow\(.*StageGraphWorkflow|start_workflow\(.*GoalDirectedWorkflow" app tests
rg -n "agent_server.*(stagegraph|goal_directed)" app tests
```
