# Control-plane implementation readiness contract

Status: canonical implementation companion  
Applies to: every work package in `implementation_work_packages_v2/`  
Architecture authority: `ADR-0003` and the canonical `SPEC-CP-*` / `SPEC-BP-*` specifications
Canonical metadata revision: `c48867a240d09a98db9cdfb4937f55176f30adf1`

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
| `CON-BP-STAGEGRAPH-V2` | `domain/control_plane` | MongoDB | compiler + StageGraph interpreter | normalization, canonical ordering, fairness, joins, late results, and liabilities |
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

`CON-BP-STAGEGRAPH-V2` is the active contract for newly published StageGraph blueprints. It defines
stage identities, edges, joins, operation slots, cycles, concurrency/fairness, waits, linked-run
slots, the obligation matrix, completion policy, and the executable rules below. This mirror is
recorded from canonical metadata revision `c48867a240d09a98db9cdfb4937f55176f30adf1`.

### 6.1 Pre-publication normalization and canonical ordering

Normalization is a pure compiler phase between authoring validation and immutable publication. It
MUST complete before canonical serialization, digest calculation, and publication, in this order:

1. Validate closed enums, scalar types, identifier constraints, references, join cardinalities,
   policy coverage, and numeric ranges.
2. Materialize only authored-definition defaults explicitly declared by V2, including the
   `default` fairness group case below and typed values for absent optional authored fields.
3. Convert every set-like collection to its contract-defined order, while preserving arrays whose
   order is explicitly semantic, such as late-policy rule precedence.
4. Serialize and digest the normalized payload using the canonical serialization decision in
   `SPEC-CP-DEFINITIONS`, then publish that exact payload and digest.

Normalization never reads environment state, aliases, clocks, provider state, or mutable defaults.
A published blueprint is never normalized, defaulted, reordered, or mutated again; execution
consumes its exact normalized bytes or a digest-verified immutable reference. If loaded content is
not byte-consistent with the published digest, execution fails closed rather than repairing it.

Unless a field declares numeric ordering, every V2 identifier and ordering string is compared
lexicographically by its canonical UTF-8 bytes, treating each byte as unsigned. Locale collation,
case folding, natural-number ordering, platform collation, and runtime Unicode normalization are
forbidden. Identifiers MUST already be Unicode NFC at authoring validation; a non-NFC identifier is
rejected rather than silently rewritten. Numeric fields compare by their declared integer value.
Tuple fields compare left-to-right and stop at the first unequal field.

Every collection in a published V2 blueprint MUST be classified by schema as either **set-like** or
an **authored semantic array**. Unclassified collections are publication errors. Set-like
collections are sorted during normalization by the following complete key registry:

| Set-like collection | Canonical ascending ordering key |
|---|---|
| `stages` | `(stage_id)` |
| `stage_mappings` | `(stage_id, mapping_id)` |
| `joins` | `(consumer_stage_id, join_id)` |
| `dependencies` / edges | `(consumer_stage_id, join_id, producer_stage_id, producer_output_slot_id, dependency_id)` |
| stage input slots | `(stage_id, input_slot_id)` |
| stage output slots | `(stage_id, output_slot_id)` |
| stage obligation slots | `(stage_id, obligation_slot_id)` |
| workflow obligation slots | `(obligation_slot_id)` |
| operation slots | `(stage_id, operation_slot_id)` |
| allowed operation variants | `(stage_id, operation_slot_id, operation_variant_id)` |
| linked-run slots | `(owner_stage_presence, owner_stage_id, linked_run_slot_id)` |
| obligation-matrix rows | `(obligation_scope, owner_stage_presence, owner_stage_id, obligation_slot_id, evidence_slot_id)` |
| fairness groups | `(group_id)` |
| policy definitions | `(policy_kind, scope_kind, scope_id, policy_id)` |
| waits | `(scope_kind, scope_id, wait_id)` |
| cycle limits / stopping conditions | `(scope_kind, scope_id, condition_kind, condition_id)` |
| invalidation and reuse declarations | `(scope_kind, scope_id, declaration_kind, declaration_id)` |
| concurrency, budget, and capacity ceilings | `(scope_kind, scope_id, dimension_kind, dimension_id)` |
| linked-run dependency declarations | `(linked_run_slot_id, dependency_id)` |
| completion-obligation references | `(obligation_scope, owner_stage_presence, owner_stage_id, obligation_slot_id)` |

Every textual component in these keys uses the canonical UTF-8 bytewise comparison above. Every
numeric component uses unsigned integer order. Presence fields use `0` for absent and `1` for
present; an absent associated identifier is the typed non-user value `NO_OWNER_STAGE`, which sorts
before every present identifier. Enum-valued key fields (`obligation_scope`, `policy_kind`,
`scope_kind`, `condition_kind`, `declaration_kind`, and `dimension_kind`) are compared by their
canonical lowercase schema token as UTF-8 bytes, not implementation enum ordinals.

The listed final identity field is the explicit tie-breaker after all preceding scope fields. A
complete-key collision is a duplicate-identity publication error; authoring order, database
insertion order, object ID, hash-map iteration, and storage-generated IDs MUST NOT break ties. If a
V2 extension adds another set-like collection, its accepted schema revision MUST add a total key to
this registry before publication; a generic runtime fallback sort is forbidden.

The following arrays have semantic order and MUST preserve their authored element order exactly
through normalization and digesting rather than be sorted:

- `slow_sibling_policy.triggers`, whose first matching trigger wins;
- `late_result_policy.rules`, whose first matching non-veto rule wins;
- any explicitly declared operation fallback/selection sequence;
- any explicitly declared evaluation, repair, or stopping-rule precedence sequence; and
- any explicitly declared sequential input-binding or output-assembly sequence.

Every semantic-array element MUST carry a unique `rule_id`, `step_id`, or other schema-declared
identity suitable for evidence and diagnostics, but that identity does not reorder the array.
Collections that merely express membership—including stages, dependencies, slots, obligations,
groups, allowed variants, declarations, and policy definitions—are set-like even when authored in
JSON/YAML array syntax. Behavioral order MUST be represented either by one of the semantic arrays
above or by an explicit numeric rank/ordinal field whose containing set-like collection still uses
its registered canonical key.

### 6.2 Authored fairness groups and weighted round-robin

Every published V2 blueprint contains a non-empty `fairness_groups` collection. Each entry has a
unique non-empty `group_id` and an integer `weight` in `[1, 65535]`. An explicitly empty collection
and boolean, fractional, zero, negative, out-of-range, duplicate, or unknown-group values are
publication errors.

Each stage names one fairness group. An omitted stage group means the exact group ID `default`. If
the author omits the entire fairness block and no stage names a group, compilation materializes
`[{group_id: "default", weight: 1}]` into the immutable published blueprint. In every other case,
every referenced group, including `default` when implied by an omitted stage group, MUST have an
authored weight; the compiler and interpreter MUST NOT invent a missing weight.

The interpreter constructs the immutable weighted group ring as follows:

1. Sort groups by canonical UTF-8 bytewise `group_id` ascending.
2. For round `r = 1..max(weight)`, append each sorted group whose weight is at least `r`.
3. Initialize the group-ring cursor to index `0`. Initialize every per-group candidate cursor to
   the typed `BEFORE_FIRST` cursor state, which compares before every candidate key and is not a
   user-representable identity.

A ready candidate has the total typed identity:

`(stage_id, mapped_instance_presence, mapped_instance_id, workflow_cycle_ordinal, stage_cycle_ordinal, operation_slot_id)`

`mapped_instance_presence` is the integer tag `0` when mapping is absent and `1` when present. When
absent, `mapped_instance_id` is the typed sentinel `NO_MAPPED_INSTANCE`, which is not a valid
authored ID and sorts before every present mapped ID. Cycle ordinals are unsigned integers.
`operation_slot_id` identifies the exact declared operation within the stage/cycle; a semantic
operation-attempt ID is created only after admission and therefore MUST NOT participate in
pre-admission ordering. The candidate ordering key is `(priority, candidate_identity)`, where lower
integer priority sorts first and identity fields follow the canonical tuple rules above. This
identity distinguishes stages, mapped expansions, semantic cycles, and multiple operation slots
without relying on a runtime-generated attempt ID.

For each frontier selection, ready candidates in a group are sorted by the total candidate ordering
key. Scanning from `BEFORE_FIRST` starts at the first candidate. Otherwise it starts at the first
key strictly greater than the stored key and wraps once; if the stored candidate is no longer
ready, its key remains the exclusive lower bound before wrap. Group slots are scanned from the
group-ring cursor and wrap once. A candidate is selectable only if all compiled authority,
capacity, reservation, concurrency, budget, resumption, and policy gates accept it atomically.

On successful authoritative admission, and in the same transaction as its reservations, the
group-ring cursor advances to `(selected_ring_index + 1) mod ring_length` and that group's candidate
cursor becomes the admitted candidate's complete ordering key. A candidate that cannot be admitted
is skipped for that scan and advances neither cursor. If a complete ring scan yields no admission,
selection stops without cursor movement. After each success the next selection starts from the
newly accepted cursors and current accepted capacity facts. This produces the authored long-run
weight ratio whenever groups remain continuously admissible, while a blocked group cannot consume
turns or prevent another group from being scanned. Simultaneously accepted results are applied by
their total semantic identity using the same typed UTF-8/numeric tuple rules before this algorithm
is run again.

### 6.3 Dependency dispositions and joins

An edge has exactly one accepted upstream disposition from this closed set: `unresolved`,
`fulfilled`, `degraded`, `omitted`, `failed`, `cancelled`, or `invalid`. `fulfilled` requires
accepted current evidence for the edge's declared output. `degraded`, `omitted`, and `invalid` are
explicit authoritative decisions, never inferences from provider status, failure, or timeout.

When previously accepted evidence is found stale, malformed, digest-invalid, outside the current
invalidation frontier, or otherwise inadmissible, authority replaces that edge generation's
disposition with `invalid`; it never silently returns the same generation to `unresolved`. If
frozen repair policy permits replacement evidence, the accepted repair/invalidation decision
creates a new dependency generation with disposition `unresolved` and lineage to the `invalid`
generation.

The satisfaction truth table is:

| Dependency class | `fulfilled` | `degraded` | `omitted` | `failed` | `cancelled` | `invalid` | `unresolved` |
|---|---:|---:|---:|---:|---:|---:|---:|
| `required` | satisfies | does not satisfy | does not satisfy | does not satisfy | does not satisfy | does not satisfy | pending |
| `degradable` | satisfies | satisfies | does not satisfy | does not satisfy | does not satisfy | does not satisfy | pending |
| `optional` | satisfies | satisfies | satisfies | satisfies | satisfies | satisfies | pending |
| `advisory` | non-gating | non-gating | non-gating | non-gating | non-gating | non-gating | non-gating |

An accepted upstream failure satisfies a degradable edge only after authority emits the separate
accepted `degraded` disposition required by that edge's frozen degradation policy. An optional edge
waits while unresolved but, once settled, its presence or typed absence satisfies the gate.
Advisory edges are recorded and may inform later evaluations, but are excluded from join
cardinality and can neither satisfy nor make a join impossible.

For a join, let `N` be its non-advisory edge count, `S` the count that satisfies the table, and `U`
the count that is unresolved and can still reach a satisfying disposition. All remaining
non-advisory edges are terminally non-satisfying.

- `all` is satisfied exactly when `S = N`, impossible exactly when `S + U < N`, and pending
  otherwise.
- `any` is satisfied exactly when `S >= 1`, impossible exactly when `S = 0` and `U = 0`, and pending
  otherwise.
- `minimum(k)` is satisfied exactly when `S >= k`, impossible exactly when `S + U < k`, and pending
  otherwise.

Publication rejects `any` with `N = 0` and rejects `minimum(k)` unless `N > 0` and `1 <= k <= N`.
`all` with `N = 0` is vacuously satisfied. Once a join is satisfied, later sibling settlement does
not revoke work already admitted from that accepted projection; using newly admitted or changed
evidence requires the frozen invalidation/cycle policy and a new accepted decision.

For linked or otherwise separately admitted results, parent result decisions map deterministically:
`admit` emits the evidence-backed `fulfilled` or `degraded` disposition declared by the result;
`conditionally_admit` remains `unresolved` until every recorded condition is accepted; `reject`
emits `failed` for required/degradable edges, `omitted` for optional edges, and only an advisory
observation for advisory edges; `defer` remains `unresolved`. Provider completion alone emits none
of these dispositions.

### 6.4 Slow siblings, late results, and durable liabilities

A **slow sibling** is an unresolved producer whose sibling join has already become satisfied and
released at least one consumer. Each join MUST freeze a `slow_sibling_policy` containing:

- one or more triggers from `join_released`, `deadline_reached`, `accepted_budget_pressure`, or
  `cancellation_requested`;
- trigger precedence in authored list order;
- an execution action of `continue` or `request_cancel`; and
- an arrival route of `evaluate_late_result` or `quarantine`.

Triggers are evaluated only from accepted projection facts. `join_released` fires in the transition
that first admits a consumer from that join; `deadline_reached` uses the frozen deadline and
accepted time fact; `accepted_budget_pressure` requires an authoritative capacity/budget fact; and
`cancellation_requested` requires an accepted cancellation command. `continue` leaves the producer
eligible to finish. `request_cancel` enters the shared cancellation reconciliation saga; it does
not imply that work stopped or that charges/effects settled.

A result is **late** when it arrives after any frozen late trigger is true. The closed trigger set
is `consumer_already_admitted`, `dependency_terminally_disposed`, `producer_invalidated`,
`generation_superseded`, `evidence_invalid`, `run_cancelling`, `terminalization_started`, or
`run_terminal`.

Late-result decision composition has absolute precedence:

1. Evaluate absolute vetoes in this fixed order before any slow-sibling route or authored
   late-policy rule: `run_terminal` or `terminalization_started` yields `quarantine`;
   `generation_superseded`, `producer_invalidated`, or `evidence_invalid` yields `quarantine`;
   `run_cancelling` yields `quarantine`; `dependency_terminally_disposed` yields `reject`. The first
   matching veto wins and no authored rule may override it. `evidence_invalid` is true exactly when
   authority has classified the arriving evidence as stale, malformed, digest-invalid, or outside
   the current invalidation frontier.
2. If no absolute veto matched and the applicable slow-sibling policy's arrival route is
   `quarantine`, the decision is `quarantine`.
3. If the route is `evaluate_late_result`, evaluate the authored `late_result_policy` in list order
   and use the first matching `admit`, `reject`, or `quarantine` decision.
4. If no late trigger is true, use ordinary result-admission rules rather than the late-result
   policy.

The authored late-result policy covers every reachable non-veto late case, including
`consumer_already_admitted`; publication rejects uncovered cases. An arrival that matches more than
one absolute veto uses the fixed order above. An arrival that matches multiple authored predicates
uses authored list order. Thus absolute safety state dominates slow-sibling routing, and a
slow-sibling quarantine route dominates discretionary late admission.

The decisions have these exact effects:

- `admit` records current accepted evidence and its dependency disposition, then includes it only
  in subsequent interpreter calculations. It never changes terminal state or inputs already frozen
  for admitted work. If consuming it would change such work, the blueprint must authorize a new
  stage/workflow cycle and invalidation frontier.
- `reject` records the immutable rejection and reason and contributes no evidence. If the edge is
  still unresolved, it projects `failed` for a required/degradable edge, `omitted` for an optional
  edge, or a non-gating observation for an advisory edge; an existing terminal disposition is
  unchanged.
- `quarantine` durably stores the result content and reason outside accepted evidence and
  obligation projections. If the edge is still unresolved, it applies the same class-specific
  terminal disposition as `reject`, while preserving the quarantined payload for audit or
  authorized review. Review cannot reopen a terminal run; before terminalization, later use
  requires a new authorized admission decision and any required cycle/invalidation decision.

Admission, rejection, or quarantine of a result settles only the result-disposition portion of its
producer liability. Every admitted producer has a durable liability covering child
quiescence/closure, reservations and observed usage, effect claims, cancellation, and result
disposition. Early join release never deletes or transfers that liability. The liability closes
only when the child is terminal or authoritatively quiesced, every reservation and charge is
settled, every effect claim has an accepted settlement (including an explicitly permitted
ambiguous settlement), cancellation is reconciled, and the result has exactly one decision.

The interpreter may propose StageGraph completion only when `REQ-BP-SG-010` is satisfied and every
producer liability is closed. Therefore a continued or cancelling slow sibling may coexist with
downstream execution but cannot outlive terminalization. A quarantined or rejected result permits
completion only after its remaining liability fields settle. Terminalization freezes all
StageGraph projections; later arrivals are retained under the frozen reject/quarantine rule and
cannot alter outcome, obligations, reuse, or completion evidence.

### 6.5 Stage decision projection

`CON-BP-STAGE-DECISION-V1` defines readiness, frontier admission, stage/workflow evaluation,
invalidation, reuse, skip/degrade, late-result, and completion proposals. Under a V2 blueprint its
proposals carry the exact fairness cursors, dependency disposition, matched policy trigger/rule,
result decision, and liability-settlement references required above.

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

The V2 workflow input contains one exact typed `OperationExecutionRequest`; deprecated family
payload kinds are not accepted. For Deep Agent execution, the activity queue is derived only from
the immutable `DeepAgentExecutionBinding`. Native execution carries one content-addressed,
qualified native placement in that same operation request. The wrapper does not duplicate or
override either placement's queue. `OperationWorkflow` is registered only on coordinator/family
workflow workers; the `agent_cognitive` worker polls only `operation.execute`.

The exact operation envelope is retained in Temporal history because the current canonical
operation contract requires replay-stable execution intent and no accepted immutable-reference
repository already replaces it. Deployment therefore requires TLS for Temporal transport,
encryption at rest for Temporal persistence/history, namespace-level authorization, and worker
service identities restricted so coordinator/family workers may schedule the declared cognitive
queue while cognitive workers may poll only that queue. These deployment controls are
preconditions, not a second BellLabs authority. Secrets and secret values remain forbidden from
the envelope. The implementation bounds the complete serialized workflow wrapper to 2,000,000
bytes, prompt segments to 64, semantic/placement/task-queue/frontier/async-child identifier lengths,
and compact frontier/async-child collections to 1,024 entries. Runtime async-child signals reject
an invalid identifier or a 1,025th unique child non-retryably; duplicate child signals remain
idempotent. Signal-with-Start child identities delivered before run initialization are merged after
the request's authored child order and in signal-delivery order, with deterministic de-duplication
and the same 1,024-child ceiling; initialization fails closed if the combined set exceeds it.

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
| Parallel dependency maps and ambiguous stage concurrency fields | typed StageGraph V2 contracts above | WP-BP-010 contract tests |
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
