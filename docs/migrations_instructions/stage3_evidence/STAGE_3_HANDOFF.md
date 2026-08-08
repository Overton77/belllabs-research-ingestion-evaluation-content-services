# Stage 3 handoff — durable runtime kernel

Status: ACCEPTED
Prepared by: GPT-5.6 Sol implementing agent
Prepared at: 2026-08-06
Repository/worktree: `biotech-research-ingestion-evaluation-system`, branch `main`, clean at acceptance
Base revision: `d40d5862b06bb74789a349087112162f0a879094`
Result revision or diff ref: `135dc90`
Accepted by: owner
Accepted at: 2026-08-06

## Outcome

The Stage 3 durable runtime kernel is implemented and accepted. Its deterministic, PostgreSQL,
static-analysis, and full-regression gates pass, and independent durability and security
re-reviews report no remaining mandatory finding. The owner accepted this handoff on 2026-08-06.

The kernel now supplies frozen V3 dispatch, exact deployment binding, common authority bootstrap,
canonical lineage, hierarchical leases, durable decisions, typed steering, cancellation/fork
policy, resumable BellLabs event translation, reconciliation incidents, audited repair/retention,
and the runtime-neutral operation executor protocol needed by Stages 4–6.

## Scope completed

- Compact common checkpoint state and deterministic fail-closed reducers.
- Production dispatch requires a frozen `RunPlanV3` with exact endpoint/deployment/assistant/graph
  route; legacy V2 dispatch is test/compatibility-only and explicitly opted in.
- Agent Server SDK adapter creates deterministic threads, tags runs with immutable BellLabs
  metadata, reconciles ambiguous creation, and never falls forward to another revision.
- Both StageGraph and GoalDirected begin with the same authoritative bootstrap node.
- Provider-qualified lineage and explicit parent edges persist independently of checkpoint bodies.
- Atomic resource leases enforce canonical hierarchy, capacity, TTL, wait retain/release rules, and
  dedicated resumption capacity.
- BellLabs decisions are persisted before resume maps are generated; parallel interrupt identities
  remain distinct.
- Typed interventions recheck scope, actor, lifecycle version, checkpoint, and privileged approval,
  then reserve the command before any provider call.
- Cooperative cancellation protects terminal cancellation from late provider success.
- Fork admission creates a new run/budget/thread once and leaves the parent immutable.
- Durable outbox translation provides monotonic cursor replay, deduplication, compact references,
  redaction, and operator-gated diagnostics.
- The complete Stage 3 incident catalog fails closed and persists decisions plus immutable repair
  evidence.
- Runtime-neutral `OperationExecutor` outcomes and the shared conformance harness are published.
- Managed graphs rely on Agent Server persistence. Standalone use has one async saver/Store process
  lifespan, explicit setup, tenant/purpose namespaces, and cancellation-safe closure.
- Migration `0014_stage3_durable_runtime_kernel.sql` and RLS-scoped PostgreSQL repositories cover
  lineage, leases, decisions, incidents, repair audit, and retention deletion audit.

## Explicitly not completed

- StageGraph business scheduling and GoalDirected business execution remain placeholders.
- No Deep Agents harness, MCP tool execution, sandbox, Store memory behavior, async subagent,
  QuickJS delegation, or production routing was enabled.
- Epoch rollover remains disabled until an exact compatible policy is published.
- Diagnostic replay remains isolated and cannot acquire effect claims.
- Arbitrary Agent Server checkpoint mutation is not exposed. Privileged reconciliation is a typed,
  authorized, durable path and unchecked state-update mapping is rejected.
- Legacy OpenAI Agents SDK/Temporal execution remains the operational fallback.

## Owner decisions and assumptions

| ID | Decision | Effect |
|---|---|---|
| D-17 | Port BellLabs semantics, not provider/model identity | Runtime contracts remain provider-neutral |
| D-18–D-22 | Stage requirements compile before later operation consumers | Stage 3 publishes kernel contracts only |
| D-23 | One canonical lineage spans every consequential identity | Lineage enum, records, edges, and provenance query are mandatory |
| D-INT | Persist BellLabs decision before same-thread resume | Resume maps reread durable decisions |
| D-REPAIR | Deny arbitrary mutation; privileged repair needs expected facts and audit | Standard action resolver rejects unchecked state updates |
| D-FORK | Retry preserves semantic identity; fork creates new authority | Fork gets new run/thread/budget/lineage |
| D-EPOCH | Epoch rollover is disabled without published compatibility policy | Recovery policy fails closed |
| D-RETENTION | Interim Stage 3 retention is 90 days with audited tenant deletion | `retain_until` and deletion audit are persisted |

## Common state and reducer manifest

Compatibility version remains the frozen state-schema digest in each `RunPlanV3` and binding.
`CommonStateMetadata` allows only identities, digests, refs, compact decisions/events/diagnostics,
and monotonic versions/cursors. It rejects payload-shaped keys, sensitive fields, and oversized
serialized state.

| Channel class | Reducer | Rule |
|---|---|---|
| Frozen identity/digest/ref | `merge_single_assignment` | same replay accepted; conflict raises |
| Version/cursor | `merge_monotonic_integer` | commutative/idempotent maximum |
| Keyed parallel result | `merge_keyed_canonical_digest` | sorted ACI merge; digest collision raises incident |
| Compact event refs | `merge_unique_events` | deterministic first-seen deduplication |

Privileged replacement is not a normal reducer. It must pass the typed operator reconciliation
path; provider-side unchecked `update_state`/`Overwrite` is not publicly mapped.

## Runtime binding and attempt state machines

Binding states:

`submitting -> accepted -> running -> waiting|paused|cancelling -> completed|failed|cancelled`

Any ambiguous, incompatible, or identity-conflicting observation moves to
`reconciliation_required`; provider status alone cannot advance BellLabs lifecycle authority.

Attempt dispositions:

`created -> accepted -> running -> succeeded|failed|cancelled`, with `ambiguous` as a durable
reconciliation state. Binding route facts, digests, deterministic thread identity, and submission
metadata are immutable.

## Dispatcher ambiguity matrix

| Observation | Action |
|---|---|
| Same idempotency key and same digest | Return existing binding/receipt |
| Same identity with changed digest/route | Fail closed |
| Timeout after possible create | Search exact thread/run metadata; do not blindly retry |
| Matching provider run on bound endpoint | Bind observed run |
| Definitive not-found | Mark failed before dispatch |
| Unknown/temporarily unavailable | Remain pending |
| Run on another deployment or metadata mismatch | Operator required |

Every resume, input, cancellation, or reconciliation call is routed by persisted
`(endpoint_id, revision, assistant_id, graph_id)`. N-to-N+1 fall-forward is prohibited.

## Interrupt and intervention sequence

1. Graph emits only a compact interrupt/decision envelope and schema/reference metadata.
2. BellLabs persists the scoped decision request and runtime-interrupt mapping.
3. Response rechecks expiry, lifecycle version, actor/scope authority, and idempotency.
4. BellLabs persists the response.
5. Action resolution rereads persisted decision refs/digests and creates a compact resume map.
6. Exact-route client invokes only the bound deployment.
7. Receipt/ambiguity is persisted and reconciled; no provider callback becomes lifecycle authority.

Active-run append defaults to `reject`; `enqueue` requires an authored policy. Async task commands
remain disabled until Stage 6. Privileged reconcile requires an operator actor, approval reference,
expected version/checkpoint, reason, evidence, and durable reservation/audit.

## Cancellation, fork, replay, and epoch policy

- Cancellation cascades runtime-owned resources, but linked BellLabs runs require their own
  commands. Cancellation settlement wins over late success.
- Technical retry retains semantic operation identity and may create a new runtime attempt.
- Fork admits a new BellLabs run and budget before copying provider checkpoint state; it creates a
  new thread and lineage while preserving the parent.
- Diagnostic replay creates isolated execution, cannot claim effects, and is non-authoritative.
- Rollback cannot rewrite authoritative history.
- Epoch rollover remains disabled.

## Stream taxonomy and cursor semantics

BellLabs durable outbox position is the only resume cursor. Reconnect requests `after=<cursor>`,
drops duplicate/non-monotonic positions, and filters to the bound run. Payloads expose status,
phase, reason/failure/retry layer, intervention kind, IDs, refs, and digests only. Node/checkpoint/
provider detail requires operator-debug authorization. Raw provider chunks are observability detail,
not authoritative BellLabs events.

## Reconciliation incident catalog

- binding without thread
- thread without initial run
- provider active while BellLabs stopped
- BellLabs active without runtime
- unsettled accepted operation
- stale decision
- orphan runtime resource
- terminal state without typed result
- incompatible checkpoint route
- outbox cursor drift
- expired resource lease
- lineage gap or collision
- missing assembly or context digest

Safe, compatible, exact-version repairs may be automatic. Ambiguous effects, incompatible routes,
lineage collisions, missing frozen digests, or incomplete expected-version facts require an
operator. Incident and repair records carry tenant, before/after versions or digests, actor, reason,
evidence, retry schedule, and idempotent identity.

## Canonical lineage and resources

Provider-qualified lineage kinds include BellLabs run, epoch, semantic attempt, runtime attempt,
agent invocation/thread/run, async task, effect claim, usage settlement, artifact, result manifest,
and trace. Parent edges are immutable, scope-bound, and may reference only persisted qualified
identities. `provenance_for_result` reconstructs final-result ancestry without transcripts or
checkpoint bodies.

Canonical acquisition order:

`tenant -> workflow_run -> stage -> operation_worker -> resumption -> model_call -> tool_call ->`
`mcp_call -> sync_subagent -> async_child -> linked_run -> provider_quota -> budget_reservation`

Requests must be unique and already ordered. Same semantic identity/envelope is idempotent; changed
facts conflict. Wait transitions explicitly retain/release lease IDs. Expiry makes capacity
reclaimable, and resumption capacity is separate from worker capacity.

## Operation executor protocol

Stages 4–6 must call the async `OperationExecutor.execute(stage_request,
exact_stage_execution_binding, execution_resource_lease, cancellation_context)` boundary and
return one discriminated outcome: `completed`, `waiting`, `paused`, `degraded`, `failed`, or
`cancelled`. Outcomes contain refs and stable failure taxonomy only; they cannot mutate BellLabs
lifecycle fields.

## Contract and compatibility impact

- `GraphExecutionSubmission` adds optional exact target deployment/graph fields; production V3
  requires both.
- `RuntimeExecutionBinding` adds immutable `graph_id`; LangGraph bindings require deployment,
  graph, and thread together.
- Stage 2/V2 contracts remain readable. Existing V2 dispatch is explicit compatibility mode only.
- Stage 3 kernel contracts and operation-outcome union are exported by the schema API.
- Existing checkpoints are admitted only when route, assembly, state-schema, binding version, and
  lifecycle projection are compatible with authority.

## Data and migration status

- `0014_stage3_durable_runtime_kernel.sql` passed against disposable local PostgreSQL.
- The non-skipped repository slice applies all migrations, exercises lineage/decision/lease/
  incident/repair/retention persistence, conflict replay, capacity, expiry, and audited deletion.
- New tables force request-scope RLS and grant bounded runtime/read-only roles.
- No production, Supabase, Atlas, or shared database was targeted.
- No production backfill or destructive production action is authorized.

## Feature maturity and flags

| Capability | Version/maturity | Stage 3 state |
|---|---|---|
| LangGraph | `1.2.10`, pinned | kernel graph/bootstrap enabled in code; production admission disabled |
| PostgreSQL checkpoint | `3.1.1`, pinned | managed by Agent Server; standalone fixture explicit |
| Agent Server API | `0.12.0`, pinned | exact-route adapter implemented; production routing disabled |
| Deep Agents | `0.7.4`, later-stage | disabled |
| MCP/sandboxes/Store memory | later-stage | disabled |
| Async subagents/QuickJS | preview/later-stage | disabled |
| LangSmith tracing/evaluation | Stage 7 owner | no gate dependency added |

## Verification evidence

| Command/drill | Environment | Result |
|---|---|---|
| `TEST_APPLICATION_POSTGRES_DSN=... uv run pytest -q tests/*stage3*.py` | locked env + disposable PostgreSQL | 67 passed |
| Stage 3 PostgreSQL integration slice alone | disposable application PostgreSQL | 1 passed, non-skipped |
| `uv run pytest -q` | local locked environment | 528 passed, 25 optional/external skips |
| `uv run ruff check app tests` | local locked environment | passed |
| `uv run mypy app` | local locked environment | passed, 286 source files |
| Persistent restart/interrupt/cancel/fork/tenant drills | accepted pre-Stage 3 Block C N/N+1 deployments | passed |
| Independent durability re-review | accepted Stage 3 implementation at `135dc90` | approved; no mandatory finding |
| Independent security re-review | accepted Stage 3 implementation at `135dc90` | clean; no gate blocker |

The DSN value is intentionally omitted. The integration guard accepted only the known disposable
localhost database.

## Failures, skips, and residual risks

| Item | Gate effect | Follow-up |
|---|---|---|
| 25 full-suite skips | Optional/external services only; focused Stage 3 suite ran without skips | Preserve explicit evidence-mode guards |
| Direct same-server N-to-N+1 resume is unsafe | Normative routing constraint, not an open failure | Keep exact route-then-invoke |
| Business runtime-plan assets are not published | Kernel passes; business launch stays fail closed | Stage 4 authors scheduling assets |
| Standalone setup can run migrations when explicitly requested | Operational risk if misused | Run only in controlled startup/migration path |
| No production cutover performed | Required non-goal | Later migration gate owns rollout |

## Security and data handling

- No credential values, secrets, PHI, raw private payloads, checkpoint bodies, or transcripts are
  recorded in this handoff or common state.
- Agent Server auth remains role- and request-scope-bound; no unscoped Studio bypass exists.
- RLS is forced on Stage 3 tables; repositories set request scope transaction-locally.
- Stream events and interrupt summaries reject sensitive payload shapes and expose references.
- Privileged repairs require matching operator authorization and immutable audit facts.
- `langgraph.block_c.env` contains variable references only, not credential values.

## Operations and rollback

- Do not enable production LangGraph admission or alter production routing at this gate.
- Apply migration `0014` only through the normal application migration runner after backup/change
  approval. Roll back application behavior first; do not erase accepted runtime history.
- Running epochs remain pinned to exact endpoint/revision/assistant/graph and frozen digests.
- Reconcile ambiguous submissions by metadata before retry.
- On process loss, expire/reconcile leases before re-admission.
- Managed deployments must not inject explicit saver/Store objects. Standalone processes create one
  `StandalonePersistenceLifespan` at startup and close it at shutdown.
- Legacy execution remains the operational fallback until later acceptance/cutover gates pass.

## Next-stage entry assessment

| Criterion | Met? | Evidence/blocker |
|---|---|---|
| Stage 3 requirements matrix complete | Yes | `STAGE_3_REQUIREMENTS_EVIDENCE.md` |
| Focused Stage 3 and PostgreSQL evidence passes | Yes | 67 passed, no skip |
| Full regression/static gates pass | Yes | 528 passed; Ruff/Mypy pass |
| Optional later-stage capabilities remain disabled | Yes | readiness/governance and explicit failure paths |
| Independent technical/security review complete | Yes | durability approved; security clean |
| Stage 3 accepted by owner/gate reviewer | Yes | owner accepted the handoff on 2026-08-06 |
| Stage 4 may begin | Yes | accepted Stage 3 kernel and handoff authorize Stage 4 entry |

## Gate recommendation

ACCEPTED. STAGE 4 MAY BEGIN.

The Stage 4 implementing agent must preserve the accepted runtime, HITL, resume, steering,
cancellation, reconciliation, compatibility-routing, lineage, and resource-lease contracts in
this handoff and the normative Stages 3–6 shared execution contract.
