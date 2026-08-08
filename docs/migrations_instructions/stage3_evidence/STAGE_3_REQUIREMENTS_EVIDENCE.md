# Stage 3 requirements-to-code and evidence matrix

Status: ACCEPTED
Prepared at: 2026-08-06
Scope: durable runtime kernel only; StageGraph business scheduling and later-stage capabilities remain disabled

| Requirement ID | Atomic requirement | Implementation location | Verification | Status |
|---|---|---|---|---|
| S3-R01 | Common state contains only compact identities, digests, projection refs, decisions, cursors, diagnostics, and result refs | `app/agent_server/common_state.py`; graph state modules | `test_stage3_kernel_contracts.py` prohibited-payload/size checks | verified |
| S3-R02 | Immutable channels are single-assignment and reject conflicting replay | `app/agent_server/reducers.py` | deterministic reducer conflict checks | verified |
| S3-R03 | Versions and outbox positions advance monotonically | `app/agent_server/reducers.py` | randomized monotonic reducer checks | verified |
| S3-R04 | Keyed parallel merges are associative, commutative, idempotent, and conflict detecting | `app/agent_server/reducers.py` | randomized ordering/duplicate/digest-conflict checks | verified |
| S3-R05 | Privileged replacement requires Overwrite, expected checkpoint/version, actor, reason, and audit | typed privileged-reconcile contract and `RuntimeInterventionService`; unchecked Agent Server state updates are denied | contract, operator-authorization, stale-version/checkpoint, durable reservation tests | verified-fail-closed |
| S3-R06 | Dispatch consumes only an authoritative frozen `RunPlanV3` submission | `app/application/graph_runtime_dispatch.py` | production-V3 and frozen-route tests | verified |
| S3-R07 | Exact endpoint, assistant, graph, assembly, thread, run, and attempt facts are persisted before active use | dispatch client/repository and migration `0014` | binding-before-submit and exact metadata tests | verified |
| S3-R08 | Duplicate delivery is idempotent and conflicting submission identity fails closed | dispatch/repositories | replay and conflict tests | verified |
| S3-R09 | Ambiguous submission is reconciled by immutable metadata before any retry | dispatch/reconciliation services | timeout-after-create metadata reconciliation tests | verified |
| S3-R10 | Every resume/steer/cancel routes only to the binding's exact compatible endpoint and assistant | exact router and pinned Agent Server client | wrong-revision/route and N/N+1 qualification tests | verified |
| S3-R11 | Canonical lineage preserves distinct BellLabs, epoch, semantic attempt, runtime attempt, thread, run, task, effect, settlement, artifact, result, and trace identities | lineage contracts/repository and migration `0014` | complete provider-qualified identity-chain test | verified |
| S3-R12 | Parent/child lineage edges are explicit and immutable | lineage repository | edge endpoint, parent-first, scope, and collision tests | verified |
| S3-R13 | Final-result provenance is queryable without checkpoints or transcripts | lineage query service | typed-result-to-runtime-retry provenance test | verified |
| S3-R14 | Resource envelopes are reserved hierarchically before work and use canonical acquisition order | resource lease service/repository | order and capacity tests | verified |
| S3-R15 | Same semantic lease and digest is idempotent; a changed digest conflicts | resource lease repository | duplicate/conflicting-envelope tests | verified |
| S3-R16 | Lease renew/release/expiry/reconciliation prevents permanent capacity leakage | resource lease service/repository | controlled-clock process-loss test plus PostgreSQL round trip | verified |
| S3-R17 | Supervisor/resumption capacity is protected from child saturation | resource lease service | saturated-worker/resumption-capacity test | verified |
| S3-R18 | Durable waits retain and release only explicitly declared resources | resource lease service | wait retain/release test | verified |
| S3-R19 | Both graph families begin with common authoritative bootstrap reconciliation | common bootstrap and both graph builders | graph topology and safe-rebuild tests | verified |
| S3-R20 | Bootstrap never treats checkpoints or provider status as lifecycle authority | bootstrap service/node | stale/ahead/incompatible projection tests | verified |
| S3-R21 | Decision request and response are durable, typed, scoped, expiring, version checked, and idempotent | decision contracts/repository/service | stale/expired/actor/scope/version and PostgreSQL tests | verified |
| S3-R22 | Interrupt payload contains compact display data and BellLabs decision ID only | durable interrupt and decision contracts | compact schema/ref and sensitive-payload rejection tests | verified |
| S3-R23 | Resume rereads the persisted decision and supports parallel interrupt-ID mapping | interrupt action resolver/client | duplicate and parallel interrupt mapping tests | verified |
| S3-R24 | Every typed intervention is authorized and persisted before provider action | intervention service/repository | authority, stale binding, reservation-before-client tests | verified |
| S3-R25 | Active-run behavior rejects by default; enqueue requires an authored declaration | intervention action policy | reject-default and authored-enqueue tests | verified |
| S3-R26 | Cancellation is a distinct cooperative lifecycle and late success cannot overwrite it | cancellation planner, settlement guard, Agent Server client | active-run cancellation and late-success tests | verified |
| S3-R27 | Fork creates a new BellLabs run, epoch, thread, budget/admission ref, and child lineage without mutating its parent | fork service/repositories | one-time admission and parent-immutability tests | verified |
| S3-R28 | Diagnostic replay cannot acquire effect claims and epoch rollover remains disabled | recovery policy | recovery-mode negative tests | verified |
| S3-R29 | BellLabs events use monotonic durable outbox cursors and deduplicate reconnect replay | event translator | reconnect/dedup tests | verified |
| S3-R30 | Runtime stream detail is non-authoritative, redacted, bounded, and retry-layer explicit | event translator | sensitive payload and operator-debug gating tests | verified |
| S3-R31 | Reconciliation covers all Stage 3 incident classes with tenant/version/idempotency guards | reconciliation service/repository and migration `0014` | complete incident-enum and replay tests | verified |
| S3-R32 | Unsafe reconciliation requires an operator decision and records before/after versions, actor, reason, evidence, and retry schedule | reconciliation service/repository | unsafe ambiguity/operator and PostgreSQL audit-fact tests | verified |
| S3-R33 | Runtime-neutral async `OperationExecutor` and discriminated outcomes are published | operation executor contracts | complete outcome-union and adapter conformance tests | verified |
| S3-R34 | Managed graphs have no explicit saver/Store; standalone persistence uses one async lifespan and closes on cancellation | graph exports and `langgraph_persistence.py` | one-lifespan/setup/cancellation tests | verified |
| S3-R35 | Cross-tenant access to bindings, leases, lineage, decisions, incidents, events, checkpoints, and forks is denied | PostgreSQL RLS in migration `0014`, scoped repositories, Agent Server auth | disposable PostgreSQL scope checks and accepted Block C tenant-denial drills | verified |
| S3-R36 | Stage 3 records follow 90-day retention with audited tenant-scoped deletion | migration `0014` and retention repository | PostgreSQL expiry/deletion/idempotent-audit test | verified |
| S3-R37 | Deep Agents, MCP, Store memory, sandboxes, async subagents, QuickJS, production routing, and business scheduling remain disabled | capability manifests/config | readiness and import-side-effect tests | verified |

## Gate rows

| Gate ID | Evidence required | Status |
|---|---|---|
| S3-G01 | Focused deterministic unit and integration suite passes | passed: 67 |
| S3-G02 | Disposable PostgreSQL migrations/RLS/repository recovery suite passes | passed: 1 non-skipped integration slice |
| S3-G03 | Persistent Agent Server restart, interrupt, cancel, fork, and exact-route drills pass | passed in accepted entry Block C; Stage 3 exact-route adapters add deterministic tests |
| S3-G04 | `uv run ruff check app tests` passes | passed |
| S3-G05 | `uv run mypy app` passes | passed: 286 source files |
| S3-G06 | Full accepted `uv run pytest` suite passes or every exception has explicit gate impact | passed: 528, 25 optional/external skips |
| S3-G07 | Technical review and security review have no unresolved mandatory findings | passed: durability and security re-reviews approved |
| S3-G08 | Outgoing handoff is accepted only by the owner/gate reviewer | passed: owner accepted the handoff on 2026-08-06 |
