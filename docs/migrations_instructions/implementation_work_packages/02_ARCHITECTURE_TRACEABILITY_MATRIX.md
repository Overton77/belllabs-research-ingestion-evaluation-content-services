# Architecture and implementation traceability matrix

Status: cross-stage coverage checklist  
Purpose: prove that high-detail architecture requirements are owned by a stage and cannot disappear between handoffs

## 1. Authority and lifecycle traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| BellLabs definitions/compiler remain semantic authority | Controlled-run §§2–3; migration plan §§1–3 | Stages 0–1, verified 4–8 |
| PostgreSQL run control remains lifecycle/budget/decision/terminal authority | Controlled-run §2.2; plan §§2–3 | Stages 1, 3–8 |
| Checkpoints are runtime facts, not lifecycle authority | Plan §§3–4, 9 | Stages 1–3, 7–8 |
| Runtime execution binding maps BellLabs run/epoch to Agent Server identities | Plan §3.2 | Stages 1 and 3 |
| One parent thread per request scope/run/epoch; fork gets a new run/thread; linked runs and async subagents use bound child threads | D-05; plan §§3, 9 | Stages 1, 3, 6–7 |
| Effect claims/attempts/settlements move to PostgreSQL while immutable semantic Operation Execution Bindings remain MongoDB/Beanie-authoritative and digest-referenced | D-13; accepted `biotech-meta`; plan §10.3 | Stages 0–1, used 4–6 |
| At-least-once execution with stable exactly-once claim/settlement identities | Round-two long-running execution; plan §11.6 | Stages 1, 3–6 |
| Runtime retries remain distinct from semantic retries | Migration recommendations retry model; plan §11.6 | Stages 1, 3–7 |
| Graph cannot terminalize without BellLabs verification/transition | D-04; plan §§5–6 | Stages 4–5 |

## 2. Graph and state traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Generic frontier scheduler wraps the pure StageGraph interpreter | D-02; plan §5 | Stage 4 |
| `Send` fan-out occurs only after authoritative reservation/admission | Plan §§5, 15 | Stages 1, 4 |
| Parallel results use associative/commutative/idempotent conflict reducers | Plan §4.2 | Stages 1, 3–4 |
| Parallel workers never mutate authoritative stage projection | Plan §4.2 | Stage 4 |
| GoalDirected is deterministic outer graph plus bounded agent/verifier | D-04; plan §6 | Stage 5 |
| Top-level state contains refs/digests, not full transcripts or corpora | D-10; plan §§4, 6 | Stages 1, 4–6 |
| `update_state` invokes reducers; replacement requires privileged `Overwrite` | Plan §4.4; persistence skill | Stages 1, 3, 7 |
| Stable node/channel/reducer/interrupt names are compatibility surfaces | D-16; plan §§8.5, 10.5 | Stages 0–3, 8 |
| Checkpoint-incompatible changes use blue/green endpoint binding | Plan §§15, 17 | Stages 0, 3, 8 |
| Every stage/variant binds one exact capability requirement and operation assembly | D-18; `02A`; `06A` §4 | Stages 1, 4–7 |
| Generic StageGraph schedules opaque operation adapters and never embeds provider mechanics | D-19; `06A` §§2–3 | Stages 4–6 |
| Stable Deep Agent adapter is shared by StageGraph and GoalDirected | D-22; `06A` §2 | Stage 5 |
| Ordinary concurrency and optimistic/speculative execution remain distinct | D-20; `06A` §§6–7 | Stages 3–6 |
| Speculation is default-off and limited to published pure/read-only policies with quarantine/commit barrier | D-20; `06A` §7 | Stages 4–6 |

## 3. Agent harness and context traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Operation binding constructs the harness; no loose runtime authority | Controlled-run §2.3 | Stages 1, 5–6 |
| Middleware order is exact contract data and duplicate/conflict validated | Plan §7.1; middleware skill | Stages 1, 5–6 |
| Deep Agents core middleware is configured, not reimplemented/duplicated | Deep Agents core skill; plan §6.2 | Stage 5 |
| All I/O tools/middleware/factories are native async | D-12; plan §13.5 | Stages 0–8 |
| Context policy and immutable assembly spec are first-class | D-14; plan §7.3 | Stages 1, 5–6 |
| Exact instructions, protected goals, evidence/citation edges, approvals, attempts, and digests are never model-summarized away | Plan §7.3 | Stages 5–7 |
| Context reconstruction verifies source digests after compaction/rollover | Plan §7.3 | Stages 5–6 |
| Store memory is purpose/tenant namespaced, non-authoritative, default-deny for scientific claims | Round-two context; plan §3.4 | Stages 1, 5–7 |
| Agent skills use progressive disclosure and exact reviewed refs | Controlled-run §6.3; Deep Agents skills docs | Stages 1, 5–6 |
| Custom subagents receive explicit skills; no implicit skill inheritance | Controlled-run §§6.3, 7.2 | Stages 5–6 |
| Filesystem search/runtime tool overlap is compiled to one clear capability surface | Controlled-run §6.1 | Stages 5–6 |
| New implementations may select different exact models/providers from the legacy path | D-17; `02A` | Stages 1, 5–8 |
| Parity evaluates BellLabs contracts/evidence/results and accepted semantic thresholds, not provider/token/trace equality | D-17; `02A` | Stages 4–8 |
| Structural compiler precedes Stage 4; stable runtime compiler precedes StageGraph/GoalDirected harness use; Stage 6 only extends it | D-21; `06A` §5 | Stages 1, 5–6 |
| Compiler predicts exact per-stage model-visible and runtime-visible surface | D-18/D-21; `06A` §§4–5 | Stages 5–7 |

## 4. Delegation traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Sync dictionary `SubAgent` and `CompiledSubAgent` remain distinct construction forms | Controlled-run §7.2 | Stages 1, 5 |
| Default general-purpose child is disabled unless explicitly selected | Controlled-run §7.2 | Stage 5 |
| Synchronous, dynamic-interpreter, async, and linked-run continuity are distinct | D-15; plan §8.2 | Stages 1, 5–6 |
| Each child receives a bounded `ContextSlice` and returns a typed result manifest | Plan §7.3; controlled-run §7.6 | Stages 1, 5–6 |
| A child cannot terminalize or widen the parent | Plan §8.2 | Stages 5–6 |
| Known Workflow Type/authority boundary forces a linked run | Controlled-run §5.6; coordinator skill | Stages 5–7 |
| Async task ID, thread ID, and current run ID remain separately typed | Controlled-run §7.3 | Stages 1 and 6 |
| Async tasks use fresh status, durable bindings, wait/resume, update/cancel/reconcile | Async docs; plan §8.2 | Stage 6 |
| Parent/child capacity reserves supervisor/resumption plus child slots | Async docs; plan §15 Phase 7 | Stage 6 |
| Async preview is a required implementation/qualification track and remains default-off until its Stage 6 promotion gate passes | Accepted Stage 0 O-OPTIONAL/D-15; `02A` | Stages 0, 3, 6 |
| Async parent stage transitions through durable wait/reconcile and releases its worker according to exact lease policy | `06A` §§6.3, 8; `09A` | Stages 3 and 6 |

## 5. MCP, interpreter, sandbox, and snapshot traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Inbound coordinator MCP, outbound operation MCP, and Agent Server protocols remain distinct | Plan §8.1; controlled-run §6.2 | Stages 6–7 |
| Outbound MCP discovery occurs outside model context and observed schemas match frozen digests | Plan §8.1 | Stage 6 |
| Each MCP tool is wrapped for auth, approval, idempotency, budget, retry, cancellation, trace | Plan §8.1 | Stage 6 |
| Streamable HTTP is production default; SSE legacy; stdio local/sandbox only | Plan §8.1 | Stages 0, 6, 8 |
| Explicit stateful MCP sessions are operation/stage scoped and closed reliably | Plan §8.1 | Stage 6 |
| MCP elicitation maps to durable BellLabs decision plus interrupt | Plan §8.1 | Stages 3 and 6 |
| QuickJS is separate from OS sandbox and defaults to explicit `call` mode | Plan §§8.3–8.4; interpreter docs | Stage 6 |
| QuickJS has exact source digest and CPU/time/memory/output/call/fan-out bounds | Plan §8.3 | Stage 6 |
| Programmatic tool calls and `task()` cannot bypass independent guards | Round-two/controlled-run §7.5 | Stage 6 |
| Interpreter `turn`/`thread` persistence is separately qualified | Controlled-run §7.4 | Stage 6 |
| LangSmith Sandbox is first adapter behind provider-neutral port | Migration recommendations sandbox decision | Stages 5–6 |
| Sandbox scopes, egress, secrets, mounts, limits, snapshots, cleanup, usage are exact | Plan §8.4 | Stages 5–8 |
| Four snapshot concepts remain qualified and distinct | Controlled-run §6.5 | Stages 1, 3, 5–6 |
| Sandbox restore clones and reacquires credentials/leases/sessions | Controlled-run §6.5 | Stages 5–6 |

## 5A. Resource, lineage, and heterogeneous composition traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Hierarchical resource envelope distinguishes run/stage/worker/model/tool/MCP/sync-child/async-child/linked-run limits | D-20; `06A` §4.4 | Stages 3–6 |
| Reservation precedes fan-out, acquisition order prevents deadlock, and resumption capacity is protected | `06A` §6 | Stages 3–6 |
| Barriers/controlled clocks prove real overlap and maximum observed concurrency | `06A` §6.4 | Stages 4–6 |
| One canonical lineage spans run/epoch/stage/semantic attempt/runtime attempt/agent/effect/child/artifact/settlement/trace | D-23; `06A` §4.5 | Stages 3–7 |
| Final typed result is queryable back to every contributing exact assembly and accepted evidence item | D-23; `06A` §11 | Stages 3–7 |
| Task, child thread, child run, Agent Server run, operation attempt, and BellLabs run identities remain non-interchangeable | D-05/D-16/D-23; `06A` §4.5 | Stages 1, 3, 6–7 |
| Heterogeneous StageGraph proves differently assembled stages run concurrently without capability leakage | D-18/D-23; `09A` | Stage 6 |
| Runtime drift cannot silently substitute model/tool/MCP/skill/sandbox/async target | `06A` §9 | Stages 3–8 |

## 6. Agent Server and deployment traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| Standard Agent Server, not Managed Deep Agents, is primary | D-01; managed skill decision table | Stages 0, 2, 8 |
| Graph exports are import side-effect free | Plan §13.4 | Stage 2 |
| Managed deployment injects checkpointer/Store; graph export does not double-configure | D-07; plan §13.3 | Stages 0, 2–3 |
| Runtime graph factory distinguishes execution from `threads.update`, `threads.read`, `assistants.read` | Current graph-rebuild docs; D-11 | Stages 0, 2, 5–6 |
| Introspection creates no sandbox, MCP session, secret resolution, budget reservation, or mutation | Round-two finding 1 | Stages 0 and 2 |
| Per-run resources use async context manager and guaranteed cleanup | Graph rebuild/production docs | Stages 0, 2, 5–6 |
| Custom auth protects threads/runs/assistants/Store/crons/custom routes by tenant | Plan §11.1 | Stages 2, 7–8 |
| Custom routes do not shadow Agent Server defaults | Plan §§2.2, 13.3 | Stage 2 |
| One codebase supports standalone FastAPI coexistence and Agent Server HTTP app | D-06 | Stages 2 and 7 |
| Release jobs own migrations; runtime credentials are non-owner | Plan §13.4 | Stages 1–2, 8 |
| Deployment build excludes personal/scratch/legacy-only assets | Plan §13.4 | Stages 2, 7–8 |
| Deployment ownership path is consistently CLI or GitHub/UI | Plan Phase 10; current deploy docs | Stages 0 and 8 |
| Serverless limits/entitlements are measured; Dedicated is evidence-driven | Round-two; deploy docs | Stages 0, 7–8 |

## 7. API, coordinator, observability, and evaluation traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| v1 remains compatible through coexistence; v2 does not duplicate domain services | Plan §11 | Stage 7 |
| REST, coordinator MCP, and custom Agent Server routes share principal mapper/facade/errors | Plan §§11.1, 11.5 | Stages 2 and 7 |
| Execution launch goes through transactional outbox dispatcher | Plan §11.3 | Stages 1, 3, 7 |
| Stream events carry a monotonic BellLabs outbox cursor | Plan §14.1 | Stages 3 and 7 |
| Coordinator sees granular capability/maturity facts, not vendor classes | Plan §20 | Stages 1, 6–7 |
| Prepare freezes harness/context/delegation/assembly digests | Controlled-run §5.9; plan §20 | Stages 1 and 7 |
| Trace hierarchy includes exact BellLabs/runtime correlations and retry layer | Plan §14.1 | Stages 2, 4–8 |
| Secrets/PHI/raw private corpora and unrestricted outputs are redacted | Plan §14.1 | Stages 2, 5–8 |
| Evaluator implementation begins only after inspecting actual output/trace shape | LangSmith evaluator skills | Stage 7 |
| One metric per evaluator; deterministic invariants use code evaluators | LangSmith evaluator skills; plan §14.2 | Stage 7 |
| Offline evaluation gates release; sampled online evaluation monitors production | Evaluation concepts; plan §14.2 | Stages 7–8 |
| LangSmith scores inform but cannot publish/terminalize | Migration recommendations evaluation | Stages 7–8 |

## 8. Migration and operational traceability

| Requirement | Primary source | Implemented/verified in |
|---|---|---|
| No broad database reset; isolated schemas/roles and restorable backup first | Recommendations safe reset; plan non-goals | Stages 0–1, 8 |
| Mongo operation-authority backfill is digest-verified and rollbackable | Plan §10.3 | Stage 1 |
| Health distinguishes liveness, dependency readiness, capability readiness, degradation | Plan §14.3 | Stages 2 and 7 |
| Production-like build verifies no local host-only dependency | Plan Phase 9 | Stage 7 |
| Shadow runtime never owns consequential provider-effect claim | Plan Phase 11 | Stages 4 and 8 |
| In-flight runs remain bound to original blue/green endpoint | Plan §§15, 17 | Stages 3 and 8 |
| Rollback routes new admissions and never deletes authority/evidence | Plan §17 | Stage 8 |
| Legacy runtime removed only after zero active runs and reconciliation | Plan Phase 12 | Stage 8 |

## 9. Cross-stage audit rule

At each stage handoff, review every row assigned to that stage and record one of:

- `implemented_and_proven`;
- `contracted_for_later_stage`;
- `deferred_optional_feature_disabled`;
- `owner_amended` with a decision link;
- `failed_gate`.

No row may disappear or be marked “not applicable” without an owner decision explaining the architecture change.
