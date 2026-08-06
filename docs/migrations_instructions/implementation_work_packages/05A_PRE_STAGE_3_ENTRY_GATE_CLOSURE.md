# Pre-Stage 3 entry-gate closure — exact isolated prerequisite task

Status: required focused prerequisite before Stage 3 implementation  
Scope: close only the Stage 0–2 blockers that can invalidate or undermine the Stage 3 durable runtime kernel  
Output: `docs/migrations_instructions/stage2_evidence/PRE_STAGE_3_ENTRY_HANDOFF.md`

## 1. Mission

Produce one compact, evidence-backed handoff that lets the Stage 3 implementation agent begin without loading the Stage 0–2 research and implementation history.

This task is not Stage 3 implementation. It must:

1. apply the D-17–D-23 compatibility amendment to the Stage 1/2 contracts;
2. complete the isolated database authority proofs on which Stage 3 dispatch, claims, attempts, settlements, leases, and reconciliation depend;
3. qualify the exact pinned Agent Server/LangGraph persistence, restart, interrupt, fork, concurrent-run, and checkpoint-compatibility mechanics that Stage 3 will wrap;
4. record the Stage 3 policy decisions that otherwise force the Stage 3 agent to rediscover earlier context;
5. explicitly defer unrelated Stage 0–2 external evidence to its true later owner;
6. write a compact accepted/rework handoff containing only facts and contracts Stage 3 needs.

Do not implement StageGraph scheduling, GoalDirected, Deep Agent harnesses, MCP, skills, Store memory, sandboxes, async subagents, QuickJS, tracing evaluators, deployment, or cutover.

## 2. Read set for this prerequisite agent

Read in full:

- `implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md`;
- `implementation_work_packages/01_GLOBAL_HANDOFF_AND_STAGE_GATE_RULES.md`;
- `implementation_work_packages/02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md`;
- `implementation_work_packages/04_STAGE_1_RUNTIME_NEUTRAL_CONTRACTS_AND_OPERATION_JOURNAL.md`;
- `implementation_work_packages/05_STAGE_2_AGENT_SERVER_FOUNDATION.md`;
- `implementation_work_packages/06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md`;
- `stage0_evidence/01_DECISION_PACKAGE.md` and `04_REQUIREMENTS_AND_QUALIFICATION_EVIDENCE.md`;
- `stage1_evidence/STAGE_1_HANDOFF.md`;
- `stage2_evidence/STAGE_2_HANDOFF.md`;
- the exact current code, migrations, tests, and lock named below.

Preserve unrelated dirty-worktree changes. Keep `biotech-meta` read-only. Never use primary Supabase, primary Atlas, production credentials, or a shared database for destructive/clean-apply tests.

## 3. Block A — targeted Stage 1/2 contract amendment

This block is mandatory before any Stage 3 coding.

### 3.1 Inspect current contracts

Inspect at minimum:

- `app/domain/graph_runtime/definitions.py`;
- `app/domain/graph_runtime/contracts.py` and `identities.py`;
- `app/domain/graph_runtime/governance.py`;
- `app/domain/operation_execution/contracts.py` and `journal.py`;
- `app/application/runtime_run_plan.py`;
- `app/application/runtime_execution_bindings.py`;
- `app/application/graph_runtime_dispatch.py`;
- `app/application/runtime_interventions.py` and `runtime_reconciliation.py`;
- `app/api/graph_runtime_schemas.py`;
- `app/agent_server/graph_factory.py`, graph state, reducers, nodes, and `langgraph.json`;
- migrations `0012` and `0013`;
- graph-runtime, dispatch, journal, migration, and Stage 2 tests.

### 3.2 Implement the compatibility amendment

Version or extend the existing contracts without mutating published content/digests. Implement the exact semantics in `06A` for:

- `StageCapabilityRequirement`;
- `OperationAssemblySpec`;
- `StageExecutionBinding`;
- `ExecutionResourceEnvelope`;
- `ExecutionLineageEnvelope`;
- shared typed failure taxonomy;
- discriminated `OperationExecutionOutcome`;
- Stage 1 structural compiler with complete stage/variant coverage, exact-ref/digest validation, authority/maturity intersection, disabled-feature handling, compatibility validation, and predicted unavailable surfaces.

Required decisions:

- if the current v1 schema is insufficient, publish v2; do not reinterpret v1 fields;
- preserve old ERC/RunPlan deserialization or produce a typed, tested incompatibility/migration result;
- define the deterministic resolution rule between global RunPlan defaults and per-stage overrides, or remove ambiguous global defaults from the new version;
- assess and version Stage 2 state/node/reducer/introspection schemas only where the new generic operation/wait channels require it;
- preserve import/introspection side-effect freedom;
- do not construct models, MCP sessions, sandboxes, Store clients, secrets, or external resources during compilation or inspection.

### 3.3 Required contract evidence

Add/update tests proving:

- every stage and variant has exactly one requirement and execution binding;
- different stages can bind different exact models/capability surfaces;
- duplicate/missing bindings and digest drift fail closed;
- no mutable alias, installed package, runtime preference, or provider discovery grants authority;
- old schema behavior remains readable or fails through the documented versioned path;
- schema export and graph introspection contain the amended contracts without side effects;
- Stage 2 stable graph IDs/nodes remain compatible or have an explicit compatibility-version migration.

Run at minimum:

```powershell
uv run pytest -q tests/test_graph_runtime_contracts.py tests/test_graph_runtime_dispatch.py tests/test_agent_server_stage2.py
uv run ruff check app tests
uv run mypy app
```

Record exact command output and the new schema/compatibility digests.

## 4. Block B — isolated PostgreSQL/Mongo authority proof

This block is mandatory before Stage 3 can rely on the Stage 1 journal and runtime binding foundation.

### 4.1 Choose and record disposable topology

Recommended topology:

- Docker Compose `application-postgres` for disposable local/integration PostgreSQL;
- a dedicated disposable MongoDB database/cluster and database name supplied only through `TEST_MONGODB_URI`;
- distinct migration-owner and runtime-role behavior tested in PostgreSQL;
- primary Supabase and primary Atlas explicitly denied as targets.

The relevant integration tests drop/recreate `belllabs_control`. Resolve and record the literal PostgreSQL host/database before running them. Stop if it is not the disposable target.

### 4.2 Bring up and verify the disposable PostgreSQL target

From the repository root:

```powershell
docker compose ps --all
docker compose up -d application-postgres
docker compose exec application-postgres pg_isready -U belllabs -d belllabs
```

Configure the test-only DSNs in the process environment or approved secret store; never commit them:

```powershell
$env:TEST_APPLICATION_POSTGRES_DSN='<literal disposable application PostgreSQL DSN>'
$env:TEST_MONGODB_URI='<literal isolated test MongoDB URI/database>'
```

Print only sanitized host/database/role facts, never passwords/tokens.

### 4.3 Required database proofs

Prove on real disposable services:

- migrations `0012` and `0013` clean-apply from empty state;
- upgrade from the accepted preceding migration state;
- migration idempotency/release-path behavior and documented rollback boundary;
- non-owner runtime role and RLS/grants, including cross-tenant denial;
- atomic rollback on injected failure before commit;
- claim/attempt/settlement/outbox idempotency and conflicting-replay rejection;
- concurrent lifecycle/version conflict behavior;
- immutable Mongo semantic binding digest/reference integrity;
- legacy Mongo-to-PostgreSQL backfill count/digest verification;
- malformed/conflicting source quarantine;
- crash/restart and resume of the backfill cursor;
- rollback-window read routing without deleting either authority;
- no live primary data modified.

Run the existing integration coverage at minimum:

```powershell
uv run pytest -q tests/test_run_control_postgres_integration.py tests/test_operation_journal_stage1.py tests/test_artifact_promotion_postgres_integration.py
uv run pytest -q tests/test_operation_execution_mongodb_integration.py tests/test_artifact_promotion_mongodb_integration.py tests/test_control_plane_mongodb_integration.py
uv run pytest -q tests/test_operation_journal_backfill.py
```

If the existing tests do not exercise real cross-store backfill, RLS/grants, crash/resume, or rollback routing, add the missing isolated integration tests. A skipped test, static SQL assertion, in-memory repository, read-only ping, or unit crash simulation is not acceptance evidence for this block.

## 5. Block C — pinned Stage 3 runtime-mechanics qualification

This block is mandatory before committing to the Stage 3 implementation topology. Keep the code in a disposable qualification fixture or narrowly scoped integration test; do not implement BellLabs Stage 3 services here.

Using the exact root lock and either a standalone persistent Agent Server fixture or an authorized managed test deployment, prove:

1. **Persistence ownership:** managed deployment injects persistence, while standalone tests construct one async PostgreSQL saver/Store in application lifespan; no per-invocation saver/Store.
2. **Real process restart:** start a thread/run, persist a checkpoint/interrupt, terminate the server process, start a new process against the same persistence, resume the same thread, and observe one accepted continuation.
3. **Interrupt semantics:** code before `interrupt()` restarts; a stable durable decision/claim prevents duplicate consequential work; duplicate resume is idempotent or conflicts deterministically.
4. **Parallel interrupts:** runtime interrupt IDs remain mapped to distinct BellLabs decision IDs.
5. **Concurrent-run strategies:** verify the exact pinned API behavior for reject/enqueue/interrupt/cancel strategies and record which strategies Stage 3 may call.
6. **Fork mechanics:** verify checkpoint inspection and fork/thread APIs; a fork creates distinct identities and cannot mutate the parent.
7. **Checkpoint compatibility:** N-on-N resume passes; N checkpoint after N+1 deployment routes to N or fails safely according to the accepted blue/green policy; incompatible reducer/channel/node/interrupt changes never resume accidentally.
8. **Cancellation/cleanup:** cancellation during a waiting/model/tool/DB/stream-shaped fixture closes resources and leaves a reconcilable typed state.
9. **Tenant isolation:** cross-scope state/history/checkpoint/interrupt/fork access is denied.
10. **Introspection safety:** state/history/schema/graph reads create no credentials, sessions, leases, sandboxes, Store writes, or mutations.

The evidence must name exact package versions, server mode, persistence provider, commands, process IDs/restarts, thread/run/checkpoint IDs in redacted form, observed API behavior, and cleanup result.

An in-memory saver, same-process resume, import-only API check, or mocked client is insufficient for process-restart and compatibility acceptance.

## 6. Block D — record Stage 3 policy decisions

Resolve these before handing work to the Stage 3 model. Use the recommended default unless the owner explicitly chooses otherwise:

| Decision | Recommended default |
|---|---|
| Active-run intervention | Reject by default; allow typed enqueue only for Workflow Implementations that declare it |
| Interrupt response | Persist BellLabs decision first, then resume same thread with decision ID/digest |
| Arbitrary state update | Denied; privileged `Overwrite` only with actor, reason, expected BellLabs version/checkpoint, and audit |
| Retry versus fork | Technical retry retains semantic identity; fork creates new BellLabs run/thread/budget/lineage |
| Epoch rollover | Disabled unless an exact compatible handoff/rollover policy is published |
| Cancellation | Cooperative cascade by default; independently authoritative linked runs require their own accepted cancellation command |
| Durable waits | Release operation worker; retain/release only explicitly declared leases; preserve resumption capacity |
| Event reconnect | BellLabs outbox cursor is durable; Agent Server stream cursor is transient detail |
| Checkpoint visibility | Redacted summaries by default; debug/value/history restricted to operator role |
| Reconciliation | Automatic only for idempotent, version-checked repairs; ambiguous effects, incompatible checkpoints, and identity conflicts require operator decision |
| Retention for Stage 3 | Record an explicit interim checkpoint/event/incident/lineage retention and deletion policy; later trace/Store/sandbox policies may remain separately blocked |
| Blue/green | Running epochs stay on their exact compatible endpoint/assembly; no revision-only routing |

Record actor, date, scope, alternatives, and revisit trigger. Do not make the Stage 3 agent reopen these decisions unless new evidence contradicts them.

## 7. Explicitly not blocking Stage 3 entry

Do not spend this prerequisite task on the following. Carry them as named later-stage blockers:

- live LangSmith trace arrival and evaluator construction — Stage 7;
- interactive Studio UI inspection — Stage 7/8 operator validation;
- final production image/deployment and Cloud rollout — Stage 7/8;
- final Serverless/Dedicated region/quota/cost/SLO decision — Stage 7/8, except any hard limit needed for the Stage 3 runtime-mechanics qualification;
- outbound MCP auth/schema/elicitation/session qualification — Stage 6;
- Agent Server Store scientific/procedural memory behavior — Stage 5/6;
- LangSmith Sandbox entitlement/egress/snapshot qualification — Stage 5/6;
- Deep Agents middleware, skills, context, model, and sync-subagent qualification — Stage 5;
- async-subagent lifecycle — required Stage 6 track;
- QuickJS/PTC/dynamic delegation — optional disabled Stage 6 track;
- field-level trace anonymization beyond the existing fail-closed hidden-I/O posture — Stage 7;
- staging issuer, production secrets, canary, cutover, or legacy drain — Stage 7/8.

These deferrals do not authorize the corresponding feature. Flags remain off and Workflow Implementations requiring them remain unavailable.

## 8. Completion gate and compact handoff

Create `docs/migrations_instructions/stage2_evidence/PRE_STAGE_3_ENTRY_HANDOFF.md` with no copied research narrative. It must contain only:

```text
status: ACCEPTED | REWORK_REQUIRED
prepared_at / actor / base revision / diff ref

contract amendment:
  schema versions and digests
  compatibility/migration result
  Stage 2 graph/introspection impact
  exact tests and results

database authority proof:
  sanitized disposable topology
  migrations tested
  RLS/grant/tenant evidence
  crash/idempotency/backfill/quarantine/rollback results
  exact tests and results

runtime mechanics qualification:
  exact versions/server mode/persistence
  restart/interrupt/parallel interrupt/concurrent strategy/fork evidence
  N/N+1 compatibility result
  cancellation/tenant/introspection result

accepted Stage 3 policy decisions:
  decision table with actor/date

deferred non-entry blockers:
  item -> owning stage -> disabled flag/fallback

Stage 3 entry:
  all required rows pass: yes | no
  remaining blocker, if any
  exact Stage 3-compatible schema/endpoint/fixture instructions
```

The handoff status is `ACCEPTED` only when Blocks A–D all pass with non-skipped evidence. Otherwise use `REWORK_REQUIRED`; do not soften missing evidence into an assumption.

After acceptance, the Stage 3 model reads only:

1. `00_MAIN_GOAL_AND_INDEX.md`;
2. `01_GLOBAL_HANDOFF_AND_STAGE_GATE_RULES.md`;
3. `06_STAGE_3_DURABILITY_HITL_STEERING_AND_RECOVERY.md`;
4. `06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md`;
5. `stage2_evidence/PRE_STAGE_3_ENTRY_HANDOFF.md`;
6. current target code/tests named by those documents.

It does not need to load the full Stage 0–2 evidence unless the compact handoff reports a contradiction or missing fact.

