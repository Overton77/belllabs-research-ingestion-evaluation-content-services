# Research runtime implementation audit

Recorded: 2026-10-01 (America/New_York)
Audited base: `f73a0e15a9411de3b184ed16d75a8dc3acf140e0`, plus the pre-existing dirty checkout described below
Disposition: implementation audit complete; prerequisite implementation and acceptance remain open

## Delivery boundary

The user will run the Qualia Life and GenerationLab missions in a **separate later session**. Prerequisite agents must stop after implementation, technical qualification, review, merge, and handoff. They must not create or launch those company fixtures, run their forks, or auto-start a subsequent fixture session. Technical unit/integration inputs, replay, failure injection, and explicitly gated provider qualifications are allowed to prove prerequisite tickets.

The final prerequisite gate is `ready_for_separate_fixture_session`, not company-mission completion or full CP-050 acceptance. The future company missions and aggregate CP-050 acceptance remain held. No company research mission was launched during this audit. Existing offline regression tests that mention companies ran as part of the normal suite; they are not live research missions.

## Authority and acceptance status

- CP-001/010/020/030/040/045 and BP-010/020 are recorded accepted in the v2 package index/evidence. These dispositions are historical package acceptance, not a claim that today's full checkout is green.
- CP-050 has not been accepted and has no executable evidence directory. Its existing spec/package requires a complete tracer through both families and capabilities.
- `SPEC-CP-DURABLE-EXECUTION` is canonical. Existing requirements cover stable identities (EXEC-005), durable message application (EXEC-006/007), cancellation reconciliation (EXEC-008), continuation (EXEC-011), and semantic snapshots/forks (EXEC-012).
- `SPEC-CP-RUN-CONTROL` is canonical and owns lifecycle/terminality; `SPEC-CP-DEEP-AGENT-RUNTIME` is canonical and owns exact materialization/workspaces/artifacts.
- `SPEC-CP-COGNITIVE-SCHEMAS` is **draft** and ADR-0004 is **proposed**. Code containing schema digests does not make those documents accepted authority. Ticket RRM-001 must resolve the authority needed for new digest/restore contracts.
- The local lifecycle brief is a design/implementation brief pending canonical specification/ticket acceptance. It must not be treated as a competing spec.

## Current implementation: verified code locations and gaps

| Concern | Current code/evidence | Audit finding | Owning follow-on |
|---|---|---|---|
| Macro runtime | `app/temporal/registration/workflows.py`; root/family/operation modules | Both family workflows and generic operations are registered; preserve one macro runtime | All tickets |
| StageGraph | `app/domain/orchestration/interpreter.py`, `app/temporal/workflows/stagegraph.py`, BP-010 evidence | Pure semantic owner; incremental child completion, joins, cycles, waits and liabilities exist | Regression protection; RRM-007 |
| GoalDirected | `app/domain/orchestration/goal_directed.py`, `goal_directed_runtime.py`, family workflow, BP-020 evidence | Executor/verifier isolation and handoff exist; paused workflow raises non-retryable `goal_paused` rather than durably waiting for resume | RRM-007 |
| Lifecycle authority | `app/domain/run_control/reducer.py`, application run-control service/repository, `app/api/run_control.py` | Authoritative phase/budget/effect/terminal commands exist; transport acceptance must be distinguished from execution application | RRM-007/008 |
| Deep Agent checkpoint use | `app/integrations/agents/deep_agents/adapter.py:79` | Only deterministic `thread_id` supplied; state read before/after `ainvoke`, but resulting checkpoint config is not captured or returned | RRM-003 |
| Result contracts | `app/domain/operation_execution/contracts.py`, `RuntimeResult` and `OperationExecutionResult` | No exact checkpoint identity carried in these production result contracts | RRM-001/003 |
| Technical attempts | `app/temporal/operation_activities.py` | No `activity.info().attempt` capture or production cognitive heartbeat in this entry path | RRM-003/008 |
| Retry/claim recovery | `app/application/operations/operation_execution.py:360` | Settled binding returns prior result; acquired durable claim prevents duplicates; unresolved claim raises `OperationExecutionInProgress`. No observed-checkpoint-to-settlement recovery in this path | RRM-004 |
| Operation wrapper | `app/temporal/workflows/operation.py` | Three technical Activity attempts; cancel signal only sets flag checked before `execute_activity`. This flag alone does not interrupt active cognition | RRM-004/008 |
| Runtime identities/storage | `app/domain/graph_runtime/identities.py`; runtime repositories; migrations 0012/0014 | Binding/attempt/checkpoint/intervention/lineage/incident/fork storage already exists; several contracts are Agent Server-shaped. `LangGraphCheckpointKey` lacks explicit namespace | RRM-001/003 |
| Unit and macro snapshot contracts | Repository searches for `RuntimeUnitIdentityV1` / `RunSnapshotManifest` | No implementation found in `app/`; existing semantic attempt/continuity contracts are useful prior art, not the complete proposed contracts | RRM-001/003/006 |
| Persistence | `app/integrations/langgraph_persistence.py`, exact materializer | Postgres saver/store lifespan exists; materializer resolves explicit registered checkpointers. Worker registration alone does not prove durable composition | RRM-003/009 |
| Inspection API | `app/api/graph_runtime_schemas.py`, included by server | `/v2/graph-runtime/schemas` exports contracts. Operational unit/history/fork routes from the brief are not implemented here | RRM-005/006 |
| Temporal visibility | Searches for Search Attributes in production code | Proposed BellLabs runtime-unit attributes not implemented | RRM-005 |
| Fork machinery | `app/application/runtime/runtime_recovery.py:365`, `PostgresForkRepository` | Admission/copy saga and idempotency/recovery behavior exist and have unit coverage. Local Deep Agent/safe macro snapshot wiring is incomplete | RRM-006 |
| Root messages | `app/temporal/workflows/belllabs_run.py` | Receipts maintained in workflow state; accepted cancel message sets flag. No generic active-family/cognition forwarding from this handler; accepted receipt is not an applied receipt | RRM-007 |
| Family controls | StageGraph wait/resume/cancel signals; GoalDirected cancel signal | Hooks exist; prove application-facade authorization, durable routing and application acknowledgement. Avoid raw-signal public bypass | RRM-007/008 |
| Sandbox snapshots | `app/application/workspaces/sandbox_snapshots.py` | Immutable compute/workspace clone semantics exist. This is distinct from macro semantic run snapshots | RRM-006/009 |
| Worker deployment | `app/temporal/worker.py:88` | Deployment factory protocol and refusal guard exist; no production factory implementation found in `app/` by symbol search | RRM-009 |
| Skills/MCP/sandbox | Exact materializer; Docker sandbox; CP-040/045 evidence | Capability mechanisms exist; network-disabled Docker placement cannot be assumed to support live browser/search. Async spawn is feature-gated | RRM-009 |

BP live test code uses in-memory run-control/checkpoint components for parts of qualification. It proves real model/family behavior; it does not substitute for the mission's shared real persistence, production worker composition, or cross-store crash recovery proof.

## Fresh verification on the audited checkout

Commands used `.venv/Scripts/python.exe`; no dependency installation or application code change was made. Provider live flags `BELLABS_RUN_WP_BP_010_LIVE`, `BELLABS_RUN_WP_BP_020_LIVE`, and `BELLABS_RUN_WP_CP_040_LIVE` were set to `0`; LangSmith tracing was disabled for these checks. Service/credential/platform tests retain their normal gates. No .env contents were printed.

| Check | Actual result |
|---|---|
| `python -m mypy app` | Pass: no issues in 340 source files |
| `python -m ruff check app tests scripts --output-format concise` | Fail: 104 errors; 55 marked fixable |
| `python -m pytest -q --tb=no` | 44 failed, 645 passed, 44 skipped, 926 warnings; 96.79 seconds |
| Focused CP-020/030/040/045 plus runtime recovery/intervention/bootstrap and both pure family suites | 1 failed, 141 passed; 20.80 seconds |
| Six representative failures rerun with short tracebacks | All reproduced; causes below |

Focused command inputs:

```text
tests/acceptance/control_plane/test_wp_cp_020.py
tests/acceptance/control_plane/test_wp_cp_030.py
tests/acceptance/control_plane/test_wp_cp_040.py
tests/acceptance/control_plane/test_wp_cp_045.py
tests/unit/runtime/test_runtime_recovery_stage3.py
tests/unit/runtime/test_runtime_interventions_stage3.py
tests/unit/runtime/test_runtime_bootstrap_stage3.py
tests/unit/orchestration/test_stagegraph_v2.py
tests/unit/orchestration/test_wp_bp_020_goal_directed.py
```

### Diagnosed baseline failures

| Reproduced failure | Observed cause | Repair constraint |
|---|---|---|
| CP-020 domain-authority import guard | Reads moved `app/application/journaled_operation_execution.py`; actual owner is under `operations/` | Restore guard against actual owner; do not remove the architecture assertion |
| Atomic family admission migration guard | Resolves `tests/unit/app/migrations/...` rather than repository migration | Correct repository-root lookup; retain DML/authority assertions |
| Reviewed capability payload copy | Resolves nonexistent `tests/unit/app/domain/coordinator/reviewed_payloads` | Fix path/provenance; do not invent reviewed payloads |
| Quarantine scanner qualification | Resolves nonexistent `tests/unit/scripts/quarantine_static_scan.py` | Correct path while retaining nonexecution/quarantine checks |
| Sandbox tamper/clone tests | Snapshot retention expires before the intended test assertion | Make time deterministic/inject a test clock; do not weaken production expiry or tamper checks |
| Control-plane publish test | Expected 201, actual 422 | Diagnose strict payload/schema mismatch before changing expected status or validation |

Other failing groups include Agent Server configuration, experiment PostgreSQL, coordinator bindings/promotion, reference-research source roots, schema authority/catalog/workspace, live-runner path checks, and snapshot restore. Their node IDs are captured in the tracked baseline findings file. They were not individually diagnosed; RRM-002 must classify each, not assert that all failures are merely stale paths. A skip does not prove real-service availability. Deployment/service readiness was not established by this audit.

Ruff findings are 55 import-order violations and 49 line-length violations across current owners and tests. Preserve user-owned dirty changes; avoid a blanket formatter/staging pass that absorbs them. The [sanitized baseline findings](migrations_instructions/implementation_work_packages_v2/research-runtime-mission/BASELINE_FINDINGS_2026-10-01.txt) preserve all 44 failing node IDs and these counts without raw logs.

## Dirty-work ownership and worktree readiness

Pre-existing modified tracked paths:

- `app/api/control_plane.py`: owner notes/comments and typing edits; contains existing trailing whitespace.
- `app/application/control_plane/service.py`: TypeAdapter annotation edit.
- `app/application/runtime/postgres_runtime_execution_repository.py`: TypeAdapter annotation edit.

Pre-existing untracked work: `app/{application,domain,integrations}/agentic_components/`, `tests/unit/agentic_components/`, `scripts/query_agentic_components.py`, `docs/AGENTIC_COMPONENTS_HARNESS.md`, domain/runtime walkthroughs, `docs/runtime_lifecycle_doc.md`, and the lifecycle implementation brief.

The agentic-component work has query/materialization planning and filesystem repository seams, but it is uncommitted and was not independently accepted by this audit. A worktree created from a commit will not contain it. Record explicit ownership and a reviewed dependency commit before consuming it. Do not silently commit it as part of baseline repair or call it production composition.

Audit documentation/tickets are committed separately. Existing modified/untracked application work remains untouched. Raw test logs remain local under `.scratch/research-runtime-mission/audit/`; only sanitized findings are tracked.

## Contract audit disposition

- Reuse: run-control authority and accepted family interpreters; operation binding/journal/effect idempotency; snapshot/artifact services where their semantics match; fork admission/copy recovery and incident/lineage concepts.
- Version or deliberately replace under accepted spec: Agent Server-shaped checkpoint/binding identities, checkpoint namespace/ancestry metadata, stable runtime-unit correlation, attempt observations, result manifest/checkpoint linkage, operation-local messaging/application receipts.
- Define before implementation: fenced cross-store recovery and terminal result reconstruction; safe macro snapshot/quiescence/reuse frontier; projection/read contracts and freshness; public boundary-command routing and GoalDirected durable pause/resume compatibility.
- Preserve as inert provenance where appropriate: obsolete runtime migration tables and historical package evidence. Do not create a competing scheduler or rewrite historical acceptance to hide today's drift.
- Deferred: arbitrary mid-invocation cognitive edits and unrestricted executable forks from arbitrary graph nodes. Scoped historical reads and qualified semantic-boundary forks remain required.

## Next frontier

M0 implementation audit and local ticket publication are complete. They do not include acceptance of new canonical contracts. Consult the [ticket index](migrations_instructions/implementation_work_packages_v2/RESEARCH_RUNTIME_MISSION_TICKETS.md).

RRM-002 baseline repair is the next **code implementation** ticket and can start under existing accepted authority. RRM-001 specification/contract reconciliation can be prepared independently; its accepted outputs gate RRM-003 onward. Recommended serial order: RRM-002 -> RRM-001 -> RRM-003 -> RRM-004 -> RRM-005 -> RRM-006 -> RRM-007 -> RRM-008 -> RRM-009 -> RRM-010. Follow blocking edges rather than assuming every serial ordering is mandatory.

After RRM-010, stop and return the readiness manifest to the user. RRM-011 is held for a later user-started fixture session. There is no elapsed-time or automatic authorization transition.
