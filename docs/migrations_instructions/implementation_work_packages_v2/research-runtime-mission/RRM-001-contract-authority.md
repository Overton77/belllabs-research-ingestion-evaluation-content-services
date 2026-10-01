# RRM-001 — Lifecycle contract authority (traceability)

Recorded: 2026-10-01
Status: **accepted 2026-10-01**. Accepted canonical meta revision: `6c89143` on `spec/research-runtime-lifecycle`, merged into meta `main` at `a50d833`. RRM-003 onward implement against that revision.

**Acceptance authority (recorded verbatim).** When asked how to handle the RRM-001 user checkpoint, the user selected: "Pre-authorize after review (Recommended)". The option read: "I author the spec amendments, get an independent Opus review, fix the findings, and record your pre-authorization word for word as acceptance. Then I continue through RRM-003…RRM-010 without stopping. You can still review the meta diff later." For the cognitive-schemas/ADR-0004 disposition, the user selected "Let RRM-001 recommend".
Application base: integration `e946742` (branch `wp/rrm-001-lifecycle-contracts`)

## 1. Accepted canonical revision

| Item | Value |
|---|---|
| Meta repository | `../biotech-meta` |
| Branch | `spec/research-runtime-lifecycle`, based on meta `main` at `c48867a` |
| Commits | `6cb20af` (AMD-RRM-001 lifecycle amendments and disposition index); `d879129` (cognitive schemas and ADR-0004 narrowed-acceptance recommendation); `18f44b7` (independent review fixes, verdict `accept_with_fixes`, §9); `6c89143` (acceptance commit with the re-review wording fixes, §9.1); merged into meta `main` at `a50d833` |
| Amendment ID | `AMD-RRM-001`, status `accepted` (2026-10-01) in every amended spec's frontmatter and amendment record; spec 05 `canonical` with REQ-CP-CS-006/008 deferred; ADR-0004 `accepted` with decision 5 and the Workflow Type part of decision 2 deferred |
| Who flipped status | The coordinator, in `6c89143`, after the independent re-review |

Authoring followed the `to-spec` process: explore, choose test seams, then synthesize. No user interview was held; decisions are recorded in §8 as open decisions with recommendations. Canonical text was amended in place, following each spec's conventions: frontmatter `requirements`, `contracts` and `version`, a new `amendments` key, and an `## Amendment record` section. No parallel lifecycle spec was created.

## 2. Summary of the amendments

**Notation.** "Clarified" means the behavior was already mandated by the cited requirement, and the amendment adds exact states, fields, or ordering. "New" means a newly specified storage, API, or protocol detail.

| Spec | Clarified | New |
|---|---|---|
| SPEC-CP-RUN-CONTROL (02) | REQ-CP-RUN-004 (requested vs applied axes), 007 (`in_doubt` disposition, checkpoint ≠ settlement), 009 (child usage settles to parent budget); `CON-CP-LIFECYCLE-V1` (pending-command projection fields) | REQ-CP-RUN-011 (scoped, non-mutating inspection reads, saver-based historical reads), REQ-CP-RUN-012 (freshness, ambiguity, redaction); `CON-CP-INSPECTION-READ-V1`; `reconcile_unit` command and `operator_reconciliation` wait condition in `CON-CP-LIFECYCLE-V1` |
| SPEC-CP-DURABLE-EXECUTION (03) | REQ-CP-EXEC-005 (generation boundaries), 006 (receipt state machine), 007 (Update = delivered; Queries diagnostic; no raw-signal commands), 008 (seven-step cancellation saga, heartbeat), 011 (carried waits, pause, expected checkpoints), 012 (patch, reuse frontier, epoch 1, no implicit copying, `cognitive_seed`); `CON-CP-TEMPORAL-IDENTITY-V1`, `CON-CP-WORKFLOW-MESSAGE-V1`, `CON-CP-CONTINUATION-V1` | REQ-CP-EXEC-013 (runtime-unit identity), 014 (Activity attempt observation and claim fence), 015 (Search Attributes only), 016 (snapshots only at safe boundaries); `CON-CP-RUNTIME-UNIT-V1` |
| SPEC-CP-DEEP-AGENT-RUNTIME (04) | REQ-CP-DA-004 (`checkpoint_resume` meaning, persistent saver), 008 (one provider run per child, `in_doubt` not orphaned), 011 (qualified provider checkpoint, attributed usage), 015 (sandbox vs macro snapshot) | REQ-CP-DA-016 (qualified checkpoint key, namespaces, root `checkpoint_ns`, `durability="sync"`, metadata stamps), 017 (transition observation with CAS), 018 (terminal reconstruction vs resume; crash windows; `in_doubt`), 019 (exact, non-scheduling async hosting); `CON-CP-CHECKPOINT-LINEAGE-V1`; `CON-CP-ASYNC-SUBAGENT-V1` changes (contract schema fields, the `in_doubt` lifecycle value, and the `adopt_provider_run` / `orphan_child` decisions) |
| SPEC-CP-COGNITIVE-SCHEMAS (05, draft) | Narrowed acceptance recommended: CS-001, 002, 003, 004 (amended), 005, 007 (amended to the stamped digest) | REQ-CP-CS-008 (split from CS-004, deferred); CS-006 deferred |
| SPEC-BP-STAGEGRAPH | REQ-BP-SG-009 (governed wait release, receipts, Continue-As-New continuity) | — |
| SPEC-BP-GOAL-DIRECTED | REQ-BP-GD-011 (durable pause and resume; already mandated, now owned by the family) | REQ-BP-GD-012 (shared-session ordering) |
| ADR-0004 (proposed) | Narrowed acceptance recommended; status unchanged | — |

Key normative decisions:

1. **Unit key.** `unit_key = "bl-unit-v1:" + sha256(canonical CON-CP-RUNTIME-UNIT-V1)`. Execution generation is outside the key, so the fence key is `(unit_key, generation)`. The `operation/{semantic_attempt_id}` wire ID is not renamed.
2. **Checkpoint namespace.** BellLabs isolation is a *cognitive session namespace* that is also the LangGraph `thread_id`. The root graph always runs with `checkpoint_ns=""`, because the pinned LangGraph 1.2.10 treats a non-empty `checkpoint_ns` as a subgraph path on state reads. Nested namespaces are evidence only. The namespace head is the result key of the last accepted transition observation, and the checkpointer's latest checkpoint is evidence only. Every submission and resumption is pinned to a `checkpoint_id`; an unpinned `ainvoke` would continue from any stray branch.
3. **Invocation metadata.** Every invocation runs with `durability="sync"` and stamps scalar metadata (`belllabs_unit_key`, generation, invocation ID, binding digest, state-schema digest), which LangGraph copies onto every checkpoint. This makes descendant attribution and the schema gate implementable.
4. **Classification before dispatch.** Each attempt classifies the unit as `settled`, `observed_unsettled`, `not_submitted`, `interrupted`, `terminal_unobserved`, or `in_doubt`, then acts. A stamped checkpoint never authorizes re-appending input. Every other state is `in_doubt` and requires an operator `reconcile_unit` decision (`CON-CP-LIFECYCLE-V1`): `accept_descendant`, `abandon_unit`, or `start_new_generation`. While a unit is `in_doubt` the run keeps its phase; if nothing else is admissible, it waits on an `operator_reconciliation` condition. A post-dispatch exception settles `failed` only when classification shows no terminal result and every effect claim is settled.
5. **Receipts.** Command receipts are `accepted → delivered → applied | rejected`, stored in PostgreSQL. Phase changes for pause, resume, and wait release happen on `applied`. Cancel moves to `cancelling` on acceptance.
6. **Forks.** A fork is an independently admitted run at epoch 1. It may be built only from a safe boundary or quiescence; active async children make the snapshot unsafe. Only settled, compatible results are reused. Cognition starts fresh, from a typed handoff, or from an explicit terminal-checkpoint `cognitive_seed` into a new namespace. Executable forks from intermediate checkpoints are deferred.
7. **GoalDirected sessions.** Pause is a durable state with an `applied` receipt, never `goal_paused` failure. A shared session is strictly ordered. A rollover starts a new session namespace instead of branching the thread. A generation boundary keeps the `unit_key`, runs fresh from the handoff in `belllabs/goal/{run}/epoch/{epoch}/unit/{unit_key}/gen/{generation}`, and the next iteration moves to a new session generation.

## 3. Contract disposition table

This table is mirrored verbatim in the meta index (`docs/specs/control-plane-foundations/README.md` § AMD-RRM-001 contract disposition).

Citations are to the application repository at integration commit `e946742`. **Reuse** means the item keeps its semantics, with only additive fields where noted. **Version** means a new schema or storage version, or a forward-only migration, replaces the item's contract while its accepted mechanism is kept. **Retire** means the item is removed from active mission paths and left inert; physical deletion is a later ticket. Successor requirements are AMD-RRM-001 IDs.

| # | Item | Citation | Disposition | Rationale and successor |
|---|---|---|---|---|
| **Identities** | | | | |
| 1 | `BellLabsRunKey`, `ExecutionEpochKey` | `app/domain/graph_runtime/identities.py:15-28` | reuse | Scope, run, and epoch identity; a fork is a new run at epoch 1 |
| 2 | `GraphIdentity` | `identities.py:31-34` | reuse (out of mission) | Not inert: it is a field of `GraphAssemblyDefinition` (`app/domain/graph_runtime/definitions.py:411`), used by the coordinator `RunPlanV3` launch (`app/application/coordinator/coordinator_launch.py:36, 102`). Retained for coordinator `RunPlanV3`; no mission contract depends on it |
| 3 | `DeploymentIdentity` | `identities.py:37-42` | version | Agent Server deployment identity moves into `AsyncSubagentContract` hosting fields (DA-019); never a macro binding |
| 4 | `AgentThreadKey` | `identities.py:45-58` | retire | Agent Server thread with fork/linked relationships encoded in it; replaced by the cognitive session namespace and qualified key (DA-016), and by the async `child_execution_id` |
| 5 | `AgentRunKey` | `identities.py:61-64` | retire | The provider run ID already lives on `AsyncSubagentExecution` (`app/domain/operation_execution/contracts.py:713`) |
| 6 | `LangGraphCheckpointKey` | `identities.py:67-71` | version | No `checkpoint_ns`, no checkpointer identity, no parent, and a mandatory deployment endpoint; replaced by the qualified checkpoint key (`CON-CP-CHECKPOINT-LINEAGE-V1`) |
| 7 | `GoalHandoffCheckpointKey` | `identities.py:74-76` | reuse | BellLabs handoff identity, not a LangGraph checkpoint |
| 8 | `SemanticOperationAttemptKey` | `identities.py:79-106` | version | Lacks epoch, mapped instance, workflow cycle, slot, revision, role, and session generation; uses a delimiter key; replaced by `CON-CP-RUNTIME-UNIT-V1` (EXEC-013) |
| 9 | `RuntimeTransportAttemptKey` | `identities.py:109-111` | retire | Agent Server submission attempt; replaced by the Activity attempt observation (EXEC-014) |
| 10 | `SubagentProfileKey`, `LinkedBellLabsRunKey` | `identities.py:114-131` | reuse | Unchanged; not on the mission path |
| 11 | `OperationAttemptIdentity` | `app/domain/operation_execution/contracts.py:51-58` | reuse | `semantic_key` stays the basis of the `operation/{id}` wire identity; the runtime unit travels beside it |
| 12 | `StageExecutionIdentity`, `GoalIterationIdentity`, `GoalAgentRunIdentity` | `app/domain/orchestration/contracts.py:261-296, 777-802` | reuse | Sources of the unit location fields |
| **Operation, binding, and result contracts** | | | | |
| 13 | `DeepAgentExecutionBinding` | `operation_execution/contracts.py:898-996` | version | Add the frozen cognitive session namespace and unit identity (DA-016); schema digests reused (CS-001, CS-007) |
| 14 | `DeepAgentExecutionPlacementProfile` | `contracts.py:840-880` | reuse | The meaning of `reconnect_behavior=checkpoint_resume` is fixed by DA-004 and DA-018 |
| 15 | `RuntimeResult`, `OperationSettlement`, `OperationExecutionResult` | `contracts.py:1220-1226, 1266-1290` | version | Add the result checkpoint key and transition ref (DA-017); runtime ambiguity settles `in_doubt` (RUN-007) |
| 16 | `OperationWorkflowRequest` / `Result` | `contracts.py:1293-1367` | version | Additive unit identity and heartbeat/cancel policy fields; no wire renames |
| 17 | `AsyncSubagentContract` | `contracts.py:610-656` | version | Add `graph_revision`, `graph_binding_digest`, and a deployment credential ref (DA-019) |
| 18 | `AsyncSubagentResultManifest` | `contracts.py:659-694` | version | Free-text `checkpoint_ref` (`:669`) becomes the qualified provider key; attributed or pending usage (DA-011) |
| 19 | `AsyncSubagentLifecycle`; `AsyncSubagentExecution`, `ParentAsyncSubagentLink`, `AsyncSubagentMessage` receipts | `contracts.py:598-607`; `contracts.py:697-775` | version (lifecycle) / reuse | `AsyncSubagentLifecycle` gains `in_doubt`, with exits by observation or the operator decisions `adopt_provider_run` / `orphan_child` (DA-008). The deterministic thread identity and parent link are reused; message receipts map to EXEC-006 states |
| 20 | Sandbox snapshot contracts and service | `contracts.py:1676-1810`; `app/application/workspaces/sandbox_snapshots.py` | reuse | `CON-CP-SNAPSHOT-V1` covers workspace and sandbox state only; it is referenced by, and distinct from, `RunSnapshotManifest` (DA-015) |
| 21 | `WorkflowMessage` / `WorkflowMessageReceipt`; root receipts | `orchestration/contracts.py:41-64`; `app/temporal/workflows/belllabs_run.py:31-58` | version | In-memory statuses are a transport cache; authoritative `accepted`/`delivered`/`applied`/`rejected` receipts live in PostgreSQL (EXEC-006) |
| 22 | `RunContinuityState` | `orchestration/contracts.py:67-99` | version | Add satisfied waits, pause state, and the expected source checkpoint per active unit (EXEC-011) |
| **graph_runtime contracts** | | | | |
| 23 | `RuntimeExecutionBinding`, `RuntimeExecutionAttempt`, `RuntimeExecutionProjection` | `app/domain/graph_runtime/contracts.py:116-175` | retire | Per-epoch binding over `legacy_temporal`/`langgraph_agent_server`; replaced by the operation binding, runtime unit, and inspection reads (RUN-011). Rows 23–27 are still exported by the production schema route `/v2/graph-runtime/schemas` (`app/server.py:23, 244`; `app/api/graph_runtime_schemas.py`). Retirement removes them from that export, or labels them non-authoritative there |
| 24 | `InterventionBase`, `SatisfyWait`, `ResumePause`, `CancelRun` interventions | `graph_runtime/contracts.py:178-225` | retire | Replaced by run-control lifecycle actions (`app/domain/run_control/contracts.py:350-377`) plus receipts; their expected-version and expected-checkpoint checks are kept in that path |
| 25 | `AppendInput`, `RespondToInterrupt`, `DurableInterrupt*` | `graph_runtime/contracts.py:198-220, 284-311` | retire | Mid-invocation editing and HITL are out of scope |
| 26 | `ForkFromCheckpointIntervention`, `ForkRequest`, `ForkReceipt` | `graph_runtime/contracts.py:228-239, 334-352` | version | Add snapshot ref, patch digest, and target admission; a qualified seed key replaces `LangGraphCheckpointKey` and `AgentThreadKey` |
| 27 | `PrivilegedOperatorReconcileIntervention`, `InterventionReceipt` | `graph_runtime/contracts.py:241-281` | version | Becomes the `reconcile_unit` command (unit key and generation; DA-018 decisions); receipt states move to EXEC-006 |
| 28 | `RedactedCheckpointSummary` and redaction allowlist | `graph_runtime/contracts.py:355-365, 465-514` | version / reuse | Summary keyed by the qualified key with compatibility checks (RUN-011/012); allowlist mechanism reused |
| 29 | `ProviderNeutralAttemptMetadata` | `graph_runtime/contracts.py:385-399` | reuse | `retry_class` semantics for consequential tools |
| 30 | Lineage kernel (`LineageKind`, `LineageParentEdge`) | `app/domain/graph_runtime/kernel.py:51-104` | reuse | Additive kinds (`langgraph_checkpoint`, `runtime_unit`, `run_snapshot`) and relationships (`derived_from`, `seeded_from`, `reuses`) |
| **Services** | | | | |
| 31 | `decide_recovery_mode` | `app/application/runtime/runtime_recovery.py:45-98` | reuse | Diagnostic replay isolated, fork as a new run, rollback forbidden |
| 32 | `build_cancellation_plan`, `apply_terminal_runtime_observation` | `runtime_recovery.py:101-183` | retire | Bound to the retired binding status machine; the saga is EXEC-008 over run control and units |
| 33 | `RuntimeForkService` saga and fork protocols | `runtime_recovery.py:186-489` | version | Keep reserve, admit, record, copy, receipt, and its reconciliation. Replace the identical-run-plan check (`:390-391`) with snapshot/patch-compiled admission; assert target epoch 1 (`:459-466`); replace provider `copy_checkpoint` (`:226-239`) with an optional local `cognitive_seed` |
| 34 | `InMemoryForkRepository` | `runtime_recovery.py:272-362` | reuse | Test double |
| 35 | `PostgresForkRepository` | `app/application/runtime/postgres_stage3_kernel_repository.py:749-1001` | version | Keep advisory guard, claims, and idempotency; `reserve()` resolves a source runtime binding (`:794-806`) and must resolve a source snapshot |
| 36 | `RuntimeInterventionService` | `app/application/runtime/runtime_interventions.py:203-323` | version | Keep the fail-closed authorization, expected-version, and reserve/reconcile pattern behind run-control delivery |
| 37 | `ExactRuntimeInterventionRouter` | `runtime_interventions.py:138-200` | retire | Requires an Agent Server route (`:160-161`) |
| 38 | Lineage service and `PostgresExecutionLineageRepository` | `app/application/runtime/runtime_lineage.py:20-156`; `postgres_stage3_kernel_repository.py:75-222` | reuse | Immutable lineage journal with parent traversal |
| 39 | Incident catalog and `PostgresRuntimeIncidentRepository` | `app/application/runtime/runtime_reconciliation.py:130-296`; `postgres_stage3_kernel_repository.py:1004-1219` | reuse | Add `checkpoint_in_doubt`, `async_submission_in_doubt`, and `stale_fence_write` types and a unit-key reference |
| 40 | `PrivilegedRuntimeRepairService` | `app/application/runtime/runtime_repairs.py:46-115` | version | Executor for `reconcile_unit` |
| 41 | Decision/HITL services and `PostgresDecisionRepository` | `app/application/runtime/runtime_decisions.py`; `postgres_stage3_kernel_repository.py:225-392` | retire | HITL is out of scope; inert |
| 42 | Resource lease journal | `app/application/runtime/runtime_resources.py`; `postgres_stage3_kernel_repository.py:395-746` | reuse | Unchanged; not on the mission path |
| 43 | Agent Server macro-runtime prior art | `graph_runtime_dispatch.py`, `agent_server_actions.py`, `runtime_bootstrap.py`, `runtime_execution_bindings.py`, and `postgres_runtime_execution_repository.py` (all in `app/application/runtime/`); `app/integrations/langgraph_agent_server.py` | retire | Not composed in production; must never be revived as a scheduler (ADR-0003) |
| 44 | Operation execution and journal services | `app/application/operations/operation_execution.py:360-475`; `journaled_operation_execution.py:91-356`; `operation_journal.py` | version | Keep claim → observe → authority settlement. Add classification (DA-018), claim fence and lease takeover (EXEC-014), and `in_doubt` for post-dispatch ambiguity (`operation_execution.py:441-459`); `technical_attempt` is hard-coded to 1 (`journaled_operation_execution.py:327`) |
| 45 | Async subagent service and adapter | `app/application/async_subagents/service.py:130-444`; `app/integrations/agents/deep_agents/async_subagents.py:23-223` | version | Keep the reservation and link before submit and the deterministic thread. Add a per-child submission fence (`async_subagents.py:76-91`); classify submission errors `in_doubt` rather than `orphaned` (`service.py:201-214`); resume admitted-unsubmitted children (`service.py:191-192`); real usage and checkpoint refs (`async_subagents.py:220-221`) |
| 46 | Deep Agent adapter | `app/integrations/agents/deep_agents/adapter.py:74-122` | version | Explicit namespace, root `checkpoint_ns`, `durability="sync"`, metadata stamps, result-config capture, and classification before submit (DA-016 to DA-018) |
| **Storage (migrations)** | | | | |
| 47 | `runtime_execution_bindings`, `runtime_execution_attempts`, `runtime_checkpoint_observations`, `runtime_intervention_commands`, `runtime_interrupt_*`, `runtime_async_tasks` | `app/migrations/0012_graph_runtime_operation_journal.sql:4-201`; `0014:1-17` | retire (inert) | Agent Server-shaped: `runtime_provider` (`0012:15-17`), one binding per epoch (`:32`), checkpoint key without namespace (`:101-104`); async tasks superseded by `0016`. A new forward-only migration adds unit, attempt, transition, and receipt records |
| 48 | `operation_effect_claims`, `operation_journal_mutations`, `operation_execution_attempts`, `operation_settlements` | `0012:203-317`; `0018` | version | Active production journal; a forward migration adds the claim fence, unit key, and transition/result checkpoint refs |
| 49 | `runtime_lineage_records` / `_edges` | `0014:25-67` | version | Reuse; extend the relationship CHECK (`:53-56`) additively |
| 50 | `runtime_reconciliation_incidents`, `runtime_repair_audit`, `runtime_retention_deletion_audit` | `0014:153-189, 215-245` | reuse | Add a unit-key column. The new migration MUST drop the FK from `runtime_reconciliation_incidents.binding_id` to the retired `runtime_execution_bindings` (`0014:176-177`); the column stays nullable and unused |
| 51 | `runtime_fork_requests` | `0014:191-213` | version | Add source snapshot and patch. The new migration MUST make `source_binding_id` nullable and drop its FK to the retired `runtime_execution_bindings` (`:195, 208-209`) |
| 52 | `runtime_decision_requests` / `_responses` | `0014:110-151` | retire (inert) | HITL is out of scope |
| 53 | `execution_resource_leases` | `0014:69-108` | reuse | Unchanged |
| 54 | `async_subagent_authority`, `_commands`, `_facts`, `_messages` | `app/migrations/0016_async_subagent_parent_child_v1.sql:4-61` | version | Reuse. A forward migration adds the per-child submission fence, the `in_doubt` lifecycle fact, and the `adopt_provider_run` / `orphan_child` decisions. These widen the `command_kind` CHECK (`0016:30`) |
| **Workflows (mechanics)** | | | | |
| 55 | Root `request_cancel`, `deliver_message`, `signal_message` | `app/temporal/workflows/belllabs_run.py:60-73` | version | Become delivery targets of recorded commands; a raw cancel bypasses journaled intent (EXEC-008) |
| 56 | StageGraph `satisfy_wait` / `resume_pause` | `app/temporal/workflows/stagegraph.py:51-61, 456-464` | version | Delivery of recorded commands; waits carried across Continue-As-New; `_resumed_pauses` is never read |
| 57 | GoalDirected `goal_paused` / `goal_cancelling` | `app/temporal/workflows/goal_directed.py:229-234, 402-425` | version | Durable pause (GD-011); cancellation completes the saga (EXEC-008) |
| 58 | `OperationWorkflow` activity call | `app/temporal/workflows/operation.py:74-90` | version | Heartbeat timeout and cancellation delivery (EXEC-008); attempt observation (EXEC-014) |

## 4. Requirement → downstream ticket → owning test seam

The proposed test modules prefer existing seams; a new module is listed only where no owner exists yet. Live and service suites stay behind their existing or new explicit opt-in flags.

| Requirement | Ticket | Owning test seam (proposed path) | Observed assertion |
|---|---|---|---|
| REQ-CP-EXEC-013, `CON-CP-RUNTIME-UNIT-V1` | RRM-003 | `tests/unit/orchestration/test_runtime_unit_identity.py` (new) | Golden canonical digests; every location field changes the key; retries, restart, and Continue-As-New keep it |
| REQ-CP-EXEC-014 | RRM-003 (observation), RRM-004 (takeover and fence) | `tests/unit/operations/test_operation_execution.py` (extend); `tests/integration/postgres/test_operation_journal_stage1.py` (extend) | Attempt numbers recorded; lease takeover; a stale-fence write is rejected |
| REQ-CP-DA-016, 017; REQ-CP-CS-007 | RRM-003 | `tests/acceptance/control_plane/test_wp_cp_040.py` (extend adapter cases); `tests/integration/deep_agents/test_checkpoint_lineage_postgres_saver.py` (new, real `AsyncPostgresSaver`, opt-in) | Before/after qualified keys; metadata stamps; root `checkpoint_ns`; CAS conflict; duplicate delivery idempotent; schema mismatch rejected |
| REQ-BP-GD-012 | RRM-003 | `tests/unit/orchestration/test_wp_bp_020_goal_directed.py` (extend); persistent-saver module above | Linear stamped lineage across iterations; a concurrent session invocation is rejected; rollover gets a new namespace |
| `reconcile_unit` and `operator_reconciliation` (`CON-CP-LIFECYCLE-V1`) | RRM-004 | `tests/unit/run_control/test_run_control.py` (extend) | `accept_descendant` reclassifies to resume or reconstruct, `abandon_unit` settles `failed`, `start_new_generation` advances the generation; stale versions are rejected; run phase while `in_doubt` |
| REQ-CP-DA-018, REQ-CP-EXEC-005, REQ-CP-RUN-007 | RRM-004 | `tests/unit/operations/test_checkpoint_recovery_classification.py` (new); `tests/integration/temporal/test_rrm_004_worker_restart_recovery.py` (new; persistent saver plus application DB) | Per crash window: model/tool invocation counts, human-input count, ancestry, final digest; ambiguous → `in_doubt` incident with no invocation |
| REQ-CP-DA-008, 011, 019; REQ-CP-RUN-009 | RRM-013 | `tests/acceptance/control_plane/test_wp_cp_045.py` (regression); `tests/unit/integrations/test_async_subagent_submission_fence.py` (new); `tests/integration/agent_server/test_rrm_013_async_subagent_live.py` (new; `AGENT_SERVER_ENDPOINT` plus explicit live flag) | One provider run per child across worker kill and server restart; `in_doubt` not orphaned, with its exits by observation, `adopt_provider_run` (other runs cancelled, their usage pending), and `orphan_child`; served graph identity verified; usage settled to parent |
| REQ-CP-RUN-011, 012; REQ-CP-EXEC-007, 015 | RRM-005 | `tests/unit/run_control/test_runtime_inspection_reads.py` (new); `tests/integration/temporal/test_rrm_005_search_attributes.py` (new, real namespace); `tests/acceptance/control_plane/test_rrm_005_inspection.py` (new) | Cross-scope denial, cursor errors, incompatible checkpoint, freshness `unavailable`, redaction, zero writes on read, two-checkpoint history plus replay |
| REQ-CP-EXEC-012, 016; `CON-CP-CONTINUATION-V1`; REQ-CP-DA-015 | RRM-006 | `tests/unit/runtime/test_run_snapshot_manifest.py` (new); `tests/unit/runtime/test_runtime_recovery_stage3.py` (extend the fork saga); `tests/integration/postgres/test_stage3_kernel_postgres_integration.py` (extend the fork repository); `tests/acceptance/control_plane/test_rrm_006_semantic_forks.py` (new) | Protected-field rejection; `snapshot_not_quiescent`; epoch 1; reuse only settled compatible results; no command or effect copying; both families |
| REQ-CP-EXEC-006, 007, 011; REQ-CP-RUN-004; REQ-BP-SG-009; REQ-BP-GD-011 | RRM-007 | `tests/unit/run_control/test_run_control.py` (extend: receipts, requested vs applied); `tests/integration/temporal/test_rrm_007_boundary_interventions.py` (new); replay in `tests/integration/temporal/test_wp_bp_010_temporal.py` and `test_wp_bp_020_temporal.py` | Distinct receipts; StageGraph wait release through the facade; GoalDirected pause without failure, then resume; ordering across worker restart and forced Continue-As-New |
| REQ-CP-EXEC-008; REQ-CP-RUN-005, 006, 007, 009, 010; REQ-CP-DA-008/011 (cancel) | RRM-008 | `tests/integration/temporal/test_rrm_008_running_cancellation.py` (new); the RRM-013 live module (cancel case) | Intent journaled first; heartbeat cancellation; real async child cancel; ambiguous effect → incident; immutable terminal `cancelled` |
| REQ-CP-DA-004 (persistent saver), DA-016 (durability), DA-019, REQ-CP-EXEC-015 (registration) | RRM-009 | `tests/integration/temporal/test_rrm_009_production_composition.py` (new) | Production factory composes the persistent saver and store, registered Search Attributes, and the async graph identity |
| All of the above | RRM-010 | Combined technical smoke (RRM-010 ticket) | Inspection, historical read, safe fork, intervention, and cancellation, including an active real async child |

## 5. Cognitive schemas (spec 05) and ADR-0004 recommendation

**Recommendation: narrow, then accept.** Do not exclude either document.

- **Accept the implemented core:** CS-001, 002, 003, 004 (amended to exact pack composition; colliding definitions must be identical), 005, and the three `CON-CP-COGNITIVE-*` contracts. WP-CP-040 code and evidence already depend on them. The binding carries both schemas (`app/domain/operation_execution/contracts.py:931-936`). The materializer composes the types (`app/integrations/agents/deep_agents/materializer.py:537-581`). Pack composition fails on collision (`app/domain/operation_execution/materialization.py:27-60`).
- **Accept CS-007, amended.** Its digest gate is required by RRM-003/004/005/006. Today no code checks a checkpoint's schema digest on resume, so the gate is unproven. AMD-RRM-001 makes it enforceable through the stamped `belllabs_state_schema_digest` (REQ-CP-DA-016).
- **Defer, remaining draft:**
  - CS-006 (sync-subagent seed projection). Slices are declared and validated (`app/domain/operation_execution/materialization.py:146-153`), but the materializer passes dictionary subagents without a BellLabs-seeded slice (`materializer.py:294-350`). The Deep Agents 0.7.5 default therefore applies. It passes every parent channel except `messages`, `todos`, `structured_response`, and private channels to the child, and merges the child's returned channels back (`deepagents/middleware/subagents.py:537`, `:484`).
  - The Workflow Type / WorkflowConfiguration pack declaration, split out as CS-008. No definition owns pack allowlists today.

  No mission ticket needs either.
- **ADR-0004.** Accept decisions 1, 3, 4, and 6, plus the binding and adapter part of decision 2. Defer decision 5 and Workflow Type pack ownership. Decision 6 is to be enforced by RRM-003/004 through the stamped digest. Accepted in meta `6c89143`.
- **Acceptance mechanism (applied in meta `6c89143`).** The acceptance commit changed the status lines:
  - spec 05 `status: draft` becomes `status: canonical`, keeping `deferred_requirements: [REQ-CP-CS-006, REQ-CP-CS-008]` as non-authority;
  - ADR-0004 `status: proposed` becomes `status: accepted`, keeping `deferred_decisions` (decision 5, and the Workflow Type part of decision 2) as proposed;
  - each `AMD-RRM-001` amendment status becomes accepted.
- **Traceability annotation (made in this branch).** `TRACEABILITY.md:56-62` listed REQ-CP-CS-001..007 as "accepted" under WP-CP-040 against a draft spec. That was package acceptance, not specification acceptance. The CS-006 row is now annotated: only slice declaration and validation exist, there is no seeding, and the requirement is deferred. The CS-007 row is now annotated: no runtime resume gate exists yet, and it is to be enforced by RRM-003/004. The CS-004 row is now annotated as amended.

## 6. to-spec synthesis (problem, solution, stories, testing)

**Problem.** An operator cannot trust a long-running research run under crash, inspection, fork, or intervention, for five reasons:

- The production adapter does not record which checkpoint an operation started from or produced.
- Retries either stall on an unrecoverable claim or would re-append a completed prompt.
- Lifecycle commands are accepted without proof that they were applied.
- GoalDirected pause fails the workflow.
- Forks and inspection have no safe snapshot or read contract.

**Solution.** The minimum canonical amendments in §2. Each operation becomes a stable runtime unit with fenced attempts and exact checkpoint transitions. Recovery classifies before acting. Commands carry durable receipts. Reads are scoped and honest about freshness. Forks are independently admitted runs from safe snapshots.

User stories:

1. As an operator, I want each operation to have one stable unit key across retries, so I can correlate Temporal, PostgreSQL, and LangGraph records.
2. As an operator, I want each unit to show the checkpoint it started from and the one it produced, so I can audit cognition.
3. As an engineer, I want a crashed worker's operation to resume without re-sending its prompt, so model cost and conversation state are not duplicated.
4. As an engineer, I want a completed but unrecorded agent result to be reconstructed rather than re-run, so terminal work is never repeated.
5. As an operator, I want ambiguous recoveries to stop and raise an `in_doubt` incident, so no speculative re-invocation happens.
6. As an operator, I want a typed reconciliation command for `in_doubt` units, so the decision is audited.
7. As an operator, I want to see whether my pause or resume was accepted, delivered, or applied, so I know the run actually paused.
8. As an operator, I want a GoalDirected run to pause durably and resume later from the same frontier, so pausing is not failure.
9. As an operator, I want to release a StageGraph review wait through the API while the run keeps running.
10. As an operator, I want cancellation to stop active cognition and reconcile usage, reservations, and effects before terminalizing.
11. As an operator, I want to list and inspect active and historical runs and units, with clear freshness and redaction.
12. As an operator, I want to read a redacted summary of an earlier checkpoint, validated for ownership and compatibility.
13. As a researcher, I want to fork a run from a safe boundary with a bounded patch, so I get an independent derived report without touching the parent.
14. As a researcher, I want a fork to reuse settled compatible results, so it does not recompute unaffected work.
15. As a security reviewer, I want reads and Search Attributes to exclude secrets, transcripts, and raw scopes.
16. As an engineer, I want async subagents to run on the Agent Server with one provider run per child and parent-owned budgets, without the server becoming a scheduler.
17. As a future platform engineer, I want these mechanisms kept free of company specifics, so they can be generalized later.

**Testing decisions.** Test external behavior at the highest existing seams: the operation execution service, the run-control service and API, the Temporal family workflows with captured-history replay, and the persistent saver integration. Prior art:

- `tests/unit/operations/test_operation_execution.py:820-873` (claimed-unsettled redelivery);
- `tests/acceptance/control_plane/test_wp_cp_040.py:574-599` (session reuse);
- `tests/unit/runtime/test_runtime_recovery_stage3.py` (fork saga);
- `tests/integration/postgres/test_stage3_kernel_postgres_integration.py`;
- `tests/integration/temporal/test_wp_bp_0{1,2}0_temporal.py` (replay).

Recovery qualification must use the persistent saver and application database with a real worker restart. Memory-only tests do not satisfy it.

**Out of scope.** Generalized framework extraction, company fixtures (RRM-011), arbitrary mid-invocation cognitive editing, HITL interrupt resumption, and executable forks from intermediate checkpoints.

## 7. Code that contradicts accepted spec text (candidate defects for downstream tickets)

| # | Location | Contradiction | Owner |
|---|---|---|---|
| 1 | `app/application/operations/journaled_operation_execution.py:537-556` (claim has no lease); `operation_execution.py:399-407` | A worker crash after claiming leaves a claim that no attempt can take over. The operation fails after three attempts without settlement. This contradicts REQ-CP-EXEC-003 (survives worker loss) and REQ-CP-RUN-007 (exactly one settlement). | RRM-004 |
| 2 | `app/application/operations/operation_execution.py:441-459` | Any exception after provider dispatch is settled as authoritative `failed` without checkpoint classification or effect-claim checks, even when the outcome is ambiguous. This contradicts REQ-CP-RUN-007 (preserve ambiguous disposition; as clarified, `failed` only when there is no terminal result and all effect claims are settled). | RRM-004 |
| 3 | `app/integrations/agents/deep_agents/adapter.py:74-99`; asserted by `tests/acceptance/control_plane/test_wp_cp_040.py:588-599` (`[1, 2, 1]`) | Re-executing the same invocation appends the prompt again on the same thread, with no source checkpoint check, default `async` durability, and no result checkpoint capture. This contradicts the placement's `reconnect_behavior: checkpoint_resume` (`CON-CP-DEEP-AGENT-PLACEMENT-V1`; `contracts.py:919`) and REQ-CP-EXEC-005. The test must be rewritten for the same-unit case; intentional cross-iteration session reuse stays valid. | RRM-003/004 |
| 4 | `app/temporal/workflows/goal_directed.py:229-234` | Pause raises the non-retryable `goal_paused` error. This contradicts SPEC-CP-RUN-CONTROL § State (`paused` requires explicit resume) and REQ-CP-RUN-004. | RRM-007 |
| 5 | `app/temporal/workflows/goal_directed.py:402-425` | Cancellation records the cancel command, then fails the family with `goal_cancelling` instead of completing reconciliation. This contradicts REQ-CP-EXEC-008. | RRM-008 |
| 6 | `app/temporal/workflows/stagegraph.py:55-61, 456-464` | Satisfied waits live only in workflow memory and are not carried across Continue-As-New. The `resume_pause` signal is recorded but never read. This contradicts REQ-CP-EXEC-011 and EXEC-006. | RRM-007 |
| 7 | `app/temporal/workflows/belllabs_run.py:31-58, 68-73` | Receipts exist only in workflow state. The raw `request_cancel` signal cancels the family with no journaled intent. This contradicts REQ-CP-EXEC-006, REQ-CP-EXEC-008, and 03 § Authority ("never the only copy of a business decision"). | RRM-007/008 |
| 8 | `app/domain/run_control/reducer.py:226-249` with no delivery path in `app/` | Accepted `pause`, `resume`, and `satisfy_wait` commands change the projection phase, but nothing delivers them to Temporal. The projection can say `paused` while the family runs. This contradicts REQ-CP-RUN-004 (as clarified) and EXEC-006. | RRM-007 |
| 9 | `app/temporal/workflows/operation.py:83-90` | The cognitive Activity has no heartbeat timeout, and cancellation is checked only before dispatch. This contradicts 03 § Failure ("long activities heartbeat") and REQ-CP-EXEC-008. | RRM-008 |
| 10 | `app/application/async_subagents/service.py:191-192, 201-214` | An admitted-but-unsubmitted child is returned without submission. Any submission exception marks the child `orphaned`, although its deterministic thread may already have a run. This contradicts REQ-CP-RUN-007 and DA "start-bind-wait/reconcile". | RRM-013 |
| 11 | `app/integrations/agents/deep_agents/async_subagents.py:76-91, 220-221` | The list-then-create reconnect is not fenced across concurrent submitters, so it can create two runs. Usage and checkpoint refs are synthetic. This contradicts REQ-CP-DA-008 and DA-011. | RRM-013 |
| 12 | `app/application/runtime/runtime_recovery.py:390-391, 459-466` | A fork requires an identical run-plan digest, so any patched fork is rejected, and the target epoch is not checked to be 1. This contradicts REQ-CP-EXEC-012. The service is not composed in production. | RRM-006 |
| 13 | `app/application/operations/journaled_operation_execution.py:327` | `technical_attempt` is hard-coded to 1. This contradicts `CON-CP-TEMPORAL-IDENTITY-V1` (Activity attempt lineage). | RRM-003 |
| 14 | `docs/migrations_instructions/implementation_work_packages_v2/TRACEABILITY.md:56-62` | Draft REQ-CP-CS-001..007 are recorded "accepted". CS-006 seeding and the CS-007 resume gate are not implemented. | Reviewer annotation (§5) |

## 8. Open decisions (each with a recommendation)

1. **Root `checkpoint_ns`.** Recommend `""`, with isolation by a namespace-derived `thread_id` (DA-016). A custom root namespace breaks `get_state` in LangGraph 1.2.10. RRM-003 re-verifies this on the Postgres saver.
2. **Storage shape.** Recommend new forward-only unit, attempt, transition, and receipt tables. Retire the Agent Server-shaped `0012` runtime tables, extend the `0012` operation journal, and extend the `0014` lineage, incident, and fork tables, as §3 rows 47-54 describe. Physical names are owned parameters (readiness §10.1).
3. **When phase changes for pause, resume, and wait release.** Recommend on the `applied` fact. Cancel moves to `cancelling` on acceptance (RUN-004 clarified).
4. **GoalDirected pause and budgets.** Recommend releasing unstarted-iteration reservations on pause and re-reserving on resume. The run-level baseline is kept.
5. **First fork boundary.** Recommend settled boundaries only: StageGraph after stage settlement, and GoalDirected after verifier settlement. Cognition starts fresh, or from a handoff for GoalDirected. Prove an explicit terminal `cognitive_seed` for at least one family if it fits in RRM-006, otherwise record it as deferred. Executable intermediate-checkpoint forks stay deferred.
6. **Active async children at fork time.** Recommend prohibiting the fork (`snapshot_not_quiescent`) rather than defining child transfer (EXEC-016).
7. **Run-control command shapes.** Recommend reusing the existing `satisfy_wait`, `pause`, `resume`, and `cancel` lifecycle actions, adding receipts and delivery, and retiring the parallel `graph_runtime` intervention commands.
8. **`durability="sync"`.** Recommend it for every BellLabs invocation. It costs latency, but at-least-once cognition is bounded to one step.
9. **Verifier session sharing.** Keep the current role-scoped verifier namespace per session generation, since BP-020 accepted it. A per-iteration verifier namespace can be decided later, without changing the contracts.
10. **Unit kinds beyond the families.** The generic artifact route (`app/api/run_control.py:329-361`) submits a separate generic artifact workflow, not an `OperationWorkflow`. It is therefore outside REQ-CP-EXEC-013 and outside mission scope. A contract revision would be needed before it ran as a runtime unit.
11. **Cognitive schemas and ADR-0004.** Narrowed acceptance, per §5.
12. **Mid-invocation steering.** Remains deferred. Cognition-targeted commands are rejected `not_applicable` (EXEC-006 clarified).

## 9. Independent review disposition (verdict `accept_with_fixes`)

All findings were applied in meta `18f44b7` and in this branch's fix commit. No finding is disputed.

| # | Finding | Resolution |
|---|---|---|
| 1 | A GoalDirected generation boundary changed `unit_key` | **Applied.** GD-012, the `CON-CP-CHECKPOINT-LINEAGE-V1` namespace rules and grammar, DA-016, and EXEC-005 now state the rule. A generation boundary keeps `unit_key` and runs fresh from the handoff in `belllabs/goal/{run}/epoch/{epoch}/unit/{unit_key}/gen/{execution_generation}`. The next iteration is admitted into a new `session_generation`. |
| 2 | Namespace head undefined; invocations unpinned | **Applied.** DA-017 defines the head as the last accepted transition's `result_key`, with the checkpointer as evidence only. Every submission and resumption is pinned to a `checkpoint_id`, and a stamped non-descendant is `in_doubt`. The `not_submitted`, `interrupted`, and `terminal_unobserved` rows and GD-012 were updated to match. |
| 3 | Async `in_doubt` had no exit and was unrecorded | **Applied.** DA-008 and 04 § State and lifecycle add the `in_doubt` value. It exits by observation, `adopt_provider_run(run_id)` (other runs cancelled, their usage pending), or `orphan_child`. Recorded in the 04 amendment record, the README summary, and disposition rows 19 and 54. |
| 4 | `accept_descendant` undefined | **Applied** in `CON-CP-CHECKPOINT-LINEAGE-V1` operator decisions. |
| 5 | EXEC-005 "exact compatible source checkpoint" had no mechanism | **Applied.** Replaced with a terminal result checkpoint seeded under the `cognitive_seed` rules of EXEC-012. |
| 6 | Cancel `applied`; run control as the target boundary | **Applied** in `CON-CP-WORKFLOW-MESSAGE-V1`. |
| 7 | `insufficient_budget` rejection reason | **Applied** to the closed reason set and GD-011 resume. |
| 8 | Run phase while a unit is `in_doubt` | **Applied** in RUN-007, with the `operator_reconciliation` wait condition recorded as new in `CON-CP-LIFECYCLE-V1`. |
| 9 | Over-broad post-dispatch `in_doubt` rule | **Applied.** A post-dispatch exception settles `failed` when there is no terminal result and every effect claim is settled; otherwise it is `in_doubt`. §7 row 2 was updated. |
| 10 | Reuse matching | **Applied** in EXEC-012: identity equality after substituting the run and epoch. |
| 11 | EXEC-008 step 3 safe-point wording | **Applied.** The adapter interrupts the in-flight step if necessary and records the latest durable checkpoint. |
| 12 | Int Search Attribute limit and test environments | **Applied.** Temporal `v1.31.0` `schema/postgresql/v12/visibility/schema.sql` pre-allocates 3 Int and 10 Keyword custom columns; the repository compose files use `DB: postgres12` and server `1.31.0`. `BellLabsSemanticAttempt` is dropped, leaving 2 Int and 7 Keyword attributes. EXEC-015 adds a composition Search Attribute policy: `required` for production and persistent namespaces, and for `start_local` fixtures that register attributes; `disabled` for `start_time_skipping` and replay. History is replayed under its recorded policy. No local Temporal image was present, so the schema was read from the Temporal source tag. |
| 13 | Disposition rows 2 and 23–27 | **Applied.** Row 2 is now reuse (out of mission), because `GraphIdentity` is used by `GraphAssemblyDefinition` (`definitions.py:411`) and the coordinator `RunPlanV3` launch (`coordinator_launch.py:36, 102`). Rows 23–27 note the production `/v2/graph-runtime/schemas` export (`server.py:23, 244`). Mirrored in the meta index. |
| 14 | Spec 05 and ADR-0004 wording | **Applied.** CS-004 is labelled amended ("…unless the colliding channel definitions are identical"). ADR decision 6 now reads "to be enforced by RRM-003/004". The CS-006 deferral names the Deep Agents default (`deepagents/middleware/subagents.py:484, 537`: `messages`, `todos`, `structured_response`, and private channels excluded; returned channels merged back). |
| 15 | Acceptance mechanism | **Applied without flipping.** Spec 05 has `deferred_requirements: [REQ-CP-CS-006, REQ-CP-CS-008]`, and ADR-0004 has `deferred_decisions`. Both carry status-line comments, so acceptance changes only status lines (§5). `TRACEABILITY.md` CS-004, CS-006, and CS-007 rows are annotated in this branch. |
| 16 | Hygiene | **Applied.** RUN-007 and DA-018 now cite `reconcile_unit` in `CON-CP-LIFECYCLE-V1`. `reconcile_unit` is marked new. EXEC-005 states its reinterpretation of "disruptive restart". A `reconcile_unit` test seam was added (§4, RRM-004, `tests/unit/run_control/test_run_control.py`). RUN-011 historical reads go through the saver (`aget_tuple`/`alist`). The generic artifact route is stated to be outside EXEC-013 (§8 item 10 and `CON-CP-RUNTIME-UNIT-V1`). Disposition rows 50–51 require the new migration to relax FKs to retired tables. The "12 decisions" count was already correct in §8 (the earlier report summarized 11 lines); no change was needed. |

### 9.1 Re-review (verdict `accept_with_fixes`, no blocking items) and acceptance

The re-review confirmed that blocking findings 1–3 are resolved everywhere they appear. The coordinator applied its remaining wording fixes in the acceptance commit `6c89143`:

| # | Finding | Resolution |
|---|---|---|
| R1 | The Search Attribute policy must be deterministic under replay | **Applied** to EXEC-015. The policy is carried in the root, family and `OperationWorkflow` inputs and through Continue-As-New. Workflow code never reads it from worker configuration. An absent field means `disabled`, and production composition rejects `disabled`. |
| R2 | The async `in_doubt` exits contradicted `orphaned` and DA-019 | **Applied** to DA-008. The observation exit requires the served graph identity to verify under DA-019. `orphan_child` cancels every provider run carrying the spawn key and records their usage as pending. `orphaned` is also reachable after `orphan_child`. |
| R3 | The DA-017 head rule was too broad | **Applied.** It now applies to a stamped root checkpoint of the unit generation that does not descend from that generation's expected source. |
| R4 | GD-012 versus the EXEC-005 seed | **Applied.** GD-012 runs fresh-from-handoff unless the accepted decision names a seed under REQ-CP-EXEC-005. |

The spec statuses were flipped in the same commit, and the spec branch was merged into meta `main` at `a50d833`.
