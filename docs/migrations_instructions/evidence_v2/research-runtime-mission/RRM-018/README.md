# RRM-018 implementation evidence

Disposition: ready_for_review (RRM-018 and RRM-019, implemented together; independent review pending)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-018 persist an unchanged Goal Revision idempotently in MongoDB, and RRM-019 terminalize GoalDirected with the accepted output set. Requirements: REQ-BP-GD-002 (immutable Goal Revisions), REQ-BP-GD-003 (iterations independently durable), REQ-BP-GD-004 (independent verification is mandatory), REQ-BP-GD-010 (stopping produces a proposal), REQ-CP-RUN-005 (terminality follows accepted evidence).
Base revision and head revision: base integration `8778632` (includes RRM-016 at `f99ac1d`). Code commit: `d552278`. Documentation commit: the one that adds this README (it changes no code). **Tested code head: `d552278`.** Not merged (the coordinator owns review and merge).
Framework/package baseline: the synced venv from the committed `uv.lock` (no dependency change); CPython 3.12 (Codex runtime); temporalio 1.30.0, deepagents 0.7.5, langgraph 1.2.10, pydantic 2.13.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-018`, branch `wp/rrm-018-019-goal-directed-multi-iteration`, created by the coordinator from integration `8778632`; clean at kickoff.
- The main checkouts were not touched; `.env` was never copied or printed (the service gate loads it with `--env-file`).
- Disposable stack only (`127.0.0.1:55432/belllabs`, `127.0.0.1:27017`), always under the shared stack lock (`stack_lock.py acquire/release RRM-018`); no container was created, stopped or removed. Temporal ran only as `start_local` and the time-skipping test server.
- Concurrency with RRM-008 (running cancellation, another branch): the edits stay out of `_enter_cancellation`, `_stop_for_cancellation`, `operation.py`, heartbeats and run control. The only workflow edit is in the terminal-output block of `GoalDirectedWorkflow.run` (after the closing drain, before `terminalize`) plus one patch constant.

## Implemented contracts and seams

### RRM-018: an unchanged Goal Revision re-persists idempotently

**The diagnosis had two causes, not one.** With `recorded_at` excluded from the comparison, the demonstration still failed at iteration 2's executor preparation with `IdempotencyConflict`:
1. **Observation time.** `GoalDirectedOperationPreparationService.prepare` persists the active Goal Revision before every executor operation with `recorded_at = request.decided_at` (that iteration's `workflow.now()`), and `_insert_exact` compared it.
2. **Container types (new).** `document_payload` is a dataclass `asdict`, which holds tuples. MongoDB returns arrays as lists, so the stored payload never equalled the new one after a round trip, even for identical content.

**Fix** (`app/application/orchestration/mongo_goal_directed_repository.py`, `_insert_exact`): on a duplicate key, both documents are compared by `_immutable_identity`, which is `canonical_json(document.model_dump(mode="python", exclude={"id", "recorded_at"}))`. This is the canonical form `document_digest` is already computed over (`sha256_digest` uses `canonical_json`): tuples and lists are the same array, sets are sorted, datetimes are UTC. The first record, and its `recorded_at`, is kept.

- **Exact-replay guarantee kept for genuine changes.** A different payload, digest, keys, revision number or envelope under the same `(request_scope, run_id, goal_revision_id)` still raises `IdempotencyConflict`.
- **Scope.** The rule applies to every GoalDirected immutable document kind that goes through `_insert_exact` (revision, iteration, handoff, verification, template): the observation time is never part of their identity. Only the revision is re-persisted in practice today; the others are covered for recovery and re-admission paths.
- **Persist once vs idempotent.** I kept "persist every executor preparation, idempotently" rather than "persist once per revision", because the persist-every-time path is what makes a recovered or re-admitted executor (RRM-016 review fix 2) safe, and it needs no workflow change.
- **No workflow command change**, so no patch. No digest is built over `model_dump(mode="json")` (RRM-015 guard).
- No datetime is carried inside any GoalDirected payload (templates are JSON-dumped), so the canonical comparison has no timezone dependence on the Mongo client; production composes a `tz_aware` client anyway.

### RRM-019: a completed run promotes exactly the verified final outputs

**Decision.** A completed GoalDirected run promotes as accepted output evidence, and its terminalization proposal names as `valid_output_refs`, exactly **the final executor's outputs that the accepting verifier admitted**. Earlier iterations' outputs remain immutable lineage refs in the family result (`GoalDirectedRunResult.output_refs`, the ordered union, unchanged), but they are not promoted.

**Spec basis** (`SPEC-BP-GOAL-DIRECTED` v2 and `SPEC-CP-RUN-CONTROL`):
- REQ-BP-GD-004: "Completion MUST require an independently bound verifier whose authority, model/tools/capabilities, inputs, rubric, and output contract are exact and whose result is accepted by application authority." The accepting verifier's inputs are exactly the final executor's outputs: the interpreter requires `verification.admitted_executor_output_refs == execution.output_refs` (`apply_verification`).
- Invariant 2: "An executor's completion claim is never sufficient; a separately bound verifier is mandatory." An earlier iteration's outputs were verified only by a verifier whose decision was not `accepted` (otherwise the run would have completed there). Promoting them would make an output valid on the executor's word alone, or on a rejecting verifier's decision.
- REQ-BP-GD-010: "The interpreter MUST emit a typed terminalization/continuation proposal binding the current goal revision, verifier decision, obligation evidence, outputs, …; only the lifecycle reducer MAY terminalize." The proposal binds one verifier decision, the final one; its outputs are the ones that decision covers (`_terminalization_proposal` already used `execution.output_refs`).
- REQ-CP-RUN-005: "Only the reducer MAY assign one terminal outcome after validating … accepted evidence". The reducer's `terminal_output_mismatch` check (`set(valid_output_refs) == accepted output evidence`) is the contract both sides now satisfy. It was correct and is unchanged.
- REQ-BP-GD-003 ("immutable output/evidence/usage refs" per iteration) is why earlier outputs stay in the result and in the Mongo iteration documents as lineage.

**The spec does not literally say "final iteration's outputs".** The rule above is derived from REQ-BP-GD-004, invariant 2 and REQ-BP-GD-010; it is recorded as open decision 1 for the coordinator. The rejected alternative, promoting every iteration's outputs, would need the proposal to name outputs no accepted verifier decision covers.

**Consequence for authors.** An output an earlier iteration produced, and that should be a run output, must be named again by the final executor so the final verifier admits it. This is what the WP-BP-020 live instructions already do ("preserve" iteration 1's ref).

**Fix** (`app/temporal/workflows/goal_directed.py`): `accepted_output_refs = terminalization_proposal.output_refs if workflow.patched("rrm-019-verified-terminal-outputs") else result.output_refs`. It feeds the evidence digest and the `record_output_evidence` promotions, and `terminalize` already sends `proposal.output_refs` as `valid_output_refs`. Promotion and proposal are therefore the same set. Partial or failed outcomes still promote nothing. The interpreter, the contracts and the reducer are unchanged.

**Replay.** Pre-patch histories (no marker) promote the union, as before. When every iteration reuses one output ref, the union equals the final outputs, so the command identities (`goal:output:{ref}:{digest}`, `goal:obligation:{ref}:{digest}`) are identical on both paths; only the marker is new.

## Requirement-to-evidence map

| Requirement | Test | Observed assertion |
|---|---|---|
| REQ-BP-GD-002/003 (RRM-018 reproduction, Mongo-gated) | `tests/integration/mongodb/test_goal_directed_documents_mongodb.py::test_unchanged_goal_revision_persists_again_at_the_next_iteration` (strict xfail removed) | The same revision persisted at `NOW`, then at `NOW + 1 min`, returns the same ref; MongoDB holds one `GoalRevisionDocument` for the identity, with `recorded_at == NOW` |
| REQ-BP-GD-002: a genuine change still conflicts (Mongo-gated) | `…::test_changed_goal_revision_under_the_same_identity_still_conflicts` | A revision with different `tactical_changes` under the same identity raises `IdempotencyConflict("…immutable document identity conflict")` at the same time and at a later time |
| RRM-018 identity rule (hermetic) | `tests/unit/orchestration/test_rrm_018_goal_document_identity.py::test_observation_time_and_container_types_are_not_identity` | The payload holds tuples; its JSON round trip (lists, as MongoDB returns it) and a later `recorded_at` have the same `_immutable_identity` |
| RRM-018 identity rule (hermetic) | `…::test_changed_revision_content_is_a_different_identity` | Changed content, or a drifted `document_digest`, gives a different identity |
| REQ-BP-GD-004/010, REQ-CP-RUN-005 (RRM-019 reproduction) | `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py::test_iterations_with_distinct_output_refs_terminalize` (strict xfail removed; production activities, real run-control authority, journaled boundary, time-skipping) | Two iterations, outputs `artifact:rrm016:1` then `artifact:rrm016:2`. Result `output_refs == (…:1, …:2)` (lineage). Iteration 1's verifier is not `accepted`; the final one is, with `admitted_executor_output_refs == (…:2,)`. Proposal `output_refs == (…:2,)`; run `COMPLETED`; `accepted_output_evidence == […:2]`; every `goal:output:` command is for `…:2`. Patch `rrm-019-verified-terminal-outputs` present; the history replays |
| Replay: RRM-019 post-change capture | `…::test_rrm_019_distinct_outputs_history_replays` | New fixture `tests/fixtures/histories/rrm019_post_change/goal_directed_distinct_output_refs.run1.json` (captured with `RRM019_CAPTURE_HISTORY_DIR`) carries the patch, promotes only `artifact:rrm016:2`, and replays |
| Replay: pre-change and RRM-016 histories | `…::test_pre_change_goal_directed_history_keeps_its_own_usage_commands`, `…::test_post_change_goal_directed_history_replays_on_the_journaled_path`, `…::test_stale_version_retry_history_replays`, `tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py` (five histories, two GoalDirected) | All replay unchanged against the patched workflow |
| Real Temporal, PostgreSQL authority, Mongo family documents, worker restart | `tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py::test_journaled_goal_directed_run_on_real_temporal` (extended) | See "Live runtime qualification" |

## Changed paths and migrations

**Migrations: none.** No storage schema, index or contract changed.

**Shared seams: none edited.** The run-control reducer, service, repositories and API, the GoalDirected interpreter and contracts, `operation.py`, heartbeats and registration are unchanged. RRM-019 needed no reducer edit: the reducer's rule was right and the family now satisfies it.

Code:
- `app/application/orchestration/mongo_goal_directed_repository.py`: `_immutable_identity` and its use in `_insert_exact` (RRM-018).
- `app/temporal/workflows/goal_directed.py`: `VERIFIED_TERMINAL_OUTPUTS_PATCH` and `accepted_output_refs` in the terminal-output block of `run` (RRM-019).

Tests and fixtures (none weakened):
- `tests/integration/mongodb/test_goal_directed_documents_mongodb.py`: RRM-018 strict xfail removed; the reproduction now also asserts one stored document with the first `recorded_at`; a new conflict test.
- `tests/unit/orchestration/test_rrm_018_goal_document_identity.py`: new, hermetic (2).
- `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py`: RRM-019 strict xfail removed; the test now asserts the promoted set, the patch and replay, and can capture the history; a new replay test for the captured history.
- `tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py`: the RRM-018 workaround (in-memory family documents) is removed. The demonstration now uses `MongoGoalDirectedDocumentRepository` as documents and distinct output refs, keeps every RRM-016 assertion, and adds the RRM-018/019 assertions.
- `tests/fixtures/goal_directed_journaled.py`: `compose_goal_directed(documents=…)` accepts any `GoalDirectedDocumentRepository`; comment on `stable_output_ref`.
- `tests/fixtures/histories/rrm019_post_change/goal_directed_distinct_output_refs.run1.json`: new captured history (392 KB; checked for credentials and DSNs, none present).

Tickets: `issues/18-…md` and `issues/19-…md` set to `implemented; independent review pending`, with a resolution note and ticked checklists.

## Deterministic verification

All runs from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time. Full runs and DSN runs under the stack lock.

| Gate | Command | Result |
|---|---|---|
| Ruff | `uv run --no-sync ruff check app tests scripts` | All checks passed |
| Mypy | `uv run --no-sync mypy app` | Success: no issues found in 366 source files |
| Owning suites (hermetic) | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LANGSMITH_TRACING=false uv run --no-sync pytest -q tests/unit/orchestration tests/unit/operations tests/unit/run_control tests/integration/temporal/test_rrm_016_goal_directed_journaled.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_rrm_007_boundary_interventions.py tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py tests/integration/temporal/test_rrm_005_search_attributes.py tests/integration/temporal/test_coordinator_temporal_runtime.py tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py tests/acceptance/control_plane/test_wp_cp_040.py tests/unit/control_plane/test_digest_set_order_guard.py tests/unit/control_plane/test_contract_digest_set_order.py tests/integration/mongodb/test_goal_directed_documents_mongodb.py` | **424 passed, 2 skipped** (the two Mongo-gated tests), 0 xfailed |
| Replay subset | see Replay and recovery artifacts | 13 passed |
| Mongo tests and demonstration (under the lock, both DSNs exported) | `uv run --no-sync pytest -q -s tests/integration/mongodb/test_goal_directed_documents_mongodb.py tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py` | **3 passed** |
| Full hermetic (under the lock) | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q` | **981 passed, 75 skipped, 2 xfailed**, 0 failed (129 s) |
| Full with services and `.env` (under the lock) | `TEST_APPLICATION_POSTGRES_DSN=<disposable> TEST_MONGODB_URI=<disposable> BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q` (DSNs exported explicitly) | **1026 passed, 30 skipped, 2 xfailed**, 0 failed (249 s) |
| `git diff --check` | | clean |

Deltas against the integration baselines (`8778632`: hermetic 977/74/3, services 1020/30/4):
- **Hermetic: +4 passed** (the RRM-019 test, now passing; its replay test; the two RRM-018 unit tests), **+1 skipped** (the new Mongo-gated conflict test), **−1 xfailed** (RRM-019).
- **Services: +6 passed** (the RRM-018 and RRM-019 reproductions, the conflict test, the replay test and the two unit tests), **−2 xfailed**.
- The remaining xfails predate this work. No test was skipped, deselected or weakened, and no marker was added. The RRM-007 time-skipping wall-clock timeout did not recur.

## Live runtime qualification

No live model call. **Spend: USD 0.**

**Real Temporal demonstration** (`test_rrm_016_goal_directed_demo.py`, extended; under the lock, both DSNs exported):
- **Runtime:** `WorkflowEnvironment.start_local`. The root is started by `TemporalWorkflowSubmitter`; pause and resume go through `POST /run-control/v1/runs/{run}/commands`.
- **Authority:** application PostgreSQL (run control with the family writer pool, boundary ledger, operation journal, checkpoint lineage).
- **Cognition:** a real `create_deep_agent` graph over a real `AsyncPostgresSaver` with the deterministic `GoalScriptedModel(stable_output_ref=False)`.
- **MongoDB:** OEB bindings, GoalDirected templates **and the family documents** (`MongoGoalDirectedDocumentRepository` as `documents`; before RRM-018 they had to be in memory).
- Iteration 1 runs on worker `rrm016-worker-1`; the run pauses at the boundary; worker 1 is stopped; a fresh composition (`rrm016-worker-2`) resumes iteration 2. Each executor preparation persists Goal Revision 1, once per worker.

Printed record (`RRM-016 EVIDENCE demonstration`, new keys):
- `rrm018_mongo_documents`: `revisions ["goal-revision:1"]` (one document), `executor_preparations_persisting_revision 2`, `revision_recorded_at` = iteration 1's time, `iterations 2`, `verifications 2`.
- `rrm019_outputs`: `iteration_outputs [["artifact:rrm016:1"], ["artifact:rrm016:2"]]`, `family_result_output_refs ["artifact:rrm016:1", "artifact:rrm016:2"]`, `proposal_output_refs ["artifact:rrm016:2"]`, `accepted_output_evidence ["artifact:rrm016:2"]`.
- `patches ["rrm-016-journaled-goal-settlement", "rrm-019-verified-terminal-outputs"]`; root and family replayed (1, 1); `terminal_outcome` `completed`.
- RRM-016 facts unchanged: four operations `completed` with `{"tokens.total": 10}`; consumed 40; nothing reserved; four settled effects; four accepted settlement evidences; receipts `pause 1 accepted/delivered/applied`, `resume 2 accepted/delivered/applied`; executor session chain across the restart; family usage commands only `goal:usage:baseline`.

**Negative control.** The first demonstration run, with only `recorded_at` excluded, failed at iteration 2's `goaldirected.prepare_executor` with `IdempotencyConflict`, which is how the tuple-vs-list cause was found.

**WP-BP-020 live harness:** unchanged and not run (flag-gated, needs OpenAI spend). Unresolved gate, as in RRM-016.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LANGSMITH_TRACING=false uv run --no-sync pytest -q tests/integration/temporal/test_rrm_016_goal_directed_journaled.py tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_wp_bp_010_temporal.py tests/integration/temporal/test_linked_runs.py tests/unit/operations/test_operation_execution.py -k "replay or Replayer or replays or history or histories"` | **13 passed** (the RRM-016 subset of 12 plus the new RRM-019 history) |

Covered:
- the five RRM-007 pre-change histories, including `goal_directed_two_iterations.run1.json`, which completes and promotes outputs on the pre-patch path;
- both RRM-016 post-change histories (`rrm016_post_change/`);
- the new RRM-019 capture;
- the WP-BP-010/020 in-run replays. In-run replays also pass in the RRM-019 test and the demonstration.

**Patching.** One new patch, `rrm-019-verified-terminal-outputs`, evaluated once at the terminal-output block, only when a terminalization proposal exists. No activity, signal, query, update or workflow type changed; no input or result contract changed. RRM-018 changed no workflow command.

**Deploy note.** No drain is needed for this change. An in-flight run that reaches terminalization after the deploy takes the patched path. With one stable output ref its commands are identical; with distinct refs it would have failed `terminal_output_mismatch` before, and now completes.

## Replacement and deletion checks

- **Replaced for new histories:** promotion of the union of every iteration's outputs, which is retained only for pre-patch histories.
- **Replaced:** whole-document equality in `_insert_exact`, now canonical-identity equality without the observation time.
- **Removed (test only):** the demonstration's in-memory family-document workaround.
- **Deleted:** nothing. No second lifecycle or document store.

## Unresolved risks and drift checks

**Open decisions (for the coordinator)**
1. **RRM-019 output rule.** "Verified final outputs" is derived, not quoted: the spec names "outputs" in the proposal (REQ-BP-GD-010) without saying which iterations'. Derivation: REQ-BP-GD-004, invariant 2, REQ-BP-GD-010, REQ-CP-RUN-005 (see Implemented contracts). Alternative: promote every iteration's outputs, which would require the proposal, and so the reducer's accepted set, to include outputs no accepted verifier decision covers.
2. **RRM-018 scope.** The observation-time exclusion applies to every GoalDirected immutable document kind, not only revisions. Alternative: restrict it to `GoalRevisionDocument`.

**Unresolved gates**
- `test_wp_bp_020_live.py`: not run (live provider, flag-gated).
- `test_wp_bp_010_recovery.py`: WSL-only, unchanged.
- The Agent Server endpoint suites: unchanged skips.

**What RRM-009 must know**
- `MongoGoalDirectedDocumentRepository` can now be the production `documents` for multi-iteration runs, and re-admission (RRM-016 review fix 2) re-persists safely.
- Only the final executor's outputs become run outputs. Production executor instructions or templates that want an earlier artifact as a run output must have the final executor name it again (the WP-BP-020 "preserve" pattern).
- `GoalDirectedRunResult.output_refs` remains the union (lineage). Consumers that need the accepted set must read `terminalization_proposal.output_refs` or run control's `accepted_output_evidence`, not `output_refs`.
- Keep `tests/fixtures/histories/rrm019_post_change/` replaying, together with `rrm016_post_change/` and the RRM-007 pre-change histories.

**For RRM-008 (concurrent)**: no overlap. The patch sits after the closing drain and before `terminalize`, outside `_enter_cancellation` and `_stop_for_cancellation`. A cancelled run never reaches the promotion block.

### Reusable seams (mission-horizon lens)

- `_immutable_identity` (canonical identity without observation time): the comparison any immutable Mongo document repository can use when its payloads round-trip containers.
- `RRM019_CAPTURE_HISTORY_DIR` capture plus a replay test: the same capture-and-pin pattern as RRM-016.

## Final disposition

ready_for_review
