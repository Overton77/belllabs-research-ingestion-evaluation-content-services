# RRM-018 — Persist an unchanged Goal Revision idempotently across iterations

**What to build:** A GoalDirected run of two or more iterations whose Goal Revision does not change persists that revision once (or idempotently) in MongoDB, so the second executor preparation does not fail with `IdempotencyConflict`.

**Blocked by:** None.
**Blocks:** Any GoalDirected run of more than one iteration on the MongoDB document repository: RRM-009 production composition and the GoalDirected company fixture (RRM-011). Whether it blocks RRM-010 is the coordinator's decision.
**Status:** implemented; independent review pending (found by RRM-016, 2026-10-02; implemented 2026-10-02 together with RRM-019; evidence `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-018/README.md`)
**Branch:** `wp/rrm-018-019-goal-directed-multi-iteration` (shared with RRM-019)
**Authority:** REQ-BP-GD-002 (immutable Goal Revisions), REQ-BP-GD-003 (iterations are independently durable); `workflow-blueprints/goal-directed.md` (MongoDB/Beanie owns immutable goal revisions)

## Diagnosis (RRM-016, 2026-10-02)

- `GoalDirectedOperationPreparationService.prepare` persists the active Goal Revision before every executor operation, with `recorded_at = request.decided_at` (that iteration's `workflow.now()`), in `app/application/orchestration/goal_directed.py`.
- `MongoGoalDirectedDocumentRepository.persist_revision` inserts a `GoalRevisionDocument` keyed by `(request_scope, run_id, goal_revision_id)`. On a duplicate key, `_insert_exact` compares the whole stored document with the new one, `recorded_at` included (`app/application/orchestration/mongo_goal_directed_repository.py`). The second iteration of an unchanged revision therefore raises `IdempotencyConflict("GoalDirected immutable document identity conflict")`, and `goaldirected.prepare_executor` fails all three attempts.
- Every existing multi-iteration GoalDirected proof used in-memory or fake documents, so this was never exercised. RRM-016's real-Temporal demonstration hit it on iteration 2 and now uses in-memory family documents (bindings and templates stay in MongoDB).
- Reproduction: `tests/integration/mongodb/test_goal_directed_documents_mongodb.py::test_unchanged_goal_revision_persists_again_at_the_next_iteration`, marked `xfail(strict=True, raises=IdempotencyConflict)` citing this ticket. Remove the marker when fixed.

## Resolution (2026-10-02)

- **A second cause.** Excluding `recorded_at` alone does not fix the conflict. `document_payload` is a dataclass `asdict` holding tuples, and MongoDB returns arrays as lists, so the payloads never compared equal after a round trip. The first demonstration run with only `recorded_at` excluded still failed with `IdempotencyConflict`.
- **Fix.** `_insert_exact` (`app/application/orchestration/mongo_goal_directed_repository.py`) compares both documents in the canonical form `document_digest` is computed over (`canonical_json`), without `id` and `recorded_at`. The first record and its time are kept. A different payload, digest or key under the same identity still conflicts. The rule applies to every GoalDirected immutable document kind. No workflow command changed.

## Acceptance

- [x] Re-persisting an identical Goal Revision is idempotent (the observation time is not part of the immutable identity, or the revision is persisted once per revision), while a different payload under the same identity still conflicts.
- [x] The RRM-018 reproduction passes without its marker.
- [x] A two-iteration GoalDirected run on MongoDB family documents (for example the RRM-016 demonstration with `MongoGoalDirectedDocumentRepository` as `documents`) completes.
