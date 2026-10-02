# RRM-015 — Make every contract digest independent of set iteration order

**What to build:** every identity, fingerprint or idempotency digest computed from a Pydantic contract is stable across processes, whatever the string-hash seed.

**Blocked by:** None. Discovered by RRM-004 while diagnosing the intermittent RRM-003 merge-gate failure.
**Blocks:** No mission ticket. RRM-004 fixed the sites on the crash-recovery path; the remaining sites are latent.
**Status:** ready-for-agent
**Branch:** `wp/rrm-015-set-order-stable-digests`
**Authority:** idempotency and replay requirements already accepted: REQ-CP-RUN-003/008, REQ-CP-EXEC-004/005, and the CP-020 operation-journal invariants

## Diagnosis (RRM-004, 2026-10-01)

`model_dump(mode="json")` lists a `frozenset` or `set` in *iteration* order. That order depends on the per-process string-hash seed (`PYTHONHASHSEED`, random by default) and on how the set was built. `sha256_digest` sorts real sets, but after a JSON-mode dump it receives a list and cannot. So `sha256_digest(x.model_dump(mode="json"))` is not an identity when `x` contains set-valued fields:

- the same contract gets a different digest in two worker processes;
- a revalidated copy can differ from the original even in one process.

RRM-004 reproduced this failure in about 1 run in 8, with the traceback "stored operation authority result conflicts with exact replay command". It fixed the sites on the replay path through the new `contract_fingerprint` (`app/domain/control_plane/canonical.py`), which dumps in Python mode so that `_normalize` sorts sets:

- run-control command, run-request and family fingerprints;
- the journal coordinator's `_execute_replayable`;
- `OperationJournalMutation.validate`;
- the OEB request fingerprint in `operation_execution.py`.

The other sites below still hash a JSON-mode dump. Each must be audited for set-valued fields, reachable through nested models, and moved to `contract_fingerprint` (or an equivalent canonical form) where they are:

- `app/application/orchestration/linked_runs.py` (linked-run request fingerprint);
- `app/application/reference_research/service.py` (several digests);
- `app/application/runtime/runtime_decisions.py`;
- `app/application/runtime/postgres_runtime_authority.py` (lifecycle and budget digests);
- `app/application/runtime/postgres_stage3_kernel_repository.py`;
- `app/application/runtime/runtime_run_plan.py`;
- `app/application/schema/schema_catalog_build.py`, `schema_workspace_binding.py`, `graph_query.py` and `supporting_graph_reconciliation.py`;
- `app/application/workspaces/mongo_workspace_repository.py`, `workspace_materialization.py` (reservation tokens) and `sandbox_snapshots.py` (creation identity);
- `app/domain/coordinator/launch.py`;
- `app/domain/graph_runtime/contracts.py` and `definitions.py`.

Where a digest is already persisted, record a forward-compatibility decision: dual-read, or re-derivation in a migration.

## Acceptance

- [ ] Audit table: site → model → whether it holds a set (transitively) → fixed / not affected.
- [ ] A shared regression helper builds two equal sets with different iteration orders, as `tests/unit/run_control/test_run_control.py::test_command_fingerprint_is_independent_of_set_iteration_order` does, and every fixed site asserts digest equality with it.
- [ ] A static guard (a lint test or ruff rule) rejects new `sha256_digest(<model>.model_dump(mode="json"...))` calls on contracts.
- [ ] Full offline suite green. Evidence under `evidence_v2/research-runtime-mission/RRM-015/`.
