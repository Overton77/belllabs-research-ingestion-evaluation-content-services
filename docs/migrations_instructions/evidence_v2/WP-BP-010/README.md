# WP-BP-010 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-11  
Qualification: `QUAL-BP-STAGEGRAPH-SEMANTICS-RECOVERY`  
Framework baseline: Temporal `1.30.0`, Deep Agents `0.7.5`
Audited repository head: `8a05094509ad015422a62c0f9cd534fc21182447` on `main`

## Canonical implementation

- `StageGraphBlueprint` is the strict V2 contract. Publication validation covers exact stages,
  dependencies, joins, slots, variants, waits, policies, cycle contracts, obligation rows,
  complete-key collisions, NFC identifiers, acyclicity, and canonical set-like ordering while
  preserving authored semantic arrays.
- `StageGraphInterpreter` is pure. It owns dependency and join truth tables, deterministic
  weighted-group-ring selection, authoritative-cursor advancement, typed operation identities,
  result and late-result dispositions, durable producer liabilities, bounded stage/workflow
  cycles, minimal invalidation/reuse, and obligation-based completion proposals.
- `StageGraphWorkflow` is the only StageGraph macro runtime. It starts durable
  `OperationWorkflow` children, reconciles completions incrementally, releases `any`/`minimum`
  consumers without a frontier barrier, keeps slow-sibling liabilities, applies accepted cycles,
  preserves semantic state through Continue-As-New, and exposes durable wait/cancel signals.
- `StageGraphDecisionService` commits initialization, admission/reservation, result disposition,
  operation-settlement evidence, output/obligation evidence, cycles, and terminalization through
  Run Control and the atomic family-admission seam.
- `StageGraphOperationPreparationService` rebuilds and persists each exact operation and Deep
  Agent binding with the StageGraph semantic identity, current control revision, reservation,
  workspace, ERC, and cycle objective before child admission.
- The legacy flat StageGraph workflow, direct semantic-activity macro registration, Agent Server
  StageGraph suites, ambiguous concurrency setting, and Agent Server StageGraph configuration ID
  are absent from the accepted worker path.

## Requirement-to-evidence map

| Requirement | Executable evidence |
|---|---|
| REQ-BP-SG-001 | `tests/test_stagegraph_v2.py`: structural, NFC, duplicate complete-key, normalization, semantic-array, and digest tests |
| REQ-BP-SG-002 | complete dependency-class and `all`/`any`/`minimum(k)` truth tables |
| REQ-BP-SG-003 | canonical ordering, order-independent digest, typed identity, and deterministic frontier tests |
| REQ-BP-SG-004 | real Temporal incremental `any` release and captured-history replay in `tests/test_wp_bp_010_temporal.py` |
| REQ-BP-SG-005 | initial/resumed weighted-ring, blocked-candidate, concurrency-slot, and admission-only cursor tests |
| REQ-BP-SG-006 | technical retry, stage-cycle, and workflow-cycle identity separation |
| REQ-BP-SG-007 | bounded stage-cycle objective, prior lineage, reuse, cycle/no-progress stopping, authored precedence preservation, reducer-governed budget usage, materialization, and Temporal execution tests |
| REQ-BP-SG-008 | minimal descendant invalidation, immutable unaffected-output reuse, and workflow-cycle Temporal test |
| REQ-BP-SG-009 | late-veto/rule precedence, slow-sibling cancellation, Continue-As-New, wait/replay, and worker-loss qualification |
| REQ-BP-SG-010 | accepted obligation evidence, failed-result rejection, exact disposition, liability closure, and terminal proposal tests |

Primary suites: `tests/test_stagegraph_v2.py`, `tests/test_wp_bp_010_temporal.py`,
`tests/test_wp_bp_010_recovery.py`, `tests/test_atomic_family_admission.py`,
`tests/test_coordinator_temporal_runtime.py`, and `tests/test_operation_execution.py`.

## Verification recorded

```text
uv run ruff check app tests scripts
All checks passed!

uv run mypy app
Success: no issues found in 318 source files

uv run pytest -q tests/test_stagegraph_v2.py tests/test_wp_bp_010_recovery.py \
  tests/test_wp_bp_010_temporal.py tests/test_atomic_family_admission.py \
  tests/test_coordinator_temporal_runtime.py tests/test_operation_execution.py
126 passed, 1 skipped
```

The one Windows skip is the explicitly Linux-qualified worker-loss case. The same repository was
mounted into WSL2 (`Linux 6.18.33.2-microsoft-standard-WSL2`, Python `3.12.11`), using a native
Linux environment and independent Temporal client connections for the original and replacement
workers. Both workers disabled sticky workflow caching so the accepted projection had to replay.

```text
wsl -d Ubuntu -- bash -lc 'cd /mnt/c/Users/Pinda/Proyectos/Biotech/biotech-research-ingestion-evaluation-system && \
  /home/overton/.venvs/biotech-stagegraph/bin/python -m pytest -q \
  tests/test_wp_bp_010_recovery.py tests/test_wp_bp_010_temporal.py --tb=short'
7 passed

uv run pytest -q tests/test_linked_runs.py --tb=short
13 passed
```

The recovery test starts the accepted projection under one worker, establishes a separately
connected replacement poller, stops the original worker, signals the declared wait, and proves
that replayed child execution reaches a terminal completion proposal with no pending dependency
or producer-liability IDs. The linked-run suite proves the deletion gate: linked children now
always launch through `BellLabsRunWorkflow`; the production-accessible direct-family fixture
switch was removed.

## Credential-gated live acceptance

The exact live path was executed through BellLabs FastAPI run-request submission, transactional
Run Control admission, `BellLabsRunWorkflow`, the real `StageGraphWorkflow`, durable
`OperationWorkflow` children, and the accepted Deep Agents adapter with real `gpt-5.6-luna` calls.
The slow branch's real model call completed before a controlled synchronization gate held its
operation result. Temporal child history—not provider latency—proved the downstream child started
at history event index `48`, before the slow child completion at index `63`.

```text
$env:BELLABS_RUN_WP_BP_010_LIVE='1'
uv run pytest -q tests/acceptance/control_plane/test_wp_bp_010_live.py -s --tb=short
1 passed in 32.80s

WP_BP_010_LIVE_EVIDENCE={
  "accepted_obligations": ["live-stagegraph-evidence"],
  "accepted_outputs": [
    "artifact:wp-bp-010-live:downstream",
    "artifact:wp-bp-010-live:fast",
    "artifact:wp-bp-010-live:slow"
  ],
  "downstream_start_history_index": 48,
  "family_workflow_id": "family/c8504e72-17bd-52f6-ad3c-5d9aca1b05f5/1",
  "model": "gpt-5.6-luna",
  "real_model_stages": ["downstream", "fast", "slow"],
  "root_workflow_id": "belllabs-run/c8504e72-17bd-52f6-ad3c-5d9aca1b05f5",
  "run_id": "c8504e72-17bd-52f6-ad3c-5d9aca1b05f5",
  "slow_completion_history_index": 63,
  "terminal_outcome": "completed"
}
```

No credential or provider response content is retained in this evidence. The accepted output refs,
current obligation evidence, and reducer-authorized terminal outcome satisfy the live gate.
