# WP-CP-020 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Contracts: `CON-CP-RUN-REQUEST-V1`, `CON-CP-LIFECYCLE-V1`,
`CON-CP-BUDGET-LEDGER-V1`, `CON-CP-DOMAIN-EVENT-V1`  
Schema versions: run-control contracts `1`, canonical JSON `canonical-json/1`  
Qualification: `QUAL-CP-TRANSACTIONAL-AUTHORITY`

## Requirement-to-test/evidence map

| Requirement | Executable evidence |
|---|---|
| REQ-CP-RUN-001 | `tests/test_run_control.py::test_admission_is_idempotent_and_rejection_creates_no_run`; PostgreSQL accepted/rejected and tenant-isolation cases in `tests/test_run_control_postgres_integration.py` |
| REQ-CP-RUN-002 | admission rollback injection, duplicate admission, and atomic start outbox assertions in `tests/test_run_control_postgres_integration.py` |
| REQ-CP-RUN-003 | concurrent CAS conflict in `tests/test_run_control_postgres_integration.py`; stable duplicate/rejected results in `tests/test_run_control.py` |
| REQ-CP-RUN-004 | `tests/test_run_control.py::test_wait_and_pause_are_separate_and_commands_use_optimistic_concurrency`; readiness/terminal-axis assertions in the same suite |
| REQ-CP-RUN-005 | stale evidence, exact obligation evidence, current control revision, exact Workflow Type digest, unsettled effects, unresolved async children, and terminal outcome tests in `tests/test_run_control.py` and `tests/acceptance/control_plane/test_wp_cp_020.py` |
| REQ-CP-RUN-006 | independent dimensions, hard caps, pending settlement, concurrent parent-child rollup, and oversubscription rejection in `tests/test_run_control.py` and the PostgreSQL qualification |
| REQ-CP-RUN-007 | `tests/acceptance/control_plane/test_wp_cp_020.py::test_effect_ambiguity_requires_one_authoritative_settlement`; PostgreSQL effect transaction rollback injection |
| REQ-CP-RUN-008 | `tests/test_run_control.py::test_outbox_consumers_deduplicate_detect_gaps_and_recover_in_order`; publish/ack ambiguity redelivery in `tests/acceptance/control_plane/test_wp_cp_020.py`; global sequence migration rehearsal |
| REQ-CP-RUN-009 | `tests/acceptance/control_plane/test_wp_cp_020.py::test_async_child_fact_cannot_mutate_parent_without_parent_decision` |
| REQ-CP-RUN-010 | `tests/test_run_control.py::test_terminal_finalization_freezes_evidence_and_uses_dedicated_budget` |

## Changed implementation and persistence paths

- `app/domain/run_control/contracts.py` — strict versioned admission, lifecycle, budget,
  consequential-effect, parent async-child, finalization, transition, and event contracts; raw
  secrets, PHI, and large/raw content are rejected.
- `app/domain/run_control/reducer.py` — the sole transition authority now owns effect
  claim/observation/settlement and parent async-child decisions in addition to lifecycle, budgets,
  evidence, finalization, and terminality.
- `app/application/run_control.py` — exact ERC admission, command coordination, effect queries, and
  at-least-once outbox relay.
- `app/application/run_control_repository.py` and
  `app/application/postgres_run_control_repository.py` — matching in-memory and PostgreSQL atomic
  boundaries for projection, budget/effect ledgers, command results, transitions, and outbox.
- `app/application/journaled_operation_execution.py` — provider dispatch now claims through run
  control before execution and submits observations, one effect settlement, and usage through
  canonical commands; its operation journal remains detailed evidence, not lifecycle authority.
- `app/api/run_control.py` — governed V1 admission/command/query surfaces plus effect state and
  effect-ledger queries and generated schemas.
- `app/migrations/0015_transactional_run_control_v1.sql` — canonical RLS-protected
  `effect_ledgers` and append-only `effect_ledger_entries` tables and indexes.
- `app/domain/orchestration/contracts.py`, `app/application/orchestration.py`, and the current
  Temporal family adapters — terminal proposals bind the exact current run-control revision and
  Workflow Type digest. Temporal continues to propose; the reducer decides.
- `tests/acceptance/control_plane/test_wp_cp_020.py` — package qualification and drift guard.

## Canonical command and fact surface

Lifecycle actions are a strict discriminated union. New authoritative commands are
`claim_effect`, `observe_effect`, `settle_effect`, `register_async_child`,
`record_async_child_fact`, and `decide_async_child_fact`. Effect completion observations remain
pending until a settlement binds one exact observation and usage-settlement reference. Provider
child completion is recorded as a fact and cannot satisfy a required parent dependency without an
authorized parent decision.

Terminalization proposals bind `expected_run_version`, exact `workflow_type_digest`, obligation
revision, accepted evidence frontier, outputs, finalization plan, budget settlement, effect
settlement, cancellation settlement, waits/links, and parent async-child decisions.

## API paths

- `POST /run-control/v1/run-requests`
- `POST /run-control/v1/runs/{run_id}/commands`
- `GET /run-control/v1/runs/{run_id}`
- `GET /run-control/v1/runs/{run_id}/budget`
- `GET /run-control/v1/runs/{run_id}/effects`
- `GET /run-control/v1/runs/{run_id}/effect-ledger`
- `GET /run-control/v1/runs/{run_id}/transitions`
- `GET /run-control/v1/outbox`
- `GET /run-control/v1/schemas`

## Commands and sanitized results

```text
.\.venv\Scripts\python.exe -m ruff check app tests
All checks passed!

.\.venv\Scripts\python.exe -m mypy app
Success: no issues found in 316 source files

.\.venv\Scripts\python.exe -m pytest -q --tb=short
565 passed, 32 skipped, 11 warnings in 42.36s

TEST_APPLICATION_POSTGRES_DSN=<local disposable application PostgreSQL>
.\.venv\Scripts\python.exe -m pytest -q tests/test_run_control_postgres_integration.py --tb=short
1 passed in 1.70s

.\.venv\Scripts\python.exe -m pytest -q tests/acceptance/control_plane/test_wp_cp_020.py
5 passed

git diff --check
exit 0; line-ending conversion warnings only, no whitespace errors
```

The default-suite skips are service/credential-gated tests. The package's disposable PostgreSQL
qualification was run separately against the healthy local application PostgreSQL service and
passed. Redis and Temporal were not running in the local Compose snapshot; they are not required
for this PostgreSQL authority package, and Temporal workflow implementation belongs to WP-CP-030.

## Transaction and migration rehearsal

The ordered migration runner applied `0015_transactional_run_control_v1.sql` twice safely after
the existing head. Admission failure injection left no decision, run, reservation, transition, or
start event. Effect-command failure injection rolled back projection version, effect state,
effect records, command result, transition, and outbox together. Concurrent commands produced
exactly one accepted CAS result and one stable stale result. RLS tenant isolation was exercised
under `belllabs_control_runtime`.

## Replacement and deletion inventory

| Responsibility | Disposition |
|---|---|
| Prototype run-control contracts/reducer/service | Replaced in place by the canonical V1 owners; no `v2`, compatibility, dual-read, or dual-write path was added. |
| Consequential operation effect authority | Provider path now enters canonical run-control claim/observe/settle commands. The operation journal retains detailed attempts/manifests as subordinate evidence only. |
| Temporal/provider completion authority | Remains a typed observation/proposal; terminal outcome is assigned only by `app/domain/run_control/reducer.py`. |
| Start/control delivery | Durable outbox plus generic at-least-once relay; WP-CP-030 supplies the Temporal start/control consumer. |
| Prototype PostgreSQL data | Recreated through ordered pre-production migrations; no backfill or production compatibility path is required. |

## Drift checks and remaining package boundaries

- `app/domain/run_control/` has no Temporal, asyncpg, Beanie, LangGraph, Deep Agents, provider, or
  transport imports; enforced by the WP-CP-020 acceptance test.
- SQL lifecycle/budget/effect writes remain inside application PostgreSQL repository adapters.
- Temporal family adapters issue typed commands and terminal proposals; they do not assign domain
  lifecycle or outcomes.
- MongoDB models/repositories do not write run lifecycle, budget, effects, or terminality.
- OpenAI Agents SDK runtime matches remain in active prototype runtime paths. Their accepted
  replacement/deletion gate is WP-CP-040, not WP-CP-020; this package did not expand them.
- Direct family Temporal submission remains until the WP-CP-030 replacement gate.

## Rollback posture and risks

Pre-production rollback is repository revert plus recreation of the application PostgreSQL schema
before downstream Temporal admission is enabled. It does not re-enable an alternate run-control
writer. The only operational warning observed was a local PostgreSQL collation-version mismatch;
it did not affect migration or qualification results and should be refreshed during local database
maintenance.

## Final disposition

Every WP-CP-020-owned requirement has an exact executable evidence mapping. Canonical schemas,
application composition, PostgreSQL migration/rollback, concurrency, redelivery, reconstruction,
effects, async-child parent authority, bounded finalization, drift checks, and shared repository
checks pass. WP-CP-020 is accepted, and WP-CP-030 is the next unblocked implementation frontier.
