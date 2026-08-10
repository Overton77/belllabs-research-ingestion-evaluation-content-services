# Blueprint-runtime foundation amendments handoff

Status: second amendment implemented, uncommitted, pending integrator review  
Original accepted BP base: `19276f0394f14e4df62b6e442080aacd6462705a`  
Branch: `foundation/bp-runtimes-amendment`

## First amendment

Commit `19276f0394f14e4df62b6e442080aacd6462705a` established the exact generic
`OperationWorkflow` request/materialization and worker-registration seams needed by both blueprint
runtimes. It did not add family admission persistence.

## Second narrow amendment addendum

The parallel family implementations require one family decision and its reducer-authorized
reservation/lifecycle transition to commit as one authoritative act. The accepted run-control
repository previously owned the only run lock and PostgreSQL transaction, so either family-side SQL
or a two-transaction saga would violate the frozen authority boundary.

This amendment adds a family-neutral closed seam:

- `AtomicFamilyMutation`, receipts, and an exact-class `FamilyAdmissionRegistry`;
- `RunControlService.execute_family_admission`, which still invokes the unchanged lifecycle reducer;
- one repository operation that commits the existing `CommandMutation` plus one typed family
  mutation under the existing run lock/transaction;
- migration `0017_atomic_family_admission_v1.sql` with tenant-scoped, forced-RLS family head,
  append-only journal, and combined-result tables;
- deterministic command/family fingerprints, exact replay and collision rules, family-version CAS,
  bounded sanitized payloads, generic ordered outbox finality, and rollback injection at both
  transaction boundaries.

Review hardening adds budget/effect authority digests to every accepted combined `CommandMutation`.
Repositories compare those digests under the run transaction/lock before any authoritative state
can be overwritten. `execute_family_admission` re-reads and re-runs the unchanged reducer after a
run-version or budget/effect-authority race, up to eight deterministic attempts. If the command's
authored run version is then stale, the combined command stores and replays one stable stale receipt
without a family head/journal advance. A stale family version remains an explicit conflict and is
not retried. Mutation-ID collision is checked before family-version CAS in both repositories.
Plain `execute` uses the same bounded retry/re-read/re-reduce rule for authority-state races and
preserves its stored stale-result behavior when another command advances the run.

The digest check precedes persistence of accepted, rejected, and stale command results in both the
plain and combined repository boundaries. A parent-budget rollup can therefore invalidate a
reducer rejection without advancing the parent's run version: the repository raises
`AuthorityStateConflict`, the service re-reads/re-reduces, and only the new authoritative outcome is
stored. Tests release a child reservation concurrently so an initially computed hard-cap rejection
becomes an acceptance and prove the stale rejection is absent and cannot replay.

Each exact mutation registration binds its concrete class, `family_kind`, `mutation_kind`,
permission, and lifecycle-action allowlist. `FamilyAdmissionCommit` validates all scope, run,
command, fingerprint, mutation, receipt, acceptance, and next-family-version identities both when
constructed and again at repository entry. Family mutations have a 65,536-byte canonical serialized
ceiling. Operation-request references use a strict non-URL opaque syntax; outbox events carry only
the reference digest.

Family packages compose policy without editing foundation internals. API composition exposes
`configure_family_admission_registry(app, registry)` and
`compose_api_run_control_service(...)`; Temporal worker composition accepts an injected
`FamilyAdmissionRegistry` in `main(...)` and routes it through
`compose_worker_run_control_service(...)`. All hooks are typed and run before service construction.
If no registry is supplied, `RunControlService` creates an empty registry and rejects every family
mutation by default.

The runtime role has no direct `INSERT`, `UPDATE`, or `DELETE` privilege on any family table and no
callable family-attachment function exists. Only the private non-login
`belllabs_family_repository_writer` capability owns the complete combined-transaction DML surface.
It is not granted to `belllabs_app` or `belllabs_control_runtime`. `PostgresRunControlRepository`
requires a distinct writer pool for combined admission and uses that pool's one connection for the
reducer-produced lifecycle state, ledgers, outbox, family journal/head, and combined receipt.
Therefore rollback at
either injection boundary removes every write, while an untrusted runtime session cannot attach a
family mutation to an existing or fabricated command result. Forced tenant RLS remains enabled.

Deployment must externally provision a separate non-superuser, non-`BYPASSRLS` login, grant only
that login membership in `belllabs_family_repository_writer`, and provide its DSN through
`APPLICATION_FAMILY_WRITER_DATABASE_DIRECT`. The normal application identity is rejected if it has
writer membership; the writer identity is rejected if it also has runtime membership. Without the
distinct writer pool, plain run control remains available but atomic family admission fails closed.

No raw database connection or callback crosses the repository port. No StageGraph or GoalDirected
contract, union, dispatch rule, interpreter behavior, or operation-request factory is introduced.

## New base and integration rule

`19276f0394f14e4df62b6e442080aacd6462705a` remains the immutable input base for this uncommitted
worktree. After review, the integrator should create one foundation-amendment commit containing
exactly this addendum, the family-neutral code/migration, and its tests. That resulting commit SHA
becomes the new shared `BP_BASE_REVISION` and must be merged unchanged into both BP branches and
`integration/bp-runtimes`.

The base must advance because family reservation and family cursor/state admission otherwise cannot
be proven atomic. Advancing both branches to the same exact amendment commit avoids either family
privately redefining run-control persistence or lock ordering.

## Changed paths

- `app/domain/run_control/family_admission.py`
- `app/application/run_control.py`
- `app/application/run_control_repository.py`
- `app/application/postgres_run_control_repository.py`
- `app/config.py`
- `app/integrations/postgres.py`
- `app/api/run_control.py`
- `app/temporal/worker.py`
- `app/migrations/0017_atomic_family_admission_v1.sql`
- `.env.example`
- `tests/test_atomic_family_admission.py`
- `tests/test_atomic_family_admission_postgres_integration.py`
- this handoff

## Qualification and disposition

Required before integration:

```text
uv run pytest -q tests/test_atomic_family_admission.py tests/test_run_control.py
uv run pytest -q tests/test_atomic_family_admission_postgres_integration.py tests/test_run_control_postgres_integration.py
uv run mypy app
uv run ruff check app tests
git diff --check
```

The PostgreSQL suite is correctly credential-gated by `TEST_APPLICATION_POSTGRES_DSN`. A skipped
PostgreSQL run is not live database evidence; the integrator must run it against disposable
application PostgreSQL before accepting the new base.

Local qualification on 2026-08-10:

- focused atomic/run-control suites: `24 passed, 5 skipped`;
- first-amendment operation/Temporal regression suites: `45 passed` (warnings only);
- PostgreSQL tests: all five skipped because `TEST_APPLICATION_POSTGRES_DSN` was unavailable;
- Docker PostgreSQL fallback: unavailable because the Docker Desktop Linux engine was not running;
- mypy: `Success: no issues found in 322 source files`;
- ruff: `All checks passed`;
- `git diff --check`: passed.

Disposition: `ready_for_review` after the commands above pass or are explicitly recorded as gated.
