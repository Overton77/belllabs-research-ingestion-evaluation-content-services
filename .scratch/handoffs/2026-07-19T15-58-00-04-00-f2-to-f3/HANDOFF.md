# BellLabs backend handoff: F2 to F3

## Handoff reference

- Created: `2026-07-19T15:58:00-04:00`
- Directory reference: `2026-07-19T15-58-00-04-00-f2-to-f3`
- Completed issues:
  - `01 — Versioned Workflow Definitions and Effective Run Configuration`
  - `02 — Transactional Run admission, lifecycle, budgets, and domain events`
- Recommended next issue: `03 — Durable StageGraph orchestration`
- F1 commit: `7de54a5` (`Establish immutable control-plane configuration foundation`)
- F2 commit: `5221bc3` (`Establish transactional workflow run authority`)
- Delivery documentation commit: `19be3b3` (`Document control-plane delivery lifecycle`)
- Repository: `https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services`
- Timezone note: all handoff datetimes are offset-aware. Persisted application datetimes
  must remain timezone-aware and are normalized to UTC by canonical contracts where required.

## Tracker and repository state

- `main` was pushed to `origin/main` and configured to track it.
- GitHub issue `#1` is closed.
- GitHub issue `#2` is closed.
- GitHub issue `#3` is open and is the next dependency-unblocked ticket:
  `https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/3`
- The only intentionally untracked path at handoff is
  `app/.cursor/rules/user_subagent_preference.mdc`, a user-specific local preference.
  Do not add it to a product commit.

## Start here

Read these in order:

1. `biotech-meta/docs/CONTEXT.md`
   - Canonical system vocabulary and distinctions.
2. `biotech-meta/docs/specs/pre-research/README.md`
   - Governing source hierarchy, persistence authority, dependency DAG, and testing rules.
3. `biotech-meta/docs/specs/pre-research/control-plane-foundations/03-durable-blueprint-orchestration-and-linked-runs.md`
   - Governing F3 specification.
4. `.scratch/control-plane-foundations-and-capabilities/issues/INDEX.md`
   - Ticket order and implementation rules.
5. `.scratch/control-plane-foundations-and-capabilities/issues/03-durable-stagegraph-orchestration.md`
   - Next implementation-sized ticket and acceptance criteria.
6. This handoff, then the F1-to-F2 handoff at
   `.scratch/handoffs/2026-07-19T14-35-00-04-00-f1-to-f2/HANDOFF.md`.

The foundation order remains strict:

```text
F1 → F2 → F3 → F4
```

F3 may now begin. GoalDirected orchestration, linked-run composition, runtime provider
implementation, workspace materialization, and artifact promotion remain separate tickets.

## Authority boundaries now implemented

The system has two application authorities and one execution authority:

- MongoDB/Beanie owns immutable definitions and Effective Run Configuration payloads.
- Application PostgreSQL owns Run Request decisions, Workflow Run projections, lifecycle
  commands and transitions, budget state and ledger entries, outbox events, and consumer cursors.
- Temporal owns durable execution mechanics. Temporal status is not domain truth and must never
  mutate Workflow Run state directly.

F2 consumes F1 through a digest-verifying adapter. PostgreSQL stores exact references and
configuration digests, not copied Mongo payloads. F3 must preserve this boundary and interpret
only the frozen, admitted StageGraph configuration.

## F2 implementation map

### Domain

- `app/domain/run_control/contracts.py`
  - Strict, immutable, extra-forbidden request, command, projection, budget, transition,
    finalization, evidence, event, and cursor contracts.
  - Enforces bounded persisted structures and timezone-aware datetimes.
- `app/domain/run_control/reducer.py`
  - Deterministic lifecycle reducer.
  - Separates phase, terminal outcome, waits, pauses, readiness, continuation, finalization,
    obligation evidence, and output evidence.
  - Maintains a canonical evidence frontier and freezes it when finalization is accepted.
- `app/domain/run_control/budget.py`
  - Parent-child budget rollup.
  - Hard caps reject new positive reservations; observed or pending usage always remains
    accountably rolled up even when it exceeds an estimate.
- `app/domain/run_control/errors.py`
  - Typed application-facing run-control failures.

### Application and persistence

- `app/application/run_control.py`
  - Admission and lifecycle command service.
  - Verifies immutable ERC authority, tenant scope, sponsorship, parent binding, required
    dimensions, idempotency ownership, and executable admission policies.
- `app/application/run_control_repository.py`
  - Repository protocol and in-memory adapter used by domain/application tests.
- `app/application/postgres_run_control_repository.py`
  - PostgreSQL adapter with transactional admission and command commits, row locking,
    optimistic run versions, parent-account locking, RLS scope setup, outbox pagination,
    durable consumer cursors, and projection reconstruction.
- `app/migrations/0001_run_control.sql`
  - Authoritative schema, indexes, constraints, grants, forced tenant RLS, and restricted
    runtime role.
- `app/integrations/postgres.py`
  - Separate migration-owner and restricted runtime pools plus ordered migration execution.

### API and runtime composition

- `app/api/run_control.py`
  - Thin authenticated command/query routes and generated schemas.
  - Caller authority is derived from `ControlPlanePrincipal`, never trusted from request claims.
- `app/api/control_plane.py`
  - Principal now carries tenant, authority, sponsorship, approval, and compilation authority.
- `app/middleware/body_limit.py`
  - Rejects oversized request bodies before contract parsing.
- `app/server.py`
  - Owns application PostgreSQL startup/shutdown, mounts run-control routes, and maps typed errors.
- `docker-compose.yml`
  - Runs application PostgreSQL separately from Temporal PostgreSQL.

## Transactional invariants F3 must use

- A rejected Run Request is an admission decision, not a Workflow Run.
- Admission identity is scoped by request scope, authenticated issuer, and request identity.
- Lifecycle commands include authenticated issuer identity and expected run version.
- Exact retries return the durable prior result; conflicting identity reuse fails.
- Accepted lifecycle changes commit projection, transition, command result, budget effects,
  parent rollups, and outbox envelopes atomically.
- Waiting and pausing are independent axes with separate authority and resume semantics.
- Readiness remains purpose-bound and is not equivalent to lifecycle phase.
- Every budget dimension is independent. No dimension can offset another.
- Reservations precede dispatch. Actual and pending usage are always recorded, even when an
  estimate was low.
- Parent-child reservations lock and update the parent account transactionally.
- Outbox delivery is at least once. Consumers deduplicate stable event identities and persist
  durable cursors; cursor gaps are explicit.
- Terminalization binds the authoritative obligation/output evidence frontier.
- PostgreSQL RLS requires a transaction-local request scope and a non-bypass runtime identity.

F3 should emit public lifecycle and budget commands through `RunControlService`; it must not
write run-control tables, mutate projections, or manufacture transitions directly.

## F3 implementation boundary

Ticket 03 should add a deterministic StageGraph interpreter in Temporal:

```text
admitted run + frozen StageGraph
  -> deterministic Temporal workflow coordination
  -> idempotent activities for every nondeterministic interaction
  -> public RunControlService commands for accepted domain facts
  -> committed PostgreSQL outbox events for downstream delivery
```

Keep Temporal Workflow code free of direct database, network, clock, random, filesystem,
provider, and mutable-definition reads. Load immutable execution inputs before or through
versioned activity results and preserve them in history. Stable semantic identities for stages,
cycles, operations, evaluations, and execution epochs must remain distinct from Temporal
workflow/activity attempt counters.

Recommended module seams:

- `app/domain/orchestration/` for pure StageGraph scheduling decisions and identities.
- `app/application/orchestration.py` for ports that issue run-control commands and resolve
  immutable admitted inputs.
- `app/temporal/` for deterministic workflow code and idempotent activities.
- Offline tests using Temporal's time-skipping environment and fake nondeterministic ports.

Do not let F3:

- invent a runtime-authored workflow DSL;
- query mutable aliases or definitions from Workflow code;
- treat Temporal retries as semantic stage or operation retries;
- bypass F2 budget reservation, lifecycle, finalization, or evidence commands;
- implement GoalDirected execution, linked runs, runtime providers, workspaces, or artifacts
  that belong to later tickets.

## Verification status

Final F2 verification before commit:

- Offline suite: `32 passed, 2 skipped`
  - Skips are environment-gated external-service acceptance paths.
- Disposable application PostgreSQL acceptance: `1 passed`
- Ruff lint: passed
- Ruff formatting check: passed
- Mypy: passed with no issues in 42 source files
- IDE diagnostics: no linter errors
- Security and correctness review findings were resolved before commit.

Commands:

```bash
uv sync
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy app
```

Real application PostgreSQL acceptance:

```bash
APPLICATION_POSTGRES_PORT=55432 docker compose up -d application-postgres

TEST_APPLICATION_POSTGRES_DSN="postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs" \
  uv run pytest tests/test_run_control_postgres_integration.py -q
```

Port `5433` was occupied by an unrelated host PostgreSQL process. Local application PostgreSQL
therefore uses loopback port `55432`. Do not stop the unrelated host database. To stop only the
application service:

```bash
docker compose stop application-postgres
```

Do not use `docker compose down -v` merely for this test; that can disturb Temporal and remove
unrelated volumes.

## Known deployment obligations

- Production must provide a restricted runtime credential through
  `APPLICATION_DATABASE_DIRECT` or `APPLICATION_DATABASE_URL`.
- Production migrations must use a distinct schema-owner credential through
  `APPLICATION_MIGRATION_DATABASE_DIRECT`.
- `get_control_plane_principal` remains a deny-by-default integration seam. A deployment must
  bind it to authenticated identity and policy data.
- The outbox transport relay is intentionally not implemented by F2. F3 or a dedicated adapter
  must claim and deliver committed intent without changing event identity or domain authority.
- Do not weaken forced RLS or connect runtime traffic with the migration-owner credential.

## Next-agent completion criteria

Before handing off ticket 03, the next agent should prove through public seams that:

- only dependency-ready stages schedule;
- joins, skips, bounded parallelism, fairness, and reservations are deterministic;
- waits and pauses are reflected through F2 commands;
- replay and worker restart do not duplicate operations, charges, artifacts, links, or lifecycle
  facts;
- semantic cycles and execution identities remain distinct from Temporal retry attempts;
- Workflow code performs deterministic coordination only;
- the full offline suite, static checks, and Temporal time-skipping acceptance tests pass.
