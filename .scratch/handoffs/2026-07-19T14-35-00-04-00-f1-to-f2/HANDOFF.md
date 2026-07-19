# BellLabs backend handoff: F1 to F2

## Handoff reference

- Created: `2026-07-19T14:35:00-04:00`
- Directory reference: `2026-07-19T14-35-00-04-00-f1-to-f2`
- Completed issue: `01 — Versioned Workflow Definitions and Effective Run Configuration`
- Recommended next issue: `02 — Transactional Run admission, lifecycle, budgets, and domain events`
- F1 commit: `7de54a5` (`Establish immutable control-plane configuration foundation`)

## Start here

Read these in order:

1. `biotech-meta/docs/CONTEXT.md`
   - Canonical system vocabulary and distinctions.
   - Pay particular attention to Run Request, Workflow Run, Effective Run Configuration,
     lifecycle phase, wait versus pause, terminal outcome, readiness, budget envelopes,
     lifecycle commands, transition records, and domain event envelopes.
2. `biotech-meta/docs/specs/pre-research/README.md`
   - Governing source hierarchy, persistence authority, dependency DAG, testing rules,
     and the first executable path.
3. `biotech-meta/docs/specs/pre-research/control-plane-foundations/02-transactional-run-admission-lifecycle-and-budgets.md`
   - Governing F2 specification.
4. `.scratch/control-plane-foundations-and-capabilities/issues/INDEX.md`
   - Local ticket order, shared implementation rules, and links to all 21 tickets.
5. `.scratch/control-plane-foundations-and-capabilities/issues/02-transactional-admission-lifecycle-budgets-events.md`
   - Implementation-sized F2 ticket and acceptance checklist.

## `biotech-meta/docs` map

The metadata repository is a sibling of this backend repository:

```text
Biotech/
├── biotech-meta/
│   └── docs/
│       ├── CONTEXT.md
│       └── specs/
│           └── pre-research/
│               ├── README.md
│               ├── control-plane-foundations/
│               │   ├── 01-versioned-workflow-definitions-and-effective-run-configuration.md
│               │   ├── 02-transactional-run-admission-lifecycle-and-budgets.md
│               │   ├── 03-durable-blueprint-orchestration-and-linked-runs.md
│               │   └── 04-operation-runtime-workspaces-artifacts-and-snapshots.md
│               ├── control-plane-capabilities/
│               │   ├── 01-schema-catalog-deployment-manifest-and-workspace-materialization.md
│               │   ├── 02-conversations-threads-session-projections-and-durable-realtime.md
│               │   ├── 03-governed-workflow-and-mission-memory.md
│               │   └── 04-governed-agentic-capability-catalogs-and-bindings.md
│               ├── knowledge-preflight/
│               │   ├── 01-knowledge-preflight-domain-contracts-and-deterministic-core.md
│               │   ├── 02-stagegraph-knowledge-preflight-vertical-slice.md
│               │   └── 03-goaldirected-preflight-extension.md
│               └── starter-content-refinement/
│                   ├── 01-refinement-domain-contracts-and-deterministic-core.md
│                   ├── 02-stagegraph-refinement-vertical-slice.md
│                   ├── 03-goaldirected-refinement-extension.md
│                   └── 04-guest-business-affiliation-summary-operation.md
└── biotech-research-ingestion-evaluation-system/
```

The foundation order is strict: `F1 → F2 → F3 → F4`. Do not start orchestration
or runtime work inside F2.

## What F1 now provides

### Domain and compiler

- `app/domain/control_plane/contracts.py`
  - Strict extra-forbidden immutable contracts.
  - Workflow Types, StageGraph and GoalDirected blueprints, profiles, workflow-specific
    configuration, exact references, overlays, authority ceilings, linked-run slots,
    secret references, authoring heads, and Effective Run Configuration.
  - Agent, prompt, skill, MCP, plugin, memory, and capability-selection kinds currently
    establish exact-reference boundaries only. Their detailed catalogs remain deferred.
- `app/domain/control_plane/compiler.py`
  - Pure compiler with no database, clock, network, or secret-store access.
  - Validates exact published records, blueprint/profile allowlists, required authority,
    parent/caller/environment intersections, workspace compatibility, overlay policy,
    extension allowlists, and linked-run constraints.
- `app/domain/control_plane/canonical.py`
  - Schema-versioned canonical JSON and SHA-256 digests.
  - Set-like values sort deterministically; ordered sequences retain order.
  - Datetimes normalize to UTC and must be timezone-aware.
- `app/domain/control_plane/extensions.py`
  - Static registered validator boundary for executable namespaced extensions.
- `app/domain/control_plane/fixtures.py`
  - Generic StageGraph and GoalDirected structural fixtures only.

### Application and persistence

- `app/application/control_plane.py`
  - Publish, draft, alias, retire, resolve, compile, and digest-verified retrieval service.
- `app/application/control_plane_repository.py`
  - In-memory test adapter and MongoDB/Beanie repository.
  - Published revisions and ERC records are immutable.
  - Retirement and alias-movement audit evidence are separate records.
  - Publication and alias movement use Mongo transactions.
  - Draft and publication revisions use compare-and-swap checks.
- `app/models/control_plane.py`
  - Beanie documents and indexes.
- `app/integrations/mongodb.py`
  - Registers bootstrap and F1 models with timezone-aware decoding.
- `app/integrations/control_plane_payloads.py`
  - Content-addressed in-memory and S3 stores.
  - Production fails explicitly if a payload must be externalized without durable object storage.

### API

- `app/api/control_plane.py`
  - Thin command/query routes and generated schemas.
  - Lazily composes MongoDB plus S3 when configured.
  - Mutation and compilation routes use a deny-by-default authorization dependency.
    A deployment must override `get_control_plane_principal` with authenticated identity
    and role mapping. Do not weaken this to caller-supplied actor authority.
- `app/server.py`
  - Mounts the routes and closes lazily initialized Mongo resources.

## F2 implementation boundary

F2 must introduce a separate application PostgreSQL authority. Do not store lifecycle,
budgets, command results, transition records, or outbox state in MongoDB.

The local `temporal-postgres` service in `docker-compose.yml` belongs only to Temporal.
Create explicit application migration ownership and disposable PostgreSQL integration
infrastructure rather than reusing that database accidentally.

The recommended F2 seam is one transactional application service plus one deterministic
lifecycle reducer:

```text
Run Request / Lifecycle Command
  -> authorization and exact F1 configuration verification
  -> deterministic admission or lifecycle reduction
  -> one PostgreSQL transaction
       - request or command result
       - current run projection
       - immutable transition record where applicable
       - budget account/reservation/ledger effects
       - versioned outbox envelopes
  -> public result
```

Admission should consume F1 through an adapter that loads and digest-verifies the immutable
Effective Run Configuration. PostgreSQL stores only exact configuration references and
digests, not the Mongo payload.

## Important F2 risks

- Keep application PostgreSQL migrations separate from Temporal persistence.
- Rejected Run Requests are decisions, not Workflow Runs or terminal outcomes.
- Exact duplicate identities return their prior result; conflicting payload reuse fails.
- Every lifecycle command carries idempotency identity and expected run version.
- Waiting and pausing are different axes with different resume authority.
- Lifecycle phase, terminal outcome, and purpose-bound readiness remain separate.
- Reserve every budget dimension before dispatch; never offset one dimension with another.
- Commit projection, transition, command result, budget effects, and outbox atomically.
- Temporal receives committed intent through a relay; Temporal status never mutates domain state.
- Do not invent workflow-specific budget values, timeout values, obligations, or completion rules.

## Verification status and commands

F1 verification at handoff:

- Offline suite: `23 passed, 1 skipped`
- Disposable real Mongo acceptance path: passed
- Mypy: passed
- Ruff lint and formatting: passed

Commands:

```bash
uv sync
uv run pytest -q
uv run mypy app
uv run ruff check .
uv run ruff format --check .
```

The real Mongo test uses a disposable database and drops it afterward:

```bash
TEST_MONGODB_URI="<test Mongo URI>" \
  uv run pytest tests/test_control_plane_mongodb_integration.py -q
```

For F2, add disposable PostgreSQL tests that inject rollback failures and concurrency.
The acceptance criteria specifically require public-seam proof of atomic admission,
idempotency, stale-version conflicts, hard-cap enforcement, cancellation settlement,
outbox deduplication/gap recovery, and projection reconstruction.

## Repository state notes

- F1 is committed on `main` at `7de54a5`.
- This handoff file was created after that commit and is intentionally uncommitted unless
  the operator asks for another commit.
- `app/.cursor/rules/user_subagent_preference.mdc` may remain untracked as a user-specific
  local preference. Do not accidentally include it in a product commit.
- Prefer GPT-5.6 Terra or Composer-class agents for routine review work unless a larger
  reviewer is specifically warranted.
