# Mission Control implementation and parity evidence

Status: local execution parity and source organization qualified, 2026-10-03.
Final fresh regression: **1,343 passed, 23 skipped, two expected failures, zero
failures**. Eleven unchanged end-to-end cases passed separately (eight Mission
Control and three canonical Agent Server profiles). Final lint, typing and both
isolated wheel checks passed. These selections cover the selected tree across two
runs; overlapping peer selections below must not be added to these counts.
See [removal and relocation guide](REMOVAL_GUIDE.md) for current package ownership.
The complete production common-schema architecture is not yet implemented/qualified.
This file distinguishes implemented local behavior from production qualification.
No live migration, Mongo data deletion, paid provider experiment, deployment, or commit
was performed. Existing unrelated working-tree changes are preserved.

## Executable entrypoints

- API: `uv run uvicorn mission_control.bootstrap.api:create_app --factory`.
- Preflight: `uv run python -m mission_control.bootstrap.preflight`; read-only configured startup.
- Worker: `uv run python -m mission_control.bootstrap.worker`; explicit application and binding pin.
- CLI: `missionctl`; downloadable skill directory: `skills/mission-control/`.
- Installation tooling: sibling `../biotech-postgres-db-contract`, CLI `biotech-db`.
- Operator setup: [MISSION_CONTROL_LOCAL_API.md](MISSION_CONTROL_LOCAL_API.md).

The configured API/worker live under `mission_control.bootstrap`. Source imports
use `src/mission_control`; old application imports and startup aliases are not the
supported entrypoints. The removal guide records deliberate compatibility retirement.

## Living parity checklist

| Capability | Implemented mapping | Evidence / remaining qualification |
|---|---|---|
| Application/installation/tenant isolation | Immutable digest-verified bindings, signed JWT identity, persisted installation attestation, exact scoped services | `tests/unit/mission_control/test_installations.py`, `test_authentication.py`, real `test_mission_control_bootstrap_postgres.py` |
| StageGraph execution | Existing production family/operation engine uses PostgreSQL definitions, bindings, workspace/artifact and candidate repositories | New `tests/acceptance/mission_control/test_postgres_runtime_parity.py`; source+fork execution and history replay passed |
| GoalDirected execution | Existing production iteration/operation/settlement engine on PostgreSQL | Same acceptance module; two iterations/four operations, real DeepAgents/local model, child/MCP, artifact settlement and 135-event replay passed |
| New workflow contract names | `mc.mission_run.v1`, `mc.operation.v1`, scoped IDs, preserved old replay registrations | `test_temporal_identities.py`; signed JWT scoped API drove both families to completion and replayed root/operation histories in `test_authenticated_scoped_runtime.py` |
| Lifecycle commands | Real reducer, durable command ledger, authorization, generation/version guards, accepted/delivered/applied distinctions | Mission lifecycle/composition PostgreSQL tests and existing run-control/Temporal suites |
| Pause/resume and restart relay | Queued boundary delivery with persisted receipts | StageGraph acceptance proof passed across actual Temporal stop/restart |
| Cancellation | Scoped normal-urgency cancellation admission and existing active-operation/family settlement; expanded immediate-urgency requests reject explicitly | Existing RRM008 tests; new PostgreSQL-only cancel-at-wait proof passed with preserved receipts and zero reserved budget |
| Inspect/checkpoints/snapshots | Actual repositories and safe-boundary manifest construction | Runtime unit/acceptance suites; StageGraph proof persisted 45 saver checkpoints |
| Semantic forks | Immutable snapshot admission, patch policies, reuse only unaffected work | StageGraph source+fork acceptance proof: unchanged draft reused, changed review rerun |
| Generic artifact promotion | Captured immutable candidates, durable artifact reference and content, idempotent activity replay | Real production artifact workflow passed; promotion replay kept four revisions, same artifact and no additional model calls |
| Reconciliation/recovery/retry | Existing generation fencing, in-doubt classification, operator reconciliation actions | `tests/unit/operations/test_checkpoint_recovery_classification.py` and mission runtime interface tests; no invented generic retry command |
| Parent/child boundaries | Existing sync subagent and durable async child records, linked independent roots | Real local Agent Protocol server completed two hosted DeepAgents child runs with PostgreSQL admission/usage settlement; cancellation during cognition and completion wait passed with provider acknowledgement, usage reconciliation and replay. Linked domain/native Temporal regression and scoped identity review passed |
| Directory skill bundles | Whole-directory canonical manifests, immutable version/digest pins, secure materialization, remote admission | 66 targeted tests including seven real PostgreSQL cases; live bucket permissions and executable read-only mounts remain unqualified |
| Persistent stores | Scoped PostgreSQL immutable docs, catalog, workspace/artifact/candidate metadata, async details; LangGraph PostgreSQL saver/store | Real RLS, concurrency, replay, mutation rejection and production execution tests |
| Installation/migration tooling | Pinned release verifier, ordered transactional apply, advisory lock, receipts, schema/RLS/grant fingerprint, read-only verify | Sibling installer: 22 passing tests including two disposable database installations/upgrades and atomic rollback |
| Common production schema | Source ownership preserved; runtime deliberately rejects unavailable `production_common` | Blocked on canonical owner component/release and its qualified adapters; transitional `belllabs_control` qualification is not production completion |

Queued/immediate intervention support follows the existing mapped controls. Broader
new control semantics and expanded goal-loop design are not silently claimed as
existing parity. Broad KnowledgeServices monorepo generalization remains deferred.

## Migrations and data safety

New versioned migrations in `src/mission_control/adapters/postgres/migrations/`:

- 0027 immutable runtime documents; 0028 workspace/artifact documents.
- 0029 async subagent details; 0030 definition catalog.
- 0031 workspace payload identity hardening; 0032 async detail payload identity.
- 0033 immutable capability bundle admissions.
- 0034 persisted installation identity; 0035 candidate descriptors.
- 0036 read-only installation identity grant for separately restricted family writers.
- Cleanup additions: 0037 scoped catalog projection processing/alerts; 0038 immutable
  external discovery/inspection contracts; 0039 sandbox snapshot contract slots;
  0040 snapshot identity indexes.
  Their exact isolated-database validation belongs to the post-organization record.

Tests apply these only to dedicated local/disposable databases. Startup does not
migrate or fabricate installation identity. The installer tests use explicitly
synthetic release SQL and do not pretend it is the absent canonical release.
No old Mongo history is backfilled or purged. Clean-break scope preserves old data;
any future historical import requires its own mapping, validation and authorization.
Recovery retains the old implementation and data, plus the initial tracked diff in
`.scratch/mission-control-parity-checkpoint/tracked.patch`. Do not route old and new
workers to the same active execution until namespace/schema compatibility is checked.

## Source organization and Mongo retirement

General runtime code now lives in `src/mission_control`, separated into contracts,
domain, application, adapters, interfaces and bootstrap. SQL adapters and migration
resources live under `adapters/postgres`; experiments are outside the runtime wheel.
Application-specific schema/research code is isolated in the optional
`integrations/biotech` package. The removal guide records completed removals and
residual qualification gates; it supersedes the earlier claim that legacy Mongo
sources remain a supported core recovery path.

No historical Mongo data is backfilled or purged. Recovery uses saved source/diff
checkpoints and preserved data; it does not require importing obsolete adapters
into the new general kernel.

## Qualification gates

1. Author/release common SQL in its approved owner,
   `C:\Users\Pinda\Proyectos\aiengineer\ai-engineer-db-contract`, then consume the
   reviewed release and qualify production adapters. That path is outside the
   authorized Biotech source scope; permission was requested separately.
2. Qualify live Supabase identity, RLS/storage policy and restricted runtime roles
   against the actual installation without destructive live operations.
3. Qualify executable directory bundles on OS-enforced read-only mounts. The
   text-only StateBackend explicitly refuses binary files.
4. Provider-specific/metered qualification requires finite spending authorization.
   The local Agent Server proof uses development persistence and does not certify
   production hosted-service restart durability.

Logs and per-slice evidence are under `.scratch/mission-control-parity-checkpoint/`.
Historical failing runs are retained; final results must identify recovered errors,
pre-existing opt-in skips and genuinely unexecuted external proofs separately.

Pre-organization regression evidence (do not treat these counts as validation of
the subsequent source moves): `regression-final.txt` reports **1,305 passed, 59 skipped,
two expected failures**. The new `tests/acceptance/mission_control` directory was
run separately: **eight E2E cases passed** across targeted runs: three PostgreSQL
family cases, generic artifact promotion/replay, hosted asynchronous child completion,
two asynchronous cancellation windows, and signed-JWT scoped execution for both
families. Skips include retained
live-provider, hosted-Agent-Server, external-store and Mongo-dependent qualification;
they do not constitute evidence of those integrations passing. Subsequent scoped
identity/configuration edits receive focused regression and type/lint checks.

Latest scoped unit selection: **95 passed**. Authentication/factory-specific proofs
are recorded separately by their owner; overlapping counts must not be summed.
Mypy passed across **429 source files**. The renamed `mission_control` wheel builds
and contains the API, worker, CLI entrypoint and SQL migration package data.
Final legacy-history and linked-child regression after identity hardening: **25
passed**, including native Temporal tests and captured history replay.

Configured worker startup/restart and actual root/family/linked polling: **three
tests passed**. Sibling installer: **22 tests passed**, including real disposable
database installation/upgrade/replay/rollback. Counts from overlapping selections
are intentionally not totaled. The pre-organization Ruff selection, CLI help smoke check and wheel build passed.
Post-organization verification uses `src/mission_control` and is recorded separately.

The two test PostgreSQL containers are retained for diagnostic recovery and stopped
after qualification; no database/container/volume is deleted. Normal Compose port
55432 is released. Restart only the explicitly named test containers when reproducing
their retained fixtures; do not treat them as a production installation.

## Post-organization verification

Results below apply to the reorganized source. The earlier broad counts above are
retained baseline evidence and are not silently reused as new-layout validation.

- Agent Server: `tests/unit/agent_server` reports 40 unit tests passed; 19 external persistent qualification drills
  remained skipped and do not prove hosted durability. Targeted typecheck passed
  for six source files and lint passed.
- `tests/integration/agent_server/test_canonical_server_local.py`: three actual local
  Agent Server profile tests passed against the sole configuration:
  runtime served the exact bounded graph identity, checkpoints, native cancellation,
  cross-scope denial and profile denial; qualification exercised RSA authentication,
  interrupt/resume and thread copy; N1-only denied N and completed N1 interrupt/resume.
- `tests/integration/postgres/test_immutable_runtime_documents.py`: six real database
  tests passed, including
  concurrent exclusive claims, restart/idempotency/conflict handling, cross-scope
  isolation, parent visibility and preserved clone identity semantics. Migrations
  0039/0040 were exercised only in the isolated local parity database. Targeted
  mypy passed for two source files and scoped Ruff passed for ten files.
- Capability/catalog selection: 123 tests passed, including real PostgreSQL custody,
  concurrency/fencing and signed-JWT catalog bootstrap, HTTP/CLI/manifest, component
  filtering, promotion/search and external inspection. Targeted mypy passed for
  23 source files; scoped Ruff passed. Migrations 0037/0038 were applied only to
  isolated local PostgreSQL. Evidence is in the organization consolidation section of
  [capability-bundles.md](../.scratch/mission-control-parity-checkpoint/capability-bundles.md).
- Optional Biotech extraction: the combined domain/configuration/PostgreSQL selection
  passed 165 tests; the final seed-focused selection passed 32 overlapping tests.
  Optional-package mypy passed across 100 files and Ruff passed. Core source has no
  Biotech/Neo4j/entity-schema/research-workflow imports or application-specific
  workflow strings.
  The optional package owns its own RLS migration and explicit registration;
  no live Neo4j or paid research run was performed. The invalid bypass smoke runner
  and an assertion-free permanently skipped test were retired with recovery.
  Evidence: `.scratch/organization-checkpoint/biotech-boundary-evidence.md`.
- Documentation: eight OKF concepts pass the adjacent catalog's validator; active
  source/concept links resolve and 13 root/scoped guides fit the 8 KiB budget.
  Structural evidence is `.scratch/organization-checkpoint/documentation-checks.txt`.

Final all-tree Ruff/format checks passed across 716 files. Final core mypy passed
across 342 files from the isolated locked environment. The core wheel has 405
entries including all 40 SQL migrations, excludes Biotech JSON resources, and
passed isolated imports and CLI execution. A clean `uv sync --locked --group biotech`
succeeded in an isolated environment, preserving the pre-existing environment;
`uv lock --check --offline` also passed. The optional wheel passed isolation with
105 files, one SQL migration and four JSON resources verified byte-for-byte.
The fresh dependency environment contains no pymongo, Beanie or Motor.

All eight Mission Control acceptance cases and all three canonical local Agent
Server profile cases passed again after reorganization. The first complete broad
run finished with **1,341 passed, 12 failed, 23 skipped and two expected failures**.
It is not reported as a clean run.

The failures covered eight resource/seed cases whose modules were loaded before
the final move, the experiment root-depth assumption, two admission tests using
old SQL/repository paths, and an optional-runner subprocess using the previous
environment without the optional package. Corrections first passed focused
selections of 32 seed tests and 11 architecture/experiment tests. The final fresh
locked-environment selection then completed **1,343 passed, 23 skipped, two expected
failures and zero failures** across 1,368 cases. It excluded only the already-passed,
unchanged eight Mission Control and three canonical Agent Server end-to-end cases.
Together those two runs cover the selected tree; peer subset counts overlap and
are not additive. Final Ruff and both core/optional typechecks passed again.
The full regression log is `.scratch/organization-checkpoint/locked-tests.txt`.

Current broad-selection exclusions are explicit:

- Three skipped paid/provider-live acceptance tests; no finite spending approval
  or live provider qualification is implied by deterministic acceptance.
- Nineteen skipped external persistent Agent Server, N/N+1 and restart drills;
  local development-server profile tests do not replace these proofs.
- One Windows-excluded legacy WSL recovery test.
- Two existing strict expected failures in the Stage 0-2 reference harness
  (RRM012): it lacks accepted run-control authority and the journal correctly
  rejects it. These historical harness cases are not current Mission Control parity.

These 23 skips and two expected failures are not counted as passing integrations.

The two owned disposable PostgreSQL containers are stopped after qualification;
their data and volumes remain available for diagnostic recovery. No live database
was modified or removed.
