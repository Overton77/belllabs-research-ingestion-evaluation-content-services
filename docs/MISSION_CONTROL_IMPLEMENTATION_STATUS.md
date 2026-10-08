---
type: Implementation Evidence
title: Mission Control implementation and parity evidence
description: "mission_control component, qualified on two local disposable databases and installed live in both Supabase projects (release 1.0.0, owner-approved 2026-10-03; no application traffic yet), plus the 2026-10-08 fast-track packet status per specification (merged at f8d325a, release 1.1.0 built but not applied live, no live mission run). See the fast-track and common component sections."
tags: [mission-control, status, evidence]
---
# Mission Control implementation and parity evidence

Status (2026-10-03): `production_common` is implemented locally on the common
`mission_control` component, qualified on two local disposable databases and
**installed live in both Supabase projects** (release 1.0.0, owner-approved 2026-10-03;
no application traffic yet — see
[common component status](#common-component-status-2026-10-03) and
[final qualification results](#final-qualification-results)).
The counts in this paragraph are the **pre-common-component baseline at `f6521c1`** and
do not validate the common component. Baseline fresh regression: **1,343 passed, 23
skipped, two expected failures, zero failures**. Eleven unchanged end-to-end cases passed separately (eight Mission
Control and three canonical Agent Server profiles). Final lint, typing and both
isolated wheel checks passed. These selections cover the selected tree across two
runs; overlapping peer selections below must not be added to these counts.
See [removal and relocation guide](REMOVAL_GUIDE.md) for current package ownership.
The complete production common-schema architecture is not yet implemented/qualified.
This file distinguishes implemented local behavior from production qualification.
No live migration, Mongo data deletion, paid provider experiment, deployment, or commit
was performed. Existing unrelated working-tree changes are preserved.

## Fast-track packet status (2026-10-08)

The fast-track packet (`docs/specs/fast-track-2026-10/`, SPEC-01 to SPEC-08, ADR-0023 to
ADR-0034) is implemented and merged to `main` at `f8d325a`: 36 tickets (A1-A8, B1-B4, C1-C4,
D1-D3, E1-E3, F1-F6, G1-G7, H1) and a readiness pass (component release 1.1.0, wiring fixes, the
[owner fixture runbook](specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md)). **No live mission has
run.** Tickets I1 to I3 (the three owner missions) are owner-run and unrun, and a live start is
blocked (see below). Everything below is local disposable proof (unit, `common_db` on a disposable
PostgreSQL 17 with pgvector, Temporal replay), recorded by the readiness pass and its handoffs; it
is not a live installation and not production parity. The 2026-10-03 counts in the rest of this
file are older baselines and are not re-stated for the packet.

Per specification. Layers: **unit** (`make test-unit`), **common_db** (real PostgreSQL 17, fails
without `MISSION_CONTROL_TEST_ADMIN_DSN`), **Temporal** (real server or captured-history replay).
"Wired" means composed by the configured API or worker, not only constructible.

| Spec (tickets) | Implemented and proven | Wired in production | Open |
| --- | --- | --- | --- |
| SPEC-01 capabilities (A1-A8) | unit: host projection, bundle custody, hybrid search, catalog CLI, seeds, hook runner. common_db: hybrid search, bundle publish and materialization | catalog kinds, `search`, `pin`, `render` routes; search is lexical unless `CAPABILITY_EMBEDDING_PROFILE` names an embedding route; Deep Agents hook middleware and host projection for the lanes | catalog custody service not composed (`publish:*` answer 503); seeds and the bucket are in release 1.1.0, not live; production seeds lack the capabilities the manifests search for (B2); worker pin file narrow and drifted (B3, B4) |
| SPEC-02 context packet (B1-B4) | unit: packer, renderers, stage and iteration handoff, continuation checkpoint, service, activities. common_db: selection ledger, handoff, continuation. Deep Agents local-model integration: packet seeding, continuation | packer composed in the worker for stage, iteration, chain and inject; checkpoint read routes | `ContinuationService` not composed, `continuation.*` activities registered on no worker, no workflow calls them; no context-health trigger source |
| SPEC-03 frames, transcript (C1-C4) | unit and common_db: frame store, closing-frame facts, usage dispositions, transcript, search. Temporal: frame expiry workflow, run list over Visibility, visibility attributes | Deep Agents and Cursor frame writers, reducer projector, `frames.expire` on the worker maintenance queue with a daily Schedule, transcript and run routes | `transcript.project` registered on no worker (search refreshes its own run); `run transcript --full` answers 501 (no `ArtifactBodyReader`) |
| SPEC-04 chains (D1-D3) | unit: compile, reducer, interfaces. common_db: tables, release in the ledger transaction. Temporal: idempotent chain start. Acceptance: two linked Goal Loops with a test launch author | release hook installed on the ledger writer (API and worker); chain read routes | `ChainIntentRelay` and a production `ChainLaunchInputPort` not composed: a released link leaves a start intent nobody delivers (B1) |
| SPEC-05 manifest (E1-E3) | unit: schema, compile, interfaces. common_db: submit. Acceptance: manifest lifecycle. Dry run: all three owner manifests compile and submit on a scratch 1.1.0 installation (with the test stand-in rows) | compile, submit and start routes, CLI and MCP tools | `start` answers `409 start_unavailable`: no production launch input author (B1), which blocks every owner mission |
| SPEC-06 interventions (F1-F6) | unit and common_db: mailbox, inject, stop fence, fork instruction, inspection, subscriptions. Temporal: mailbox, inject, immediate cancel | mailbox, stop fence and continuation commands composed in the API; subscription relay opt-in (`MISSION_CONTROL_SUBSCRIPTION_RELAY=1`); SSE `events watch`; `command queue|inject|cancel` | manifest subscription names `activation.completed`, `human_task.opened`, `run.completed` are never emitted (B7); `request_continuation` records only; no WebSocket adapter |
| SPEC-07 harness, Cursor lanes (G1-G7) | unit: protocol, registry, dispatch, `lane.turn`, describe honesty, both Cursor harnesses against replaying fakes and recorded-shape fixtures. common_db: lane state, bindings, leases, lane controls. Temporal: `lane.turn`, captured lane histories, worker versioning, visibility. `temporalio` 1.34 | registry, `lane.turn` activities, Cursor harnesses (when `CURSOR_API_KEY` is bound), hook callback listener, Worker Deployment versioning | both Cursor profiles `qualified=False`; the paid drill has not run (B5); `cursor_local` needs a Proactor or Unix event loop (B6); `cursor_cloud` fork restore lacks a recorded branch ref |
| SPEC-08 skills (H1) | unit: manifest digests (`make skills-check`) | router skill and five bundles in the repository and in the common seed | H3 (OVE-58, the headless-agent walkthrough) is blocked by I1 and has not run |

Readiness-pass evidence (owner runbook section 9, recorded at the readiness head, not re-run for
this documentation update): `pytest -m common_db` over `tests/integration/postgres`,
`tests/qualification` and three acceptance suites, 176 passed with two failures and five errors
that all trace to the drifted workspace `agent-browser` pin (B4); Temporal replay and lane suites,
65 passed, 4 skipped (the paid live drills); `make lane-qualify` offline for both profiles passed;
`make check` lint, format, ty, deptry and architecture passed; unit suite 1916 passed and 8
failed (seven environmental: the missing sibling `../biotech-kg` and the B4 pin; one pre-existing).
Two scratch proofs on the disposable server (databases dropped): 1.0.0 to 1.1.0 upgrade, and a fresh
1.1.0 install, each with seeds and replay.

Blockers that stop a live run, from the runbook (each needs the named closure):

| # | Blocker | Closure |
| --- | --- | --- |
| B1 | No production author of lane execution templates (`LaunchInputPort`, `ChainLaunchInputPort`); `mission start` is unavailable for all three missions | A follow-up ticket plus owner decisions on model, sandbox and secret-ref mappings |
| B2 | Production seeds lack capabilities the manifests search for | Publish reviewed definitions; the Biotech `kg_ingest` tool is app-owned |
| B3 | Worker launches only components in its pin file | Extend the pin file together with B1 |
| B4 | `agent-browser` workspace skill drifted from its pin; worker startup fails | Restore the pinned bytes or re-pin in a reviewed change |
| B5 | Cursor lanes unqualified | Run the paid drill, then a reviewed release; local proof only with `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true` |
| B6 | `cursor_local` cannot launch on the Windows selector loop | Run the worker on WSL or Linux |
| B7 | Subscription event-name gap | Emit the SPEC-06 names or alias them |

Owner decisions still open: `pg_trgm` on both Supabase projects and the approval to apply release
1.1.0 (the plan admits the live 1.0.0 fingerprint); the storage claim name for the bundle bucket
and OVE-23 approval; a revision 2 of the coordinator skill after the approved-assets 1.0.1
succession; a security review of the family-writer INSERT widening (migration 0028); real
`NCBI_API_KEY` and `EDGAR_IDENTITY` secret refs; a dedicated clone and worker host for Mission 3;
PostgreSQL 17 for the compose database (compose runs 16). The runbook lists them in full.

What remains after the owner's mission runs (ticket I4 owns the evidence half): the three mission
acceptance results with passed, failed, blocked and unrun checks per mission, the lane
qualification records, and any ADR or glossary corrections they force. Release 1.1.0 is **not
applied** to either live Supabase project; the live state is still release 1.0.0.

## Executable entrypoints

- API: `uv run uvicorn mission_control.bootstrap.api:create_app --factory`.
- Preflight: `uv run python -m mission_control.bootstrap.preflight`; read-only configured startup.
- Worker: `uv run python -m mission_control.bootstrap.worker`; explicit application and binding pin.
- CLI: `missionctl`; downloadable skill directory: `skills/mission-control/`.
- Installation tooling: `packages/mission-control-db-contract` (distribution
  `mission-control-db-contract`, CLI `mission-db`); app targets in `deployments/<app>/`.
  The sibling `../biotech-postgres-db-contract` (`biotech-db`) is superseded and pending
  its reviewed thin-wrapper replacement.
- Operator setup: [MISSION_CONTROL_LOCAL_API.md](MISSION_CONTROL_LOCAL_API.md); owner mission
  runs: [OWNER-FIXTURE-RUNBOOK.md](specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md).
- Fast-track tooling: `make lane-qualify PROFILE=cursor_local|cursor_cloud [LIVE=1]`,
  `make skills-check`, `make skills-manifest`, `make seeds-validate`,
  `scripts/fast_track_dry_run.py`.

The configured API/worker live under `mission_control.bootstrap`. Source imports
use `src/mission_control`; old application imports and startup aliases are not the
supported entrypoints. The removal guide records deliberate compatibility retirement.

## Common component status (2026-10-03)

Implemented locally:

- One storage mode, `production_common`. API, worker and preflight verify the
  persisted installation identity, complete attested release, schema fingerprint,
  writer version `mission-control-runtime/1` and restricted pool roles; no
  transitional or memory fallback remains in bootstrap.
- All PostgreSQL repositories read and write `mission_control` (business authority)
  and `mission_control_search` (projection) with transaction-local `mc.*` scope and
  forced RLS. Legacy tables without a live writer are retired with evidence (see the
  [removal guide](REMOVAL_GUIDE.md#retired-legacy-tables)).
- LangGraph saver/store live only in `mission_control_runtime`, provisioned by
  `mission-db runtime-apply`; `LANGGRAPH_CHECKPOINT_SCHEMA` defaults to it and legacy or
  business schemas are refused.
- Seed bundles (`packages/mission-control-db-contract/seeds/`) apply and replay with
  receipts.

Qualified on two local disposable clusters (final run `20261003-g3-r2`, evidence in
`docs/qualification/two-project/disposable-{biotech,ai-engineer}/20261003-g3-r2/` and
`comparison-20261003-g3-r2.json`; `r1` is superseded by a comment-only migration change): identical release, schema fingerprint
`sha256:ef5a9e71c9c4a4fe23f343a5708c476a757bc1c5a61f9aacae3a76577b209c09` and generated
contract `sha256:dbce50116df01e9a8d814f78e9ff61d91943afdfcc5fcb1a1d2d2001e488ab7f`;
protected-object diffs limited to 12 allowed role/membership additions; apply, runtime
and seed phases replayed as no-ops. This is synthetic local qualification, not live
evidence or production parity.

**Live installation (owner-approved, executed 2026-10-03, Biotech first):** release
`mission_control` 1.0.0 is installed in both Supabase projects —
`biotech-research-ingestion` (`bxnetwiimwhtlrjlbtab`, app `biotech`) and
`supabase-blue-ocean` (`wkythqbofmckbuoothhn`, app `ai-engineer`). Each now holds the
schemas `mission_control` (111 tables), `mission_control_search` (3) and
`mission_control_runtime` (6, pinned LangGraph saver/store), six NOLOGIN capability roles,
18 release receipts, one active installation row and the common catalog plus app-binding
seeds (`mc.app.bindings` 1.0.0 frozen as applied, 1.0.1 recording the approved targets).
Both report the identical fingerprint `sha256:ef5a9e71…` and contract `sha256:dbce5011…`;
pre-existing objects (including Biotech's legacy `capability_search` rows and all 25
AI Engineer domain schemas) are unchanged apart from the 12 approved role/membership
additions. Evidence: `docs/qualification/two-project/{biotech-research-ingestion,supabase-blue-ocean}/20261003-live-r1/`
and `comparison-20261003-live-r1.json`; plan in `docs/qualification/two-project/LIVE_PLAN.md`.

Still not done (each needs its own approval or decision):

1. **Application traffic.** No API/worker is deployed against the new schemas; runtime
   LOGIN roles and memberships (access expansion) do not exist yet.
2. **Tenant/actor mappings and issuer/audience bindings** for real users.
3. **Storage** buckets (`mission-artifacts`, `capability-bundles`) and their
   `storage.objects` policies.
4. **Agent Server** native persistence topology and production license
   (`packages/mission-control-db-contract/runtime/AGENT_SERVER_TOPOLOGY.md`): the pinned
   server cannot use a private schema of the business database.
5. **Recovery point.** No PITR/backup was verified before apply; the change was additive
   and its protected comparison is clean, but restore remains unproven.
6. Production paid-provider/semantic-search qualification (no embeddings generated).

The legacy migration runner (`apply_application_migrations`,
`apply_capability_search_migrations`), its two `scripts/migrate_*.py` callers and
`bootstrap/installation.py` were removed on 2026-10-03 (pre-removal bytes and SHA-256 in
`.scratch/two-project-rollout-checkpoint/retired-legacy-runner/`). Sandbox snapshot persistence is retired (no common table),
so that capability is unavailable.

## Final qualification results

Run 2026-10-03 on Python 3.12.14 with disposable PostgreSQL 17 + pgvector and the cached
local Temporal dev server (`.scratch/two-project-rollout-checkpoint/final-verification/`).

| Check | Command | Result |
| --- | --- | --- |
| Full regression (all markers, services configured) | `uv run --group biotech pytest` | 1439 passed, 1 failed, 25 skipped, 2 xfailed; the failure (seed-bundle drift after the live targets were approved) was fixed by freezing `mc.app.bindings@1.0.0` and adding 1.0.1: `tests/unit/control_plane/test_catalog_seed_bundles.py` 7 passed |
| Skips (individually justified, 25) | — | 19 live Agent Server Block C drills (`AGENT_SERVER_ENDPOINT`), 3 metered live qualifications (`BELLABS_RUN_WP_*_LIVE`), 2 legacy experiment notebooks (`TEST_APPLICATION_POSTGRES_DSN`), 1 WSL-only recovery test; the 2 xfails are the known RRM-012 reference harness |
| Acceptance on the common component | `pytest tests/acceptance/mission_control` (+ converted control-plane/integration files) | 30 passed on real PostgreSQL + Temporal; canonical lineage asserted (`local-real-stack/20261003-g3/persistence-trace.json`) |
| Independent two-project qualification | `pytest tests/qualification/two_project` | included in the full run (34 tests, 0 failed) |
| Package `mission-db` | `pytest packages/mission-control-db-contract/tests` | 64 passed on two disposable clusters (one installed by a non-superuser CREATEROLE login) |
| Thin Biotech consumer | `../biotech-postgres-db-contract`: `pytest tests` | 17 passed |
| Two-disposable CLI proof | `scripts/qualify_two_disposables.py --run-id 20261003-g3-r2` | pass on both targets; identical fingerprint/contract; protected diff = allowed only |
| Lint / typing | `ruff check src tests scripts experiments integrations/biotech/src`; mypy (runtime, Biotech package, `mission-db` strict) | clean; 346 + 100 + 13 files, no issues |
| Wheels | isolated `uv run --isolated --with <wheel>` | runtime wheel imports (no `mission_control_db_contract`, no legacy SQL); `mission-db` wheel runs standalone |
| Live targets | `mission-db apply/verify/runtime-apply/seed-apply/qualify` | Biotech then Blue Ocean: verified, protected diff allowed-only, decision pass |

## Living parity checklist

| Capability | Implemented mapping | Evidence / remaining qualification |
|---|---|---|
| Application/installation/tenant isolation | Immutable digest-verified bindings, signed JWT identity, persisted installation attestation, exact scoped services | `tests/unit/mission_control/test_installations.py`, `test_authentication.py`, real `test_mission_control_bootstrap_postgres.py` |
| StageGraph execution | Existing production family/operation engine uses PostgreSQL definitions, bindings, workspace/artifact and candidate repositories | New `tests/acceptance/mission_control/test_postgres_runtime_parity.py`; source+fork execution and history replay passed |
| GoalDirected execution | Existing production iteration/operation/settlement engine on PostgreSQL | Same acceptance module; two iterations/four operations, real DeepAgents/local model, child/MCP, artifact settlement and 135-event replay passed |
| New workflow contract names | `mc.mission_run.v1`, `mc.operation.v1`, scoped IDs, preserved old replay registrations | `test_temporal_identities.py`; signed JWT scoped API drove both families to completion and replayed root/operation histories in `test_authenticated_scoped_runtime.py` |
| Lifecycle commands | Real reducer, durable command ledger, authorization, generation/version guards, accepted/delivered/applied distinctions | Mission lifecycle/composition PostgreSQL tests and existing run-control/Temporal suites |
| Pause/resume and restart relay | Queued boundary delivery with persisted receipts | StageGraph acceptance proof passed across actual Temporal stop/restart |
| Cancellation | Scoped normal-urgency cancellation admission and existing active-operation/family settlement; expanded immediate-urgency requests reject explicitly (as of 2026-10-03; immediate cancel with a persisted Stop Fence is now implemented, see the fast-track section) | Existing RRM008 tests; new PostgreSQL-only cancel-at-wait proof passed with preserved receipts and zero reserved budget |
| Inspect/checkpoints/snapshots | Actual repositories and safe-boundary manifest construction | Runtime unit/acceptance suites; StageGraph proof persisted 45 saver checkpoints |
| Semantic forks | Immutable snapshot admission, patch policies, reuse only unaffected work | StageGraph source+fork acceptance proof: unchanged draft reused, changed review rerun |
| Generic artifact promotion | Captured immutable candidates, durable artifact reference and content, idempotent activity replay | Real production artifact workflow passed; promotion replay kept four revisions, same artifact and no additional model calls |
| Reconciliation/recovery/retry | Existing generation fencing, in-doubt classification, operator reconciliation actions | `tests/unit/operations/test_checkpoint_recovery_classification.py` and mission runtime interface tests; no invented generic retry command |
| Parent/child boundaries | Existing sync subagent and durable async child records, linked independent roots | Real local Agent Protocol server completed two hosted DeepAgents child runs with PostgreSQL admission/usage settlement; cancellation during cognition and completion wait passed with provider acknowledgement, usage reconciliation and replay. Linked domain/native Temporal regression and scoped identity review passed |
| Directory skill bundles | Whole-directory canonical manifests, immutable version/digest pins, secure materialization, remote admission | 66 targeted tests including seven real PostgreSQL cases; live bucket permissions and executable read-only mounts remain unqualified |
| Persistent stores | Scoped PostgreSQL immutable docs, catalog, workspace/artifact/candidate metadata, async details; LangGraph PostgreSQL saver/store | Real RLS, concurrency, replay, mutation rejection and production execution tests |
| Installation/migration tooling | `mission-db`: release build/lock, plan bound to before-fingerprint, atomic apply under advisory lock, receipts + attestation, `mc-pg-catalog-v2` fingerprint over both owned schemas, runtime phase, seeds, snapshot/compare/qualify | Two-disposable CLI proof `20261003-g3-r1` and independent `tests/qualification/two_project/`; the sibling installer's 22 historical tests remain baseline only |
| Common production schema | Release `mission_control` 1.0.0 (migrations 0001-0005, 0010-0017, 0020-0024) from `packages/mission-control-db-contract`; all repositories use `mission_control`/`mission_control_search`; saver/store in `mission_control_runtime`; startup accepts only `production_common` | Qualified on two local disposables (identical fingerprint and contract digest); **installed live in both Supabase projects on 2026-10-03** (release 1.0.0, no application traffic yet); remaining gates in the still-not-done list above |

Queued/immediate intervention support follows the existing mapped controls. Broader
new control semantics and expanded goal-loop design are not silently claimed as
existing parity. Broad KnowledgeServices monorepo generalization remains deferred.

## Migrations and data safety

Historical note: the list below is the transitional `belllabs_control` chain in
`src/mission_control/adapters/postgres/migrations/` (40 files, bytes pinned by
`docs/organization/legacy-belllabs-control-chain.json`). It is no longer applied by
the runtime and is never applied to a live project; the common component's own chain
lives in `packages/mission-control-db-contract/component/migrations/`.

Transitional migrations recorded at the time:

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
migrate or fabricate installation identity. (At the time, the sibling installer tests
used synthetic release SQL; the common release now exists and is installed only by
`mission-db`.)
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

1. Done locally: the common SQL is authored and released from its corrected owner,
   `packages/mission-control-db-contract` (ownership amendment D09; the earlier
   `ai-engineer-db-contract` placement is superseded), and the production adapters are
   converted. Live application remains blocked; see the common component status.
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
