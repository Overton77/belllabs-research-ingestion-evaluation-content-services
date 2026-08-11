# Blueprint-runtime foundation amendments handoff

Status: third amendment implemented, uncommitted, pending integrator review
Original accepted BP base: `19276f0394f14e4df62b6e442080aacd6462705a`  
Third amendment input base: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`
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

## Third narrow amendment addendum

The StageGraph result-acceptance blocker requires usage and pending-usage settlement, effect
observation and settlement, accepted obligation/output evidence, the family projection decision,
both resulting versions, and outbox finality to share one transaction. The second amendment could
combine only one lifecycle action with one family mutation, so recording multiple accepted evidence
items as follow-up commands left a durable partial-acceptance window.

This amendment adds the family-neutral `ApplyAuthorityBatchAction`:

- its closed component union contains only `RecordUsageAction`, `SettlePendingUsageAction`,
  `ObserveEffectAction`, `SettleEffectAction`, `RecordObligationEvidenceAction`, and
  `RecordOutputEvidenceAction`;
- batches are non-empty, bounded to 64 components, canonically ordered, and reject duplicate
  authority identities and duplicate effect/evidence settlement targets;
- reservations, lifecycle phase changes, terminalization, readiness/finalization, effect claims,
  async-child actions, and nested batches are not representable;
- the reducer applies each component through the existing single-action reducer semantics against
  a private working state, checks every component's existing permission, then emits one projection
  version, one transition, combined ledger entries, and one `workflow_run.apply_authority_batch`
  outbox event;
- `execute_family_admission` appends the existing
  `workflow_run.family_admission_committed` final event in the same aggregate version and existing
  repository transaction.
- the sequence-1 batch event carries `authority_batch_digest`, the canonical SHA-256 digest of the
  exact validated batch, plus a canonical ordered `action_identity_summary`; the summary identifies
  each usage, pending settlement, effect observation/settlement, obligation, and output through
  fixed-size SHA-256 identity digests rather than copying potentially large references into the
  outbox payload. Effect-settlement identities also include the digest of their exact
  `usage_settlement_ref`.

Security-review hardening makes the seam fail closed:

- a family registration that allows `apply_authority_batch` must separately declare its allowed
  and required nested action kinds and a typed mutation-to-batch reference validator; foundation
  validation applies the closed action union first, and the family validator can only narrow it;
- effect settlement now requires `usage_settlement_ref` to name an authoritative
  digest-bound `UsageSettlementRecord` whose originating usage and reservation match the effect
  claim and whose explicit operation-authority reference matches the effect claim's exact operation
  reference. `SettlePendingUsageAction` names its originating usage, must non-emptily reconcile that
  usage's complete pending amounts, cannot reuse any usage identity as its settlement identity, and
  cannot settle one usage twice under different IDs;
- immutable usage records and provenance-digested settlement records live inside the existing
  canonical `BudgetState` JSON projection together with deterministic outstanding-usage IDs; no
  database migration is required;
- service and reducer entries strictly reconstruct `LifecycleCommand`, reducer defense-in-depth
  checks exact nested classes and cardinality, and missing/unknown permission mappings reject
  instead of implying no permission;
- family admission resolves the exact registered mutation class and strictly reconstructs the
  mutation before kind binding, batch reference policy, fingerprinting, or repository access;
- one dedicated `authority_state_digest` now computes budget/effect CAS digests from Python-mode
  state and canonical serialization. Service creation and both in-memory/PostgreSQL plain and
  family-admission comparisons use it consistently, so semantically equal frozensets/maps cannot
  conflict because of JSON array/object construction order; request and command fingerprints remain
  unchanged;
- both `ApplyAuthorityBatchAction` and `LifecycleCommand` enforce a 65,536-byte canonical serialized
  ceiling, including aggregate payloads assembled from prebuilt nested models; the outbox identity
  summary has its own 32,768-byte ceiling and remains bounded at the maximum 64 actions.

No existing FastAPI request-body-limit owner was found in the run-control API. A transport-level
limit therefore remains an integration qualification item; the domain boundary enforces the byte
ceiling now regardless of transport.

`JournaledOperationExecutionCoordinator` now records operation usage before effect settlement inside
one bounded `ApplyAuthorityBatchAction` after effect observation. Legitimate completed operations
therefore establish matching usage provenance and settle the effect in one run-version transition;
the focused regression also proves the resulting settled run can terminalize.

When operation usage remains externally pending, the coordinator records only the usage and a
`reconciliation_required` journal revision while leaving the effect open. Its explicit later
`settle_pending_usage` path atomically settles the named pending usage and the exactly bound effect,
then stores the terminal journal revision. Zero-pending and pending-then-settled regressions both
terminalize.

Journal-only settlement mutations now carry the exact accepted run-control authority result and
the exact `LifecycleCommand`. Service validation recomputes the command fingerprint and proves the
action binds the expected usage, effect, operation authority, and settlement. In-memory tests seed
the authoritative command/result/event triple and fail closed without it; PostgreSQL compares the
proof against lifecycle-command results, transition command/version, and sequence-1 outbox event,
including the batch digest. Both journals permit the journal row to commit when the locked current
run version has advanced beyond that authority result, while rejecting missing, stale,
wrong-command, unrelated-action, or fingerprint-colliding authority.

Coordinator retries first look up the stable command identity. After an authority-to-journal crash,
they rebuild with the original accepted prior version, verify the original fingerprint, and reuse
the accepted result rather than authoring against the current version. Failure injection covers
both initial authority settlement and pending-reconciliation revision 2.

Each usage settlement is durably bound to at most one effect ID, so a second effect cannot consume
the same settlement even when reservation and operation authority match. PostgreSQL settlement
revision validation now mirrors in-memory rules: revision 1 is mandatory initially, only
`reconciliation_required` may advance, the next revision is exactly prior + 1, and settlement,
effect, and scope identities remain stable. Settlement mutation identities include their revision
so pending and terminal revisions replay independently.

Final proof hardening strictly reconstructs copied claim, settlement, prior-settlement, command, and
result models. The accepted command accounts exactly for settlement usage, released and pending
amounts, outcome/status, effect and usage identities, operation authority, and result-manifest
evidence; omitted, substituted, extra, negative, or malformed values fail closed. Budget reduction
also validates each amount map independently before merge/max operations.

Migration `0012_graph_runtime_operation_journal.sql` is preserved unchanged because migrations
`0013` through `0017` already follow it. Forward migration
`0018_operation_settlement_revisions_v1.sql` changes the settlement primary key to
`(request_scope, settlement_id, settlement_revision)` and adds a latest-revision index while
preserving existing rows. Repository validation retains the stable logical settlement identity and
strict prior-revision chain.

Legacy settlement payloads that truly omit both `digest_version` and `released_usage` are restored
as explicit `legacy-v1` revision-1 records and verified against the exact historical canonical
shape. Every new `create` emits `complete-v2`; journal mutations reject legacy digests as new writes,
and payloads that inject `released_usage` without a version fail rather than downgrade. Revision 2
may cite a verified legacy prior while its own digest remains complete-v2.

Migration 0018 also performs a fail-closed pending-usage provenance backfill only when exactly one
`reconciliation_required` operation settlement maps to the run, its pending amounts exactly equal
the aggregate pending budget, and the authoritative effect ledger supplies a nonempty reservation
and operation reference. Missing, multiple, mismatched, or already-upgraded candidates are left
untouched. Application startup must apply 0018 before permitting reconciliation; unresolved rows
require operator investigation rather than permissive fallback.

No family-specific contract or dispatch behavior is added. Terminality remains a separate
reducer-only action. Existing run/family CAS, authority digests, fingerprints, replay/collision
rules, rollback boundaries, and exact family registration remain unchanged. No family-admission
schema migration is required; the separate forward operation-journal key migration is described
above.

## New base and integration rule

`20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e` is the immutable input base for the third amendment.
After review, the integrator should create one foundation-amendment commit containing exactly this
addendum, the family-neutral contract/reducer/service changes, and focused tests. That resulting
commit SHA becomes the new shared `BP_BASE_REVISION` and must be merged unchanged into both BP
branches and `integration/bp-runtimes`.

The base must advance because family reservation and family cursor/state admission otherwise cannot
be proven atomic. Advancing both branches to the same exact amendment commit avoids either family
privately redefining run-control persistence or lock ordering.

## Changed paths

Third amendment:

- `app/domain/run_control/contracts.py`
- `app/domain/run_control/reducer.py`
- `app/application/run_control.py`
- `app/application/run_control_repository.py`
- `app/application/postgres_run_control_repository.py`
- `app/application/journaled_operation_execution.py`
- `app/application/operation_journal.py`
- `app/application/postgres_operation_journal.py`
- `app/domain/operation_execution/journal.py`
- `app/migrations/0018_operation_settlement_revisions_v1.sql`
- `tests/test_atomic_family_admission.py`
- `tests/test_atomic_family_admission_postgres_integration.py`
- `tests/test_operation_execution.py`
- `tests/test_operation_journal_stage1.py`
- `tests/acceptance/control_plane/test_wp_cp_020.py`
- this handoff

Second amendment:

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

- critical acquisition-order suites:
  `86 passed, 1 skipped, 13 warnings`; command identity is now derived from request scope,
  operation-contract digest, and journal idempotency key, so concurrent regenerated claim IDs share
  one command identity and conflict on fingerprint before a second run mutation;
- critical PostgreSQL-gated command: `10 passed, 9 skipped, 1 warning`; credentials remained
  unavailable, while exact replay and in-memory concurrent collision assertions passed;
- both operation-journal adapters treat authority command/result as child mutation evidence:
  exact claim+proof replay is idempotent, while regenerated identity/proof is rejected and cannot
  leave a second authoritative effect claim;
- P1 legacy-upgrade suites:
  `85 passed, 1 skipped, 13 warnings`; exact legacy digest fixtures, downgrade/mismatch rejection,
  deterministic pending-provenance reconstruction, successful reconciled effect settlement, and
  ambiguous/missing evidence rejection are covered;
- P1 PostgreSQL-gated command: `9 passed, 9 skipped, 1 warning`; credentials remained unavailable,
  while all non-PostgreSQL migration and upgrade checks passed;
- final medium-finding suites:
  `83 passed, 1 skipped, 13 warnings`; exact claim+proof replay succeeds, while regenerated claim
  identity with child authority evidence is rejected instead of canonicalized;
- final PostgreSQL-gated command: `8 passed, 9 skipped, 1 warning`; credentials remained
  unavailable, while the in-memory and collection-level claim collision assertions passed;
- both journal repositories now classify `authority_command` and `authority_result` as claim child
  mutation evidence, preserving proof-free legacy key canonicalization only when no authoritative
  run-control child can be orphaned;
- current final-verification suites:
  `82 passed, 1 skipped, 13 warnings`; the skip is credential-gated PostgreSQL journal coverage and
  warnings were existing LangSmith and Temporal/Pydantic deprecations;
- current combined PostgreSQL-gated command: `7 passed, 9 skipped, 1 warning`; credentials remained
  unavailable, while all non-PostgreSQL proof/migration assertions passed;
- final verification adds stable claim-command replay after later run advances, claim payload
  digest binding, complete settlement-payload evidence inside the accepted authority batch,
  canonical `pending:{settlement_id}` revision-2 identity enforcement, and strict model
  reconstruction/adversarial field substitutions;
- current final security-audit and migration suites:
  `81 passed, 1 skipped, 13 warnings`; the skip is credential-gated PostgreSQL journal coverage and
  warnings were existing LangSmith and Temporal/Pydantic deprecations;
- current combined PostgreSQL-gated command: `6 passed, 9 skipped, 1 warning`; credentials remained
  unavailable, while all non-PostgreSQL journal/migration assertions passed;
- current adversarial coverage mutates usage, release, pending, status/outcome, manifest,
  settlement identity, and malformed copied payloads; it also proves independent negative-map
  rejection, crash replay, strict in-memory revision parity, and the forward migration shape;
- latest final security-audit suites:
  `77 passed, 1 skipped, 13 warnings`; the skip is the credential-gated PostgreSQL journal case and
  warnings were existing LangSmith and Temporal/Pydantic deprecations;
- latest combined PostgreSQL-gated command: `4 passed, 9 skipped, 1 warning`; credentials remained
  unavailable, so all live PostgreSQL cases skipped;
- latest regressions cover authority-to-journal crash/retry for initial and pending flows,
  exact command fingerprint reuse, forged command/action proof rejection, one-settlement/one-effect,
  and strict journal settlement revision chains;
- final complete authority/run-control/operation/CP-020/journal suites:
  `74 passed, 1 skipped, 13 warnings`; the skip is the credential-gated PostgreSQL journal case and
  warnings were existing LangSmith and Temporal/Pydantic deprecations;
- final combined PostgreSQL-gated command: `4 passed, 9 skipped, 1 warning`; credentials were
  unavailable, so the four in-memory journal tests ran while all live PostgreSQL cases skipped;
- final liveness coverage includes usage/settlement identity collision, double settlement,
  same-reservation wrong-operation rejection, zero-pending settlement, pending-then-settled
  operation recovery, journal replay/collision, and journal commit after a later run-version advance;
- final-review authority/run-control/operation/CP-020 suites:
  `68 passed, 13 warnings`; warnings were existing LangSmith and Temporal/Pydantic deprecations;
- final-review PostgreSQL-gated suites: `8 skipped` because
  `TEST_APPLICATION_POSTGRES_DSN` was unavailable; this is not live PostgreSQL evidence;
- final-review coverage includes empty and unrelated usage-settlement rejection, exact
  usage/reservation provenance and digest persistence, same-batch linkage, replay/rollback,
  malformed/oversized/wrong-type family mutation rejection, operation effect settlement, and
  terminalization;
- CAS regressions prove equal reordered `BudgetState` and `EffectLedgerState` values share one
  authority digest and that reordered reads commit successfully through plain and combined
  in-memory paths; the equivalent PostgreSQL path is present behind the credential gate;
- hardened third-amendment focused in-memory suites:
  `35 passed` (`tests/test_atomic_family_admission.py tests/test_run_control.py`);
- focused event assertions cover exact batch-digest and nested-identity stability, command replay,
  consumer redelivery/deduplication, maximum-cardinality summary boundedness, and unchanged
  sequence-1 batch/sequence-2 family finality;
- hardened control-plane effect-settlement regression:
  `5 passed` (`tests/acceptance/control_plane/test_wp_cp_020.py`), included in an extended
  `40 passed` run with the focused suites;
- third-amendment PostgreSQL-gated suites: `6 skipped` because
  `TEST_APPLICATION_POSTGRES_DSN` was unavailable; this is not live PostgreSQL evidence;
- mypy after the third amendment: the first final invocation exited with mypy 1.20.2's internal
  error; an immediate unchanged-tree retry succeeded with
  `Success: no issues found in 322 source files`;
- ruff after the third amendment: `All checks passed`;
- `git diff --check` after the third amendment: passed (Git emitted only local working-tree
  line-ending conversion notices);
- second-amendment focused atomic/run-control suites: `24 passed, 5 skipped`;
- first-amendment operation/Temporal regression suites: `45 passed` (warnings only);
- second-amendment PostgreSQL tests: all five skipped because
  `TEST_APPLICATION_POSTGRES_DSN` was unavailable;
- Docker PostgreSQL fallback: unavailable because the Docker Desktop Linux engine was not running;

Disposition: `ready_for_review` after the commands above pass or are explicitly recorded as gated.
