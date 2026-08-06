# Stage 1 completion work package — Mongo claim/settlement backfill

Status: required remaining work  
Owner: next implementation agent  
Production execution authorized: no

## 1. Mission

Finish Stage 1 by implementing and proving the accepted D-13 migration:

- PostgreSQL becomes authoritative for effect claims, technical attempts, usage, settlements,
  lifecycle/version changes, budgets, and outbox coordination.
- MongoDB/Beanie remains authoritative for immutable semantic
  `OperationExecutionBinding` (OEB) records.
- Existing legacy Mongo claim and settlement records are backfilled into PostgreSQL with
  canonical identity, digest, source lineage, timestamps, and request scope.
- Legacy Mongo claim/settlement records remain immutable and readable only during the bounded
  rollback window.

Do not treat the existing Mongo OEB-v2 projection/backfill as satisfying this work package. It
does not migrate legacy claim/settlement authority into PostgreSQL.

## 2. Why this remains open

Stage 0 explicitly states:

- the backfill targets Mongo claim/settlement records, not semantic binding authority;
- PostgreSQL receives effect claim/attempt/usage/settlement authority;
- PostgreSQL references the Mongo-authoritative OEB by stable identity and digest;
- live transaction, RLS/grant, backfill, and crash-at-commit proof remained blocked.

Stage 1 currently contains:

- PostgreSQL runtime/operation journal schema in
  `app/migrations/0012_graph_runtime_operation_journal.sql`;
- `PostgresAtomicOperationJournalRepository`;
- the active journaled new-write path;
- scoped Mongo OEB reads and configured v1/v2 binding authority selection;
- a Mongo OEB-v2 migration repository.

Stage 1 does **not** currently contain the required legacy
Mongo-claim/settlement-to-PostgreSQL backfill service or its service-backed proof.

## 3. Source and destination

### Mongo source collections

- `operation_execution_claims`
  - model: `OperationExecutionClaimDocument`
  - fields currently include `request_scope`, `side_effect_key`, `binding_id`, and
    `claimed_at`; older records may omit `request_scope`.
- `operation_execution_settlements`
  - model: `OperationSettlementDocument`
  - fields currently include `request_scope`, `settlement_id`, `binding_id`, payload, and
    `settled_at`; older records may omit `request_scope`.
- `operation_execution_bindings`
  - model: `OperationExecutionBindingDocument`
  - remains the semantic authority;
  - use it to reconstruct and verify scope and immutable claim identity;
  - never delete it or copy its full semantic payload into PostgreSQL.

### PostgreSQL destinations

- `belllabs_control.operation_effect_claims`
- `belllabs_control.operation_settlements`
- no synthetic technical-attempt rows unless an accepted source record proves those facts;
- related migration lineage, batch checkpoint, and quarantine tables introduced by a new
  forward migration.

## 4. Required PostgreSQL migration

Add a new forward migration rather than assuming `0012` has never been applied:

`app/migrations/0013_legacy_operation_journal_backfill.sql`

The migration should add idempotent lineage support to claims and settlements, for example:

- `source_system text`;
- `source_collection text`;
- `source_document_id text`;
- `source_recorded_at timestamptz`;
- `source_canonical_digest text`;
- scoped unique indexes over source collection/document identity.

Also add:

- a backfill batch/checkpoint table containing run ID, status, cursor, source/target counts,
  source/target aggregate digests, timestamps, and failure summary;
- a quarantine table containing source collection/document ID, reason code, observed digest,
  expected digest, request scope when recoverable, and quarantine timestamp;
- RLS and least-privilege grants consistent with the existing `belllabs_control` pattern.

Do not store raw OEB payloads, raw secrets, PHI, checkpoint bodies, or unrestricted settlement
output bodies in the new lineage/quarantine tables.

## 5. Canonical transformation

For each legacy claim:

1. Load its OEB by `binding_id`.
2. Validate the OEB as `OperationExecutionBinding`.
3. Resolve `request_scope` from the validated OEB. If a newer source document also declares
   scope, both values must match.
4. Require the legacy `side_effect_key` to match the OEB `side_effect_key`.
5. Produce the PostgreSQL effect claim:
   - `effect_claim_id`: the same deterministic UUIDv5 grammar used by
     `JournaledOperationExecutionCoordinator`;
   - `request_scope`: OEB request scope;
   - `belllabs_run_id`: OEB run ID;
   - `operation_contract_digest`: canonical digest of the OEB operation contract reference;
   - `idempotency_key`: OEB side-effect key;
   - `request_digest`: OEB request fingerprint;
   - `semantic_binding_id`: OEB binding ID;
   - `semantic_binding_digest`: canonical digest of the complete immutable OEB;
   - `semantic_attempt_key`: OEB semantic attempt key;
   - `claim_mode`: `active`;
   - `claimed_by`: a documented migration principal because the legacy model did not preserve
     the original worker identity;
   - `claimed_at`: original Mongo claim timestamp;
   - status derived only from verified settlement state.
6. Record original Mongo collection, document ID, timestamp, and canonical source digest.

For each legacy settlement:

1. Require exactly one matching validated claim and OEB in the same request scope.
2. Validate its payload as `OperationSettlement`.
3. Require `settlement.binding_id` to match the OEB binding.
4. Produce settlement revision `1` with a deterministic canonical digest.
5. Preserve status, usage, pending external usage, provider/failure metadata, and original
   timestamp.
6. Do not copy unrestricted `output_text`, `structured_output`, or `event_payloads` into
   PostgreSQL.
7. If replay metadata is needed, stage a policy-safe manifest containing only governed
   metadata and output references in a request-scope-isolated object-store prefix. Raw output
   requires the normal policy-gated artifact path.
8. Record original Mongo collection, document ID, timestamp, and canonical source digest.

If identity, scope, digest, lineage, or payload validation fails, quarantine the record and do
not partially admit it.

## 6. Backfill service behavior

Add a dedicated async application service and repository. Suggested files:

- `app/application/operation_journal_backfill.py`
- `app/application/postgres_operation_journal_backfill.py`

Required behavior:

- cursor-based, bounded batches;
- stable ordering by Mongo source document identity;
- one PostgreSQL transaction per batch;
- scoped advisory locking so only one active backfill owns a migration stream;
- exact replay idempotency;
- same source identity/different digest is a durable conflict;
- no dual-write to old Mongo claims/settlements;
- no updates or deletes to source records;
- resumable after process failure;
- dry-run mode that performs all validation and digest/count computation without writes;
- explicit completion verification across the entire source set, not only the final batch;
- rollback/read-routing procedure that never destructively removes either side.

Cross-database atomicity is impossible. Safety comes from immutable source records,
content-addressed transformations, scoped source-lineage uniqueness, transactional PostgreSQL
batches, and resumable verification.

## 7. Read routing and cutover

The new-write path must remain PostgreSQL-authoritative.

During the accepted rollback window:

- PostgreSQL is read first for migrated/new claims and settlements;
- legacy Mongo fallback is allowed only under an explicit rollback-window configuration;
- fallback must be request-scope qualified and digest verified;
- no code path may write the same claim or settlement to both stores.

After verification and rollback-window closure:

- disable legacy claim/settlement fallback by default;
- retain source collections read-only for the accepted retention period;
- do not remove the Mongo OEB authority.

## 8. Required tests

### Pure/unit tests

- deterministic transformation and digest round trip;
- old records without explicit scope resolve through the OEB and fail closed on mismatch;
- same source ID/same digest replay;
- same source ID/different digest conflict;
- orphan claim, orphan settlement, malformed payload, missing OEB, and cross-scope quarantine;
- completed, failed, cancelled, and timed-out settlement mapping;
- pending external usage preservation;
- unrestricted output/event payloads never enter PostgreSQL or the replay manifest;
- dry run performs no writes;
- cursor resume and full-set verification;
- rollback routing is explicit and bounded.

### Service-backed integration tests

Use isolated, disposable Atlas and PostgreSQL targets:

- clean migration and upgrade through `0013`;
- positive and negative RLS tests for every new table;
- migration-owner/runtime/readonly grant tests;
- representative backfill count/digest/source-lineage proof;
- concurrent/replayed batch idempotency;
- crash before PostgreSQL commit and restart/resume;
- quarantine proof without source mutation;
- PostgreSQL-authoritative read after cutover;
- explicit rollback-window legacy read;
- proof that no Mongo source record was updated or deleted.

## 9. Environment and safety

Observed configuration on 2026-08-05:

- MongoDB Atlas is configured and a read-only ping succeeded.
- Primary Supabase PostgreSQL is configured and a read-only query succeeded.
- `APPLICATION_DATABASE_*` and `APPLICATION_MIGRATION_DATABASE_*` currently point to
  `127.0.0.1:55432`, which was not running.
- `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` were unset.

Do not run destructive clean-apply, quarantine, backfill, or rollback tests against the
existing primary Supabase database or production Atlas database.

Before integration proof:

1. Provision an isolated Supabase branch/database or disposable PostgreSQL service.
2. Provision an isolated Atlas database/collections populated with representative fixtures.
3. Configure distinct migration-owner and non-privileged runtime credentials.
4. Set `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI`.
5. Record sanitized target identities and explicit owner approval.

Production backfill requires a separate explicit approval after dry-run evidence is reviewed.

## 10. Acceptance gate

This work package is complete only when:

- migration `0013` applies cleanly and upgrades an existing schema;
- the backfill service and bounded rollback routing are implemented;
- representative and service-backed tests pass;
- source/target counts and aggregate digests match for every admitted record;
- every non-admitted source record has a deterministic quarantine reason;
- RLS and grants pass using non-superuser roles;
- crash/restart proof shows no duplicate or lost admitted facts;
- no legacy Mongo record is mutated or deleted;
- no full OEB semantic payload is copied into PostgreSQL;
- the Stage 1 handoff is updated with exact commands, counts, digests, migration IDs, and
  sanitized environment evidence.

Until then, Stage 1 implementation is not operationally accepted.
