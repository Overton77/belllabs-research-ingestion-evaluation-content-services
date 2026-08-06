ALTER TABLE belllabs_control.operation_effect_claims
    ADD COLUMN source_system text,
    ADD COLUMN source_collection text,
    ADD COLUMN source_document_id text,
    ADD COLUMN source_recorded_at timestamptz,
    ADD COLUMN source_canonical_digest text CHECK (
        source_canonical_digest IS NULL
        OR source_canonical_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    ADD COLUMN target_canonical_digest text CHECK (
        target_canonical_digest IS NULL
        OR target_canonical_digest ~ '^sha256:[0-9a-f]{64}$'
    );

CREATE UNIQUE INDEX operation_effect_claims_source_lineage_idx
    ON belllabs_control.operation_effect_claims (
        request_scope, source_system, source_collection, source_document_id
    )
    WHERE source_system IS NOT NULL;

ALTER TABLE belllabs_control.operation_settlements
    ADD COLUMN source_system text,
    ADD COLUMN source_collection text,
    ADD COLUMN source_document_id text,
    ADD COLUMN source_recorded_at timestamptz,
    ADD COLUMN source_canonical_digest text CHECK (
        source_canonical_digest IS NULL
        OR source_canonical_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    ADD COLUMN target_canonical_digest text CHECK (
        target_canonical_digest IS NULL
        OR target_canonical_digest ~ '^sha256:[0-9a-f]{64}$'
    );

CREATE UNIQUE INDEX operation_settlements_source_lineage_idx
    ON belllabs_control.operation_settlements (
        request_scope, source_system, source_collection, source_document_id
    )
    WHERE source_system IS NOT NULL;

CREATE TABLE belllabs_control.operation_journal_backfill_batches (
    request_scope text NOT NULL,
    migration_stream text NOT NULL,
    run_id text NOT NULL,
    status text NOT NULL CHECK (
        status IN ('running', 'completed', 'failed', 'dry_run')
    ),
    source_cursor text,
    source_snapshot_digest text NOT NULL CHECK (
        source_snapshot_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    source_snapshot_payload jsonb NOT NULL,
    source_claim_count bigint NOT NULL DEFAULT 0 CHECK (source_claim_count >= 0),
    source_settlement_count bigint NOT NULL DEFAULT 0 CHECK (source_settlement_count >= 0),
    target_claim_count bigint NOT NULL DEFAULT 0 CHECK (target_claim_count >= 0),
    target_settlement_count bigint NOT NULL DEFAULT 0 CHECK (target_settlement_count >= 0),
    quarantine_count bigint NOT NULL DEFAULT 0 CHECK (quarantine_count >= 0),
    source_aggregate_digest text CHECK (
        source_aggregate_digest IS NULL
        OR source_aggregate_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    target_aggregate_digest text CHECK (
        target_aggregate_digest IS NULL
        OR target_aggregate_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    started_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    completed_at timestamptz,
    failure_summary text,
    PRIMARY KEY (request_scope, migration_stream, run_id)
);

CREATE TABLE belllabs_control.operation_journal_backfill_applied_batches (
    request_scope text NOT NULL,
    migration_stream text NOT NULL,
    run_id text NOT NULL,
    batch_cursor text NOT NULL,
    previous_cursor text,
    batch_digest text NOT NULL CHECK (batch_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_count bigint NOT NULL CHECK (source_count >= 1),
    admitted_claim_count bigint NOT NULL CHECK (admitted_claim_count >= 0),
    admitted_settlement_count bigint NOT NULL CHECK (admitted_settlement_count >= 0),
    quarantine_count bigint NOT NULL CHECK (quarantine_count >= 0),
    applied_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, migration_stream, run_id, batch_cursor),
    FOREIGN KEY (request_scope, migration_stream, run_id)
        REFERENCES belllabs_control.operation_journal_backfill_batches (
            request_scope, migration_stream, run_id
        )
);

CREATE UNIQUE INDEX operation_journal_backfill_active_stream_idx
    ON belllabs_control.operation_journal_backfill_batches (
        request_scope, migration_stream
    )
    WHERE status = 'running';

CREATE INDEX operation_journal_backfill_resume_idx
    ON belllabs_control.operation_journal_backfill_batches (
        request_scope, migration_stream, updated_at, run_id
    )
    WHERE status IN ('running', 'failed');

CREATE TABLE belllabs_control.operation_journal_backfill_quarantine (
    quarantine_id text NOT NULL,
    request_scope text NOT NULL,
    migration_stream text NOT NULL,
    source_collection text NOT NULL,
    source_document_id text NOT NULL,
    reason_code text NOT NULL,
    observed_digest text CHECK (
        observed_digest IS NULL OR observed_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    expected_digest text CHECK (
        expected_digest IS NULL OR expected_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    observed_request_scope text,
    quarantined_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, quarantine_id),
    UNIQUE (
        request_scope, migration_stream, source_collection, source_document_id
    )
);

CREATE INDEX operation_journal_backfill_quarantine_reason_idx
    ON belllabs_control.operation_journal_backfill_quarantine (
        request_scope, migration_stream, reason_code, source_document_id
    );

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'operation_journal_backfill_batches',
        'operation_journal_backfill_applied_batches',
        'operation_journal_backfill_quarantine'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name
        );
        EXECUTE format(
            'CREATE POLICY request_scope_isolation ON belllabs_control.%I
             USING (request_scope = current_setting(''belllabs.request_scope'', true))
             WITH CHECK (request_scope = current_setting(''belllabs.request_scope'', true))',
            table_name
        );
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name
        );
    END LOOP;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_operation_backfill'
    ) THEN
        CREATE ROLE belllabs_operation_backfill NOLOGIN NOSUPERUSER NOCREATEDB
            NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA belllabs_control TO belllabs_operation_backfill;

GRANT SELECT, INSERT
    ON belllabs_control.operation_effect_claims,
       belllabs_control.operation_settlements,
       belllabs_control.operation_journal_backfill_batches,
       belllabs_control.operation_journal_backfill_applied_batches,
       belllabs_control.operation_journal_backfill_quarantine
    TO belllabs_operation_backfill;

GRANT UPDATE (
    status, heartbeat_at, lease_expires_at
)
    ON belllabs_control.operation_effect_claims
    TO belllabs_operation_backfill;

GRANT UPDATE (
    status, source_cursor, source_claim_count, source_settlement_count,
    target_claim_count, target_settlement_count, quarantine_count,
    source_aggregate_digest, target_aggregate_digest, updated_at,
    completed_at, failure_summary
)
    ON belllabs_control.operation_journal_backfill_batches
    TO belllabs_operation_backfill;

GRANT UPDATE (
    reason_code, observed_digest, expected_digest, observed_request_scope
)
    ON belllabs_control.operation_journal_backfill_quarantine
    TO belllabs_operation_backfill;

REVOKE UPDATE ON belllabs_control.operation_effect_claims
    FROM belllabs_control_runtime;
REVOKE INSERT ON belllabs_control.operation_effect_claims,
    belllabs_control.operation_settlements
    FROM belllabs_control_runtime;
GRANT INSERT (
    effect_claim_id, request_scope, belllabs_run_id,
    operation_contract_digest, idempotency_key, request_digest,
    semantic_binding_id, semantic_binding_digest, semantic_attempt_key,
    claim_mode, status, claimed_by, claimed_at, heartbeat_at, lease_expires_at
)
    ON belllabs_control.operation_effect_claims
    TO belllabs_control_runtime;
GRANT INSERT (
    settlement_id, request_scope, effect_claim_id, settlement_revision,
    settlement_digest, status, usage_payload, pending_external_usage_payload,
    result_manifest_ref, result_manifest_digest, result_manifest_size_bytes,
    failure_code, settlement_payload, settled_at
)
    ON belllabs_control.operation_settlements
    TO belllabs_control_runtime;
GRANT UPDATE (status, heartbeat_at, lease_expires_at)
    ON belllabs_control.operation_effect_claims
    TO belllabs_control_runtime;

GRANT SELECT
    ON belllabs_control.operation_journal_backfill_batches,
       belllabs_control.operation_journal_backfill_applied_batches,
       belllabs_control.operation_journal_backfill_quarantine
    TO belllabs_operations_readonly;
