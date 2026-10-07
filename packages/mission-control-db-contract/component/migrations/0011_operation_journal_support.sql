-- mission-control-db-contract common release: operation journal support records.
-- Claim-before-effect is the canonical operation_intent; the exact claim identity and
-- lease (support operation_claim), technical provider attempts (observations, never
-- attempt_no), idempotent journal mutations and settlement revisions are support. Exactly
-- one terminal settlement per claim becomes the canonical operation_receipt.

CREATE TABLE mission_control.operation_claim (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_claim_id uuid PRIMARY KEY,
    claim_key text NOT NULL CHECK (claim_key <> ''),
    operation_intent_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    operation_contract_digest text NOT NULL CHECK (operation_contract_digest ~ '^sha256:[0-9a-f]{64}$'),
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    semantic_binding_key text NOT NULL CHECK (semantic_binding_key <> ''),
    semantic_binding_digest text NOT NULL CHECK (semantic_binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    semantic_attempt_key text NOT NULL CHECK (semantic_attempt_key <> ''),
    unit_key text CHECK (unit_key IS NULL OR unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    claim_mode text NOT NULL CHECK (claim_mode = 'active'),
    status text NOT NULL CHECK (status IN ('claimed', 'executing', 'settled', 'reconciliation_required', 'cancelled')),
    claimed_by text NOT NULL CHECK (claimed_by <> ''),
    claimed_at timestamptz NOT NULL,
    heartbeat_at timestamptz,
    lease_expires_at timestamptz,
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, operation_claim_id),
    UNIQUE (installation_id, application_id, tenant_id, claim_key),
    UNIQUE (installation_id, application_id, tenant_id, operation_contract_digest, idempotency_key),
    UNIQUE (installation_id, application_id, tenant_id, semantic_attempt_key),
    UNIQUE (installation_id, application_id, tenant_id, operation_intent_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, operation_intent_id) REFERENCES mission_control.operation_intent (installation_id, application_id, tenant_id, operation_intent_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX operation_claim_run_fk_idx ON mission_control.operation_claim (installation_id, application_id, tenant_id, run_key);
CREATE INDEX operation_claim_unit_idx ON mission_control.operation_claim (installation_id, application_id, tenant_id, unit_key) WHERE unit_key IS NOT NULL;
CREATE INDEX operation_claim_reconcile_idx ON mission_control.operation_claim (installation_id, application_id, tenant_id, status, lease_expires_at, claim_key) WHERE status = 'reconciliation_required';

CREATE TABLE mission_control.operation_journal_mutation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_journal_mutation_id uuid PRIMARY KEY,
    mutation_key text NOT NULL CHECK (mutation_key <> ''),
    claim_key text NOT NULL CHECK (claim_key <> ''),
    mutation_digest text NOT NULL CHECK (mutation_digest ~ '^sha256:[0-9a-f]{64}$'),
    mutation_payload jsonb NOT NULL CHECK (jsonb_typeof(mutation_payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, operation_journal_mutation_id),
    UNIQUE (installation_id, application_id, tenant_id, mutation_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, claim_key) REFERENCES mission_control.operation_claim (installation_id, application_id, tenant_id, claim_key)
);
CREATE INDEX operation_journal_mutation_claim_fk_idx ON mission_control.operation_journal_mutation (installation_id, application_id, tenant_id, claim_key);
CREATE TRIGGER operation_journal_mutation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.operation_journal_mutation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.operation_technical_attempt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_technical_attempt_id uuid PRIMARY KEY,
    attempt_key text NOT NULL CHECK (attempt_key <> ''),
    claim_key text NOT NULL CHECK (claim_key <> ''),
    technical_attempt bigint NOT NULL CHECK (technical_attempt >= 1),
    provider text NOT NULL CHECK (provider <> ''),
    provider_attempt_key text,
    disposition text NOT NULL CHECK (disposition IN ('created', 'running', 'ambiguous', 'succeeded', 'failed', 'cancelled')),
    idempotency_supported boolean NOT NULL,
    retry_class text NOT NULL CHECK (retry_class IN ('safe', 'claim_then_reconcile', 'non_retryable')),
    usage_payload jsonb NOT NULL CHECK (jsonb_typeof(usage_payload) = 'object'),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    failure_code text,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, operation_technical_attempt_id),
    UNIQUE (installation_id, application_id, tenant_id, attempt_key),
    UNIQUE (installation_id, application_id, tenant_id, claim_key, technical_attempt),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, claim_key) REFERENCES mission_control.operation_claim (installation_id, application_id, tenant_id, claim_key)
);
CREATE INDEX operation_technical_attempt_provider_idx ON mission_control.operation_technical_attempt (installation_id, application_id, tenant_id, provider, provider_attempt_key) WHERE provider_attempt_key IS NOT NULL;
CREATE TRIGGER operation_technical_attempt_immutable
    BEFORE UPDATE OR DELETE ON mission_control.operation_technical_attempt
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.operation_settlement (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_settlement_id uuid PRIMARY KEY,
    settlement_key text NOT NULL CHECK (settlement_key <> ''),
    claim_key text NOT NULL CHECK (claim_key <> ''),
    settlement_revision bigint NOT NULL CHECK (settlement_revision >= 1),
    settlement_digest text NOT NULL CHECK (settlement_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('completed', 'failed', 'cancelled', 'timed_out', 'reconciliation_required')),
    usage_payload jsonb NOT NULL CHECK (jsonb_typeof(usage_payload) = 'object'),
    pending_external_usage_payload jsonb NOT NULL CHECK (jsonb_typeof(pending_external_usage_payload) = 'object'),
    result_manifest_ref text,
    result_manifest_digest text CHECK (result_manifest_digest IS NULL OR result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_size_bytes bigint CHECK (result_manifest_size_bytes IS NULL OR result_manifest_size_bytes >= 1),
    failure_code text,
    settlement_contract text NOT NULL CHECK (settlement_contract = 'mc.operation-settlement/1'),
    settlement_payload jsonb NOT NULL CHECK (jsonb_typeof(settlement_payload) = 'object'),
    operation_receipt_id uuid,
    settled_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((status = 'reconciliation_required') = (operation_receipt_id IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, operation_settlement_id),
    UNIQUE (installation_id, application_id, tenant_id, settlement_key, settlement_revision),
    UNIQUE (installation_id, application_id, tenant_id, claim_key, settlement_revision),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, claim_key) REFERENCES mission_control.operation_claim (installation_id, application_id, tenant_id, claim_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, operation_receipt_id) REFERENCES mission_control.operation_receipt (installation_id, application_id, tenant_id, operation_receipt_id)
);
CREATE UNIQUE INDEX operation_settlement_one_terminal_idx ON mission_control.operation_settlement (installation_id, application_id, tenant_id, claim_key) WHERE status <> 'reconciliation_required';
CREATE INDEX operation_settlement_receipt_fk_idx ON mission_control.operation_settlement (installation_id, application_id, tenant_id, operation_receipt_id);
CREATE TRIGGER operation_settlement_immutable
    BEFORE UPDATE OR DELETE ON mission_control.operation_settlement
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'operation_claim',
        'operation_journal_mutation',
        'operation_technical_attempt',
        'operation_settlement'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id())',
            table_name || '_scope', table_name);
    END LOOP;
END
$policies$;

REVOKE ALL ON mission_control.operation_claim, mission_control.operation_journal_mutation,
    mission_control.operation_technical_attempt, mission_control.operation_settlement FROM PUBLIC;

-- Port of the legacy control runtime role: claims are inserted once, then only their lease and
-- status columns change; attempts, mutations, settlements and receipts are insert-only.
GRANT SELECT, INSERT ON mission_control.operation_receipt, mission_control.operation_claim,
    mission_control.operation_journal_mutation, mission_control.operation_technical_attempt,
    mission_control.operation_settlement
TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.operation_intent TO mission_control_runtime;
GRANT UPDATE (status, heartbeat_at, lease_expires_at, version, updated_at)
ON mission_control.operation_claim TO mission_control_runtime;
GRANT SELECT ON mission_control.operation_intent, mission_control.operation_receipt,
    mission_control.operation_claim, mission_control.operation_journal_mutation,
    mission_control.operation_technical_attempt, mission_control.operation_settlement
TO mission_control_readonly;
