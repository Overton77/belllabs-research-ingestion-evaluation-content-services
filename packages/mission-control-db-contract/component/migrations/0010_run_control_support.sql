-- mission-control-db-contract common release: run-control support records and grants.
-- Source owner: runtime authority lane. Canonical lifecycle, acceptance, events and outbox
-- stay in mission_run / request_receipt / ledger_commit / mission_event / outbox; the
-- support records below hold only the exact legacy contracts those tables cannot express
-- (per-run effect ledger, version-indexed lifecycle transitions, atomic family admission).

CREATE TABLE mission_control.run_lifecycle_transition (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_lifecycle_transition_id uuid PRIMARY KEY,
    transition_key text NOT NULL CHECK (transition_key <> ''),
    run_key text NOT NULL CHECK (run_key <> ''),
    command_key text NOT NULL CHECK (command_key <> ''),
    prior_version bigint NOT NULL CHECK (prior_version >= 0),
    resulting_version bigint NOT NULL CHECK (resulting_version >= 1),
    ledger_commit_id uuid,
    transition_contract text NOT NULL CHECK (transition_contract = 'mc.run-lifecycle-transition/1'),
    transition jsonb NOT NULL CHECK (jsonb_typeof(transition) = 'object'),
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (resulting_version > prior_version),
    UNIQUE (installation_id, application_id, tenant_id, run_lifecycle_transition_id),
    UNIQUE (installation_id, application_id, tenant_id, transition_key),
    UNIQUE (installation_id, application_id, tenant_id, run_key, resulting_version),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, ledger_commit_id) REFERENCES mission_control.ledger_commit (installation_id, application_id, tenant_id, ledger_commit_id)
);
CREATE INDEX run_lifecycle_transition_ledger_commit_fk_idx ON mission_control.run_lifecycle_transition (installation_id, application_id, tenant_id, ledger_commit_id);
CREATE TRIGGER run_lifecycle_transition_immutable
    BEFORE UPDATE OR DELETE ON mission_control.run_lifecycle_transition
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.effect_ledger (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    effect_ledger_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    state_contract text NOT NULL CHECK (state_contract = 'mc.effect-ledger/1'),
    state jsonb NOT NULL CHECK (jsonb_typeof(state) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, effect_ledger_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);

CREATE TABLE mission_control.effect_ledger_entry (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    effect_ledger_entry_id uuid PRIMARY KEY,
    entry_key text NOT NULL CHECK (entry_key <> ''),
    run_key text NOT NULL CHECK (run_key <> ''),
    effect_key text NOT NULL CHECK (effect_key <> ''),
    kind text NOT NULL CHECK (kind IN ('claim', 'observation', 'settlement')),
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    entry_contract text NOT NULL CHECK (entry_contract = 'mc.effect-ledger-entry/1'),
    entry jsonb NOT NULL CHECK (jsonb_typeof(entry) = 'object'),
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, effect_ledger_entry_id),
    UNIQUE (installation_id, application_id, tenant_id, entry_key),
    UNIQUE (installation_id, application_id, tenant_id, run_key, effect_key, kind, idempotency_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.effect_ledger (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX effect_ledger_entry_run_order_idx ON mission_control.effect_ledger_entry (installation_id, application_id, tenant_id, run_key, occurred_at, entry_key);
CREATE TRIGGER effect_ledger_entry_immutable
    BEFORE UPDATE OR DELETE ON mission_control.effect_ledger_entry
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.family_admission_head (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    family_admission_head_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    family_kind text NOT NULL CHECK (family_kind <> ''),
    family_version bigint NOT NULL CHECK (family_version >= 1),
    mutation_fingerprint text NOT NULL CHECK (mutation_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    mutation_contract text NOT NULL CHECK (mutation_contract = 'mc.family-mutation/1'),
    mutation jsonb NOT NULL CHECK (jsonb_typeof(mutation) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, family_admission_head_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key, family_kind),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);

CREATE TABLE mission_control.family_admission_journal (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    family_admission_journal_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    family_kind text NOT NULL CHECK (family_kind <> ''),
    family_version bigint NOT NULL CHECK (family_version >= 1),
    mutation_kind text NOT NULL CHECK (mutation_kind <> ''),
    mutation_key text NOT NULL CHECK (mutation_key <> ''),
    mutation_fingerprint text NOT NULL CHECK (mutation_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    mutation_contract text NOT NULL CHECK (mutation_contract = 'mc.family-mutation/1'),
    mutation jsonb NOT NULL CHECK (jsonb_typeof(mutation) = 'object'),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, family_admission_journal_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key, family_kind, mutation_key),
    UNIQUE (installation_id, application_id, tenant_id, run_key, family_kind, family_version),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE TRIGGER family_admission_journal_immutable
    BEFORE UPDATE OR DELETE ON mission_control.family_admission_journal
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.family_admission_result (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    family_admission_result_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    idempotency_issuer text NOT NULL CHECK (idempotency_issuer <> ''),
    command_key text NOT NULL CHECK (command_key <> ''),
    command_fingerprint text NOT NULL CHECK (command_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    family_mutation_fingerprint text NOT NULL CHECK (family_mutation_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    request_receipt_id uuid NOT NULL,
    receipt_contract text NOT NULL CHECK (receipt_contract = 'mc.family-admission-receipt/1'),
    receipt jsonb NOT NULL CHECK (jsonb_typeof(receipt) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, family_admission_result_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key, idempotency_issuer, command_key),
    UNIQUE (installation_id, application_id, tenant_id, request_receipt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, request_receipt_id) REFERENCES mission_control.request_receipt (installation_id, application_id, tenant_id, request_receipt_id)
);
CREATE TRIGGER family_admission_result_immutable
    BEFORE UPDATE OR DELETE ON mission_control.family_admission_result
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'run_lifecycle_transition',
        'effect_ledger',
        'effect_ledger_entry',
        'family_admission_head',
        'family_admission_journal',
        'family_admission_result'
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

REVOKE ALL ON mission_control.run_lifecycle_transition, mission_control.effect_ledger,
    mission_control.effect_ledger_entry, mission_control.family_admission_head,
    mission_control.family_admission_journal, mission_control.family_admission_result FROM PUBLIC;

-- Canonical tables written by run control (port of the legacy control runtime role grants).
GRANT USAGE ON SEQUENCE mission_control.outbox_global_position
    TO mission_control_runtime, mission_control_family_writer;
GRANT SELECT, INSERT ON mission_control.request_receipt, mission_control.definition_snapshot,
    mission_control.mission_revision, mission_control.compiled_program,
    mission_control.ledger_commit, mission_control.mission_event, mission_control.budget_entry,
    mission_control.delivery_report
TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.mission, mission_control.mission_run,
    mission_control.budget_account, mission_control.outbox, mission_control.consumer_cursor,
    mission_control.command
TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.run_lifecycle_transition,
    mission_control.effect_ledger_entry
TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.effect_ledger TO mission_control_runtime;
-- Family records are written only by the family writer; runtime reads them.
GRANT SELECT ON mission_control.family_admission_head, mission_control.family_admission_journal,
    mission_control.family_admission_result
TO mission_control_runtime;

-- Family writer: exactly what commit_family_admission uses (port of
-- the legacy family repository writer role). It reads commands and appends delivery reports for
-- receipts of already accepted commands; it never inserts commands.
GRANT SELECT, INSERT ON mission_control.request_receipt, mission_control.ledger_commit,
    mission_control.mission_event, mission_control.outbox, mission_control.budget_entry,
    mission_control.delivery_report, mission_control.run_lifecycle_transition,
    mission_control.effect_ledger_entry, mission_control.family_admission_journal,
    mission_control.family_admission_result
TO mission_control_family_writer;
GRANT SELECT, UPDATE ON mission_control.mission, mission_control.mission_run,
    mission_control.budget_account, mission_control.effect_ledger
TO mission_control_family_writer;
GRANT SELECT ON mission_control.command TO mission_control_family_writer;
GRANT UPDATE (lifecycle, outcome, version, updated_at) ON mission_control.command
TO mission_control_family_writer;
GRANT SELECT, INSERT, UPDATE ON mission_control.family_admission_head
TO mission_control_family_writer;

-- Outbox relay: claim, lease and acknowledge only (no mission state mutation).
GRANT SELECT ON mission_control.outbox TO mission_control_outbox_worker;
GRANT UPDATE (delivery_state, lease_owner, lease_expires_at, attempts, next_attempt_at,
    delivered_at, version)
ON mission_control.outbox TO mission_control_outbox_worker;

-- Inspection (port of the legacy operations read-only role).
GRANT SELECT ON mission_control.request_receipt, mission_control.mission,
    mission_control.definition_snapshot, mission_control.mission_revision,
    mission_control.compiled_program, mission_control.mission_run,
    mission_control.ledger_commit, mission_control.mission_event, mission_control.outbox,
    mission_control.budget_account, mission_control.budget_entry, mission_control.command,
    mission_control.delivery_report, mission_control.run_lifecycle_transition,
    mission_control.effect_ledger, mission_control.effect_ledger_entry,
    mission_control.family_admission_head, mission_control.family_admission_journal,
    mission_control.family_admission_result
TO mission_control_readonly;
