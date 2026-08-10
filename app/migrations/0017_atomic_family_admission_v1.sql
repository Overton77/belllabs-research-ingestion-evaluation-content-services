-- Family-neutral atomic journal for reducer-authorized blueprint admission decisions.
-- Only the private application repository role may mutate these tables. The runtime role can read
-- tenant-scoped records but has no DML and no callable attachment function.

ALTER TABLE belllabs_control.workflow_runs
    ADD CONSTRAINT workflow_runs_run_scope_unique UNIQUE (run_id, request_scope);

CREATE TABLE belllabs_control.family_admission_heads (
    request_scope text NOT NULL,
    run_id text NOT NULL,
    family_kind text NOT NULL,
    family_version bigint NOT NULL CHECK (family_version >= 1),
    mutation_fingerprint text NOT NULL,
    mutation jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, run_id, family_kind),
    FOREIGN KEY (run_id, request_scope)
        REFERENCES belllabs_control.workflow_runs(run_id, request_scope)
);

CREATE TABLE belllabs_control.family_admission_journal (
    request_scope text NOT NULL,
    run_id text NOT NULL,
    family_kind text NOT NULL,
    family_version bigint NOT NULL CHECK (family_version >= 1),
    mutation_kind text NOT NULL,
    mutation_id text NOT NULL,
    mutation_fingerprint text NOT NULL,
    mutation jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, run_id, family_kind, mutation_id),
    UNIQUE (request_scope, run_id, family_kind, family_version),
    FOREIGN KEY (run_id, request_scope)
        REFERENCES belllabs_control.workflow_runs(run_id, request_scope)
);

CREATE TABLE belllabs_control.family_admission_results (
    request_scope text NOT NULL,
    run_id text NOT NULL,
    idempotency_issuer text NOT NULL,
    command_id text NOT NULL,
    command_fingerprint text NOT NULL,
    family_mutation_fingerprint text NOT NULL,
    receipt jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, run_id, idempotency_issuer, command_id),
    FOREIGN KEY (run_id, request_scope)
        REFERENCES belllabs_control.workflow_runs(run_id, request_scope),
    FOREIGN KEY (run_id, idempotency_issuer, command_id)
        REFERENCES belllabs_control.lifecycle_command_results(
            run_id, idempotency_issuer, command_id
        )
);

CREATE INDEX family_admission_journal_order_idx
    ON belllabs_control.family_admission_journal
       (request_scope, run_id, family_kind, family_version);

ALTER TABLE belllabs_control.family_admission_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.family_admission_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.family_admission_results ENABLE ROW LEVEL SECURITY;

CREATE POLICY family_admission_heads_scope
    ON belllabs_control.family_admission_heads
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY family_admission_journal_scope
    ON belllabs_control.family_admission_journal
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY family_admission_results_scope
    ON belllabs_control.family_admission_results
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.family_admission_heads FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.family_admission_journal FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.family_admission_results FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = 'belllabs_family_repository_writer'
    ) THEN
        CREATE ROLE belllabs_family_repository_writer
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
END
$$;

REVOKE belllabs_family_repository_writer FROM belllabs_control_runtime;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
        REVOKE belllabs_family_repository_writer FROM belllabs_app;
    END IF;
END
$$;

REVOKE ALL
    ON belllabs_control.family_admission_heads,
       belllabs_control.family_admission_journal,
       belllabs_control.family_admission_results
    FROM PUBLIC, belllabs_control_runtime;
GRANT USAGE ON SCHEMA belllabs_control
    TO belllabs_family_repository_writer;
GRANT SELECT
    ON belllabs_control.family_admission_heads,
       belllabs_control.family_admission_journal,
       belllabs_control.family_admission_results
    TO belllabs_control_runtime;
GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.family_admission_heads
    TO belllabs_family_repository_writer;
GRANT SELECT, INSERT
    ON belllabs_control.family_admission_journal,
       belllabs_control.family_admission_results
    TO belllabs_family_repository_writer;
GRANT SELECT, INSERT
    ON belllabs_control.lifecycle_command_results,
       belllabs_control.lifecycle_transitions,
       belllabs_control.budget_ledger,
       belllabs_control.effect_ledger_entries,
       belllabs_control.outbox
    TO belllabs_family_repository_writer;
GRANT SELECT, UPDATE
    ON belllabs_control.workflow_runs,
       belllabs_control.budget_accounts,
       belllabs_control.effect_ledgers
    TO belllabs_family_repository_writer;
GRANT USAGE, SELECT
    ON SEQUENCE belllabs_control.outbox_position_seq
    TO belllabs_family_repository_writer;

-- Deployment provisioning is intentionally external to migrations:
--   CREATE ROLE <dedicated-login> LOGIN ...;
--   GRANT belllabs_family_repository_writer TO <dedicated-login>;
-- Never grant belllabs_family_repository_writer to belllabs_app or belllabs_control_runtime.
