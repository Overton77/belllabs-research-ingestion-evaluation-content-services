CREATE SCHEMA IF NOT EXISTS belllabs_control;

CREATE TABLE IF NOT EXISTS belllabs_control.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS belllabs_control.run_request_decisions (
    request_scope text NOT NULL,
    idempotency_issuer text NOT NULL,
    request_id text NOT NULL,
    request_fingerprint text NOT NULL,
    decision jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, idempotency_issuer, request_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.workflow_runs (
    run_id text PRIMARY KEY,
    request_scope text NOT NULL,
    idempotency_issuer text NOT NULL,
    request_id text NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    phase text NOT NULL CHECK (
        phase IN ('pending', 'active', 'waiting', 'paused', 'cancelling', 'terminal')
    ),
    projection jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (request_scope, idempotency_issuer, request_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.lifecycle_command_results (
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    idempotency_issuer text NOT NULL,
    command_id text NOT NULL,
    command_fingerprint text NOT NULL,
    result jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, idempotency_issuer, command_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.lifecycle_transitions (
    transition_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    command_id text NOT NULL,
    prior_version bigint NOT NULL CHECK (prior_version >= 0),
    resulting_version bigint NOT NULL CHECK (resulting_version >= 1),
    transition jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (run_id, resulting_version)
);

CREATE TABLE IF NOT EXISTS belllabs_control.budget_accounts (
    account_id text PRIMARY KEY,
    run_id text NOT NULL UNIQUE REFERENCES belllabs_control.workflow_runs(run_id),
    parent_account_id text NULL REFERENCES belllabs_control.budget_accounts(account_id),
    state jsonb NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS belllabs_control.budget_ledger (
    entry_id text PRIMARY KEY,
    account_id text NOT NULL REFERENCES belllabs_control.budget_accounts(account_id),
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    idempotency_id text NOT NULL,
    kind text NOT NULL,
    entry jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (account_id, kind, idempotency_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.outbox (
    event_id text PRIMARY KEY,
    position bigint NOT NULL UNIQUE CHECK (position >= 1),
    aggregate_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    aggregate_version bigint NOT NULL CHECK (aggregate_version >= 1),
    sequence integer NOT NULL CHECK (sequence >= 1),
    event_type text NOT NULL,
    envelope jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    delivery_attempts integer NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    delivered_at timestamptz NULL,
    UNIQUE (aggregate_id, aggregate_version, sequence)
);

CREATE INDEX IF NOT EXISTS outbox_pending_order_idx
    ON belllabs_control.outbox (position)
    WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS belllabs_control.consumer_cursors (
    consumer_id text NOT NULL,
    aggregate_id text NOT NULL,
    cursor jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_id, aggregate_id)
);

ALTER TABLE belllabs_control.run_request_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.workflow_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.lifecycle_command_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.lifecycle_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.budget_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.budget_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.consumer_cursors ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS request_scope_isolation ON belllabs_control.run_request_decisions;
CREATE POLICY request_scope_isolation
    ON belllabs_control.run_request_decisions
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

DROP POLICY IF EXISTS run_scope_isolation ON belllabs_control.workflow_runs;
CREATE POLICY run_scope_isolation
    ON belllabs_control.workflow_runs
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

DROP POLICY IF EXISTS command_scope_isolation
    ON belllabs_control.lifecycle_command_results;
CREATE POLICY command_scope_isolation
    ON belllabs_control.lifecycle_command_results
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = lifecycle_command_results.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = lifecycle_command_results.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS transition_scope_isolation
    ON belllabs_control.lifecycle_transitions;
CREATE POLICY transition_scope_isolation
    ON belllabs_control.lifecycle_transitions
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = lifecycle_transitions.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = lifecycle_transitions.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS budget_account_scope_isolation
    ON belllabs_control.budget_accounts;
CREATE POLICY budget_account_scope_isolation
    ON belllabs_control.budget_accounts
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = budget_accounts.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = budget_accounts.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS budget_ledger_scope_isolation
    ON belllabs_control.budget_ledger;
CREATE POLICY budget_ledger_scope_isolation
    ON belllabs_control.budget_ledger
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = budget_ledger.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = budget_ledger.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS outbox_scope_isolation ON belllabs_control.outbox;
CREATE POLICY outbox_scope_isolation
    ON belllabs_control.outbox
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = outbox.aggregate_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = outbox.aggregate_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS consumer_cursor_scope_isolation
    ON belllabs_control.consumer_cursors;
CREATE POLICY consumer_cursor_scope_isolation
    ON belllabs_control.consumer_cursors
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = consumer_cursors.aggregate_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = consumer_cursors.aggregate_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

ALTER TABLE belllabs_control.run_request_decisions FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.workflow_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.lifecycle_command_results FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.lifecycle_transitions FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.budget_accounts FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.budget_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.outbox FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.consumer_cursors FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_control_runtime') THEN
        CREATE ROLE belllabs_control_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOBYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA belllabs_control TO belllabs_control_runtime;
GRANT SELECT, INSERT
    ON belllabs_control.run_request_decisions,
       belllabs_control.lifecycle_command_results,
       belllabs_control.lifecycle_transitions,
       belllabs_control.budget_ledger,
       belllabs_control.outbox
    TO belllabs_control_runtime;
GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.workflow_runs,
       belllabs_control.budget_accounts,
       belllabs_control.consumer_cursors
    TO belllabs_control_runtime;
GRANT UPDATE (delivery_attempts, delivered_at)
    ON belllabs_control.outbox TO belllabs_control_runtime;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
        GRANT belllabs_control_runtime TO belllabs_app;
    END IF;
END
$$;
