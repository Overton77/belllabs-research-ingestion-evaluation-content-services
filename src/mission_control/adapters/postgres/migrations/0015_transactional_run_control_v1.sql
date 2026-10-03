-- CON-CP-BUDGET-LEDGER-V1 consequential-effect authority.
-- Application PostgreSQL is the sole writer; operation/provider records are observations only.

CREATE TABLE IF NOT EXISTS belllabs_control.effect_ledgers (
    run_id text PRIMARY KEY REFERENCES belllabs_control.workflow_runs(run_id),
    state jsonb NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS belllabs_control.effect_ledger_entries (
    entry_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    effect_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('claim', 'observation', 'settlement')),
    idempotency_id text NOT NULL,
    entry jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (run_id, effect_id, kind, idempotency_id)
);

CREATE INDEX IF NOT EXISTS effect_ledger_run_order_idx
    ON belllabs_control.effect_ledger_entries (run_id, occurred_at, entry_id);

ALTER TABLE belllabs_control.effect_ledgers ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.effect_ledger_entries ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS effect_ledgers_scope_isolation
    ON belllabs_control.effect_ledgers;
CREATE POLICY effect_ledgers_scope_isolation
    ON belllabs_control.effect_ledgers
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = effect_ledgers.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = effect_ledgers.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

DROP POLICY IF EXISTS effect_ledger_entries_scope_isolation
    ON belllabs_control.effect_ledger_entries;
CREATE POLICY effect_ledger_entries_scope_isolation
    ON belllabs_control.effect_ledger_entries
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = effect_ledger_entries.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.workflow_runs run
        WHERE run.run_id = effect_ledger_entries.run_id
          AND run.request_scope = current_setting('belllabs.request_scope', true)
    ));

ALTER TABLE belllabs_control.effect_ledgers FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.effect_ledger_entries FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.effect_ledgers
    TO belllabs_control_runtime;
GRANT SELECT, INSERT
    ON belllabs_control.effect_ledger_entries
    TO belllabs_control_runtime;
