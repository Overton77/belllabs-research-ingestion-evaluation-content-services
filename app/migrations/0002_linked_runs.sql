CREATE UNIQUE INDEX IF NOT EXISTS workflow_runs_scope_run_id_uq
    ON belllabs_control.workflow_runs (request_scope, run_id);

CREATE UNIQUE INDEX IF NOT EXISTS budget_accounts_account_run_uq
    ON belllabs_control.budget_accounts (account_id, run_id);

CREATE TABLE IF NOT EXISTS belllabs_control.run_composition_links (
    link_id text PRIMARY KEY,
    request_identity text NOT NULL UNIQUE,
    request_fingerprint text NOT NULL,
    request_scope text NOT NULL,
    parent_run_id text NOT NULL,
    child_run_id text NOT NULL UNIQUE,
    linked_budget_account_id text NOT NULL,
    link jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (link_id, parent_run_id, child_run_id),
    UNIQUE (link_id, child_run_id),
    FOREIGN KEY (request_scope, parent_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id),
    FOREIGN KEY (request_scope, child_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id),
    FOREIGN KEY (linked_budget_account_id, child_run_id)
        REFERENCES belllabs_control.budget_accounts(account_id, run_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.run_dependency_revisions (
    revision_id text PRIMARY KEY,
    link_id text NOT NULL REFERENCES belllabs_control.run_composition_links(link_id),
    revision integer NOT NULL CHECK (revision >= 2),
    decision jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    UNIQUE (link_id, revision)
);

CREATE TABLE IF NOT EXISTS belllabs_control.linked_run_result_decisions (
    decision_id text PRIMARY KEY,
    link_id text NOT NULL REFERENCES belllabs_control.run_composition_links(link_id),
    parent_run_id text NOT NULL,
    child_run_id text NOT NULL,
    exact_output_ref text NOT NULL,
    decision jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    UNIQUE (link_id, exact_output_ref),
    FOREIGN KEY (link_id, parent_run_id, child_run_id)
        REFERENCES belllabs_control.run_composition_links(
            link_id, parent_run_id, child_run_id
        )
);

CREATE TABLE IF NOT EXISTS belllabs_control.linked_child_terminal_records (
    terminal_record_id text PRIMARY KEY,
    link_id text NOT NULL UNIQUE,
    child_run_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('completed', 'failed', 'cancelled', 'timed_out')),
    record jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    FOREIGN KEY (link_id, child_run_id)
        REFERENCES belllabs_control.run_composition_links(link_id, child_run_id)
);

ALTER TABLE belllabs_control.run_composition_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.run_dependency_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.linked_run_result_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.linked_child_terminal_records ENABLE ROW LEVEL SECURITY;

CREATE POLICY composition_link_scope_isolation
    ON belllabs_control.run_composition_links
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

CREATE POLICY dependency_revision_scope_isolation
    ON belllabs_control.run_dependency_revisions
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = run_dependency_revisions.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = run_dependency_revisions.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ));

CREATE POLICY linked_result_scope_isolation
    ON belllabs_control.linked_run_result_decisions
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = linked_run_result_decisions.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = linked_run_result_decisions.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ));

CREATE POLICY linked_terminal_scope_isolation
    ON belllabs_control.linked_child_terminal_records
    USING (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = linked_child_terminal_records.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ))
    WITH CHECK (EXISTS (
        SELECT 1 FROM belllabs_control.run_composition_links link
        WHERE link.link_id = linked_child_terminal_records.link_id
          AND link.request_scope = current_setting('belllabs.request_scope', true)
    ));

ALTER TABLE belllabs_control.run_composition_links FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.run_dependency_revisions FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.linked_run_result_decisions FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.linked_child_terminal_records FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT ON belllabs_control.run_composition_links
    TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.run_dependency_revisions
    TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.linked_run_result_decisions
    TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.linked_child_terminal_records
    TO belllabs_control_runtime;
