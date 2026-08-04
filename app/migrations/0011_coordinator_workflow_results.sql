CREATE TABLE IF NOT EXISTS belllabs_control.coordinator_workflow_results (
    run_id text PRIMARY KEY
        REFERENCES belllabs_control.workflow_runs(run_id),
    tenant_scope text NOT NULL,
    request_scope text NOT NULL,
    blueprint_family text NOT NULL CHECK (
        blueprint_family IN ('StageGraph', 'GoalDirected')
    ),
    terminal_outcome text NOT NULL CHECK (
        terminal_outcome IN (
            'completed',
            'partially_completed',
            'failed',
            'cancelled'
        )
    ),
    completed_at timestamptz NOT NULL,
    result_digest text NOT NULL UNIQUE CHECK (
        result_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    result_payload jsonb NOT NULL,
    UNIQUE (tenant_scope, request_scope, run_id)
);

CREATE INDEX IF NOT EXISTS coordinator_workflow_results_scope_time_idx
    ON belllabs_control.coordinator_workflow_results (
        tenant_scope,
        request_scope,
        completed_at DESC,
        run_id
    );

ALTER TABLE belllabs_control.coordinator_workflow_results
    ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS coordinator_workflow_result_scope_isolation
    ON belllabs_control.coordinator_workflow_results;
CREATE POLICY coordinator_workflow_result_scope_isolation
    ON belllabs_control.coordinator_workflow_results
    USING (
        request_scope = current_setting('belllabs.request_scope', true)
        AND tenant_scope = current_setting('belllabs.tenant_scope', true)
    )
    WITH CHECK (
        request_scope = current_setting('belllabs.request_scope', true)
        AND tenant_scope = current_setting('belllabs.tenant_scope', true)
    );

ALTER TABLE belllabs_control.coordinator_workflow_results
    FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT
    ON belllabs_control.coordinator_workflow_results
    TO belllabs_control_runtime;
