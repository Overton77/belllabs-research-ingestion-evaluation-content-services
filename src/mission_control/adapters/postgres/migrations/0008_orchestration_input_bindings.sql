CREATE TABLE IF NOT EXISTS belllabs_control.workflow_semantic_input_bindings (
    binding_id text PRIMARY KEY,
    request_scope text NOT NULL,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    blueprint_family text NOT NULL CHECK (
        blueprint_family IN ('StageGraph', 'GoalDirected')
    ),
    effective_configuration_digest text NOT NULL,
    blueprint_digest text NOT NULL,
    binding_digest text NOT NULL,
    binding_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (request_scope, run_id),
    CHECK (binding_id ~ '^semantic-binding:[0-9a-f]{64}$'),
    CHECK (effective_configuration_digest ~ '^sha256:[0-9a-f]{64}$'),
    CHECK (blueprint_digest ~ '^sha256:[0-9a-f]{64}$'),
    CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS workflow_semantic_input_bindings_configuration_idx
    ON belllabs_control.workflow_semantic_input_bindings (
        request_scope,
        effective_configuration_digest,
        blueprint_digest
    );

ALTER TABLE belllabs_control.workflow_semantic_input_bindings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS workflow_semantic_input_binding_scope_isolation
    ON belllabs_control.workflow_semantic_input_bindings;
CREATE POLICY workflow_semantic_input_binding_scope_isolation
    ON belllabs_control.workflow_semantic_input_bindings
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.workflow_semantic_input_bindings FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT
    ON belllabs_control.workflow_semantic_input_bindings
    TO belllabs_control_runtime;
