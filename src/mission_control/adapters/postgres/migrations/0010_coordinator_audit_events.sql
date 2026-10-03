CREATE TABLE IF NOT EXISTS belllabs_control.coordinator_audit_events (
    event_id uuid PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    operation text NOT NULL,
    actor_id text NOT NULL,
    tenant_scope text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('succeeded', 'failed')),
    correlation_id text NOT NULL,
    request_digest text NOT NULL CHECK (
        request_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    response_digest text CHECK (
        response_digest IS NULL
        OR response_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    error_code text,
    CHECK (
        (
            outcome = 'succeeded'
            AND response_digest IS NOT NULL
            AND error_code IS NULL
        )
        OR
        (
            outcome = 'failed'
            AND response_digest IS NULL
            AND error_code IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS coordinator_audit_events_scope_time_idx
    ON belllabs_control.coordinator_audit_events (
        tenant_scope,
        occurred_at DESC,
        event_id
    );

CREATE INDEX IF NOT EXISTS coordinator_audit_events_correlation_idx
    ON belllabs_control.coordinator_audit_events (
        tenant_scope,
        correlation_id
    );

ALTER TABLE belllabs_control.coordinator_audit_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS coordinator_audit_event_scope_isolation
    ON belllabs_control.coordinator_audit_events;
CREATE POLICY coordinator_audit_event_scope_isolation
    ON belllabs_control.coordinator_audit_events
    USING (tenant_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (tenant_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.coordinator_audit_events FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT
    ON belllabs_control.coordinator_audit_events
    TO belllabs_control_runtime;
