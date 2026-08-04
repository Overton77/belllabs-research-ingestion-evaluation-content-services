CREATE TABLE IF NOT EXISTS belllabs_control.coordinator_launch_tickets (
    ticket_id uuid PRIMARY KEY,
    tenant_scope text NOT NULL,
    caller_id text NOT NULL,
    request_scope text NOT NULL,
    state text NOT NULL CHECK (
        state IN ('prepared', 'consumed', 'expired', 'invalidated')
    ),
    prepared_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    proposal_digest text NOT NULL,
    workflow_type_ref jsonb NOT NULL,
    blueprint_ref jsonb NOT NULL,
    blueprint_family text NOT NULL CHECK (
        blueprint_family IN ('StageGraph', 'GoalDirected')
    ),
    initial_goal text,
    initial_goal_digest text,
    effective_configuration_digest text NOT NULL,
    run_request_digest text NOT NULL,
    resolved_asset_refs jsonb NOT NULL,
    authority_decisions jsonb NOT NULL,
    availability_decisions jsonb NOT NULL,
    approval_refs jsonb NOT NULL,
    policy_snapshot_digest text NOT NULL,
    environment_snapshot_digest text NOT NULL,
    warnings jsonb NOT NULL DEFAULT '[]'::jsonb,
    launchable boolean NOT NULL,
    idempotency_issuer text NOT NULL,
    idempotency_key text NOT NULL,
    frozen_run_request jsonb NOT NULL,
    consumed_run_id text REFERENCES belllabs_control.workflow_runs(run_id),
    consumed_at timestamptz,
    invalidation_reason text,
    ticket_payload jsonb NOT NULL,
    UNIQUE (
        tenant_scope,
        caller_id,
        idempotency_issuer,
        idempotency_key
    ),
    CHECK (expires_at > prepared_at),
    CHECK (
        (
            blueprint_family = 'StageGraph'
            AND initial_goal IS NULL
            AND initial_goal_digest IS NULL
        )
        OR
        (
            blueprint_family = 'GoalDirected'
            AND initial_goal IS NOT NULL
            AND length(trim(initial_goal)) > 0
            AND initial_goal_digest IS NOT NULL
        )
    ),
    CHECK (
        (
            state = 'consumed'
            AND consumed_run_id IS NOT NULL
            AND consumed_at IS NOT NULL
        )
        OR
        (
            state <> 'consumed'
            AND consumed_run_id IS NULL
            AND consumed_at IS NULL
        )
    ),
    CHECK (
        state <> 'invalidated'
        OR invalidation_reason IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS coordinator_launch_tickets_expiry_idx
    ON belllabs_control.coordinator_launch_tickets (state, expires_at);

CREATE INDEX IF NOT EXISTS coordinator_launch_tickets_scope_run_idx
    ON belllabs_control.coordinator_launch_tickets (request_scope, consumed_run_id);

ALTER TABLE belllabs_control.coordinator_launch_tickets ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS coordinator_launch_ticket_scope_isolation
    ON belllabs_control.coordinator_launch_tickets;
CREATE POLICY coordinator_launch_ticket_scope_isolation
    ON belllabs_control.coordinator_launch_tickets
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.coordinator_launch_tickets FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.coordinator_launch_tickets
    TO belllabs_control_runtime;
