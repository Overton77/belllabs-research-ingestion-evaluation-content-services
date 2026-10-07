-- mission-control-db-contract common release: coordinator support records.
-- Launch tickets, digest-only audit events and typed terminal workflow results are
-- support records with distinct semantics: a result never fabricates acceptance and a
-- ticket never replaces the canonical admission (request_receipt, mission, mission_run).

CREATE TABLE mission_control.coordinator_launch_ticket (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    coordinator_launch_ticket_id uuid PRIMARY KEY,
    ticket_key text NOT NULL CHECK (ticket_key <> ''),
    tenant_scope text NOT NULL CHECK (tenant_scope <> ''),
    caller_key text NOT NULL CHECK (caller_key <> ''),
    state text NOT NULL CHECK (state IN ('prepared', 'consumed', 'expired', 'invalidated')),
    prepared_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    proposal_digest text NOT NULL CHECK (proposal_digest ~ '^sha256:[0-9a-f]{64}$'),
    blueprint_family text NOT NULL CHECK (blueprint_family IN ('StageGraph', 'GoalDirected')),
    initial_goal_digest text,
    effective_configuration_digest text NOT NULL,
    run_request_digest text NOT NULL,
    idempotency_issuer text NOT NULL CHECK (idempotency_issuer <> ''),
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    consumed_run_key text,
    consumed_at timestamptz,
    invalidation_reason text,
    ticket_contract text NOT NULL CHECK (ticket_contract = 'mc.coordinator-launch-ticket/1'),
    ticket_payload jsonb NOT NULL CHECK (jsonb_typeof(ticket_payload) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (expires_at > prepared_at),
    CHECK ((blueprint_family = 'StageGraph' AND initial_goal_digest IS NULL)
        OR (blueprint_family = 'GoalDirected' AND initial_goal_digest IS NOT NULL)),
    CHECK ((state = 'consumed' AND consumed_run_key IS NOT NULL AND consumed_at IS NOT NULL)
        OR (state <> 'consumed' AND consumed_run_key IS NULL AND consumed_at IS NULL)),
    CHECK (state <> 'invalidated' OR invalidation_reason IS NOT NULL),
    UNIQUE (installation_id, application_id, tenant_id, coordinator_launch_ticket_id),
    UNIQUE (installation_id, application_id, tenant_id, ticket_key),
    UNIQUE (installation_id, application_id, tenant_id, tenant_scope, caller_key, idempotency_issuer, idempotency_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, consumed_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX coordinator_launch_ticket_run_fk_idx ON mission_control.coordinator_launch_ticket (installation_id, application_id, tenant_id, consumed_run_key);
CREATE INDEX coordinator_launch_ticket_expiry_idx ON mission_control.coordinator_launch_ticket (installation_id, application_id, tenant_id, state, expires_at);

CREATE TABLE mission_control.coordinator_audit_event (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    coordinator_audit_event_id uuid PRIMARY KEY,
    event_key text NOT NULL CHECK (event_key <> ''),
    tenant_scope text NOT NULL CHECK (tenant_scope <> ''),
    occurred_at timestamptz NOT NULL,
    operation text NOT NULL CHECK (operation <> ''),
    actor_key text NOT NULL CHECK (actor_key <> ''),
    outcome text NOT NULL CHECK (outcome IN ('succeeded', 'failed')),
    correlation_key text NOT NULL CHECK (correlation_key <> ''),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    response_digest text CHECK (response_digest IS NULL OR response_digest ~ '^sha256:[0-9a-f]{64}$'),
    error_code text,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((outcome = 'succeeded' AND response_digest IS NOT NULL AND error_code IS NULL)
        OR (outcome = 'failed' AND response_digest IS NULL AND error_code IS NOT NULL)),
    UNIQUE (installation_id, application_id, tenant_id, coordinator_audit_event_id),
    UNIQUE (installation_id, application_id, tenant_id, event_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX coordinator_audit_event_correlation_idx ON mission_control.coordinator_audit_event (installation_id, application_id, tenant_id, correlation_key);
CREATE INDEX coordinator_audit_event_time_idx ON mission_control.coordinator_audit_event (installation_id, application_id, tenant_id, tenant_scope, occurred_at DESC, event_key);
CREATE TRIGGER coordinator_audit_event_immutable
    BEFORE UPDATE OR DELETE ON mission_control.coordinator_audit_event
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.coordinator_workflow_result (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    coordinator_workflow_result_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    tenant_scope text NOT NULL CHECK (tenant_scope <> ''),
    blueprint_family text NOT NULL CHECK (blueprint_family IN ('StageGraph', 'GoalDirected')),
    terminal_outcome text NOT NULL CHECK (terminal_outcome IN ('completed', 'partially_completed', 'failed', 'cancelled')),
    completed_at timestamptz NOT NULL,
    result_digest text NOT NULL CHECK (result_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_contract text NOT NULL CHECK (result_contract = 'mc.coordinator-workflow-result/1'),
    result_payload jsonb NOT NULL CHECK (jsonb_typeof(result_payload) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, coordinator_workflow_result_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key),
    UNIQUE (installation_id, application_id, tenant_id, result_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX coordinator_workflow_result_time_idx ON mission_control.coordinator_workflow_result (installation_id, application_id, tenant_id, completed_at DESC, run_key);
CREATE TRIGGER coordinator_workflow_result_immutable
    BEFORE UPDATE OR DELETE ON mission_control.coordinator_workflow_result
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'coordinator_launch_ticket',
        'coordinator_audit_event',
        'coordinator_workflow_result'
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

REVOKE ALL ON mission_control.coordinator_launch_ticket, mission_control.coordinator_audit_event,
    mission_control.coordinator_workflow_result FROM PUBLIC;

-- Port of the legacy control runtime role: tickets change state by compare-and-swap; audit events
-- and results are insert-only.
GRANT SELECT, INSERT, UPDATE ON mission_control.coordinator_launch_ticket
TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.coordinator_audit_event,
    mission_control.coordinator_workflow_result
TO mission_control_runtime;
-- The legacy read-only operations role had no coordinator grants; none are added.
