-- mission-control-db-contract common release: runtime kernel support records.
-- An Agent Server runtime binding is admitted once as an immutable canonical
-- execution_binding; its mutable submission status, runtime submission attempts and
-- intervention idempotency are support. Interventions are canonical command rows whose
-- receipts are delivery_report rows. Durable decision and approval requests are canonical
-- human_task rows (typed inline request packet) answered once by human_resolution.

CREATE TABLE mission_control.runtime_execution_binding (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_execution_binding_id uuid PRIMARY KEY,
    binding_key text NOT NULL CHECK (binding_key <> ''),
    execution_binding_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    submission_key text NOT NULL CHECK (submission_key <> ''),
    submission_idempotency_key text NOT NULL CHECK (submission_idempotency_key <> ''),
    submission_digest text NOT NULL CHECK (submission_digest ~ '^sha256:[0-9a-f]{64}$'),
    run_plan_digest text NOT NULL CHECK (run_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    graph_assembly_digest text NOT NULL CHECK (graph_assembly_digest ~ '^sha256:[0-9a-f]{64}$'),
    state_schema_digest text NOT NULL CHECK (state_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    runtime_provider text NOT NULL CHECK (runtime_provider IN ('legacy_temporal', 'langgraph_agent_server')),
    deployment_endpoint_key text,
    deployment_revision text,
    deployment_key text,
    assistant_key text,
    graph_key text,
    agent_server_thread_key text,
    status text NOT NULL CHECK (status IN ('submitting', 'accepted', 'running', 'waiting', 'paused', 'cancelling', 'completed', 'failed', 'cancelled', 'reconciliation_required')),
    active boolean NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    binding_payload jsonb NOT NULL CHECK (jsonb_typeof(binding_payload) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, runtime_execution_binding_id),
    UNIQUE (installation_id, application_id, tenant_id, binding_key),
    UNIQUE (installation_id, application_id, tenant_id, run_key, execution_epoch),
    UNIQUE (installation_id, application_id, tenant_id, submission_key),
    UNIQUE (installation_id, application_id, tenant_id, submission_idempotency_key),
    UNIQUE (installation_id, application_id, tenant_id, execution_binding_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, execution_binding_id) REFERENCES mission_control.execution_binding (installation_id, application_id, tenant_id, execution_binding_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX runtime_execution_binding_provider_idx ON mission_control.runtime_execution_binding (installation_id, application_id, tenant_id, deployment_endpoint_key, agent_server_thread_key) WHERE deployment_endpoint_key IS NOT NULL;
CREATE INDEX runtime_execution_binding_reconcile_idx ON mission_control.runtime_execution_binding (installation_id, application_id, tenant_id, updated_at, binding_key) WHERE status = 'reconciliation_required';

CREATE TABLE mission_control.runtime_execution_attempt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_execution_attempt_id uuid PRIMARY KEY,
    binding_key text NOT NULL CHECK (binding_key <> ''),
    runtime_attempt bigint NOT NULL CHECK (runtime_attempt >= 1),
    submission_key text NOT NULL CHECK (submission_key <> ''),
    disposition text NOT NULL CHECK (disposition IN ('created', 'accepted', 'running', 'ambiguous', 'succeeded', 'failed', 'cancelled')),
    provider_request_digest text NOT NULL CHECK (provider_request_digest ~ '^sha256:[0-9a-f]{64}$'),
    agent_server_run_key text,
    provider_detail jsonb NOT NULL CHECK (jsonb_typeof(provider_detail) = 'object'),
    started_at timestamptz NOT NULL,
    heartbeat_at timestamptz,
    lease_expires_at timestamptz,
    finished_at timestamptz,
    failure_code text,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, runtime_execution_attempt_id),
    UNIQUE (installation_id, application_id, tenant_id, binding_key, runtime_attempt),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, binding_key) REFERENCES mission_control.runtime_execution_binding (installation_id, application_id, tenant_id, binding_key)
);
CREATE INDEX runtime_execution_attempt_provider_run_idx ON mission_control.runtime_execution_attempt (installation_id, application_id, tenant_id, agent_server_run_key) WHERE agent_server_run_key IS NOT NULL;
CREATE TRIGGER runtime_execution_attempt_immutable
    BEFORE UPDATE OR DELETE ON mission_control.runtime_execution_attempt
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.runtime_intervention (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_intervention_id uuid PRIMARY KEY,
    command_key text NOT NULL CHECK (command_key <> ''),
    command_id uuid NOT NULL,
    binding_key text NOT NULL CHECK (binding_key <> ''),
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    intervention_kind text NOT NULL CHECK (intervention_kind <> ''),
    expected_run_version bigint NOT NULL CHECK (expected_run_version >= 1),
    expected_checkpoint_key text,
    status text NOT NULL CHECK (status IN ('pending', 'accepted', 'stale', 'rejected', 'reconciliation_required')),
    requested_at timestamptz NOT NULL,
    recorded_at timestamptz,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, runtime_intervention_id),
    UNIQUE (installation_id, application_id, tenant_id, command_key),
    UNIQUE (installation_id, application_id, tenant_id, idempotency_key),
    UNIQUE (installation_id, application_id, tenant_id, command_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, command_id) REFERENCES mission_control.command (installation_id, application_id, tenant_id, command_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, binding_key) REFERENCES mission_control.runtime_execution_binding (installation_id, application_id, tenant_id, binding_key)
);
CREATE INDEX runtime_intervention_pending_idx ON mission_control.runtime_intervention (installation_id, application_id, tenant_id, requested_at, command_key) WHERE status IN ('pending', 'reconciliation_required');

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'runtime_execution_binding',
        'runtime_execution_attempt',
        'runtime_intervention'
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

REVOKE ALL ON mission_control.runtime_execution_binding, mission_control.runtime_execution_attempt,
    mission_control.runtime_intervention FROM PUBLIC;

-- Port of the legacy control runtime role (the legacy agent runtime role's SELECT-only grants fold into the
-- runtime capability; no separate capability is required for these reads).
GRANT SELECT, INSERT, UPDATE ON mission_control.runtime_execution_binding,
    mission_control.runtime_intervention, mission_control.human_task
TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.runtime_execution_attempt,
    mission_control.human_resolution
TO mission_control_runtime;
GRANT SELECT ON mission_control.runtime_execution_binding, mission_control.runtime_execution_attempt,
    mission_control.runtime_intervention, mission_control.human_task,
    mission_control.human_resolution
TO mission_control_readonly;
