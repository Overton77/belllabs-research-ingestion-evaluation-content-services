ALTER TABLE belllabs_control.workflow_runs
    ADD CONSTRAINT workflow_runs_scope_run_unique UNIQUE (request_scope, run_id);

CREATE TABLE belllabs_control.runtime_execution_bindings (
    binding_id text PRIMARY KEY,
    request_scope text NOT NULL,
    belllabs_run_id text NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    submission_id text NOT NULL,
    submission_idempotency_key text NOT NULL,
    submission_digest text NOT NULL CHECK (submission_digest ~ '^sha256:[0-9a-f]{64}$'),
    run_plan_digest text NOT NULL CHECK (run_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    graph_assembly_digest text NOT NULL CHECK (graph_assembly_digest ~ '^sha256:[0-9a-f]{64}$'),
    state_schema_digest text NOT NULL CHECK (state_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    runtime_provider text NOT NULL CHECK (
        runtime_provider IN ('legacy_temporal', 'langgraph_agent_server')
    ),
    deployment_endpoint_id text,
    deployment_revision text,
    agent_server_thread_id text,
    status text NOT NULL CHECK (
        status IN (
            'submitting', 'accepted', 'running', 'waiting', 'paused', 'cancelling',
            'completed', 'failed', 'cancelled', 'reconciliation_required'
        )
    ),
    active boolean NOT NULL DEFAULT true,
    version bigint NOT NULL CHECK (version >= 1),
    binding_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (request_scope, belllabs_run_id, execution_epoch),
    UNIQUE (request_scope, submission_id),
    UNIQUE (request_scope, submission_idempotency_key),
    UNIQUE (request_scope, binding_id),
    FOREIGN KEY (request_scope, belllabs_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id)
);

CREATE UNIQUE INDEX runtime_execution_bindings_active_epoch_idx
    ON belllabs_control.runtime_execution_bindings (
        request_scope, belllabs_run_id, execution_epoch
    ) WHERE active;
CREATE INDEX runtime_execution_bindings_reconcile_idx
    ON belllabs_control.runtime_execution_bindings (request_scope, updated_at, binding_id)
    WHERE status = 'reconciliation_required';
CREATE INDEX runtime_execution_bindings_provider_idx
    ON belllabs_control.runtime_execution_bindings (
        request_scope, deployment_endpoint_id, agent_server_thread_id
    ) WHERE deployment_endpoint_id IS NOT NULL;

CREATE TABLE belllabs_control.runtime_execution_attempts (
    attempt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    runtime_attempt bigint NOT NULL CHECK (runtime_attempt >= 1),
    submission_id text NOT NULL,
    disposition text NOT NULL CHECK (
        disposition IN ('created', 'accepted', 'running', 'ambiguous', 'succeeded',
                        'failed', 'cancelled')
    ),
    provider_request_digest text NOT NULL CHECK (
        provider_request_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    agent_server_run_id text,
    provider_detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL,
    heartbeat_at timestamptz,
    lease_expires_at timestamptz,
    finished_at timestamptz,
    failure_code text,
    UNIQUE (request_scope, binding_id, runtime_attempt),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id)
);

CREATE INDEX runtime_execution_attempts_lease_idx
    ON belllabs_control.runtime_execution_attempts (
        request_scope, lease_expires_at, attempt_id
    ) WHERE finished_at IS NULL;
CREATE INDEX runtime_execution_attempts_provider_run_idx
    ON belllabs_control.runtime_execution_attempts (
        request_scope, agent_server_run_id
    ) WHERE agent_server_run_id IS NOT NULL;

CREATE TABLE belllabs_control.runtime_checkpoint_observations (
    observation_id text PRIMARY KEY,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    deployment_endpoint_id text NOT NULL,
    agent_server_thread_id text NOT NULL,
    langgraph_checkpoint_id text NOT NULL,
    state_schema_digest text NOT NULL CHECK (state_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    graph_assembly_digest text NOT NULL CHECK (
        graph_assembly_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    status text NOT NULL,
    summary_digest text NOT NULL CHECK (summary_digest ~ '^sha256:[0-9a-f]{64}$'),
    redacted_summary jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    UNIQUE (
        request_scope, deployment_endpoint_id, agent_server_thread_id,
        langgraph_checkpoint_id
    ),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id)
);

CREATE TABLE belllabs_control.runtime_intervention_commands (
    command_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    intervention_kind text NOT NULL,
    expected_belllabs_version bigint NOT NULL CHECK (expected_belllabs_version >= 1),
    expected_checkpoint_id text,
    command_payload jsonb NOT NULL,
    receipt_payload jsonb,
    status text NOT NULL CHECK (
        status IN ('pending', 'accepted', 'stale', 'rejected', 'reconciliation_required')
    ),
    requested_at timestamptz NOT NULL,
    recorded_at timestamptz,
    PRIMARY KEY (request_scope, command_id),
    UNIQUE (request_scope, idempotency_key),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id)
);

CREATE INDEX runtime_intervention_pending_idx
    ON belllabs_control.runtime_intervention_commands (
        request_scope, requested_at, command_id
    ) WHERE status IN ('pending', 'reconciliation_required');

CREATE TABLE belllabs_control.runtime_interrupt_requests (
    interrupt_request_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    deployment_endpoint_id text NOT NULL,
    agent_server_thread_id text NOT NULL,
    langgraph_checkpoint_id text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    request_payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'answered', 'expired', 'cancelled')),
    requested_at timestamptz NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (request_scope, interrupt_request_id),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id)
);

CREATE TABLE belllabs_control.runtime_interrupt_decisions (
    response_id text NOT NULL,
    request_scope text NOT NULL,
    interrupt_request_id text NOT NULL,
    response_digest text NOT NULL CHECK (response_digest ~ '^sha256:[0-9a-f]{64}$'),
    response_payload_ref text NOT NULL,
    decision_payload jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, response_id),
    UNIQUE (request_scope, interrupt_request_id),
    FOREIGN KEY (request_scope, interrupt_request_id)
        REFERENCES belllabs_control.runtime_interrupt_requests(request_scope, interrupt_request_id)
);

CREATE TABLE belllabs_control.runtime_async_tasks (
    async_task_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    deployment_endpoint_id text NOT NULL,
    child_thread_id text NOT NULL,
    child_run_id text,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (
        status IN (
            'submitted', 'running', 'waiting', 'completed', 'failed',
            'cancel_requested', 'cancelled', 'orphaned', 'reconciliation_required'
        )
    ),
    result_manifest_ref text,
    version bigint NOT NULL CHECK (version >= 1),
    heartbeat_at timestamptz,
    lease_expires_at timestamptz,
    task_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, async_task_id),
    UNIQUE (request_scope, deployment_endpoint_id, child_thread_id),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id)
);

CREATE INDEX runtime_async_tasks_pending_idx
    ON belllabs_control.runtime_async_tasks (
        request_scope, status, updated_at, async_task_id
    ) WHERE status NOT IN ('completed', 'failed', 'cancelled');
CREATE INDEX runtime_async_tasks_lease_idx
    ON belllabs_control.runtime_async_tasks (
        request_scope, lease_expires_at, async_task_id
    ) WHERE status IN ('submitted', 'running', 'waiting', 'cancel_requested');

CREATE TABLE belllabs_control.operation_effect_claims (
    effect_claim_id text PRIMARY KEY,
    request_scope text NOT NULL,
    belllabs_run_id text NOT NULL,
    operation_contract_digest text NOT NULL CHECK (
        operation_contract_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    idempotency_key text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    semantic_binding_id text NOT NULL,
    semantic_binding_digest text NOT NULL CHECK (
        semantic_binding_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    semantic_attempt_key text NOT NULL,
    claim_mode text NOT NULL CHECK (claim_mode = 'active'),
    status text NOT NULL CHECK (
        status IN ('claimed', 'executing', 'settled', 'reconciliation_required', 'cancelled')
    ),
    claimed_by text NOT NULL,
    claimed_at timestamptz NOT NULL,
    heartbeat_at timestamptz,
    lease_expires_at timestamptz,
    UNIQUE (request_scope, operation_contract_digest, idempotency_key),
    UNIQUE (request_scope, semantic_attempt_key),
    UNIQUE (request_scope, effect_claim_id),
    FOREIGN KEY (request_scope, belllabs_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id)
);

CREATE UNIQUE INDEX operation_effect_claims_active_consequential_idx
    ON belllabs_control.operation_effect_claims (
        request_scope, operation_contract_digest, idempotency_key
    ) WHERE claim_mode = 'active';
CREATE INDEX operation_effect_claims_reconcile_idx
    ON belllabs_control.operation_effect_claims (
        request_scope, status, lease_expires_at, effect_claim_id
    ) WHERE status = 'reconciliation_required';

CREATE TABLE belllabs_control.operation_journal_mutations (
    request_scope text NOT NULL,
    mutation_id text NOT NULL,
    effect_claim_id text NOT NULL,
    mutation_digest text NOT NULL CHECK (mutation_digest ~ '^sha256:[0-9a-f]{64}$'),
    mutation_payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, mutation_id),
    FOREIGN KEY (request_scope, effect_claim_id)
        REFERENCES belllabs_control.operation_effect_claims(request_scope, effect_claim_id)
);

CREATE TABLE belllabs_control.operation_execution_attempts (
    operation_attempt_id text NOT NULL,
    request_scope text NOT NULL,
    effect_claim_id text NOT NULL,
    technical_attempt bigint NOT NULL CHECK (technical_attempt >= 1),
    provider text NOT NULL,
    provider_attempt_id text,
    disposition text NOT NULL CHECK (
        disposition IN ('created', 'running', 'ambiguous', 'succeeded', 'failed', 'cancelled')
    ),
    idempotency_supported boolean NOT NULL,
    retry_class text NOT NULL CHECK (
        retry_class IN ('safe', 'claim_then_reconcile', 'non_retryable')
    ),
    usage_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    failure_code text,
    PRIMARY KEY (request_scope, operation_attempt_id),
    UNIQUE (request_scope, effect_claim_id, technical_attempt),
    FOREIGN KEY (request_scope, effect_claim_id)
        REFERENCES belllabs_control.operation_effect_claims(request_scope, effect_claim_id)
);

CREATE INDEX operation_execution_attempts_provider_idx
    ON belllabs_control.operation_execution_attempts (
        request_scope, provider, provider_attempt_id
    ) WHERE provider_attempt_id IS NOT NULL;

CREATE TABLE belllabs_control.operation_settlements (
    settlement_id text NOT NULL,
    request_scope text NOT NULL,
    effect_claim_id text NOT NULL,
    settlement_revision bigint NOT NULL CHECK (settlement_revision >= 1),
    settlement_digest text NOT NULL CHECK (settlement_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (
        status IN ('completed', 'failed', 'cancelled', 'timed_out', 'reconciliation_required')
    ),
    usage_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    pending_external_usage_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_manifest_ref text,
    result_manifest_digest text CHECK (
        result_manifest_digest IS NULL
        OR result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    result_manifest_size_bytes bigint CHECK (
        result_manifest_size_bytes IS NULL OR result_manifest_size_bytes >= 1
    ),
    failure_code text,
    settlement_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, settlement_id),
    UNIQUE (request_scope, effect_claim_id, settlement_revision),
    FOREIGN KEY (request_scope, effect_claim_id)
        REFERENCES belllabs_control.operation_effect_claims(request_scope, effect_claim_id)
);

CREATE INDEX operation_settlements_lineage_idx
    ON belllabs_control.operation_settlements (
        request_scope, effect_claim_id, settlement_revision
    );

CREATE UNIQUE INDEX operation_settlements_one_terminal_idx
    ON belllabs_control.operation_settlements (request_scope, effect_claim_id)
    WHERE status <> 'reconciliation_required';

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'runtime_execution_bindings',
        'runtime_execution_attempts',
        'runtime_checkpoint_observations',
        'runtime_intervention_commands',
        'runtime_interrupt_requests',
        'runtime_interrupt_decisions',
        'runtime_async_tasks',
        'operation_effect_claims',
        'operation_journal_mutations',
        'operation_execution_attempts',
        'operation_settlements'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name
        );
        EXECUTE format(
            'DROP POLICY IF EXISTS request_scope_isolation ON belllabs_control.%I', table_name
        );
        EXECUTE format(
            'CREATE POLICY request_scope_isolation ON belllabs_control.%I
             USING (request_scope = current_setting(''belllabs.request_scope'', true))
             WITH CHECK (request_scope = current_setting(''belllabs.request_scope'', true))',
            table_name
        );
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name
        );
    END LOOP;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_agent_runtime') THEN
        CREATE ROLE belllabs_agent_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_operations_readonly') THEN
        CREATE ROLE belllabs_operations_readonly NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOBYPASSRLS;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA belllabs_control
    TO belllabs_control_runtime, belllabs_agent_runtime, belllabs_operations_readonly;

GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.runtime_execution_bindings,
       belllabs_control.runtime_checkpoint_observations,
       belllabs_control.runtime_intervention_commands,
       belllabs_control.runtime_interrupt_requests,
       belllabs_control.runtime_interrupt_decisions,
       belllabs_control.runtime_async_tasks,
       belllabs_control.operation_effect_claims
    TO belllabs_control_runtime;

GRANT SELECT, INSERT
    ON belllabs_control.runtime_execution_attempts,
       belllabs_control.operation_journal_mutations,
       belllabs_control.operation_execution_attempts,
       belllabs_control.operation_settlements
    TO belllabs_control_runtime;

GRANT SELECT
    ON belllabs_control.runtime_execution_attempts,
       belllabs_control.runtime_checkpoint_observations,
       belllabs_control.runtime_async_tasks
    TO belllabs_agent_runtime;
GRANT SELECT
    ON belllabs_control.runtime_execution_bindings,
       belllabs_control.runtime_interrupt_requests
    TO belllabs_agent_runtime;

GRANT SELECT
    ON belllabs_control.runtime_execution_bindings,
       belllabs_control.runtime_execution_attempts,
       belllabs_control.runtime_checkpoint_observations,
       belllabs_control.runtime_intervention_commands,
       belllabs_control.runtime_interrupt_requests,
       belllabs_control.runtime_interrupt_decisions,
       belllabs_control.runtime_async_tasks,
       belllabs_control.operation_effect_claims,
       belllabs_control.operation_journal_mutations,
       belllabs_control.operation_execution_attempts,
       belllabs_control.operation_settlements
    TO belllabs_operations_readonly;

GRANT USAGE, SELECT ON SEQUENCE
    belllabs_control.runtime_execution_attempts_attempt_id_seq
    TO belllabs_control_runtime;
