ALTER TABLE belllabs_control.runtime_execution_bindings
    ADD COLUMN deployment_id text,
    ADD COLUMN assistant_id text,
    ADD COLUMN graph_id text;

ALTER TABLE belllabs_control.runtime_checkpoint_observations
    ADD COLUMN retain_until timestamptz;
UPDATE belllabs_control.runtime_checkpoint_observations
SET retain_until = observed_at + interval '90 days'
WHERE retain_until IS NULL;
ALTER TABLE belllabs_control.runtime_checkpoint_observations
    ALTER COLUMN retain_until SET NOT NULL,
    ALTER COLUMN retain_until SET DEFAULT (clock_timestamp() + interval '90 days');
CREATE INDEX runtime_checkpoint_retention_idx
    ON belllabs_control.runtime_checkpoint_observations (
        request_scope, retain_until, observation_id
    );

CREATE INDEX runtime_execution_bindings_exact_route_idx
    ON belllabs_control.runtime_execution_bindings (
        request_scope, deployment_endpoint_id, deployment_revision, graph_id, assistant_id
    )
    WHERE deployment_endpoint_id IS NOT NULL;

CREATE TABLE belllabs_control.runtime_lineage_records (
    lineage_id text NOT NULL,
    request_scope text NOT NULL,
    belllabs_run_id text NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    lineage_digest text NOT NULL CHECK (lineage_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_ref text,
    lineage_payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    PRIMARY KEY (request_scope, lineage_id),
    UNIQUE (request_scope, lineage_digest),
    FOREIGN KEY (request_scope, belllabs_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id),
    CHECK (retain_until > recorded_at)
);

CREATE INDEX runtime_lineage_result_idx
    ON belllabs_control.runtime_lineage_records (request_scope, result_manifest_ref, recorded_at)
    WHERE result_manifest_ref IS NOT NULL;
CREATE INDEX runtime_lineage_retention_idx
    ON belllabs_control.runtime_lineage_records (request_scope, retain_until, lineage_id);

CREATE TABLE belllabs_control.runtime_lineage_edges (
    request_scope text NOT NULL,
    lineage_id text NOT NULL,
    parent_identity_key text NOT NULL,
    child_identity_key text NOT NULL,
    relationship text NOT NULL CHECK (
        relationship IN (
            'contains', 'attempt_of', 'invokes', 'spawns', 'produces', 'traces', 'claims'
        )
    ),
    edge_digest text NOT NULL CHECK (edge_digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (
        request_scope, lineage_id, parent_identity_key, child_identity_key, relationship
    ),
    UNIQUE (request_scope, edge_digest),
    FOREIGN KEY (request_scope, lineage_id)
        REFERENCES belllabs_control.runtime_lineage_records(request_scope, lineage_id),
    CHECK (parent_identity_key <> child_identity_key)
);

CREATE TABLE belllabs_control.execution_resource_leases (
    lease_id text NOT NULL,
    request_scope text NOT NULL,
    semantic_identity text NOT NULL,
    envelope_digest text NOT NULL CHECK (envelope_digest ~ '^sha256:[0-9a-f]{64}$'),
    canonical_digest text NOT NULL CHECK (canonical_digest ~ '^sha256:[0-9a-f]{64}$'),
    resources jsonb NOT NULL,
    acquisition_order integer NOT NULL CHECK (acquisition_order >= 1),
    status text NOT NULL CHECK (
        status IN (
            'requested', 'acquired', 'retained', 'released', 'expired',
            'reconciliation_required'
        )
    ),
    retained_for_wait boolean NOT NULL DEFAULT false,
    owner_instance_id text NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    acquired_at timestamptz,
    renewed_at timestamptz,
    expires_at timestamptz,
    released_at timestamptz,
    lease_payload jsonb NOT NULL,
    PRIMARY KEY (request_scope, lease_id),
    UNIQUE (request_scope, semantic_identity),
    CHECK (jsonb_typeof(resources) = 'array'),
    CHECK (
        status NOT IN ('acquired', 'retained')
        OR (acquired_at IS NOT NULL AND expires_at IS NOT NULL)
    ),
    CHECK (status <> 'released' OR released_at IS NOT NULL)
);

CREATE INDEX execution_resource_leases_capacity_idx
    ON belllabs_control.execution_resource_leases (
        request_scope, status, expires_at, lease_id
    )
    WHERE status IN ('requested', 'acquired', 'retained');
CREATE INDEX execution_resource_leases_expiry_idx
    ON belllabs_control.execution_resource_leases (request_scope, expires_at, lease_id)
    WHERE status IN ('requested', 'acquired', 'retained');

CREATE TABLE belllabs_control.runtime_decision_requests (
    decision_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    decision_type text NOT NULL,
    request_schema_ref text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    expected_belllabs_version bigint NOT NULL CHECK (expected_belllabs_version >= 1),
    policy_ref text NOT NULL,
    request_payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'answered', 'expired', 'cancelled')),
    requested_at timestamptz NOT NULL,
    expires_at timestamptz,
    retain_until timestamptz NOT NULL,
    PRIMARY KEY (request_scope, decision_id),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id),
    CHECK (retain_until > requested_at)
);

CREATE INDEX runtime_decision_pending_idx
    ON belllabs_control.runtime_decision_requests (
        request_scope, status, expires_at, decision_id
    )
    WHERE status = 'pending';
CREATE INDEX runtime_decision_retention_idx
    ON belllabs_control.runtime_decision_requests (request_scope, retain_until, decision_id);

CREATE TABLE belllabs_control.runtime_decision_responses (
    response_id text NOT NULL,
    request_scope text NOT NULL,
    decision_id text NOT NULL,
    response_digest text NOT NULL CHECK (response_digest ~ '^sha256:[0-9a-f]{64}$'),
    actor_id text NOT NULL,
    actor_type text NOT NULL,
    response_payload jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, response_id),
    UNIQUE (request_scope, decision_id),
    FOREIGN KEY (request_scope, decision_id)
        REFERENCES belllabs_control.runtime_decision_requests(request_scope, decision_id)
);

CREATE TABLE belllabs_control.runtime_reconciliation_incidents (
    incident_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text,
    incident_type text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    status text NOT NULL CHECK (
        status IN ('open', 'retry_scheduled', 'operator_required', 'resolved')
    ),
    identity_digest text NOT NULL CHECK (identity_digest ~ '^sha256:[0-9a-f]{64}$'),
    before_version bigint,
    after_version bigint,
    actor_ref text NOT NULL,
    reason text NOT NULL,
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    retry_at timestamptz,
    incident_payload jsonb NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    recorded_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    PRIMARY KEY (request_scope, incident_id),
    UNIQUE (request_scope, incident_type, identity_digest),
    FOREIGN KEY (request_scope, binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id),
    CHECK (retain_until > recorded_at)
);

CREATE INDEX runtime_reconciliation_work_idx
    ON belllabs_control.runtime_reconciliation_incidents (
        request_scope, status, retry_at, incident_id
    )
    WHERE status <> 'resolved';
CREATE INDEX runtime_reconciliation_retention_idx
    ON belllabs_control.runtime_reconciliation_incidents (
        request_scope, retain_until, incident_id
    );

CREATE TABLE belllabs_control.runtime_fork_requests (
    request_scope text NOT NULL,
    request_id text NOT NULL,
    idempotency_key text NOT NULL,
    source_binding_id text NOT NULL,
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    request_payload jsonb NOT NULL,
    admission_payload jsonb,
    receipt_payload jsonb,
    status text NOT NULL CHECK (
        status IN ('reserved', 'admitting', 'admitted', 'copying', 'accepted')
    ),
    requested_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    PRIMARY KEY (request_scope, request_id),
    UNIQUE (request_scope, idempotency_key),
    FOREIGN KEY (request_scope, source_binding_id)
        REFERENCES belllabs_control.runtime_execution_bindings(request_scope, binding_id),
    CHECK (retain_until > requested_at)
);
CREATE INDEX runtime_fork_retention_idx
    ON belllabs_control.runtime_fork_requests (request_scope, retain_until, request_id);

CREATE TABLE belllabs_control.runtime_repair_audit (
    request_scope text NOT NULL,
    audit_id text NOT NULL,
    incident_id text,
    command_id text NOT NULL,
    actor_id text NOT NULL,
    reason text NOT NULL,
    expected_belllabs_version bigint NOT NULL CHECK (expected_belllabs_version >= 1),
    expected_checkpoint_id text,
    before_digest text NOT NULL CHECK (before_digest ~ '^sha256:[0-9a-f]{64}$'),
    after_digest text NOT NULL CHECK (after_digest ~ '^sha256:[0-9a-f]{64}$'),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, audit_id),
    FOREIGN KEY (request_scope, incident_id)
        REFERENCES belllabs_control.runtime_reconciliation_incidents(request_scope, incident_id)
);

CREATE TABLE belllabs_control.runtime_retention_deletion_audit (
    request_scope text NOT NULL,
    deletion_id text NOT NULL,
    record_class text NOT NULL CHECK (
        record_class IN ('checkpoint', 'event', 'incident', 'lineage', 'decision', 'fork')
    ),
    cutoff_at timestamptz NOT NULL,
    deleted_count bigint NOT NULL CHECK (deleted_count >= 0),
    actor_id text NOT NULL,
    reason text NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, deletion_id)
);

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'runtime_lineage_records',
        'runtime_lineage_edges',
        'execution_resource_leases',
        'runtime_decision_requests',
        'runtime_decision_responses',
        'runtime_reconciliation_incidents',
        'runtime_fork_requests',
        'runtime_repair_audit',
        'runtime_retention_deletion_audit'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name
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

GRANT SELECT, INSERT, UPDATE, DELETE
    ON belllabs_control.runtime_lineage_records,
       belllabs_control.runtime_lineage_edges,
       belllabs_control.execution_resource_leases,
       belllabs_control.runtime_decision_requests,
       belllabs_control.runtime_decision_responses,
       belllabs_control.runtime_reconciliation_incidents,
       belllabs_control.runtime_fork_requests,
       belllabs_control.runtime_repair_audit,
       belllabs_control.runtime_retention_deletion_audit
    TO belllabs_control_runtime;

GRANT DELETE
    ON belllabs_control.runtime_checkpoint_observations,
       belllabs_control.outbox
    TO belllabs_control_runtime;

GRANT SELECT
    ON belllabs_control.runtime_lineage_records,
       belllabs_control.runtime_lineage_edges,
       belllabs_control.execution_resource_leases,
       belllabs_control.runtime_decision_requests,
       belllabs_control.runtime_decision_responses,
       belllabs_control.runtime_reconciliation_incidents,
       belllabs_control.runtime_fork_requests,
       belllabs_control.runtime_repair_audit,
       belllabs_control.runtime_retention_deletion_audit
    TO belllabs_operations_readonly;

GRANT SELECT
    ON belllabs_control.runtime_decision_requests,
       belllabs_control.execution_resource_leases
    TO belllabs_agent_runtime;
