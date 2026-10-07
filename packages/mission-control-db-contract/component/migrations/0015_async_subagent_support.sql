-- mission-control-db-contract common release: async subagent support records.
-- An async child is a canonical subordinate_execution under its parent mission_run.
-- Admission, cancellation, result and settlement commands are canonical command rows;
-- observed facts are canonical native_observation rows; typed in_doubt incidents are
-- canonical reconciliation_case rows. The support records keep the exact admission and
-- submission fence, typed messages, observed provider runs with their usage disposition,
-- and the immutable contract plus mutable execution/link detail envelopes.

CREATE TABLE mission_control.subordinate_admission (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_admission_id uuid PRIMARY KEY,
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    subordinate_id uuid NOT NULL,
    parent_run_key text NOT NULL CHECK (parent_run_key <> ''),
    parent_operation_key text NOT NULL CHECK (parent_operation_key <> ''),
    parent_binding_key text,
    link_key text NOT NULL CHECK (link_key <> ''),
    contract_key text NOT NULL CHECK (contract_key <> ''),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[0-9a-f]{64}$'),
    reservation_key text NOT NULL CHECK (reservation_key <> ''),
    dependency_class text NOT NULL CHECK (dependency_class IN ('required_blocking', 'degradable_blocking', 'nonblocking', 'advisory')),
    execution_generation integer NOT NULL CHECK (execution_generation >= 1),
    lifecycle text NOT NULL CHECK (lifecycle IN ('proposed', 'admitted', 'submitted', 'running', 'waiting', 'completed', 'failed', 'cancelled', 'orphaned', 'in_doubt')),
    lifecycle_updated_at timestamptz,
    cancellation_requested boolean NOT NULL,
    result_decision text CHECK (result_decision IN ('admit', 'conditionally_admit', 'reject', 'defer')),
    result_manifest_digest text,
    settlement_ref text,
    provider_thread_key text,
    provider_run_key text,
    submission_fence bigint NOT NULL CHECK (submission_fence >= 0),
    submission_holder text,
    submission_lease_expires_at timestamptz,
    in_doubt_reason text CHECK (in_doubt_reason IS NULL OR in_doubt_reason IN ('submission_unobservable', 'multiple_provider_runs', 'graph_identity_mismatch', 'provider_binding_lost')),
    incident_key text,
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((lifecycle = 'in_doubt') = (in_doubt_reason IS NOT NULL)),
    CHECK ((provider_thread_key IS NULL) = (provider_run_key IS NULL)),
    CHECK ((submission_holder IS NULL) = (submission_lease_expires_at IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_admission_id),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_id),
    UNIQUE (installation_id, application_id, tenant_id, link_key),
    UNIQUE (installation_id, application_id, tenant_id, reservation_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_id) REFERENCES mission_control.subordinate_execution (installation_id, application_id, tenant_id, subordinate_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX subordinate_admission_parent_idx ON mission_control.subordinate_admission (installation_id, application_id, tenant_id, parent_run_key, lifecycle);
CREATE INDEX subordinate_admission_binding_idx ON mission_control.subordinate_admission (installation_id, application_id, tenant_id, parent_binding_key);

CREATE TABLE mission_control.subordinate_message (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_message_id uuid PRIMARY KEY,
    message_key text NOT NULL CHECK (message_key <> ''),
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    direction text NOT NULL CHECK (direction IN ('parent_to_child', 'child_to_parent')),
    target_sequence bigint NOT NULL CHECK (target_sequence >= 1),
    receipt text NOT NULL CHECK (receipt <> ''),
    message_contract text NOT NULL CHECK (message_contract = 'belllabs.async-subagent-message.v1'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_message_id),
    UNIQUE (installation_id, application_id, tenant_id, message_key),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key, direction, target_sequence),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_key) REFERENCES mission_control.subordinate_admission (installation_id, application_id, tenant_id, subordinate_key)
);
CREATE TRIGGER subordinate_message_immutable
    BEFORE UPDATE OR DELETE ON mission_control.subordinate_message
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.async_provider_run (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    async_provider_run_id uuid PRIMARY KEY,
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    provider_run_key text NOT NULL CHECK (provider_run_key <> ''),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.async-provider-run.v1'),
    provider_thread_key text NOT NULL CHECK (provider_thread_key <> ''),
    disposition text NOT NULL CHECK (disposition IN ('bound', 'duplicate_cancelled', 'orphaned_cancelled', 'cancel_ambiguous')),
    provider_status text NOT NULL CHECK (provider_status <> ''),
    usage_attribution text NOT NULL CHECK (usage_attribution IN ('provider_attributed', 'pending', 'ambiguous')),
    attributed_amounts jsonb NOT NULL CHECK (jsonb_typeof(attributed_amounts) = 'object'),
    pending_amounts jsonb NOT NULL CHECK (jsonb_typeof(pending_amounts) = 'object'),
    record_payload jsonb NOT NULL CHECK (jsonb_typeof(record_payload) = 'object'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, async_provider_run_id),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key, provider_run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_key) REFERENCES mission_control.subordinate_admission (installation_id, application_id, tenant_id, subordinate_key)
);
CREATE INDEX async_provider_run_child_idx ON mission_control.async_provider_run (installation_id, application_id, tenant_id, subordinate_key, observed_at);

CREATE TABLE mission_control.subordinate_contract (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_contract_id uuid PRIMARY KEY,
    contract_key text NOT NULL CHECK (contract_key <> ''),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (((payload ->> 'schema_version') = 'belllabs.async-subagent-contract.v1'
            AND (payload ->> 'contract_id') = contract_key
            AND (payload ->> 'contract_digest') = contract_digest) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_contract_id),
    UNIQUE (installation_id, application_id, tenant_id, contract_key),
    UNIQUE (installation_id, application_id, tenant_id, contract_key, contract_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE TRIGGER subordinate_contract_immutable
    BEFORE UPDATE OR DELETE ON mission_control.subordinate_contract
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.subordinate_execution_detail (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_execution_detail_id uuid PRIMARY KEY,
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    contract_key text NOT NULL,
    contract_digest text NOT NULL,
    parent_run_key text NOT NULL,
    parent_operation_key text NOT NULL,
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (((payload ->> 'schema_version') = 'belllabs.async-subagent-execution.v1'
            AND (payload ->> 'child_execution_id') = subordinate_key
            AND (payload ->> 'contract_id') = contract_key
            AND (payload ->> 'contract_digest') = contract_digest
            AND (payload ->> 'parent_run_id') = parent_run_key
            AND (payload ->> 'parent_operation_id') = parent_operation_key
            AND (payload ->> 'execution_generation') = execution_generation::text
            AND ((payload ->> 'updated_at'))::timestamptz = updated_at) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_execution_detail_id),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key, parent_run_key, parent_operation_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, contract_key, contract_digest) REFERENCES mission_control.subordinate_contract (installation_id, application_id, tenant_id, contract_key, contract_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX subordinate_execution_detail_contract_idx ON mission_control.subordinate_execution_detail (installation_id, application_id, tenant_id, contract_key, contract_digest);
CREATE INDEX subordinate_execution_detail_run_idx ON mission_control.subordinate_execution_detail (installation_id, application_id, tenant_id, parent_run_key);

CREATE TABLE mission_control.subordinate_link_detail (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_link_detail_id uuid PRIMARY KEY,
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    link_key text NOT NULL CHECK (link_key <> ''),
    parent_run_key text NOT NULL,
    parent_operation_key text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (((payload ->> 'schema_version') = 'belllabs.parent-async-subagent-link.v1'
            AND (payload ->> 'child_execution_id') = subordinate_key
            AND (payload ->> 'link_id') = link_key
            AND (payload ->> 'parent_run_id') = parent_run_key
            AND (payload ->> 'parent_operation_id') = parent_operation_key
            AND ((payload ->> 'updated_at'))::timestamptz = updated_at) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_link_detail_id),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key),
    UNIQUE (installation_id, application_id, tenant_id, link_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_key, parent_run_key, parent_operation_key) REFERENCES mission_control.subordinate_execution_detail (installation_id, application_id, tenant_id, subordinate_key, parent_run_key, parent_operation_key)
);

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'subordinate_admission',
        'subordinate_message',
        'async_provider_run',
        'subordinate_contract',
        'subordinate_execution_detail',
        'subordinate_link_detail'
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

REVOKE ALL ON mission_control.subordinate_admission, mission_control.subordinate_message,
    mission_control.async_provider_run, mission_control.subordinate_contract,
    mission_control.subordinate_execution_detail, mission_control.subordinate_link_detail
FROM PUBLIC;

-- Port of the legacy control runtime role / the legacy operations read-only role grants.
GRANT SELECT, INSERT, UPDATE ON mission_control.subordinate_execution,
    mission_control.subordinate_admission, mission_control.async_provider_run
TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.native_observation, mission_control.subordinate_message,
    mission_control.subordinate_contract, mission_control.subordinate_execution_detail,
    mission_control.subordinate_link_detail
TO mission_control_runtime;
GRANT UPDATE (execution_generation, payload, updated_at)
ON mission_control.subordinate_execution_detail TO mission_control_runtime;
GRANT UPDATE (payload, updated_at)
ON mission_control.subordinate_link_detail TO mission_control_runtime;
GRANT SELECT ON mission_control.subordinate_execution, mission_control.native_observation,
    mission_control.subordinate_admission, mission_control.subordinate_message,
    mission_control.async_provider_run, mission_control.subordinate_contract,
    mission_control.subordinate_execution_detail, mission_control.subordinate_link_detail
TO mission_control_readonly;
