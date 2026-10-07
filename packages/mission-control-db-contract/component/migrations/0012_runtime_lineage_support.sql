-- mission-control-db-contract common release: runtime units and checkpoint lineage.
-- A runtime unit is a canonical activation (activation_key = unit key) and each execution
-- generation is a canonical attempt (attempt_no = generation, fencing_token = claim
-- fence, lease_expires_at = claim lease). The support records keep the immutable lineage
-- evidence: generation detail, cognitive namespaces, activity attempt observations
-- (technical retries are observations), checkpoint transitions, unit result observations
-- and stale-fence rejections. Observations never accept a mission.

CREATE TABLE mission_control.runtime_unit_generation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_unit_generation_id uuid PRIMARY KEY,
    attempt_key text NOT NULL CHECK (attempt_key <> ''),
    unit_key text NOT NULL CHECK (unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    execution_generation bigint NOT NULL CHECK (execution_generation >= 1),
    binding_key text NOT NULL CHECK (binding_key <> ''),
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    cognitive_namespace text,
    state_schema_digest text CHECK (state_schema_digest IS NULL OR state_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    lease_holder text,
    superseded boolean NOT NULL,
    recorded_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((cognitive_namespace IS NULL) = (state_schema_digest IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, runtime_unit_generation_id),
    UNIQUE (installation_id, application_id, tenant_id, attempt_key),
    UNIQUE (installation_id, application_id, tenant_id, unit_key, execution_generation),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_key) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, unit_key) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_key)
);

CREATE TABLE mission_control.cognitive_namespace (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    cognitive_namespace_id uuid PRIMARY KEY,
    namespace_key text NOT NULL CHECK (namespace_key <> ''),
    owner_kind text NOT NULL CHECK (owner_kind IN ('stage_unit_generation', 'goal_session_role', 'goal_unit_generation')),
    owner_digest text NOT NULL CHECK (owner_digest ~ '^sha256:[0-9a-f]{64}$'),
    head_checkpoint jsonb CHECK (head_checkpoint IS NULL OR jsonb_typeof(head_checkpoint) = 'object'),
    head_checkpoint_key text,
    head_transition_key text,
    head_state_schema_digest text,
    head_version bigint NOT NULL CHECK (head_version >= 0),
    in_flight_unit_key text,
    in_flight_generation bigint,
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((head_checkpoint IS NULL) = (head_checkpoint_key IS NULL)),
    CHECK ((in_flight_unit_key IS NULL) = (in_flight_generation IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, cognitive_namespace_id),
    UNIQUE (installation_id, application_id, tenant_id, namespace_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

CREATE TABLE mission_control.activity_attempt_observation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    activity_attempt_observation_id uuid PRIMARY KEY,
    observation_key text NOT NULL CHECK (observation_key <> ''),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.activity-attempt-observation.v1'),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    temporal_workflow_id text NOT NULL,
    temporal_run_id text NOT NULL,
    temporal_activity_id text NOT NULL,
    activity_attempt bigint NOT NULL CHECK (activity_attempt >= 1),
    worker_identity text NOT NULL,
    binding_key text NOT NULL,
    cognitive_namespace text,
    expected_source_checkpoint jsonb,
    dispatching boolean NOT NULL,
    observation_payload jsonb NOT NULL CHECK (jsonb_typeof(observation_payload) = 'object'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, activity_attempt_observation_id),
    UNIQUE (installation_id, application_id, tenant_id, observation_key),
    UNIQUE (installation_id, application_id, tenant_id, unit_key, execution_generation, temporal_workflow_id, temporal_run_id, temporal_activity_id, activity_attempt),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, unit_key, execution_generation) REFERENCES mission_control.runtime_unit_generation (installation_id, application_id, tenant_id, unit_key, execution_generation)
);
CREATE INDEX activity_attempt_observation_unit_idx ON mission_control.activity_attempt_observation (installation_id, application_id, tenant_id, unit_key, execution_generation, activity_attempt);
CREATE TRIGGER activity_attempt_observation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.activity_attempt_observation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.checkpoint_transition (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    checkpoint_transition_id uuid PRIMARY KEY,
    transition_key text NOT NULL CHECK (transition_key <> ''),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.checkpoint-transition.v1'),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    namespace_key text NOT NULL,
    source_checkpoint_key text,
    result_checkpoint_key text NOT NULL,
    result_parent_checkpoint_key text NOT NULL,
    ancestry_verified boolean NOT NULL CHECK (ancestry_verified),
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    state_schema_digest text NOT NULL CHECK (state_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    classification text NOT NULL CHECK (classification IN ('not_submitted', 'interrupted', 'terminal_unobserved')),
    invocation_digest text NOT NULL CHECK (invocation_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_ref text NOT NULL,
    result_manifest_digest text NOT NULL CHECK (result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    redacted_summary_digest text NOT NULL CHECK (redacted_summary_digest ~ '^sha256:[0-9a-f]{64}$'),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    transition_payload jsonb NOT NULL CHECK (jsonb_typeof(transition_payload) = 'object'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, checkpoint_transition_id),
    UNIQUE (installation_id, application_id, tenant_id, transition_key),
    UNIQUE (installation_id, application_id, tenant_id, namespace_key, result_checkpoint_key),
    UNIQUE (installation_id, application_id, tenant_id, unit_key, execution_generation),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, namespace_key) REFERENCES mission_control.cognitive_namespace (installation_id, application_id, tenant_id, namespace_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, unit_key, execution_generation) REFERENCES mission_control.runtime_unit_generation (installation_id, application_id, tenant_id, unit_key, execution_generation)
);
CREATE UNIQUE INDEX checkpoint_transition_single_successor_idx ON mission_control.checkpoint_transition (installation_id, application_id, tenant_id, namespace_key, COALESCE(source_checkpoint_key, ''));
CREATE TRIGGER checkpoint_transition_immutable
    BEFORE UPDATE OR DELETE ON mission_control.checkpoint_transition
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.unit_result_observation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    unit_result_observation_id uuid PRIMARY KEY,
    observation_key text NOT NULL CHECK (observation_key <> ''),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.unit-result-observation.v1'),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    binding_key text NOT NULL,
    settlement_key text NOT NULL,
    status text NOT NULL CHECK (status IN ('completed', 'failed', 'cancelled', 'timed_out')),
    result_manifest_ref text NOT NULL,
    result_manifest_digest text NOT NULL CHECK (result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_size_bytes bigint NOT NULL CHECK (result_manifest_size_bytes >= 1),
    checkpoint_transition_key text,
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_payload jsonb NOT NULL CHECK (jsonb_typeof(result_payload) = 'object'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, unit_result_observation_id),
    UNIQUE (installation_id, application_id, tenant_id, observation_key),
    UNIQUE (installation_id, application_id, tenant_id, unit_key, execution_generation),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, checkpoint_transition_key) REFERENCES mission_control.checkpoint_transition (installation_id, application_id, tenant_id, transition_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, unit_key, execution_generation) REFERENCES mission_control.runtime_unit_generation (installation_id, application_id, tenant_id, unit_key, execution_generation)
);
CREATE TRIGGER unit_result_observation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.unit_result_observation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.lineage_write_rejection (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    lineage_write_rejection_id uuid PRIMARY KEY,
    rejection_key text NOT NULL CHECK (rejection_key <> ''),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    presented_fence bigint NOT NULL,
    current_fence bigint NOT NULL,
    current_generation bigint NOT NULL,
    reason text NOT NULL CHECK (reason IN ('stale_claim_fence', 'stale_execution_generation')),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    rejected_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, lineage_write_rejection_id),
    UNIQUE (installation_id, application_id, tenant_id, rejection_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, unit_key) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_key)
);
CREATE INDEX lineage_write_rejection_unit_idx ON mission_control.lineage_write_rejection (installation_id, application_id, tenant_id, unit_key, rejected_at);
CREATE TRIGGER lineage_write_rejection_immutable
    BEFORE UPDATE OR DELETE ON mission_control.lineage_write_rejection
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'runtime_unit_generation',
        'cognitive_namespace',
        'activity_attempt_observation',
        'checkpoint_transition',
        'unit_result_observation',
        'lineage_write_rejection'
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

REVOKE ALL ON mission_control.runtime_unit_generation, mission_control.cognitive_namespace,
    mission_control.activity_attempt_observation, mission_control.checkpoint_transition,
    mission_control.unit_result_observation, mission_control.lineage_write_rejection FROM PUBLIC;

-- Port of the legacy control runtime role: units are insert-only identities; generations change
-- only their fence, lease and supersession; namespaces move their head and in-flight claim;
-- observations, transitions, results and rejections are insert-only.
GRANT SELECT, INSERT ON mission_control.activation, mission_control.attempt,
    mission_control.runtime_unit_generation, mission_control.cognitive_namespace,
    mission_control.activity_attempt_observation, mission_control.checkpoint_transition,
    mission_control.unit_result_observation, mission_control.lineage_write_rejection
TO mission_control_runtime;
GRANT UPDATE (fencing_token, lease_expires_at, version, updated_at)
ON mission_control.attempt TO mission_control_runtime;
GRANT UPDATE (lease_holder, superseded, updated_at)
ON mission_control.runtime_unit_generation TO mission_control_runtime;
GRANT UPDATE (head_checkpoint, head_checkpoint_key, head_transition_key, head_state_schema_digest,
    head_version, in_flight_unit_key, in_flight_generation, updated_at)
ON mission_control.cognitive_namespace TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.reconciliation_case TO mission_control_runtime;
GRANT SELECT ON mission_control.activation, mission_control.attempt,
    mission_control.reconciliation_case, mission_control.runtime_unit_generation,
    mission_control.cognitive_namespace, mission_control.activity_attempt_observation,
    mission_control.checkpoint_transition, mission_control.unit_result_observation,
    mission_control.lineage_write_rejection
TO mission_control_readonly;
