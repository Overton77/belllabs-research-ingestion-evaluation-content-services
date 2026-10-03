-- AMD-RRM-001 forward-only migration (RRM-003): runtime units, Activity attempt
-- observations, cognitive namespaces, and checkpoint transition observations.
-- Schema identities: belllabs.runtime-unit.v1 (CON-CP-RUNTIME-UNIT-V1),
-- belllabs.activity-attempt-observation.v1 (REQ-CP-EXEC-014),
-- belllabs.checkpoint-transition.v1 (CON-CP-CHECKPOINT-LINEAGE-V1).
-- LangGraph checkpoints remain subordinate evidence: these rows hold qualified keys and
-- digests only, never checkpoint bodies, transcripts, prompts, or secrets.
-- The Agent Server-shaped 0012 runtime tables stay inert (RRM-001 disposition row 47).

CREATE TABLE belllabs_control.runtime_units (
    request_scope text NOT NULL,
    unit_key text NOT NULL CHECK (unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.runtime-unit.v1'),
    belllabs_run_id text NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    family text NOT NULL CHECK (family IN ('stage_graph', 'goal_directed')),
    unit_kind text NOT NULL CHECK (
        unit_kind IN ('stage_operation', 'goal_executor', 'goal_verifier')
    ),
    semantic_operation_id text NOT NULL,
    semantic_attempt bigint NOT NULL CHECK (semantic_attempt >= 1),
    identity_payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, unit_key),
    FOREIGN KEY (request_scope, belllabs_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id)
);

CREATE TABLE belllabs_control.runtime_unit_generations (
    request_scope text NOT NULL,
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL CHECK (execution_generation >= 1),
    claim_fence bigint NOT NULL DEFAULT 1 CHECK (claim_fence >= 1),
    binding_id text NOT NULL,
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    cognitive_namespace text,
    state_schema_digest text CHECK (
        state_schema_digest IS NULL OR state_schema_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    recorded_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, unit_key, execution_generation),
    FOREIGN KEY (request_scope, unit_key)
        REFERENCES belllabs_control.runtime_units(request_scope, unit_key),
    CHECK ((cognitive_namespace IS NULL) = (state_schema_digest IS NULL))
);

CREATE TABLE belllabs_control.runtime_activity_attempt_observations (
    request_scope text NOT NULL,
    observation_id text NOT NULL,
    schema_version text NOT NULL CHECK (
        schema_version = 'belllabs.activity-attempt-observation.v1'
    ),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    temporal_workflow_id text NOT NULL,
    temporal_run_id text NOT NULL,
    temporal_activity_id text NOT NULL,
    activity_attempt bigint NOT NULL CHECK (activity_attempt >= 1),
    worker_identity text NOT NULL,
    binding_id text NOT NULL,
    cognitive_namespace text,
    expected_source_checkpoint jsonb,
    dispatching boolean NOT NULL,
    observation_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, observation_id),
    UNIQUE (
        request_scope, unit_key, execution_generation, temporal_workflow_id,
        temporal_run_id, temporal_activity_id, activity_attempt
    ),
    FOREIGN KEY (request_scope, unit_key, execution_generation)
        REFERENCES belllabs_control.runtime_unit_generations(
            request_scope, unit_key, execution_generation
        )
);

CREATE INDEX runtime_activity_attempts_unit_idx
    ON belllabs_control.runtime_activity_attempt_observations (
        request_scope, unit_key, execution_generation, activity_attempt
    );

CREATE TABLE belllabs_control.runtime_cognitive_namespaces (
    request_scope text NOT NULL,
    cognitive_namespace text NOT NULL,
    owner_kind text NOT NULL CHECK (
        owner_kind IN ('stage_unit_generation', 'goal_session_role', 'goal_unit_generation')
    ),
    owner_digest text NOT NULL CHECK (owner_digest ~ '^sha256:[0-9a-f]{64}$'),
    head_checkpoint jsonb,
    head_checkpoint_id text,
    head_transition_id text,
    head_state_schema_digest text,
    head_version bigint NOT NULL DEFAULT 0 CHECK (head_version >= 0),
    in_flight_unit_key text,
    in_flight_generation bigint,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, cognitive_namespace),
    CHECK ((head_checkpoint IS NULL) = (head_checkpoint_id IS NULL)),
    CHECK ((in_flight_unit_key IS NULL) = (in_flight_generation IS NULL))
);

CREATE TABLE belllabs_control.runtime_checkpoint_transitions (
    request_scope text NOT NULL,
    transition_id text NOT NULL,
    schema_version text NOT NULL CHECK (
        schema_version = 'belllabs.checkpoint-transition.v1'
    ),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    cognitive_namespace text NOT NULL,
    source_checkpoint_id text,
    result_checkpoint_id text NOT NULL,
    result_parent_checkpoint_id text NOT NULL,
    ancestry_verified boolean NOT NULL CHECK (ancestry_verified),
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    state_schema_digest text NOT NULL CHECK (
        state_schema_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    classification text NOT NULL CHECK (
        classification IN ('not_submitted', 'interrupted', 'terminal_unobserved')
    ),
    invocation_id text NOT NULL CHECK (invocation_id ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_ref text NOT NULL,
    result_manifest_digest text NOT NULL CHECK (
        result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    redacted_summary_digest text NOT NULL CHECK (
        redacted_summary_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    transition_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, transition_id),
    UNIQUE (request_scope, unit_key, execution_generation),
    UNIQUE (request_scope, cognitive_namespace, result_checkpoint_id),
    FOREIGN KEY (request_scope, unit_key, execution_generation)
        REFERENCES belllabs_control.runtime_unit_generations(
            request_scope, unit_key, execution_generation
        ),
    FOREIGN KEY (request_scope, cognitive_namespace)
        REFERENCES belllabs_control.runtime_cognitive_namespaces(
            request_scope, cognitive_namespace
        )
);

-- Compare-and-set backstop: at most one accepted transition leaves any namespace head.
CREATE UNIQUE INDEX runtime_checkpoint_transitions_single_successor_idx
    ON belllabs_control.runtime_checkpoint_transitions (
        request_scope, cognitive_namespace, COALESCE(source_checkpoint_id, '')
    );

CREATE TABLE belllabs_control.runtime_lineage_write_rejections (
    request_scope text NOT NULL,
    rejection_id text NOT NULL,
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    presented_fence bigint NOT NULL,
    current_fence bigint NOT NULL,
    current_generation bigint NOT NULL,
    reason text NOT NULL CHECK (
        reason IN ('stale_claim_fence', 'stale_execution_generation')
    ),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    rejected_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, rejection_id)
);

-- Version the active operation journal (RRM-001 disposition row 48): the effect claim
-- carries the runtime unit it fences. Transition and result checkpoint refs are linked
-- from the transition row to the digest-bound result manifest the settlement records.
ALTER TABLE belllabs_control.operation_effect_claims
    ADD COLUMN unit_key text CHECK (
        unit_key IS NULL OR unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'
    );
CREATE INDEX operation_effect_claims_unit_idx
    ON belllabs_control.operation_effect_claims (request_scope, unit_key)
    WHERE unit_key IS NOT NULL;

-- RRM-001 disposition rows 50-51: relax foreign keys to the retired Agent Server-shaped
-- runtime_execution_bindings. Incidents gain a unit-key reference for in_doubt units.
ALTER TABLE belllabs_control.runtime_reconciliation_incidents
    ADD COLUMN unit_key text CHECK (
        unit_key IS NULL OR unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'
    );
ALTER TABLE belllabs_control.runtime_fork_requests
    ALTER COLUMN source_binding_id DROP NOT NULL;

DO $$
DECLARE constraint_record record;
BEGIN
    FOR constraint_record IN
        SELECT conrelid::regclass AS table_name, conname
        FROM pg_constraint
        WHERE contype = 'f'
          AND confrelid = 'belllabs_control.runtime_execution_bindings'::regclass
          AND conrelid IN (
              'belllabs_control.runtime_reconciliation_incidents'::regclass,
              'belllabs_control.runtime_fork_requests'::regclass
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_record.table_name,
            constraint_record.conname
        );
    END LOOP;
END
$$;

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'runtime_units',
        'runtime_unit_generations',
        'runtime_activity_attempt_observations',
        'runtime_cognitive_namespaces',
        'runtime_checkpoint_transitions',
        'runtime_lineage_write_rejections'
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

-- Least privilege: the runtime updates only fences and namespace heads. Unit identity,
-- attempt observations, transitions and rejections are insert-only; per-unit writes are
-- serialized with pg_advisory_xact_lock, which needs no table privilege.
GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.runtime_unit_generations,
       belllabs_control.runtime_cognitive_namespaces
    TO belllabs_control_runtime;
GRANT SELECT, INSERT
    ON belllabs_control.runtime_units,
       belllabs_control.runtime_activity_attempt_observations,
       belllabs_control.runtime_checkpoint_transitions,
       belllabs_control.runtime_lineage_write_rejections
    TO belllabs_control_runtime;
GRANT SELECT
    ON belllabs_control.runtime_units,
       belllabs_control.runtime_unit_generations,
       belllabs_control.runtime_activity_attempt_observations,
       belllabs_control.runtime_cognitive_namespaces,
       belllabs_control.runtime_checkpoint_transitions,
       belllabs_control.runtime_lineage_write_rejections
    TO belllabs_operations_readonly;
