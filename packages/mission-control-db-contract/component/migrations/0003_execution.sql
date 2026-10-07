-- mission-control-db-contract common release: runs, activations, attempts and
-- agent execution records. Execution completion never sets mission acceptance.

CREATE TABLE mission_control.mission_run (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    mission_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    scheduling_revision_id uuid NOT NULL,
    request_key text NOT NULL CHECK (request_key <> ''),
    input_manifest_ref text NOT NULL CHECK (input_manifest_ref <> ''),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    admission_binding_ref text NOT NULL CHECK (admission_binding_ref <> ''),
    admission_binding_digest text NOT NULL CHECK (admission_binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    workflow_family text CHECK (workflow_family <> ''),
    phase text NOT NULL CHECK (phase IN ('pending', 'active', 'waiting', 'paused', 'cancelling', 'terminal')),
    lifecycle text NOT NULL CHECK (lifecycle IN ('admitted', 'active', 'waiting', 'paused', 'cancelling', 'completed')),
    terminal_outcome text CHECK (terminal_outcome IN ('succeeded', 'partially_completed', 'failed', 'cancelled', 'rejected')),
    temporal_workflow_id text CHECK (temporal_workflow_id <> ''),
    temporal_run_id text CHECK (temporal_run_id <> ''),
    execution_epoch bigint NOT NULL CHECK (execution_epoch > 0),
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    technical_segment bigint NOT NULL CHECK (technical_segment > 0),
    admitted_policy_ref text NOT NULL CHECK (admitted_policy_ref <> ''),
    admitted_build_ref text NOT NULL CHECK (admitted_build_ref <> ''),
    projection_contract text NOT NULL CHECK (projection_contract ~ '^[a-z0-9_.]+/[0-9]+$'),
    projection jsonb NOT NULL CHECK (jsonb_typeof(projection) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    admitted_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK ((lifecycle = 'completed') = (terminal_outcome IS NOT NULL)),
    CHECK ((phase = 'terminal') = (lifecycle = 'completed')),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, run_id),
    UNIQUE (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, request_key),
    UNIQUE (installation_id, application_id, tenant_id, temporal_workflow_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, mission_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id, scheduling_revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, mission_id, revision_id)
);
CREATE INDEX mission_run_mission_id_revision_id_fk_idx ON mission_control.mission_run (installation_id, application_id, tenant_id, mission_id, revision_id);
CREATE INDEX mission_run_mission_id_scheduling_revision_id_fk_idx ON mission_control.mission_run (installation_id, application_id, tenant_id, mission_id, scheduling_revision_id);
CREATE INDEX mission_run_lifecycle_idx ON mission_control.mission_run (installation_id, application_id, tenant_id, mission_id, lifecycle);

CREATE TABLE mission_control.run_revision_transition (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    transition_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    source_revision_id uuid NOT NULL,
    target_revision_id uuid NOT NULL,
    impact_manifest_ref text NOT NULL CHECK (impact_manifest_ref <> ''),
    impact_manifest_digest text NOT NULL CHECK (impact_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding_manifest_ref text NOT NULL CHECK (binding_manifest_ref <> ''),
    binding_manifest_digest text NOT NULL CHECK (binding_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    activation_policies jsonb NOT NULL CHECK (jsonb_typeof(activation_policies) = 'object'),
    lifecycle text NOT NULL CHECK (lifecycle IN ('proposed', 'applying', 'completed')),
    terminal_outcome text CHECK (terminal_outcome IN ('applied', 'rejected', 'superseded')),
    applied_event_refs text[] NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    CHECK ((lifecycle = 'completed') = (terminal_outcome IS NOT NULL)),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, transition_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, target_revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX run_revision_transition_run_id_fk_idx ON mission_control.run_revision_transition (installation_id, application_id, tenant_id, run_id);
CREATE INDEX run_revision_transition_source_revision_id_fk_idx ON mission_control.run_revision_transition (installation_id, application_id, tenant_id, source_revision_id);
CREATE INDEX run_revision_transition_target_revision_id_fk_idx ON mission_control.run_revision_transition (installation_id, application_id, tenant_id, target_revision_id);

CREATE TABLE mission_control.activation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    activation_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    activation_key text NOT NULL CHECK (activation_key <> ''),
    revision_id uuid NOT NULL,
    program_node_id uuid,
    parent_activation_id uuid,
    expansion_key text NOT NULL CHECK (expansion_key <> ''),
    repetition_ordinal bigint NOT NULL CHECK (repetition_ordinal >= 0),
    lifecycle text NOT NULL CHECK (lifecycle IN ('pending', 'released', 'running', 'waiting', 'paused', 'cancelling', 'completed')),
    phase text NOT NULL CHECK (phase <> ''),
    terminal_outcome text CHECK (terminal_outcome IN ('succeeded', 'failed', 'cancelled', 'skipped', 'superseded')),
    completion_decision_id uuid,
    governor_projection jsonb NOT NULL CHECK (jsonb_typeof(governor_projection) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    CHECK ((lifecycle = 'completed') = (terminal_outcome IS NOT NULL)),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, activation_id),
    UNIQUE (installation_id, application_id, tenant_id, activation_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, run_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, program_node_id) REFERENCES mission_control.program_node (installation_id, application_id, tenant_id, revision_id, program_node_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id)
);
CREATE INDEX activation_run_id_fk_idx ON mission_control.activation (installation_id, application_id, tenant_id, run_id);
CREATE INDEX activation_revision_id_fk_idx ON mission_control.activation (installation_id, application_id, tenant_id, revision_id);
CREATE INDEX activation_parent_activation_id_fk_idx ON mission_control.activation (installation_id, application_id, tenant_id, parent_activation_id);
CREATE INDEX activation_lifecycle_idx ON mission_control.activation (installation_id, application_id, tenant_id, run_id, lifecycle);

CREATE TABLE mission_control.attempt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    attempt_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    activation_id uuid NOT NULL,
    attempt_key text NOT NULL CHECK (attempt_key <> ''),
    attempt_no bigint NOT NULL CHECK (attempt_no > 0),
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    execution_outcome text CHECK (execution_outcome IN ('succeeded', 'failed', 'cancelled', 'abandoned', 'unknown')),
    failure_class text CHECK (failure_class <> ''),
    binding_ref text CHECK (binding_ref <> ''),
    binding_digest text CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    lease_expires_at timestamptz,
    fencing_token bigint NOT NULL CHECK (fencing_token >= 0),
    started_at timestamptz,
    ended_at timestamptz,
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, attempt_id),
    UNIQUE (installation_id, application_id, tenant_id, attempt_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, activation_id, attempt_no),
    UNIQUE (installation_id, application_id, tenant_id, run_id, attempt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, run_id, activation_id)
);
CREATE INDEX attempt_run_id_activation_id_fk_idx ON mission_control.attempt (installation_id, application_id, tenant_id, run_id, activation_id);

CREATE TABLE mission_control.harness_execution (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    harness_execution_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    runtime_kind text NOT NULL CHECK (runtime_kind <> ''),
    provider_kind text NOT NULL CHECK (provider_kind <> ''),
    placement_kind text NOT NULL CHECK (placement_kind <> ''),
    intended_binding_digest text NOT NULL CHECK (intended_binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    actual_binding_digest text CHECK (actual_binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    native_execution_refs text[] NOT NULL,
    observation_cursor text CHECK (observation_cursor <> ''),
    recovery_state text NOT NULL CHECK (recovery_state IN ('live', 'recovering', 'recovered', 'lost')),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, harness_execution_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, run_id, attempt_id)
);
CREATE INDEX harness_execution_run_id_attempt_id_fk_idx ON mission_control.harness_execution (installation_id, application_id, tenant_id, run_id, attempt_id);

CREATE TABLE mission_control.agent_session (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    session_id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL,
    harness_execution_id uuid NOT NULL,
    native_session_ref text NOT NULL CHECK (native_session_ref <> ''),
    state text NOT NULL CHECK (state IN ('open', 'suspended', 'closed')),
    checkpoint_namespace text CHECK (checkpoint_namespace <> ''),
    checkpoint_ref text CHECK (checkpoint_ref <> ''),
    predecessor_session_id uuid,
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, session_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, native_session_ref),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, harness_execution_id) REFERENCES mission_control.harness_execution (installation_id, application_id, tenant_id, harness_execution_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, predecessor_session_id) REFERENCES mission_control.agent_session (installation_id, application_id, tenant_id, session_id)
);
CREATE INDEX agent_session_attempt_id_fk_idx ON mission_control.agent_session (installation_id, application_id, tenant_id, attempt_id);
CREATE INDEX agent_session_harness_execution_id_fk_idx ON mission_control.agent_session (installation_id, application_id, tenant_id, harness_execution_id);
CREATE INDEX agent_session_predecessor_session_id_fk_idx ON mission_control.agent_session (installation_id, application_id, tenant_id, predecessor_session_id);

CREATE TABLE mission_control.session_turn (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    turn_id uuid PRIMARY KEY,
    session_id uuid NOT NULL,
    turn_no bigint NOT NULL CHECK (turn_no > 0),
    native_turn_ref text CHECK (native_turn_ref <> ''),
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    observation_refs text[] NOT NULL,
    usage_refs text[] NOT NULL,
    execution_outcome text CHECK (execution_outcome IN ('succeeded', 'failed', 'interrupted', 'unknown')),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, turn_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, session_id, turn_no),
    FOREIGN KEY (installation_id, application_id, tenant_id, session_id) REFERENCES mission_control.agent_session (installation_id, application_id, tenant_id, session_id)
);
CREATE INDEX session_turn_session_id_fk_idx ON mission_control.session_turn (installation_id, application_id, tenant_id, session_id);
CREATE TRIGGER session_turn_immutable
    BEFORE UPDATE OR DELETE ON mission_control.session_turn
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.workspace_lease (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    workspace_lease_id uuid PRIMARY KEY,
    attempt_id uuid,
    lease_key text NOT NULL CHECK (lease_key <> ''),
    owner_ref text NOT NULL CHECK (owner_ref <> ''),
    provider_kind text NOT NULL CHECK (provider_kind <> ''),
    native_workspace_ref text CHECK (native_workspace_ref <> ''),
    profile_digest text CHECK (profile_digest ~ '^sha256:[0-9a-f]{64}$'),
    snapshot_digest text CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    lease_expires_at timestamptz,
    fencing_token bigint NOT NULL CHECK (fencing_token >= 0),
    desired_state text NOT NULL CHECK (desired_state IN ('active', 'released')),
    observed_state text NOT NULL CHECK (observed_state IN ('pending', 'active', 'released', 'lost', 'unknown')),
    cleanup_status text NOT NULL CHECK (cleanup_status IN ('not_required', 'pending', 'completed', 'failed')),
    repository_allowlist_ref text CHECK (repository_allowlist_ref <> ''),
    base_commit text CHECK (base_commit <> ''),
    patch_ref text CHECK (patch_ref <> ''),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, workspace_lease_id),
    UNIQUE (installation_id, application_id, tenant_id, lease_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id)
);
CREATE INDEX workspace_lease_attempt_id_fk_idx ON mission_control.workspace_lease (installation_id, application_id, tenant_id, attempt_id);

CREATE TABLE mission_control.continuation_checkpoint (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    checkpoint_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    checkpoint_key text NOT NULL CHECK (checkpoint_key <> ''),
    activation_id uuid,
    attempt_id uuid,
    predecessor_checkpoint_id uuid,
    manifest_ref text NOT NULL CHECK (manifest_ref <> ''),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    contract_version text NOT NULL CHECK (contract_version <> ''),
    runtime_checkpoint_ref text CHECK (runtime_checkpoint_ref <> ''),
    sandbox_snapshot_ref text CHECK (sandbox_snapshot_ref <> ''),
    source_revision_digest text CHECK (source_revision_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_binding_digest text CHECK (source_binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    source_input_digest text CHECK (source_input_digest ~ '^sha256:[0-9a-f]{64}$'),
    execution_epoch bigint NOT NULL CHECK (execution_epoch > 0),
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    event_frontier text CHECK (event_frontier <> ''),
    harness_format text CHECK (harness_format <> ''),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, checkpoint_id),
    UNIQUE (installation_id, application_id, tenant_id, checkpoint_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, predecessor_checkpoint_id) REFERENCES mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, checkpoint_id)
);
CREATE INDEX continuation_checkpoint_run_id_fk_idx ON mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, run_id);
CREATE INDEX continuation_checkpoint_activation_id_fk_idx ON mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX continuation_checkpoint_attempt_id_fk_idx ON mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, attempt_id);
CREATE INDEX continuation_checkpoint_predecessor_checkpoint_id_fk_idx ON mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, predecessor_checkpoint_id);
CREATE TRIGGER continuation_checkpoint_immutable
    BEFORE UPDATE OR DELETE ON mission_control.continuation_checkpoint
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.checkpoint_validation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    checkpoint_validation_id uuid PRIMARY KEY,
    checkpoint_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('sealing', 'valid', 'invalid')),
    reason text CHECK (reason <> ''),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, checkpoint_validation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, checkpoint_id) REFERENCES mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, checkpoint_id)
);
CREATE INDEX checkpoint_validation_checkpoint_id_fk_idx ON mission_control.checkpoint_validation (installation_id, application_id, tenant_id, checkpoint_id);
CREATE TRIGGER checkpoint_validation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.checkpoint_validation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.journal_segment (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    journal_segment_id uuid PRIMARY KEY,
    activation_id uuid NOT NULL,
    first_entry_seq bigint NOT NULL CHECK (first_entry_seq >= 1),
    last_entry_seq bigint NOT NULL CHECK (last_entry_seq >= 1),
    segment_ref text NOT NULL CHECK (segment_ref <> ''),
    segment_digest text NOT NULL CHECK (segment_digest ~ '^sha256:[0-9a-f]{64}$'),
    previous_segment_digest text CHECK (previous_segment_digest ~ '^sha256:[0-9a-f]{64}$'),
    summarized_through_seq bigint,
    CHECK (last_entry_seq >= first_entry_seq),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, journal_segment_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, activation_id, first_entry_seq),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id)
);
CREATE INDEX journal_segment_activation_id_fk_idx ON mission_control.journal_segment (installation_id, application_id, tenant_id, activation_id);
CREATE TRIGGER journal_segment_immutable
    BEFORE UPDATE OR DELETE ON mission_control.journal_segment
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.subordinate_execution (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subordinate_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    subordinate_key text NOT NULL CHECK (subordinate_key <> ''),
    parent_activation_id uuid,
    parent_attempt_id uuid,
    execution_kind text NOT NULL CHECK (execution_kind IN ('sync', 'async')),
    dependency_class text NOT NULL CHECK (dependency_class IN ('required', 'degradable', 'nonblocking')),
    binding_ref text NOT NULL CHECK (binding_ref <> ''),
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    dependency_policy jsonb NOT NULL CHECK (jsonb_typeof(dependency_policy) = 'object'),
    native_task_ref text CHECK (native_task_ref <> ''),
    native_thread_ref text CHECK (native_thread_ref <> ''),
    generation bigint NOT NULL CHECK (generation > 0),
    desired_lifecycle text NOT NULL CHECK (desired_lifecycle <> ''),
    observed_lifecycle text NOT NULL CHECK (observed_lifecycle <> ''),
    output_refs text[] NOT NULL,
    result_admission text NOT NULL CHECK (result_admission IN ('pending', 'admitted', 'rejected', 'not_applicable')),
    deadline_at timestamptz,
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_id),
    UNIQUE (installation_id, application_id, tenant_id, subordinate_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id)
);
CREATE INDEX subordinate_execution_run_id_fk_idx ON mission_control.subordinate_execution (installation_id, application_id, tenant_id, run_id);
CREATE INDEX subordinate_execution_parent_activation_id_fk_idx ON mission_control.subordinate_execution (installation_id, application_id, tenant_id, parent_activation_id);
CREATE INDEX subordinate_execution_parent_attempt_id_fk_idx ON mission_control.subordinate_execution (installation_id, application_id, tenant_id, parent_attempt_id);

CREATE TABLE mission_control.completion_candidate (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    candidate_id uuid PRIMARY KEY,
    activation_id uuid NOT NULL,
    attempt_id uuid,
    candidate_key text NOT NULL CHECK (candidate_key <> ''),
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    candidate jsonb NOT NULL CHECK (jsonb_typeof(candidate) = 'object'),
    candidate_digest text NOT NULL CHECK (candidate_digest ~ '^sha256:[0-9a-f]{64}$'),
    artifact_refs text[] NOT NULL,
    evidence_refs text[] NOT NULL,
    submitted_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, candidate_id),
    UNIQUE (installation_id, application_id, tenant_id, candidate_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id)
);
CREATE INDEX completion_candidate_activation_id_fk_idx ON mission_control.completion_candidate (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX completion_candidate_attempt_id_fk_idx ON mission_control.completion_candidate (installation_id, application_id, tenant_id, attempt_id);
CREATE TRIGGER completion_candidate_immutable
    BEFORE UPDATE OR DELETE ON mission_control.completion_candidate
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.completion_decision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    completion_decision_id uuid PRIMARY KEY,
    decision_key text NOT NULL CHECK (decision_key <> ''),
    run_id uuid NOT NULL,
    activation_id uuid,
    goal_id uuid,
    candidate_id uuid,
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_disposition text NOT NULL CHECK (result_disposition IN ('accepted', 'rejected', 'needs_more_work', 'abandoned')),
    requirement_refs text[] NOT NULL,
    result_refs text[] NOT NULL,
    reason_codes text[] NOT NULL,
    decision jsonb NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, completion_decision_id),
    UNIQUE (installation_id, application_id, tenant_id, decision_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, candidate_id) REFERENCES mission_control.completion_candidate (installation_id, application_id, tenant_id, candidate_id)
);
CREATE INDEX completion_decision_run_id_fk_idx ON mission_control.completion_decision (installation_id, application_id, tenant_id, run_id);
CREATE INDEX completion_decision_activation_id_fk_idx ON mission_control.completion_decision (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX completion_decision_candidate_id_fk_idx ON mission_control.completion_decision (installation_id, application_id, tenant_id, candidate_id);
CREATE TRIGGER completion_decision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.completion_decision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'mission_run',
        'run_revision_transition',
        'activation',
        'attempt',
        'harness_execution',
        'agent_session',
        'session_turn',
        'workspace_lease',
        'continuation_checkpoint',
        'checkpoint_validation',
        'journal_segment',
        'subordinate_execution',
        'completion_candidate',
        'completion_decision'
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

REVOKE ALL ON mission_control.mission_run, mission_control.run_revision_transition, mission_control.activation, mission_control.attempt, mission_control.harness_execution, mission_control.agent_session, mission_control.session_turn, mission_control.workspace_lease, mission_control.continuation_checkpoint, mission_control.checkpoint_validation, mission_control.journal_segment, mission_control.subordinate_execution, mission_control.completion_candidate, mission_control.completion_decision FROM PUBLIC;
