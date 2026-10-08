-- mission-control-db-contract common release: mission authoring records.
-- Immutable submitted definitions, revisions, compiled programs and goal trees.

CREATE TABLE mission_control.authoring_session (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    authoring_session_id uuid PRIMARY KEY,
    mission_id uuid,
    client_ref text NOT NULL CHECK (client_ref <> ''),
    participant_refs text[] NOT NULL CHECK (cardinality(participant_refs) > 0),
    decision_artifact_refs text[] NOT NULL,
    state text NOT NULL CHECK (state IN ('open', 'submitted', 'abandoned')),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, authoring_session_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

CREATE TABLE mission_control.mission (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    mission_id uuid PRIMARY KEY,
    mission_key text NOT NULL CHECK (mission_key <> ''),
    title text NOT NULL CHECK (title <> ''),
    owner_actor_ref text NOT NULL CHECK (owner_actor_ref <> ''),
    lifecycle text NOT NULL CHECK (lifecycle IN ('draft', 'admitted', 'active', 'paused', 'closing', 'closed')),
    scheduling_head_revision_id uuid,
    next_event_seq bigint NOT NULL CHECK (next_event_seq >= 1),
    acceptance_disposition text CHECK (acceptance_disposition IN ('accepted', 'rejected', 'withdrawn')),
    closure_outcome text CHECK (closure_outcome IN ('succeeded', 'failed', 'cancelled', 'abandoned')),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    last_event_seq bigint NOT NULL CHECK (last_event_seq >= 0),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, mission_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

CREATE TABLE mission_control.mission_draft (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    mission_draft_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    draft_version bigint NOT NULL CHECK (draft_version >= 1),
    definition_contract_version text NOT NULL CHECK (definition_contract_version <> ''),
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, mission_draft_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, draft_version),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id)
);
CREATE INDEX mission_draft_mission_id_fk_idx ON mission_control.mission_draft (installation_id, application_id, tenant_id, mission_id);

CREATE TABLE mission_control.definition_snapshot (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    definition_snapshot_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    definition_contract_version text NOT NULL CHECK (definition_contract_version <> ''),
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    definition_digest text NOT NULL CHECK (definition_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, definition_snapshot_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, definition_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id)
);
CREATE INDEX definition_snapshot_mission_id_fk_idx ON mission_control.definition_snapshot (installation_id, application_id, tenant_id, mission_id);
CREATE TRIGGER definition_snapshot_immutable
    BEFORE UPDATE OR DELETE ON mission_control.definition_snapshot
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.mission_revision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    revision_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    revision_no bigint NOT NULL CHECK (revision_no >= 1),
    parent_revision_id uuid,
    definition_snapshot_id uuid NOT NULL,
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    committed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, revision_no),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, definition_snapshot_id) REFERENCES mission_control.definition_snapshot (installation_id, application_id, tenant_id, definition_snapshot_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX mission_revision_mission_id_fk_idx ON mission_control.mission_revision (installation_id, application_id, tenant_id, mission_id);
CREATE INDEX mission_revision_definition_snapshot_id_fk_idx ON mission_control.mission_revision (installation_id, application_id, tenant_id, definition_snapshot_id);
CREATE INDEX mission_revision_parent_revision_id_fk_idx ON mission_control.mission_revision (installation_id, application_id, tenant_id, parent_revision_id);
CREATE TRIGGER mission_revision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.mission_revision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.revision_proposal (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    proposal_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    base_revision_id uuid,
    definition_snapshot_id uuid NOT NULL,
    change_set jsonb NOT NULL CHECK (jsonb_typeof(change_set) = 'object'),
    lifecycle text NOT NULL CHECK (lifecycle IN ('proposed', 'validating', 'awaiting_review', 'resolved')),
    resolution text CHECK (resolution IN ('accepted', 'rejected', 'stale', 'withdrawn')),
    validation_ref text CHECK (validation_ref <> ''),
    impact_ref text CHECK (impact_ref <> ''),
    review_task_ref text CHECK (review_task_ref <> ''),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    CHECK ((lifecycle = 'resolved') = (resolution IS NOT NULL)),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, proposal_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, base_revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, definition_snapshot_id) REFERENCES mission_control.definition_snapshot (installation_id, application_id, tenant_id, definition_snapshot_id)
);
CREATE INDEX revision_proposal_mission_id_fk_idx ON mission_control.revision_proposal (installation_id, application_id, tenant_id, mission_id);
CREATE INDEX revision_proposal_base_revision_id_fk_idx ON mission_control.revision_proposal (installation_id, application_id, tenant_id, base_revision_id);
CREATE INDEX revision_proposal_definition_snapshot_id_fk_idx ON mission_control.revision_proposal (installation_id, application_id, tenant_id, definition_snapshot_id);

CREATE TABLE mission_control.compiled_program (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    compiled_program_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    compiler_version text NOT NULL CHECK (compiler_version <> ''),
    program_schema_version text NOT NULL CHECK (program_schema_version <> ''),
    program jsonb NOT NULL CHECK (jsonb_typeof(program) = 'object'),
    program_digest text NOT NULL CHECK (program_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, compiled_program_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX compiled_program_revision_id_fk_idx ON mission_control.compiled_program (installation_id, application_id, tenant_id, revision_id);
CREATE TRIGGER compiled_program_immutable
    BEFORE UPDATE OR DELETE ON mission_control.compiled_program
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.program_node (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    program_node_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    node_key text NOT NULL CHECK (node_key <> ''),
    parent_node_key text CHECK (parent_node_key <> ''),
    behavior_kind text NOT NULL CHECK (behavior_kind IN ('stage_graph', 'goal_directed', 'stage', 'goal_iteration', 'operation', 'gate', 'wait', 'join')),
    definition_digest text NOT NULL CHECK (definition_digest ~ '^sha256:[0-9a-f]{64}$'),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    node_definition jsonb NOT NULL CHECK (jsonb_typeof(node_definition) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, program_node_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, node_key),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, program_node_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, parent_node_key) REFERENCES mission_control.program_node (installation_id, application_id, tenant_id, revision_id, node_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX program_node_revision_id_fk_idx ON mission_control.program_node (installation_id, application_id, tenant_id, revision_id);
CREATE TRIGGER program_node_immutable
    BEFORE UPDATE OR DELETE ON mission_control.program_node
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.goal (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    goal_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    goal_key text NOT NULL CHECK (goal_key <> ''),
    description text NOT NULL CHECK (description <> ''),
    importance integer NOT NULL CHECK (importance BETWEEN 0 AND 100),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, goal_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, goal_key),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, goal_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX goal_revision_id_fk_idx ON mission_control.goal (installation_id, application_id, tenant_id, revision_id);
CREATE TRIGGER goal_immutable
    BEFORE UPDATE OR DELETE ON mission_control.goal
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.objective (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    objective_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    objective_key text NOT NULL CHECK (objective_key <> ''),
    goal_id uuid NOT NULL,
    parent_objective_id uuid,
    description text NOT NULL CHECK (description <> ''),
    CHECK (parent_objective_id IS DISTINCT FROM objective_id),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, objective_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, objective_key),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, objective_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, goal_id) REFERENCES mission_control.goal (installation_id, application_id, tenant_id, revision_id, goal_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, parent_objective_id) REFERENCES mission_control.objective (installation_id, application_id, tenant_id, revision_id, objective_id)
);
CREATE TRIGGER objective_immutable
    BEFORE UPDATE OR DELETE ON mission_control.objective
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.success_criterion (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    criterion_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    goal_id uuid NOT NULL,
    criterion_key text NOT NULL CHECK (criterion_key <> ''),
    description text NOT NULL CHECK (description <> ''),
    criterion_contract jsonb NOT NULL CHECK (jsonb_typeof(criterion_contract) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, criterion_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id, criterion_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, goal_id) REFERENCES mission_control.goal (installation_id, application_id, tenant_id, revision_id, goal_id)
);
CREATE TRIGGER success_criterion_immutable
    BEFORE UPDATE OR DELETE ON mission_control.success_criterion
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.node_objective (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    node_objective_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    program_node_id uuid NOT NULL,
    objective_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, node_objective_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, program_node_id, objective_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, program_node_id) REFERENCES mission_control.program_node (installation_id, application_id, tenant_id, revision_id, program_node_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id, objective_id) REFERENCES mission_control.objective (installation_id, application_id, tenant_id, revision_id, objective_id)
);
CREATE TRIGGER node_objective_immutable
    BEFORE UPDATE OR DELETE ON mission_control.node_objective
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'authoring_session',
        'mission',
        'mission_draft',
        'definition_snapshot',
        'mission_revision',
        'revision_proposal',
        'compiled_program',
        'program_node',
        'goal',
        'objective',
        'success_criterion',
        'node_objective'
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

REVOKE ALL ON mission_control.authoring_session, mission_control.mission, mission_control.mission_draft, mission_control.definition_snapshot, mission_control.mission_revision, mission_control.revision_proposal, mission_control.compiled_program, mission_control.program_node, mission_control.goal, mission_control.objective, mission_control.success_criterion, mission_control.node_objective FROM PUBLIC;
