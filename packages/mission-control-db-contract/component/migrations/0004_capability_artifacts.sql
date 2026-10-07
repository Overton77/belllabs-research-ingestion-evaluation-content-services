-- mission-control-db-contract common release: installation catalog, execution
-- bindings and artifact custody. Catalog admission is installation scoped.

CREATE TABLE mission_control.asset_version (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    asset_version_id uuid PRIMARY KEY,
    asset_id text NOT NULL CHECK (asset_id <> ''),
    version text NOT NULL CHECK (version <> ''),
    kind text NOT NULL CHECK (kind IN ('skill', 'tool', 'mcp_server', 'plugin', 'blueprint', 'profile', 'schema', 'policy', 'workflow_template', 'operation_binding', 'hook', 'model_route')),
    contract text NOT NULL CHECK (contract <> ''),
    manifest_ref text NOT NULL CHECK (manifest_ref <> ''),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    required_compatibility text[] NOT NULL,
    status text NOT NULL CHECK (status IN ('proposed', 'admitted', 'revoked', 'retired')),
    version_no bigint NOT NULL CHECK (version_no >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, asset_version_id),
    FOREIGN KEY (installation_id, application_id) REFERENCES mission_control.application_installation (installation_id, application_id),
    UNIQUE (installation_id, application_id, asset_id, version)
);
CREATE INDEX asset_version_kind_status_idx ON mission_control.asset_version (installation_id, application_id, kind, status);

CREATE TABLE mission_control.asset_decision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    asset_decision_id uuid PRIMARY KEY,
    asset_version_id uuid NOT NULL,
    decision text NOT NULL CHECK (decision IN ('admit', 'revoke', 'retire', 'reject')),
    disposition text NOT NULL CHECK (disposition <> ''),
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    evidence_refs text[] NOT NULL,
    policy_ref text NOT NULL CHECK (policy_ref <> ''),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, asset_decision_id),
    FOREIGN KEY (installation_id, application_id) REFERENCES mission_control.application_installation (installation_id, application_id),
    FOREIGN KEY (installation_id, application_id, asset_version_id) REFERENCES mission_control.asset_version (installation_id, application_id, asset_version_id)
);
CREATE INDEX asset_decision_asset_version_id_fk_idx ON mission_control.asset_decision (installation_id, application_id, asset_version_id);
CREATE TRIGGER asset_decision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.asset_decision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.capability_grant (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    capability_grant_id uuid PRIMARY KEY,
    asset_version_id uuid NOT NULL,
    actor_selector text NOT NULL CHECK (actor_selector <> ''),
    resource_selector text NOT NULL CHECK (resource_selector <> ''),
    allowed_invocation_classes text[] NOT NULL,
    allowed_side_effect_classes text[] NOT NULL,
    ceilings jsonb NOT NULL CHECK (jsonb_typeof(ceilings) = 'object'),
    valid_from timestamptz NOT NULL,
    valid_until timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, capability_grant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, asset_version_id) REFERENCES mission_control.asset_version (installation_id, application_id, asset_version_id)
);

CREATE TABLE mission_control.execution_binding (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    execution_binding_id uuid PRIMARY KEY,
    binding_key text NOT NULL CHECK (binding_key <> ''),
    revision_id uuid,
    run_id uuid,
    program_node_id uuid,
    subordinate_id uuid,
    binding_contract text NOT NULL CHECK (binding_contract ~ '^[a-z0-9_.-]+/[0-9]+$'),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    manifest_ref text CHECK (manifest_ref <> ''),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    admission_decision text NOT NULL CHECK (admission_decision IN ('admitted', 'rejected')),
    admitted_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, execution_binding_id),
    UNIQUE (installation_id, application_id, tenant_id, binding_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id) REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_id) REFERENCES mission_control.subordinate_execution (installation_id, application_id, tenant_id, subordinate_id)
);
CREATE INDEX execution_binding_revision_id_fk_idx ON mission_control.execution_binding (installation_id, application_id, tenant_id, revision_id);
CREATE INDEX execution_binding_run_id_fk_idx ON mission_control.execution_binding (installation_id, application_id, tenant_id, run_id);
CREATE INDEX execution_binding_subordinate_id_fk_idx ON mission_control.execution_binding (installation_id, application_id, tenant_id, subordinate_id);
CREATE TRIGGER execution_binding_immutable
    BEFORE UPDATE OR DELETE ON mission_control.execution_binding
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.artifact (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    artifact_id uuid PRIMARY KEY,
    artifact_key text NOT NULL CHECK (artifact_key <> ''),
    producer_run_id uuid,
    producer_activation_id uuid,
    producer_attempt_id uuid,
    kind text NOT NULL CHECK (kind <> ''),
    schema_ref text CHECK (schema_ref <> ''),
    media_type text NOT NULL CHECK (media_type <> ''),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    byte_size bigint NOT NULL CHECK (byte_size >= 0),
    storage_kind text NOT NULL CHECK (storage_kind IN ('object_store', 'external', 'inline_document')),
    object_key text CHECK (object_key <> ''),
    external_issuer text CHECK (external_issuer <> ''),
    external_ref text CHECK (external_ref <> ''),
    artifact_version bigint NOT NULL CHECK (artifact_version >= 1),
    custody_state text NOT NULL CHECK (custody_state IN ('reserved', 'registered', 'quarantined', 'orphaned')),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, artifact_id),
    UNIQUE (installation_id, application_id, tenant_id, artifact_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, producer_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, producer_activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, producer_attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id)
);
CREATE INDEX artifact_producer_run_id_fk_idx ON mission_control.artifact (installation_id, application_id, tenant_id, producer_run_id);
CREATE INDEX artifact_producer_activation_id_fk_idx ON mission_control.artifact (installation_id, application_id, tenant_id, producer_activation_id);
CREATE INDEX artifact_producer_attempt_id_fk_idx ON mission_control.artifact (installation_id, application_id, tenant_id, producer_attempt_id);
CREATE INDEX artifact_digest_idx ON mission_control.artifact (installation_id, application_id, tenant_id, content_digest);

CREATE TABLE mission_control.artifact_relation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    artifact_relation_id uuid PRIMARY KEY,
    source_artifact_id uuid NOT NULL,
    target_artifact_id uuid NOT NULL,
    relation_kind text NOT NULL CHECK (relation_kind <> ''),
    producing_context_ref text CHECK (producing_context_ref <> ''),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, artifact_relation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, source_artifact_id, target_artifact_id, relation_kind),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_artifact_id) REFERENCES mission_control.artifact (installation_id, application_id, tenant_id, artifact_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, target_artifact_id) REFERENCES mission_control.artifact (installation_id, application_id, tenant_id, artifact_id)
);
CREATE INDEX artifact_relation_source_artifact_id_fk_idx ON mission_control.artifact_relation (installation_id, application_id, tenant_id, source_artifact_id);
CREATE INDEX artifact_relation_target_artifact_id_fk_idx ON mission_control.artifact_relation (installation_id, application_id, tenant_id, target_artifact_id);
CREATE TRIGGER artifact_relation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.artifact_relation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.evidence_assessment (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    assessment_id uuid PRIMARY KEY,
    artifact_id uuid NOT NULL,
    requirement_ref text NOT NULL CHECK (requirement_ref <> ''),
    assessor_identity text NOT NULL CHECK (assessor_identity <> ''),
    capability_version text NOT NULL CHECK (capability_version <> ''),
    rubric_version text NOT NULL CHECK (rubric_version <> ''),
    policy_version text NOT NULL CHECK (policy_version <> ''),
    disposition text NOT NULL CHECK (disposition IN ('satisfied', 'unsatisfied', 'inconclusive')),
    report_ref text CHECK (report_ref <> ''),
    report_digest text CHECK (report_digest ~ '^sha256:[0-9a-f]{64}$'),
    assessed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, assessment_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, artifact_id) REFERENCES mission_control.artifact (installation_id, application_id, tenant_id, artifact_id)
);
CREATE INDEX evidence_assessment_artifact_id_fk_idx ON mission_control.evidence_assessment (installation_id, application_id, tenant_id, artifact_id);
CREATE TRIGGER evidence_assessment_immutable
    BEFORE UPDATE OR DELETE ON mission_control.evidence_assessment
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'capability_grant',
        'execution_binding',
        'artifact',
        'artifact_relation',
        'evidence_assessment'
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
    FOREACH table_name IN ARRAY ARRAY[
        'asset_version',
        'asset_decision'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id())',
            table_name || '_scope', table_name);
    END LOOP;
END
$policies$;

REVOKE ALL ON mission_control.asset_version, mission_control.asset_decision, mission_control.capability_grant, mission_control.execution_binding, mission_control.artifact, mission_control.artifact_relation, mission_control.evidence_assessment FROM PUBLIC;

-- Additive (catalog/artifacts lane): explicit capability grants. Catalog publication and
-- admission run on the restricted runtime pool and the catalog writer; lifecycle status
-- moves only through asset_version_transition_guard (0020). Tenant artifact custody is
-- runtime-written; nothing here is granted to PUBLIC or bypasses forced RLS.
GRANT SELECT ON mission_control.asset_version, mission_control.asset_decision
TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker,
   mission_control_readonly;
GRANT INSERT ON mission_control.asset_version, mission_control.asset_decision
TO mission_control_runtime, mission_control_catalog_writer;
GRANT UPDATE (status, version_no, updated_at) ON mission_control.asset_version
TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT ON mission_control.capability_grant
TO mission_control_runtime, mission_control_family_writer, mission_control_readonly;
GRANT SELECT, INSERT ON mission_control.execution_binding, mission_control.artifact,
    mission_control.artifact_relation, mission_control.evidence_assessment
TO mission_control_runtime;
GRANT SELECT ON mission_control.execution_binding, mission_control.artifact,
    mission_control.artifact_relation, mission_control.evidence_assessment
TO mission_control_readonly;
