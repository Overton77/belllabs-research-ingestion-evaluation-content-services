-- mission-control-db-contract common release: workspace and artifact custody support
-- records (catalog/artifacts lane; ports of legacy 0003/0028/0031/0035). Tenant scoped:
-- composite (installation_id, application_id, tenant_id) FK to tenant, ENABLE + FORCE
-- RLS with the three-column context policy, append-only triggers. Durable artifact
-- references are canonical mission_control.artifact rows (registered once) whose
-- 'artifact.admitted' event is a canonical mission_control.outbox row in the same commit.

-- A promotion and a durable reference identify exactly one registered artifact.
CREATE UNIQUE INDEX artifact_promotion_key_idx ON mission_control.artifact
    (installation_id, application_id, tenant_id, (detail->>'promotion_id'))
    WHERE detail ? 'promotion_id';
CREATE UNIQUE INDEX artifact_durable_reference_idx ON mission_control.artifact
    (installation_id, application_id, tenant_id, (detail->>'durable_reference'))
    WHERE detail ? 'durable_reference';

-- Immutable promotion metadata history (port of artifact_metadata_revisions). The
-- stored request_scope must equal the row's own composite scope.
CREATE TABLE mission_control.artifact_metadata_revision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    artifact_metadata_revision_id uuid PRIMARY KEY,
    request_scope text NOT NULL,
    artifact_key text NOT NULL CHECK (artifact_key <> ''),
    intent_key text NOT NULL CHECK (intent_key <> ''),
    promotion_key text NOT NULL CHECK (promotion_key <> ''),
    revision integer NOT NULL CHECK (revision > 0),
    state text NOT NULL CHECK (state <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (request_scope = 'mc/' || installation_id::text || '/' || application_id || '/'
        || tenant_id::text),
    CHECK ((payload->>'request_scope' = request_scope
        AND payload->>'artifact_id' = artifact_key
        AND payload->>'intent_key' = intent_key
        AND payload->>'promotion_id' = promotion_key
        AND payload->'revision' = to_jsonb(revision)
        AND payload->>'state' = state) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, artifact_metadata_revision_id),
    UNIQUE (installation_id, application_id, tenant_id, artifact_key, revision),
    UNIQUE (installation_id, application_id, tenant_id, intent_key, revision),
    UNIQUE (installation_id, application_id, tenant_id, promotion_key, revision),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

-- Immutable workspace manifest lineage (port of workspace_manifests).
CREATE TABLE mission_control.workspace_manifest (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    workspace_manifest_id uuid PRIMARY KEY,
    namespace_key text NOT NULL CHECK (namespace_key <> ''),
    workspace_key text NOT NULL CHECK (workspace_key <> ''),
    revision integer NOT NULL CHECK (revision > 0),
    manifest_key text NOT NULL CHECK (manifest_key <> ''),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    prior_manifest_digest text CHECK (prior_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    manifest_created_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((revision = 1) = (prior_manifest_digest IS NULL)),
    CHECK ((payload->>'namespace_id' = namespace_key
        AND payload->>'workspace_id' = workspace_key
        AND payload->>'manifest_id' = manifest_key
        AND payload->'revision' = to_jsonb(revision)
        AND payload->>'manifest_digest' = manifest_digest
        AND payload ? 'prior_manifest_digest'
        AND (payload->>'prior_manifest_digest') IS NOT DISTINCT FROM prior_manifest_digest
    ) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, workspace_manifest_id),
    UNIQUE (installation_id, application_id, tenant_id, namespace_key, workspace_key, revision),
    UNIQUE (installation_id, application_id, tenant_id, manifest_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

-- Immutable exclusive-write slot ownership (port of workspace_slot_reservations).
CREATE TABLE mission_control.workspace_slot_reservation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    slot_reservation_id uuid PRIMARY KEY,
    namespace_key text NOT NULL CHECK (namespace_key <> ''),
    logical_path text NOT NULL CHECK (logical_path <> ''),
    workspace_key text NOT NULL CHECK (workspace_key <> ''),
    owner_key text NOT NULL CHECK (owner_key <> ''),
    reservation_token text NOT NULL CHECK (reservation_token <> ''),
    reserved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, slot_reservation_id),
    UNIQUE (installation_id, application_id, tenant_id, namespace_key, logical_path),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

-- Immutable captured workspace candidate descriptors (port of
-- workspace_candidate_descriptors); bytes stay in the configured artifact payload store.
-- A captured file is not an attributable completion proposal (no activation/attempt),
-- so it is not written to completion_candidate.
CREATE TABLE mission_control.workspace_candidate_descriptor (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    candidate_descriptor_id uuid PRIMARY KEY,
    candidate_key text NOT NULL CHECK (candidate_key <> ''),
    namespace_key text NOT NULL CHECK (namespace_key <> ''),
    workspace_key text NOT NULL CHECK (workspace_key <> ''),
    logical_path text NOT NULL CHECK (logical_path <> ''),
    descriptor_contract text NOT NULL CHECK (descriptor_contract = 'CapturedWorkspaceCandidate/1'),
    descriptor jsonb NOT NULL CHECK (jsonb_typeof(descriptor) = 'object'),
    descriptor_digest text NOT NULL CHECK (descriptor_digest ~ '^sha256:[0-9a-f]{64}$'),
    object_ref text NOT NULL CHECK (object_ref <> ''),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((descriptor->>'candidate_id' = candidate_key
        AND descriptor->>'namespace_id' = namespace_key
        AND descriptor->>'workspace_id' = workspace_key
        AND descriptor->>'logical_path' = logical_path
        AND descriptor->>'content_digest' = content_digest
        AND descriptor->'size_bytes' = to_jsonb(size_bytes)) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, candidate_descriptor_id),
    UNIQUE (installation_id, application_id, tenant_id, candidate_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX workspace_candidate_descriptor_path_idx
    ON mission_control.workspace_candidate_descriptor
    (installation_id, application_id, tenant_id, namespace_key, workspace_key, logical_path,
     recorded_at DESC);

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'artifact_metadata_revision',
        'workspace_manifest',
        'workspace_slot_reservation',
        'workspace_candidate_descriptor'
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
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON mission_control.%I '
            'FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation()',
            table_name || '_immutable', table_name);
        EXECUTE pg_catalog.format('REVOKE ALL ON mission_control.%I FROM PUBLIC', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT, INSERT ON mission_control.%I TO mission_control_runtime', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT ON mission_control.%I TO mission_control_readonly', table_name);
    END LOOP;
END
$policies$;
