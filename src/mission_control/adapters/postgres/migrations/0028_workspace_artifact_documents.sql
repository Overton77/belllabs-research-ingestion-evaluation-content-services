-- Transitional existing-runtime persistence, not the canonical common mission_control schema.
-- Additive only: existing Mongo records require separately verified, scoped backfill.
CREATE TABLE belllabs_control.workspace_manifests (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    namespace_id text NOT NULL,
    workspace_id text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    manifest_id text NOT NULL,
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    prior_manifest_digest text,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, namespace_id, workspace_id, revision),
    UNIQUE (request_scope, manifest_id),
    CHECK ((revision = 1) = (prior_manifest_digest IS NULL)),
    CHECK (payload->>'namespace_id' = namespace_id AND payload->>'workspace_id' = workspace_id
        AND payload->>'manifest_id' = manifest_id
        AND (payload->>'revision')::integer = revision
        AND payload->>'manifest_digest' = manifest_digest)
);
CREATE TABLE belllabs_control.workspace_slot_reservations (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    namespace_id text NOT NULL,
    logical_path text NOT NULL,
    workspace_id text NOT NULL,
    owner_id text NOT NULL,
    reservation_token text NOT NULL,
    reserved_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, namespace_id, logical_path)
);
CREATE TABLE belllabs_control.artifact_metadata_revisions (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    artifact_id text NOT NULL,
    intent_key text NOT NULL,
    promotion_id text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    state text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, artifact_id, revision),
    UNIQUE (request_scope, intent_key, revision),
    UNIQUE (request_scope, promotion_id, revision),
    CHECK (payload->>'request_scope' = request_scope AND payload->>'artifact_id' = artifact_id
        AND payload->>'intent_key' = intent_key AND payload->>'promotion_id' = promotion_id
        AND (payload->>'revision')::integer = revision AND payload->>'state' = state)
);
DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'workspace_manifests', 'workspace_slot_reservations', 'artifact_metadata_revisions'
    ] LOOP
        EXECUTE format('ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE format('ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE format(
            'CREATE POLICY request_scope ON belllabs_control.%I '
            'USING (request_scope = current_setting(''belllabs.request_scope'', true)) '
            'WITH CHECK (request_scope = current_setting(''belllabs.request_scope'', true))',
            table_name);
        EXECUTE format('GRANT SELECT, INSERT ON belllabs_control.%I TO belllabs_control_runtime',
            table_name);
        EXECUTE format('GRANT SELECT ON belllabs_control.%I TO belllabs_operations_readonly',
            table_name);
        EXECUTE format('CREATE TRIGGER append_only BEFORE UPDATE OR DELETE '
            'ON belllabs_control.%I FOR EACH ROW '
            'EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation()', table_name);
    END LOOP;
END;
$$;
