-- Transitional, immutable candidate custody descriptors for PostgreSQL workers.
-- Bytes remain in the configured artifact payload store; no Mongo dependency.
CREATE TABLE belllabs_control.workspace_candidate_descriptors (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    candidate_id text NOT NULL,
    namespace_id text NOT NULL,
    workspace_id text NOT NULL,
    logical_path text NOT NULL,
    descriptor_contract text NOT NULL DEFAULT 'CapturedWorkspaceCandidate/1'
        CHECK (descriptor_contract = 'CapturedWorkspaceCandidate/1'),
    descriptor jsonb NOT NULL CHECK (jsonb_typeof(descriptor) = 'object'),
    descriptor_digest text NOT NULL CHECK (descriptor_digest ~ '^sha256:[a-f0-9]{64}$'),
    object_ref text NOT NULL,
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[a-f0-9]{64}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (request_scope, candidate_id)
);
CREATE INDEX workspace_candidate_path ON belllabs_control.workspace_candidate_descriptors
    (request_scope, namespace_id, workspace_id, logical_path, recorded_at DESC);
ALTER TABLE belllabs_control.workspace_candidate_descriptors ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.workspace_candidate_descriptors FORCE ROW LEVEL SECURITY;
CREATE POLICY workspace_candidate_scope ON belllabs_control.workspace_candidate_descriptors
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
GRANT SELECT, INSERT ON belllabs_control.workspace_candidate_descriptors TO belllabs_control_runtime;
GRANT SELECT ON belllabs_control.workspace_candidate_descriptors TO belllabs_operations_readonly;
CREATE TRIGGER workspace_candidate_append_only
    BEFORE UPDATE OR DELETE ON belllabs_control.workspace_candidate_descriptors
    FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
