-- Transitional PostgreSQL replacement for existing immutable execution documents.
-- NOT the canonical common mission_control component; its SQL owner remains
-- ai-engineer-db-contract. Fresh executions only; no Mongo history migration.
CREATE TABLE belllabs_control.immutable_documents (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    contract text NOT NULL CHECK (contract IN (
        'goal.revision/1', 'goal.iteration/1', 'goal.handoff/1',
        'goal.verification/1', 'goal.template/1', 'stagegraph.template/1',
        'operation.binding/1', 'operation.binding-index/1',
        'operation.settlement/1', 'operation.claim/1'
    )),
    identity text NOT NULL CHECK (identity <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    digest text NOT NULL CHECK (digest ~ '^sha256:[a-f0-9]{64}$'),
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, contract, identity)
);
ALTER TABLE belllabs_control.immutable_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.immutable_documents FORCE ROW LEVEL SECURITY;
CREATE POLICY immutable_documents_scope ON belllabs_control.immutable_documents
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
GRANT SELECT, INSERT ON belllabs_control.immutable_documents TO belllabs_control_runtime;
GRANT SELECT ON belllabs_control.immutable_documents TO belllabs_operations_readonly;

CREATE FUNCTION belllabs_control.reject_immutable_document_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'immutable runtime documents cannot be updated or deleted';
END;
$$;
CREATE TRIGGER immutable_documents_append_only
    BEFORE UPDATE OR DELETE ON belllabs_control.immutable_documents
    FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
