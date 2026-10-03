-- Transitional installation-scoped authoring catalog replacing Mongo documents.
-- Not a second editable source for the common mission_control SQL component.
CREATE TABLE belllabs_control.definition_catalog_heads (
    catalog_scope text NOT NULL CHECK (catalog_scope <> ''),
    kind text NOT NULL,
    logical_id text NOT NULL,
    published_revision bigint NOT NULL DEFAULT 0 CHECK (published_revision >= 0),
    draft_revision bigint NOT NULL DEFAULT 0 CHECK (draft_revision >= 0),
    draft jsonb,
    PRIMARY KEY (catalog_scope, kind, logical_id),
    CHECK ((draft_revision = 0 AND draft IS NULL) OR
           (draft_revision > 0 AND jsonb_typeof(draft) = 'object'))
);
CREATE TABLE belllabs_control.definition_catalog_records (
    catalog_scope text NOT NULL CHECK (catalog_scope <> ''),
    contract text NOT NULL CHECK (contract IN (
        'published-definition/1', 'definition-retirement/1', 'alias-movement/1',
        'effective-run-configuration/1', 'compilation-index/1', 'catalog-event/1'
    )),
    identity text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    PRIMARY KEY (catalog_scope, contract, identity)
);
CREATE TABLE belllabs_control.definition_catalog_aliases (
    catalog_scope text NOT NULL CHECK (catalog_scope <> ''),
    kind text NOT NULL,
    logical_id text NOT NULL,
    alias text NOT NULL,
    binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
    PRIMARY KEY (catalog_scope, kind, logical_id, alias)
);
DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'definition_catalog_heads', 'definition_catalog_records', 'definition_catalog_aliases'
    ] LOOP
        EXECUTE format('ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE format('ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE format('CREATE POLICY catalog_scope ON belllabs_control.%I USING
          (catalog_scope = current_setting(''belllabs.catalog_scope'', true)) WITH CHECK
          (catalog_scope = current_setting(''belllabs.catalog_scope'', true))', table_name);
        EXECUTE format('GRANT SELECT, INSERT ON belllabs_control.%I TO belllabs_control_runtime', table_name);
        EXECUTE format('GRANT SELECT ON belllabs_control.%I TO belllabs_operations_readonly', table_name);
    END LOOP;
END;
$$;
GRANT UPDATE ON belllabs_control.definition_catalog_heads,
    belllabs_control.definition_catalog_aliases TO belllabs_control_runtime;
CREATE TRIGGER definition_catalog_records_append_only
    BEFORE UPDATE OR DELETE ON belllabs_control.definition_catalog_records
    FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
