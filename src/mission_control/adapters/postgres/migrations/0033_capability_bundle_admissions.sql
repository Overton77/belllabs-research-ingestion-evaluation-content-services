-- Transitional immutable bundle admissions, not the unreleased common MC component.
-- Bytes are verified before admission. This migration never uploads/deletes storage objects.
CREATE TABLE belllabs_control.capability_bundle_admissions (
    catalog_scope text NOT NULL CHECK (catalog_scope <> ''),
    asset_id text NOT NULL CHECK (asset_id <> ''),
    version bigint NOT NULL CHECK (version > 0),
    definition_digest text NOT NULL CHECK (definition_digest ~ '^sha256:[a-f0-9]{64}$'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    bundle_digest text NOT NULL CHECK (bundle_digest ~ '^sha256:[a-f0-9]{64}$'),
    skill_md_digest text NOT NULL CHECK (skill_md_digest ~ '^sha256:[a-f0-9]{64}$'),
    storage_namespace text NOT NULL CHECK (storage_namespace <> ''),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (catalog_scope, asset_id, version),
    CHECK ((manifest->>'schema_version' = 'mission-control.skill-bundle.v1'
        AND manifest->>'asset_id' = asset_id
        AND (manifest->>'version')::bigint = version
        AND manifest->>'bundle_digest' = bundle_digest
        AND jsonb_typeof(manifest->'files') = 'array') IS TRUE)
);
ALTER TABLE belllabs_control.capability_bundle_admissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.capability_bundle_admissions FORCE ROW LEVEL SECURITY;
CREATE POLICY capability_bundle_catalog_scope
    ON belllabs_control.capability_bundle_admissions
    USING (catalog_scope = current_setting('belllabs.catalog_scope', true))
    WITH CHECK (catalog_scope = current_setting('belllabs.catalog_scope', true));
GRANT SELECT, INSERT ON belllabs_control.capability_bundle_admissions
    TO belllabs_control_runtime;
GRANT SELECT ON belllabs_control.capability_bundle_admissions TO belllabs_operations_readonly;
CREATE TRIGGER capability_bundle_admissions_append_only
    BEFORE UPDATE OR DELETE ON belllabs_control.capability_bundle_admissions
    FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
