-- mission-control-db-contract common release: whole-directory skill bundle admission
-- (catalog/artifacts lane; port of capability_bundle_admissions). The canonical
-- authority is asset_version (asset_id 'skill-bundle:<logical_id>', kind 'skill',
-- contract 'mission-control.skill-bundle.v1', manifest_digest = bundle manifest digest)
-- plus an 'admit' asset_decision. This support row pins the exact verified byte
-- identities and storage namespace. Bytes are verified before admission; this
-- component never uploads, overwrites or deletes storage objects.

CREATE TABLE mission_control.capability_bundle_admission (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    bundle_admission_id uuid PRIMARY KEY,
    asset_version_id uuid NOT NULL,
    skill_logical_id text NOT NULL CHECK (skill_logical_id <> ''),
    bundle_version bigint NOT NULL CHECK (bundle_version > 0),
    definition_digest text NOT NULL CHECK (definition_digest ~ '^sha256:[0-9a-f]{64}$'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    bundle_digest text NOT NULL CHECK (bundle_digest ~ '^sha256:[0-9a-f]{64}$'),
    skill_md_digest text NOT NULL CHECK (skill_md_digest ~ '^sha256:[0-9a-f]{64}$'),
    storage_namespace text NOT NULL CHECK (storage_namespace <> ''),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    registered_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((manifest->>'schema_version' = 'mission-control.skill-bundle.v1'
        AND manifest->>'asset_id' = skill_logical_id
        AND (manifest->>'version')::bigint = bundle_version
        AND manifest->>'bundle_digest' = bundle_digest
        AND jsonb_typeof(manifest->'files') = 'array') IS TRUE),
    UNIQUE (installation_id, application_id, bundle_admission_id),
    UNIQUE (installation_id, application_id, skill_logical_id, bundle_version),
    UNIQUE (installation_id, application_id, asset_version_id),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id),
    FOREIGN KEY (installation_id, application_id, asset_version_id)
        REFERENCES mission_control.asset_version (installation_id, application_id, asset_version_id)
);
CREATE TRIGGER capability_bundle_admission_immutable
    BEFORE UPDATE OR DELETE ON mission_control.capability_bundle_admission
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

ALTER TABLE mission_control.capability_bundle_admission ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.capability_bundle_admission FORCE ROW LEVEL SECURITY;
CREATE POLICY capability_bundle_admission_scope ON mission_control.capability_bundle_admission
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id());

REVOKE ALL ON mission_control.capability_bundle_admission FROM PUBLIC;
-- Registration runs from the catalog writer (operator tooling) and, as before, from the
-- runtime pool used by the registration script; workers only read.
GRANT SELECT, INSERT ON mission_control.capability_bundle_admission
    TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT ON mission_control.capability_bundle_admission TO mission_control_readonly;
