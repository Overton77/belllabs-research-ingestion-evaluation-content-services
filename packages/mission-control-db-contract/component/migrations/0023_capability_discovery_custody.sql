-- mission-control-db-contract common release: external capability discovery and
-- quarantine inspection custody (catalog/artifacts lane; port of the five
-- external-* immutable_documents contracts from legacy 0038). Installation catalog
-- scoped. Discovery and inspection evidence is NOT installation, admission or execution
-- permission: there is deliberately no reference to asset_version/asset_decision or
-- capability_grant, and nothing here can make an asset selectable.

CREATE TABLE mission_control.capability_discovery_record (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    discovery_record_id uuid PRIMARY KEY,
    contract text NOT NULL CHECK (contract IN (
        'external-discovery-evidence/1', 'external-discovery-candidate/1',
        'external-inspection-workspace/1', 'external-inspection-report/1',
        'external-inspection-workspace-report/1'
    )),
    record_key text NOT NULL CHECK (record_key <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, discovery_record_id),
    -- One immutable body per identity; for the workspace-report contract this is also
    -- the "one report per inspection workspace" guarantee.
    UNIQUE (installation_id, application_id, contract, record_key),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);
CREATE TRIGGER capability_discovery_record_immutable
    BEFORE UPDATE OR DELETE ON mission_control.capability_discovery_record
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

ALTER TABLE mission_control.capability_discovery_record ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.capability_discovery_record FORCE ROW LEVEL SECURITY;
CREATE POLICY capability_discovery_record_scope ON mission_control.capability_discovery_record
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id());

REVOKE ALL ON mission_control.capability_discovery_record FROM PUBLIC;
GRANT SELECT, INSERT ON mission_control.capability_discovery_record
    TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT ON mission_control.capability_discovery_record TO mission_control_readonly;
