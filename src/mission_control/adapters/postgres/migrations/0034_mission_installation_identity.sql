-- Explicit transitional installation identity. This is not the unreleased common
-- mission_control component schema or its release attestation. Startup reads only.
CREATE TABLE belllabs_control.mission_installation_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    installation_id uuid NOT NULL UNIQUE,
    application_id text NOT NULL CHECK (application_id ~ '^[a-z][a-z0-9-]{0,62}$'),
    project_ref text NOT NULL CHECK (project_ref <> ''),
    component_version text NOT NULL CHECK (component_version = 'transitional-local-v1'),
    database_name text NOT NULL CHECK (database_name <> ''),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON belllabs_control.mission_installation_identity FROM PUBLIC;
GRANT SELECT ON belllabs_control.mission_installation_identity
    TO belllabs_control_runtime, belllabs_operations_readonly;
GRANT SELECT ON belllabs_control.schema_migrations TO belllabs_control_runtime;
CREATE TRIGGER mission_installation_identity_immutable
    BEFORE UPDATE OR DELETE ON belllabs_control.mission_installation_identity
    FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
