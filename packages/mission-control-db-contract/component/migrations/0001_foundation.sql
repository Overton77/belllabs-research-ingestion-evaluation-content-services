-- mission-control-db-contract common release: foundation.
-- Source owner: mission-control-db-contract (mission-control/packages/). Identical
-- bytes are installed in every application project. No domain schema, app helper,
-- auth.users, Neo4j or Mongo dependency is permitted in this component.

CREATE SCHEMA mission_control;
CREATE SCHEMA mission_control_search;
REVOKE ALL ON SCHEMA mission_control FROM PUBLIC;
REVOKE ALL ON SCHEMA mission_control_search FROM PUBLIC;

-- NOLOGIN capability roles. Login identities are granted membership separately,
-- only after an explicit access-expansion approval. An existing same-named role
-- must already be a restricted NOLOGIN role; anything else holds the install.
DO $roles$
DECLARE
    role_name text;
    existing record;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'mission_control_runtime',
        'mission_control_family_writer',
        'mission_control_catalog_writer',
        'mission_control_outbox_worker',
        'mission_control_readonly'
    ] LOOP
        SELECT rolsuper, rolbypassrls, rolcanlogin, rolcreaterole, rolcreatedb
          INTO existing FROM pg_catalog.pg_roles WHERE rolname = role_name;
        IF FOUND THEN
            IF existing.rolsuper OR existing.rolbypassrls OR existing.rolcanlogin
               OR existing.rolcreaterole OR existing.rolcreatedb THEN
                RAISE EXCEPTION 'existing role % is not a restricted NOLOGIN role', role_name;
            END IF;
            -- A capability role must not inherit or SET ROLE into any other authority.
            IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_auth_members m
                JOIN pg_catalog.pg_roles r ON r.oid = m.member
                WHERE r.rolname = role_name
            ) THEN
                RAISE EXCEPTION 'existing role % is a member of another role', role_name;
            END IF;
        ELSE
            EXECUTE pg_catalog.format(
                'CREATE ROLE %I NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT',
                role_name
            );
        END IF;
    END LOOP;
END
$roles$;

GRANT USAGE ON SCHEMA mission_control TO
    mission_control_runtime, mission_control_family_writer, mission_control_catalog_writer,
    mission_control_outbox_worker, mission_control_readonly;
GRANT USAGE ON SCHEMA mission_control_search TO
    mission_control_runtime, mission_control_catalog_writer, mission_control_readonly;

-- Trusted transaction-local scope. Missing or malformed context yields NULL or an
-- error, so every policy comparison fails closed.
CREATE FUNCTION mission_control.ctx_installation_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT NULLIF(pg_catalog.current_setting('mc.installation_id', true), '')::uuid
$$;
CREATE FUNCTION mission_control.ctx_application_id() RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT NULLIF(pg_catalog.current_setting('mc.application_id', true), '')
$$;
CREATE FUNCTION mission_control.ctx_tenant_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT NULLIF(pg_catalog.current_setting('mc.tenant_id', true), '')::uuid
$$;
CREATE FUNCTION mission_control.ctx_actor_ref() RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT NULLIF(pg_catalog.current_setting('mc.actor_ref', true), '')
$$;
REVOKE ALL ON FUNCTION mission_control.ctx_installation_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION mission_control.ctx_application_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION mission_control.ctx_tenant_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION mission_control.ctx_actor_ref() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
    mission_control.ctx_installation_id(), mission_control.ctx_application_id(),
    mission_control.ctx_tenant_id(), mission_control.ctx_actor_ref()
TO mission_control_runtime, mission_control_family_writer, mission_control_catalog_writer,
   mission_control_outbox_worker, mission_control_readonly;

CREATE FUNCTION mission_control.reject_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'mission_control.% is immutable', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$;
REVOKE ALL ON FUNCTION mission_control.reject_mutation() FROM PUBLIC;

-- Release receipts: one immutable row per applied migration of this component.
CREATE TABLE mission_control.component_release (
    component_version text NOT NULL CHECK (component_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
    migration_key text PRIMARY KEY CHECK (migration_key ~ '^[0-9]{4}_[a-z0-9_]+$'),
    migration_digest text NOT NULL CHECK (migration_digest ~ '^sha256:[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL,
    applied_by text NOT NULL CHECK (applied_by <> '')
);
CREATE TRIGGER component_release_immutable
    BEFORE UPDATE OR DELETE ON mission_control.component_release
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Independently versioned attestation of the release manifest and generated contract.
CREATE TABLE mission_control.release_attestation (
    component_version text PRIMARY KEY CHECK (component_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    contract_schema_digest text NOT NULL CHECK (contract_schema_digest ~ '^sha256:[0-9a-f]{64}$'),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    fingerprint_algorithm text NOT NULL CHECK (fingerprint_algorithm <> ''),
    supported_reader_versions text[] NOT NULL CHECK (cardinality(supported_reader_versions) > 0),
    supported_writer_versions text[] NOT NULL CHECK (cardinality(supported_writer_versions) > 0),
    attested_at timestamptz NOT NULL,
    attested_by text NOT NULL CHECK (attested_by <> '')
);
CREATE TRIGGER release_attestation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.release_attestation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.application_installation (
    installation_id uuid PRIMARY KEY,
    application_id text NOT NULL CHECK (application_id ~ '^[a-z][a-z0-9-]{0,62}$'),
    supabase_project_ref text NOT NULL CHECK (supabase_project_ref ~ '^[a-z0-9-]{1,63}$'),
    environment text NOT NULL
        CHECK (environment IN ('disposable', 'development', 'staging', 'production')),
    schema_component_version text NOT NULL
        CHECK (schema_component_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'disabled')),
    created_at timestamptz NOT NULL,
    UNIQUE (installation_id, application_id)
);
CREATE UNIQUE INDEX application_installation_one_active
    ON mission_control.application_installation ((true)) WHERE state = 'active';

CREATE TABLE mission_control.tenant (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid PRIMARY KEY,
    external_tenant_ref text NOT NULL CHECK (external_tenant_ref <> ''),
    state text NOT NULL CHECK (state IN ('active', 'suspended', 'retired')),
    qualification_fixture boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, external_tenant_ref),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);

CREATE TABLE mission_control.actor_binding (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    actor_binding_id uuid PRIMARY KEY,
    issuer text NOT NULL CHECK (issuer <> ''),
    subject text NOT NULL CHECK (subject <> ''),
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    actor_kind text NOT NULL CHECK (actor_kind IN ('human', 'service', 'agent')),
    state text NOT NULL CHECK (state IN ('active', 'revoked')),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, actor_binding_id),
    UNIQUE (installation_id, application_id, tenant_id, issuer, subject),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

CREATE TABLE mission_control.actor_grant (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    actor_grant_id uuid PRIMARY KEY,
    actor_binding_id uuid NOT NULL,
    scope text NOT NULL CHECK (scope <> ''),
    resource_selector text NOT NULL CHECK (resource_selector <> ''),
    policy_ref text NOT NULL CHECK (policy_ref <> ''),
    valid_from timestamptz NOT NULL,
    valid_until timestamptz,
    revoked_at timestamptz,
    revocation_reason text,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (valid_until IS NULL OR valid_until > valid_from),
    CHECK ((revoked_at IS NULL) = (revocation_reason IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, actor_grant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, actor_binding_id)
        REFERENCES mission_control.actor_binding
            (installation_id, application_id, tenant_id, actor_binding_id)
);
CREATE INDEX actor_grant_binding_idx ON mission_control.actor_grant
    (installation_id, application_id, tenant_id, actor_binding_id);

-- A revocation is terminal: grants can be revoked but never revived.
CREATE FUNCTION mission_control.actor_grant_revocation_only() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' OR OLD.revoked_at IS NOT NULL
       OR NEW.revoked_at IS NULL
       OR (NEW.installation_id, NEW.application_id, NEW.tenant_id, NEW.actor_grant_id,
           NEW.actor_binding_id, NEW.scope, NEW.resource_selector, NEW.policy_ref,
           NEW.valid_from, NEW.valid_until, NEW.created_at, NEW.created_by_actor_ref)
          IS DISTINCT FROM
          (OLD.installation_id, OLD.application_id, OLD.tenant_id, OLD.actor_grant_id,
           OLD.actor_binding_id, OLD.scope, OLD.resource_selector, OLD.policy_ref,
           OLD.valid_from, OLD.valid_until, OLD.created_at, OLD.created_by_actor_ref) THEN
        RAISE EXCEPTION 'actor grants only accept one terminal revocation'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.actor_grant_revocation_only() FROM PUBLIC;
CREATE TRIGGER actor_grant_revocation_only
    BEFORE UPDATE OR DELETE ON mission_control.actor_grant
    FOR EACH ROW EXECUTE FUNCTION mission_control.actor_grant_revocation_only();

-- Seed receipts: same key/version/digest replays; changed digest conflicts.
CREATE TABLE mission_control.installation_seed_receipt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    seed_key text NOT NULL CHECK (seed_key ~ '^mc(\.[a-z0-9-]+)+$'),
    seed_version text NOT NULL CHECK (seed_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
    seed_digest text NOT NULL CHECK (seed_digest ~ '^sha256:[0-9a-f]{64}$'),
    component_version text NOT NULL,
    record_counts jsonb NOT NULL CHECK (jsonb_typeof(record_counts) = 'object'),
    applied_at timestamptz NOT NULL,
    applied_by text NOT NULL CHECK (applied_by <> ''),
    PRIMARY KEY (installation_id, seed_key, seed_version),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);
CREATE TRIGGER installation_seed_receipt_immutable
    BEFORE UPDATE OR DELETE ON mission_control.installation_seed_receipt
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Stable logical seed keys resolve to UUIDv7 identities allocated exactly once.
CREATE TABLE mission_control.seed_identity (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    logical_key text NOT NULL CHECK (logical_key <> ''),
    record_kind text NOT NULL CHECK (record_kind ~ '^[a-z_]+$'),
    record_id uuid NOT NULL,
    first_seed_key text NOT NULL,
    first_seed_version text NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, record_kind, logical_key),
    UNIQUE (installation_id, record_id),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);
CREATE TRIGGER seed_identity_immutable
    BEFORE UPDATE OR DELETE ON mission_control.seed_identity
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Installation-scoped operator records: the seed engine binds installation/app context.
ALTER TABLE mission_control.installation_seed_receipt ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.installation_seed_receipt FORCE ROW LEVEL SECURITY;
CREATE POLICY installation_seed_receipt_scope ON mission_control.installation_seed_receipt
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id());
ALTER TABLE mission_control.seed_identity ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.seed_identity FORCE ROW LEVEL SECURITY;
CREATE POLICY seed_identity_scope ON mission_control.seed_identity
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id());

ALTER TABLE mission_control.tenant ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.tenant FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope ON mission_control.tenant
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id());

ALTER TABLE mission_control.actor_binding ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.actor_binding FORCE ROW LEVEL SECURITY;
CREATE POLICY actor_binding_scope ON mission_control.actor_binding
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id());

ALTER TABLE mission_control.actor_grant ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.actor_grant FORCE ROW LEVEL SECURITY;
CREATE POLICY actor_grant_scope ON mission_control.actor_grant
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id()
       AND tenant_id = mission_control.ctx_tenant_id());

REVOKE ALL ON ALL TABLES IN SCHEMA mission_control FROM PUBLIC;
GRANT SELECT ON mission_control.component_release, mission_control.release_attestation,
    mission_control.application_installation
TO mission_control_runtime, mission_control_family_writer, mission_control_catalog_writer,
   mission_control_outbox_worker, mission_control_readonly;
GRANT SELECT ON mission_control.tenant, mission_control.actor_binding,
    mission_control.actor_grant
TO mission_control_runtime, mission_control_family_writer, mission_control_readonly;
GRANT SELECT ON mission_control.installation_seed_receipt, mission_control.seed_identity
TO mission_control_catalog_writer, mission_control_readonly;
