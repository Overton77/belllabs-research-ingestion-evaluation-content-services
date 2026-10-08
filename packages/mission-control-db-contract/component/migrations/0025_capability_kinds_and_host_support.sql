-- mission-control-db-contract common release: agent-composition capability kinds
-- (ADR-0023, SPEC-01, FT-A1). Additive only: asset_version gains the five
-- agent-composition literals plus 'middleware' (which no longer maps to 'hook'), a
-- provider-neutral host_support matrix and named secret_refs; plugins record their exact
-- member pins in capability_plugin_member, and a plugin can be admitted only while every
-- member is admitted. The old literals 'skill', 'tool' and 'hook' stay valid for existing
-- rows; a later migration retires them once no row carries one.

ALTER TABLE mission_control.asset_version DROP CONSTRAINT asset_version_kind_check;
ALTER TABLE mission_control.asset_version ADD CONSTRAINT asset_version_kind_check
    CHECK (kind IN (
        'skill_bundle', 'mcp_server', 'mcp_tool', 'hook_script', 'subagent_profile', 'plugin',
        'blueprint', 'profile', 'schema', 'policy', 'workflow_template', 'operation_binding',
        'middleware', 'model_route',
        -- legacy literals kept for rows written before 0025
        'skill', 'tool', 'hook'));

-- mc.capability_host_support.v1: profiles is an object keyed by the five lane profile ids;
-- each entry has a status and an optional overlay object.
CREATE FUNCTION mission_control.capability_host_support_valid(document jsonb)
RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT document->>'schema_version' = 'mc.capability_host_support.v1'
       AND jsonb_typeof(document->'profiles') = 'object'
       AND NOT EXISTS (
           SELECT 1
             FROM jsonb_each(document->'profiles') AS profile(lane, entry)
            WHERE profile.lane NOT IN
                      ('deep_agents', 'cursor_local', 'cursor_cloud', 'claude_agent_sdk', 'codex')
               OR jsonb_typeof(profile.entry) <> 'object'
               OR coalesce(profile.entry->>'status', '') NOT IN
                      ('supported', 'unsupported', 'unqualified')
               OR (profile.entry ? 'overlay'
                   AND jsonb_typeof(profile.entry->'overlay') <> 'object'))
$$;
REVOKE ALL ON FUNCTION mission_control.capability_host_support_valid(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mission_control.capability_host_support_valid(jsonb)
    TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker,
       mission_control_readonly, mission_control_family_writer;

-- Secret references are names the lane injects through its native mechanism, never values.
CREATE FUNCTION mission_control.capability_secret_refs_valid(refs text[])
RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT coalesce(bool_and(ref ~ '^[A-Z][A-Z0-9_]*$'), true)
       AND count(*) = count(DISTINCT ref)
      FROM unnest(refs) AS ref
$$;
REVOKE ALL ON FUNCTION mission_control.capability_secret_refs_valid(text[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mission_control.capability_secret_refs_valid(text[])
    TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker,
       mission_control_readonly, mission_control_family_writer;

ALTER TABLE mission_control.asset_version
    ADD COLUMN host_support jsonb NOT NULL
        DEFAULT '{"schema_version":"mc.capability_host_support.v1","profiles":{}}'::jsonb,
    ADD COLUMN secret_refs text[] NOT NULL DEFAULT '{}'::text[];
ALTER TABLE mission_control.asset_version
    ADD CONSTRAINT asset_version_host_support_check
        CHECK (mission_control.capability_host_support_valid(host_support)),
    ADD CONSTRAINT asset_version_secret_refs_check
        CHECK (mission_control.capability_secret_refs_valid(secret_refs));

-- host_support and secret_refs are part of the immutable published row, like the manifest.
-- (asset_version_transition_guard from 0020 predates these columns and is left unchanged.)
CREATE FUNCTION mission_control.asset_version_capability_core_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.host_support IS DISTINCT FROM OLD.host_support
       OR NEW.secret_refs IS DISTINCT FROM OLD.secret_refs THEN
        RAISE EXCEPTION 'asset version host_support and secret_refs are immutable'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.asset_version_capability_core_guard() FROM PUBLIC;
CREATE TRIGGER asset_version_capability_core_guard
    BEFORE UPDATE ON mission_control.asset_version
    FOR EACH ROW EXECUTE FUNCTION mission_control.asset_version_capability_core_guard();

-- Plugin expansion: one row per member, in position order, pinning the exact member
-- version and manifest digest. Rows are immutable; a new plugin version records anew.
CREATE TABLE mission_control.capability_plugin_member (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    plugin_asset_id text NOT NULL CHECK (plugin_asset_id <> ''),
    plugin_version text NOT NULL CHECK (plugin_version <> ''),
    position integer NOT NULL CHECK (position >= 0),
    member_asset_id text NOT NULL CHECK (member_asset_id <> ''),
    member_version text NOT NULL CHECK (member_version <> ''),
    member_digest text NOT NULL CHECK (member_digest ~ '^sha256:[0-9a-f]{64}$'),
    role text NOT NULL
        CHECK (role IN ('skill', 'mcp_server', 'hook', 'subagent', 'prompt', 'resource')),
    optional boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    PRIMARY KEY (installation_id, application_id, plugin_asset_id, plugin_version, position),
    UNIQUE (installation_id, application_id, plugin_asset_id, plugin_version, member_asset_id),
    CHECK ((plugin_asset_id, plugin_version) IS DISTINCT FROM (member_asset_id, member_version)),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id),
    FOREIGN KEY (installation_id, application_id, plugin_asset_id, plugin_version)
        REFERENCES mission_control.asset_version (installation_id, application_id, asset_id, version),
    FOREIGN KEY (installation_id, application_id, member_asset_id, member_version)
        REFERENCES mission_control.asset_version (installation_id, application_id, asset_id, version)
);
CREATE INDEX capability_plugin_member_member_idx ON mission_control.capability_plugin_member
    (installation_id, application_id, member_asset_id, member_version);
CREATE TRIGGER capability_plugin_member_immutable
    BEFORE UPDATE OR DELETE ON mission_control.capability_plugin_member
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- A member row must name a plugin row and pin the member's exact manifest digest.
CREATE FUNCTION mission_control.capability_plugin_member_shape_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM mission_control.asset_version plugin
         WHERE plugin.installation_id = NEW.installation_id
           AND plugin.application_id = NEW.application_id
           AND plugin.asset_id = NEW.plugin_asset_id
           AND plugin.version = NEW.plugin_version
           AND plugin.kind = 'plugin') THEN
        RAISE EXCEPTION 'capability_plugin_member must reference a plugin asset version'
            USING ERRCODE = 'check_violation';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM mission_control.asset_version member
         WHERE member.installation_id = NEW.installation_id
           AND member.application_id = NEW.application_id
           AND member.asset_id = NEW.member_asset_id
           AND member.version = NEW.member_version
           AND member.manifest_digest = NEW.member_digest) THEN
        RAISE EXCEPTION 'plugin member digest does not match the member asset version'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.capability_plugin_member_shape_guard() FROM PUBLIC;
CREATE TRIGGER capability_plugin_member_shape_guard
    BEFORE INSERT ON mission_control.capability_plugin_member
    FOR EACH ROW EXECUTE FUNCTION mission_control.capability_plugin_member_shape_guard();

-- Plugin admission: checked at commit (deferred), so a plugin and its member rows can be
-- written in one transaction in any order. An admitted plugin needs at least one recorded
-- member, and every member must itself be admitted.
CREATE FUNCTION mission_control.plugin_admission_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    v_installation uuid := NEW.installation_id;
    v_application text := NEW.application_id;
    v_asset text;
    v_version text;
    v_status text;
BEGIN
    IF TG_TABLE_NAME = 'asset_version' THEN
        v_asset := NEW.asset_id;
        v_version := NEW.version;
    ELSE
        v_asset := NEW.plugin_asset_id;
        v_version := NEW.plugin_version;
    END IF;
    SELECT plugin.status INTO v_status
      FROM mission_control.asset_version plugin
     WHERE plugin.installation_id = v_installation
       AND plugin.application_id = v_application
       AND plugin.asset_id = v_asset
       AND plugin.version = v_version
       AND plugin.kind = 'plugin';
    IF v_status IS DISTINCT FROM 'admitted' THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM mission_control.capability_plugin_member pm
         WHERE pm.installation_id = v_installation
           AND pm.application_id = v_application
           AND pm.plugin_asset_id = v_asset
           AND pm.plugin_version = v_version) THEN
        RAISE EXCEPTION 'plugin % @ % cannot be admitted without recorded members',
            v_asset, v_version USING ERRCODE = 'check_violation';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM mission_control.capability_plugin_member pm
          JOIN mission_control.asset_version member
            ON member.installation_id = pm.installation_id
           AND member.application_id = pm.application_id
           AND member.asset_id = pm.member_asset_id
           AND member.version = pm.member_version
         WHERE pm.installation_id = v_installation
           AND pm.application_id = v_application
           AND pm.plugin_asset_id = v_asset
           AND pm.plugin_version = v_version
           AND member.status <> 'admitted') THEN
        RAISE EXCEPTION 'plugin % @ % cannot be admitted while a member is not admitted',
            v_asset, v_version USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.plugin_admission_guard() FROM PUBLIC;
CREATE CONSTRAINT TRIGGER asset_version_plugin_admission
    AFTER INSERT OR UPDATE OF status ON mission_control.asset_version
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.kind = 'plugin' AND NEW.status = 'admitted')
    EXECUTE FUNCTION mission_control.plugin_admission_guard();
CREATE CONSTRAINT TRIGGER capability_plugin_member_admission
    AFTER INSERT ON mission_control.capability_plugin_member
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION mission_control.plugin_admission_guard();

ALTER TABLE mission_control.capability_plugin_member ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.capability_plugin_member FORCE ROW LEVEL SECURITY;
CREATE POLICY capability_plugin_member_scope ON mission_control.capability_plugin_member
    USING (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
       AND application_id = mission_control.ctx_application_id());

REVOKE ALL ON mission_control.capability_plugin_member FROM PUBLIC;
GRANT SELECT, INSERT ON mission_control.capability_plugin_member
    TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT ON mission_control.capability_plugin_member
    TO mission_control_outbox_worker, mission_control_readonly;
