-- mission-control-db-contract common release: Mission Chains and authoring provenance
-- (fast-track FT-D1, SPEC-04 / SPEC-05, ADR-0029, ADR-0034). Tenant scoped: composite
-- (installation_id, application_id, tenant_id) FK to tenant, ENABLE + FORCE RLS with the
-- three-column context policy. A chain is authored in one manifest (missions + links);
-- members are whole missions with their own revision, run, budget and acceptance. The
-- chain reducer (FT-D2) moves link state forward in the same transaction as the mission
-- event that satisfies the link's release condition.

-- One authored chain. members = [{mission_key, mission_id, revision_id, order}].
CREATE TABLE mission_control.mission_chain (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    chain_id uuid PRIMARY KEY,
    chain_key text NOT NULL CHECK (chain_key ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'),
    title text NOT NULL CHECK (title <> ''),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    lifecycle text NOT NULL CHECK (lifecycle IN ('pending', 'running', 'completed')),
    phase text NOT NULL CHECK (phase IN ('releasing', 'draining', 'blocked')),
    terminal_outcome text CHECK (terminal_outcome IN
        ('accepted', 'not_accepted', 'cancelled', 'execution_failed')),
    members jsonb NOT NULL CHECK (jsonb_typeof(members) = 'array'
        AND jsonb_array_length(members) >= 2),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((lifecycle = 'completed') = (terminal_outcome IS NOT NULL)),
    UNIQUE (installation_id, application_id, tenant_id, chain_id),
    UNIQUE (installation_id, application_id, tenant_id, chain_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

-- One typed link between two member missions. release_condition is
-- {"kind": "goal_accepted", "goal_key": ...} | {"kind": "mission_accepted"} |
-- {"kind": "execution_complete"}; bindings = [{output_name, input_name, schema_ref, expand}].
CREATE TABLE mission_control.chain_link (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    link_id uuid PRIMARY KEY,
    chain_id uuid NOT NULL,
    link_key text NOT NULL CHECK (link_key <> ''),
    from_mission_key text NOT NULL CHECK (from_mission_key <> ''),
    to_mission_key text NOT NULL CHECK (to_mission_key <> ''),
    from_mission_id uuid NOT NULL,
    to_mission_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('supplies', 'depends_on')),
    bindings jsonb NOT NULL CHECK (jsonb_typeof(bindings) = 'array'),
    release_condition jsonb NOT NULL CHECK (jsonb_typeof(release_condition) = 'object'
        AND release_condition->>'kind' IN ('goal_accepted', 'mission_accepted', 'execution_complete')
        AND ((release_condition->>'kind' = 'goal_accepted')
            = (release_condition ? 'goal_key' AND release_condition->>'goal_key' <> ''))),
    on_upstream_cancel text NOT NULL CHECK (on_upstream_cancel IN ('cancel_downstream', 'detach')),
    on_upstream_not_accepted text NOT NULL CHECK (on_upstream_not_accepted = 'stop'),
    state text NOT NULL CHECK (state IN ('armed', 'released', 'blocked', 'detached', 'cancelled')),
    released_run_id uuid,
    released_at timestamptz,
    blocked_reason text CHECK (blocked_reason <> ''),
    packet_ref text CHECK (packet_ref <> ''),
    packet_digest text CHECK (packet_digest ~ '^sha256:[0-9a-f]{64}$'),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (from_mission_id <> to_mission_id AND from_mission_key <> to_mission_key),
    CHECK ((kind = 'supplies') = (jsonb_array_length(bindings) > 0)),
    CHECK ((state = 'blocked') = (blocked_reason IS NOT NULL)),
    CHECK (state <> 'released' OR released_at IS NOT NULL),
    UNIQUE (installation_id, application_id, tenant_id, link_id),
    UNIQUE (installation_id, application_id, tenant_id, chain_id, link_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, chain_id)
        REFERENCES mission_control.mission_chain (installation_id, application_id, tenant_id, chain_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, from_mission_id)
        REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, to_mission_id)
        REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, released_run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX chain_link_chain_id_fk_idx
    ON mission_control.chain_link (installation_id, application_id, tenant_id, chain_id);
CREATE INDEX chain_link_from_mission_id_fk_idx
    ON mission_control.chain_link (installation_id, application_id, tenant_id, from_mission_id);
CREATE INDEX chain_link_to_mission_id_fk_idx
    ON mission_control.chain_link (installation_id, application_id, tenant_id, to_mission_id);
CREATE INDEX chain_link_released_run_id_fk_idx
    ON mission_control.chain_link (installation_id, application_id, tenant_id, released_run_id);

-- FT-D2: the frozen admission of every chain member that is not admitted at submit. The
-- submit verifies the member's Run Request against its compiled configuration exactly as an
-- ordinary admission does and freezes it here; the chain reducer admits the run from these
-- rows inside the ledger transaction that releases the member's last incoming link.
CREATE TABLE mission_control.chain_member_admission (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    chain_member_admission_id uuid PRIMARY KEY,
    chain_id uuid NOT NULL,
    mission_id uuid NOT NULL,
    mission_key text NOT NULL CHECK (mission_key <> ''),
    revision_id uuid NOT NULL,
    family text NOT NULL CHECK (family IN ('StageGraph', 'GoalDirected')),
    initial_goal text CHECK (initial_goal <> ''),
    autostart boolean NOT NULL,
    run_request jsonb NOT NULL CHECK (jsonb_typeof(run_request) = 'object'),
    verified_configuration jsonb NOT NULL CHECK (jsonb_typeof(verified_configuration) = 'object'),
    request_fingerprint text NOT NULL CHECK (request_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((family = 'GoalDirected') = (initial_goal IS NOT NULL)),
    UNIQUE (installation_id, application_id, tenant_id, chain_member_admission_id),
    UNIQUE (installation_id, application_id, tenant_id, chain_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, chain_id)
        REFERENCES mission_control.mission_chain (installation_id, application_id, tenant_id, chain_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id)
        REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id)
        REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id)
);
CREATE INDEX chain_member_admission_mission_id_fk_idx
    ON mission_control.chain_member_admission (installation_id, application_id, tenant_id, mission_id);
CREATE INDEX chain_member_admission_revision_id_fk_idx
    ON mission_control.chain_member_admission (installation_id, application_id, tenant_id, revision_id);

-- Authoring provenance (SPEC-05): the manifest bytes and its resolution stored beside the
-- committed revision. The revision is the typed MissionDefinition@1, never the YAML.
CREATE TABLE mission_control.authoring_provenance (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    authoring_provenance_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    mission_key text NOT NULL CHECK (mission_key <> ''),
    chain_id uuid,
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    manifest_yaml text NOT NULL CHECK (manifest_yaml <> ''),
    resolution jsonb NOT NULL CHECK (jsonb_typeof(resolution) = 'object'
        AND resolution->>'schema_version' = 'mc.manifest_resolution.v1'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, authoring_provenance_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, revision_id)
        REFERENCES mission_control.mission_revision (installation_id, application_id, tenant_id, revision_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, chain_id)
        REFERENCES mission_control.mission_chain (installation_id, application_id, tenant_id, chain_id)
);
CREATE INDEX authoring_provenance_chain_id_fk_idx
    ON mission_control.authoring_provenance (installation_id, application_id, tenant_id, chain_id);
CREATE INDEX authoring_provenance_manifest_digest_idx
    ON mission_control.authoring_provenance (installation_id, application_id, tenant_id, manifest_digest);

-- mission_relationship gains the Mission Graph kinds of workflow-types/06; the three
-- existing values keep their meaning.
ALTER TABLE mission_control.mission_relationship
    DROP CONSTRAINT mission_relationship_kind_check;
ALTER TABLE mission_control.mission_relationship
    ADD CONSTRAINT mission_relationship_kind_check CHECK (kind IN (
        'composition', 'fork', 'dependency',
        'supplies', 'depends_on', 'parent_of', 'adopted_from', 'successor_of', 'forked_from'));

-- Forward-only chain lifecycle: identity, manifest and members never change; version
-- advances by exactly one; pending -> running|completed, running -> completed; a
-- completed chain is final.
CREATE FUNCTION mission_control.mission_chain_transition_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'mission_control.mission_chain rows are never deleted'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.installation_id, NEW.application_id, NEW.tenant_id, NEW.chain_id, NEW.chain_key,
        NEW.title, NEW.manifest_digest, NEW.members, NEW.created_at, NEW.created_by_actor_ref)
       IS DISTINCT FROM
       (OLD.installation_id, OLD.application_id, OLD.tenant_id, OLD.chain_id, OLD.chain_key,
        OLD.title, OLD.manifest_digest, OLD.members, OLD.created_at, OLD.created_by_actor_ref)
       OR NEW.version <> OLD.version + 1
       OR OLD.lifecycle = 'completed'
       OR NOT (
           NEW.lifecycle = OLD.lifecycle
           OR (OLD.lifecycle = 'pending' AND NEW.lifecycle IN ('running', 'completed'))
           OR (OLD.lifecycle = 'running' AND NEW.lifecycle = 'completed')
       ) THEN
        RAISE EXCEPTION 'mission chains only move forward'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.mission_chain_transition_guard() FROM PUBLIC;
CREATE TRIGGER mission_chain_transition_guard
    BEFORE UPDATE OR DELETE ON mission_control.mission_chain
    FOR EACH ROW EXECUTE FUNCTION mission_control.mission_chain_transition_guard();

-- Forward-only link state (asset_version precedent): identity, binding and policy columns
-- never change; version advances by exactly one; armed -> released|blocked|detached|
-- cancelled, released -> detached; blocked, detached and cancelled are final. Release
-- facts are written once: when the link is released, or (released_run_id and packet only)
-- when the consumer is admitted by the last of its incoming links (FT-D2).
CREATE FUNCTION mission_control.chain_link_transition_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'mission_control.chain_link rows are never deleted'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.installation_id, NEW.application_id, NEW.tenant_id, NEW.link_id, NEW.chain_id,
        NEW.link_key, NEW.from_mission_key, NEW.to_mission_key, NEW.from_mission_id,
        NEW.to_mission_id, NEW.kind, NEW.bindings, NEW.release_condition,
        NEW.on_upstream_cancel, NEW.on_upstream_not_accepted, NEW.created_at,
        NEW.created_by_actor_ref)
       IS DISTINCT FROM
       (OLD.installation_id, OLD.application_id, OLD.tenant_id, OLD.link_id, OLD.chain_id,
        OLD.link_key, OLD.from_mission_key, OLD.to_mission_key, OLD.from_mission_id,
        OLD.to_mission_id, OLD.kind, OLD.bindings, OLD.release_condition,
        OLD.on_upstream_cancel, OLD.on_upstream_not_accepted, OLD.created_at,
        OLD.created_by_actor_ref)
       OR NEW.version <> OLD.version + 1
       OR NOT (
           (OLD.state = 'armed' AND NEW.state IN ('released', 'blocked', 'detached', 'cancelled'))
           OR (OLD.state = 'released' AND NEW.state = 'detached')
           -- FT-D2: a link released before its consumer could start (another incoming link
           -- still armed) records the admitted run and packet once, when the last incoming
           -- link releases; nothing else changes.
           OR (OLD.state = 'released' AND NEW.state = 'released'
               AND OLD.released_run_id IS NULL AND NEW.released_run_id IS NOT NULL
               AND NEW.released_at IS NOT DISTINCT FROM OLD.released_at
               AND (OLD.packet_ref IS NULL OR NEW.packet_ref IS NOT DISTINCT FROM OLD.packet_ref)
               AND (OLD.packet_digest IS NULL
                    OR NEW.packet_digest IS NOT DISTINCT FROM OLD.packet_digest))
       )
       OR (OLD.state = 'released' AND NEW.state = 'detached'
           AND (NEW.released_run_id, NEW.released_at, NEW.packet_ref, NEW.packet_digest)
           IS DISTINCT FROM (OLD.released_run_id, OLD.released_at, OLD.packet_ref,
           OLD.packet_digest)) THEN
        RAISE EXCEPTION 'chain links only accept one forward state transition'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.chain_link_transition_guard() FROM PUBLIC;
CREATE TRIGGER chain_link_transition_guard
    BEFORE UPDATE OR DELETE ON mission_control.chain_link
    FOR EACH ROW EXECUTE FUNCTION mission_control.chain_link_transition_guard();

CREATE TRIGGER authoring_provenance_immutable
    BEFORE UPDATE OR DELETE ON mission_control.authoring_provenance
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
CREATE TRIGGER chain_member_admission_immutable
    BEFORE UPDATE OR DELETE ON mission_control.chain_member_admission
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'mission_chain',
        'chain_link',
        'authoring_provenance',
        'chain_member_admission'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id())',
            table_name || '_scope', table_name);
        EXECUTE pg_catalog.format('REVOKE ALL ON mission_control.%I FROM PUBLIC', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT ON mission_control.%I TO mission_control_readonly', table_name);
    END LOOP;
END
$policies$;

-- Runtime (submit and the chain reducer inside the canonical ledger writer): insert chains,
-- links and provenance; move only the mutable state columns. Same grant shape as
-- mission_relationship (0014) plus column-limited updates.
GRANT SELECT, INSERT ON mission_control.mission_chain, mission_control.chain_link,
    mission_control.authoring_provenance, mission_control.chain_member_admission
TO mission_control_runtime;
GRANT UPDATE (lifecycle, phase, terminal_outcome, version, updated_at)
ON mission_control.mission_chain TO mission_control_runtime;
GRANT UPDATE (state, released_run_id, released_at, blocked_reason, packet_ref, packet_digest,
    version, updated_at)
ON mission_control.chain_link TO mission_control_runtime;

-- FT-D2: the chain reducer runs inside append_events, which the family writer also calls
-- (StageGraph terminalization is a family admission). In that transaction it reads the
-- member chains, moves link and chain state, admits the consumer run from its frozen
-- admission (run, budget account, effect ledger, admission receipt, events), seals the
-- consumer's chain packet (context_selection, granted by 0027) and records the Mission
-- Graph relationship. These are exactly the writes RunControlService.admit makes on the
-- runtime role; RLS scope still applies.
GRANT SELECT ON mission_control.mission_chain, mission_control.chain_link,
    mission_control.chain_member_admission
TO mission_control_family_writer;
GRANT UPDATE (lifecycle, phase, terminal_outcome, version, updated_at)
ON mission_control.mission_chain TO mission_control_family_writer;
GRANT UPDATE (state, released_run_id, released_at, blocked_reason, packet_ref, packet_digest,
    version, updated_at)
ON mission_control.chain_link TO mission_control_family_writer;
GRANT INSERT ON mission_control.mission_run, mission_control.budget_account,
    mission_control.effect_ledger
TO mission_control_family_writer;
GRANT SELECT, INSERT ON mission_control.mission_relationship TO mission_control_family_writer;
-- Supplier outputs are read as custody metadata (never bytes) to seal the consumer packet.
GRANT SELECT ON mission_control.artifact, mission_control.workspace_candidate_descriptor
TO mission_control_family_writer;
