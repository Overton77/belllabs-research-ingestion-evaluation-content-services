-- mission-control-db-contract common release: Native Event Store (SPEC-03, ADR-0028).
-- Additive only. Every provider frame a lane observes is persisted verbatim (redacted,
-- digested, excerpted under a cap) before any derivation, keyed by
-- (harness_execution_id, generation, provider_key) with a writer-assigned arrival ordinal.
-- The reducer reads only closing frames; mission events point back by native_event_ref.
--
-- Reconciled against 0003: harness_execution already carries observation_cursor (text),
-- so it is reused (a JSON document as text) rather than redefined; lane_profile,
-- generation, native_identity and lifecycle are added. session_turn stays immutable
-- (0003 trigger): a turn row is written once, when its closing turn_ended frame lands,
-- with the started and ended frame ids; the open turn lives in
-- harness_execution.native_identity until then. continuation_checkpoint exists in 0003
-- and is not redefined; context_selection (SPEC-02) is absent and is added here.
-- transcript_document (SPEC-03 run search, ticket C4) is created here so the search
-- projection needs no further migration.
--
-- Tenant scoped: composite (installation_id, application_id, tenant_id) FKs, ENABLE +
-- FORCE RLS with the three-column context policy. frame_retention_policy is
-- installation/application scoped (two-column policy), like mission_control_search.

-- ---------------------------------------------------------------------------------------
-- Native identity records: columns the lane-neutral writers need.
ALTER TABLE mission_control.harness_execution
    ADD COLUMN IF NOT EXISTS lane_profile text
        CHECK (lane_profile IN ('deep_agents', 'cursor_local', 'cursor_cloud',
                                'claude_agent_sdk', 'codex')),
    ADD COLUMN IF NOT EXISTS generation bigint CHECK (generation > 0),
    ADD COLUMN IF NOT EXISTS native_identity jsonb
        CHECK (native_identity IS NULL OR jsonb_typeof(native_identity) = 'object'),
    ADD COLUMN IF NOT EXISTS lifecycle text
        CHECK (lifecycle IN ('launching', 'live', 'ended', 'lost'));

ALTER TABLE mission_control.agent_session
    ADD COLUMN IF NOT EXISTS transferred_to uuid,
    ADD COLUMN IF NOT EXISTS ended_at timestamptz;

ALTER TABLE mission_control.session_turn
    ADD COLUMN IF NOT EXISTS started_frame_id uuid,
    ADD COLUMN IF NOT EXISTS ended_frame_id uuid,
    ADD COLUMN IF NOT EXISTS usage jsonb
        CHECK (usage IS NULL OR jsonb_typeof(usage) = 'object'),
    ADD COLUMN IF NOT EXISTS result_summary_ref text CHECK (result_summary_ref <> ''),
    ADD COLUMN IF NOT EXISTS stop_reason text CHECK (stop_reason <> '');

-- ---------------------------------------------------------------------------------------
-- Provider frames (mc.provider_frame.v1). Rows are immutable; only the retention job
-- deletes expired rows.
CREATE TABLE mission_control.provider_frame (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    frame_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    activation_id uuid NOT NULL,
    attempt_no bigint NOT NULL CHECK (attempt_no > 0),
    harness_execution_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    lane_profile text NOT NULL
        CHECK (lane_profile IN ('deep_agents', 'cursor_local', 'cursor_cloud',
                                'claude_agent_sdk', 'codex')),
    native_session_ref text NOT NULL CHECK (native_session_ref <> ''),
    native_turn_ref text CHECK (native_turn_ref <> ''),
    provider_key text NOT NULL CHECK (provider_key <> '' AND length(provider_key) <= 1024),
    arrival_ordinal bigint NOT NULL CHECK (arrival_ordinal > 0),
    observed_at timestamptz NOT NULL,
    provider_timestamp timestamptz,
    kind text NOT NULL CHECK (kind IN (
        'session_init', 'session_state', 'turn_started', 'message_delta', 'message',
        'thinking_delta', 'tool_call_started', 'tool_call_delta', 'tool_call_completed',
        'tool_call_failed', 'approval_requested', 'approval_resolved', 'hook_invoked',
        'hook_result', 'before_compaction', 'after_compaction', 'usage', 'turn_ended',
        'run_result', 'status', 'error', 'heartbeat', 'unknown')),
    closing boolean NOT NULL,
    raw_kind text NOT NULL CHECK (raw_kind <> ''),
    subordinate_ref text CHECK (subordinate_ref <> ''),
    tool_call_ref text CHECK (tool_call_ref <> ''),
    body_digest text NOT NULL CHECK (body_digest ~ '^sha256:[0-9a-f]{64}$'),
    body_bytes integer NOT NULL CHECK (body_bytes >= 0),
    body_media_type text NOT NULL CHECK (body_media_type <> ''),
    body_excerpt bytea NOT NULL,
    body_artifact_ref text CHECK (body_artifact_ref <> ''),
    redactions jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(redactions) = 'array'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    -- The closing rule of mc.provider_frame.v1: the reducer reads exactly these kinds.
    CHECK (closing = (kind IN (
        'tool_call_completed', 'tool_call_failed', 'approval_resolved', 'hook_result',
        'after_compaction', 'usage', 'turn_ended', 'run_result', 'session_state', 'error'))),
    UNIQUE (installation_id, application_id, tenant_id, frame_id),
    CONSTRAINT provider_frame_dedupe_key UNIQUE (harness_execution_id, generation, provider_key),
    CONSTRAINT provider_frame_arrival_key UNIQUE (harness_execution_id, generation, arrival_ordinal),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id)
        REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, harness_execution_id)
        REFERENCES mission_control.harness_execution
            (installation_id, application_id, tenant_id, harness_execution_id)
);
CREATE INDEX provider_frame_run_order ON mission_control.provider_frame
    (installation_id, application_id, tenant_id, run_id, observed_at, arrival_ordinal);
CREATE INDEX provider_frame_closing ON mission_control.provider_frame
    (harness_execution_id, generation, arrival_ordinal) WHERE closing;
CREATE INDEX provider_frame_tool_call ON mission_control.provider_frame
    (installation_id, application_id, tenant_id, run_id, tool_call_ref)
    WHERE tool_call_ref IS NOT NULL;
CREATE INDEX provider_frame_activation_fk_idx ON mission_control.provider_frame
    (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX provider_frame_expiry_idx ON mission_control.provider_frame
    (installation_id, application_id, tenant_id, observed_at) WHERE NOT closing;
CREATE TRIGGER provider_frame_immutable
    BEFORE UPDATE ON mission_control.provider_frame
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Per-application retention of the Native Event Store; absent row = defaults.
CREATE TABLE mission_control.frame_retention_policy (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    retain_days integer NOT NULL DEFAULT 30 CHECK (retain_days >= 1),
    keep_closing_frames boolean NOT NULL DEFAULT true,
    excerpt_cap_bytes integer NOT NULL DEFAULT 8192
        CHECK (excerpt_cap_bytes BETWEEN 256 AND 1048576),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    PRIMARY KEY (installation_id, application_id),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);

-- ---------------------------------------------------------------------------------------
-- mc.context_selection.v1 (SPEC-02): the authority record of one sealed Context Packet.
CREATE TABLE mission_control.context_selection (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    selection_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    activation_id uuid,
    attempt_no bigint CHECK (attempt_no > 0),
    generation bigint NOT NULL CHECK (generation > 0),
    purpose text NOT NULL CHECK (purpose <> ''),
    packet_digest text NOT NULL CHECK (packet_digest ~ '^sha256:[0-9a-f]{64}$'),
    packet jsonb NOT NULL CHECK (jsonb_typeof(packet) = 'object'),
    prompt_plan_digest text CHECK (prompt_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    file_plan_digest text CHECK (file_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    sealed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, selection_id),
    UNIQUE NULLS NOT DISTINCT
        (installation_id, application_id, tenant_id, run_id, activation_id, attempt_no,
         generation, purpose, packet_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id)
        REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id)
);
CREATE INDEX context_selection_run_idx ON mission_control.context_selection
    (installation_id, application_id, tenant_id, run_id);
CREATE INDEX context_selection_activation_fk_idx ON mission_control.context_selection
    (installation_id, application_id, tenant_id, activation_id);
CREATE TRIGGER context_selection_immutable
    BEFORE UPDATE OR DELETE ON mission_control.context_selection
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- ---------------------------------------------------------------------------------------
-- Transcript search projection (rebuildable; never an authorization store).
CREATE TABLE mission_control_search.transcript_document (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_id uuid NOT NULL,
    cursor text NOT NULL CHECK (cursor <> ''),
    kind text NOT NULL CHECK (kind <> ''),
    role text CHECK (role <> ''),
    title text NOT NULL,
    body_excerpt text,
    canonical boolean NOT NULL,
    recorded_at timestamptz NOT NULL,
    fts tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english'::regconfig, coalesce(title, '')), 'A')
        || setweight(to_tsvector('english'::regconfig, coalesce(body_excerpt, '')), 'B')
    ) STORED,
    PRIMARY KEY (installation_id, application_id, tenant_id, run_id, cursor),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX transcript_document_fts ON mission_control_search.transcript_document
    USING gin (fts);

-- ---------------------------------------------------------------------------------------
DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['provider_frame', 'context_selection'] LOOP
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
    END LOOP;
END
$policies$;

ALTER TABLE mission_control.frame_retention_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.frame_retention_policy FORCE ROW LEVEL SECURITY;
CREATE POLICY frame_retention_policy_scope ON mission_control.frame_retention_policy
    USING (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id());

ALTER TABLE mission_control_search.transcript_document ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control_search.transcript_document FORCE ROW LEVEL SECURITY;
CREATE POLICY transcript_document_scope ON mission_control_search.transcript_document
    USING (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id());

REVOKE ALL ON mission_control.provider_frame, mission_control.frame_retention_policy,
    mission_control.context_selection FROM PUBLIC;
REVOKE ALL ON mission_control_search.transcript_document FROM PUBLIC;

-- Worker (runtime) writes frames and the identity records in the frame-append
-- transaction; the retention job (runtime) deletes expired frames. Readers read.
GRANT SELECT, INSERT, DELETE ON mission_control.provider_frame TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.harness_execution, mission_control.agent_session,
    mission_control.session_turn TO mission_control_runtime;
GRANT UPDATE (lane_profile, generation, native_identity, observation_cursor, lifecycle,
    actual_binding_digest, native_execution_refs, recovery_state, version, updated_at)
ON mission_control.harness_execution TO mission_control_runtime;
GRANT UPDATE (state, checkpoint_ref, transferred_to, ended_at, version, updated_at)
ON mission_control.agent_session TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.frame_retention_policy
TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.context_selection
TO mission_control_runtime, mission_control_family_writer;
GRANT SELECT ON mission_control.provider_frame, mission_control.harness_execution,
    mission_control.agent_session, mission_control.session_turn,
    mission_control.frame_retention_policy, mission_control.context_selection
TO mission_control_readonly;
-- Projection writers rebuild the transcript search documents; readers search them.
GRANT SELECT, INSERT, UPDATE, DELETE ON mission_control_search.transcript_document
TO mission_control_runtime, mission_control_outbox_worker;
GRANT SELECT ON mission_control_search.transcript_document TO mission_control_readonly;
