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
-- and is not redefined; context_selection (SPEC-02) is added by the FT-B2 section below.
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
    FOREACH table_name IN ARRAY ARRAY['provider_frame'] LOOP
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

REVOKE ALL ON mission_control.provider_frame, mission_control.frame_retention_policy FROM PUBLIC;
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
GRANT SELECT ON mission_control.provider_frame, mission_control.harness_execution,
    mission_control.agent_session, mission_control.session_turn,
    mission_control.frame_retention_policy
TO mission_control_readonly;
-- Projection writers rebuild the transcript search documents; readers search them.
GRANT SELECT, INSERT, UPDATE, DELETE ON mission_control_search.transcript_document
TO mission_control_runtime, mission_control_outbox_worker;
GRANT SELECT ON mission_control_search.transcript_document TO mission_control_readonly;

-- section: B2 (FT-B2, OVE-31) context_selection -------------------------------------------
-- One sealed mc.context_packet.v1 and its mc.context_selection.v1 authority record per
-- (run, activation, attempt, generation, purpose). The packet is the plan, the selection is
-- the authority record (SPEC-02 "mc.context_selection.v1 linkage"). Rows are immutable; a
-- correction is a new packet naming the old packet_id in producer_refs.
CREATE TABLE mission_control.context_selection (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    context_selection_id uuid PRIMARY KEY,
    selection_key text NOT NULL CHECK (selection_key <> ''),
    packet_key text NOT NULL CHECK (packet_key <> ''),
    run_key text NOT NULL CHECK (run_key <> ''),
    node_key text NOT NULL CHECK (node_key <> ''),
    activation_key text NOT NULL CHECK (activation_key <> ''),
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    generation integer NOT NULL CHECK (generation >= 0),
    purpose text NOT NULL CHECK (purpose IN (
        'stage_start', 'iteration_start', 'continuation', 'fork', 'chain_link',
        'follow_up_turn'
    )),
    packer_version text NOT NULL CHECK (packer_version <> ''),
    packet_digest text NOT NULL CHECK (packet_digest ~ '^sha256:[0-9a-f]{64}$'),
    prompt_plan_digest text NOT NULL CHECK (prompt_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    file_plan_digest text NOT NULL CHECK (file_plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    packet jsonb NOT NULL CHECK (jsonb_typeof(packet) = 'object'),
    selection jsonb NOT NULL CHECK (jsonb_typeof(selection) = 'object'),
    sealed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((packet->>'schema_version' = 'mc.context_packet.v1'
        AND packet->>'packet_id' = packet_key
        AND packet->>'packet_digest' = packet_digest
        AND packet->>'context_selection_ref' = selection_key
        AND packet->'target'->>'run_id' = run_key
        AND packet->'target'->>'activation_id' = activation_key
        AND packet->'target'->'attempt_no' = to_jsonb(attempt_no)
        AND packet->'target'->'generation' = to_jsonb(generation)
        AND packet->'target'->>'purpose' = purpose
        AND selection->>'schema_version' = 'mc.context_selection.v1'
        AND selection->>'selection_id' = selection_key
        AND selection->>'packet_digest' = packet_digest
        AND selection->>'prompt_plan_digest' = prompt_plan_digest
        AND selection->>'file_plan_digest' = file_plan_digest) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, context_selection_id),
    UNIQUE (installation_id, application_id, tenant_id, selection_key),
    UNIQUE (installation_id, application_id, tenant_id, packet_key),
    UNIQUE (installation_id, application_id, tenant_id, run_key, activation_key, attempt_no,
        generation, purpose),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX context_selection_run_idx ON mission_control.context_selection
    (installation_id, application_id, tenant_id, run_key, node_key, sealed_at);

ALTER TABLE mission_control.context_selection ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.context_selection FORCE ROW LEVEL SECURITY;
CREATE POLICY context_selection_scope ON mission_control.context_selection
    USING (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id());
CREATE TRIGGER context_selection_immutable
    BEFORE UPDATE OR DELETE ON mission_control.context_selection
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
REVOKE ALL ON mission_control.context_selection FROM PUBLIC;
GRANT SELECT, INSERT ON mission_control.context_selection
TO mission_control_runtime, mission_control_family_writer;
GRANT SELECT ON mission_control.context_selection TO mission_control_readonly;
-- end section: B2 ----------------------------------------------------------------------------

-- section: B4 (FT-B4, OVE-33) continuation_transfer -----------------------------------------
-- One continuation of one logical execution, from its trigger (context health, provider
-- compaction frame, turn count, workflow boundary or a request_continuation command) to the
-- fresh session's hydration (SPEC-02 "Continuation checkpoint and compaction",
-- workflow-types/08 sections 8, 9 and 13). The sealed mc.continuation_checkpoint.v1 manifests
-- live in continuation_checkpoint (0003) with their checkpoint_validation verdicts; this row
-- carries the mutable saga: status, held mailbox commands, compaction attempts and the
-- cumulative governor ledger. Optimistic version; never deleted.
CREATE TABLE mission_control.continuation_transfer (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    continuation_transfer_id uuid PRIMARY KEY,
    transfer_key text NOT NULL CHECK (transfer_key <> ''),
    run_key text NOT NULL CHECK (run_key <> ''),
    activation_key text NOT NULL CHECK (activation_key <> ''),
    logical_execution_id text NOT NULL CHECK (logical_execution_id <> ''),
    lane_profile text NOT NULL
        CHECK (lane_profile IN ('deep_agents', 'cursor_local', 'cursor_cloud',
                                'claude_agent_sdk', 'codex')),
    trigger_kind text NOT NULL CHECK (trigger_kind IN (
        'context_health_soft', 'context_health_hard', 'provider_compaction', 'turn_count',
        'workflow_boundary', 'request_continuation')),
    trigger_ref text NOT NULL CHECK (trigger_ref <> ''),
    delivery text NOT NULL CHECK (delivery IN ('turn_boundary_guaranteed', 'wait_then_send')),
    status text NOT NULL CHECK (status IN (
        'requested', 'parked', 'sealed', 'transferred', 'human_review', 'failed',
        'governor_exhausted')),
    source_session_ref text NOT NULL CHECK (source_session_ref <> ''),
    target_session_ref text CHECK (target_session_ref <> ''),
    checkpoint_key text CHECK (checkpoint_key <> ''),
    released boolean NOT NULL DEFAULT false,
    failure_reason text CHECK (failure_reason <> ''),
    transfer jsonb NOT NULL CHECK (jsonb_typeof(transfer) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    requested_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (status <> 'transferred' OR (target_session_ref IS NOT NULL
        AND checkpoint_key IS NOT NULL)),
    CHECK ((transfer->>'transfer_id' = transfer_key
        AND transfer->>'status' = status
        AND (transfer->>'version')::bigint = version) IS TRUE),
    UNIQUE (installation_id, application_id, tenant_id, continuation_transfer_id),
    UNIQUE (installation_id, application_id, tenant_id, transfer_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX continuation_transfer_run_idx ON mission_control.continuation_transfer
    (installation_id, application_id, tenant_id, run_key, logical_execution_id, requested_at);
CREATE INDEX continuation_transfer_open_idx ON mission_control.continuation_transfer
    (installation_id, application_id, tenant_id, run_key)
    WHERE status IN ('requested', 'parked', 'sealed');

ALTER TABLE mission_control.continuation_transfer ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.continuation_transfer FORCE ROW LEVEL SECURITY;
CREATE POLICY continuation_transfer_scope ON mission_control.continuation_transfer
    USING (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
        AND application_id = mission_control.ctx_application_id()
        AND tenant_id = mission_control.ctx_tenant_id());
REVOKE ALL ON mission_control.continuation_transfer FROM PUBLIC;
-- The worker (runtime) runs triggers, seals and transfers; the API records
-- request_continuation triggers through the same runtime role; readers read.
GRANT SELECT, INSERT ON mission_control.continuation_transfer TO mission_control_runtime;
GRANT UPDATE (status, target_session_ref, checkpoint_key, released, failure_reason, transfer,
    version, updated_at)
ON mission_control.continuation_transfer TO mission_control_runtime;
GRANT SELECT ON mission_control.continuation_transfer TO mission_control_readonly;
-- end section: B4 ----------------------------------------------------------------------------
