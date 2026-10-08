-- mission-control-db-contract: FT-G1 lane bindings (SPEC-07 Persistence, ADR-0018, ADR-0030).
--
-- `lane_profile` is the installation-independent registry of Lane Profiles: one row per
-- profile carrying its declared `mc.lane_describe.v1` matrix and whether it is qualified.
-- It is reference data like an enum, so it is not tenant scoped: every role may read it,
-- no runtime role may write it, and RLS is forced with a read-only policy. Cursor profiles
-- are seeded unqualified; a recorded qualification (FT-G6) flips them through a later
-- release. `execution_binding` gains the profile a binding executes on (existing rows are
-- `deep_agents`, the only lane before this release) and the typed lane binding document
-- (`mc.cursor_binding.v1` for Cursor). Additive only: no existing row or column changes.

CREATE TABLE mission_control.lane_profile (
    lane_profile text PRIMARY KEY CHECK (lane_profile ~ '^[a-z][a-z0-9_]{0,62}$'),
    lane text NOT NULL CHECK (lane IN ('deep_agents', 'cursor')),
    placement text NOT NULL CHECK (placement IN ('worker_hosted', 'cloud')),
    describe jsonb NOT NULL CHECK (jsonb_typeof(describe) = 'object'),
    qualified boolean NOT NULL DEFAULT false,
    qualified_at timestamptz,
    qualification_ref text CHECK (qualification_ref <> ''),
    CHECK ((describe->>'schema_version' = 'mc.lane_describe.v1'
        AND describe->>'lane_profile' = lane_profile
        AND describe->>'lane' = lane
        AND describe->>'placement' = placement
        AND describe->'qualified' = to_jsonb(qualified)) IS TRUE),
    CHECK (qualified = (qualified_at IS NOT NULL AND qualification_ref IS NOT NULL)),
    CHECK (qualified_at IS NULL OR qualification_ref IS NOT NULL)
);

INSERT INTO mission_control.lane_profile
    (lane_profile, lane, placement, describe, qualified, qualified_at, qualification_ref)
VALUES
    ('deep_agents', 'deep_agents', 'worker_hosted',
     '{"controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"native","prepare":"native","reattach":"native","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"pause_at_tool_gate","queue_instruction":"turn_boundary_guaranteed","request_continuation":"emulated","resume":"turn_boundary_guaranteed"},"hooks":{"events_supported":["session_start","before_model","after_model","before_tool","after_tool","after_tool_failure","before_compaction","stop","session_end"],"fail_closed":true,"mechanism":"middleware"},"identity":{"cursor":"checkpoint_id","effect_ref":"tool_call_id","session_ref":"thread_id","turn_ref":"checkpoint_id"},"instruction_channel":["system_prompt"],"lane":"deep_agents","lane_profile":"deep_agents","placement":"worker_hosted","qualified":true,"schema_version":"mc.lane_describe.v1","subagents":{"file":null,"inline":"SubAgent","readonly_supported_inline":false},"usage":{"cost":"estimated","tokens":"settled_per_turn"},"versions":{"deepagents":"0.7.5","langgraph":"1.2.10"}}'::jsonb,
     true, TIMESTAMPTZ '2026-10-07 00:00:00+00', 'acceptance:wp-cp-040+postgres-runtime-parity'),
    ('cursor_local', 'cursor', 'worker_hosted',
     '{"controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"unsupported","prepare":"native","reattach":"emulated","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"unsupported","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"hooks":{"events_supported":["session_start","before_tool","after_tool","after_tool_failure","before_shell","after_shell","after_file_edit","before_prompt","before_compaction","subagent_start","subagent_stop","stop","session_end"],"fail_closed":true,"mechanism":"command_hooks"},"identity":{"cursor":"bridge_offset","effect_ref":"call_id","session_ref":"agent_id","turn_ref":"run_id"},"instruction_channel":["AGENTS.md",".cursor/rules/mc-mission.mdc","prompt_prefix"],"lane":"cursor","lane_profile":"cursor_local","placement":"worker_hosted","qualified":false,"schema_version":"mc.lane_describe.v1","subagents":{"file":".cursor/agents/*.md","inline":"AgentOptions.agents","readonly_supported_inline":false},"usage":{"cost":"estimated_then_settled","tokens":"settled_per_turn"},"versions":{"bridge":"1.0.37","cursor_sdk":"1.0.37","protocol":"sdk.v1"}}'::jsonb,
     false, NULL, NULL),
    ('cursor_cloud', 'cursor', 'cloud',
     '{"controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"unsupported","prepare":"native","reattach":"native","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"unsupported","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"hooks":{"events_supported":["before_tool","after_tool","after_tool_failure","before_shell","after_shell","after_file_edit","before_prompt","before_compaction","subagent_start","subagent_stop","stop"],"fail_closed":false,"mechanism":"command_hooks"},"identity":{"cursor":"sse_event_id","effect_ref":"call_id","session_ref":"agent_id","turn_ref":"run_id"},"instruction_channel":["AGENTS.md",".cursor/rules/mc-mission.mdc"],"lane":"cursor","lane_profile":"cursor_cloud","placement":"cloud","qualified":false,"schema_version":"mc.lane_describe.v1","subagents":{"file":".cursor/agents/*.md","inline":"customSubagents","readonly_supported_inline":false},"usage":{"cost":"estimated_then_settled","tokens":"settled_per_turn"},"versions":{"bridge":"1.0.37","cloud_api":"v1","cursor_sdk":"1.0.37","protocol":"sdk.v1"}}'::jsonb,
     false, NULL, NULL);

ALTER TABLE mission_control.lane_profile ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.lane_profile FORCE ROW LEVEL SECURITY;
CREATE POLICY lane_profile_read ON mission_control.lane_profile FOR SELECT USING (true);
CREATE TRIGGER lane_profile_immutable
    BEFORE UPDATE OR DELETE ON mission_control.lane_profile
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
REVOKE ALL ON mission_control.lane_profile FROM PUBLIC;
GRANT SELECT ON mission_control.lane_profile
    TO mission_control_runtime, mission_control_family_writer, mission_control_readonly;

ALTER TABLE mission_control.execution_binding
    ADD COLUMN lane_profile text NOT NULL DEFAULT 'deep_agents'
        REFERENCES mission_control.lane_profile (lane_profile),
    ADD COLUMN lane_binding jsonb,
    ADD CONSTRAINT execution_binding_lane_binding_shape CHECK (lane_binding IS NULL OR (
        jsonb_typeof(lane_binding) = 'object'
        AND (lane_profile NOT IN ('cursor_local', 'cursor_cloud')
            OR (lane_binding->>'schema_version' = 'mc.cursor_binding.v1'
                AND lane_binding->>'lane_profile' = lane_profile)))),
    ADD CONSTRAINT execution_binding_cursor_lane_binding
        CHECK (lane_profile NOT IN ('cursor_local', 'cursor_cloud') OR lane_binding IS NOT NULL);
CREATE INDEX execution_binding_lane_profile_idx
    ON mission_control.execution_binding (lane_profile);

-- section: G2 (FT-G2, OVE-51) harness_execution lane state --------------------------------
-- `lane.turn` records the native session and turn on the harness execution before it
-- observes (a recorded turn is never sent again), the provider cursor and segment time at
-- every segment boundary, and the usage disposition at settlement. The Cursor profiles'
-- pinned versions, bridge state root and cloud branch/agent URL are recorded alongside.
-- The row itself is opened by the frame store (0027); these columns are nullable and
-- additive, so existing rows and the Deep Agents writer are unchanged.
ALTER TABLE mission_control.harness_execution
    ADD COLUMN IF NOT EXISTS native_session_ref text CHECK (native_session_ref <> ''),
    ADD COLUMN IF NOT EXISTS native_turn_ref text CHECK (native_turn_ref <> ''),
    ADD COLUMN IF NOT EXISTS provider_cursor text CHECK (provider_cursor <> ''),
    ADD COLUMN IF NOT EXISTS cursor_sdk_version text CHECK (cursor_sdk_version <> ''),
    ADD COLUMN IF NOT EXISTS bridge_state_root text CHECK (bridge_state_root <> ''),
    ADD COLUMN IF NOT EXISTS cloud_branch text CHECK (cloud_branch <> ''),
    ADD COLUMN IF NOT EXISTS cloud_agent_url text CHECK (cloud_agent_url <> ''),
    ADD COLUMN IF NOT EXISTS usage_disposition text
        CHECK (usage_disposition IN ('estimated', 'settled', 'unknown')),
    ADD COLUMN IF NOT EXISTS last_segment_at timestamptz;

GRANT UPDATE (native_session_ref, native_turn_ref, provider_cursor, cursor_sdk_version,
    bridge_state_root, cloud_branch, cloud_agent_url, usage_disposition, last_segment_at)
ON mission_control.harness_execution TO mission_control_runtime;
-- end section: G2

-- section: G3 (FT-G3, OVE-52) Cursor local: workspace leases, hook task tokens, intents ------
-- The Cursor local lane leases a git worktree per harness execution generation in the
-- existing `workspace_lease` table (0003; the runtime had no grants on it). A Kernel Hook
-- callback authenticates with a task token minted for (scope, run, attempt, generation,
-- harness execution): only the token's digest is stored, with a secret-free context, and it
-- expires with the lease or is revoked at session end. A permission Kernel Hook writes the
-- Operation Intent of the effect it admits (keyed on the provider's effect ref) before it
-- answers allow. Tokens and intents are tenant scoped under forced RLS; intents are
-- insert-only.
GRANT SELECT, INSERT ON mission_control.workspace_lease TO mission_control_runtime;
GRANT UPDATE (desired_state, observed_state, cleanup_status, patch_ref, native_workspace_ref,
    lease_expires_at, fencing_token, detail, version, updated_at)
ON mission_control.workspace_lease TO mission_control_runtime;
GRANT SELECT ON mission_control.workspace_lease TO mission_control_readonly;

CREATE TABLE mission_control.hook_task_token (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    token_hash text PRIMARY KEY CHECK (token_hash ~ '^sha256:[0-9a-f]{64}$'),
    harness_execution_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    run_key text NOT NULL CHECK (run_key <> ''),
    lane_profile text NOT NULL REFERENCES mission_control.lane_profile (lane_profile),
    context jsonb NOT NULL CHECK (jsonb_typeof(context) = 'object'),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    CHECK (expires_at > issued_at),
    CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
    UNIQUE (installation_id, application_id, tenant_id, token_hash),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX hook_task_token_execution_idx ON mission_control.hook_task_token
    (installation_id, application_id, tenant_id, harness_execution_id, generation);

CREATE TABLE mission_control.hook_effect_intent (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    intent_id uuid PRIMARY KEY,
    run_key text NOT NULL CHECK (run_key <> ''),
    generation bigint NOT NULL CHECK (generation > 0),
    harness_execution_id uuid NOT NULL,
    effect_ref text NOT NULL CHECK (effect_ref <> '' AND length(effect_ref) <= 512),
    effect_kind text NOT NULL
        CHECK (effect_kind IN ('shell', 'mcp', 'file', 'task', 'model', 'other')),
    hook_event text NOT NULL CHECK (hook_event <> ''),
    lane_profile text NOT NULL REFERENCES mission_control.lane_profile (lane_profile),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL,
    UNIQUE (installation_id, application_id, tenant_id, run_key, generation, effect_ref),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE INDEX hook_effect_intent_execution_idx ON mission_control.hook_effect_intent
    (installation_id, application_id, tenant_id, harness_execution_id, generation);
CREATE TRIGGER hook_effect_intent_immutable
    BEFORE UPDATE OR DELETE ON mission_control.hook_effect_intent
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['hook_task_token', 'hook_effect_intent'] LOOP
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

REVOKE ALL ON mission_control.hook_task_token, mission_control.hook_effect_intent FROM PUBLIC;
GRANT SELECT, INSERT ON mission_control.hook_task_token TO mission_control_runtime;
GRANT UPDATE (revoked_at) ON mission_control.hook_task_token TO mission_control_runtime;
GRANT SELECT, INSERT ON mission_control.hook_effect_intent TO mission_control_runtime;
GRANT SELECT ON mission_control.hook_effect_intent TO mission_control_readonly;
-- end section: G3
