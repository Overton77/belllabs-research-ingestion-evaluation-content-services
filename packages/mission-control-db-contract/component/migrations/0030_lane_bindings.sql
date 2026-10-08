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
     '{"controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"unsupported","prepare":"native","reattach":"native","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"unsupported","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"hooks":{"events_supported":["before_tool","after_tool","after_tool_failure","before_shell","after_shell","after_file_edit","before_prompt","before_compaction","subagent_start","subagent_stop","stop"],"fail_closed":true,"mechanism":"command_hooks"},"identity":{"cursor":"sse_event_id","effect_ref":"call_id","session_ref":"agent_id","turn_ref":"run_id"},"instruction_channel":["AGENTS.md",".cursor/rules/mc-mission.mdc"],"lane":"cursor","lane_profile":"cursor_cloud","placement":"cloud","qualified":false,"schema_version":"mc.lane_describe.v1","subagents":{"file":".cursor/agents/*.md","inline":"customSubagents","readonly_supported_inline":false},"usage":{"cost":"estimated_then_settled","tokens":"settled_per_turn"},"versions":{"bridge":"1.0.37","cloud_api":"v1","cursor_sdk":"1.0.37","protocol":"sdk.v1"}}'::jsonb,
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
