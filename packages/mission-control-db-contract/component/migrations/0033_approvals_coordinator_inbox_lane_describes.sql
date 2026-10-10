-- mission-control-db-contract: multi-provider wave-4 integration, component release 1.2.0.
-- Extends 0029 (subscriptions), 0030/0031 (lane profiles) and the 0005/0010 human task and
-- run records. Additive except the lane describe refresh in section 3, which revises four
-- `lane_profile` reference rows (see that section). Three sections, in this order:
--
-- 1. MP-11 (SPEC-03 "Human control"): `approval_correlation`, the expiring native request
--    handle and the reply actually sent for an approval Human Task, and
--    `governed_effect_intent`, the approval-bound intent of a Mission-Control-owned tool with
--    its one immutable receipt. Approval tasks themselves are `human_task` rows.
-- 2. MP-15 (SPEC-04 "Coordinator subscriptions"): `coordinator_inbox`,
--    `coordinator_notification` and `coordinator_causation` beside the 0029
--    `mission_subscription` row each inbox extends.
-- 3. Lane describe refresh (MP-07, MP-08, MP-09): the declared v2 matrices of `cursor_local`,
--    `cursor_cloud`, `claude_agent_sdk` and `codex`, all still unqualified.
--
-- Not included: MP-12's optional continuation phase/activation columns and constraints are
-- deferred to a later release (integrator decision); nothing here anticipates them.

-- ---------------------------------------------------------------------------------------
-- section: MP-11 native approval correlations and governed effect intents
-- (SPEC-03 "Human control", ticket MP-11). Reviewed from the adapter proposal
-- (`adapters/postgres/approvals`); same conventions as 0029: composite tenant scope, forced
-- RLS, REVOKE PUBLIC, no deletes, column-limited runtime UPDATE grants.
CREATE TABLE mission_control.approval_correlation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    approval_correlation_id uuid PRIMARY KEY,
    human_task_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    harness_execution_id text NOT NULL
        CHECK (harness_execution_id <> '' AND length(harness_execution_id) <= 512),
    generation integer NOT NULL CHECK (generation >= 1),
    connection_ref text NOT NULL CHECK (connection_ref <> '' AND length(connection_ref) <= 512),
    native jsonb NOT NULL CHECK (jsonb_typeof(native) = 'object'),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('live', 'answered', 'expired', 'lost', 'superseded')),
    reply jsonb CHECK (reply IS NULL OR jsonb_typeof(reply) = 'object'),
    reply_digest text CHECK (reply_digest ~ '^sha256:[0-9a-f]{64}$'),
    close_reason text CHECK (close_reason <> '' AND length(close_reason) <= 128),
    replayed_from text CHECK (replayed_from <> ''),
    opened_at timestamptz NOT NULL,
    wait_deadline_at timestamptz,
    closed_at timestamptz,
    version bigint NOT NULL CHECK (version >= 1),
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((state = 'live') = (closed_at IS NULL)),
    CHECK (state <> 'answered' OR (reply IS NOT NULL AND reply_digest IS NOT NULL)),
    UNIQUE (installation_id, application_id, tenant_id, approval_correlation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, human_task_id)
        REFERENCES mission_control.human_task
            (installation_id, application_id, tenant_id, human_task_id)
);
CREATE INDEX approval_correlation_task_idx ON mission_control.approval_correlation
    (installation_id, application_id, tenant_id, human_task_id);
CREATE INDEX approval_correlation_live_idx ON mission_control.approval_correlation
    (installation_id, application_id, tenant_id, harness_execution_id)
    WHERE state = 'live';

CREATE TABLE mission_control.governed_effect_intent (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    governed_effect_intent_id uuid PRIMARY KEY,
    intent_key text NOT NULL CHECK (intent_key <> ''),
    run_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    harness_execution_id text NOT NULL
        CHECK (harness_execution_id <> '' AND length(harness_execution_id) <= 512),
    generation integer NOT NULL CHECK (generation >= 1),
    lane_profile text NOT NULL CHECK (lane_profile <> ''),
    tool_name text NOT NULL CHECK (tool_name <> '' AND length(tool_name) <= 256),
    effect_kind text NOT NULL
        CHECK (effect_kind IN ('shell', 'mcp', 'file', 'task', 'model', 'other')),
    arguments jsonb NOT NULL CHECK (jsonb_typeof(arguments) = 'object'),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    human_task_id uuid,
    state text NOT NULL CHECK (state IN ('pending_approval', 'ready', 'executing', 'executed',
        'denied', 'cancelled', 'expired', 'fenced', 'stale', 'in_doubt')),
    reason text CHECK (reason <> '' AND length(reason) <= 128),
    claimant_ref text CHECK (claimant_ref <> ''),
    claimed_at timestamptz,
    receipt jsonb CHECK (receipt IS NULL OR jsonb_typeof(receipt) = 'object'),
    receipt_digest text CHECK (receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((state = 'executed') = (receipt IS NOT NULL)),
    CHECK ((receipt IS NULL) = (receipt_digest IS NULL)),
    CHECK (state <> 'pending_approval' OR human_task_id IS NOT NULL),
    CHECK (state <> 'executing' OR claimed_at IS NOT NULL),
    UNIQUE (installation_id, application_id, tenant_id, governed_effect_intent_id),
    UNIQUE (installation_id, application_id, tenant_id, intent_key),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, human_task_id)
        REFERENCES mission_control.human_task
            (installation_id, application_id, tenant_id, human_task_id)
);
CREATE INDEX governed_effect_intent_run_idx ON mission_control.governed_effect_intent
    (installation_id, application_id, tenant_id, run_id);
CREATE INDEX governed_effect_intent_task_idx ON mission_control.governed_effect_intent
    (installation_id, application_id, tenant_id, human_task_id);

-- A receipt, once written, never changes; an executed/terminal intent never changes again
-- (no column of a settled row moves, so neither its receipt nor its receipt digest can).
CREATE FUNCTION mission_control.governed_effect_intent_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'governed effect intents are never deleted'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.receipt IS NOT NULL AND NEW.receipt IS DISTINCT FROM OLD.receipt THEN
        RAISE EXCEPTION 'a governed effect receipt is immutable'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.state IN ('executed', 'denied', 'cancelled', 'expired', 'fenced', 'stale',
                     'in_doubt') THEN
        RAISE EXCEPTION 'a settled governed effect intent does not move'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.intent_key IS DISTINCT FROM OLD.intent_key
       OR NEW.arguments IS DISTINCT FROM OLD.arguments
       OR NEW.input_digest IS DISTINCT FROM OLD.input_digest
       OR NEW.policy_digest IS DISTINCT FROM OLD.policy_digest
       OR NEW.human_task_id IS DISTINCT FROM OLD.human_task_id THEN
        RAISE EXCEPTION 'a governed effect intent is bound to its arguments and review'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END
$guard$;
REVOKE ALL ON FUNCTION mission_control.governed_effect_intent_guard() FROM PUBLIC;
CREATE TRIGGER governed_effect_intent_guard
    BEFORE UPDATE OR DELETE ON mission_control.governed_effect_intent
    FOR EACH ROW EXECUTE FUNCTION mission_control.governed_effect_intent_guard();

-- A closed correlation never reopens and its reply never changes.
CREATE FUNCTION mission_control.approval_correlation_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'approval correlations are never deleted'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.state <> 'live' THEN
        RAISE EXCEPTION 'a closed approval correlation does not move'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.human_task_id IS DISTINCT FROM OLD.human_task_id
       OR NEW.connection_ref IS DISTINCT FROM OLD.connection_ref
       OR NEW.native IS DISTINCT FROM OLD.native
       OR NEW.generation IS DISTINCT FROM OLD.generation
       OR NEW.input_digest IS DISTINCT FROM OLD.input_digest THEN
        RAISE EXCEPTION 'an approval correlation is bound to its native request'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END
$guard$;
REVOKE ALL ON FUNCTION mission_control.approval_correlation_guard() FROM PUBLIC;
CREATE TRIGGER approval_correlation_guard
    BEFORE UPDATE OR DELETE ON mission_control.approval_correlation
    FOR EACH ROW EXECUTE FUNCTION mission_control.approval_correlation_guard();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['approval_correlation', 'governed_effect_intent'] LOOP
        EXECUTE pg_catalog.format(
            'ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
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
            'GRANT SELECT, INSERT ON mission_control.%I TO mission_control_runtime', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT ON mission_control.%I TO mission_control_readonly', table_name);
    END LOOP;
END
$policies$;
GRANT UPDATE (state, reply, reply_digest, close_reason, replayed_from, closed_at, version)
    ON mission_control.approval_correlation TO mission_control_runtime;
GRANT UPDATE (state, reason, claimant_ref, claimed_at, receipt, receipt_digest, version,
    updated_at)
    ON mission_control.governed_effect_intent TO mission_control_runtime;
-- end section: MP-11

-- ---------------------------------------------------------------------------------------
-- section: MP-15 coordinator inbox, notifications and causation
-- (SPEC-04 "Coordinator subscriptions", ticket MP-15). Reviewed from the adapter proposal
-- (`adapters/postgres/subscriptions/coordinator_inbox.py`); same conventions as 0029.
CREATE TABLE mission_control.coordinator_inbox (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subscription_id uuid PRIMARY KEY,
    coordinator_ref text NOT NULL CHECK (coordinator_ref <> ''),
    coordinator_run_ref text CHECK (coordinator_run_ref <> ''),
    profile jsonb NOT NULL CHECK (jsonb_typeof(profile) = 'object'),
    delivery_kind text NOT NULL CHECK (delivery_kind IN ('poll', 'mcp_session', 'webhook')),
    delivery jsonb NOT NULL CHECK (jsonb_typeof(delivery) = 'object'
        AND delivery->>'kind' = delivery_kind
        AND NOT (delivery ?| ARRAY['secret', 'secret_value', 'password', 'token'])),
    prompting boolean NOT NULL,
    next_inbox_seq bigint NOT NULL CHECK (next_inbox_seq >= 1),
    acked_inbox_seq bigint NOT NULL CHECK (acked_inbox_seq >= 0),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (acked_inbox_seq < next_inbox_seq),
    CHECK (NOT prompting OR coordinator_run_ref IS NOT NULL),
    UNIQUE (installation_id, application_id, tenant_id, subscription_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subscription_id)
        REFERENCES mission_control.mission_subscription
            (installation_id, application_id, tenant_id, subscription_id)
);

CREATE TABLE mission_control.coordinator_notification (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    notification_id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL,
    inbox_seq bigint CHECK (inbox_seq >= 1),
    state text NOT NULL CHECK (state IN ('open', 'pending', 'acknowledged', 'suppressed')),
    kind text NOT NULL CHECK (kind IN ('review_required', 'blocked', 'failed',
        'terminal_result', 'accepted_output', 'child_lifecycle', 'review_closed', 'progress',
        'rate_limited')),
    actionable boolean NOT NULL,
    mission_id uuid NOT NULL,
    run_id uuid,
    seq_from bigint NOT NULL CHECK (seq_from >= 1),
    seq_to bigint NOT NULL,
    event_count integer NOT NULL CHECK (event_count >= 1),
    anchor_event_id uuid NOT NULL,
    depth integer NOT NULL CHECK (depth >= 0),
    suppressed_reason text CHECK (suppressed_reason IN ('recursion_bound', 'rate_folded')),
    body jsonb NOT NULL CHECK (jsonb_typeof(body) = 'object'
        AND body->>'schema_version' = 'mc.coordinator_notification.v1'),
    opened_at timestamptz NOT NULL,
    sealed_at timestamptz,
    acknowledged_at timestamptz,
    acknowledged_by_actor_ref text CHECK (acknowledged_by_actor_ref <> ''),
    prompt_request_id uuid,
    prompt_state text CHECK (prompt_state IN ('requested', 'admitted', 'skipped', 'failed')),
    prompt_request jsonb,
    prompt_detail text,
    prompted_at timestamptz,
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (seq_to >= seq_from),
    CHECK ((state IN ('pending', 'acknowledged')) = (inbox_seq IS NOT NULL)),
    CHECK ((state = 'suppressed') = (suppressed_reason IS NOT NULL)),
    CHECK ((state = 'acknowledged') = (acknowledged_at IS NOT NULL)),
    CHECK (state = 'open' OR sealed_at IS NOT NULL),
    CHECK (prompt_state IS NULL OR prompt_state = 'skipped' OR prompt_request_id IS NOT NULL),
    UNIQUE (installation_id, application_id, tenant_id, notification_id),
    UNIQUE (installation_id, application_id, tenant_id, subscription_id, inbox_seq),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subscription_id)
        REFERENCES mission_control.coordinator_inbox
            (installation_id, application_id, tenant_id, subscription_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, anchor_event_id)
        REFERENCES mission_control.mission_event
            (installation_id, application_id, tenant_id, event_id)
);
CREATE INDEX coordinator_notification_open_idx
    ON mission_control.coordinator_notification
        (installation_id, application_id, tenant_id, subscription_id)
    WHERE state = 'open';
CREATE INDEX coordinator_notification_pending_idx
    ON mission_control.coordinator_notification
        (installation_id, application_id, tenant_id, subscription_id, inbox_seq)
    WHERE state = 'pending';
CREATE INDEX coordinator_notification_sealed_idx
    ON mission_control.coordinator_notification
        (installation_id, application_id, tenant_id, subscription_id, run_id, sealed_at)
    WHERE state IN ('pending', 'acknowledged');
CREATE INDEX coordinator_notification_anchor_event_id_fk_idx
    ON mission_control.coordinator_notification
        (installation_id, application_id, tenant_id, anchor_event_id);

-- A command admitted because of a notification (prompt or triggered), by request id. The
-- events it causes carry `causation_ref = <request id>` and inherit `depth`. Immutable.
CREATE TABLE mission_control.coordinator_causation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    command_request_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    notification_id uuid NOT NULL,
    origin text NOT NULL CHECK (origin IN ('prompt', 'triggered')),
    target_run_ref text NOT NULL CHECK (target_run_ref <> ''),
    depth integer NOT NULL CHECK (depth >= 1),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    PRIMARY KEY (installation_id, application_id, tenant_id, command_request_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subscription_id)
        REFERENCES mission_control.coordinator_inbox
            (installation_id, application_id, tenant_id, subscription_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, notification_id)
        REFERENCES mission_control.coordinator_notification
            (installation_id, application_id, tenant_id, notification_id)
);
CREATE INDEX coordinator_causation_window_idx
    ON mission_control.coordinator_causation
        (installation_id, application_id, tenant_id, subscription_id, origin, recorded_at);
CREATE INDEX coordinator_causation_notification_id_fk_idx
    ON mission_control.coordinator_causation
        (installation_id, application_id, tenant_id, notification_id);

CREATE TRIGGER coordinator_causation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.coordinator_causation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
CREATE TRIGGER coordinator_inbox_no_delete
    BEFORE DELETE ON mission_control.coordinator_inbox
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
CREATE TRIGGER coordinator_notification_no_delete
    BEFORE DELETE ON mission_control.coordinator_notification
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['coordinator_inbox', 'coordinator_notification',
            'coordinator_causation'] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY',
            table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY',
            table_name);
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
        EXECUTE pg_catalog.format(
            'GRANT SELECT, INSERT ON mission_control.%I TO mission_control_runtime', table_name);
    END LOOP;
END
$policies$;

GRANT UPDATE (next_inbox_seq, acked_inbox_seq, version, updated_at)
ON mission_control.coordinator_inbox TO mission_control_runtime;
GRANT UPDATE (inbox_seq, state, actionable, seq_from, seq_to, event_count, anchor_event_id,
    depth, suppressed_reason, body, sealed_at, acknowledged_at, acknowledged_by_actor_ref,
    prompt_request_id, prompt_state, prompt_request, prompt_detail, prompted_at, version,
    updated_at)
ON mission_control.coordinator_notification TO mission_control_runtime;
-- end section: MP-15

-- ---------------------------------------------------------------------------------------
-- section: lane describe refresh (MP-07 Claude Agent SDK, MP-08 Codex, MP-09 Cursor lanes)
-- GENERATED by packages/mission-control-db-contract/scripts/lane_describe_refresh.py from
-- DECLARED_LANE_MATRICES (src/mission_control/application/execution/harness/describe.py);
-- never edit by hand. 0030 seeded `cursor_local`/`cursor_cloud` with `mc.lane_describe.v1`
-- and 0031 seeded `claude_agent_sdk`/`codex` as `mc.lane_describe.v2` stubs; the lanes now
-- declare implemented v2 matrices (Cursor promoted to v2). Only `describe` changes:
-- `qualified`, `qualified_at` and `qualification_ref` stay false/NULL/NULL (fixture and
-- offline proof are not a qualification), and a qualified row is never overwritten.
-- `lane_profile` is reference data guarded by `lane_profile_immutable` and by forced RLS with
-- a SELECT-only policy (0030). As in 0031, FORCE is lifted only around this revision so a
-- non-superuser migration principal (the table owner) can write, and the immutability trigger
-- is disabled only for these statements; both are restored before the section ends, so the
-- table leaves this migration exactly as 0031 left it.
ALTER TABLE mission_control.lane_profile NO FORCE ROW LEVEL SECURITY;
ALTER TABLE mission_control.lane_profile DISABLE TRIGGER lane_profile_immutable;
DO $lane_describes$
DECLARE
    refreshed bigint;
BEGIN
    UPDATE mission_control.lane_profile
       SET describe = '{"approval_modes":["workflow_gate"],"compaction_control":"unsupported","controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"unsupported","prepare":"native","reattach":"emulated","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"unsupported","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"enforcement_coverage":{"file":"unqualified","mcp":"unqualified","shell":"unqualified"},"features":{"approval_suspension":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"cursor_sdk==1.0.37 types.py SDKRequestMessage(request_id); no respond API in _async_run.py/_async_agent.py/_async_client.py","implemented":false,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"unqualified","transport":"cursor-sdk bridge (local)"},"cancel":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"configuration_materialization":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"continuation":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_describe_honesty.py::request_continuation","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"emulated","transport":"cursor-sdk bridge (local)"},"environment_selection":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"follow_up":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"launch":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"observe":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_cursor_local.py::test_a_resume_from_a_stored_offset_stores_no_frame_twice","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"output_custody":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"emulated","transport":"cursor-sdk bridge (local)"},"status":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"subordinate_lineage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/integration/cursor/test_kernel_hook_roundtrip.py","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"},"usage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_cursor_local.py::test_error_run_fails_and_settles_cost_when_the_provider_reports_it","implemented":true,"os":"Linux|Darwin (WSL 2 on a Windows host)","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"cursor-sdk bridge (local)"}},"hooks":{"events_supported":["session_start","before_tool","after_tool","after_tool_failure","before_shell","after_shell","after_file_edit","before_prompt","before_compaction","subagent_start","subagent_stop","stop","session_end"],"fail_closed":true,"mechanism":"command_hooks"},"identity":{"cursor":"bridge_offset","effect_ref":"call_id","session_ref":"agent_id","turn_ref":"run_id"},"instruction_channel":["AGENTS.md",".cursor/rules/mc-mission.mdc","prompt_prefix"],"lane":"cursor","lane_profile":"cursor_local","placement":"worker_hosted","qualified":false,"schema_version":"mc.lane_describe.v2","subagents":{"file":".cursor/agents/*.md","inline":"AgentOptions.agents","readonly_supported_inline":false},"subordinate_visibility":"unqualified","usage":{"cost":"estimated_then_settled","tokens":"settled_per_turn"},"versions":{"bridge":"1.0.37","cursor_sdk":"1.0.37","protocol":"sdk.v1"}}'::jsonb
     WHERE lane_profile = 'cursor_local' AND NOT qualified;
    GET DIAGNOSTICS refreshed = ROW_COUNT;
    IF refreshed <> 1 THEN
        RAISE EXCEPTION 'lane_profile cursor_local was not refreshed (% rows)', refreshed
            USING ERRCODE = 'data_exception';
    END IF;
    UPDATE mission_control.lane_profile
       SET describe = '{"approval_modes":["workflow_gate"],"compaction_control":"unsupported","controls":{"cancel_turn":"native","end_session":"native","fork":"emulated","observe":"native","pause":"unsupported","prepare":"native","reattach":"native","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"unsupported","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"enforcement_coverage":{"file":"unqualified","mcp":"unsupported","shell":"unqualified"},"features":{"approval_suspension":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"cursor_sdk==1.0.37 types.py SDKRequestMessage(request_id); no respond API in _async_run.py/_async_agent.py/_async_client.py","implemented":false,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"unsupported","transport":"Cloud Agents API v1 over httpx"},"cancel":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"configuration_materialization":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"continuation":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_describe_honesty.py::request_continuation","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"emulated","transport":"Cloud Agents API v1 over httpx"},"environment_selection":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"follow_up":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/cursor/test_cloud_reconcile.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"launch":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/cursor/test_cloud_capacity_and_busy.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"observe":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/cursor/test_cloud_stream_expiry.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"output_custody":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/cursor/test_cloud_workspace_and_resume.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"emulated","transport":"Cloud Agents API v1 over httpx"},"status":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/harness/test_lane_qualification_fixtures.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"},"subordinate_lineage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"../mission-control-general/runtime-facts/CURSOR_SDK_FACTS.md section 7: cloud agents run command hooks only; no sessionStart/sessionEnd/beforeMCPExecution/afterMCPExecution","implemented":false,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"unqualified","transport":"Cloud Agents API v1 over httpx"},"usage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":"tests/unit/cursor/test_cloud_usage_unknown.py","implemented":true,"os":"any worker host","provider_scope":"cursor account bound at launch","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"1.0.37","status":"native","transport":"Cloud Agents API v1 over httpx"}},"hooks":{"events_supported":["before_tool","after_tool","after_tool_failure","before_shell","after_shell","after_file_edit","before_prompt","before_compaction","subagent_start","subagent_stop","stop"],"fail_closed":false,"mechanism":"command_hooks"},"identity":{"cursor":"sse_event_id","effect_ref":"call_id","session_ref":"agent_id","turn_ref":"run_id"},"instruction_channel":["AGENTS.md",".cursor/rules/mc-mission.mdc"],"lane":"cursor","lane_profile":"cursor_cloud","placement":"cloud","qualified":false,"schema_version":"mc.lane_describe.v2","subagents":{"file":".cursor/agents/*.md","inline":"customSubagents","readonly_supported_inline":false},"subordinate_visibility":"unqualified","usage":{"cost":"estimated_then_settled","tokens":"settled_per_turn"},"versions":{"bridge":"1.0.37","cloud_api":"v1","cursor_sdk":"1.0.37","protocol":"sdk.v1"}}'::jsonb
     WHERE lane_profile = 'cursor_cloud' AND NOT qualified;
    GET DIAGNOSTICS refreshed = ROW_COUNT;
    IF refreshed <> 1 THEN
        RAISE EXCEPTION 'lane_profile cursor_cloud was not refreshed (% rows)', refreshed
            USING ERRCODE = 'data_exception';
    END IF;
    UPDATE mission_control.lane_profile
       SET describe = '{"approval_modes":["workflow_gate","provider_permission","governed_effect"],"compaction_control":"unqualified","controls":{"cancel_turn":"native","end_session":"native","fork":"unqualified","observe":"native","pause":"unqualified","prepare":"native","reattach":"emulated","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"pause_at_tool_gate","queue_instruction":"turn_boundary_guaranteed","request_continuation":"emulated","resume":"turn_boundary_guaranteed"},"enforcement_coverage":{"file":"native","mcp":"native","shell":"native"},"features":{"approval_suspension":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"cancel":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"configuration_materialization":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"continuation":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"emulated","transport":"stdio_subprocess"},"environment_selection":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":false,"os":null,"provider_scope":null,"qualified":false,"qualified_at":null,"sdk_language":null,"sdk_version":null,"status":"unqualified","transport":null},"follow_up":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"launch":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"observe":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"output_custody":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"emulated","transport":"stdio_subprocess"},"status":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"subordinate_lineage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"},"usage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux","provider_scope":"anthropic:claude_code_local","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"0.2.165","status":"native","transport":"stdio_subprocess"}},"hooks":{"events_supported":["session_start","before_prompt","before_tool","after_tool","after_tool_failure","before_shell","after_shell","before_mcp","after_file_edit","before_compaction","after_compaction","subagent_start","subagent_stop","stop","session_end"],"fail_closed":true,"mechanism":"sdk_callbacks+command_hooks"},"identity":{"cursor":"session_log_ordinal","effect_ref":"tool_use_id","session_ref":"session_id","turn_ref":"user_message_uuid"},"instruction_channel":["CLAUDE.md","prompt",".claude/settings.json"],"lane":"claude","lane_profile":"claude_agent_sdk","placement":"worker_hosted","qualified":false,"schema_version":"mc.lane_describe.v2","subagents":{"file":".claude/agents/*.md","inline":"ClaudeAgentOptions.agents","readonly_supported_inline":true},"subordinate_visibility":"lifecycle_only","usage":{"cost":"estimated","tokens":"settled_per_turn"},"versions":{"claude_agent_sdk":"0.2.165","claude_code_bundled":"2.1.294","transport":"stdio_subprocess"}}'::jsonb
     WHERE lane_profile = 'claude_agent_sdk' AND NOT qualified;
    GET DIAGNOSTICS refreshed = ROW_COUNT;
    IF refreshed <> 1 THEN
        RAISE EXCEPTION 'lane_profile claude_agent_sdk was not refreshed (% rows)', refreshed
            USING ERRCODE = 'data_exception';
    END IF;
    UPDATE mission_control.lane_profile
       SET describe = '{"approval_modes":["workflow_gate","provider_permission","provider_question","mcp_elicitation","governed_effect"],"compaction_control":"native","controls":{"cancel_turn":"native","end_session":"native","fork":"unqualified","observe":"native","pause":"emulated","prepare":"native","reattach":"emulated","send_turn":"native","snapshot":"emulated","start":"native","usage":"native"},"delivery_semantics":{"cancel":"turn_boundary_guaranteed","fork":"emulated","hard_pause":"unsupported","interrupt_and_inject":"cancel_and_replace","pause":"pause_at_tool_gate","queue_instruction":"wait_then_send","request_continuation":"emulated","resume":"wait_then_send"},"enforcement_coverage":{"file":"native","mcp":"emulated","shell":"native"},"features":{"approval_suspension":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"cancel":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"configuration_materialization":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"continuation":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"emulated","transport":"app-server jsonrpc v2 over stdio"},"environment_selection":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":false,"os":null,"provider_scope":null,"qualified":false,"qualified_at":null,"sdk_language":null,"sdk_version":null,"status":"unsupported","transport":null},"follow_up":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"launch":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"observe":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"output_custody":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"emulated","transport":"app-server jsonrpc v2 over stdio"},"status":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"},"subordinate_lineage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"emulated","transport":"app-server jsonrpc v2 over stdio"},"usage":{"account_enabled":"unknown","deployment_digest":null,"evidence_ref":null,"implemented":true,"os":"linux (WSL)","provider_scope":"local worker, owner account","qualified":false,"qualified_at":null,"sdk_language":"python","sdk_version":"codex-cli 0.162.0","status":"native","transport":"app-server jsonrpc v2 over stdio"}},"hooks":{"events_supported":["session_start","before_prompt","before_tool","after_tool","before_shell","after_shell","before_mcp","after_file_edit","before_compaction","after_compaction","subagent_start","subagent_stop","stop","session_end"],"fail_closed":true,"mechanism":"command_hooks"},"identity":{"cursor":"turn_id@connection_epoch:event_seq","effect_ref":"item_id","session_ref":"thread_id","turn_ref":"turn_id"},"instruction_channel":["AGENTS.md",".codex/config.toml","developer_instructions"],"lane":"codex","lane_profile":"codex","placement":"worker_hosted","qualified":false,"schema_version":"mc.lane_describe.v2","subagents":{"file":".codex/agents/*.toml","inline":null,"readonly_supported_inline":false},"subordinate_visibility":"lifecycle_only","usage":{"cost":"estimated","tokens":"settled_per_turn"},"versions":{"app_server_protocol":"v2","app_server_schema_sha256":"0bf5254bede109d4ae03ce2e81372e4c93a30b359c0749ec7dce7a9382a7f857","codex_cli":"0.162.0"}}'::jsonb
     WHERE lane_profile = 'codex' AND NOT qualified;
    GET DIAGNOSTICS refreshed = ROW_COUNT;
    IF refreshed <> 1 THEN
        RAISE EXCEPTION 'lane_profile codex was not refreshed (% rows)', refreshed
            USING ERRCODE = 'data_exception';
    END IF;
END
$lane_describes$;
ALTER TABLE mission_control.lane_profile ENABLE TRIGGER lane_profile_immutable;
ALTER TABLE mission_control.lane_profile FORCE ROW LEVEL SECURITY;
-- end section: lane describe refresh
