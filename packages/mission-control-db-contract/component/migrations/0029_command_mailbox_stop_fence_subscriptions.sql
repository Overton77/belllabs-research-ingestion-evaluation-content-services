-- mission-control-db-contract: command mailbox, Stop Fence and subscriptions (SPEC-06).
-- Migration 0029 is shared by FT-F1 (mailbox), FT-F3 (stop fence) and FT-F5
-- (subscriptions); each ticket writes only its own delimited section and the integrator
-- concatenates them in section order (F1 mailbox, F3 stop fence, F5 subscriptions).

-- section: F3
-- Stop Fence of an immediate cancel (ADR-0008, ADR-0032; FT-F3). One insert-only row per
-- run execution generation, written before any provider cancel is attempted; a later
-- generation's fence leaves earlier rows for audit and nothing is ever deleted. Kernel
-- Hooks admit side effects against the fence under the same per-run transaction lock as
-- the fence write, recording each decision once per effect in
-- stop_fence_effect_admission, so an admission committed after the fence is always a
-- denial. stop_fence_milestone carries the provider-acknowledged and settled timestamps
-- of the immediate cancel's Delivery Report (requested and persisted are on the fence).
CREATE TABLE mission_control.stop_fence (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    stop_fence_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    generation integer NOT NULL CHECK (generation >= 1),
    command_id text NOT NULL CHECK (command_id <> ''),
    reason text NOT NULL CHECK (reason <> ''),
    requested_at timestamptz NOT NULL,
    fenced_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (fenced_at >= requested_at),
    UNIQUE (installation_id, application_id, tenant_id, stop_fence_id),
    UNIQUE (installation_id, application_id, tenant_id, run_id, generation),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX stop_fence_run_idx ON mission_control.stop_fence
    (installation_id, application_id, tenant_id, run_key, generation DESC);

CREATE TABLE mission_control.stop_fence_milestone (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    stop_fence_milestone_id uuid PRIMARY KEY,
    stop_fence_id uuid NOT NULL,
    milestone text NOT NULL CHECK (milestone IN ('provider_acknowledged', 'settled')),
    unit_key text NOT NULL DEFAULT '',
    recorded_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, stop_fence_milestone_id),
    UNIQUE (installation_id, application_id, tenant_id, stop_fence_id, milestone, unit_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, stop_fence_id)
        REFERENCES mission_control.stop_fence
            (installation_id, application_id, tenant_id, stop_fence_id)
);

CREATE TABLE mission_control.stop_fence_effect_admission (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    effect_admission_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    generation integer NOT NULL CHECK (generation >= 1),
    effect_ref text NOT NULL CHECK (effect_ref <> ''),
    effect_kind text NOT NULL
        CHECK (effect_kind IN ('shell', 'mcp', 'file', 'task', 'model', 'other')),
    lane_profile text NOT NULL CHECK (lane_profile <> ''),
    decision text NOT NULL CHECK (decision IN ('allow', 'deny')),
    reason_code text CHECK (reason_code = 'STOP_FENCED'),
    stop_fence_id uuid,
    decided_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((decision = 'deny') = (reason_code IS NOT NULL)),
    CHECK ((decision = 'deny') = (stop_fence_id IS NOT NULL)),
    UNIQUE (installation_id, application_id, tenant_id, effect_admission_id),
    UNIQUE (installation_id, application_id, tenant_id, run_id, generation, effect_ref),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, stop_fence_id)
        REFERENCES mission_control.stop_fence
            (installation_id, application_id, tenant_id, stop_fence_id)
);

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'stop_fence',
        'stop_fence_milestone',
        'stop_fence_effect_admission'
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
        EXECUTE pg_catalog.format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON mission_control.%I '
            'FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation()',
            table_name || '_immutable', table_name);
        EXECUTE pg_catalog.format('REVOKE ALL ON mission_control.%I FROM PUBLIC', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT, INSERT ON mission_control.%I TO mission_control_runtime', table_name);
        EXECUTE pg_catalog.format(
            'GRANT SELECT ON mission_control.%I TO mission_control_readonly', table_name);
    END LOOP;
END
$policies$;
-- end section: F3

-- section: F5
-- Subscriptions (FT-F5): durable consumers of committed mission events, delivered at least
-- once by the subscription relay with a per-subscription cursor. Channel rows carry
-- references only (webhook URL and secret_ref name, stream ticket id, MCP session ref);
-- secret values are resolved at send time and never stored.
CREATE TABLE mission_control.mission_subscription (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    subscription_id uuid PRIMARY KEY,
    target_kind text NOT NULL CHECK (target_kind IN ('mission', 'run')),
    mission_id uuid NOT NULL,
    run_id uuid,
    event_types text[] NOT NULL CHECK (cardinality(event_types) >= 1),
    node_keys text[] NOT NULL DEFAULT '{}',
    channel_kind text NOT NULL CHECK (channel_kind IN ('webhook', 'stream_ticket', 'mcp_session')),
    channel jsonb NOT NULL CHECK (jsonb_typeof(channel) = 'object'
        AND channel->>'kind' = channel_kind
        AND NOT (channel ?| ARRAY['secret', 'secret_value', 'password', 'token'])),
    cursor_seq bigint NOT NULL CHECK (cursor_seq >= 0),
    state text NOT NULL CHECK (state IN ('active', 'paused', 'dead_lettered', 'closed')),
    failure_count integer NOT NULL CHECK (failure_count >= 0),
    next_attempt_at timestamptz NOT NULL,
    lease_owner text CHECK (lease_owner <> ''),
    lease_expires_at timestamptz,
    dead_lettered_at timestamptz,
    closed_at timestamptz,
    closed_by_actor_ref text CHECK (closed_by_actor_ref <> ''),
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((target_kind = 'run') = (run_id IS NOT NULL)),
    CHECK ((state = 'dead_lettered') = (dead_lettered_at IS NOT NULL)),
    CHECK ((state = 'closed') = (closed_at IS NOT NULL)),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    UNIQUE (installation_id, application_id, tenant_id, subscription_id),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id)
        REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX mission_subscription_mission_id_fk_idx
    ON mission_control.mission_subscription (installation_id, application_id, tenant_id, mission_id);
CREATE INDEX mission_subscription_run_id_fk_idx
    ON mission_control.mission_subscription (installation_id, application_id, tenant_id, run_id);
CREATE INDEX mission_subscription_due_idx
    ON mission_control.mission_subscription (installation_id, application_id, tenant_id, next_attempt_at)
    WHERE state = 'active';

-- Immutable delivery receipts: one row per attempt. A relay that crashes after sending and
-- before recording redelivers the same event (same event_id) on its next pass.
CREATE TABLE mission_control.subscription_delivery (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    delivery_id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL,
    event_id uuid NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 1),
    attempt integer NOT NULL CHECK (attempt >= 1),
    status text NOT NULL CHECK (status IN ('delivered', 'failed', 'dead_lettered')),
    response_code integer CHECK (response_code BETWEEN 100 AND 599),
    latency_ms integer CHECK (latency_ms >= 0),
    error_class text CHECK (error_class <> ''),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, delivery_id),
    UNIQUE (installation_id, application_id, tenant_id, subscription_id, event_id, attempt),
    FOREIGN KEY (installation_id, application_id, tenant_id)
        REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subscription_id)
        REFERENCES mission_control.mission_subscription
            (installation_id, application_id, tenant_id, subscription_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, event_id)
        REFERENCES mission_control.mission_event (installation_id, application_id, tenant_id, event_id)
);
CREATE INDEX subscription_delivery_event_id_fk_idx
    ON mission_control.subscription_delivery (installation_id, application_id, tenant_id, event_id);
CREATE TRIGGER subscription_delivery_immutable
    BEFORE UPDATE OR DELETE ON mission_control.subscription_delivery
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Subscriptions are closed, never deleted.
CREATE TRIGGER mission_subscription_no_delete
    BEFORE DELETE ON mission_control.mission_subscription
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['mission_subscription', 'subscription_delivery'] LOOP
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

GRANT SELECT, INSERT ON mission_control.mission_subscription, mission_control.subscription_delivery
TO mission_control_runtime;
GRANT UPDATE (cursor_seq, state, failure_count, next_attempt_at, lease_owner, lease_expires_at,
    dead_lettered_at, closed_at, closed_by_actor_ref, version, updated_at)
ON mission_control.mission_subscription TO mission_control_runtime;
-- end section: F5
