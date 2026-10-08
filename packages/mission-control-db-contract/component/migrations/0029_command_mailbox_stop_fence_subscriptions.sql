-- mission-control-db-contract common release: command mailbox, stop fence and subscriptions
-- (fast-track SPEC-06, ADR-0032). This file is assembled from per-ticket sections; each
-- section is delimited by "-- section: <ticket>" and owned by that ticket. Tenant scoped:
-- composite (installation_id, application_id, tenant_id) FK to tenant, ENABLE + FORCE RLS
-- with the three-column context policy.

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
