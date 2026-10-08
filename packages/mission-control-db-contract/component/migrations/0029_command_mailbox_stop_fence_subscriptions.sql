-- mission-control-db-contract: command mailbox, Stop Fence and subscriptions (SPEC-06).
-- Migration 0029 is shared by FT-F1 (mailbox), FT-F3 (stop fence) and FT-F5
-- (subscriptions); each ticket writes only its own delimited section and the integrator
-- concatenates them in section order. This branch carries the FT-F3 section only.

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
