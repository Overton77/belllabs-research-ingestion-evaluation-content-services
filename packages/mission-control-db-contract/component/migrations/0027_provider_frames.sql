-- mission-control-db-contract common release: fast-track T2 (SPEC-02, SPEC-03).
-- Migration 0027 is shared by team T2: FT-C1 owns the provider frame store; FT-B2 owns the
-- Context Packet selection ledger. Each ticket writes only its own delimited section so the
-- integrator can concatenate them. Tenant scoped: composite (installation_id, application_id,
-- tenant_id) FK to tenant, ENABLE + FORCE RLS with the three-column context policy,
-- append-only triggers.

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
GRANT SELECT, INSERT ON mission_control.context_selection TO mission_control_runtime;
GRANT SELECT ON mission_control.context_selection TO mission_control_readonly;
-- end section: B2 ----------------------------------------------------------------------------
