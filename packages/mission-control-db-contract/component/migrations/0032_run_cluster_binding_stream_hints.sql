-- mission-control-db-contract: multi-provider wave-3 integration (SPEC-01 "Temporal
-- integration", SPEC-04 "Replay algorithm"; MP-22 delta 1, MP-14 delta 4). Extends 0003, 0005
-- and 0027. Additive only: no existing row, column, policy or grant changes.
--
-- 1. `run_cluster_binding`: the Temporal cluster a run (by its application `run_key`, the
--    identity the launch service holds) was bound to at its first admitted launch, written
--    once by `RunLaunchService.launch` before it submits. A later launch of the
--    same run from a service bound to another cluster is refused (`run_bound_to_other_cluster`),
--    so an outage drill never starts a second copy of an active mission in another cluster.
--    `preflight guard-launch` keeps a file ledger of the same shape for operators.
-- 2. Stream hints: AFTER INSERT triggers on `mission_event` and `provider_frame` notify the
--    `mc_stream_hint` channel with the request scope and the mission or harness execution id
--    only (never event data). The `/missions` socket (`bootstrap/realtime.py`) wakes its pumps
--    on a hint and still replays from PostgreSQL, so a missed notification delays, never loses.

-- ---------------------------------------------------------------------------------------
-- section: MP-22 run cluster binding
CREATE TABLE mission_control.run_cluster_binding (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_key text NOT NULL CHECK (run_key <> ''),
    workflow_id text NOT NULL CHECK (workflow_id <> '' AND length(workflow_id) <= 1024),
    cluster_id text NOT NULL CHECK (cluster_id <> '' AND length(cluster_id) <= 512),
    temporal_target text NOT NULL CHECK (temporal_target IN ('local', 'cloud')),
    temporal_address text NOT NULL CHECK (temporal_address <> '' AND length(temporal_address) <= 512),
    temporal_namespace text NOT NULL
        CHECK (temporal_namespace <> '' AND length(temporal_namespace) <= 512),
    task_queue text NOT NULL CHECK (task_queue <> '' AND length(task_queue) <= 512),
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key)
        REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
ALTER TABLE mission_control.run_cluster_binding ENABLE ROW LEVEL SECURITY;
ALTER TABLE mission_control.run_cluster_binding FORCE ROW LEVEL SECURITY;
CREATE POLICY run_cluster_binding_scope ON mission_control.run_cluster_binding
    USING (installation_id = mission_control.ctx_installation_id()
           AND application_id = mission_control.ctx_application_id()
           AND tenant_id = mission_control.ctx_tenant_id())
    WITH CHECK (installation_id = mission_control.ctx_installation_id()
           AND application_id = mission_control.ctx_application_id()
           AND tenant_id = mission_control.ctx_tenant_id());
CREATE TRIGGER run_cluster_binding_immutable
    BEFORE UPDATE OR DELETE ON mission_control.run_cluster_binding
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();
REVOKE ALL ON mission_control.run_cluster_binding FROM PUBLIC;
GRANT SELECT, INSERT ON mission_control.run_cluster_binding TO mission_control_runtime;
GRANT SELECT ON mission_control.run_cluster_binding TO mission_control_readonly;
-- end section: MP-22

-- ---------------------------------------------------------------------------------------
-- section: MP-14 stream hints (LISTEN/NOTIFY wake-ups; the ledger stays the tables)
CREATE FUNCTION mission_control.notify_stream_hint() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_TABLE_NAME = 'mission_event' THEN
        PERFORM pg_notify('mc_stream_hint', json_build_object(
            'scope', 'mc/' || NEW.installation_id || '/' || NEW.application_id || '/' || NEW.tenant_id,
            'mission_id', NEW.mission_id)::text);
    ELSE
        PERFORM pg_notify('mc_stream_hint', json_build_object(
            'scope', 'mc/' || NEW.installation_id || '/' || NEW.application_id || '/' || NEW.tenant_id,
            'execution_id', NEW.harness_execution_id)::text);
    END IF;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.notify_stream_hint() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mission_control.notify_stream_hint()
    TO mission_control_runtime, mission_control_family_writer, mission_control_outbox_worker;
CREATE TRIGGER mission_event_stream_hint
    AFTER INSERT ON mission_control.mission_event
    FOR EACH ROW EXECUTE FUNCTION mission_control.notify_stream_hint();
CREATE TRIGGER provider_frame_stream_hint
    AFTER INSERT ON mission_control.provider_frame
    FOR EACH ROW EXECUTE FUNCTION mission_control.notify_stream_hint();
-- end section: MP-14
