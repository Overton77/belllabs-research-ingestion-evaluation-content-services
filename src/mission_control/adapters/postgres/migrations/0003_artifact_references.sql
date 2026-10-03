CREATE TABLE IF NOT EXISTS belllabs_control.durable_artifact_references (
    artifact_id text PRIMARY KEY,
    request_scope text NOT NULL,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    promotion_id text NOT NULL UNIQUE,
    metadata_revision bigint NOT NULL CHECK (metadata_revision >= 1),
    manifest_revision bigint NOT NULL CHECK (manifest_revision >= 1),
    content_digest text NOT NULL,
    object_ref text NOT NULL,
    durable_reference text NOT NULL UNIQUE,
    admitted_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS belllabs_control.artifact_reference_outbox (
    event_id text PRIMARY KEY,
    request_scope text NOT NULL,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    artifact_id text NOT NULL
        REFERENCES belllabs_control.durable_artifact_references(artifact_id),
    event_type text NOT NULL,
    envelope jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    delivery_attempts integer NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    delivered_at timestamptz NULL,
    UNIQUE (artifact_id, event_type)
);

CREATE INDEX IF NOT EXISTS artifact_reference_outbox_pending_idx
    ON belllabs_control.artifact_reference_outbox (recorded_at, event_id)
    WHERE delivered_at IS NULL;

ALTER TABLE belllabs_control.durable_artifact_references ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.artifact_reference_outbox ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS artifact_reference_scope_isolation
    ON belllabs_control.durable_artifact_references;
CREATE POLICY artifact_reference_scope_isolation
    ON belllabs_control.durable_artifact_references
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

DROP POLICY IF EXISTS artifact_outbox_scope_isolation
    ON belllabs_control.artifact_reference_outbox;
CREATE POLICY artifact_outbox_scope_isolation
    ON belllabs_control.artifact_reference_outbox
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.durable_artifact_references FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.artifact_reference_outbox FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT
    ON belllabs_control.durable_artifact_references,
       belllabs_control.artifact_reference_outbox
    TO belllabs_control_runtime;
GRANT UPDATE (delivery_attempts, delivered_at)
    ON belllabs_control.artifact_reference_outbox TO belllabs_control_runtime;
