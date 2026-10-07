-- mission-control-db-contract common release: installation catalog support records
-- (catalog/artifacts lane). Published definitions are canonical asset_version rows
-- (contract 'mission-control.published-definition/1') with asset_decision history;
-- these support tables hold only drafts, alias pointers, immutable side records and
-- projection job state. Installation catalog scope: (installation_id, application_id),
-- no tenant. Missing context denies.

-- asset_version is append-only except for a forward-only lifecycle transition:
-- proposed -> admitted|rejected-by-revoke, admitted -> retired|revoked, retired -> revoked.
-- Identity and content columns never change; version_no advances by exactly one.
CREATE FUNCTION mission_control.asset_version_transition_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'mission_control.asset_version rows are never deleted'
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF (NEW.installation_id, NEW.application_id, NEW.asset_version_id, NEW.asset_id,
        NEW.version, NEW.kind, NEW.contract, NEW.manifest_ref, NEW.manifest_digest,
        NEW.manifest, NEW.required_compatibility, NEW.created_at, NEW.created_by_actor_ref)
       IS DISTINCT FROM
       (OLD.installation_id, OLD.application_id, OLD.asset_version_id, OLD.asset_id,
        OLD.version, OLD.kind, OLD.contract, OLD.manifest_ref, OLD.manifest_digest,
        OLD.manifest, OLD.required_compatibility, OLD.created_at, OLD.created_by_actor_ref)
       OR NEW.version_no <> OLD.version_no + 1
       OR NOT (
           (OLD.status = 'proposed' AND NEW.status IN ('admitted', 'revoked'))
           OR (OLD.status = 'admitted' AND NEW.status IN ('retired', 'revoked'))
           OR (OLD.status = 'retired' AND NEW.status = 'revoked')
       ) THEN
        RAISE EXCEPTION 'asset versions only accept one forward lifecycle transition'
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION mission_control.asset_version_transition_guard() FROM PUBLIC;
CREATE TRIGGER asset_version_transition_guard
    BEFORE UPDATE OR DELETE ON mission_control.asset_version
    FOR EACH ROW EXECUTE FUNCTION mission_control.asset_version_transition_guard();

-- Mutable authoring draft head (port of definition_catalog_heads). Published revision
-- is derived from asset_version, never stored twice. Writes are compare-and-swap on
-- draft_revision under the installation catalog advisory lock.
CREATE TABLE mission_control.catalog_head (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    catalog_head_id uuid PRIMARY KEY,
    definition_kind text NOT NULL CHECK (definition_kind ~ '^[a-z_]+$'),
    logical_id text NOT NULL CHECK (logical_id <> ''),
    draft_revision bigint NOT NULL CHECK (draft_revision >= 1),
    draft jsonb NOT NULL CHECK (jsonb_typeof(draft) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (version = draft_revision),
    UNIQUE (installation_id, application_id, catalog_head_id),
    UNIQUE (installation_id, application_id, definition_kind, logical_id),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);

-- Immutable catalog side records (port of definition_catalog_records minus the
-- published-definition/definition-retirement contracts, which are now
-- asset_version/asset_decision). record_key keeps the legacy opaque identity.
CREATE TABLE mission_control.catalog_record (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    catalog_record_id uuid PRIMARY KEY,
    contract text NOT NULL CHECK (contract IN (
        'alias-movement/1', 'effective-run-configuration/1', 'compilation-index/1',
        'catalog-event/1'
    )),
    record_key text NOT NULL CHECK (record_key <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, catalog_record_id),
    UNIQUE (installation_id, application_id, contract, record_key),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);
CREATE TRIGGER catalog_record_immutable
    BEFORE UPDATE OR DELETE ON mission_control.catalog_record
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

-- Alias pointer (port of definition_catalog_aliases). Movement is compare-and-swap on
-- (version, target_asset_version_id); every movement also appends an immutable
-- 'alias-movement/1' catalog_record. Targets are admitted published-definition assets.
CREATE TABLE mission_control.catalog_alias (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    catalog_alias_id uuid PRIMARY KEY,
    definition_kind text NOT NULL CHECK (definition_kind ~ '^[a-z_]+$'),
    logical_id text NOT NULL CHECK (logical_id <> ''),
    alias text NOT NULL CHECK (alias <> ''),
    target_asset_version_id uuid NOT NULL,
    binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((binding->'alias_ref'->>'kind' = definition_kind
        AND binding->'alias_ref'->>'logical_id' = logical_id
        AND binding->'alias_ref'->>'alias' = alias) IS TRUE),
    UNIQUE (installation_id, application_id, catalog_alias_id),
    UNIQUE (installation_id, application_id, definition_kind, logical_id, alias),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id),
    FOREIGN KEY (installation_id, application_id, target_asset_version_id)
        REFERENCES mission_control.asset_version (installation_id, application_id, asset_version_id)
);
CREATE INDEX catalog_alias_target_fk_idx ON mission_control.catalog_alias
    (installation_id, application_id, target_asset_version_id);

-- Leased projection job per immutable catalog event (port of
-- catalog_projection_processing). One row per event; state transitions under
-- FOR UPDATE SKIP LOCKED claims with lease fencing on (lease_owner, attempt_count).
CREATE TABLE mission_control.catalog_projection_job (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    projection_job_id uuid PRIMARY KEY,
    event_key text NOT NULL CHECK (event_key ~ '^sha256:[0-9a-f]{64}$'),
    definition_kind text NOT NULL CHECK (definition_kind ~ '^[a-z_]+$'),
    logical_id text NOT NULL CHECK (logical_id <> ''),
    revision bigint NOT NULL CHECK (revision >= 1),
    source_digest text NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('pending', 'processing', 'retry', 'completed', 'poison')),
    attempt_count integer NOT NULL CHECK (attempt_count >= 0),
    lease_owner text CHECK (lease_owner <> ''),
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((payload->>'event_id' = event_key
        AND payload->>'state' = state
        AND (payload->>'attempt_count')::integer = attempt_count
        AND payload->>'asset_kind' = definition_kind
        AND payload->>'logical_id' = logical_id
        AND (payload->>'revision')::bigint = revision
        AND payload->>'source_digest' = source_digest) IS TRUE),
    CHECK ((state = 'processing') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
    UNIQUE (installation_id, application_id, projection_job_id),
    UNIQUE (installation_id, application_id, event_key),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);
-- The job's event is the immutable 'catalog-event/1' catalog_record with record_key =
-- event_key; the repository verifies its payload digest before enqueueing.
CREATE INDEX catalog_projection_job_due_idx ON mission_control.catalog_projection_job
    (installation_id, application_id, next_attempt_at, created_at)
    WHERE state IN ('pending', 'retry', 'processing');
CREATE INDEX catalog_projection_job_ref_idx ON mission_control.catalog_projection_job
    (installation_id, application_id, definition_kind, logical_id, revision);

-- Immutable operational alert for a poisoned projection job.
CREATE TABLE mission_control.catalog_projection_alert (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    projection_alert_id uuid PRIMARY KEY,
    alert_key text NOT NULL CHECK (alert_key ~ '^sha256:[0-9a-f]{64}$'),
    event_key text NOT NULL CHECK (event_key ~ '^sha256:[0-9a-f]{64}$'),
    error_code text NOT NULL CHECK (error_code <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((payload->>'alert_id' = alert_key AND payload->>'event_id' = event_key) IS TRUE),
    UNIQUE (installation_id, application_id, projection_alert_id),
    UNIQUE (installation_id, application_id, alert_key),
    FOREIGN KEY (installation_id, application_id, event_key)
        REFERENCES mission_control.catalog_projection_job (installation_id, application_id, event_key)
);
CREATE TRIGGER catalog_projection_alert_immutable
    BEFORE UPDATE OR DELETE ON mission_control.catalog_projection_alert
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'catalog_head',
        'catalog_record',
        'catalog_alias',
        'catalog_projection_job',
        'catalog_projection_alert'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id())',
            table_name || '_scope', table_name);
    END LOOP;
END
$policies$;

REVOKE ALL ON mission_control.catalog_head, mission_control.catalog_record,
    mission_control.catalog_alias, mission_control.catalog_projection_job,
    mission_control.catalog_projection_alert FROM PUBLIC;
-- Publication runs through the restricted runtime pool (API/MCP/worker composition)
-- and the catalog writer (seed/admission tooling); both hold identical catalog grants.
GRANT SELECT, INSERT, UPDATE (draft_revision, draft, version, updated_at)
    ON mission_control.catalog_head TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT, INSERT ON mission_control.catalog_record
    TO mission_control_runtime, mission_control_catalog_writer;
GRANT SELECT, INSERT, UPDATE (target_asset_version_id, binding, version, updated_at)
    ON mission_control.catalog_alias TO mission_control_runtime, mission_control_catalog_writer;
-- Projection jobs: enqueued with publication; claimed/acknowledged by the runtime
-- (legacy projection processor role) or the dedicated outbox/projection worker.
GRANT SELECT, INSERT ON mission_control.catalog_projection_job
    TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker;
GRANT UPDATE (state, attempt_count, lease_owner, lease_expires_at, next_attempt_at, payload,
    version, updated_at)
    ON mission_control.catalog_projection_job
    TO mission_control_runtime, mission_control_outbox_worker;
GRANT SELECT, INSERT ON mission_control.catalog_projection_alert
    TO mission_control_runtime, mission_control_outbox_worker;
GRANT SELECT ON mission_control.catalog_record, mission_control.catalog_alias
    TO mission_control_outbox_worker;
GRANT SELECT ON mission_control.catalog_head, mission_control.catalog_record,
    mission_control.catalog_alias, mission_control.catalog_projection_job,
    mission_control.catalog_projection_alert TO mission_control_readonly;
