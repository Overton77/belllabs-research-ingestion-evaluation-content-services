-- mission-control-db-contract common release: run snapshots, semantic forks and lineage.
-- A sealed run snapshot is a canonical continuation_checkpoint (with a 'valid'
-- checkpoint_validation); a fork is a canonical recovery_request of kind 'fork' and, once
-- materialized, a fork_lineage row. Support keeps the snapshot boundary detail, the fork
-- saga (claims, admission, receipt, materialization), per-unit reuse decisions and the
-- immutable execution-lineage journal.

CREATE TABLE mission_control.run_snapshot (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_snapshot_id uuid PRIMARY KEY,
    snapshot_key text NOT NULL CHECK (snapshot_key ~ '^run-snapshot:[0-9a-f]{64}$'),
    checkpoint_id uuid NOT NULL,
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.run-snapshot.v1'),
    source_run_key text NOT NULL CHECK (source_run_key <> ''),
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    family text NOT NULL CHECK (family IN ('stage_graph', 'goal_directed')),
    boundary_kind text NOT NULL CHECK (boundary_kind IN ('stage_settled', 'goal_verifier_settled')),
    projection_version bigint NOT NULL CHECK (projection_version >= 1),
    snapshot_digest text NOT NULL CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    taken_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, run_snapshot_id),
    UNIQUE (installation_id, application_id, tenant_id, snapshot_key),
    UNIQUE (installation_id, application_id, tenant_id, snapshot_digest),
    UNIQUE (installation_id, application_id, tenant_id, checkpoint_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, checkpoint_id) REFERENCES mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, checkpoint_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX run_snapshot_source_idx ON mission_control.run_snapshot (installation_id, application_id, tenant_id, source_run_key, projection_version);
CREATE TRIGGER run_snapshot_immutable
    BEFORE UPDATE OR DELETE ON mission_control.run_snapshot
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.fork_request (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    fork_request_id uuid PRIMARY KEY,
    request_key text NOT NULL CHECK (request_key <> ''),
    recovery_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.run-fork-request.v2'),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    request_payload jsonb NOT NULL CHECK (jsonb_typeof(request_payload) = 'object'),
    admission_payload jsonb CHECK (admission_payload IS NULL OR jsonb_typeof(admission_payload) = 'object'),
    receipt_payload jsonb CHECK (receipt_payload IS NULL OR jsonb_typeof(receipt_payload) = 'object'),
    materialization_payload jsonb CHECK (materialization_payload IS NULL OR jsonb_typeof(materialization_payload) = 'object'),
    status text NOT NULL CHECK (status IN ('reserved', 'admitting', 'admitted', 'copying', 'accepted')),
    source_run_key text NOT NULL CHECK (source_run_key <> ''),
    source_snapshot_key text NOT NULL,
    patch_digest text NOT NULL CHECK (patch_digest ~ '^sha256:[0-9a-f]{64}$'),
    target_run_key text NOT NULL CHECK (target_run_key <> ''),
    requested_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (retain_until > requested_at),
    UNIQUE (installation_id, application_id, tenant_id, fork_request_id),
    UNIQUE (installation_id, application_id, tenant_id, request_key),
    UNIQUE (installation_id, application_id, tenant_id, idempotency_key),
    UNIQUE (installation_id, application_id, tenant_id, target_run_key),
    UNIQUE (installation_id, application_id, tenant_id, recovery_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, recovery_id) REFERENCES mission_control.recovery_request (installation_id, application_id, tenant_id, recovery_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_snapshot_key) REFERENCES mission_control.run_snapshot (installation_id, application_id, tenant_id, snapshot_key)
);
CREATE INDEX fork_request_snapshot_fk_idx ON mission_control.fork_request (installation_id, application_id, tenant_id, source_snapshot_key);
CREATE INDEX fork_request_retention_idx ON mission_control.fork_request (installation_id, application_id, tenant_id, retain_until, request_key);

CREATE TABLE mission_control.fork_reuse_decision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    fork_reuse_decision_id uuid PRIMARY KEY,
    fork_request_key text NOT NULL CHECK (fork_request_key <> ''),
    derived_run_key text NOT NULL CHECK (derived_run_key <> ''),
    derived_unit_key text NOT NULL CHECK (derived_unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    source_run_key text NOT NULL CHECK (source_run_key <> ''),
    source_unit_key text NOT NULL CHECK (source_unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.fork-reuse-decision.v1'),
    decision text NOT NULL CHECK (decision IN ('reuse', 'invalidated', 'not_reusable', 'excluded')),
    reason text NOT NULL CHECK (reason <> ''),
    result_manifest_ref text,
    result_manifest_digest text CHECK (result_manifest_digest IS NULL OR result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    decision_digest text NOT NULL CHECK (decision_digest ~ '^sha256:[0-9a-f]{64}$'),
    decision_payload jsonb NOT NULL CHECK (jsonb_typeof(decision_payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK ((decision = 'reuse') = (result_manifest_ref IS NOT NULL)),
    CHECK ((result_manifest_ref IS NULL) = (result_manifest_digest IS NULL)),
    CHECK (derived_run_key <> source_run_key),
    UNIQUE (installation_id, application_id, tenant_id, fork_reuse_decision_id),
    UNIQUE (installation_id, application_id, tenant_id, fork_request_key, derived_unit_key),
    UNIQUE (installation_id, application_id, tenant_id, derived_run_key, derived_unit_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, fork_request_key) REFERENCES mission_control.fork_request (installation_id, application_id, tenant_id, request_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, derived_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE TRIGGER fork_reuse_decision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.fork_reuse_decision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.execution_lineage_record (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    execution_lineage_record_id uuid PRIMARY KEY,
    lineage_key text NOT NULL CHECK (lineage_key <> ''),
    run_key text NOT NULL CHECK (run_key <> ''),
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    lineage_digest text NOT NULL CHECK (lineage_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_manifest_ref text,
    lineage_payload jsonb NOT NULL CHECK (jsonb_typeof(lineage_payload) = 'object'),
    recorded_at timestamptz NOT NULL,
    retain_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (retain_until > recorded_at),
    UNIQUE (installation_id, application_id, tenant_id, execution_lineage_record_id),
    UNIQUE (installation_id, application_id, tenant_id, lineage_key),
    UNIQUE (installation_id, application_id, tenant_id, lineage_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX execution_lineage_record_run_fk_idx ON mission_control.execution_lineage_record (installation_id, application_id, tenant_id, run_key);
CREATE INDEX execution_lineage_record_result_idx ON mission_control.execution_lineage_record (installation_id, application_id, tenant_id, result_manifest_ref, recorded_at) WHERE result_manifest_ref IS NOT NULL;
CREATE TRIGGER execution_lineage_record_immutable
    BEFORE UPDATE OR DELETE ON mission_control.execution_lineage_record
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.execution_lineage_edge (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    execution_lineage_edge_id uuid PRIMARY KEY,
    lineage_key text NOT NULL CHECK (lineage_key <> ''),
    parent_identity_key text NOT NULL,
    child_identity_key text NOT NULL,
    relationship text NOT NULL CHECK (relationship IN ('contains', 'attempt_of', 'invokes', 'spawns', 'produces', 'traces', 'claims', 'derived_from', 'seeded_from', 'reuses')),
    edge_digest text NOT NULL CHECK (edge_digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (parent_identity_key <> child_identity_key),
    UNIQUE (installation_id, application_id, tenant_id, execution_lineage_edge_id),
    UNIQUE (installation_id, application_id, tenant_id, lineage_key, parent_identity_key, child_identity_key, relationship),
    UNIQUE (installation_id, application_id, tenant_id, edge_digest),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, lineage_key) REFERENCES mission_control.execution_lineage_record (installation_id, application_id, tenant_id, lineage_key)
);
CREATE TRIGGER execution_lineage_edge_immutable
    BEFORE UPDATE OR DELETE ON mission_control.execution_lineage_edge
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'run_snapshot',
        'fork_request',
        'fork_reuse_decision',
        'execution_lineage_record',
        'execution_lineage_edge'
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
    END LOOP;
END
$policies$;

REVOKE ALL ON mission_control.run_snapshot, mission_control.fork_request,
    mission_control.fork_reuse_decision, mission_control.execution_lineage_record,
    mission_control.execution_lineage_edge FROM PUBLIC;

-- Port of the legacy control runtime role: snapshots, decisions and lineage are insert-only; the
-- fork saga row changes only its claim status and recorded payloads (no DELETE: the
-- retention deletion path is retired with its tests-only repository).
GRANT SELECT, INSERT ON mission_control.continuation_checkpoint,
    mission_control.checkpoint_validation, mission_control.fork_lineage,
    mission_control.run_snapshot, mission_control.fork_reuse_decision,
    mission_control.execution_lineage_record, mission_control.execution_lineage_edge
TO mission_control_runtime;
GRANT SELECT, INSERT, UPDATE ON mission_control.recovery_request, mission_control.fork_request
TO mission_control_runtime;
GRANT SELECT ON mission_control.continuation_checkpoint, mission_control.checkpoint_validation,
    mission_control.recovery_request, mission_control.fork_lineage, mission_control.run_snapshot,
    mission_control.fork_request, mission_control.fork_reuse_decision,
    mission_control.execution_lineage_record, mission_control.execution_lineage_edge
TO mission_control_readonly;
