-- AMD-RRM-001 forward-only migration (RRM-006): safe macro snapshots and semantic forks.
-- Schema identities: belllabs.run-snapshot.v1 (`RunSnapshotManifest`, REQ-CP-EXEC-016),
-- belllabs.run-fork-request.v2 and belllabs.run-fork-receipt.v2 (the versioned fork saga,
-- REQ-CP-EXEC-012), belllabs.fork-reuse-decision.v1 (the recorded reuse frontier), and
-- belllabs.fork-lineage-manifest.v1 (fork lineage, carried in the receipt and materialization).
-- Rows hold identities, digests, refs and decisions only: never checkpoint bodies, transcripts,
-- prompts, outputs, or secrets. Numbers 0021 and 0023 belong to concurrent tickets.

-- REQ-CP-EXEC-016: one immutable manifest per run version and declared boundary. The
-- snapshot is insert-only and content-addressed; it is distinct from the workspace/sandbox
-- snapshots of CON-CP-SNAPSHOT-V1 (REQ-CP-DA-015), which it may only reference.
CREATE TABLE belllabs_control.run_snapshot_manifests (
    request_scope text NOT NULL,
    snapshot_id text NOT NULL CHECK (snapshot_id ~ '^run-snapshot:[0-9a-f]{64}$'),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.run-snapshot.v1'),
    source_run_id text NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    family text NOT NULL CHECK (family IN ('stage_graph', 'goal_directed')),
    boundary_kind text NOT NULL CHECK (
        boundary_kind IN ('stage_settled', 'goal_verifier_settled')
    ),
    projection_version bigint NOT NULL CHECK (projection_version >= 1),
    snapshot_digest text NOT NULL CHECK (snapshot_digest ~ '^sha256:[0-9a-f]{64}$'),
    manifest jsonb NOT NULL,
    taken_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, snapshot_id),
    UNIQUE (request_scope, snapshot_digest),
    FOREIGN KEY (request_scope, source_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id)
);
CREATE INDEX run_snapshot_manifests_source_idx
    ON belllabs_control.run_snapshot_manifests (request_scope, source_run_id, projection_version);

-- RRM-001 disposition row 51: version the fork saga storage. A v2 row binds the source run,
-- its snapshot, the patch digest and the derived run; `source_binding_id` (the retired
-- Agent Server binding, nullable since 0019) stays NULL. The `copying` status of the saga is
-- the v2 materialization claim (lineage and reuse decisions; nothing is copied).
ALTER TABLE belllabs_control.runtime_fork_requests
    ADD COLUMN schema_version text CHECK (
        schema_version IS NULL OR schema_version = 'belllabs.run-fork-request.v2'
    ),
    ADD COLUMN source_run_id text,
    ADD COLUMN source_snapshot_id text,
    ADD COLUMN patch_digest text CHECK (
        patch_digest IS NULL OR patch_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    ADD COLUMN target_run_id text,
    ADD COLUMN materialization_payload jsonb,
    ADD CONSTRAINT runtime_fork_requests_v2_shape CHECK (
        schema_version IS NULL
        OR (
            source_run_id IS NOT NULL
            AND source_snapshot_id IS NOT NULL
            AND patch_digest IS NOT NULL
            AND target_run_id IS NOT NULL
            AND source_binding_id IS NULL
        )
    ),
    ADD CONSTRAINT runtime_fork_requests_materialized_shape CHECK (
        materialization_payload IS NULL OR schema_version IS NOT NULL
    ),
    ADD CONSTRAINT runtime_fork_requests_snapshot_fk
        FOREIGN KEY (request_scope, source_snapshot_id)
        REFERENCES belllabs_control.run_snapshot_manifests(request_scope, snapshot_id);
-- One fork per derived run: the derived run's units find their reuse decisions by run.
CREATE UNIQUE INDEX runtime_fork_requests_target_run_idx
    ON belllabs_control.runtime_fork_requests (request_scope, target_run_id)
    WHERE target_run_id IS NOT NULL;

-- REQ-CP-EXEC-012: every recorded reuse decision, keyed by the derived unit whose
-- CON-CP-RUNTIME-UNIT-V1 identity equals the source unit's after substituting the run and
-- epoch. A `reuse` decision names the immutable source result manifest; it never re-settles
-- that result in the source run. Insert-only.
CREATE TABLE belllabs_control.run_fork_reuse_decisions (
    request_scope text NOT NULL,
    fork_request_id text NOT NULL,
    derived_run_id text NOT NULL,
    derived_unit_key text NOT NULL CHECK (derived_unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    source_run_id text NOT NULL,
    source_unit_key text NOT NULL CHECK (source_unit_key ~ '^bl-unit-v1:[0-9a-f]{64}$'),
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.fork-reuse-decision.v1'),
    decision text NOT NULL CHECK (
        decision IN ('reuse', 'invalidated', 'not_reusable', 'excluded')
    ),
    reason text NOT NULL,
    result_manifest_ref text,
    result_manifest_digest text CHECK (
        result_manifest_digest IS NULL OR result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    decision_digest text NOT NULL CHECK (decision_digest ~ '^sha256:[0-9a-f]{64}$'),
    decision_payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, fork_request_id, derived_unit_key),
    UNIQUE (request_scope, derived_run_id, derived_unit_key),
    -- An audited fork retention purge (0014 `runtime_retention_deletion_audit`, class
    -- `fork`) removes the fork's decisions with it.
    FOREIGN KEY (request_scope, fork_request_id)
        REFERENCES belllabs_control.runtime_fork_requests(request_scope, request_id)
        ON DELETE CASCADE,
    FOREIGN KEY (request_scope, derived_run_id)
        REFERENCES belllabs_control.workflow_runs(request_scope, run_id),
    CHECK ((decision = 'reuse') = (result_manifest_ref IS NOT NULL)),
    CHECK ((result_manifest_ref IS NULL) = (result_manifest_digest IS NULL)),
    CHECK (derived_run_id <> source_run_id)
);

-- RRM-001 disposition rows 30 and 49: additive lineage relationships for forks.
DO $$
DECLARE constraint_record record;
BEGIN
    FOR constraint_record IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'belllabs_control.runtime_lineage_edges'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%relationship%'
    LOOP
        EXECUTE format(
            'ALTER TABLE belllabs_control.runtime_lineage_edges DROP CONSTRAINT %I',
            constraint_record.conname
        );
    END LOOP;
END
$$;
ALTER TABLE belllabs_control.runtime_lineage_edges
    ADD CONSTRAINT runtime_lineage_edges_relationship_check CHECK (
        relationship IN (
            'contains', 'attempt_of', 'invokes', 'spawns', 'produces', 'traces', 'claims',
            'derived_from', 'seeded_from', 'reuses'
        )
    );

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'run_snapshot_manifests',
        'run_fork_reuse_decisions'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I ENABLE ROW LEVEL SECURITY', table_name
        );
        EXECUTE format(
            'CREATE POLICY request_scope_isolation ON belllabs_control.%I
             USING (request_scope = current_setting(''belllabs.request_scope'', true))
             WITH CHECK (request_scope = current_setting(''belllabs.request_scope'', true))',
            table_name
        );
        EXECUTE format(
            'ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name
        );
    END LOOP;
END
$$;

-- Least privilege: snapshots and reuse decisions are insert-only for the runtime role (no
-- UPDATE or DELETE); runtime_fork_requests keeps its 0014 grants. The read-only operations
-- role reads both for inspection and evidence.
GRANT SELECT, INSERT
    ON belllabs_control.run_snapshot_manifests,
       belllabs_control.run_fork_reuse_decisions
    TO belllabs_control_runtime;
GRANT SELECT
    ON belllabs_control.run_snapshot_manifests,
       belllabs_control.run_fork_reuse_decisions
    TO belllabs_operations_readonly;
