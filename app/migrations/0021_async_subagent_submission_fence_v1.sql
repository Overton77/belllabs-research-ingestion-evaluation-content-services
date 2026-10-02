-- AMD-RRM-001 forward-only migration (RRM-013): async subagent submission fence, lifecycle
-- mirror, in_doubt incidents, the adopt_provider_run / orphan_child decisions, and every
-- provider run observed for a child (REQ-CP-DA-008, REQ-CP-DA-011, REQ-CP-DA-019,
-- REQ-CP-RUN-009). Schema identities: belllabs.async-provider-run.v1 and
-- belllabs.async-subagent-incident.v1 (stored in runtime_reconciliation_incidents under
-- incident_type 'async_submission_in_doubt'). Rows hold identities, statuses and amounts only:
-- never thread content, prompts, outputs or secrets. The 0016 tables are reused (RRM-001
-- disposition row 54); nothing is renamed.

-- REQ-CP-DA-008: submission is fenced per child. A submitter holds the fence under a lease; a
-- later submitter takes an expired or released lease over only by advancing the fence.
ALTER TABLE belllabs_control.async_subagent_authority
    ADD COLUMN lifecycle text NOT NULL DEFAULT 'admitted' CHECK (
        lifecycle IN (
            'proposed', 'admitted', 'submitted', 'running', 'waiting', 'completed', 'failed',
            'cancelled', 'orphaned', 'in_doubt'
        )
    ),
    ADD COLUMN provider_thread_id text,
    ADD COLUMN provider_run_id text,
    ADD COLUMN submission_fence bigint NOT NULL DEFAULT 0 CHECK (submission_fence >= 0),
    ADD COLUMN submission_holder text,
    ADD COLUMN submission_lease_expires_at timestamptz,
    ADD COLUMN in_doubt_reason text CHECK (
        in_doubt_reason IS NULL OR in_doubt_reason IN (
            'submission_unobservable', 'multiple_provider_runs', 'graph_identity_mismatch',
            'provider_binding_lost'
        )
    ),
    ADD COLUMN incident_id text,
    ADD COLUMN lifecycle_updated_at timestamptz,
    ADD CONSTRAINT async_subagent_authority_submission_lease_shape
        CHECK ((submission_holder IS NULL) = (submission_lease_expires_at IS NULL)),
    ADD CONSTRAINT async_subagent_authority_provider_binding_shape
        CHECK ((provider_thread_id IS NULL) = (provider_run_id IS NULL)),
    ADD CONSTRAINT async_subagent_authority_in_doubt_shape
        CHECK ((lifecycle = 'in_doubt') = (in_doubt_reason IS NOT NULL));

-- REQ-CP-DA-008 operator decisions join the immutable command ledger.
ALTER TABLE belllabs_control.async_subagent_commands
    DROP CONSTRAINT IF EXISTS async_subagent_commands_command_kind_check;
ALTER TABLE belllabs_control.async_subagent_commands
    ADD CONSTRAINT async_subagent_commands_command_kind_check CHECK (
        command_kind IN (
            'admit', 'cancel', 'result_decision', 'settle', 'adopt_provider_run', 'orphan_child'
        )
    );

-- REQ-CP-DA-011 / REQ-CP-RUN-009: every provider run observed for a child, including the
-- duplicates an adopt_provider_run cancels and the runs an orphan_child cancels, with its
-- usage attributed, pending or ambiguous. Nothing is dropped.
CREATE TABLE belllabs_control.async_subagent_provider_runs (
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    provider_run_id text NOT NULL,
    schema_version text NOT NULL CHECK (schema_version = 'belllabs.async-provider-run.v1'),
    provider_thread_id text NOT NULL,
    disposition text NOT NULL CHECK (
        disposition IN ('bound', 'duplicate_cancelled', 'orphaned_cancelled')
    ),
    provider_status text NOT NULL,
    usage_attribution text NOT NULL CHECK (
        usage_attribution IN ('provider_attributed', 'pending', 'ambiguous')
    ),
    attributed_amounts jsonb NOT NULL DEFAULT '{}'::jsonb,
    pending_amounts jsonb NOT NULL DEFAULT '{}'::jsonb,
    record_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, child_execution_id, provider_run_id),
    FOREIGN KEY (request_scope, child_execution_id)
        REFERENCES belllabs_control.async_subagent_authority(request_scope, child_execution_id)
);

CREATE INDEX async_subagent_provider_runs_child_idx
    ON belllabs_control.async_subagent_provider_runs (request_scope, child_execution_id, observed_at);

-- REQ-CP-RUN-011: inspection reads child lineage by parent run.
CREATE INDEX async_subagent_authority_lifecycle_idx
    ON belllabs_control.async_subagent_authority (request_scope, parent_run_id, lifecycle);

-- Typed in_doubt incidents of async children are looked up by child.
CREATE INDEX runtime_reconciliation_incidents_async_child_idx
    ON belllabs_control.runtime_reconciliation_incidents (
        request_scope, incident_type, (incident_payload->>'child_execution_id')
    )
    WHERE incident_type = 'async_submission_in_doubt';

ALTER TABLE belllabs_control.async_subagent_provider_runs ENABLE ROW LEVEL SECURITY;
CREATE POLICY async_subagent_provider_runs_scope ON belllabs_control.async_subagent_provider_runs
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
ALTER TABLE belllabs_control.async_subagent_provider_runs FORCE ROW LEVEL SECURITY;

-- Least privilege, consistent with 0016 and 0020: the authority row is updated (fence,
-- lifecycle mirror) under its existing UPDATE grant; provider runs are insert-only with an
-- UPDATE grant for the disposition and status of an observed run; incidents keep 0014 grants.
GRANT SELECT, INSERT, UPDATE
    ON belllabs_control.async_subagent_provider_runs
    TO belllabs_control_runtime;
GRANT SELECT
    ON belllabs_control.async_subagent_provider_runs
    TO belllabs_operations_readonly;
