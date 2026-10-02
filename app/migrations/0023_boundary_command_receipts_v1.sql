-- AMD-RRM-001 forward-only migration (RRM-007): governed boundary interventions.
-- Schema identities: belllabs.boundary-command.v1 (the immutable, authorized command bound
-- to scope, target, expected version and generation) and belllabs.boundary-receipt.v1 (one
-- row per receipt transition: accepted -> delivered -> applied | rejected,
-- CON-CP-WORKFLOW-MESSAGE-V1 / REQ-CP-EXEC-006). Receipts in workflow memory are only a
-- transport de-duplication cache; these rows are the authoritative copy. Rows hold
-- identities, digests and typed reasons: never prompts, outputs, checkpoints or secrets.
-- Number 0021 is a concurrent ticket's and 0022 is RRM-005's; numbers need not be contiguous.

CREATE TABLE belllabs_control.boundary_commands (
    request_scope text NOT NULL,
    run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    command_id text NOT NULL,
    idempotency_issuer text NOT NULL,
    kind text NOT NULL CHECK (
        kind IN ('pause', 'resume', 'satisfy_wait', 'cancel', 'reconcile_unit')
    ),
    target_kind text NOT NULL CHECK (target_kind IN ('run_control', 'root', 'family', 'unit')),
    target_ref text NOT NULL,
    -- Commands routed through one root share one contiguous sequence space per run;
    -- `reconcile_unit` commands sequence per unit generation. 0 = rejected before sequencing.
    sequence_space text NOT NULL,
    target_sequence bigint NOT NULL CHECK (target_sequence >= 0),
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 1),
    execution_generation bigint NOT NULL CHECK (execution_generation >= 1),
    accepted_run_version bigint NOT NULL CHECK (accepted_run_version >= 1),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    command jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, run_id, command_id)
);

-- Monotonic, unique per-target sequencing (REQ-CP-EXEC-006); unsequenced rejections are 0.
CREATE UNIQUE INDEX boundary_commands_target_sequence_idx
    ON belllabs_control.boundary_commands (request_scope, run_id, sequence_space, target_sequence)
    WHERE target_sequence > 0;

CREATE TABLE belllabs_control.boundary_command_receipts (
    request_scope text NOT NULL,
    run_id text NOT NULL,
    command_id text NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 1),
    state text NOT NULL CHECK (state IN ('accepted', 'delivered', 'applied', 'rejected')),
    rejection_reason text CHECK (
        rejection_reason IS NULL OR rejection_reason IN (
            'stale_target', 'stale_generation', 'stale_version', 'not_applicable',
            'terminal_run', 'superseded', 'unauthorized', 'insufficient_budget'
        )
    ),
    recorded_by text NOT NULL,
    transport_ref text,
    applied_run_version bigint CHECK (applied_run_version IS NULL OR applied_run_version >= 1),
    receipt jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, run_id, command_id, ordinal),
    FOREIGN KEY (request_scope, run_id, command_id)
        REFERENCES belllabs_control.boundary_commands(request_scope, run_id, command_id),
    CONSTRAINT boundary_receipt_rejection_shape
        CHECK ((state = 'rejected') = (rejection_reason IS NOT NULL)),
    CONSTRAINT boundary_receipt_applied_shape
        CHECK (state = 'applied' OR applied_run_version IS NULL)
);

-- Pending delivery and application lookups read the latest receipt per command.
CREATE INDEX boundary_command_receipts_state_idx
    ON belllabs_control.boundary_command_receipts (request_scope, run_id, state);

ALTER TABLE belllabs_control.boundary_commands ENABLE ROW LEVEL SECURITY;
CREATE POLICY request_scope_isolation ON belllabs_control.boundary_commands
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
ALTER TABLE belllabs_control.boundary_commands FORCE ROW LEVEL SECURITY;

ALTER TABLE belllabs_control.boundary_command_receipts ENABLE ROW LEVEL SECURITY;
CREATE POLICY request_scope_isolation ON belllabs_control.boundary_command_receipts
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
ALTER TABLE belllabs_control.boundary_command_receipts FORCE ROW LEVEL SECURITY;

-- Least privilege: commands and receipts are insert-only ledgers; nothing updates or deletes
-- them. The read-only operations role serves inspection reads (REQ-CP-RUN-011).
GRANT SELECT, INSERT
    ON belllabs_control.boundary_commands,
       belllabs_control.boundary_command_receipts
    TO belllabs_control_runtime;
GRANT SELECT
    ON belllabs_control.boundary_commands,
       belllabs_control.boundary_command_receipts
    TO belllabs_operations_readonly;
