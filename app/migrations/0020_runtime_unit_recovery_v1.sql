-- AMD-RRM-001 forward-only migration (RRM-004): checkpoint and settlement crash recovery.
-- Schema identities: belllabs.unit-result-observation.v1 (REQ-CP-EXEC-014, the fenced
-- result-manifest write), belllabs.unit-reconciliation-incident.v1 (REQ-CP-DA-018 /
-- REQ-CP-RUN-007 typed in_doubt incident, stored in runtime_reconciliation_incidents).
-- Rows hold keys, digests and manifest refs only: never checkpoint bodies, transcripts,
-- prompts, outputs, or secrets.

-- REQ-CP-EXEC-014: the claim lease of one unit generation. The holder is the exact Activity
-- attempt observation; a later attempt takes over an expired or released lease only by
-- advancing claim_fence (decided under the per-unit advisory lock).
-- REQ-CP-EXEC-005: `superseded` records an accepted generation boundary
-- (`reconcile_unit` start_new_generation); every write of a superseded generation is fenced.
ALTER TABLE belllabs_control.runtime_unit_generations
    ADD COLUMN lease_holder text,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN superseded boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT runtime_unit_generations_lease_shape
        CHECK ((lease_holder IS NULL) = (lease_expires_at IS NULL));

CREATE TABLE belllabs_control.runtime_unit_result_observations (
    request_scope text NOT NULL,
    observation_id text NOT NULL,
    schema_version text NOT NULL CHECK (
        schema_version = 'belllabs.unit-result-observation.v1'
    ),
    unit_key text NOT NULL,
    execution_generation bigint NOT NULL,
    claim_fence bigint NOT NULL CHECK (claim_fence >= 1),
    binding_id text NOT NULL,
    settlement_id text NOT NULL,
    status text NOT NULL CHECK (
        status IN ('completed', 'failed', 'cancelled', 'timed_out')
    ),
    result_manifest_ref text NOT NULL,
    result_manifest_digest text NOT NULL CHECK (
        result_manifest_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    result_manifest_size_bytes bigint NOT NULL CHECK (result_manifest_size_bytes >= 1),
    checkpoint_transition_id text,
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    result_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, observation_id),
    -- One result manifest per unit generation: it is fixed once and never rewritten.
    UNIQUE (request_scope, unit_key, execution_generation),
    FOREIGN KEY (request_scope, unit_key, execution_generation)
        REFERENCES belllabs_control.runtime_unit_generations(
            request_scope, unit_key, execution_generation
        ),
    FOREIGN KEY (request_scope, checkpoint_transition_id)
        REFERENCES belllabs_control.runtime_checkpoint_transitions(
            request_scope, transition_id
        )
);

-- Typed in_doubt incidents of runtime units are looked up by unit generation.
CREATE INDEX runtime_reconciliation_incidents_unit_idx
    ON belllabs_control.runtime_reconciliation_incidents (request_scope, unit_key)
    WHERE unit_key IS NOT NULL;

ALTER TABLE belllabs_control.runtime_unit_result_observations ENABLE ROW LEVEL SECURITY;
CREATE POLICY request_scope_isolation ON belllabs_control.runtime_unit_result_observations
    USING (request_scope = current_setting('belllabs.request_scope', true))
    WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
ALTER TABLE belllabs_control.runtime_unit_result_observations FORCE ROW LEVEL SECURITY;

-- Least privilege, consistent with 0019: the result observation is insert-only; the lease
-- columns live on runtime_unit_generations, whose UPDATE grant 0019 already gives the
-- runtime role. runtime_reconciliation_incidents grants are unchanged from 0014.
GRANT SELECT, INSERT
    ON belllabs_control.runtime_unit_result_observations
    TO belllabs_control_runtime;
GRANT SELECT
    ON belllabs_control.runtime_unit_result_observations
    TO belllabs_operations_readonly;
