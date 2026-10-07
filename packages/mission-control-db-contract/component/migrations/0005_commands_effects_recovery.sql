-- mission-control-db-contract common release: requests, commands, events, outbox,
-- effects, accounting and recovery. Accepted, delivered and applied stay distinct.

CREATE SEQUENCE mission_control.outbox_global_position AS bigint MINVALUE 1;
REVOKE ALL ON SEQUENCE mission_control.outbox_global_position FROM PUBLIC;

CREATE TABLE mission_control.request_receipt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    request_receipt_id uuid PRIMARY KEY,
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    action text NOT NULL CHECK (action <> ''),
    request_key text NOT NULL CHECK (request_key <> ''),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('accepted', 'completed', 'rejected')),
    resource_ref text CHECK (resource_ref <> ''),
    result jsonb CHECK (jsonb_typeof(result) = 'object'),
    error_ref text CHECK (error_ref <> ''),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, request_receipt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, actor_ref, action, request_key)
);

CREATE TABLE mission_control.ledger_commit (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    ledger_commit_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    commit_key text NOT NULL CHECK (commit_key <> ''),
    expected_versions jsonb NOT NULL CHECK (jsonb_typeof(expected_versions) = 'object'),
    first_event_seq bigint NOT NULL CHECK (first_event_seq >= 1),
    last_event_seq bigint NOT NULL CHECK (last_event_seq >= 1),
    result_ref text CHECK (result_ref <> ''),
    CHECK (last_event_seq >= first_event_seq),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, ledger_commit_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, commit_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id)
);
CREATE INDEX ledger_commit_mission_id_fk_idx ON mission_control.ledger_commit (installation_id, application_id, tenant_id, mission_id);
CREATE TRIGGER ledger_commit_immutable
    BEFORE UPDATE OR DELETE ON mission_control.ledger_commit
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.mission_event (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    event_id uuid PRIMARY KEY,
    mission_id uuid NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 1),
    ledger_commit_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type <> ''),
    event_version integer NOT NULL CHECK (event_version >= 1),
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    run_id uuid,
    activation_id uuid,
    happened_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    causation_ref text CHECK (causation_ref <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_ref text CHECK (payload_ref <> ''),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, event_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, mission_id, seq),
    FOREIGN KEY (installation_id, application_id, tenant_id, mission_id) REFERENCES mission_control.mission (installation_id, application_id, tenant_id, mission_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, ledger_commit_id) REFERENCES mission_control.ledger_commit (installation_id, application_id, tenant_id, ledger_commit_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX mission_event_mission_id_fk_idx ON mission_control.mission_event (installation_id, application_id, tenant_id, mission_id);
CREATE INDEX mission_event_ledger_commit_id_fk_idx ON mission_control.mission_event (installation_id, application_id, tenant_id, ledger_commit_id);
CREATE INDEX mission_event_run_id_fk_idx ON mission_control.mission_event (installation_id, application_id, tenant_id, run_id);
CREATE TRIGGER mission_event_immutable
    BEFORE UPDATE OR DELETE ON mission_control.mission_event
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.outbox (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    outbox_id uuid PRIMARY KEY,
    global_position bigint NOT NULL DEFAULT nextval('mission_control.outbox_global_position') UNIQUE,
    ledger_commit_id uuid,
    event_id uuid,
    delivery_key text NOT NULL CHECK (delivery_key <> ''),
    destination_kind text NOT NULL CHECK (destination_kind <> ''),
    event_type text NOT NULL CHECK (event_type <> ''),
    aggregate_key text CHECK (aggregate_key <> ''),
    aggregate_version bigint,
    aggregate_sequence integer,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_ref text CHECK (payload_ref <> ''),
    delivery_state text NOT NULL CHECK (delivery_state IN ('held', 'pending', 'leased', 'delivered', 'dead')),
    lease_owner text CHECK (lease_owner <> ''),
    lease_expires_at timestamptz,
    attempts integer NOT NULL CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL,
    delivered_at timestamptz,
    version bigint NOT NULL CHECK (version >= 1),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, outbox_id),
    UNIQUE (installation_id, application_id, tenant_id, delivery_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, aggregate_key, aggregate_version, aggregate_sequence),
    FOREIGN KEY (installation_id, application_id, tenant_id, ledger_commit_id) REFERENCES mission_control.ledger_commit (installation_id, application_id, tenant_id, ledger_commit_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, event_id) REFERENCES mission_control.mission_event (installation_id, application_id, tenant_id, event_id)
);
CREATE INDEX outbox_ledger_commit_id_fk_idx ON mission_control.outbox (installation_id, application_id, tenant_id, ledger_commit_id);
CREATE INDEX outbox_event_id_fk_idx ON mission_control.outbox (installation_id, application_id, tenant_id, event_id);
CREATE INDEX outbox_due_idx ON mission_control.outbox (delivery_state, next_attempt_at) WHERE delivery_state IN ('pending', 'leased');

CREATE TABLE mission_control.consumer_cursor (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    consumer_cursor_id uuid PRIMARY KEY,
    consumer_key text NOT NULL CHECK (consumer_key <> ''),
    aggregate_key text NOT NULL CHECK (aggregate_key <> ''),
    cursor jsonb NOT NULL CHECK (jsonb_typeof(cursor) = 'object'),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, consumer_cursor_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, consumer_key, aggregate_key)
);

CREATE TABLE mission_control.command (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    command_id uuid PRIMARY KEY,
    command_key text NOT NULL CHECK (command_key <> ''),
    mission_id uuid,
    run_id uuid,
    activation_id uuid,
    subordinate_id uuid,
    target_generation bigint,
    target_version bigint,
    command_kind text NOT NULL CHECK (command_kind <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    payload_ref text CHECK (payload_ref <> ''),
    deadline_at timestamptz,
    lifecycle text NOT NULL CHECK (lifecycle IN ('accepted', 'delivering', 'delivered', 'applied', 'rejected', 'expired', 'superseded')),
    outcome text CHECK (outcome <> ''),
    requested_by_actor_ref text NOT NULL CHECK (requested_by_actor_ref <> ''),
    -- Additive (runtime authority, G2): typed boundary target and its sequence space.
    target_kind text CHECK (target_kind <> ''),
    target_ref text,
    sequence_space text CHECK (sequence_space <> ''),
    target_sequence bigint CHECK (target_sequence >= 0),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, command_id),
    UNIQUE (installation_id, application_id, tenant_id, command_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_id) REFERENCES mission_control.subordinate_execution (installation_id, application_id, tenant_id, subordinate_id)
);
CREATE INDEX command_run_id_fk_idx ON mission_control.command (installation_id, application_id, tenant_id, run_id);
CREATE INDEX command_activation_id_fk_idx ON mission_control.command (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX command_subordinate_id_fk_idx ON mission_control.command (installation_id, application_id, tenant_id, subordinate_id);
CREATE INDEX command_target_idx ON mission_control.command (installation_id, application_id, tenant_id, run_id, lifecycle);
CREATE UNIQUE INDEX command_target_sequence_idx ON mission_control.command (installation_id, application_id, tenant_id, run_id, sequence_space, target_sequence) WHERE target_sequence > 0;
-- Additive (runtime authority, G2): one typed reconciliation decision per async child.
CREATE UNIQUE INDEX command_single_subordinate_decision_idx ON mission_control.command (installation_id, application_id, tenant_id, subordinate_id) WHERE command_kind IN ('adopt_provider_run', 'orphan_child');

CREATE TABLE mission_control.delivery_report (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    delivery_report_id uuid PRIMARY KEY,
    command_id uuid NOT NULL,
    report_key text NOT NULL CHECK (report_key <> ''),
    delivery_semantics text NOT NULL CHECK (delivery_semantics <> ''),
    reported_at timestamptz NOT NULL,
    native_refs text[] NOT NULL,
    observed_outcome text NOT NULL CHECK (observed_outcome IN ('delivered', 'applied', 'rejected', 'emulated', 'unknown')),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, delivery_report_id),
    UNIQUE (installation_id, application_id, tenant_id, report_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, command_id) REFERENCES mission_control.command (installation_id, application_id, tenant_id, command_id)
);
CREATE INDEX delivery_report_command_id_fk_idx ON mission_control.delivery_report (installation_id, application_id, tenant_id, command_id);
CREATE TRIGGER delivery_report_immutable
    BEFORE UPDATE OR DELETE ON mission_control.delivery_report
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.human_task (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    human_task_id uuid PRIMARY KEY,
    task_key text NOT NULL CHECK (task_key <> ''),
    target_ref text NOT NULL CHECK (target_ref <> ''),
    kind text NOT NULL CHECK (kind <> ''),
    request_packet_ref text NOT NULL CHECK (request_packet_ref <> ''),
    assignee_scope text NOT NULL CHECK (assignee_scope <> ''),
    deadline_at timestamptz,
    on_timeout text CHECK (on_timeout <> ''),
    lifecycle text NOT NULL CHECK (lifecycle IN ('open', 'resolved', 'expired', 'cancelled')),
    -- Additive (runtime authority, G2): the immutable typed request packet when stored inline.
    request_packet jsonb CHECK (jsonb_typeof(request_packet) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, human_task_id),
    UNIQUE (installation_id, application_id, tenant_id, task_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);

CREATE TABLE mission_control.human_resolution (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    resolution_id uuid PRIMARY KEY,
    human_task_id uuid NOT NULL,
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    answer jsonb NOT NULL CHECK (jsonb_typeof(answer) = 'object'),
    answer_ref text CHECK (answer_ref <> ''),
    answer_digest text NOT NULL CHECK (answer_digest ~ '^sha256:[0-9a-f]{64}$'),
    expected_task_version bigint NOT NULL CHECK (expected_task_version >= 1),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, resolution_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, human_task_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, human_task_id) REFERENCES mission_control.human_task (installation_id, application_id, tenant_id, human_task_id)
);
CREATE INDEX human_resolution_human_task_id_fk_idx ON mission_control.human_resolution (installation_id, application_id, tenant_id, human_task_id);
CREATE TRIGGER human_resolution_immutable
    BEFORE UPDATE OR DELETE ON mission_control.human_resolution
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.event_receipt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    event_receipt_id uuid PRIMARY KEY,
    activation_id uuid,
    wait_key text NOT NULL CHECK (wait_key <> ''),
    source_event_key text NOT NULL CHECK (source_event_key <> ''),
    source_event_digest text NOT NULL CHECK (source_event_digest ~ '^sha256:[0-9a-f]{64}$'),
    matched_at timestamptz NOT NULL,
    consumed_at timestamptz,
    rearm_ordinal bigint NOT NULL CHECK (rearm_ordinal >= 0),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, event_receipt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, wait_key, source_event_key, rearm_ordinal),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id)
);
CREATE INDEX event_receipt_activation_id_fk_idx ON mission_control.event_receipt (installation_id, application_id, tenant_id, activation_id);

CREATE TABLE mission_control.mission_relationship (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    relationship_id uuid PRIMARY KEY,
    relationship_key text NOT NULL CHECK (relationship_key <> ''),
    source_run_id uuid NOT NULL,
    target_run_id uuid,
    kind text NOT NULL CHECK (kind IN ('composition', 'fork', 'dependency')),
    invocation_ref text CHECK (invocation_ref <> ''),
    grant_ref text CHECK (grant_ref <> ''),
    projected_output_policy jsonb NOT NULL CHECK (jsonb_typeof(projected_output_policy) = 'object'),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, relationship_id),
    UNIQUE (installation_id, application_id, tenant_id, relationship_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, target_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX mission_relationship_source_run_id_fk_idx ON mission_control.mission_relationship (installation_id, application_id, tenant_id, source_run_id);
CREATE INDEX mission_relationship_target_run_id_fk_idx ON mission_control.mission_relationship (installation_id, application_id, tenant_id, target_run_id);

CREATE TABLE mission_control.operation_intent (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_intent_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    activation_id uuid,
    attempt_id uuid,
    action_ref text NOT NULL CHECK (action_ref <> ''),
    effect_key text NOT NULL CHECK (effect_key <> ''),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    policy_digest text CHECK (policy_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding_digest text CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    side_effect_class text NOT NULL CHECK (side_effect_class <> ''),
    reservation_ref text CHECK (reservation_ref <> ''),
    state text NOT NULL CHECK (state IN ('claimed', 'dispatched', 'ambiguous', 'settled', 'abandoned')),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, operation_intent_id),
    UNIQUE (installation_id, application_id, tenant_id, effect_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, activation_id) REFERENCES mission_control.activation (installation_id, application_id, tenant_id, activation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, attempt_id) REFERENCES mission_control.attempt (installation_id, application_id, tenant_id, attempt_id)
);
CREATE INDEX operation_intent_run_id_fk_idx ON mission_control.operation_intent (installation_id, application_id, tenant_id, run_id);
CREATE INDEX operation_intent_activation_id_fk_idx ON mission_control.operation_intent (installation_id, application_id, tenant_id, activation_id);
CREATE INDEX operation_intent_attempt_id_fk_idx ON mission_control.operation_intent (installation_id, application_id, tenant_id, attempt_id);

CREATE TABLE mission_control.operation_receipt (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    operation_receipt_id uuid PRIMARY KEY,
    operation_intent_id uuid NOT NULL,
    receipt_contract text NOT NULL CHECK (receipt_contract <> ''),
    outcome text NOT NULL CHECK (outcome IN ('applied', 'rejected', 'noop', 'partial', 'failed', 'cancelled', 'timed_out')),
    external_operation_ref text CHECK (external_operation_ref <> ''),
    output_refs text[] NOT NULL,
    receipt_digest text NOT NULL CHECK (receipt_digest ~ '^sha256:[0-9a-f]{64}$'),
    reconciliation jsonb NOT NULL CHECK (jsonb_typeof(reconciliation) = 'object'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, operation_receipt_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, operation_intent_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, operation_intent_id) REFERENCES mission_control.operation_intent (installation_id, application_id, tenant_id, operation_intent_id)
);
CREATE INDEX operation_receipt_operation_intent_id_fk_idx ON mission_control.operation_receipt (installation_id, application_id, tenant_id, operation_intent_id);
CREATE TRIGGER operation_receipt_immutable
    BEFORE UPDATE OR DELETE ON mission_control.operation_receipt
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.budget_account (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    budget_account_id uuid PRIMARY KEY,
    account_key text NOT NULL CHECK (account_key <> ''),
    run_id uuid,
    parent_budget_account_id uuid,
    currency text CHECK (currency <> ''),
    ceilings jsonb NOT NULL CHECK (jsonb_typeof(ceilings) = 'object'),
    reserved jsonb NOT NULL CHECK (jsonb_typeof(reserved) = 'object'),
    settled jsonb NOT NULL CHECK (jsonb_typeof(settled) = 'object'),
    state jsonb NOT NULL CHECK (jsonb_typeof(state) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, budget_account_id),
    UNIQUE (installation_id, application_id, tenant_id, account_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_budget_account_id) REFERENCES mission_control.budget_account (installation_id, application_id, tenant_id, budget_account_id)
);
CREATE INDEX budget_account_run_id_fk_idx ON mission_control.budget_account (installation_id, application_id, tenant_id, run_id);
CREATE INDEX budget_account_parent_budget_account_id_fk_idx ON mission_control.budget_account (installation_id, application_id, tenant_id, parent_budget_account_id);

CREATE TABLE mission_control.budget_entry (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    budget_entry_id uuid PRIMARY KEY,
    budget_account_id uuid NOT NULL,
    run_id uuid,
    source_key text NOT NULL CHECK (source_key <> ''),
    dimension text NOT NULL CHECK (dimension <> ''),
    amount bigint NOT NULL,
    entry_kind text NOT NULL CHECK (entry_kind <> ''),
    causation_ref text CHECK (causation_ref <> ''),
    entry jsonb NOT NULL CHECK (jsonb_typeof(entry) = 'object'),
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, budget_entry_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, budget_account_id, source_key, dimension, entry_kind),
    FOREIGN KEY (installation_id, application_id, tenant_id, budget_account_id) REFERENCES mission_control.budget_account (installation_id, application_id, tenant_id, budget_account_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX budget_entry_budget_account_id_fk_idx ON mission_control.budget_entry (installation_id, application_id, tenant_id, budget_account_id);
CREATE INDEX budget_entry_run_id_fk_idx ON mission_control.budget_entry (installation_id, application_id, tenant_id, run_id);
CREATE TRIGGER budget_entry_immutable
    BEFORE UPDATE OR DELETE ON mission_control.budget_entry
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.native_observation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    native_observation_id uuid PRIMARY KEY,
    harness_execution_id uuid,
    subordinate_id uuid,
    native_event_key text NOT NULL CHECK (native_event_key <> ''),
    cursor text CHECK (cursor <> ''),
    generation bigint NOT NULL CHECK (generation > 0),
    received_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_ref text CHECK (payload_ref <> ''),
    retain_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, native_observation_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, native_event_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, harness_execution_id) REFERENCES mission_control.harness_execution (installation_id, application_id, tenant_id, harness_execution_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, subordinate_id) REFERENCES mission_control.subordinate_execution (installation_id, application_id, tenant_id, subordinate_id)
);
CREATE INDEX native_observation_harness_execution_id_fk_idx ON mission_control.native_observation (installation_id, application_id, tenant_id, harness_execution_id);
CREATE INDEX native_observation_subordinate_id_fk_idx ON mission_control.native_observation (installation_id, application_id, tenant_id, subordinate_id);
CREATE TRIGGER native_observation_immutable
    BEFORE UPDATE OR DELETE ON mission_control.native_observation
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.reconciliation_case (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    case_id uuid PRIMARY KEY,
    case_key text NOT NULL CHECK (case_key <> ''),
    target_kind text NOT NULL CHECK (target_kind <> ''),
    target_ref text NOT NULL CHECK (target_ref <> ''),
    uncertainty_reason text NOT NULL CHECK (uncertainty_reason <> ''),
    desired_state_ref text CHECK (desired_state_ref <> ''),
    observed_state_ref text CHECK (observed_state_ref <> ''),
    lease_owner text CHECK (lease_owner <> ''),
    lease_expires_at timestamptz,
    next_action text NOT NULL CHECK (next_action <> ''),
    outcome text CHECK (outcome IN ('resolved_applied', 'resolved_not_applied', 'abandoned', 'escalated')),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, case_id),
    UNIQUE (installation_id, application_id, tenant_id, case_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
-- Additive (runtime authority, G2): incident lookup by target.
CREATE INDEX reconciliation_case_target_idx ON mission_control.reconciliation_case (installation_id, application_id, tenant_id, target_kind, target_ref);

CREATE TABLE mission_control.recovery_request (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    recovery_id uuid PRIMARY KEY,
    source_run_id uuid NOT NULL,
    source_attempt_id uuid,
    source_checkpoint_id uuid,
    kind text NOT NULL CHECK (kind IN ('inspect', 'diagnostic_replay', 'technical_retry', 'fork')),
    actor_ref text NOT NULL CHECK (actor_ref <> ''),
    action text NOT NULL CHECK (action <> ''),
    request_key text NOT NULL CHECK (request_key <> ''),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    expected_source_version bigint,
    expected_generation bigint,
    state text NOT NULL CHECK (state IN ('admitted', 'running', 'completed', 'blocked', 'failed')),
    target_run_id uuid,
    reason_ref text CHECK (reason_ref <> ''),
    result_ref text CHECK (result_ref <> ''),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    version bigint NOT NULL CHECK (version >= 1),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, recovery_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, actor_ref, action, request_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, target_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_checkpoint_id) REFERENCES mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, checkpoint_id)
);
CREATE INDEX recovery_request_source_run_id_fk_idx ON mission_control.recovery_request (installation_id, application_id, tenant_id, source_run_id);
CREATE INDEX recovery_request_target_run_id_fk_idx ON mission_control.recovery_request (installation_id, application_id, tenant_id, target_run_id);
CREATE INDEX recovery_request_source_checkpoint_id_fk_idx ON mission_control.recovery_request (installation_id, application_id, tenant_id, source_checkpoint_id);

CREATE TABLE mission_control.fork_lineage (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    fork_lineage_id uuid PRIMARY KEY,
    source_run_id uuid NOT NULL,
    target_run_id uuid NOT NULL,
    source_checkpoint_id uuid,
    source_checkpoint_digest text NOT NULL CHECK (source_checkpoint_digest ~ '^sha256:[0-9a-f]{64}$'),
    copied_artifact_manifest_digest text NOT NULL CHECK (copied_artifact_manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    budget_admission_ref text NOT NULL CHECK (budget_admission_ref <> ''),
    grant_admission_ref text CHECK (grant_admission_ref <> ''),
    reason text NOT NULL CHECK (reason <> ''),
    detail jsonb NOT NULL CHECK (jsonb_typeof(detail) = 'object'),
    CHECK (source_run_id <> target_run_id),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, fork_lineage_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, target_run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, target_run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, source_checkpoint_id) REFERENCES mission_control.continuation_checkpoint (installation_id, application_id, tenant_id, checkpoint_id)
);
CREATE INDEX fork_lineage_source_run_id_fk_idx ON mission_control.fork_lineage (installation_id, application_id, tenant_id, source_run_id);
CREATE INDEX fork_lineage_target_run_id_fk_idx ON mission_control.fork_lineage (installation_id, application_id, tenant_id, target_run_id);
CREATE INDEX fork_lineage_source_checkpoint_id_fk_idx ON mission_control.fork_lineage (installation_id, application_id, tenant_id, source_checkpoint_id);
CREATE TRIGGER fork_lineage_immutable
    BEFORE UPDATE OR DELETE ON mission_control.fork_lineage
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.runtime_segment (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_segment_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    technical_segment bigint NOT NULL CHECK (technical_segment > 0),
    temporal_run_id text NOT NULL CHECK (temporal_run_id <> ''),
    worker_build_ref text NOT NULL CHECK (worker_build_ref <> ''),
    command_frontier text CHECK (command_frontier <> ''),
    event_frontier text CHECK (event_frontier <> ''),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, runtime_segment_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    UNIQUE (installation_id, application_id, tenant_id, run_id, technical_segment),
    FOREIGN KEY (installation_id, application_id, tenant_id, run_id) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_id)
);
CREATE INDEX runtime_segment_run_id_fk_idx ON mission_control.runtime_segment (installation_id, application_id, tenant_id, run_id);
CREATE TRIGGER runtime_segment_immutable
    BEFORE UPDATE OR DELETE ON mission_control.runtime_segment
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.replay_report (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    replay_report_id uuid PRIMARY KEY,
    recovery_id uuid NOT NULL,
    source_history_ref text NOT NULL CHECK (source_history_ref <> ''),
    source_history_digest text NOT NULL CHECK (source_history_digest ~ '^sha256:[0-9a-f]{64}$'),
    diagnostic_thread_ref text NOT NULL CHECK (diagnostic_thread_ref <> ''),
    code_version text NOT NULL CHECK (code_version <> ''),
    schema_version text NOT NULL CHECK (schema_version <> ''),
    mode text NOT NULL CHECK (mode <> ''),
    results_artifact_ref text NOT NULL CHECK (results_artifact_ref <> ''),
    results_digest text NOT NULL CHECK (results_digest ~ '^sha256:[0-9a-f]{64}$'),
    effect_claims_disabled boolean NOT NULL CHECK (effect_claims_disabled),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, replay_report_id),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, recovery_id) REFERENCES mission_control.recovery_request (installation_id, application_id, tenant_id, recovery_id)
);
CREATE INDEX replay_report_recovery_id_fk_idx ON mission_control.replay_report (installation_id, application_id, tenant_id, recovery_id);
CREATE TRIGGER replay_report_immutable
    BEFORE UPDATE OR DELETE ON mission_control.replay_report
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'request_receipt',
        'ledger_commit',
        'mission_event',
        'outbox',
        'consumer_cursor',
        'command',
        'delivery_report',
        'human_task',
        'human_resolution',
        'event_receipt',
        'mission_relationship',
        'operation_intent',
        'operation_receipt',
        'budget_account',
        'budget_entry',
        'native_observation',
        'reconciliation_case',
        'recovery_request',
        'fork_lineage',
        'runtime_segment',
        'replay_report'
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

REVOKE ALL ON mission_control.request_receipt, mission_control.ledger_commit, mission_control.mission_event, mission_control.outbox, mission_control.consumer_cursor, mission_control.command, mission_control.delivery_report, mission_control.human_task, mission_control.human_resolution, mission_control.event_receipt, mission_control.mission_relationship, mission_control.operation_intent, mission_control.operation_receipt, mission_control.budget_account, mission_control.budget_entry, mission_control.native_observation, mission_control.reconciliation_case, mission_control.recovery_request, mission_control.fork_lineage, mission_control.runtime_segment, mission_control.replay_report FROM PUBLIC;
