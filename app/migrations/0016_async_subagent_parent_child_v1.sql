-- CON-CP-ASYNC-SUBAGENT-V1 parent authority and ordered message ledger.
-- Provider state is observation only; every accepted decision remains application PostgreSQL state.

CREATE TABLE IF NOT EXISTS belllabs_control.async_subagent_authority (
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    parent_run_id text NOT NULL REFERENCES belllabs_control.workflow_runs(run_id),
    parent_operation_id text NOT NULL,
    link_id text NOT NULL,
    contract_id text NOT NULL,
    contract_digest text NOT NULL,
    reservation_id text NOT NULL,
    dependency_class text NOT NULL CHECK (dependency_class IN ('required_blocking', 'degradable_blocking', 'nonblocking', 'advisory')),
    execution_generation integer NOT NULL CHECK (execution_generation >= 1),
    cancellation_requested boolean NOT NULL DEFAULT false,
    result_decision text CHECK (result_decision IN ('admit', 'conditionally_admit', 'reject', 'defer')),
    result_manifest_digest text,
    settlement_ref text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, child_execution_id),
    UNIQUE (request_scope, link_id),
    UNIQUE (request_scope, reservation_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.async_subagent_commands (
    command_id text PRIMARY KEY,
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    command_kind text NOT NULL CHECK (command_kind IN ('admit', 'cancel', 'result_decision', 'settle')),
    payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    FOREIGN KEY (request_scope, child_execution_id)
      REFERENCES belllabs_control.async_subagent_authority(request_scope, child_execution_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.async_subagent_facts (
    fact_id text PRIMARY KEY,
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    fact_kind text NOT NULL CHECK (fact_kind IN ('lifecycle', 'result', 'cancellation', 'settlement')),
    fact_ref text NOT NULL,
    recorded_at timestamptz NOT NULL,
    UNIQUE (request_scope, child_execution_id, fact_kind, fact_ref),
    FOREIGN KEY (request_scope, child_execution_id)
      REFERENCES belllabs_control.async_subagent_authority(request_scope, child_execution_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.async_subagent_messages (
    message_id text PRIMARY KEY,
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    direction text NOT NULL CHECK (direction IN ('parent_to_child', 'child_to_parent')),
    target_sequence bigint NOT NULL CHECK (target_sequence >= 1),
    receipt text NOT NULL,
    payload jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    UNIQUE (request_scope, child_execution_id, direction, target_sequence),
    FOREIGN KEY (request_scope, child_execution_id)
      REFERENCES belllabs_control.async_subagent_authority(request_scope, child_execution_id)
);

CREATE INDEX IF NOT EXISTS async_subagent_parent_idx
  ON belllabs_control.async_subagent_authority(parent_run_id, parent_operation_id);
CREATE INDEX IF NOT EXISTS async_subagent_fact_order_idx
  ON belllabs_control.async_subagent_facts(request_scope, child_execution_id, recorded_at, fact_id);

ALTER TABLE belllabs_control.async_subagent_authority ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_facts ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_messages ENABLE ROW LEVEL SECURITY;

CREATE POLICY async_subagent_authority_scope ON belllabs_control.async_subagent_authority
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY async_subagent_commands_scope ON belllabs_control.async_subagent_commands
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY async_subagent_facts_scope ON belllabs_control.async_subagent_facts
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY async_subagent_messages_scope ON belllabs_control.async_subagent_messages
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));

ALTER TABLE belllabs_control.async_subagent_authority FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_commands FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_facts FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_messages FORCE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE ON belllabs_control.async_subagent_authority TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.async_subagent_commands TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.async_subagent_facts TO belllabs_control_runtime;
GRANT SELECT, INSERT, UPDATE ON belllabs_control.async_subagent_messages TO belllabs_control_runtime;
