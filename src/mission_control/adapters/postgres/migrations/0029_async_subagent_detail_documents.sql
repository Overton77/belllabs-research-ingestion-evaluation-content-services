-- Transitional replacement of async child Mongo details, not the common
-- mission_control component. That component remains owned by ai-engineer-db-contract.
CREATE TABLE belllabs_control.async_subagent_contract_details (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    contract_id text NOT NULL CHECK (contract_id <> ''),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    PRIMARY KEY (request_scope, contract_id),
    UNIQUE (request_scope, contract_id, contract_digest)
);
CREATE TABLE belllabs_control.async_subagent_execution_details (
    request_scope text NOT NULL CHECK (request_scope <> ''),
    child_execution_id text NOT NULL CHECK (child_execution_id <> ''),
    contract_id text NOT NULL,
    contract_digest text NOT NULL,
    parent_run_id text NOT NULL,
    parent_operation_id text NOT NULL,
    execution_generation bigint NOT NULL CHECK (execution_generation > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, child_execution_id),
    UNIQUE (request_scope, child_execution_id, parent_run_id, parent_operation_id),
    FOREIGN KEY (request_scope, contract_id, contract_digest)
      REFERENCES belllabs_control.async_subagent_contract_details
        (request_scope, contract_id, contract_digest)
);
CREATE INDEX async_subagent_execution_details_contract_idx
    ON belllabs_control.async_subagent_execution_details
       (request_scope, contract_id, contract_digest);
CREATE TABLE belllabs_control.async_subagent_link_details (
    request_scope text NOT NULL,
    child_execution_id text NOT NULL,
    link_id text NOT NULL CHECK (link_id <> ''),
    parent_run_id text NOT NULL,
    parent_operation_id text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, child_execution_id),
    UNIQUE (request_scope, link_id),
    FOREIGN KEY (request_scope, child_execution_id, parent_run_id, parent_operation_id)
      REFERENCES belllabs_control.async_subagent_execution_details
        (request_scope, child_execution_id, parent_run_id, parent_operation_id)
);
ALTER TABLE belllabs_control.async_subagent_contract_details ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_contract_details FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_execution_details ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_execution_details FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_link_details ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.async_subagent_link_details FORCE ROW LEVEL SECURITY;
CREATE POLICY async_contract_details_scope ON belllabs_control.async_subagent_contract_details
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY async_execution_details_scope ON belllabs_control.async_subagent_execution_details
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
CREATE POLICY async_link_details_scope ON belllabs_control.async_subagent_link_details
  USING (request_scope = current_setting('belllabs.request_scope', true))
  WITH CHECK (request_scope = current_setting('belllabs.request_scope', true));
GRANT SELECT, INSERT ON belllabs_control.async_subagent_contract_details
  TO belllabs_control_runtime;
GRANT SELECT, INSERT ON belllabs_control.async_subagent_execution_details,
  belllabs_control.async_subagent_link_details TO belllabs_control_runtime;
GRANT UPDATE (payload, updated_at, execution_generation)
  ON belllabs_control.async_subagent_execution_details TO belllabs_control_runtime;
GRANT UPDATE (payload, updated_at)
  ON belllabs_control.async_subagent_link_details TO belllabs_control_runtime;
GRANT SELECT ON belllabs_control.async_subagent_contract_details,
  belllabs_control.async_subagent_execution_details, belllabs_control.async_subagent_link_details
  TO belllabs_operations_readonly;
CREATE TRIGGER async_subagent_contract_details_append_only
  BEFORE UPDATE OR DELETE ON belllabs_control.async_subagent_contract_details
  FOR EACH ROW EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
