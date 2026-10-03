-- Additive integrity hardening for the transitional async detail port.
-- Missing and JSON-null fields fail closed; SQL identity and returned payload agree.
ALTER TABLE belllabs_control.async_subagent_contract_details
  ADD CONSTRAINT async_subagent_contract_payload_identity CHECK ((
    payload->>'schema_version' = 'belllabs.async-subagent-contract.v1'
    AND payload->>'contract_id' = contract_id
    AND payload->>'contract_digest' = contract_digest
  ) IS TRUE);
ALTER TABLE belllabs_control.async_subagent_execution_details
  ADD CONSTRAINT async_subagent_execution_payload_identity CHECK ((
    payload->>'schema_version' = 'belllabs.async-subagent-execution.v1'
    AND payload->>'child_execution_id' = child_execution_id
    AND payload->>'contract_id' = contract_id
    AND payload->>'contract_digest' = contract_digest
    AND payload->>'parent_run_id' = parent_run_id
    AND payload->>'parent_operation_id' = parent_operation_id
    AND payload->>'execution_generation' = execution_generation::text
    AND (payload->>'updated_at')::timestamptz = updated_at
  ) IS TRUE);
ALTER TABLE belllabs_control.async_subagent_link_details
  ADD CONSTRAINT async_subagent_link_payload_identity CHECK ((
    payload->>'schema_version' = 'belllabs.parent-async-subagent-link.v1'
    AND payload->>'child_execution_id' = child_execution_id
    AND payload->>'link_id' = link_id
    AND payload->>'parent_run_id' = parent_run_id
    AND payload->>'parent_operation_id' = parent_operation_id
    AND (payload->>'updated_at')::timestamptz = updated_at
  ) IS TRUE);
