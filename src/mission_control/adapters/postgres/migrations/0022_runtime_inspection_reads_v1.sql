-- AMD-RRM-001 forward-only migration (RRM-005): scoped, non-mutating inspection reads.
-- Schema identity: belllabs.inspection-read.v1 (CON-CP-INSPECTION-READ-V1, REQ-CP-RUN-011/012).
-- No table is created or altered: inspection is a read model over existing authority.
-- This migration only lets the read-only operations role serve those reads (least
-- privilege, SELECT only) and indexes the scoped keyset pagination of the run list.
-- Every table below already enables and forces request-scope RLS (0001, 0015, 0016), so
-- the read-only role sees exactly the scope set in `belllabs.request_scope`.
-- Number 0021 is reserved for a concurrent ticket; numbers need not be contiguous.

-- Run authority: projection, budget account and effect ledger state.
GRANT SELECT
    ON belllabs_control.workflow_runs,
       belllabs_control.budget_accounts,
       belllabs_control.effect_ledgers
    TO belllabs_operations_readonly;

-- Async-child parent authority (0016): identity, decisions and lifecycle facts. Message
-- and command payloads are not granted: inspection never reads message content.
GRANT SELECT
    ON belllabs_control.async_subagent_authority,
       belllabs_control.async_subagent_facts
    TO belllabs_operations_readonly;

-- Keyset pagination of a scope's runs (ORDER BY run_id after the cursor position).
CREATE INDEX IF NOT EXISTS workflow_runs_scope_run_idx
    ON belllabs_control.workflow_runs (request_scope, run_id);

-- Children of one parent run within its scope.
CREATE INDEX IF NOT EXISTS async_subagent_scope_parent_idx
    ON belllabs_control.async_subagent_authority (request_scope, parent_run_id);
