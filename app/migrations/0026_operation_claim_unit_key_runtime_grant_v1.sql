-- AMD-RRM-001 forward-only migration (RRM-009): the runtime role may stamp the fenced unit
-- on the operation effect claim it opens. Migration 0019 added `unit_key` to
-- belllabs_control.operation_effect_claims (RRM-001 disposition row 48) and the journal
-- repository inserts it, but the column-level INSERT grant from 0013 was never widened, so
-- a worker running as the least-privilege login (`belllabs_app`, member of
-- belllabs_control_runtime) could not open a claim: the production composition qualified
-- in RRM-009 is the first path that journals as that login rather than as the schema owner.
-- Grant-only: no rows, digests or identities change. Number 0025 belongs to a concurrent ticket.

GRANT INSERT (unit_key)
    ON belllabs_control.operation_effect_claims
    TO belllabs_control_runtime;
