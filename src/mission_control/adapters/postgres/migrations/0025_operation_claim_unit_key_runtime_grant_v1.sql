-- AMD-RRM-001 forward-only migration (RRM-009): the runtime role may stamp the fenced unit
-- on the operation effect claim it opens. Migration 0019 added `unit_key` to
-- belllabs_control.operation_effect_claims (RRM-001 disposition row 48) and the journal
-- repository inserts it, but the column-level INSERT grant from 0013 was never widened, so
-- a worker running as the least-privilege login (`belllabs_app`, member of
-- belllabs_control_runtime) could not open a claim: the production composition qualified
-- in RRM-009 is the first path that journals as that login rather than as the schema owner.
-- Grant-only and least-privilege: one column of INSERT, no UPDATE, no new table privilege,
-- and the table keeps its forced row-level security. No rows, digests or identities change.
-- Numbered 0025: RRM-008, the only concurrent ticket, adds no migration.

GRANT INSERT (unit_key)
    ON belllabs_control.operation_effect_claims
    TO belllabs_control_runtime;
