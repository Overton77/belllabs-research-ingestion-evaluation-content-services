-- AMD-RRM-001 forward-only migration (RRM-009, composing RRM-008): a family that terminalizes
-- through its atomic admission (StageGraph's completion proposal) records the terminal
-- receipts of the run's pending boundary commands in the same commit, as the plain
-- `terminalize` path does (RRM-008 review F1: a cancel is `applied` by a `cancelled` outcome,
-- any other pending command reaches `rejected`). That commit runs as the family repository
-- writer, which could read and write neither boundary ledger, so a production StageGraph
-- cancel stayed `delivered` after the run was terminal.
-- Grant-only and least-privilege: the writer reads both insert-only ledgers and appends
-- receipts; it never records a boundary command and never updates or deletes. Forced
-- row-level security is kept on both tables. No rows, digests or identities change.

GRANT SELECT
    ON belllabs_control.boundary_commands,
       belllabs_control.boundary_command_receipts
    TO belllabs_family_repository_writer;
GRANT INSERT
    ON belllabs_control.boundary_command_receipts
    TO belllabs_family_repository_writer;
