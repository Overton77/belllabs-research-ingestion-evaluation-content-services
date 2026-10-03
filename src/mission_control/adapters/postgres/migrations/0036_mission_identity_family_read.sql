-- A family writer must attest the same persisted installation as its runtime pool.
-- Reading this operator-owned identity never grants mutation or catalog authority.
GRANT SELECT ON belllabs_control.mission_installation_identity
    TO belllabs_family_repository_writer;
