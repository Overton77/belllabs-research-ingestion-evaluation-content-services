"""Provider-neutral workspace leases, worktrees and snapshots (MP-04).

Import the submodules directly (`errors`, `policy`, `snapshots`, `ports`, `service`): the lease
ledger in `application.execution.harness.leases` imports `errors`, so this package stays free of
eager imports to keep that dependency acyclic.
"""
