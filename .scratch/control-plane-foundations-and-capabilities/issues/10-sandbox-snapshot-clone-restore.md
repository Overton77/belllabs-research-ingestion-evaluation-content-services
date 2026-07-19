# 10 — Implement immutable Sandbox Snapshot clone and restore

**What to build:** Persist reproducible workspace state as immutable snapshots and restore it only by cloning into a new workspace whose present authority, credentials, connections, mounts, and runtime compatibility are independently re-established.

**Blocked by:**
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts
- 07 — Implement isolated Run Workspace namespaces and materialization

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/10


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Store immutable snapshot metadata separately from content-addressed filesystem/provider payloads and keep clone lineage separate from artifact/workflow lineage.
- Treat restore as new workspace provisioning followed by compatibility and current-authority validation; never reconnect stale live resources from snapshot data.
- Represent authored migration as a new semantic operation rather than silently adapting an incompatible snapshot.

## Verification approach

Use a sandbox conformance adapter to snapshot once, restore twice, tamper with payload and runtime digests, revoke authority, and verify new identities with no copied live capabilities.

## Explicit non-goals

- In-place workspace resume, credential persistence, treating snapshots as promoted artifacts, and final retention defaults.
- Restoring authority from filesystem contents.

## Acceptance criteria

- [ ] Snapshot metadata and content-addressed payload preserve source workspace, parent snapshot, provider identity, filesystem/runtime/image/package/environment digests, reason, producer binding, capability shape, and retention.
- [ ] Snapshot creation follows bound policy and is idempotent for the accepted identity.
- [ ] Every restore creates a new workspace identity with explicit parent-workspace and parent-snapshot lineage and leaves source state immutable.
- [ ] Restore verifies payload/runtime digests, environment compatibility, mounts, retention, requested binding, and current authority before operation execution.
- [ ] Secrets, credentials, leases, MCP connections, sockets, and writable ownership are re-resolved or reacquired rather than copied.
- [ ] Incompatible restore fails unless an authored migration policy creates a new semantic operation with preserved lineage.
- [ ] Restored files still require valid artifact promotion before downstream use.
- [ ] Tests cover two clones from one snapshot, tampering, stale authority, and absence of restored live capabilities.

## Source basis

- Operation Runtime, Workspaces, Artifacts, and Snapshots
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
