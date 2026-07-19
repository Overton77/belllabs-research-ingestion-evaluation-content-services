# 07 — Implement isolated Run Workspace namespaces and materialization

**What to build:** Provision provider-neutral Sandbox Workspaces inside logical Run Workspace Namespaces using compiled templates and contracts, with explicit ownership, read-only governed inputs, and durable path lineage.

**Blocked by:**
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/7


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Compile logical slots from exact Workspace Template and Workflow Workspace Contract revisions; do not persist host paths as portable identities.
- Separate provisioning, mounting, ownership validation, and manifest recording behind provider-neutral sandbox ports.
- Reserve child-private writable namespaces before delegate execution and make shared governed assets verifiably read-only.

## Verification approach

Use temporary real filesystems plus a sandbox conformance adapter. Exercise parallel owners, slot conflicts, read-only mounts, unmapped files, and complete manifest lineage.

## Explicit non-goals

- Artifact promotion, snapshot restore, permanent sandbox-provider selection, and implicit synchronization of workspace folders.
- Treating workspace files as canonical domain records.

## Acceptance criteria

- [ ] Exact Workspace Templates and Workflow Workspace Contracts resolve logical slots for runs, stages, cycles, iterations, evaluators, agents, and delegates.
- [ ] Shared durable inputs, schema resources, skills, plugins, and memory packs mount read-only with verified digests.
- [ ] Every writable slot has one owner; parallel branches and delegates cannot inherit overlapping writes, ambient credentials, or undeclared capabilities.
- [ ] A versioned Workspace Materialization Manifest maps governed paths to durable inputs, local candidates, promoted artifacts, or stale/superseded entries.
- [ ] Slot conflicts, digest mismatches, unsupported runtime requirements, and undeclared mounts return typed failures without fallback or silent merging.
- [ ] Cross-workspace exchange uses promoted references or typed durable messages, never uncontrolled shared writable directories.
- [ ] Creating a workspace file alone cannot make it a domain artifact, memory item, or input.

## Source basis

- Operation Runtime, Workspaces, Artifacts, and Snapshots
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
