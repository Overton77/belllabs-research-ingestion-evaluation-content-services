# 18 — Implement immutable Mission Memory Packs

**What to build:** Materialize a bounded policy-selected memory view as an immutable, read-only Mission Memory Pack with generated advisory guidance and complete provenance, then bind it safely into workspaces and snapshot restoration.

**Blocked by:**
- 07 — Implement isolated Run Workspace namespaces and materialization
- 10 — Implement immutable Sandbox Snapshot clone and restore
- 17 — Implement governed Workflow and Mission Memory

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/18


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Build packs through a shared application operation using an exact policy, corpus position, selected item revisions, rendering version, purpose, and workspace slot.
- Keep generated guidance bounded, provenance-bearing, and visibly advisory; item text must not become capability or workflow authority.
- Coordinate exact-pack restore and successor decisions with snapshot clone lineage rather than modifying an existing workspace binding.

## Verification approach

Verify deterministic bundle bytes/manifests, object and file digests, read-only mounts, tenant isolation, tamper rejection, exact restore, and explicit successor authorization.

## Explicit non-goals

- A writable memory database inside the sandbox, direct acceptance of workspace notes, automatic procedural promotion, and final retention durations.
- Replacing ordinary governed memory APIs.

## Acceptance criteria

- [ ] A shared operation selects accepted item revisions under an exact Memory Policy and recorded corpus position.
- [ ] The pack contains bounded memory files, generated advisory navigation, and a machine-readable manifest of item revisions, sources, scopes, policy, decisions, and file digests.
- [ ] Object storage owns immutable files while PostgreSQL records exact digests, governance metadata, revisions, and bindings.
- [ ] Idempotency binds purpose, policy, corpus position, selected revisions, renderer, and materialization policy; changed inputs create a successor.
- [ ] Materialization verifies digests, mounts read-only, updates the workspace manifest, and emits an immutable binding.
- [ ] Snapshot restore reuses the exact pack unless an authorized compatibility/applicability decision selects a successor for the cloned workspace.
- [ ] Workspace notes remain local candidates until accepted through Memory Write Proposals; pack guidance and embedded instructions grant no authority.
- [ ] Tests prove deterministic contents, lineage, isolation, tamper rejection, exact restore, explicit successor authorization, and inert instructions.

## Source basis

- Governed Workflow and Mission Memory
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
