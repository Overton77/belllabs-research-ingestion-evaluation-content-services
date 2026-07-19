# 13 — Materialize Schema Workspaces and gate graph access

**What to build:** Expose one reusable run- or stage-scoped operation that mounts exact schema resources read-only, records complete lineage, and prevents graph-reading work until strict deployment compatibility and independent graph authority are established.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 07 — Implement isolated Run Workspace namespaces and materialization
- 11 — Build deterministic Schema Catalogs and governed schema selections
- 12 — Implement Schema Deployment Manifest attestation

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/13


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Implement one reusable application operation callable at run or stage scope; consuming Workflow Types supply policy and exact resources instead of owning copies.
- Evaluate strict compatibility and graph authority before dependent graph work, while allowing explicitly declared offline schema work to record its narrower status.
- Use workspace slot and materialization-manifest contracts from ticket 07 unchanged.

## Verification approach

Exercise a complete build/selection/attestation/materialization request with temporary workspace and test graph gate. Verify no Neo4j call occurs on every incompatibility outcome.

## Explicit non-goals

- Graph mutation, final schema MCP tools, full schema-selection workflow, and implicit credential grants from mounted files.
- Workflow-specific schema subsets or search tuning.

## Acceptance criteria

- [ ] A typed request binds exact catalog, accepted selection/projection, policy, purpose, requested resources, graph intent, workspace instance, and slot.
- [ ] Materialization verifies object/resource digests, bounded selection, supported versions, and workspace ownership before writing.
- [ ] Resources and authorized navigation skills mount read-only, and every governed path maps to its durable source and digest.
- [ ] The operation updates the Workspace Materialization Manifest and emits an immutable Schema Workspace Binding before dependent work.
- [ ] Idempotency binds all exact inputs, workspace, and slot; conflicting reuse fails.
- [ ] Graph-reading admission requires equality between the attested deployed SDL hash and the catalog Schema Definition hash plus separately admitted graph capability.
- [ ] Missing/revoked/mismatched attestation blocks Neo4j before a query; explicitly permitted offline work records that no live compatibility was established.
- [ ] An end-to-end test proves build, selection, attestation, materialization, binding, and dependent graph gate.

## Source basis

- Reusable Schema Catalog, Deployment Manifest, and Schema Workspace Materialization
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
