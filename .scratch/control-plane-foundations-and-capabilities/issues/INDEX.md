# Control-plane foundations and capabilities ticket index

This directory is the local companion to the control-plane GitHub issues in
`Overton77/belllabs-research-ingestion-evaluation-content-services`.

The ticket bodies are intentionally self-contained enough for a fresh implementation agent.
The `biotech-meta` documents remain the governing architectural source and should be consulted
whenever a ticket leaves a contract, policy, authority boundary, or domain term unclear.

## Governing documents

Read these before implementing any ticket:

1. [Human Upgrade System Context](../../../../biotech-meta/docs/CONTEXT.md) — canonical domain
   vocabulary and distinctions. Do not introduce synonyms that collapse separate concepts such as
   Workflow Run versus SDK session, wait versus pause, Source Snapshot versus Source Work, or
   capability availability versus authority.
2. [Pre-research specification suite index](../../../../biotech-meta/docs/specs/pre-research/README.md)
   — governing source hierarchy, persistence authority, canonical dependency DAG, shared testing
   rules, and the first executable path.

When a ticket conflicts with one of these documents, stop and reconcile the ticket rather than
silently choosing an implementation interpretation.

## Foundation specifications

These specifications establish the contracts that later Workflow Types must consume rather than
reimplement:

- [F1 — Versioned Workflow Definitions and Effective Run Configuration](../../../../biotech-meta/docs/specs/pre-research/control-plane-foundations/01-versioned-workflow-definitions-and-effective-run-configuration.md)
- [F2 — Transactional Run admission, lifecycle, and budgets](../../../../biotech-meta/docs/specs/pre-research/control-plane-foundations/02-transactional-run-admission-lifecycle-and-budgets.md)
- [F3 — Durable blueprint orchestration and linked runs](../../../../biotech-meta/docs/specs/pre-research/control-plane-foundations/03-durable-blueprint-orchestration-and-linked-runs.md)
- [F4 — Operation runtime, workspaces, artifacts, and snapshots](../../../../biotech-meta/docs/specs/pre-research/control-plane-foundations/04-operation-runtime-workspaces-artifacts-and-snapshots.md)

Canonical foundation order:

```text
F1 → F2 → F3 → F4
```

The first implementation frontier is ticket 01. A later ticket may be started only when every
ticket in its **Blocked by** section is complete.

## Capability specifications

These capabilities build on the foundation contracts:

- [C1 — Schema Catalog, Deployment Manifest, and Schema Workspace Materialization](../../../../biotech-meta/docs/specs/pre-research/control-plane-capabilities/01-schema-catalog-deployment-manifest-and-workspace-materialization.md)
- [C2 — Conversations, session projections, and durable realtime](../../../../biotech-meta/docs/specs/pre-research/control-plane-capabilities/02-conversations-threads-session-projections-and-durable-realtime.md)
- [C3 — Governed Workflow and Mission Memory](../../../../biotech-meta/docs/specs/pre-research/control-plane-capabilities/03-governed-workflow-and-mission-memory.md)
- [C4 — Governed agentic capability catalogs and exact bindings](../../../../biotech-meta/docs/specs/pre-research/control-plane-capabilities/04-governed-agentic-capability-catalogs-and-bindings.md)

The suite-level dependency rule is:

```text
F1 + F2 + F3 + F4 → C1
F1 + F2 + F4      → C2, C3, and core C4
```

Individual capability tickets may expose preparatory sub-frontiers earlier, but no capability may
claim its full end-to-end contract before the governing foundation dependencies exist.

## Ticket map

### Foundation configuration and transactional authority

- [01 — Workflow Definitions and Effective Run Configuration](01-workflow-definitions-and-effective-run-configuration.md) — F1
- [02 — Run admission, lifecycle, budgets, and events](02-transactional-admission-lifecycle-budgets-events.md) — F2

### Durable orchestration

- [03 — StageGraph orchestration](03-durable-stagegraph-orchestration.md) — F3
- [04 — GoalDirected orchestration](04-durable-goaldirected-orchestration.md) — F3
- [05 — Linked runs and orchestration continuity](05-linked-runs-and-orchestration-continuity.md) — F3

### Runtime, workspaces, artifacts, and snapshots

- [06 — Operation Execution Bindings and runtime contracts](06-operation-execution-bindings-runtime-contracts.md) — F4
- [07 — Workspace namespaces and materialization](07-workspace-namespaces-and-materialization.md) — F4
- [08 — Durable artifact promotion](08-transactional-artifact-promotion.md) — F4
- [09 — OpenAI Agents runtime and delegation](09-openai-runtime-and-bounded-delegation.md) — F4
- [10 — Sandbox snapshot clone and restore](10-sandbox-snapshot-clone-restore.md) — F4

### Schema capability

- [11 — Schema Catalogs and selections](11-schema-catalogs-and-selections.md) — C1
- [12 — Schema Deployment Manifest attestation](12-schema-deployment-manifest-attestation.md) — C1
- [13 — Schema Workspace materialization and graph gate](13-schema-workspace-materialization-and-graph-gate.md) — C1

### Conversation and realtime capability

- [14 — Conversations, Threads, messages, and forks](14-canonical-conversations-threads-messages-forks.md) — C2
- [15 — Conversation promotion and SDK session projections](15-conversation-promotion-and-session-projections.md) — C2
- [16 — Authenticated durable realtime](16-authenticated-durable-realtime.md) — C2

### Governed memory capability

- [17 — Workflow and Mission Memory](17-governed-workflow-and-mission-memory.md) — C3
- [18 — Mission Memory Packs](18-mission-memory-packs.md) — C3

### Governed agentic capability catalog

- [19 — Prompt and Agent Skill catalog intake](19-prompt-and-skill-catalog-intake.md) — C4
- [20 — MCP and Plugin catalog intake](20-mcp-and-plugin-catalog-intake.md) — C4
- [21 — Capability selection and governed execution](21-capability-selection-and-governed-execution.md) — C4

## Implementation rules shared by every ticket

- Preserve the accepted authority split: PostgreSQL owns transactional control-plane state;
  MongoDB/Beanie owns immutable definition, configuration, binding, and workflow-shaped documents;
  object storage owns large immutable payloads; Temporal owns durable execution mechanics; Neo4j
  owns approved canonical graph knowledge.
- Treat prompts, messages, memory, schema resources, catalog descriptions, deployment availability,
  and agent output as non-authoritative until an application-owned typed decision admits them.
- Resolve mutable aliases before compilation or admission. Historical runs and bindings always
  retain exact immutable revisions and digests.
- Keep infrastructure retry identities separate from semantic attempts, cycles, iterations, and
  revisions.
- Test public application behavior and durable outcomes. Avoid tests coupled to repository calls,
  private helper methods, SQL statement ordering, SDK constructors, Temporal history shape, or host
  filesystem layout.
- Do not invent workflow-specific stages, obligation matrices, gates, thresholds, budgets, prompts,
  or evaluation criteria that are deferred to the chosen Workflow Type specifications.

## Current implementation baseline

The target repository is an infrastructure bootstrap rather than an existing domain application.
It currently provides Python 3.12, FastAPI/Socket.IO, Temporal, the OpenAI Agents SDK sandbox probe,
and clients for PostgreSQL/Supabase, MongoDB/Beanie, Neo4j, and S3.

Before or as part of ticket 01:

- make the application importable and testable without an external `PYTHONPATH` workaround;
- establish a package/domain layout that later tickets can extend;
- retain the existing infrastructure marker and sandbox probe as diagnostics only;
- avoid treating the current hard-coded model, prompt, image, or sandbox manifest as production
  defaults.

Ticket 02 introduces application PostgreSQL migrations. The current local PostgreSQL container is
Temporal persistence and must not become the application authority accidentally.

The schema capability additionally requires an authoritative directive SDL and a deployment
manifest issuer. If those assets have not yet been migrated into an accessible repository, use
explicit fixtures for bounded tests and do not claim strict live-graph compatibility.

## Tracker and provenance notes

- GitHub issues are the shared tracker and cloud-agent handoff surface.
- These Markdown files are the local mirror and preserve dependency ordering and source context.
- Keep local and GitHub acceptance criteria synchronized when a ticket changes.
- The `biotech-meta` relative links work from this local workspace layout. Until those documents are
  committed and pushed, a Cloud Agent must rely on the self-contained issue body or be given the
  relevant source material explicitly.
