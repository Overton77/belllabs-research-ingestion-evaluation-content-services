---
type: Concept
title: Knowledge Services boundary
description: Which knowledge contracts are shared by the kernel and which stay application-owned, what the optional Biotech integration implements today, and the documented tension over when knowledge integration is required.
tags: [mission-control, knowledge-services, biotech, neo4j, boundary, implementation]
---

# Knowledge Services boundary

[Knowledge Services](../../GLOSSARY.md) are shared contracts; a
[Domain Service](../../GLOSSARY.md) is application-owned and the kernel never writes
entities. This concept names the two sides of that line, summarises the Biotech package
that lives outside the kernel, and records an unresolved wording tension in the
specification.

## Shared contracts

Specified (expansion/KNOWLEDGE-SERVICES.md; SPECIFICATION.md, Accepted storage and
Knowledge Services boundary): source identity, version, snapshot and locator contracts;
artifact custody and provenance; governed [Intent](../../GLOSSARY.md), plan, apply and
[Receipt](../../GLOSSARY.md) with a stable effect identity and receipt lookup before any
uncertain repeat; evidence tracking; [Advisory Memory](../../GLOSSARY.md) with explicit
admission and no save-on-chat; and a retrieval envelope whose score is not evidence
validity. The common wrapper `mc.knowledge_request.v1` pins identity, exact capability
binding, native intent digest, authority and preconditions, and wraps a domain payload
unchanged (the AI Engineer `knowledge-ingestion-intent.v1` keeps camelCase). Eleven
logical interfaces are to be bound in the catalog: `knowledge.describe`,
`capture_prepare`, `verify`, `classify_propose`, `classify_admit`, `query`, `retrieve`,
`ingestion_plan`, `ingestion_apply`, `receipt_get`, `verify_result`.

## Application-owned stores

AI Engineer entity reads and writes go to its PostgreSQL domain adapter; Biotech entity
reads and writes go to its Neo4j adapter. Both applications keep mission state in their
own Supabase `mission_control` schema. General code cannot query either entity store or
depend on its schema; even colocated PostgreSQL domain tables give the kernel no write
authority. The Biotech adapter must declare its real graph concurrency guarantee, use a
unique scoped intent marker per apply transaction and reconcile a lost receipt from that
marker; the spec states the Neo4j writer is a required new proof, not existing code.

## What `integrations/biotech` implements today

The optional distribution `biotech-mission-adapters` (`integrations/biotech`, namespace
`biotech_mission_adapters`, depends on `mission-control`, `neo4j` and `graphql-core`)
is a transition home that the kernel never imports (`tests/architecture/test_package_boundaries.py`).
Its packages:

- `domain/`: `schema_catalog` (parser, derivation, overlay, renderer, validation),
  `schema_context` (canonicalization, expansion, projection), `schema_grounding`
  (contracts, authority, definitions), `reference_research` contracts and
  `coordinator` web-research runtime fixtures.
- `application/`: `schema/` (catalog build, context selection and derivation, grounding
  repository and semantic handlers, workspace binding, authority issuance, bounded
  `graph_query`, supporting-graph reconciliation, artifact cleanup), `web_research/`,
  `reference_research/service.py` (Stage 0 to 2 Q/D verticals over the generic
  operation executor seams), `capabilities/reviewed_capability_promotion.py` and
  `execution/` admission policies for schema grounding and web research.
- `adapters/`: `infrastructure/` (Neo4j driver, `Neo4jReadExecutor`, a factory that
  creates the graph client only after canonical admission, schema deployment, schema
  agent runtime and payloads), `postgres_records.py` (an optional scoped
  `biotech_mission_adapters.records` table with forced row security and its own
  migration `migrations/0001_scoped_records.sql`, never part of the common release),
  `temporal/` coordinator runtime, grounding activities and smoke runners, and one
  `agent_server` reference-research operation.
- `interfaces/http/schema_grounding.py`: a read router mounted on the domain service by
  `bootstrap/composition.py::mount_biotech_read_api`; `register_biotech_contracts`
  registers extensions and admission policies explicitly. `BiotechSettings` owns Neo4j
  credentials; core settings have none.
- `qualification/schema_context_selection/` and `resources/` (schema catalog, reviewed
  payloads).

This is read-side schema grounding, context selection and research verticals. No
`knowledge.*` interface, `mc.knowledge_request.v1` wrapper, intent marker, Neo4j writer
or AI Engineer PostgreSQL adapter exists in this repository; a kernel search for
"knowledge" finds only unrelated identifiers.

## Required now or after parity: a documented tension

SPECIFICATION.md says, in its scope paragraph (line 13 of the current file; line 7 before
the frontmatter was added), that "knowledge integration is required scope now; earlier
detailed-design deferrals cannot omit it". Its fixed-architecture section (line 55,
formerly 49) says shared Knowledge Services contracts and application adapters "are in
design scope; their detailed implementation follows the Mission Control parity
milestone". Later lines repeat both readings: "complete domain implementations are not a
prerequisite for Mission Control parity" and "the common boundary is required now". The
implementation status and [qualification](qualification.md) record broad Knowledge
Services generalization as deferred. These statements are left as they stand; this
concept does not pick one.

# Citations

- Spec: `../mission-control-general/general-mission-control/expansion/KNOWLEDGE-SERVICES.md`;
  `../mission-control-general/general-mission-control/SPECIFICATION.md` (scope paragraph,
  fixed architecture, acceptance and release scope, accepted storage and Knowledge
  Services boundary).
- ADR: [0007](../adr/0007-authoritative-state-separate-from-advisory-memory.md).
- Code: [Biotech package guide](../../integrations/biotech/README.md),
  [Biotech composition](../../integrations/biotech/src/biotech_mission_adapters/bootstrap/composition.py),
  [Neo4j bounded reads](../../integrations/biotech/src/biotech_mission_adapters/adapters/infrastructure/schema_neo4j_executor.py),
  [scoped records](../../integrations/biotech/src/biotech_mission_adapters/adapters/postgres_records.py),
  [reference research service](../../integrations/biotech/src/biotech_mission_adapters/application/reference_research/service.py),
  [package metadata](../../integrations/biotech/pyproject.toml).
- Tests: [package boundaries](../../tests/architecture/test_package_boundaries.py),
  [static architecture](../../tests/qualification/two_project/test_static_architecture.py),
  [Biotech adapter records](../../tests/integration/postgres/test_biotech_adapter_records.py),
  [reviewed capability promotion](../../tests/unit/capability/test_reviewed_capability_promotion.py).
- Status: `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md` (optional Biotech extraction;
  deferred generalization).
