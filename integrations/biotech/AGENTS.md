# Optional Biotech integration

This package contains application-specific schema grounding, reference research,
web research and Neo4j adapters extracted from the general runtime. Its import
namespace is biotech_mission_adapters. The general mission_control kernel must
never import this package; reviewed deployment composition supplies its policies,
extension validators and bounded capability bindings explicitly.

Retain application-specific tests and precise dependency requirements. Do not
manufacture permissive registrations or restore default Biotech branches in the
kernel. Extraction is not proof that these domain services are production-ready
or completion of the deferred Knowledge Services generalization audit (the shared
contracts are required scope; see docs/knowledge/knowledge-services.md).

Read the root `GLOSSARY.md` (Domain Service, Entity Store, Intent, Receipt, Knowledge
Services) before changing anything here. Install with `uv sync --group biotech`.

## Where things are (src/biotech_mission_adapters)

| Concern | Start |
| --- | --- |
| Schema catalog, schema context selection and grounding (read side) | domain/ |
| Reference and web research verticals, reviewed capability promotion | application/ |
| Neo4j Aura reads, FastMCP web-research runtime, scoped PostgreSQL records | adapters/ |
| Read-only HTTP routers | interfaces/ |
| Temporal schema-grounding activities and composition (`register_biotech_contracts`) | bootstrap/ |
| Biotech-specific scoped records SQL | migrations/ |
| Schema-context selection evaluation harness | qualification/ |
| Fixtures and reference data | resources/ |

## Invariants

No entity writer exists here yet; Biotech domain writes must arrive through the
Knowledge Services intent and receipt contract, never through a sandbox or kernel path.
Neo4j credentials and MCP secrets stay server-side. Tests that need Neo4j or Temporal
are gated by environment variables and must skip, not fail, when those are absent.

See repository docs/REMOVAL_GUIDE.md, docs/knowledge/architecture.md, and
docs/organization/biotech-module-map.json for source lineage.
