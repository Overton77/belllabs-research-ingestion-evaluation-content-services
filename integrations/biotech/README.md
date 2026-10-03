# Biotech Mission Control adapters

This optional transition package owns Biotech knowledge schema/catalog/context/grounding,
Neo4j access, reference research, and the existing web research workflow configuration.
It is staged outside `src/mission_control` for later KnowledgeServices placement. It is not
a second mission kernel and does not generalize KnowledgeServices. The general distribution
does not depend on this package, import its modules, register its policies, or initialize
its databases. Neo4j and GraphQL dependencies belong to this distribution.

Install the general distribution and then `integrations/biotech` explicitly in the domain
service environment. In this source checkout:

```powershell
uv pip install -e . -e integrations/biotech
```

Domain bootstrap calls `biotech_mission_adapters.bootstrap.composition.register_biotech_contracts`
with its explicit extension and admission policy registries. The optional read router can be
mounted with `mount_biotech_read_api(app, records)` on the domain service; its principal
dependency must be authenticated by that service. Missing repository wiring fails closed.
Domain semantic handlers are registered explicitly in a supplied generic handler registry;
the core coordinator no longer takes domain-specific configuration arguments. Temporal
continues to own scheduling; domain handlers and bounded Agent Server operations do not
create an alternate runtime.

`BiotechSettings` owns Neo4j credentials and schema authority identifiers. Plain core
`Settings` has none of these fields. No credential values are committed or recorded here.
The bounded read adapters still enforce exact schema deployment, query intent and grant
contracts. The retained cleanup planner recognizes historical Neo4j property names such as
`mongoPlanId` only to identify obsolete graph objects; there is no MongoDB client or storage.

## Optional PostgreSQL records

`adapters.postgres_records` implements the existing schema grounding and web research ports
with an explicitly supplied asyncpg pool. Every operation sets its request scope locally in
the transaction and uses scoped predicates. The optional `biotech_mission_adapters.records`
table has forced row security, append-only records, identity constraints and distinct
record/intent uniqueness. Replayed identical content returns the same record; conflicting
immutable identities fail. Model payload digests are verified on reads/writes.

Its migration is `src/biotech_mission_adapters/migrations/0001_scoped_records.sql`. It is
deliberately absent from the general Mission Control migration chain. Apply it only to the
explicit domain database as an authorized migration owner, in one transaction with errors
stopping execution. Grant `biotech_mission_adapter_runtime` to the owning domain service's
restricted login; never give that login superuser/BYPASSRLS privileges. No core SQL schema
or common SQL component is copied or amended. Reapplying this initial installation is
idempotent. The database must have this migration before a live research runner is invoked.

No legacy Mongo data is imported. This is fresh-execution persistence under the approved
clean transformation. Rollback is to stop/unmount the optional adapter and retain its tables
and immutable evidence; no down migration deletes data. The real SQL tests create unique
disposable databases and preserve them for inspection.

## Relocated entry points

All package paths below begin with `biotech_mission_adapters`:

| Previous ownership | Current ownership |
| --- | --- |
| Kernel `domain/schema_catalog`, `schema_context`, `schema_grounding`, `reference_research` | Same suffix under this optional package |
| Kernel `domain/coordinator/web_research_runtime` | `domain.coordinator.web_research_runtime` |
| Kernel `application/schema`, `reference_research`, domain `web_research` | Same suffix under this optional package |
| Kernel schema/research admission policies | `application.execution.schema_grounding_admission`, `web_research_admission` |
| Kernel Neo4j/schema/research infrastructure | `adapters.infrastructure` |
| Kernel schema Temporal composition and research smokes | `adapters.temporal` |
| Kernel reference operation canary | `adapters.agent_server.operations.reference_research` |
| Root schema selection experiments | `qualification.schema_context_selection` |
| Root `schema-catalog` overlay/reference assets | `resources/schema-catalog` |
| Reviewed research seed and promotion | `domain.coordinator.web_capability_fixtures`, `application.capabilities.reviewed_capability_promotion` |
| Reviewed provider snapshots | `resources/reviewed_payloads` |
| Root domain command scripts | `bootstrap.scripts` |

The nine script module names are `promote_schema_grounding_surface`,
`provision_schema_deployment_evidence`, `compare_schema_context_runs`,
`diagnose_schema_deployment_snapshot`, `load_trudiagnostic_graph`,
`reconcile_zero_count_schema_artifacts`, `run_web_research_coordinator_live`, `promote_reviewed_web_capabilities`,
and `stage_schema_grounding_live_inputs`.
Run them as `python -m biotech_mission_adapters.bootstrap.scripts.<name> --help`.
Old source paths have no compatibility aliases. The obsolete
`run_web_research_stagegraph_local` script was removed: its fake lifecycle bypassed admission
and its call no longer matched the operation-boundary API. Use the admitted live coordinator
or the deterministic production-stack qualification tests instead. Its source is recoverable
from the organization checkpoint. Live domain writes, schema cleanup,
provider calls, and model spending still require their original explicit authorization;
moving these tools has not executed them.

Generic external capability discovery/inspection remains in Mission Control's capability
adapters. Generic browser subprocess execution moved to
`mission_control.adapters.capabilities.browser_subprocess`, so DeepAgents operations
do not import this domain package.

## Evidence

`tests/unit/schema`, `tests/unit/web_research`, and the Neo4j adapter tests preserve the
existing semantic, authority and bounded operation assertions after relocation.
`tests/integration/postgres/test_biotech_adapter_records.py` verifies the optional migration,
restricted role isolation, replay/concurrency, immutable conflicts and malformed payload
rejection against real disposable PostgreSQL. No live Neo4j or metered research proof was
run as part of this move.
