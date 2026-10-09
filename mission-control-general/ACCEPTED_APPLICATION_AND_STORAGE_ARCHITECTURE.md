# Accepted application and storage architecture

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

> **Subsequent owner instruction:** [Execution directive](EXECUTION_DIRECTIVE.md) supersedes this record's legacy preservation/migration assumptions. The backend becomes `mission-control`; no backward compatibility or old Mongo data preservation is required. Deliver current capability parity first, consolidate the full specification second, and design Knowledge Services third. Application-scoped database, bucket, identity, and runtime-binding decisions below remain in force.

Date: 2026-10-02, America/New_York.

Status: Owner-approved architecture direction. Runtime mechanics below specify the required implementation; they are not a claim that it exists or has passed qualification.

## Owner acceptance and scope

The owner explicitly approved the application and storage separation described in this conversation: “Alright I APPROVE.” The owner requested that the acceptance, full details, and runtime differentiation be recorded in `mission-control-general/`.

The approved invariant is:

> Mission Control is one general system with application-scoped installations. Each installation owns its mission records, users, artifact storage, capability grants, and domain integrations. Shared contracts and execution semantics are implemented once; application differences are expressed through versioned bindings, policies, templates, and adapters.

The owner also directed that `biotech-research-ingestion-evaluation-system` be transformed and renamed into Mission Control, with StageGraph and GoalDirected refactored against the synthesized specification. This replaces the assumption that the general runtime must remain only a new subpackage of the unchanged Biotech application. The final repository name and physical relocation remain to be specified; this record does not rename files or services.

Temporal Cloud will serve both applications. Docker may provide development infrastructure. Temporal plus Deep Agents is the first delivery priority; Cursor Cloud SDK follows. Shared Knowledge Services responsibilities are now in synthesis scope, with explicit application-specific domain adapters.

This approval establishes architecture direction. It is not evidence that databases, authentication providers, buckets, namespaces, or deployment credentials have been inspected or configured. This documentation change performs no runtime refactor, migration, provisioning, or deletion.

## Authority and reconciliation

This record is the latest owner-approved amendment for application differentiation, storage, implementation direction, and delivery priority. It takes precedence on those subjects over the [earlier general specification pack](../../aiengineer/ai-engineer-meta/ai-engineer-architecture/specs/general-mission-control/README.md), the [historical architecture](MISSION_CONTROL_ARCHITECTURE.md), and earlier recommendations in this conversation.

Unchanged workflow semantics and lifecycle contracts remain synthesis inputs. This record does not silently accept every detail of every older proposal.

| Earlier position | Approved direction or synthesis baseline |
| --- | --- |
| S3 as the default for new application artifacts | Private Supabase Storage buckets in each application's project |
| A new package inside an otherwise unchanged Biotech backend | Transform and rename the existing backend into the general Mission Control system |
| Deep Agents and Cursor both required for the initial release | Temporal plus Deep Agents first; Cursor Cloud SDK afterward |
| Knowledge Services generalization entirely out of scope | Synthesize shared services and application-specific domain adapters |
| A separate checkpoint database required immediately | Begin with isolated runtime persistence in each application's PostgreSQL, subject to compatibility and load qualification |
| MongoDB required by the new general control plane | Preserve legacy responsibilities during migration; no new MongoDB dependency |

Specification synthesis will produce a canonical set and remove superseded design material after requirements, decisions, and references have been accounted for. Preserve historical qualification evidence and migration provenance. No documents are deleted by this acceptance record.

## Database and infrastructure responsibilities

The owner reports two Supabase Pro projects, a LangSmith Pro plan, a Temporal Cloud account, AWS services, Neo4j, and MongoDB. Actual project identifiers, contents, entitlements, regions, and capacity remain deployment inputs, not inferred facts.

| Responsibility | AI Engineer | Biotech |
| --- | --- | --- |
| Supabase project | `supabase-blue-ocean` | `biotech-research-ingestion` |
| Users and authentication | Project-local Supabase Auth | Project-local Supabase Auth |
| Profiles, memberships, and application permissions | Application PostgreSQL tables | Application PostgreSQL tables |
| Mission state | Project-local `mission_control` schema | Identical project-local `mission_control` schema |
| Entity knowledge and relationships | Existing PostgreSQL domain schemas | Neo4j Aura |
| Shared Knowledge Services metadata | Common component installed locally | Same common component installed locally |
| Durable artifact bytes | This project's private Storage buckets | This project's private Storage buckets |
| Capability catalog, installations, grants, and bindings | Local PostgreSQL records | Local PostgreSQL records |
| Deep Agents recovery checkpoints | Private runtime schema and restricted role | Private runtime schema and restricted role |
| Domain ingestion and queries | Governed SQL executors | Governed Cypher/Neo4j executors |

One migration/component source defines the common Mission Control schema; both installations pin compatible releases and checksums. The earlier SQL ownership in `ai-engineer-db-contract` remains the distribution input until explicitly reconciled. Refactoring runtime code does not create a second editable SQL authority. Schema identity does not require identical application data, templates, or grants.

PostgreSQL owns admission, commands, lifecycle transitions, acceptance, budgets, operation records, and transactional inbox/outbox state. Versioned JSONB may represent structured definitions. Temporal owns durable execution mechanics and its own history. A provider result, checkpoint, trace, or file cannot independently authorize a mission transition.

AWS is the deployment baseline for services and workers. Existing S3 artifacts remain addressable during migration; S3 may later serve a demonstrated archival or storage-protection requirement. Redis is optional for caching or event fan-out, with recovery from durable stores. Neither adds a second mission authority.

Use application/environment isolation in Temporal Cloud, with separate production namespaces as the recommended starting deployment. Task queues route work; they do not replace authentication or namespace access control. Development Docker infrastructure preserves the same logical ownership boundaries.

LangSmith supplies tracing, evaluations, and qualified sandbox facilities. Its deployment-managed persistence is not assumed to be an arbitrary application database included with Pro. Checkpoint adapter compatibility, connection limits, retention, and isolation must be qualified before using the proposed project-local runtime schemas. Separate runtime persistence physically later if measured requirements justify it.

## Buckets and artifact custody

Create the same logical private bucket layout in each Supabase project. Identical bucket names in different projects are distinct storage locations.

| Bucket | Responsibility |
| --- | --- |
| `mission-artifacts` | Intent files, query files, execution outputs, reports, manifests, continuation packages, and durable workspace snapshots |
| `knowledge-artifacts` | Captured sources, extracted representations, ingestion inputs, and evidence documents |
| `capability-bundles` | Immutable skill bundles, tool/plugin packages, hook scripts, and supporting files |

Assign each artifact one authoritative byte location; associate it with multiple consumers through references rather than automatically copying it between buckets. Intent files are artifacts even when executing them changes a database. Knowledge-specific media types and metadata may differ between applications.

An artifact reference includes installation, application, tenant, artifact identity, version/digest, store binding, bucket, and object key. Database records own lineage, authorization, custody, and acceptance. Signed URLs are temporary access mechanisms, never durable identity.

Use immutable object keys, disallow overwrite through ordinary application paths, verify byte digests, and restrict deletion. Supabase Storage does not provide S3 bucket versioning; application-level immutability is not storage-level write-once protection. Backup and recovery must cover object bytes as well as database metadata.

Upload and PostgreSQL registration are separate operations: authorize upload, upload bytes, verify ownership/digest, commit registration, then expose the artifact to downstream selection. Handle orphan uploads and missing objects explicitly. A failed registration does not constitute an accepted output. No cross-service atomic transaction is assumed.

Private-bucket policies enforce tenant access for user requests. Server credentials that bypass RLS require equivalent application authorization and must never reach agents. Prefer short-lived, narrowly scoped access when materializing artifacts into sandboxes.

## Runtime application binding

The shared service runs the same code for both applications. An operator-managed, versioned deployment registry resolves an authenticated request into an application installation. Request bodies cannot supply database URLs, bucket endpoints, or arbitrary target domains.

The conceptual binding contract is:

```text
ApplicationBinding {
  application_id, installation_id, environment,
  binding_version, binding_digest,
  supabase_project_ref, database_secret_ref,
  artifact_store_binding_ref, bucket_mapping,
  accepted_issuers, accepted_audiences, tenant_resolver_ref,
  schema_component_version, capability_catalog_ref,
  domain_adapter_refs, template_catalog_ref, policy_profile_ref,
  temporal_namespace, task_queue_bindings,
  checkpoint_store_ref, checkpoint_namespace,
  langsmith_project_ref, sandbox_profile_refs,
  resource_limits, enabled
}
```

These are logical contract fields for synthesis, not a deployed schema. Secret references resolve server-side. Installation identity distinguishes environments and database installations even when application names or UUIDs coincide. Runtime binding and authorization are separate: pinning a binding never freezes a revoked permission.

### Request resolution and admission

1. The API, CLI, MCP client, or dashboard requests an application scope. A selector such as `/v1/applications/biotech` is a request, not proof of authority.
2. Validate the caller against configured issuer/audience trust and actor grants before granting database access. Resolve tenant membership server-side. Changing a header or body field cannot turn an AI Engineer credential into a Biotech credential.
3. Resolve the approved registry entry. Verify database installation identity and schema compatibility. A mismatch or unavailable project fails for that application; there is no fallback to the other database.
4. Resolve available templates, exact capability versions, grants, policies, and domain adapters. Validate and compile against the admitted inventory. Unsupported behavior is rejected before effects.
5. In the selected application's PostgreSQL transaction, record the run, immutable inputs and execution binding, budget reservation, and dispatch outbox entry. Pin application, installation, tenant, binding version/digest, and relevant component versions.
6. The relay dispatches to the bound Temporal namespace using a stable execution identity. Retries attach to or reconcile that identity rather than launch duplicate work.

HTTP, CLI, MCP, and dashboards use the same application handlers. A unified operator view queries each authorized installation and labels every result by application; it does not require a central mission database or a distributed transaction.

### Workers and durable execution

Temporal payloads carry scoped identities, immutable references, and digests. They do not carry credentials or large artifact bodies. Activities resolve the pinned installation through trusted configuration and verify that their worker is permitted to serve it.

The selected runtime context supplies restricted repositories, artifact access, capability bindings, domain clients, and checkpoint namespaces. Domain differences live behind typed interfaces; the general scheduler contains no SQL-versus-Cypher branch for business logic. Database pools and any caches must be keyed by installation/environment, with tenant scope where applicable. Mutable process-global “current application” state is prohibited.

Root runs, stages, attempts, subordinates, child missions, events, commands, budgets, and outputs preserve application ownership. Child execution inherits scope and can receive narrower grants. Cross-application execution or artifact transfer is not implicit; it requires a separately specified, explicitly authorized integration. UUID equality does not authorize reuse across installations.

On worker restart, retry, continuation, or resume, reload the persisted binding identity. Never resolve “whatever database is current” from a mutable default. Credential rotation may update a secret behind the same installation; moving the run to another project requires an explicit migration. Recheck current revocations and execution grants at relevant action boundaries.

### Stage materialization and context

For a downstream stage, the common kernel evaluates dependency and acceptance conditions, then freezes exact selected outputs under the consumer's scope. Selection must resolve the intended producer activation and artifact version rather than an ambiguous “latest” file.

The materializer retrieves permitted bytes from that application's buckets, verifies digests, mounts read-only inputs, provisions the owned writable workspace, and resolves exact skills/tools/MCP endpoints. It also selects authorized source and memory context under a context budget. Record an input manifest, materialization receipt, and exact submitted context manifest before starting dependent cognition.

An available artifact, mounted file, submitted model context, and observed file access are distinct facts. Mounting a file does not prove the agent read it. Checkpoints and workspace snapshots retain installation and execution provenance when hydrating a replacement session.

Outputs return to the same application's artifact registry and buckets. Registration precedes completion assessment; acceptance precedes ordinary downstream release. Explicit provisional-input policies, if supported, remain visible and versioned. Missing bytes, wrong ownership, or digest mismatch block materialization instead of selecting another application's data.

## Knowledge Services and deterministic domain operations

Generalize artifact custody, provenance, source identity/version/snapshot/locator contracts, intent execution protocols, operation receipts, evidence tracking, memory policy, and retrieval envelopes. Keep entity semantics, schema navigation, query planning constraints, evidence interpretation, and domain writes in application adapters.

AI Engineer exposes its PostgreSQL knowledge as a navigable workspace and executes ingestion intents and knowledge query artifacts through governed SQL interfaces. Biotech exposes Neo4j graph knowledge and Graph RAG through bounded Cypher/graph interfaces. Mission Control invokes typed domain capabilities; agents do not receive unrestricted database credentials.

Both lanes execute admitted immutable intent artifacts with a pinned executor version, target schema version, authorization, preconditions, input digest, and idempotency identity. Deterministic execution does not imply safe repetition after an uncertain response. Persist intent before dispatch and reconcile an ambiguous result through the domain's durable receipt/effect identity before repeating a write.

PostgreSQL mission state and Neo4j effects do not share a normal database transaction. Use explicit intent, receipt, and reconciliation semantics. Even where AI Engineer tables share one physical PostgreSQL project, preserve domain-service ownership; the mission kernel must not become an unrestricted domain writer.

Runtime checkpoints, continuation packages, operational memory, and long-term domain memory remain separate. Memory selection and write proposals are governed and attributable; retrieved memory is advisory, not accepted knowledge merely because it was recalled.

## Capability definitions and application grants

Separate immutable capability definitions from application installation and execution grants. A reusable definition can be distributed to both applications while each installation independently selects a version, policy, endpoint, and credentials.

Skills include the entire versioned supporting bundle. MCP definitions describe endpoints, schemas, and authentication bindings. Tools and plugins carry compatibility and execution manifests. Hooks declare trigger, runtime support, permissions, and failure semantics; support is qualified per harness rather than assumed universal. Catalog metadata and decisions live in PostgreSQL, bundle bytes in `capability-bundles`, and secret values in deployment secret management.

Agents may propose new assets but cannot self-install or self-grant them. Revocation is recorded separately from immutable asset content and remains effective for future actions. Shared content does not grant cross-application read access.

## Worked runtime distinction

An AI Engineer mission authenticates against its allowed issuer, resolves the `ai-engineer` installation, writes admission to `supabase-blue-ocean`, and materializes an intent from that project's `mission-artifacts`. Its admitted domain capability executes SQL and returns a domain receipt. The common kernel evaluates the completion contract and records the result in the same installation.

A Biotech mission follows the identical kernel flow but resolves `biotech`, persists admission in `biotech-research-ingestion`, reads its own buckets, and invokes the Neo4j capability. Its Cypher operation returns a scoped receipt and evidence references. The common kernel does not reinterpret that operation as SQL or equate a successful graph write with mission acceptance.

## Required implementation proofs

These are future release gates, not tests run for this document:

- Both applications run through one service build and matching common-schema component releases.
- Wrong issuer, tenant, installation, artifact reference, or application selector cannot read or mutate the other application's resources.
- Concurrent requests and worker executions do not leak database clients, credentials, cache values, or context across installations.
- Duplicate dispatch and ambiguous domain effects reconcile without duplicate ingestion; failure of one project never reroutes to the other.
- Dependency release consumes exact accepted outputs, with verifiable sandbox and context manifests.
- Crash recovery, retry, continuation, child execution, and revocation preserve the pinned ownership boundary.
- SQL and Cypher executors return the common receipt contract while preserving domain-specific validation and evidence semantics.
- Upload failures, orphan objects, deletion restrictions, backup/recovery, and scoped capability access are exercised.

Existing StageGraph, GoalDirected, and Deep Agents qualification evidence remains a regression baseline. Refactoring must establish conformance to the new canonical semantics rather than merely renaming old classes.

## References and documentation validation

- [Existing general runtime contracts](../../aiengineer/ai-engineer-meta/ai-engineer-architecture/specs/general-mission-control/RUNTIME-CONTRACTS.md)
- [Existing general database specification](../../aiengineer/ai-engineer-meta/ai-engineer-architecture/specs/general-mission-control/DATABASE.md)
- [Mission storage and retrieval synthesis input](MISSION_STORAGE_AND_RETRIEVAL_PROJECTION.md)
- [Source intelligence research](../biotech-meta/docs/research/2026-07-16-source-intelligence-workflow-research.md)
- [Governed workflow and mission memory](../biotech-meta/docs/specs/pre-research/control-plane-capabilities/03-governed-workflow-and-mission-memory.md)
- [Supabase Auth architecture](https://supabase.com/docs/guides/auth/architecture)
- [Supabase private buckets](https://supabase.com/docs/guides/storage/buckets/fundamentals)
- [Supabase Storage S3 compatibility and versioning limits](https://supabase.com/docs/guides/storage/s3/compatibility)
- [LangSmith data plane](https://docs.langchain.com/langsmith/data-plane)

External references were consulted during the architecture discussion; account-specific entitlements were not inspected. Documentation validation covers local links, code fences, and consistency with the recorded approval. It does not certify runtime behavior or infrastructure readiness.
