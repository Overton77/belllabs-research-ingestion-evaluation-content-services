---
type: Specification
title: Mission Control codebase and database package organization
description: "Review proposal, 2026-10-02. Architecture and ownership constraints come from SPECIFICATION.md. The concrete directory layout below is proposed; this document does not create, move, or rename runtime repositories."
tags: [mission-control, spec, normative]
---
# Mission Control codebase and database package organization

Review proposal, 2026-10-02. Architecture and ownership constraints come from [SPECIFICATION.md](SPECIFICATION.md). The concrete directory layout below is proposed; this document does not create, move, or rename runtime repositories.

## Repository boundaries

| Root | Responsibility | Must not own |
| --- | --- | --- |
| `Biotech/mission-control/` | General Python service, contracts, compiler, application handlers, Temporal execution, Agent Server integration, harnesses, CLI/MCP, skill and deployment | Application entity schemas or application-specific mission engines |
| `Biotech/mission-control/packages/mission-control-db-contract/` | One independently released common Mission Control SQL component, generated MC-only contract, release manifest/digests and the shared tested installer (`mission-db`) | General runtime behavior, application entity tables or app-specific branching |
| `Biotech/mission-control/deployments/{biotech,ai-engineer}/` | Authoritative per-app target, binding and seed manifests pinning one common release | Editable common SQL |
| Existing `ai-engineer-db-contract` repository | AI Engineer domain entity tables, migrations and generated application types | Ownership of, or a prerequisite for, the common Mission Control component |
| `Biotech/biotech-postgres-db-contract/` | Proposed Python installation/verification package for the Biotech Supabase project; pins the common component | A second editable copy of common SQL or a Biotech fork of mission tables |
| AI Engineer database installation tooling | Installs the same pinned common component in AI Engineer's project | Biotech database access |
| App-owned domain services and dashboards | Domain knowledge, SQL/Cypher operations, application identity, templates and product UI | Direct mutation of Mission Control execution tables |

The runtime root is the clean transformation/rename of `biotech-research-ingestion-evaluation-system`, not a permanent nested package or a second kernel. Preserve reusable behavior and tests; no old Mongo data migration, old endpoint aliases, or old engine history support is required. New executions still require retry/replay/recovery correctness. Sequence the physical rename with active work when implementation is undertaken.

Ownership amendment 2026-10-03 (owner correction): the owner decided the neutral authority. The common component is `mission-control-db-contract` inside the Mission Control repository; `biotech-postgres-db-contract` becomes a thin consumer of its installer, and AI Engineer installs the same release through `deployments/ai-engineer/`. `ai-engineer-db-contract` keeps only AI Engineer entity tables. No common SQL was ever released from `ai-engineer-db-contract`, so nothing is moved out of it.

## General runtime layout

```text
mission-control/
  pyproject.toml
  uv.lock
  src/mission_control/
    contracts/                 # Pydantic public models, enums, operation catalog
    domain/
      authoring/               # definitions, revisions, deterministic compilation
      programs/                # stage_graph, goal_loop, parallel_swarm, evaluator_optimizer
      completion/              # typed acceptance expressions and decisions
      policies/                # authority, governors, budgets, carry-forward
    application/
      ports/                   # repositories, harness, artifacts, capabilities, domain operations
      installations/           # authenticated resolution and pinned binding
      missions/                # author, validate, commit, start, inspect
      execution/               # admission, transitions, commands, effect receipts
      subordinates/            # sync/async admission, settlement and cancellation
      artifacts/               # custody, materialization and context delivery
      recovery/                # continuation, fork, retry and reconciliation
    adapters/
      postgres/                # scoped repositories/UoW; consumes SQL contract
      temporal/                # deterministic workflows, activities, client dispatch
      deep_agents/             # graph assembly, exact materializer and harness
      agent_server/            # authenticated graph submission/observation/recovery
      cursor_cloud/            # qualified Python Cursor SDK Cloud harness
      frontier/                # model routes and bounded direct-provider execution
      supabase_storage/        # private artifact/bundle object port
      langsmith/               # tracing and qualified sandbox backend
      capabilities/            # generic HTTP/MCP/domain intent invocation
    interfaces/
      http/                    # FastAPI transport and SSE
      mcp/                     # tools/resources calling the same handlers
      cli/                     # missionctl via the public client
    bootstrap/                 # configuration, DI, entrypoints and role wiring
  agent_server/                # graph registration and server deployment configuration
  clients/typescript/          # generated contract/client; no runtime kernel
  skills/mission-control/      # one canonical skill and complete reference bundle
  deploy/                     # image, process roles, app-bound pools, local profiles
  tests/
    unit/                     # compiler/reducers/authority; no live providers
    contract/                 # schema, interface and adapter conformance
    integration/              # PostgreSQL, Temporal, Agent Server, storage
    acceptance/               # both app bindings, failure recovery, public verticals
  docs/                       # runtime runbooks and release qualification records
```

Start with one Python distribution, `mission-control`, importing `mission_control`. These modules are ownership seams, not a demand for dozens of separately published libraries. Split distributions only for a demonstrated deployment/dependency need. SDK objects stay inside adapters. Generated TypeScript is a consumer of Python contracts; it cannot become a second contract source.

Dependency direction: interfaces call application handlers; handlers use domain rules and ports; adapters implement ports; bootstrap wires them. Domain rules import neither FastAPI, Temporal, provider SDKs, nor database clients. Temporal workflow code uses deterministic contracts and activity results; activities call application services. General modules cannot import Biotech or AI Engineer source trees. Domain adapters execute behind admitted capability endpoints.

## Process roles and application binding

| Role | Work | Isolation |
| --- | --- | --- |
| `api` | HTTP, MCP, SSE, authentication and admission | Resolve trusted installation for each request |
| `relay` | Outbox dispatch, inbox ingestion, recovery and cleanup | Claims scoped per installation; independent retry/backoff |
| `worker-control` | Temporal roots, recursive composition, durable controls | App/environment-bound namespace and queues |
| `worker-agent` | Bounded harness activities and usage/artifact collection | App-bound credentials, quotas and pools |
| `agent-server` | Registered Deep Agents graphs and asynchronous subordinate execution | Required for both applications; app-bound server deployment/pool and runtime persistence |

Reuse one release manifest and, where dependencies permit, one image with separate process entrypoints. Agent Server may require its own supported image/runtime packaging; pin that artifact in the same manifest. A shared codebase does not require a shared credential pool. Temporal alone owns mission scheduling. Agent Server owns bounded graph execution and native run/thread state; PostgreSQL owns admission and acceptance.

App domain packs contain templates, output/assessment schemas, rubrics and capability references. Their content is versioned and admitted into each installation, not imported into the kernel as conditional branches. SQL versus Cypher is resolved by a capability binding.

## Biotech database installation package

Proposed distribution: `biotech-postgres-db-contract`; Python import: `biotech_postgres_db_contract`; administrative CLI: `biotech-db`. It is deployment tooling, never a dependency needed to execute general runtime code.

```text
biotech-postgres-db-contract/
  pyproject.toml
  uv.lock
  component.lock.json         # release/version, source identity and all checksums
  src/biotech_postgres_db_contract/
    cli.py                    # plan, verify, apply; explicit installation target
    installer.py              # invokes shared component installer under migration lock
    preflight.py              # installation/schema identity and compatibility
    verification.py           # fingerprint, privileges, constraints and grants
    seeds.py                  # versioned, idempotent installation/catalog seed application
  installation/
    biotech.example.toml      # secret REFERENCES, not credentials
    storage-buckets.json      # mission-artifacts, knowledge-artifacts, capability-bundles
    runtime-persistence.toml  # private Agent Server/checkpoint binding
  seeds/                      # scoped installation, policy, template/capability manifests
  tests/                      # disposable install, repeat apply/seed, mismatch, isolation
  README.md
```

`plan` reports intended versions, current fingerprint, migration checksums, role/bucket changes and incompatible state without mutation. `apply` verifies the exact target, takes a database migration lock, applies the pinned component once, and records its release/checksums. Unknown or changed applied checksums fail closed. `verify` reads identity, privileges and component fingerprint independently of an apply. Common migration behavior belongs to the shared component tooling; this wrapper supplies the Biotech target and installation checks.

Do not regenerate copied SQL locally. The shared release ships source provenance, ordered migrations, checksums, minimum database requirements, generated contract exports and compatibility metadata. Package builds consume a pinned `mission-control-db-contract` release artifact and never resolve sibling repository directories at runtime. An offline/reproducible build may include a verified immutable release artifact, clearly generated and read-only.

The installer manages common `mission_control` installation and app-specific provisioning configuration. Agent Server/LangGraph schema setup follows its pinned supported tooling with a restricted migration role and separate component/version record; do not edit vendor tables by hand. Private runtime persistence starts in the corresponding app's PostgreSQL, subject to compatibility/load qualification. App membership/domain tables remain owned by their existing installers. Supabase Auth users are not copied between projects; Neo4j is outside this package.

Runtime APIs/workers receive restricted roles. Migration credentials stay with deployment tooling. Objects in the three private buckets have independent upload/registration and cleanup protocols; a database transaction cannot atomically provision remote buckets or upload bytes. A partial provisioning attempt records progress and resumes by resource identity.

## Release and verification

The release manifest pins runtime build/lock, common schema release/checksums, public schema/operation catalog, Agent Server artifact/graphs, provider profiles, sandbox profiles, skill bundle and application binding versions. App bindings independently select grants and endpoints while using compatible common releases.

Required proofs: isolated-checkout build; identical common component in two disposable databases; no AI Engineer domain schema dependency; repeated install without duplicate application; checksum drift rejection; cross-app/tenant access denial; both apps' Agent Server lost-launch recovery; and no direct domain imports in the kernel. Provisioning and deployed qualification remain separate from this documentation review.

## Application seed contract

Schema installation and application seeding are separate operations. Both apps receive identical common tables. Versioned seed bundles populate their own installation record, policy profiles, templates and admitted capability definitions/bindings. Seeds take actual installation IDs and secret references from trusted configuration, not hardcoded project credentials. User accounts/memberships and privileged grants require their owning application provisioning path; a seed cannot manufacture operator authority.

Each seed has a stable key, version and content digest; record an application receipt and reject same-version/different-content reapplication. Repeated application is idempotent. Updating a template or capability creates a new immutable version; it cannot rewrite pinned run bindings or silently restore revoked grants. Database seed changes and their receipt commit together; remote bucket provisioning has separate resumable receipts. Both installers verify the common schema fingerprint before seeding. The wider package name permits later Biotech PostgreSQL components without placing Neo4j entity schema or a second common Mission Control schema under this package.
