# Mission Control execution directive

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

Date: 2026-10-02, America/New_York.

Status: Owner-directed priorities and clean-break implementation policy. Reusable handoff for Claude Code ultracode, Codex, and subsequent implementation sessions. This document records the work to execute; it does not claim that the refactor is complete.

## Read this first

Transform `biotech-research-ingestion-evaluation-system` into `mission-control`. Refactor the existing Python implementation into a general Mission Control system, adopt the specification's names and semantics, replace MongoDB/Beanie persistence with PostgreSQL, and recover the current Biotech system's demonstrated Stage Graph and goal-directed capability level through the new general runtime.

Do not wait for the full canonical specification before delivering this working milestone. Do not build a second kernel beside the old system. Reuse working implementation and behavioral tests, change them decisively, and remove obsolete paths.

The owner states that there are no live users and no critical data requiring preservation. Existing Biotech Mongo collection contents are disposable. Backward compatibility, legacy data migration, old API aliases, dual writes, old worker history support, and parallel old/new engines are not deliverables.

This directive supersedes preservation and compatibility requirements in the [accepted architecture](ACCEPTED_APPLICATION_AND_STORAGE_ARCHITECTURE.md), [implementation plan](general-mission-control/IMPLEMENTATION.md), and other earlier specifications. The approved per-application Supabase databases, users, buckets, grants, and runtime binding remain in force. Historical evidence is useful to understand behavior; old persisted execution identities are not compatibility constraints.

## Priority order

| Priority | Outcome | Completion boundary |
| --- | --- | --- |
| 1 | Carve out, rename, generalize, and clean up Mission Control with PostgreSQL | Working Stage Graph and Goal Loop missions at the verified current Biotech capability level, including the relevant Deep Agents and Temporal lifecycle behavior |
| 2 | Consolidate the complete canonical specification and remove superseded documents | One coherent authority for implemented and planned behavior, including memory and source intelligence, with no competing naming/specification chains |
| 3 | Design Knowledge Services for both applications | Decide shared versus application-specific responsibilities from concrete SQL and Neo4j workflows; separate implementations remain an acceptable outcome |

Small specification decisions required to implement Priority 1 belong in Priority 1. An exhaustive specification exercise does not. Cursor Cloud SDK, Eve, a TypeScript server, new workflow families, a marketplace, and complete Knowledge Services implementation are not prerequisites for the parity milestone.

## Clean break policy

Rename the backend/repository directory to `mission-control` and use the neutral Python import/package identity `mission_control`. Update entrypoints, imports, dependency manifests, deployment configuration, tests, environment documentation, and developer instructions consistently. Do not retain the old application as a compatibility shell or leave the implementation under `biotech-research-ingestion-evaluation-system/packages/mission-control/`.

When a physical move conflicts with an active working directory or another session, sequence the move with the current workers rather than inventing permanent aliases. Verify absolute source and destination paths and avoid overwriting an existing destination. This is coordination of the work, not a requirement to support legacy callers.

Replace Mongo repositories, Beanie models, connection setup, startup checks, dependencies, environment variables, development containers, and Mongo-specific tests for the general runtime. No import/backfill/export of existing Mongo documents is required. Start the new schema with fresh data and fresh executions. Reuse existing PostgreSQL authority and transactions where sound rather than replacing them unnecessarily.

Remove obsolete runtime code and alternate names after their needed behavior has moved. No old endpoints, deprecated aliases, migration shims, dual schemas for old/new mission models, or old Temporal workflow names are required for compatibility. New runs must survive their own retries, restarts, and replay; clean-break policy does not remove durability requirements from the new system.

“PostgreSQL everywhere” means all authoritative structured Mission Control persistence, including the document responsibilities currently assigned to Mongo. It does not mean putting artifact bytes in PostgreSQL, replacing Temporal Cloud history, removing LangSmith traces, or moving Biotech's domain graph out of Neo4j. Use typed relational tables plus validated/versioned JSONB where appropriate; do not reproduce Mongo as one undocumented JSON dump.

The owner has waived preservation of old application execution data and Biotech Mongo contents. Do not turn that into a command to wipe unrelated projects, AI Engineer entity schemas, Neo4j, credentials, or shared cloud resources. Discarding old Mongo data means it need not be migrated or kept running; remote collection deletion is not necessary to finish this refactor. This document authorizes no blanket infrastructure reset.

## Minimal contract authority for the first milestone

Read these sources with this directive taking precedence:

1. [Accepted application and storage architecture](ACCEPTED_APPLICATION_AND_STORAGE_ARCHITECTURE.md): application ownership, Supabase buckets, runtime bindings, and infrastructure responsibilities.
2. [System proposal](MISSION_CONTROL_SYSTEM_PROPOSAL.md), especially Part A: naming input to repurpose for Python. Its Eve, TypeScript runtime, deferred capabilities, and compatibility assumptions do not govern this build.
3. [Mission Control architecture](MISSION_CONTROL_ARCHITECTURE.md) and [workflow semantics](workflow-types/index.md): domain vocabulary, workflow structure, lifecycle, and acceptance semantics. They are substantial but not a complete canonical specification.
4. The local [runtime contracts](general-mission-control/RUNTIME-CONTRACTS.md), [database specification](general-mission-control/DATABASE.md), and [implementation plan](general-mission-control/IMPLEMENTATION.md): reuse applicable contracts and tests, replacing their stale package-home, compatibility, and initial-Cursor requirements.
5. Current production Biotech code, tests, and CP-030/CP-040/BP-010/BP-020 evidence: inspect the actual behavior and proof limits before declaring parity.

Record a compact, implementation-ready naming/semantic map and capability checklist at the start of Priority 1. Resolve only the ambiguities needed for this milestone; record larger gaps for Priority 2. Keep one selected contract per concept rather than carrying alternative names in the implementation.

### Naming rules

| Surface or concept | Rule for this build |
| --- | --- |
| Repository and Python package | `mission-control` and `mission_control` |
| Wire keys, Python fields/functions/modules, SQL names | `lower_snake_case`, preserving the same domain term across surfaces |
| Python model/class names | `PascalCase`; Pydantic contracts and Python protocols replace Zod/TypeScript runtime constructs |
| Enum values | Exact common vocabulary; no application-specific alternate lifecycle values |
| Mission execution | Mission, Mission Revision, Mission Run, Activation, Attempt, Agent Session, Session Turn, and Harness Execution remain distinct identities |
| StageGraph | Stage Graph in prose; `StageGraph` in Python types; `stage_graph` in wire/module names |
| GoalDirected | Goal Loop in the general vocabulary; `GoalLoop` in types; `goal_loop` in wire/module names |
| AgentConfig and materialization | Distinguish reusable authored configuration from an immutable resolved execution binding; map current fields by meaning rather than renaming all configuration objects to one type |
| HTTP, CLI, MCP | Apply the proposal's consistent resource/action grammar through a single operation catalog; no legacy aliases |
| Events and Temporal registration | Select the applicable common event and registration vocabulary once; update producers, consumers, workers, and tests together |

The GoalDirected-to-Goal Loop change requires semantic review. Separate iteration, infrastructure retry, continuation, fork, and revision. Separate lifecycle, phase, and terminal outcome where required by the target contract. An agent's successful completion is not automatically accepted mission output. Do not claim every advanced Goal Loop feature is implemented merely because a class was renamed.

Retain the system proposal's naming discipline while removing its language/provider coupling. Python is the runtime. TypeScript may remain a generated client or existing UI language. Eve is not an implementation lane for this milestone.

## Capability parity inventory

Inspect these existing areas before moving files. Paths below are discovery anchors under the current backend; update the execution record with their new locations after renaming.

| Capability | Inspection anchors | Required new-system evidence |
| --- | --- | --- |
| Stage Graph | `app/temporal/workflows/stagegraph.py`, BP-010 evidence and tests | Dependencies, applicable joins, incremental release, output admission, bounded cycles/retries, cancellation and recovery |
| Goal-directed execution | `app/temporal/workflows/goal_directed.py`, BP-020 evidence and tests | Iteration, independent verification, convergence/governors, session rollover and exact context/artifact handoff |
| Deep Agents assembly | `app/integrations/agents/deep_agents/materializer.py`, CP-040 evidence | Exact configured model, skills, MCP/tools, middleware, schemas, workspace and checkpoint/store bindings materialize correctly |
| Agent Server | `app/agent_server/`, `app/integrations/langgraph_agent_server.py` | Governed bounded graph execution and reconciliation through the general application scope |
| Async subagents | `app/application/async_subagents/`, `app/integrations/agents/deep_agents/async_subagents.py` | Launch, ownership, observation, settlement, cancellation, recovery, and stale-result rejection with PostgreSQL persistence |
| Continuation and fork | `app/application/runtime/runtime_recovery.py`, associated tests | Continuation preserves intended logical identity; fork creates fresh run/session/budget identity and does not inherit live children accidentally |
| Durable reconciliation | Existing run-control/runtime PostgreSQL repositories and Temporal activities | Idempotent dispatch, generation fencing, authoritative state transitions, uncertain-effect reconciliation and crash recovery |
| Workspace and artifacts | `app/application/workspaces/`, materialization contracts | Correct application's artifacts hydrate the sandbox; manifests and output receipts remain attributable and durable |

These are parity targets, not assertions that every existing path is already complete. For each capability record: current evidence, observed limitation, destination, changes, and new proof result. Fix defects encountered in required paths. Do not spend the first milestone implementing unrelated future features.

Temporal remains the sole macro scheduler. Agent Server and async subagents remain bounded participants; neither becomes a second mission authority. Preserve these runtime capabilities instead of deleting them to make the refactor look simpler.

## Fastest implementation sequence

1. **Inventory and freeze the small target.** Inspect current working changes, instruction files, production wiring, dependency versions, and relevant evidence. Write the naming map, parity checklist, and a short sequence of executable slices. Proceed into implementation in the same session.
2. **Make the clean structural cut.** Establish the neutral package and rename the application, update entrypoints and configuration, separate domain-specific Biotech operations from the kernel, and eliminate duplicate bootstrap paths. Keep enough targeted tests runnable to diagnose each slice.
3. **Finish PostgreSQL persistence.** Map every retained Mongo responsibility to an owned table/JSONB contract, constraints, transactional behavior, and repository. Include immutable definitions, configuration/bindings, Goal Loop details, subagent documents, manifests, and metadata actually used by the retained runtime. Remove Mongo/Beanie entirely from the runnable system. Fresh schema installation is sufficient; legacy data upgrades are not required.
4. **Implement real application differentiation.** Use the same schema release in two application installations. Authenticate before selecting the trusted binding; pin application/installation/tenant and configuration digest at admission. Route database pools, private Supabase buckets, capabilities, workers, Agent Server calls, checkpoints, and sandbox materialization through that binding. No hardcoded Biotech schema or Neo4j client belongs in the common kernel.
5. **Close Stage Graph and Goal Loop verticals.** Adapt existing interpreters and durable execution to the selected names and semantics, with materialization, independent verification, async subagents, continuation, fork, and reconciliation intact. Reuse known working logic; do not recreate it from scratch merely to fit a folder layout.
6. **Qualify and clean.** Run the focused parity/isolation/failure tests, repair failures, remove dead paths and dependencies, and produce a concise runbook and evidence report. Distinguish local qualification from live Supabase/Temporal Cloud deployment. Use finite live probes only against configured, authorized targets; missing credentials do not block local implementation.

Build the smallest public API/CLI surface needed to launch, inspect, and control these missions using the common handlers. Full dashboard redesign, marketplace discovery, complete remote-client onboarding, and advanced recursive workflow composition must not become accidental prerequisites.

Use PostgreSQL and Docker locally when useful; production targets are each app's Supabase project and the shared Temporal Cloud account. Use two isolated test installations and different app capability fixtures to prove generality before complete Knowledge Services exists. An executable fixture is evidence of the boundary, not a claim that real domain ingestion has shipped.

## First milestone completion criteria

Priority 1 is complete when all of the following have recorded evidence:

- The renamed neutral Python application builds and runs without sibling application imports or a Biotech-only bootstrap.
- PostgreSQL owns all retained Mission Control structured persistence. No MongoDB/Beanie dependency, connection requirement, dual write, or fallback remains in the runnable application.
- The same build executes Stage Graph and Goal Loop missions for both application bindings, using separate database installations and private artifact stores.
- Deep Agents configuration materialization, bounded Agent Server, async subagents, continuation, semantic fork, and Temporal reconciliation meet the inspected parity checklist.
- Application A cannot select B's database, artifact, session, checkpoint, capability, or tenant scope. Equal resource UUIDs across installations and concurrent requests do not leak context.
- Worker restart, duplicate delivery, cancellation, stale generations, dependency release, and uncertain effects preserve new-system correctness.
- Tests and runbooks use the new names and contracts. Unsupported advanced features reject before execution; they are not advertised as implemented.
- The final report states what ran, what passed, what was skipped, any remaining defect, and whether the result is locally qualified or actually deployed. Old evidence is never reported as a new test run.

## Specification consolidation after the working milestone

Repurpose the system proposal's naming material into the canonical Python naming contract. Synthesize the architecture, workflow specifications, local general specification pack, storage/runtime acceptance, actual implementation decisions, memory requirements, and source-intelligence research into one coherent specification set. The canonical set may contain focused documents; it must have one clear authority and entry point.

Include [source intelligence](../biotech-meta/docs/research/2026-07-16-source-intelligence-workflow-research.md) and [governed memory](../biotech-meta/docs/specs/pre-research/control-plane-capabilities/03-governed-workflow-and-mission-memory.md). Define their boundary and intended contracts without claiming their full implementation is necessary for Priority 1. Separate implemented behavior from future requirements.

Remove superseded documents after transferring their relevant requirements, resolving conflicts, and updating links. Do not maintain competing canonical copies here and in AI Engineer. Evidence may remain in a clearly non-normative evidence area; it does not impose runtime backward compatibility.

## Knowledge Services follows

Investigate concrete AI Engineer SQL workspace/intent/query workflows and Biotech Cypher/Neo4j ingestion/Graph RAG workflows. Both use immutable intent artifacts and deterministic executors, but common receipt or artifact contracts do not prove that the full service implementation should be shared.

Recommend a shared implementation only for responsibilities whose semantics actually align. Application-specific services, or two services sharing a small contract/library, are valid outcomes. Do not force this decision into the Mission Control refactor or teach the kernel domain ingestion algorithms.

## Multi-session execution handoff

Use one durable execution log next to this directive, created by the first implementation session. Record the active checkout and eventual renamed path, current revision/working changes, naming decisions, schema source/version, capability checklist, completed slices, exact tests/results, known gaps, and next action. The log reports progress; it cannot redefine this directive.

Sessions should read the log and current code before acting. Do not repeat baseline research unnecessarily. Concurrent sessions need explicit file ownership and an integrator for package renames, shared contracts, schema, and entrypoints. Never have multiple agents independently move the same tree or edit the canonical naming map. A handoff must identify incomplete changes so another session can continue without inventing compatibility scaffolding.

### Prompt for Claude Code ultracode or another implementation session

> Read `mission-control-general/EXECUTION_DIRECTIVE.md` and its accepted architecture reference. Execute Priority 1 now: transform and rename the existing Biotech backend into `mission-control`, adopt neutral Python contracts and the selected Stage Graph/Goal Loop vocabulary, replace all retained Mongo/Beanie persistence with PostgreSQL, implement application-bound Supabase database/bucket/runtime routing, and restore the current demonstrated Deep Agents plus Temporal capabilities, including Agent Server, async subagents, continuation, forking, and reconciliation. There are no live users or legacy-data preservation requirements. Do not build backward compatibility, dual writes, legacy history support, or a second kernel. Inspect and reuse current production behavior and tests; record a small naming map and capability checklist, then implement without waiting for the full specification. Keep the execution log current, qualify the new verticals and isolation, and report exact proof results. Full canonical-spec consolidation is Priority 2. Knowledge Services design is Priority 3. Do not substitute either for delivering the working Mission Control milestone.

This document's creation changes documentation only. It does not launch an external coding session, rename the repository, remove data, or claim a completed runtime milestone.
