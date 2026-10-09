---
type: Specification
title: General Mission Control implementation and acceptance plan
description: The expanded dependency-linked issue packet and team workspace are the detailed execution plan for the current expanded owner request. This base sequence describes integration ordering; it cannot defer required…
tags: [mission-control, spec, normative]
---
# General Mission Control implementation and acceptance plan

The [expanded dependency-linked issue packet](expansion/REQUIREMENTS.md) and [team workspace](expansion/TEAM-WORKSPACE.md) are the detailed execution plan for the current expanded owner request. This base sequence describes integration ordering; it cannot defer required Knowledge Services integration, WebSocket, mobile, generative UI or MCP Apps/MCP-UI beyond complete release. Foundational proofs, feature implementations, data readiness, integration and release have separate stable issue IDs and gates. No calendar timeline or live authorization follows from this plan.

Canonical consolidation 3, 2026-10-02. This is the implementation sequence for [SPECIFICATION.md](SPECIFICATION.md), not an instruction to refactor or deploy during the documentation review. [CODEBASE-ORGANIZATION.md](CODEBASE-ORGANIZATION.md) defines the proposed physical layout.

## 1. Establish the neutral runtime and common contract

Transform/rename the existing `biotech-research-ingestion-evaluation-system` backend to `mission-control`, import `mission_control`. Inspect active work and destination before the move. Generalize the current production Python modules and behavioral tests; do not create a parallel kernel or retain old endpoints, Mongo/Beanie, old engine registries, or legacy execution-data migration. Preserve unrelated application/domain data. New runs still require their own release/replay compatibility.

Implement strict Pydantic contracts, discriminated behaviors, deterministic compiler and one operation catalog. Generate public JSON Schema/OpenAPI/client artifacts. Use the local workflow suite as the semantic contract; reject unimplemented behaviors before execution. Separate domain rules/ports from persistence, Temporal and vendor SDKs. Pin one reproducible dependency lock; exact library versions require implementation-time qualification.

**Exit:** isolated-checkout Python build with no sibling imports; common contract fixtures and lifecycle vocabulary agree; no Biotech-only scheduler/bootstrap; known current capability evidence mapped to new tests without claiming automatic parity.

## 2. Install PostgreSQL and application bindings

Release the self-contained common SQL component from `mission-control-db-contract` (`mission-control/packages/mission-control-db-contract/`, ownership amendment 2026-10-03). Install/verify that exact release with its shared `mission-db` installer using `deployments/biotech/` (the `biotech-postgres-db-contract` thin wrapper) and `deployments/ai-engineer/`; `ai-engineer-db-contract` entity migrations are not involved. Seed app-specific installation/catalog/policy data through versioned idempotent bundles. No independent common SQL regeneration or app-domain schema dependencies.

Implement authenticated application/tenant resolution, scoped repositories, request receipts, aggregate locking, budgets, event writer, inbox/outbox and effect intents/receipts. Use two disposable installations with intentionally equal tenant/resource UUIDs. Configure private `mission-artifacts`, `knowledge-artifacts` and `capability-bundles` stores. Begin Agent Server/checkpointer persistence in qualified restricted app-local runtime schemas; keep its role/versioning separate.

**Exit:** identical schema fingerprints; repeat apply/seed is idempotent; checksum or installation mismatch fails; concurrent pooled calls cannot cross applications; no raw database credentials reach agents. Object upload/registration failures reconcile. Existing unrelated Auth/entity data remains untouched.

## 3. Restore the useful Temporal and Deep Agents vertical first

Reuse production Stage Graph and goal-directed implementation candidates against canonical Stage Graph/Goal Loop semantics. Implement root/activation/operation durability, exact accepted input release, human/proof gates, timers/events, bounded governors, commands and externally computed completion. Temporal payloads contain scoped references; all external I/O runs in activities/services.

Generalize exact Deep Agents graph/model/tool/MCP/skill/middleware/workspace materialization. Qualify one frontier model route and the sandbox backend. Bind Agent Server for asynchronous subagents in **both** applications; preserve sync execution too. Implement launch identity, lost-response recovery, child grants/reservations, dependency policy, cancellation and result admission. This component is required, not a later optional optimization.

Prove continuation, technical retry, semantic fork and diagnostic replay as separate operations. Pause stops releases and reconciles existing work at qualified boundaries; cancellation does not declare settlement merely because a provider accepted a cancel call. Restore counters, receipts and ownership without duplicate effects.

**Exit — first parity milestone:** same Python build performs Stage Graph and Goal Loop for both app bindings with Deep Agents, Agent Server async children, PostgreSQL state and own artifact stores. Prove required demonstrated behavior with current fixtures and failure injection. Cursor, Swarm/Optimizer completion, full dashboards and complete Knowledge Services are not prerequisites. Unsupported advanced behavior remains explicitly unavailable.

## 4. Complete the coordinator and public control surfaces

Ship draft/validate/proposal/commit/activate/start, inspection, human decisions, commands and receipt recovery through shared HTTP/MCP handlers and the CLI client. Ship one canonical Agent Skill with pinned supporting files. Resource-bound authentication, application grants and execution reporting scopes are enforced at application boundaries.

Expose canonical SSE with replay, gaps and resync, and bounded MCP event reads. Bind both existing dashboards through the generated client. Include meaningful recovery, outstanding effects/children, budgets and delivery reports. Asset selection resolves admitted immutable versions; absent domain capabilities produce visible blockers.

**Exit:** equivalent CLI/MCP/HTTP requests have equivalent authority, digest and receipt semantics; validation has no side effects; real supported host onboarding and skill loading are qualified separately from file generation. Both dashboards preserve app scope and show stale streams honestly.

## 5. Add Cursor SDK Cloud and direct frontier-provider profiles

Implement the Python Cursor SDK Cloud adapter behind the common harness protocol. Lock SDK/bridge artifacts and qualify native launch/reattach/observe/cancel/usage semantics. Use isolated repository workspaces with pinned base, path grants and immutable patch/test outputs. Do not assume native fork or pause.

Implement bounded direct-provider execution profiles separately from the frontier model route already used by Deep Agents. Profile limits, result schemas, billing uncertainty and unsupported controls are explicit. Provider failover must respect immutable binding and effect uncertainty.

**Exit:** Cursor coding fixture supplies a patch, independent tests and review without unauthorized merge/deploy. Each enabled frontier profile passes accounting, cancellation/recovery and output admission; profiles cannot claim unsupported session behavior. Same contracts apply in both installations.

## 6. Complete all required workflow semantics

Implement Parallel Swarm, Evaluator Optimizer, full recursive composition, Stage Graph revisit/fan-out/provisional inputs, mission invocation and relationships, revision transitions/carry-forward, and complete continuation controls. These are committed general-system scope, not optional future research.

Use distinct counters/identities for infrastructure retry, semantic remediation, revisit, Goal Loop iteration, optimization round and continuation. Required outputs and child outcomes govern acceptance. An independent mission requires its own admission/goals/grants; native subagent fan-out is not Child Mission Invocation.

**Exit:** all workflow suite conformance cases pass; quorum/governor exhaustion is explicit; producer/evaluator independence holds; new revisions do not reinterpret in-flight work; exact output projection and digest-based carry-forward are enforced.

## 7. Qualify deployment and domain integrations

Build/pin API, relay, worker and Agent Server artifacts in one release manifest. Use app-bound worker/server pools, recommended per-app/environment production namespaces, independent secrets, project-local stores and qualified runtime persistence. AWS/Temporal Cloud/LangSmith remain the deployment baseline, not evidence of deployment. Schema/account/profile identifiers are operator inputs.

Qualify both apps with actual Auth, admitted domain endpoints and finite authorized provider budgets. A database outage fails that app's admission without rerouting to the other. Prove backups include both database metadata and referenced object bytes. Subsequent releases of the new system preserve active run compatibility. Complete Knowledge Services design from governed SQL and Neo4j workflows; separate service implementations remain a valid outcome.

**Exit:** deployed evidence is separately recorded from local tests; both dashboards and coordinator clients complete an authorized representative mission in their own application. Production qualification cannot be inferred from historical local evidence.

## Reuse map

These are discovery anchors in the existing backend, not claims that each file was requalified in this documentation pass:

| Current area | General destination / responsibility |
| --- | --- |
| `app/application/run_control/` | Scoped admission, commands, budgets, authoritative transitions and outbox |
| `app/temporal/workflows/` | Temporal root/composition/operation adapters |
| `app/integrations/agents/deep_agents/` | Exact materializer and bounded Deep Agents harness |
| `app/agent_server/`, `app/integrations/langgraph_agent_server.py` | Required graph hosting and server adapter |
| `app/application/async_subagents/` | General subordinate admission/settlement services |
| `app/application/runtime/runtime_recovery.py` | Continuation/fork/retry policies and effect reconciliation |
| `app/application/workspaces/` | Scoped artifact/workspace materialization |
| Existing domain ingestion/knowledge services | App-owned admitted SQL/Cypher capabilities |

Historical CP-030, CP-040, BP-010 and BP-020 evidence supplies regression expectations. Tests and source presence alone do not prove neutral two-app execution. Keep historical reports attributed; do not import older experimental modules into production just to reuse their names.

## Acceptance matrix

| ID | Proof | Stage |
| --- | --- | --- |
| MC-I01 | Same component fingerprint in two databases; no domain-schema dependencies | 2 |
| MC-I02 | Spoofed app/tenant/installation and equal UUIDs cannot cross scope | 2–3 |
| MC-I03 | Standalone build and single authoritative kernel | 1 |
| MC-I04 | Repeat seeds are idempotent; changed digest rejects; revocations survive reseed | 2 |
| MC-D01 | Committed start/outbox retries recover one root and one effect identity | 3 |
| MC-A01 | Both app-bound Agent Servers recover lost launch without duplicate child | 3 |
| MC-A02 | Parent cancellation, server restart, stale result and unknown child usage settle correctly | 3 |
| MC-L01 | Technical retry preserves semantic identities and effect keys | 3 |
| MC-L02 | Fork gets new run/session/workspace/budget; no live child/command ownership copied | 3 |
| MC-L03 | Diagnostic replay has no domain writes or business effect claims | 3 |
| MC-L04 | Pause/resume validates checkpoint/frontier; reports actual delivery semantics | 3 |
| MC-L05 | Cancellation leaves uncertain effects and usage visible until resolved | 3 |
| MC-L06 | Stale execution generation cannot settle outputs or reservations | 3 |
| MC-C01 | Fake completion, missing evidence and unresolved required children block acceptance | 3 |
| MC-P01 | HTTP/CLI/MCP parity, attributed human resolution, skill bundle integrity | 4 |
| MC-P02 | Stream reconnection deduplicates; expired cursors require resync | 4 |
| MC-H01 | Deep Agents first, then each enabled Cursor/frontier profile passes harness conformance | 3, 5 |
| MC-H02 | Cursor patch/tests/review vertical respects repository and publication authority | 5 |
| MC-W01 | Four systems, invocation, revisions and continuation satisfy workflow suite | 6 |
| MC-R01 | Subsequent new-system releases preserve captured-history replay | 7 |
| MC-R02 | Both deployed applications complete scoped representative workflows | 7 |

Record exact builds, fixtures, environments, evidence digests, failures and skipped tests for each gate. A first parity milestone is not completion of the full general system. This plan records requirements; no tests or deployments have been performed by writing it.
