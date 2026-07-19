# 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts

**What to build:** Create the application service and provider-neutral ports that prepare, bind, execute, observe, and settle one semantic operation without exposing SDK, sandbox, MCP, or provider types as domain contracts.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 03 — Implement durable StageGraph orchestration

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/6


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Define provider-neutral domain and application contracts first, then place OpenAI, Docker, MCP, secret-store, and object-store details behind adapters.
- Persist intended execution before provider invocation and preserve preparation failure, actual resolved use, and settlement against the same semantic attempt.
- Use pre-provisioned immutable fixture assets initially so this ticket does not depend circularly on the later governed capability catalog.

## Verification approach

Invoke the application service from a real Temporal test activity with conformance fake providers. Prove binding-before-side-effect and exact retry behavior at the public result seam.

## Explicit non-goals

- Full OpenAI adapter behavior, capability catalog intake, workspace implementation, and workflow-specific prompts or models.
- Direct lifecycle or budget writes from MongoDB execution records.

## Acceptance criteria

- [ ] The service validates run/configuration/control revisions, semantic attempt identity, authority, capability selection, workspace contract, and reservation before execution.
- [ ] An immutable MongoDB/Beanie Operation Execution Binding exists before semantic external side effects and records all exact assets, policies, digests, authority, workspace, snapshot, tracing, and budget references.
- [ ] Provider-neutral runtime, sandbox, MCP, secret, event, artifact, and snapshot ports have a conformance implementation for integration testing.
- [ ] Preparation failure remains recorded and causes no provider invocation; unsupported required policy fails unless authored degradation explicitly permits it.
- [ ] Exact Temporal retry reuses the semantic attempt, binding, and side-effect keys and does not duplicate provider effects, usage, events, promotions, or lifecycle facts.
- [ ] A new semantic attempt receives a new binding with lineage to prior work.
- [ ] Secrets resolve just in time from references and never appear in persisted records, Temporal payloads, prompts, traces, events, manifests, snapshots, or errors.
- [ ] Usage and pending charges reconcile idempotently through PostgreSQL budget authority.

## Source basis

- Operation Runtime, Workspaces, Artifacts, and Snapshots
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
