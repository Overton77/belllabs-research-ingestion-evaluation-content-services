# 09 — Implement the OpenAI Agents runtime adapter and bounded delegation

**What to build:** Map validated Operation Execution Bindings to OpenAI Agents SDK primitives behind the provider-neutral contract, translate execution evidence into project records, and support only bounded operation-local handoff and task-subagent delegation.

**Blocked by:**
- 05 — Implement linked-run composition and durable orchestration continuity
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts
- 07 — Implement isolated Run Workspace namespaces and materialization

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/9


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Keep SDK construction, hooks, event classes, sessions, and streaming entirely inside the adapter; emit project-owned contracts at its boundary.
- Validate delegation before creating agents or workspaces, and preserve one operation attempt across handoff and task-subagent execution.
- Make the recognized Workflow Type boundary check explicit and return a typed linked-run requirement instead of launching hidden durable work.

## Verification approach

Run adapter contract tests against fakes plus one minimal non-destructive real SDK scenario. Assert exact binding, bounds, durable results, translated events, and side effects—not prose.

## Explicit non-goals

- Making SDK sessions canonical, allowing hooks to mutate domain stores, or using agents as an unrecorded workflow engine.
- Final model defaults, prompt content, or provider-specific domain types.

## Acceptance criteria

- [ ] The adapter maps exact models, prompts/instructions, structured outputs, tools, MCP, approvals, guardrails, hooks, tracing, sessions, and sandbox policy or returns a typed unsupported-policy result.
- [ ] SDK events translate into versioned project envelopes without exposing SDK event classes as application contracts.
- [ ] Accepted final messages and structured results are durable before acknowledgement; fine-grained token deltas may remain ephemeral.
- [ ] SDK sessions are replaceable runtime projections and cannot mutate Conversations, Workflow Runs, approvals, or canonical outputs.
- [ ] Handoff and task_subagent remain inside one operation and receive intersected authority, budget, depth, concurrency, tools, data, network, and private workspace policy.
- [ ] Dynamic Agent Definitions and delegation results are immutable and fully bound.
- [ ] Work needing independent lifecycle, reusable output, substantial budget, durable waits/cycles, or a recognized Workflow Type is rejected and routed to linked-run admission.
- [ ] A minimal real-adapter compatibility test is strict on structured bounds and side effects while remaining tolerant of model wording.

## Source basis

- Operation Runtime, Workspaces, Artifacts, and Snapshots
- Governed Agentic Capability Catalogs and Exact Bindings

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
