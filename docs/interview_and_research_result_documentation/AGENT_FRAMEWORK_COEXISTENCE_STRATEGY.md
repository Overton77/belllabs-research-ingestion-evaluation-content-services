# BellLabs agent-framework coexistence strategy

Status: active architecture direction  
Scope: `biotech-research-ingestion-evaluation-system`  
Implementation priority: LangGraph + Deep Agents Agent Server

## Statement

BellLabs will support multiple leading agent frameworks and model-provider runtimes behind shared BellLabs contracts. The system is simultaneously:

1. the research, ingestion, and evaluation service for the BellLabs biotech recommendation application; and
2. a frontier agent-framework and model service for testing how different agent systems perform on demanding, evidence-aware biotech work.

The immediate goal is to bring the LangChain/LangGraph/Deep Agents/LangSmith path—especially Deep Agents on Agent Server—up to speed for real research and governed ingestion. After that path is operational, BellLabs will implement comparable OpenAI-native and Anthropic-native execution paths and evaluate them rather than assuming one universal framework is best.

This is a coexistence strategy, not permission to create competing domain models or parallel sources of truth.

## Runtime families in scope

### LangChain ecosystem — current implementation priority

- LangChain for model, tool, middleware, structured-output, and provider integrations.
- LangGraph for explicit graphs, durable state, checkpoints, interrupts, streaming, and resumable execution.
- Deep Agents for long-horizon planning, subagents, context management, filesystem-oriented work, and reusable agent harnesses.
- LangSmith for tracing, datasets, evaluation, debugging, and operational visibility.
- Agent Server as the first actively developed agent-serving runtime, using the owner's available LangSmith plan and deployment capabilities where appropriate.

The near-term milestone is an end-to-end path that can research, preserve evidence and provenance, propose ingestion candidates, pass BellLabs validation/review gates, and settle typed results through the existing control plane.

### OpenAI-native ecosystem — comparison path

- OpenAI API and models.
- OpenAI Agents SDK for native agent, tool, handoff, tracing, and guardrail patterns.
- Temporal for durable orchestration where it provides the required workflow guarantees.
- FastAPI for governed service and application-facing transport.

Existing OpenAI Agents SDK, Temporal, and FastAPI work is useful implementation evidence and may remain an executable path. It must be adapted to the same runtime-neutral contracts used by the LangGraph path rather than becoming a separate BellLabs authority.

### Anthropic-native ecosystem — comparison path

- Anthropic API and models.
- Anthropic's native agent SDK/tooling for provider-native agent behavior, tool use, context handling, and observability where available.

This path should also enter through the same BellLabs execution ports and exact bindings. Provider-specific strengths should remain available through explicit capability declarations instead of being erased to achieve superficial uniformity.

## Architectural principle: stable BellLabs core, plural execution runtimes

BellLabs owns the meaning of a run. Frameworks execute bounded work within that meaning.

The shared BellLabs layer remains authoritative for:

- workflow types and exact Workflow Implementations;
- immutable definitions, compilation, Effective Run Configurations, and run plans;
- admission, identity, lifecycle transitions, budgets, commands, approvals, and cancellation;
- source registration, evidence, claims, provenance, artifacts, and ingestion candidates;
- schema grounding, graph access policy, ingestion validation, review, and approved writes;
- evaluation cases, evaluator versions, acceptance gates, and result settlement;
- tenant isolation, security policy, audit history, and durable domain state.

Execution runtimes may own their mechanics—threads, checkpoints, activities, spans, tool-call state, provider sessions, and recovery mechanisms—but those mechanics do not redefine BellLabs truth.

```text
BellLabs intent + exact run binding
                 |
       runtime-neutral ports
        /        |         \
 LangGraph    OpenAI +     Anthropic
Deep Agents   Temporal     native agents
        \        |         /
      typed results + evidence
                 |
 BellLabs validation, evaluation,
 ingestion gates, and settlement
```

## Shared contracts without a least-common denominator

Coexistence does not mean flattening every runtime into the smallest shared feature set. Contracts should have two layers:

1. **Shared semantic contracts** describe the provider-neutral work: intent, inputs, evidence requirements, tool policy, budgets, expected result types, lifecycle events, ingestion gates, and settlement.
2. **Provider-qualified capability bindings** describe exact runtime mechanics: model/provider identifiers, graph or agent definitions, checkpoint and durability modes, handoffs/subagents, sandbox or computer-use support, tool protocol, tracing integration, and supported intervention/recovery behavior.

A Workflow Implementation binds to one exact runtime profile at compilation/admission time. Runtime selection must not change silently during a run. Unsupported capabilities should fail compilation or select an explicitly accepted fallback; adapters must not emulate guarantees they cannot provide.

Provider identifiers must be qualified (`langgraph_*`, `openai_*`, `anthropic_*`, or an equivalent accepted grammar). Unqualified fields such as `run_id` are prohibited wherever they could refer to both a BellLabs run and a provider runtime run.

## Research-to-ingestion boundary

Agent output is a research result, not canonical knowledge merely because a capable model or framework produced it.

Every runtime must preserve the BellLabs research-to-ingestion sequence:

1. acquire and register sources;
2. preserve citations, source lineage, and relevant content evidence;
3. extract typed claims, entities, relationships, and ingestion candidates;
4. evaluate identity, applicability, contradiction, quality, safety, and uncertainty;
5. produce a reviewable ingestion plan;
6. apply policy, authorization, and human gates where required;
7. write only approved changes through governed BellLabs services;
8. validate the result and settle durable artifacts and provenance.

No agent runtime receives an independent path around graph authority, evidence requirements, idempotency, or review policy. Research supports decision intelligence and is not medical advice.

## Evaluation hypothesis and method

BellLabs will test the hypothesis that model providers optimize their models to work especially well with their own agent tooling. This is plausible, but it is an evaluation question—not an architectural assumption.

Comparisons should use versioned, replayable cases with equivalent:

- research questions and source-access policy;
- starting context and schema context;
- allowed tools and consequential-action boundaries;
- time, token, cost, and iteration budgets;
- typed output contracts and ingestion acceptance criteria;
- evaluator definitions and human-review rubrics;
- model/framework/agent configuration snapshots.

At minimum, evaluations should measure:

| Dimension | Example evidence |
| --- | --- |
| Research quality | coverage, relevance, source diversity, contradiction handling |
| Evidence fidelity | citation correctness, claim-to-source support, provenance completeness |
| Ingestion quality | schema validity, entity resolution, duplicate avoidance, accepted candidate rate |
| Safety and governance | policy violations, unsupported medical claims, gate-bypass attempts |
| Agent capability | planning, tool selection, delegation, context management, convergence |
| Reliability | completion rate, idempotency, resume/recovery, intervention behavior |
| Operations | latency, cost, token use, trace quality, debugging effort |

Results must identify the complete implementation under test: provider, model and version, SDK/framework versions, agent/graph definition digest, prompts, tools, middleware, evaluator versions, runtime/deployment profile, and budget. A model-only label is insufficient.

The objective is not necessarily to crown one permanent winner. BellLabs may select different exact Workflow Implementations for different research, ingestion, or evaluation workloads when evidence supports the choice.

## Phased direction

### Phase 1 — operational LangGraph/Deep Agents path

- Complete the Agent Server foundation and runtime-neutral adapter.
- Run governed biotech research with registered sources, evidence artifacts, and typed outputs.
- Connect research results to ingestion candidate generation and existing BellLabs review/write boundaries.
- Add LangSmith tracing and versioned evaluation datasets.
- Prove lifecycle, budgets, interrupts, cancellation, recovery, and settlement through BellLabs contracts.

### Phase 2 — frozen comparison harness

- Select representative StageGraph and GoalDirected cases.
- Freeze inputs, tool policy, budgets, result schemas, evaluators, and human-review rubrics.
- Establish a replayable baseline from the LangGraph/Deep Agents implementation.

### Phase 3 — OpenAI-native implementation

- Adapt the OpenAI Agents SDK/API path, using Temporal and FastAPI where their mechanics are beneficial.
- Bind it to the same semantic contracts and comparison cases.
- Record native capabilities and limitations without changing the acceptance criteria mid-comparison.

### Phase 4 — Anthropic-native implementation

- Add an Anthropic-native adapter and agent implementation.
- Bind it to the same semantic contracts and comparison cases.
- Record provider-native capabilities and limitations explicitly.

### Phase 5 — evidence-based routing

- Compare results by workload and capability, including quality, safety, reliability, cost, and operational burden.
- Publish approved runtime capability/maturity records.
- Select runtimes only through exact Workflow Implementation bindings and governed deployment policy.

## Guardrails

- One BellLabs run has one authoritative lifecycle, even if multiple runtimes participate in an evaluation or shadow exercise.
- Shadow or comparison runs use distinct identities and cannot acquire active consequential-effect claims.
- Framework checkpoints, provider threads, traces, and memory stores are execution records—not BellLabs domain authority.
- Secrets and plan/account entitlements are deployment configuration; never place them in published definitions, traces, or committed files.
- Provider-specific code stays behind integration/runtime adapters. Pure domain modules do not import LangGraph, Deep Agents, LangSmith, OpenAI, Anthropic, or Temporal SDKs.
- Adding a provider does not authorize a new ingestion path, a second control plane, or relaxed evidence and safety requirements.
- Architecture decisions should prefer measured capability records over framework loyalty or anecdotal demos.

## Decision summary

BellLabs is committing first to making the LangGraph + Deep Agents Agent Server useful for real biotech research and governed ingestion. It is not committing the product to a single agent framework forever.

The durable investment is the BellLabs contract and evaluation layer. LangChain/LangGraph/Deep Agents/LangSmith, OpenAI-native agents with Temporal/FastAPI, and Anthropic-native agents can then compete and collaborate as execution systems while BellLabs retains consistent scientific, ingestion, security, and lifecycle governance.
