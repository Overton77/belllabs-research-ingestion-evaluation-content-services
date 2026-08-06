# Owner amendments for Stages 3–6 — granular capability-authored workflows

Recorded: 2026-08-06  
Status: owner-provided migration-plan amendments; implementation evidence remains required  
Scope: work packages 06–09 and coordinator-facing workflow composition

## 1. Decisions

| ID | Accepted direction | Implementation consequence |
|---|---|---|
| D-17 | The LangGraph/Deep Agents work is a port, migration, and enhancement. New Workflow Implementations are not constrained to reuse the models/providers selected by the OpenAI Agents SDK/Temporal implementation. | Models, prompts, tools, specialist agents, and verifiers are exact versioned implementation choices. Parity compares BellLabs contracts, authority, evidence, typed results, budgets, failures, and owner-approved semantic-quality thresholds rather than provider/model/token/trace equality. |
| D-18 | StageGraph stages must encode granular execution requirements so operators and the coordinator can build heterogeneous workflows from governed capabilities. | Every executable stage/variant receives an immutable `StageCapabilityRequirement`, `StageExecutionBinding`, and `OperationAssemblySpec`; no node hard-codes a model/tool/skill/MCP/subagent surface. |
| D-19 | Scheduling and capability mechanics remain separate. | Stage 4 implements a generic scheduler and operation port. Stage 5/6 adapters plug into that port without changing scheduler topology or lifecycle authority. |
| D-20 | Parallelism is hierarchical and explicit; optimistic execution is a separate governed feature. | Resource envelopes cover stage workers and nested model/tool/MCP/sync-child/async-child/linked-run capacity. Speculation is default-off and limited to explicitly published pure/read-only policies with quarantine and commit barriers. |
| D-21 | Capability compilation must precede its consumers. | Stage 1 performs structural compilation, Stage 5 compiles the stable executable surface, and Stage 6 extends it for providers/async/optional interpreter modes. Stage 6 does not introduce the first compiler after Stage 4/5 already need it. |
| D-22 | The reusable Deep Agents harness serves both StageGraph and GoalDirected. | Stage 5 implements compiler/harness first, proves StageGraph composition second, and ports GoalDirected third. GoalDirected nodes may not embed a private harness construction path. |
| D-23 | End-to-end lineage and heterogeneous composition require direct acceptance evidence. | Stages 3–6 implement one canonical lineage envelope and Stage 6 completes the required `09A` composition proof. |

## 2. Prior accepted decisions preserved

These amendments do not weaken D-01–D-16. In particular:

- BellLabs remains lifecycle, budget, approval, effect, evidence-acceptance, and terminality authority;
- the pure StageGraph and GoalDirected interpreters remain authoritative deterministic mechanics;
- Standard Agent Server remains the primary runtime;
- async subagents remain a distinct required migration track, default-off until the Stage 6 promotion gate passes;
- QuickJS/PTC/dynamic delegation remains optional and disabled unless separately accepted;
- mutable aliases, runtime discovery, installed packages, prompts, models, tools, skills, children, traces, and checkpoints cannot grant authority;
- `biotech-meta` remains read-only absent explicit authorization.

## 3. Application to earlier-stage artifacts

These decisions were recorded after the original Stage 0–2 package drafts and therefore require a targeted compatibility amendment before Stage 3 implementation:

1. inspect the actual Stage 1 definitions, RunPlan compiler, API schemas, persistence refs, migrations, tests, and handoff;
2. version or extend the thin `StageImplementationBinding`/`GraphAssemblySpec` contracts to satisfy `06A` without mutating already-published digests;
3. add structural compilation and complete-stage-coverage tests;
4. assess whether Stage 2 introspection schemas, placeholder graph state/channels, `langgraph.json`, or compatibility manifests require a deliberately versioned update;
5. publish a focused Stage 1/2 amendment handoff with migration/backward-compatibility evidence;
6. only then mark the amended Stage 3 entry criterion satisfied.

Do not rerun or discard accepted earlier work unnecessarily. Preserve valid runtime-neutral contracts and Stage 2 foundation code, add the missing versioned contracts, and record exactly which earlier evidence remains valid.

## 4. Required evidence before these decisions are considered implemented

- schemas and pure compilation tests for every per-stage requirement/binding/assembly;
- coordinator preparation output that predicts the exact effective stage surfaces;
- multiple stages with different models/capabilities running concurrently under one StageGraph;
- actual-overlap, ceiling, backpressure, deadlock, and recovery measurements;
- stable Deep Agent adapter used unchanged by StageGraph and GoalDirected;
- async child StageGraph wait/resume and reconciliation evidence;
- final-result lineage query across operation, agent, tool/MCP, child, evidence, effect, settlement, and trace identities;
- evaluations demonstrating accepted contract and semantic-quality behavior for intentionally changed model/provider choices;
- owner/gate-review disposition in each outgoing stage handoff.
