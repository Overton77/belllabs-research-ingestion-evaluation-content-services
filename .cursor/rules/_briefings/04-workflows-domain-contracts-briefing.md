# Briefing: Workflows + Domain Contracts Deep-Dive Cursor Rule

**Target rule file:** `.cursor/rules/workflows-domain-contracts.mdc`  
**Apply mode:** File-specific globs, `alwaysApply: false`  
**Thesis:** Temporal = macro execution runtime. Deep Agents (LangChain) = cognitive intelligence inside Temporal operation activities. Domain contracts + application services = semantic authority.

## Relationship to existing rules

- Keep coexistence philosophy in `agent-framework-coexistence.mdc` (alwaysApply).
- Keep store/worker-pool authority in `tech-stack-authority.mdc`.
- Keep WP-BP-010/020 package ownership in those rules.
- This rule is the **path inventory + invocation chain + contract map + LLM agent structural roles**.
- Point to biotech-meta SPECs/ADRs; do not paste full REQ tables.

## Frontmatter (required)

```yaml
---
description: Temporal macro runtime, Deep Agents cognition seam, and domain contract map
globs: app/temporal/**/*.py,app/integrations/agents/**/*.py,app/domain/control_plane/**/*.py,app/domain/orchestration/**/*.py,app/domain/operation_execution/**/*.py,app/domain/run_control/**/*.py,app/domain/coordinator/**/*.py,app/application/orchestration/**/*.py,app/application/control_plane/**/*.py,app/application/run_control/**/*.py,app/application/operations/**/*.py,app/application/async_subagents/**/*.py,app/application/coordinator/**/*.py,.cursor/lang_url_maps/**
alwaysApply: false
---
```

## Layering diagram (encode)

```text
Admit/compile (Postgres + ControlPlane)
  → BellLabsRunWorkflow (Temporal root)
    → StageGraphWorkflow | GoalDirectedWorkflow
      → OperationWorkflow
        → activity operation.execute
          → OperationExecutionService
            → DeepAgentRuntimeAdapter.create_deep_agent()  # sole site
              → LangGraph checkpoint / LangSmith (evidence, not authority)
```

## Temporal inventory (encode compactly)

Workflows (`app/temporal/registration/workflows.py` + `workflows/`):

| Name | Path | Role |
|------|------|------|
| `belllabs.run.v1` | `workflows/belllabs_run.py` | Root per admitted run |
| `belllabs.stagegraph` | `workflows/stagegraph.py` | StageGraph family mechanics |
| `belllabs.goal-directed` | `workflows/goal_directed.py` | Executor/verifier loop |
| `belllabs.operation.v2` | `workflows/operation.py` | One semantic attempt shell |
| linked-run / observer | `linked_run_workflow.py` | Composition |
| generic-artifact | `artifact_workflow.py` | Artifact promotion |

Key activities:

- `operation.execute` ← **only cognitive entry** (`operation_activities.py`)
- StageGraph admit/decide/apply (`orchestration_activities.py`) — no LLM
- GoalDirected prepare/reconcile (`activities/goal_directed.py`) — cognition via OperationWorkflow
- schema_grounding.*, linked_run.*, artifact.promote, control-plane.apply

Task queues (5): `{base}-coordinator-family`, `-agent-cognitive`, `-ingestion-io`, `-sandbox-external-job`, `-verification-reconciliation`

Worker: `app/temporal/worker.py`

## Deep Agents seam

- Sole `create_deep_agent`: `app/integrations/agents/deep_agents/adapter.py`
- Materializer: `materializer.py`; async subagents: `async_subagents.py`
- Binding: ERC → `FlattenedDeepAgentBinding` / `DeepAgentExecutionBinding`
- Async delegation classifier: `domain/operation_execution/delegation.py` → subordinate | operation | linked_run
- Framework URL maps: `.cursor/lang_url_maps/deepagents_*.txt` (+ langchain/langgraph/langsmith)

## Domain contracts map

| Package | Owns |
|---------|------|
| `domain/control_plane/` | Blueprints, AgentProfile, OperationAssembly, DeepAgentPlacement, ERC |
| `domain/orchestration/` | BellLabsRunInput, StageGraph/GoalDirected interpreters (pure) |
| `domain/operation_execution/` | DeepAgent profiles/bindings, RuntimeInvocation/Result, journal/settlement |
| `domain/run_control/` | Admission, lifecycle reducer, budgets, terminality |
| `domain/coordinator/` | Capability/policy; web-research grants |

Interpreters **propose**; run-control **terminalizes**. Temporal never owns readiness/convergence semantics.

## StageGraph vs GoalDirected (brief)

- StageGraph: dependency DAG frontier → ops → join → complete proposal
- GoalDirected: revision → executor op → verifier op → converge/continue
- Specs: `../biotech-meta/docs/specs/workflow-blueprints/{stagegraph,goal-directed}.md`
- Sibling rules: `wp-bp-010-stagegraph.mdc`, `wp-bp-020-goal-directed.mdc`

## LLM / agent structural roles (not free-form personas)

1. `AgentProfileDefinition` — catalog identity (prompts/skills/MCP/tools/ceilings)
2. `AgentDefinition` — operation-level name/description/instructions/tools
3. `DeepAgentProfile` / `DeepAgentExecutionBinding` — exact frozen assembly
4. `SyncSubagentProfile` / `AsyncSubagentContract`
5. GoalDirected roles only: `executor` | `verifier`
6. Descriptions/prompt text are not capability or budget authority

## Pitfalls (must include)

1. No LLM calls in family workflow code — activities only.
2. Interpreter proposal ≠ lifecycle terminal.
3. Agent Server / LangGraph ≠ macro scheduler.
4. No silent placement fallback; digest drift → fail closed.
5. Async subagent ≠ automatic new Workflow Run without classifier.
6. Do not reintroduce OpenAI Agents SDK (WP-CP-040).
7. Temporal history = replay mechanics; product events = BellLabs ledger.
8. Secrets as refs/handles only — never in checkpoints/run-control payloads.

## Meta pointers (IDs only)

- ADR-0003, SPEC-CP-01..04 (+05/ADR-0004 if cognitive schemas — proposed)
- workflow-blueprints README + stagegraph + goal-directed
- Manual rule: `biotech-meta-reference.mdc`

## Style

Allow up to ~180 lines (deep dive). Prefer diagrams, tables, path lists. No pasted SPEC requirement matrices.
