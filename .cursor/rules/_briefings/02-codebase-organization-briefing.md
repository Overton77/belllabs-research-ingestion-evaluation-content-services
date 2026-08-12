# Briefing: Codebase Organization Cursor Rule

**Target rule file:** `.cursor/rules/codebase-organization.mdc`  
**Apply mode:** File-specific globs, `alwaysApply: false`  
**Relationship to existing `project-organization.mdc`:** That rule stays `alwaysApply` for SPEC/WP reading order and high-level layering. This new rule is the compact **filesystem + import + function map** agents need when editing `app/`. Do not re-paste the full WP reading-order boilerplate.

## Purpose

When editing application code: know where things live, what each top package does, and the inward-only dependency direction. Prefer extend-in-place; create projected paths only when a WP authorizes them.

## Frontmatter (required)

```yaml
---
description: Compact app/ package map, ownership, and inward dependency direction
globs: app/**/*.py,tests/**/*.py,docs/interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md,docs/migrations_instructions/implementation_work_packages_v2/SUPPLEMENT_CODEBASE_ORGANIZATION.md
alwaysApply: false
---
```

## Dependency direction (encode as hard rule)

```text
app/domain  ←  app/application  ←  app/api | app/temporal | app/integrations
```

- Domain: framework-neutral meaning; no FastAPI/Temporal/provider SDKs as authority.
- Application: use cases + ports; persists domain contracts; does not redefine meaning.
- API: transport DTOs + translation only.
- Temporal: sole macro runtime mechanics; activities call application.
- Integrations: concrete adapters (DB, Temporal client, Deep Agents, providers).
- `app/server.py`: sole composition root / BellLabs API facade.
- `app/agent_server/`, `app/mcp/`: bounded cognition/transport — not second app roots or macro schedulers.
- `app/models/`: persistence shapes, not domain authority.
- `app/experiments/`: non-production; promote before production import.

## Package map (encode as compact table, 1-line each)

| Path | Purpose |
|------|---------|
| `app/domain/control_plane/` | Immutable defs, blueprints, ERC compilation contracts |
| `app/domain/run_control/` | Admitted run lifecycle, budgets, transitions, events |
| `app/domain/orchestration/` | Pure StageGraph/GoalDirected interpreters |
| `app/domain/operation_execution/` | Journal/effect/settlement/workspace/delegation |
| `app/domain/graph_runtime/` | Assembly/binding facts/receipts (not journal lifecycle) |
| `app/domain/coordinator/` | Coordinator semantic contracts/policy |
| `app/domain/schema_*` | Schema catalog/context/grounding meaning |
| `app/application/*` | Use-case services, ports, admission, orchestration I/O |
| `app/api/` | Public HTTP routers |
| `app/temporal/workflows/` | Durable family/operation/run mechanics |
| `app/temporal/activities/` | Idempotent calls into application |
| `app/integrations/agents/deep_agents/` | Sole `create_deep_agent` adapter seam |
| `app/migrations/` | Application PostgreSQL only |

## Placement test (encode)

1. Pure meaning / interpreter? → `domain`
2. Use-case + persistence coordination? → `application`
3. HTTP DTO? → `api`
4. Durable schedule/retry/child workflow? → `temporal`
5. Concrete SDK/DB client? → `integrations`
6. Bounded LangGraph op graph / qual? → `agent_server` (never family scheduler)

## Forbidden (short list)

- Domain importing api/temporal/integrations.
- API DTOs reused as domain authority.
- Temporal workflows under agent_server or integrations.
- Merging app Postgres with Temporal Postgres.
- Inventing undeclared task queues.
- Bulk relocating packages without active WP.

## Canonical docs (point, don't paste)

- `docs/interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`
- `docs/migrations_instructions/implementation_work_packages_v2/SUPPLEMENT_CODEBASE_ORGANIZATION.md`
- Active WP + `IMPLEMENTATION_READINESS.md` for exact filenames

## Defer

- Store roles / five worker pools → `tech-stack-authority.mdc`
- Framework coexistence / no OpenAI Agents SDK → `agent-framework-coexistence.mdc`
- SPEC reading order → `project-organization.mdc`
- Temporal×DeepAgents deep dive → `workflows-domain-contracts.mdc` (sibling rule)

## Style

Keep under ~100 lines. Tables + bullets. As-built may be flatter than Stage 8 target — follow target direction; don't invent mass moves.
