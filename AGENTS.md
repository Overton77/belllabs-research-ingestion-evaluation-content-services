# Agent instructions

Thin index. Prefer Cursor rules under `.cursor/rules/` over duplicating guidance here.

## Cursor rules (start here)

| Rule | Apply | Use when |
|------|-------|----------|
| [`api-runbook.mdc`](.cursor/rules/api-runbook.mdc) | Globs (Compose/API/server/worker) | Start Docker, hit HTTP, routers, worker |
| [`codebase-organization.mdc`](.cursor/rules/codebase-organization.mdc) | Globs (`app/`, tests, org docs) | Package map, imports, placement |
| [`workflows-domain-contracts.mdc`](.cursor/rules/workflows-domain-contracts.mdc) | Globs (temporal/domain/agents) | Temporal macro runtime + Deep Agents + contracts |
| [`biotech-meta-reference.mdc`](.cursor/rules/biotech-meta-reference.mdc) | **Manual** | Canonical SPECs/ADRs in `../biotech-meta` |
| [`project-organization.mdc`](.cursor/rules/project-organization.mdc) | Always | SPEC/WP reading order, layering hard rules |
| [`tech-stack-authority.mdc`](.cursor/rules/tech-stack-authority.mdc) | Manual/related | Stores, runtime roles, worker pools |
| [`agent-framework-coexistence.mdc`](.cursor/rules/agent-framework-coexistence.mdc) | Always | Framework-neutral contracts; Deep Agents bounded |
| [`wp-bp-010-stagegraph.mdc`](.cursor/rules/wp-bp-010-stagegraph.mdc) / [`wp-bp-020-goal-directed.mdc`](.cursor/rules/wp-bp-020-goal-directed.mdc) | Family globs | Blueprint package ownership |
| [`engineering-sequence.mdc`](.cursor/rules/engineering-sequence.mdc) / [`parallel-blueprint-worktrees.mdc`](.cursor/rules/parallel-blueprint-worktrees.mdc) | As configured | Spec→implement sequence; parallel WP worktrees |

## Non-negotiables (pointers only)

- Application lives in `app/`. Dependency direction: `domain ← application ← api | temporal | integrations`.
- Temporal = sole production macro runtime. Deep Agents = bounded cognition inside `operation.execute`.
- BellLabs API (`app.server:asgi_app`) = governed public facade. Agent Server is not a competing scheduler.
- Specs: `../biotech-meta` (read-only unless the task owns a meta change). WPs cannot amend `SPEC-*`.
- Canonical org doc: `docs/interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`.
- v2 packages: `docs/migrations_instructions/implementation_work_packages_v2/`. Stage 0–8 tree is historical only.

## Cloud / local stack (one-liners)

Full runbook → [`api-runbook.mdc`](.cursor/rules/api-runbook.mdc).

```bash
docker compose up -d
uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000   # make server
uv run python -m app.temporal.worker                              # make worker (separate)
```

Compose readiness: `application-postgres`, `redis`, `temporal-postgres`, `temporal`, `temporal-ui` running; `temporal-schema` / `temporal-create-namespace` exit 0. Do not `docker compose down --volumes` unless asked. Never commit `.env` — use `.env.example` names in Cursor secrets.

```bash
uv run ruff check app tests
uv run mypy app
uv run pytest
```
