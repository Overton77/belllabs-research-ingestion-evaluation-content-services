# Cursor rules proposal — biotech-research-ingestion-evaluation-system

Date: 2026-07-20  
Status: **proposal only** — no `.cursor/rules/*.mdc` files created yet  
Repo remote (proof): `git@github-legacy:Overton77/belllabs-research-ingestion-evaluation-content-services.git`

## Intent

Three compact, high-powered project rules for an early backend (~5–10% built). They should steer agents without duplicating full specs, playbooks, or issue bodies.

**Install location (when approved):**  
`biotech-research-ingestion-evaluation-system/.cursor/rules/`  
(not workspace-root; not `app/.cursor/` — that tree already has a local preference rule)

---

## Proposed rules (3)

### 1. `engineering-sequence.mdc` — Application engineering sequence

| Field | Proposal |
|--------|----------|
| **Purpose** | Force the interview → checkpoint → spec → ticket → implement order. Stop agents from inventing domain behavior during coding, or skipping tickets when implementing. |
| **Scope** | `alwaysApply: true` |
| **Globs** | none |

**What it would enforce (compact):**

```text
grill-with-docs + domain-modeling
  -> checkpoints / summaries in biotech-meta
  -> optional handoff (.scratch/handoffs/ or biotech-meta handoffs)
  -> to-specs  →  biotech-meta/docs/specs/...
  -> to-tickets → GitHub issues + markdown mirrors under .scratch/.../issues/
  -> implement (+ tests) against a specific issue/spec seam
```

**Hard rules for agents:**

- Do not implement from interview notes alone. Require an accepted spec path and a ticket (GitHub and/or `.scratch` issue).
- Specs live in `biotech-meta/docs/specs/` (local specs ≠ GitHub issues). Ticket publication is a separate step.
- Respect the pre-research DAG (`F* → C* → P*/R*`) from `biotech-meta/docs/specs/pre-research/README.md`. Do not jump ahead of prerequisites.
- Optional handoff: write/read `.scratch/handoffs/` when crossing sessions mid-slice.
- Branch/PR lifecycle: follow `.scratch/ISSUE_BRANCH_PR_LIFECYCLE.md` (one branch/PR per issue unless deliberately batched).
- Skills map (operator-owned, not reimplemented in the rule): grill-with-docs, domain-modeling, to-specs, to-tickets, implement.

**Keep out of this rule:** full playbook text, checkpoint history, TDD seam catalogs.

**Source of truth to cite, not copy:**  
`biotech-meta/docs/checkpoints/internal_implementation_docs/2026-07-16-belllabs-engineering-delivery-playbook-special-checkpoint.md`

---

### 2. `project-organization.mdc` — Project layout and layering

| Field | Proposal |
|--------|----------|
| **Purpose** | Tell agents where code, specs, issues, sandbox, and infra live — and which layer owns what. |
| **Scope** | `alwaysApply: true` |
| **Globs** | none (or later: `app/**`, `tests/**`, `infra/**` if always-apply feels noisy) |

**Map it would encode:**

| Path | Role |
|------|------|
| `app/domain/` | Pure domain contracts / types / invariants (no I/O) |
| `app/application/` | Use-cases, orchestration ports, repositories interfaces + adapters used by control plane |
| `app/api/` | FastAPI HTTP surface |
| `app/models/` | Persistence-facing models (Beanie/docs, shared shapes) |
| `app/integrations/` | External system adapters (Neo4j, S3, Agents SDK, etc.) |
| `app/temporal/` | Workflows, activities, workers, probes |
| `app/migrations/` | Application Postgres schema migrations |
| `app/middleware/` | ASGI/HTTP middleware |
| `tests/` | Unit + integration tests |
| `infra/` | Deploy / compose-adjacent infra (not domain) |
| `sandbox-work/` | Disposable sandbox experiments — not product authority |
| `scripts/` | Operator/dev scripts |
| `.scratch/` | Working issues, handoffs, lifecycle notes — **not** durable product docs |
| `../biotech-meta/docs/specs/` | Accepted specifications |
| `../biotech-meta/docs/checkpoints/` | Interview/architecture checkpoints |

**Hard rules:**

- Prefer extending existing packages over new top-level folders.
- Domain authority stays in domain/application; Temporal/Docker/Agents SDK are mechanics, not truth.
- Do not treat `.scratch/` or `sandbox-work/` as production contracts.
- Specs and vocabulary: prefer `biotech-meta` over inventing names.

**Keep out:** per-file naming nits, formatter settings (ruff/mypy live in tooling).

---

### 3. `tech-stack-authority.mdc` — Stack roles and data authority

| Field | Proposal |
|--------|----------|
| **Purpose** | Freeze *current* stack roles and authority boundaries so agents put state in the right store. Mark as evolving. |
| **Scope** | `alwaysApply: true` |
| **Globs** | none |

**Authority table (from accepted pre-research README + project README):**

| System | Used for | Authority notes |
|--------|----------|-----------------|
| **FastAPI** | HTTP API, startup migrations hook, docs | Entry surface only |
| **uv / Python** | Project env, `uv run`, lockfile | Standard invoke path |
| **Postgres (application)** | Run lifecycle, commands, transitions, budgets, outbox, cursors, authz projections, linked-run rollups | **Authoritative** for control-plane transactional state (local port `55432`) |
| **Postgres (Temporal)** | Temporal persistence only | Separate DB — **not** domain authority |
| **Supabase** | Hosted Postgres path + **vector** for memory-related retrieval | Use for memory/vector features when wired; do not confuse with Temporal DB |
| **MongoDB + Beanie** | Immutable definitions, Effective Run Config, operation bindings, workflow-shaped docs, findings/plans/evals metadata | **Authoritative** for those document payloads (`AsyncMongoClient`, DB `belllabsbiotech`) |
| **Temporal** | Durable workflows/activities, retries, sandbox-bound agent work | Execution mechanics only |
| **Neo4j** | Approved canonical graph knowledge | Graph authority; preflight/refinement treat as read-only unless a later spec says otherwise |
| **Object storage (S3/aioboto3)** | Large immutable payloads, artifacts, reports, snapshots | Blob authority |
| **OpenAI Agents SDK** | Governed agent runs, tools, Docker and/or Temporal sandboxes, Temporal durable-workflow plugin | Bound through ports/bindings; never invents control-plane state by existing |

**Hard rules:**

- Temporal must not become the system of record for run lifecycle or budgets.
- Prompts, skills, MCP tools, plugins, memory packs, and model output are never authority merely because they exist (catalog + exact bindings required where specs say so).
- When unsure where state lives, check the matching `F*`/`C*` spec before coding.
- This rule is a **living snapshot** — update when stack roles change; do not invent new stores in code without a spec/ticket.

**Keep out:** connection strings, env var dumps, full Compose topology (point at `README.md` / `.env.example`).

---

## Why not more rules yet

Deferred until patterns stabilize:

| Deferred | Why wait |
|----------|----------|
| Python/FastAPI style | Early; ruff/mypy already constrain |
| Temporal workflow patterns | Codify after F3/F4 settle |
| Testing conventions | After TDD seams are repeated 2–3 times |
| GitHub issue body templates | Covered by to-tickets skill + `.scratch` mirrors |
| Workspace-level AGENTS.md | Suite map already at Biotech root |

Three always-apply rules stay under ~40–50 lines each if we cite paths instead of pasting specs.

---

## Open decisions (need your call)

1. **Always apply all three?** Recommendation: yes for this repo — early stage, agents need orientation every session. Alternative: only #1 always; #2/#3 via globs `app/**`, `tests/**`, `infra/**`.
2. **Supabase wording:** keep “Postgres + vector memory” as above, or split Supabase into its own row only after C3 memory work lands?
3. **Cross-repo paths:** rules will reference `../biotech-meta/...` from this repo. Confirm that is acceptable (multi-root workspace), vs. “read biotech-meta via workspace” without relative paths.
4. **Issue mirrors:** should the sequence rule require agents to update `.scratch/.../issues/` when GitHub issues change, or treat scratch as write-once from to-tickets?

---

## Next step after approval

1. Create the three `.mdc` files under `.cursor/rules/`.
2. Optionally add a one-line pointer in project `README.md` (“see `.cursor/rules/`”) — only if you want it.
3. Revisit stack rule when C3 (memory) / C4 (capability catalogs) land.
