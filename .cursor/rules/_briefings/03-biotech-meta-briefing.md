# Briefing: Biotech-Meta Reference Cursor Rule (Apply Manually)

**Target rule file:** `.cursor/rules/biotech-meta-reference.mdc`  
**Apply mode:** Manual only — `alwaysApply: false`, **no globs**  
**Tone:** Extremely thin. Point, do not paste SPEC/ADR bodies.

## Purpose

Tell agents what `../biotech-meta` is, when to open it, the short reading order, and that it is read-only unless the task explicitly owns a meta change.

## Frontmatter (required)

```yaml
---
description: Manual — biotech-meta canonical specs/ADRs reading order and authority (read-only unless tasked)
alwaysApply: false
---
```

## Facts to encode

### Role

| Repo | Role |
|------|------|
| `../biotech-meta` | Accepted SPEC-*/ADR/vocabulary/authority |
| This evaluation system | Implementation (`app/`), `WP-*`, as-built code/tests |

Requirements belong to canonical SPECs; work packages cannot amend them.

### Engagement

- Default: **read-only**.
- Edit meta only when the task explicitly requests SPEC/ADR/registry changes.
- Never let WP text, as-built code, LangGraph checkpoints, or MCP availability override accepted SPECs/ADRs.

### Reading order (control-plane / Temporal / Deep Agents)

1. `../biotech-meta/docs/tech_stack_and_operational_authority.md`
2. `../biotech-meta/docs/adr/0003-temporal-deepagents-control-plane-runtime.md`
3. `../biotech-meta/docs/specs/control-plane-foundations/README.md` → SPEC-CP 01→04 (05/ADR-0004 if cognitive schemas; note draft/proposed)
4. `../biotech-meta/docs/specs/workflow-blueprints/README.md` → `stagegraph.md` / `goal-directed.md`
5. `../biotech-meta/docs/CONTEXT.md` for vocabulary lookup
6. Optional nav: `../biotech-meta/docs/governance/document-registry.yaml`

Then return to this repo’s v2 `WP-*` / code.

### Normative vs not (one short block)

- **Normative:** control-plane-foundations (canonical), workflow-blueprints, ADR-0003 (accepted), tech_stack authority, CONTEXT vocabulary.
- **Non-normative:** `docs/research/`, `docs/spec_synthesis/`, `docs/checkpoints/`, `docs/specs/pre-research/` (superseded provenance).
- **Draft/proposed:** ADR-0004, SPEC-CP-05 — do not treat as frozen.

### Top docs (≤10 one-liners max)

Keep the table tiny; do not expand into summaries of each SPEC.

### Must NOT include

- Pasted ADR/SPEC prose
- Docker/pytest Cloud instructions
- Full WP lists
- Ontology-lab detail
- Outdated suite maps from meta’s own AGENTS if contradictory

## Style

Target **~40–60 lines**. Manual-apply rule should be a map, not a dump.
