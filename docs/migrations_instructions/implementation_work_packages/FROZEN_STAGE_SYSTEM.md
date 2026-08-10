# Frozen Stage 0–8 package system

Status: superseded planning history  
Frozen: 2026-08-09  
Last reached frontier: Stage 3 planning; `05A = REWORK_REQUIRED`, `S3-00 = NOT_STARTED`  
Active agents at freeze: none, by owner confirmation

## Decision

The numbered Stage 0–8 packages in this directory no longer authorize implementation, gate transitions, or architecture changes. They remain in place because they contain valuable decision history, migration analysis, test/evidence expectations, and prior implementation observations.

The replacement authority chain is:

```text
architecture proposal and pre-research sources
  -> ADR-0003
  -> canonical SPEC and REQ owners in biotech-meta
  -> WP packages in implementation_work_packages_v2
  -> code and migrations
  -> qualification/evidence
```

## Why the system was frozen

- Its stage organization accumulated multiple framework eras and mixed Agent Server, OpenAI Agents SDK, LangGraph, Deep Agents, and Temporal assumptions.
- The accepted current target uses Temporal as the sole macro runtime and Deep Agents as the primary bounded cognitive framework.
- Foundation contracts, StageGraph semantics, GoalDirected semantics, and capability materialization need separate owners and atomic requirement lineage.
- Stage numbers combined architecture, implementation order, gate state, and evidence packaging in documents too large for one reliable implementation context.

## Preservation rules

- Do not delete these files until their unique requirements, decisions, and evidence references are inventoried and extracted.
- Do not cite them as normative authority from new code or work packages.
- Historical links may remain for provenance but must be labeled historical.
- The old Stage 3 execution ledger is frozen state evidence and cannot be advanced.
- Any still-valid requirement must receive a canonical `REQ-*` owner before implementation.

## New authority

- `biotech-meta/docs/adr/0003-temporal-deepagents-control-plane-runtime.md`
- `biotech-meta/docs/specs/control-plane-foundations/`
- `biotech-meta/docs/specs/workflow-blueprints/`
- `docs/migrations_instructions/implementation_work_packages_v2/`

