---
type: Agent Configuration
title: Domain Docs
description: "How the engineering skills should consume this repo's domain documentation when exploring the codebase."
tags: [mission-control, agents, process]
---
# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`GLOSSARY.md`** at the repo root: the shared language for Mission Control.
- **`docs/adr/`**: read ADRs that touch the area you're about to work in.
- **`docs/knowledge/`**: the Open Knowledge Format bundle that explains how the code realizes the glossary and the decisions; start at `docs/knowledge/index.md`.

If any of these files don't exist, **proceed silently**. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

This is a **single-context** repo:

```
/
├── GLOSSARY.md
├── docs/adr/
│   ├── 0001-....md
│   └── ...
├── docs/knowledge/          ← OKF explanations (not a glossary, not ADRs)
├── src/mission_control/
├── packages/mission-control-db-contract/   ← shares the root glossary
└── integrations/biotech/                   ← shares the root glossary
```

The accepted specification lives in the sibling `../mission-control-general/general-mission-control/` and is normative. The glossary and ADRs here restate its decisions in the shared language; when they disagree, fix the one that is wrong rather than keeping both.

### Path to a split

Split into multiple contexts only when a subsystem develops language the kernel does not share (candidates: the SQL component package, the Biotech integration, a future Knowledge Services package). Then add `GLOSSARY-MAP.md` at the root pointing to a `GLOSSARY.md` and `docs/adr/` inside that subsystem, and leave system-wide decisions in the root `docs/adr/`. Subsystem `AGENTS.md` files are navigation, not glossaries; they may exist without a split.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `GLOSSARY.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (authoritative state separate from advisory memory), but worth reopening because…_
