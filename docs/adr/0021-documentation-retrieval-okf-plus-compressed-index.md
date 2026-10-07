---
type: Decision Record
title: "Agent-facing documentation is an Open Knowledge Format bundle plus a compressed index embedded in AGENTS.md, measured by retrieval evals"
description: "Explanatory documentation lives as Open Knowledge Format concepts (one concept per file, YAML type, title, description and tags) under docs/knowledge/, with the normative spec pack kept in place and given the same…"
tags: [mission-control, adr, decision]
status: proposed
source: interview 2026-10-07 (Q16, Q17); Vercel, "AGENTS.md outperforms skills in our agent evals" (2026)
---

# Agent-facing documentation is an Open Knowledge Format bundle plus a compressed index embedded in AGENTS.md, measured by retrieval evals

Explanatory documentation lives as Open Knowledge Format concepts (one concept per file, YAML type, title, description and tags) under `docs/knowledge/`, with the normative spec pack kept in place and given the same frontmatter. A generated, compressed index of every concept, ADR, glossary and spec file is embedded in `AGENTS.md` between markers, so that the agent always knows where documents are without loading them, and a search skill covers the cases the index does not. We chose passive index plus searchable corpus over skills-only after Vercel's evals showed skills are invoked unreliably, and we keep a retrieval experiment (`experiments/docs_retrieval/`) so the choice is re-measured rather than assumed.
