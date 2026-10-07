---
name: mission-control-docs
description: Look up Mission Control knowledge before asserting it. Use when a task names missions, runs, revisions, activations, workflow systems (Stage Graph, Goal Loop, Parallel Swarm, Evaluator Optimizer), harness or lanes, capabilities or the catalog, Agent Server, mission-db, Temporal, outbox, effects, checkpoints, forks, or any term you cannot define from GLOSSARY.md.
---

# Mission Control docs

Retrieval-led, not pretraining-led: Mission Control vocabulary and contracts are project-specific and recent.

1. Search the corpus: `python docs/tools/okf_search.py "<two to five terms>" --limit 6`. Glossary terms, ADRs, OKF concepts and the sibling spec pack are all ranked together. Narrow with `--type "Decision Record"` or `--type Concept` when the hit list is noisy.
2. Open the top hit before answering. A glossary hit (`GLOSSARY.md#Term`) gives the canonical word and its Avoid list; an ADR gives the decision and why; a concept under `docs/knowledge/` points at the code and tests; the spec pack is normative.
3. Answer in glossary terms and cite the path you read.

Precedence when documents disagree: spec pack, then `docs/adr/`, then `docs/knowledge/`, then `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`. Report the disagreement instead of picking silently.

The compressed index in `AGENTS.md` lists every document by directory; use it to jump straight to a file when you already know which one you need.
