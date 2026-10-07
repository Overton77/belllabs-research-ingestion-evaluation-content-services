# Knowledge update log

## 2026-10-07

- Expanded the bundle from 8 to 22 concepts. Workflow and authoring:
  [workflow-systems](workflow-systems.md), [parallel-swarm](parallel-swarm.md),
  [evaluator-optimizer](evaluator-optimizer.md), [revisions](revisions.md),
  [durable-controls](durable-controls.md), [completion](completion.md),
  [authoring](authoring.md). Platform: [events-and-commands](events-and-commands.md),
  [interfaces](interfaces.md), [budgets-and-usage](budgets-and-usage.md),
  [lanes-and-harness](lanes-and-harness.md),
  [context-and-continuation](context-and-continuation.md),
  [knowledge-services](knowledge-services.md),
  [release-and-qualification](release-and-qualification.md). Each separates
  implemented behavior (cited code) from specified-only behavior (cited spec).
- Aligned the original concepts with the new shared language: Compiled Program for
  the code name ERC, GoalDirected named as the code family implementing part of the
  Goal Loop, Knowledge Services spelling, and the Temporal placement wording.
- Added `docs/tools/validate_okf.py` (repo-aware links, required title and
  description, Citations heading, index coverage), `okf_search.py`,
  `agents_docs_index.py` and `okf_frontmatter.py`. The compressed index in
  `AGENTS.md` is generated from this bundle, the ADRs, the glossary and the spec pack.
- Recorded 24 spec-versus-code conflicts found while writing, as a local draft for
  the tracker (`.scratch/spec-code-conflicts/spec.md`).

## 2026-10-03

- Created [architecture](architecture.md) and application-logic navigation for the
  general src/mission_control package organization.
- Added [lifecycle](lifecycle.md), [execution](execution.md), [recovery](recovery.md),
  [persistence](persistence.md), [capabilities](capabilities.md),
  [operations](operations.md) and [qualification](qualification.md).
- Recorded the production common-schema release gate separately from local proof.
- Rewrote [persistence](persistence.md) for the common `mission_control` component:
  three namespaces, transaction-local `mc.*` scope with forced RLS, `mission-db`
  installation and the retired transitional schema. Updated
  [qualification](qualification.md) and [operations](operations.md) with the
  two-disposable qualification and the blocked live gates. No live project was
  installed.
- Owner-approved live installation of release `mission_control` 1.0.0 into both Supabase
  projects (Biotech first, then Blue Ocean); both verified with identical fingerprints and
  clean protected-object comparisons. See [qualification](qualification.md) and
  `docs/qualification/two-project/comparison-20261003-live-r1.json`.
