---
type: Decision Record
title: "A manifest may declare several missions and their links; a Mission Chain compiles to independent missions with supplies and depends_on relationships, released by the application outbox, and state moves across links as a Context Packet"
description: "Workflow composition is a chain of whole missions, each with its own run, revision, budget and acceptance; a chain link names the supplying mission's projected outputs and the acceptance condition; when that condition is recorded the chain reducer admits the next run in the same transaction and the relay starts its Temporal root; no wrapper workflow, no shared mutable state."
tags: [mission-control, adr, decision, composition]
status: accepted
source: fast-track interview 2026-10-07 (requirement 2c); workflow-types/06 (relationships, supplies, child mission invocation); ADR-0004; docs/specs/fast-track-2026-10/research/temporal-lifecycle.md (chain via outbox with USE_EXISTING); docs/specs/fast-track-2026-10/research/codebase-map.md (LinkedRunService has no production caller)
---

# A manifest may declare several missions and their links; a Mission Chain compiles to independent missions with supplies and depends_on relationships, released by the application outbox, and state moves across links as a Context Packet

The owner wants several Stage Graph or Goal Loop workflows composed into one authored unit with mission state carried forward. The specification already offers two shapes: a child mission invocation node inside a program, and mission relationships between independent missions. We choose relationships as the default composition: a manifest's `missions:` list compiles into N missions plus `mission_relationship` rows of kind `supplies` and `depends_on`, each link carrying a typed output-to-input binding and an acceptance condition (`goal_accepted` by default, `mission_accepted` or `execution_complete` by declaration). The chain reducer runs on the ledger: when the condition's mission event commits, the next mission's run is admitted in the same transaction with a Context Packet built from the supplying mission's accepted outputs, final checkpoint reference and journal digest, and the outbox relay starts its root with a workflow id derived from the run and `USE_EXISTING` conflict policy so retries never duplicate. Child mission invocation remains available for a node that must wait inside a program. We rejected a wrapper Temporal workflow spanning missions because it would create a second scheduler of record for budgets and acceptance, and rejected shared workspaces because the specification forbids shared mutable state across activations.

## Consequences

- `missionctl mission submit` on a chain returns one chain id and N mission ids; `missionctl chain inspect` shows link states.
- Cancelling a mission in a chain applies the link's `on_upstream_cancel` policy (`cancel_downstream` default, `detach`).
- The existing `LinkedRunService` is reused for `await`-style links inside programs; chain links do not use it.
