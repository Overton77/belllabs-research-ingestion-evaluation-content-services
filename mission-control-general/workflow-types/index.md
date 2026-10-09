# General Mission Control workflow specifications

Canonical behavior annex to [SPECIFICATION.md](../general-mission-control/SPECIFICATION.md), consolidated 2026-10-02. All four workflow systems and the cross-cutting contracts below will be implemented. Stage Graph and Goal Loop lead delivery; Parallel Swarm and Evaluator Optimizer remain required scope. [IMPLEMENTATION.md](../general-mission-control/IMPLEMENTATION.md) sequences qualification, not omission.

The runtime is Python + Temporal, with Deep Agents first, required Agent Server async execution in both applications, then Cursor SDK Cloud and bounded frontier-provider execution. PostgreSQL is the mission ledger. This suite defines behavior; [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md) binds it to the qualified harness and public interfaces. Old milestone labels record interview provenance, not a second delivery plan.

## Documents

- [`00-EXECUTION_PROGRAM_MODEL.md`](./00-EXECUTION_PROGRAM_MODEL.md) — taxonomy, composition, nesting, lifecycle scopes, and shared laws
- [`01-STAGE_GRAPH.md`](./01-STAGE_GRAPH.md) — dependency-directed composition system
- [`02-GOAL_LOOP.md`](./02-GOAL_LOOP.md) — goal-directed adaptive loop
- [`03-PARALLEL_SWARM.md`](./03-PARALLEL_SWARM.md) — bounded parallel exploration and convergence
- [`04-EVALUATOR_OPTIMIZER.md`](./04-EVALUATOR_OPTIMIZER.md) — producer/evaluator improvement system
- [`05-EXECUTORS_AND_DURABLE_CONTROLS.md`](./05-EXECUTORS_AND_DURABLE_CONTROLS.md) — execution identity, Agent and Deterministic Executors, Event Wait, Timer, Human Gate, Proof Gate
- [`06-MISSION_INVOCATION.md`](./06-MISSION_INVOCATION.md) — Mission Invocation modes, Child Mission Invocation node, Portals, Spawn Grants, Mission Graph relationships
- [`07-MISSION_REVISION_AND_RUNTIME_EVOLUTION.md`](./07-MISSION_REVISION_AND_RUNTIME_EVOLUTION.md) — editing, immutable Revisions, and running-Mission evolution
- [`08-CONTINUATION_COMPACTION_AND_TRANSFER.md`](./08-CONTINUATION_COMPACTION_AND_TRANSFER.md) — fresh-session continuation through validated checkpoints
- [`09-EVENTS_COMMANDS_AND_STREAMS.md`](./09-EVENTS_COMMANDS_AND_STREAMS.md) — Mission Event envelope and vocabulary, Native Event Store, delivery guarantees, stream contract, Commands, delivery semantics, authority scopes

## Contract ownership

- This suite owns composition, lifecycle, acceptance, invocation, revision and continuation semantics.
- The canonical specification owns general architecture and application boundaries; DATABASE owns tables and transactional protocols.
- Runtime adapters report qualified controls rather than inheriting vendor promises from historical fact sheets.
- The full intervention vocabulary includes semantic requests such as fork/revision. Public routing may use dedicated typed operations rather than the run-command endpoint; unsupported operations reject before effects.
- Existing code/tables are reuse inputs, not permission to weaken these requirements. Preserve unrelated app schemas; no legacy engine compatibility is required.

Each implementation release publishes a behavior availability matrix and conformance evidence. Closed historical interviews are not claims of implemented behavior. No external glossary, obsolete root architecture or M0–M8 plan is needed as architecture authority.
