---
type: Concept
title: Mission chains
description: How a manifest with missions and links compiles into independent missions joined by typed links, how the chain reducer releases the next mission through the outbox in the same transaction as the event that satisfies the link, what state transfers, the production relay and cross-provider chain evidence, and what still keeps Mission 2 from starting as authored.
tags: [mission-control, chains, mission-chain, outbox, reducer, implementation]
---

# Mission chains

A [Mission Chain](../../GLOSSARY.md) is an ordered set of missions authored in one manifest
and linked by [Chain Links](../../GLOSSARY.md). Each member is a whole mission with its own
revision, run, budget and acceptance; nothing is shared except what a link transfers.
Compile and submit belong to [authoring](authoring.md); state moves as a Context Packet
([context and continuation](context-and-continuation.md)). Decision: ADR-0029.

## Implemented

**Contracts and compile.** `domain/composition/chain.py` defines `mc.chain.v1` and
`mc.chain_link.v1` (also exported as JSON Schema,
`contracts/schemas/mc.chain.v1.json` and `mc.chain_link.v1.json`, and reachable through
`missionctl mission schema --contract`). A link has kind `supplies` or `depends_on`, a typed
output binding, a `ReleaseCondition` (`goal_accepted{goal_key}`, `mission_accepted` or
`execution_complete`) and `on_upstream_cancel` (`cancel_downstream` or `detach`). `compile_chain`
validates the link graph and bindings against the parsed manifest and returns the
topological order and blockers with JSON pointers; the catalog-free
`application/chains/service.py::ManifestStructureService` composes it with inheritance.

**Tables.** Migration 0028 (`0028_mission_chains.sql`) adds `mission_chain`, `chain_link`,
`chain_member_admission` (the frozen admission request of each later member) and
`authoring_provenance`, tenant scoped with forced row-level security. Link states are
forward-only (`armed` to `released`, `blocked`, `detached` or `cancelled`; `released` to
`detached`), enforced by a trigger that mirrors `LINK_TRANSITIONS`.

**Submit.** A chain manifest submits in one application transaction: every member's revision,
the `mission_chain` and `chain_link` rows, the first member's admitted run and the frozen
admissions of the later members. Nothing starts
(`adapters/postgres/chains/store.py::insert_chain`,
`adapters/postgres/control_plane/manifest_submission.py`).

**Release through the outbox.** `adapters/postgres/chains/store.py::ChainReleaseHook` is the
post-append hook of `canonical.append_events`. On the same connection, before the ledger
commit commits, it loads every chain the committing mission belongs to, runs the pure
`application/chains/reducer.py::ChainReducer` over chain rows and current run facts, and
applies the transition: link and chain state (compare-and-swap on version), the consumer's
admission from its frozen request (receipt, run, budget, effect ledger, events), the sealed
chain-link Context Packet, `mission_relationship` rows, the `mc.chain.start_run` and
`mc.chain.cancel_run` outbox intents, and the events `chain_link.released`,
`chain_link.blocked`, `chain_link.detached`, `chain_link.cancelled` and `chain.completed` in
every member's stream. Release conditions are evaluated on the supplier run's state after
the commit, so a replayed commit reaches the same decision. A `supplies` link additionally
waits for accepted output evidence, so a consumer never starts from provisional outputs; a
terminal supplier that does not satisfy a link blocks it (`upstream_not_accepted`,
`upstream_execution_failed`); a consumer is admitted when every incoming link is released; a
consumer that already has a run the chain did not admit blocks the link
(`consumer_already_started`). Migration 0028 relies on a widened family-writer INSERT
(`mission_run`, `budget_account`, `effect_ledger`), recorded in the authority matrix test; a
security review of that widening is an open owner decision.

**State transfer.** `application/chains/packet.py` builds the consumer's first Context
Packet (`purpose: chain_link`): per supplied output the artifact as a `chain_supply` item
bound `<from_mission_key>.<output_name>` (a `materialize` item lands under
`/inputs/<from>.<output>/`), per supplier the acceptance disposition, final checkpoint and
journal-digest references with a `missionctl run transcript` retrieval instruction, and the
chain identity. Nothing from a supplier's workspace is restored: files travel only as
registered artifacts.

**Reading.** `GET /v1/applications/{app}/chains/{chain_id}` and `GET .../chains?mission_id=`
(`interfaces/http/chains.py`), `missionctl chain inspect`, MCP tool `mission_chain_inspect`
and resource `mc://applications/{application_id}/chains/{chain_id}`; run inspection folds in
the run's chain membership ([interfaces](interfaces.md)). Members are cancelled individually
in v1.

## Proven

Unit: `tests/unit/chains/` (compile, reducer, interfaces). `common_db`:
`tests/integration/postgres/test_mission_chain_tables.py`, `test_chain_release.py`. Temporal:
`tests/integration/temporal/test_chain_start_idempotent.py` (second delivery attaches to the
workflow the first started). Acceptance: `tests/acceptance/mission_control/test_chain_two_goal_loops.py`
runs two linked Goal Loops with a test-only launch input author.

## Production relay (MP-02)

`application/chains/relay.py::ChainIntentRelay` leases the outbox intents and starts the
consumer through a `ChainLaunchInputPort`. Since MP-02 the worker runs `ChainRelayPump` when
`CHAIN_RELAY_ENABLED=1` (`bootstrap/worker.py`, `compose_chain_relay_pump` in
`bootstrap/manifests.py`) over the production `ManifestChainLaunchInputs`; enabled without
`MANIFEST_LAUNCH_BINDINGS_PATH` it refuses startup. A redelivered `mc.chain.start_run` attaches to
the consumer's existing workflow (`tests/integration/temporal/test_manifest_launch_production.py`,
deterministic cognition). Every launch binds the run to its Temporal cluster
([operations](operations.md)). Members on Claude, Codex or Cursor lanes start through the same
author once the bindings file binds them; a linked child family that concluded failed is recorded
`failed` (patch `mp20-linked-child-concluded-failed`). MP-20 proves two linked Goal Loops across
providers with a replayed release that starts the consumer once, on real PostgreSQL and Temporal
with FIXTURE providers. Mission 2 of the fixtures (a Cursor Cloud research Goal Loop supplying a
Deep Agents ingestion Goal Loop) no longer fails at the consumer's review: its undeclared Goal Loop
reviewer is the `owner` role ([Human Gates](human-gates.md)). As authored it still cannot start:
its research member's hook script has no slot in `mc.cursor_binding.v1`
([mission manifest](mission-manifest.md)).

# Citations

- Spec: [SPEC-04](../specs/fast-track-2026-10/SPEC-04-mission-chains.md);
  [00-ARCHITECTURE](../specs/fast-track-2026-10/00-ARCHITECTURE.md).
- ADRs: [0029](../adr/0029-mission-chains-compile-to-linked-missions-released-by-outbox.md),
  [0004](../adr/0004-temporal-sole-scheduler-transactional-outbox.md),
  [0034](../adr/0034-mission-manifest-v1-settled-environment-inheritance-chains-agents-hooks.md).
- Code: [chain contracts and compile](../../src/mission_control/domain/composition/chain.py),
  [structure service](../../src/mission_control/application/chains/service.py),
  [reducer](../../src/mission_control/application/chains/reducer.py),
  [chain packet](../../src/mission_control/application/chains/packet.py),
  [intent relay](../../src/mission_control/application/chains/relay.py),
  [PostgreSQL chains](../../src/mission_control/adapters/postgres/chains/store.py),
  [chain routes](../../src/mission_control/interfaces/http/chains.py),
  [chain MCP tool](../../src/mission_control/interfaces/mcp/mission_tools.py),
  [migration 0028](../../packages/mission-control-db-contract/component/migrations/0028_mission_chains.sql).
- Tests: [compile](../../tests/unit/chains/test_chain_compile.py),
  [reducer](../../tests/unit/chains/test_chain_reducer.py),
  [interfaces](../../tests/unit/chains/test_chain_interfaces.py),
  [tables](../../tests/integration/postgres/test_mission_chain_tables.py),
  [release](../../tests/integration/postgres/test_chain_release.py),
  [idempotent start](../../tests/integration/temporal/test_chain_start_idempotent.py),
  [two linked Goal Loops](../../tests/acceptance/mission_control/test_chain_two_goal_loops.py),
  [MP-20 cross-provider chains](../../tests/integration/temporal/test_mp20_workflow_parity.py),
  [linked child concluded failed](../../tests/unit/orchestration/test_linked_child_concluded_failed.py).
