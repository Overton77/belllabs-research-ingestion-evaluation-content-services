---
type: Specification
title: "SPEC-04: Mission Chains — composing whole missions through supplies and depends_on links released by the outbox"
description: "Full specification of mc.chain.v1 and mc.chain_link.v1: how a manifest's missions list compiles to independent missions plus typed links, the chain reducer that admits the next Run in the same transaction as the link condition's mission event and hands it a Context Packet, the outbox release with idempotent Temporal start, chain lifecycle and events, cancellation cascade, the CLI, HTTP and MCP surface, the relationship to child mission invocation and the unused LinkedRunService, and the Mission 2 acceptance scenario; tickets D1 to D3."
tags: [mission-control, spec, fast-track, composition]
---

# SPEC-04: Mission Chains

Decisions: ADR-0029 (chains compile to linked missions released by the outbox), ADR-0027 (Context Packet carries state across links), ADR-0004 (Temporal is the sole scheduler, fed by the transactional outbox), ADR-0031 (idempotent start with `USE_EXISTING`). Normative background: workflow-types/06 (Mission Invocation, Portals, Mission Graph relationships `supplies`, `depends_on`). Companions: [SPEC-05 Mission Manifest](SPEC-05-mission-manifest.md) (the `missions` and `links` form), [SPEC-02 Context Packet](SPEC-02-context-packet.md) (the packer), [SPEC-06](SPEC-06-interventions-inspection-subscriptions.md) (events and subscriptions). Codebase facts: [research/codebase-map.md](research/codebase-map.md) section 7 and gap (e); [research/temporal-lifecycle.md](research/temporal-lifecycle.md) section 8 (e).

## Problem Statement

The owner wants to compose several Stage Graph or Goal Loop workflows into one authored unit where the second workflow begins with the first one's results, and where "mission state" (accepted outputs, decisions, the journal so far) moves across the boundary. Today the kernel has the pieces for a related but different thing: `LinkedRunService`, its two Temporal workflows and four tables let a running family request a child run and wait for its result, but nothing in production calls it, there is no HTTP route, no way to map a child's outputs into a consumer's inputs, and no notion of a chain as an authored, inspectable object. There is also no `Portal`, no `supplies` or `depends_on` relationship kind in `mission_relationship`, and no mechanism to start a mission when another one is accepted.

## Solution

A Mission Chain is an ordered set of whole missions, each with its own Revision, Run, budget and acceptance, connected by Chain Links of kind `supplies` (a typed output-to-input binding) or `depends_on` (a release dependency without data). A chain is authored in one manifest (`missions:` plus `links:`), compiled into N missions and N-1 or more links, and submitted as one unit that admits the first mission's Run. The chain reducer lives on the ledger: when the mission event that satisfies a link's acceptance condition commits, the reducer, in the same transaction, builds a Context Packet for the consumer from the supplier's accepted outputs, final Continuation Checkpoint reference and Loop Journal digest, admits the consumer's Run, writes the outbox intent and emits `chain_link.released`. The outbox relay starts the consumer's Temporal root idempotently. No wrapper workflow spans missions; no workspace is shared; the Portal of workflow-types/06 is the only cross-mission read.

## User Stories

1. As an owner, I want to declare two or more missions and their links in one manifest, so that research and ingestion are authored and reviewed together.
2. As an owner, I want each mission in a chain to keep its own budget, governors and acceptance, so that an overrun in one does not silently consume the other.
3. As an owner, I want a `supplies` link to carry named outputs from the supplier into named inputs of the consumer, so that the consumer's first iteration starts from accepted artifacts, not from prose.
4. As an owner, I want the consumer to receive the supplier's final checkpoint reference and journal digest in its Context Packet, so that mission state is transferred, not only files.
5. As an owner, I want the link's release condition to be `goal_accepted` by default, so that provisional outputs never start downstream work.
6. As an owner, I want to choose `mission_accepted` or `execution_complete` when a link should wait for the whole mission or merely for execution to finish, so that I control the handoff strictness.
7. As an owner, I want a `depends_on` link that releases without data, so that ordering alone can be expressed.
8. As an operator, I want `missionctl chain inspect CHAIN_ID` to show every mission's lifecycle and every link's state, so that I see where a chain is waiting.
9. As a coordinator agent, I want `mission_chain_inspect` as an MCP tool and `GET /chains/{id}` on HTTP, so that hosts and dashboards read the same projection.
10. As an owner, I want cancelling an upstream mission to cancel downstream missions by default, so that a stopped chain does not leave orphan runs.
11. As an owner, I want to declare `on_upstream_cancel: detach` on a link, so that a downstream mission that already started can finish independently.
12. As an owner, I want an upstream mission ending `not_accepted` or `governor_exhausted` to block the link and the chain to stop with a typed reason, so that downstream work never runs on rejected inputs.
13. As an operator, I want chain events (`chain.created`, `chain_link.released`, `chain_link.blocked`, `chain.completed`) in the mission event stream of every participating mission, so that subscriptions see chain progress.
14. As an operator, I want the consumer's Run to be admitted in the same database transaction as the link condition's event, so that a crash between acceptance and admission cannot lose the release.
15. As an operator, I want the Temporal start of the consumer to be idempotent through a run-derived workflow id, so that relay retries never create a duplicate run.
16. As a reviewer, I want `mission_relationship` to record `supplies` and `depends_on` rows with the link's binding, so that the Mission Graph of workflow-types/06 is queryable.
17. As an owner, I want submitting a chain to return one chain id and every mission id, so that I can address the whole or any part.
18. As an owner, I want the chain's first mission admitted at submit and started by the ordinary `start` verb, so that chains follow the same commit-then-start law as single missions.
19. As an owner, I want to fork a mission inside a chain without forking the chain, so that recovery stays local; the fork is `forked_from` and not a chain member unless I attach it.
20. As a coordinator agent, I want a chain whose link references an output the supplier never declares to fail at compile with a pointer, so that wiring errors are caught early.
21. As an operator, I want a consumer that receives a `supplies` packet larger than its model budget to get references and materialized files rather than a compile failure, so that chains do not depend on prompt size.
22. As an owner, I want the chain to complete `accepted` only when every member mission closes `mission_accepted`, so that chain acceptance is computed, never implied by the last run finishing.
23. As an operator, I want a chain with a link whose consumer is already running (manual start) to record `chain_link.blocked{reason: consumer_already_started}` rather than admit a second run, so that manual actions and the reducer never race into duplicates.
24. As an owner, I want child mission invocation to remain available for a node that must wait inside a program, so that chains and portals coexist.
25. As a developer, I want the existing `LinkedRunService` left untouched and documented as the `await`-style mechanism inside programs, so that no second linked-run engine appears.

## Contracts

### `mc.chain.v1`

```text
MissionChain {
  schema_version: "mc.chain.v1"
  chain_id                         // UUIDv7
  scope: {installation_id, application_id, tenant_id}
  chain_key                        // slug from the manifest file name or mission keys, unique per tenant
  title
  manifest_digest                  // the chain's manifest (SPEC-05)
  members[]: { mission_key, mission_id, revision_id, order }   // order = topological position
  links[]: ChainLink
  lifecycle: pending | running | completed
  phase: releasing | draining | blocked
  terminal_outcome?: accepted | not_accepted | cancelled | execution_failed
  created_at, created_by_actor_ref, version
}
```

### `mc.chain_link.v1`

```text
ChainLink {
  schema_version: "mc.chain_link.v1"
  link_id
  chain_id
  from_mission_key, to_mission_key
  kind: supplies | depends_on
  bindings[]: { output_name, input_name, schema_ref, expand: inline | reference | materialize | auto }   // supplies only
  on: goal_accepted { goal_key } | mission_accepted | execution_complete           // default goal_accepted of the goal owning the first bound output
  on_upstream_cancel: cancel_downstream | detach                                     // default cancel_downstream
  on_upstream_not_accepted: stop                                                     // v1: only stop
  state: armed | released | blocked | detached | cancelled
  released_run_id?                 // the consumer run admitted by this link
  released_at?, blocked_reason?, packet_ref?, packet_digest?
}
```

A consumer with several incoming links is released when **all** incoming links are `released` (the default expression is `all`; `any` is deferred to v2). The packets of all incoming `supplies` links are merged by the Context Packer into one packet; items are namespaced by `from_mission_key`.

### Release condition → mission event

| `on` | Mission event that satisfies it |
| --- | --- |
| `goal_accepted{goal_key}` | `disposition.recorded{source: completion_contract, goal_key, value: accepted}` for the supplier mission's current Run |
| `mission_accepted` | `mission.closed{closure_outcome: mission_accepted}` |
| `execution_complete` | `run.completed` regardless of `terminal_outcome` (outputs may be provisional and are labelled so in the packet) |

Blocking events: `run.completed{terminal_outcome ∈ not_accepted, governor_exhausted, no_progress, execution_failed, stopped_by_policy}` blocks every outgoing link with `blocked_reason: upstream_<outcome>`; `run.completed{terminal_outcome: cancelled}` applies `on_upstream_cancel`.

### Events

| Event | Payload |
| --- | --- |
| `chain.created` | `chain_id`, `chain_key`, members, links (keys only) |
| `chain_link.released` | `link_id`, `from_mission_id`, `to_mission_id`, `released_run_id`, `packet_ref`, `packet_digest` |
| `chain_link.blocked` | `link_id`, `blocked_reason` |
| `chain_link.detached` | `link_id` |
| `chain.completed` | `chain_id`, `terminal_outcome`, per-member outcomes |

Chain events are written to **every** member mission's stream (same `event_id`, one `seq` per mission) so a subscription on any member sees the chain; `child_mission.*` events are not used for chains.

## Implementation Decisions

### Compile

`ManifestCompileService.compile` (SPEC-05) compiles each mission in `missions:` independently (independent Validation Reports, aggregated), then validates `links`:

- `from` and `to` name distinct missions in the file; the link graph is acyclic (`INVALID_DEFINITION`, `chain_cycle`);
- for `supplies`, every `outputs` entry is an output projected by the supplier's program root, and the consumer declares an input with `from: <from_key>.<output_name>` whose `schema_ref` matches (`INVALID_DEFINITION`, `unbound_chain_output` or `schema_mismatch`);
- `on.goal_key`, when given, is a goal of the supplier;
- the first mission in topological order has no incoming links (it is the one admitted at submit); every other mission has ≥1 incoming link;
- a consumer's lane need not equal the supplier's; packets render on every lane.

The resolution (`mc.manifest_resolution.v1`) gains `chain: { order[], links[] }`.

### Persistence (migration `0028_mission_chains.sql`, team T3)

```sql
CREATE TABLE mission_control.mission_chain (
  installation_id uuid NOT NULL, application_id text NOT NULL, tenant_id uuid NOT NULL,
  chain_id uuid PRIMARY KEY, chain_key text NOT NULL, title text NOT NULL,
  manifest_digest text NOT NULL, lifecycle text NOT NULL CHECK (lifecycle IN ('pending','running','completed')),
  phase text NOT NULL CHECK (phase IN ('releasing','draining','blocked')),
  terminal_outcome text CHECK (terminal_outcome IN ('accepted','not_accepted','cancelled','execution_failed')),
  members jsonb NOT NULL CHECK (jsonb_typeof(members) = 'array'),
  version bigint NOT NULL CHECK (version >= 1), created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
  created_by_actor_ref text NOT NULL,
  UNIQUE (installation_id, application_id, tenant_id, chain_key),
  FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (...)
);
CREATE TABLE mission_control.chain_link (
  installation_id uuid NOT NULL, application_id text NOT NULL, tenant_id uuid NOT NULL,
  link_id uuid PRIMARY KEY, chain_id uuid NOT NULL REFERENCES mission_control.mission_chain (chain_id),
  from_mission_id uuid NOT NULL, to_mission_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('supplies','depends_on')),
  bindings jsonb NOT NULL CHECK (jsonb_typeof(bindings) = 'array'),
  release_condition jsonb NOT NULL, on_upstream_cancel text NOT NULL CHECK (on_upstream_cancel IN ('cancel_downstream','detach')),
  on_upstream_not_accepted text NOT NULL CHECK (on_upstream_not_accepted = 'stop'),
  state text NOT NULL CHECK (state IN ('armed','released','blocked','detached','cancelled')),
  released_run_id uuid, released_at timestamptz, blocked_reason text, packet_ref text, packet_digest text,
  version bigint NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
  UNIQUE (installation_id, application_id, tenant_id, link_id)
);
CREATE TABLE mission_control.authoring_provenance ( ... );   -- SPEC-05
ALTER TABLE mission_control.mission_relationship DROP CONSTRAINT mission_relationship_kind_check;
ALTER TABLE mission_control.mission_relationship ADD CONSTRAINT mission_relationship_kind_check
  CHECK (kind IN ('composition','fork','dependency','supplies','depends_on','parent_of','adopted_from','successor_of','forked_from'));
```

All three tables are RLS-forced with the transaction-local `mc.*` scope like the rest of the component, granted to the same NOLOGIN roles as `mission_relationship`, and included in the release fingerprint. Each `chain_link` also writes one `mission_relationship` row (`kind = supplies | depends_on`, `source_run_id` = supplier's run once known, `detail` = link id and bindings) so the Mission Graph is complete; `mission_relationship.source_run_id` is populated at release time because the supplier's run id exists from submit but the consumer's only from release.

### Chain reducer

`application/chains/reducer.py::ChainReducer.on_events(commit)` is invoked inside `append_events` (the canonical ledger writer) **after** the mission events of a commit are written and **before** the transaction commits, for every event whose type is in the release or blocking tables above. It is pure over `(chain rows, link rows, event)` and returns a typed `ChainTransition` that the caller applies in the same transaction:

1. Load the chain(s) whose links name the event's mission as `from`.
2. For each armed link whose condition the event satisfies: mark `released`; if every incoming link of `to_mission` is now released, call the Context Packer (`domain/context/packet.py`, SPEC-02) with the merged bindings, the supplier runs' accepted outputs (artifact refs and digests from `artifact.registered` and the completion dispositions), the supplier's final `mc.continuation_checkpoint.v1` reference and Loop Journal digest (when the supplier root is a Goal Loop), and the consumer's effective environment; seal the packet; **admit** the consumer Run through the existing admission path (`mc.runtime_admission.v1` built from the consumer's Compiled Program, input manifest = the packet's `inputs.json`), write the outbox intent (`start_run`) and the `chain_link.released` event into the same commit.
3. For each blocking event: mark outgoing links `blocked` with reason; set chain `phase = blocked`; emit `chain_link.blocked`. If no further release is possible, close the chain `terminal_outcome = not_accepted` (or `execution_failed`, `cancelled`) and emit `chain.completed`.
4. On `mission.closed{mission_accepted}` for the last member in order, when every member closed `mission_accepted`: `chain.completed{accepted}`.
5. Guard: if the consumer already has a run that is not `pending` (manual start raced), do not admit; mark the link `blocked{consumer_already_started}`.

The reducer never calls Temporal, a provider or storage; it reads artifact metadata and writes rows and outbox intents.

### Release through the outbox

The existing relay delivers `start_run` intents by `client.start_workflow(MissionRunWorkflow, input, id=f"mission-run:{run_id}", id_conflict_policy=USE_EXISTING, id_reuse_policy=REJECT_DUPLICATE, task_queue=<app control queue>)`. A retry after a lost response attaches to the existing workflow; a conflicting run id is impossible because the id is derived from the admitted run. This is the same path a single mission's `start` uses, so chains add no second scheduler. Note: the consumer is admitted **and started** by the reducer (chains are the one case where start is not a separate human call); this is authorized at submit time by `controls.chain_autostart: true` (default true for chains) and recorded as `actor_ref = chain:<chain_id>` on the launch.

### State transfer

The packet given to the consumer contains, per supplied output: the artifact reference and digest (tier per binding `expand`, default `auto`); the supplier's acceptance disposition for the owning goal (`inline`); the supplier's final Progress Review summary and Loop State (`inline`, bounded); the supplier's final Continuation Checkpoint reference and Journal Digest reference (`reference`, with `missionctl run transcript <supplier_run> --since` as the retrieval instruction); the chain identity and the link (`inline`). Nothing from the supplier's workspace is restored (`workspace` tier is not used across links); files travel only as registered artifacts. The consumer agent never writes to the supplier.

### Cancellation cascade

`cancel` on a member mission: the mission's own cancel path runs (SPEC-06); on its `run.completed{cancelled}` the reducer applies each outgoing link's `on_upstream_cancel`: `cancel_downstream` admits a `cancel{urgency: normal, reason: upstream_cancelled}` command for any released consumer run and marks armed links `cancelled`; `detach` marks links `detached` and the downstream continues. A `cancel` addressed to the chain (`missionctl chain cancel`) is v2; in v1 operators cancel members.

### Relationship to child mission invocation and LinkedRunService

Child mission invocation (workflow-types/06) is a Program Node inside a mission that spawns or attaches a child and may `await` it; its runtime implementation reuses `LinkedRunService` and the `belllabs.linked-run` workflows (a node must wait inside a family). Chains have no waiting node: the supplier is complete before the consumer exists. Both record `mission_relationship` rows; both read the other mission only through artifacts and the Portal. This packet does not wire child mission invocation (out of scope); it leaves `LinkedRunService` as is.

### Interfaces

| Surface | Verb | Behavior |
| --- | --- | --- |
| CLI | `missionctl chain inspect CHAIN_ID [--json] [--wait SECONDS]` | members with lifecycle, phase, terminal outcome, run ids; links with state, reason, released run, packet digest |
| CLI | `missionctl mission submit chain.yml` | returns `chain_id` and members (SPEC-05) |
| HTTP | `GET /v1/applications/{app}/chains/{chain_id}` | `mc.chain.v1` projection |
| HTTP | `GET /v1/applications/{app}/chains?mission_id=` | chains a mission belongs to |
| MCP | `mission_chain_inspect(chain_id)` | read-only |
| Resource | `mc://applications/{app}/chains/{chain_id}` | the same projection |

Grants: `mission.read` for reads; the reducer runs under the service's installation authority.

### Insertion points

- New: `domain/composition/chain.py` (contracts, pure transitions), `application/chains/service.py` (inspect, submit helpers), `application/chains/reducer.py`, `interfaces/http/chains.py`, CLI `chain` group, MCP tool and resource.
- Change: `adapters/postgres/run_control/canonical.py::append_events` gains a post-write hook list where the chain reducer registers (integrator-owned line); the outbox relay's `start_run` intent handler is reused unchanged; `ManifestCompileService` (SPEC-05) validates links and persists chain rows at submit.
- Reuse: admission path from `CoordinatorWorkflowLaunchService.launch` (frozen run request → reducer admit → outbox); `PostgresArtifactDurableReferenceRepository` to read accepted outputs; `domain/context/packet.py` (SPEC-02).

## Testing Decisions

Good tests drive the reducer with real ledger commits and assert on rows, outbox intents and events; they never assert on internal transition objects except through persisted effects.

- Unit (`tests/unit/chains/test_chain_compile.py`): link validation errors with pointers (cycle, unbound output, schema mismatch, missing incoming link); topological order; resolution `chain` section.
- Unit (`test_chain_reducer.py`): pure reducer over fixtures: release on `goal_accepted`, `mission_accepted`, `execution_complete`; all-incoming-links rule; blocking on each terminal outcome; cascade `cancel_downstream` vs `detach`; `consumer_already_started` guard; chain completion only when every member is `mission_accepted`.
- Integration (`tests/integration/postgres/test_chain_release.py`, `common_db`): submit a two-mission chain; append the supplier's acceptance events through `append_events`; assert in one transaction: link `released`, consumer run admitted (`mission_run` row `pending`), outbox `start_run` intent, `chain_link.released` in both missions' streams with the same `event_id`; replay the same commit → no-op; crash injection between event write and reducer (exception) rolls back everything.
- Integration (`tests/integration/temporal/test_chain_start_idempotent.py`, time-skipping environment): relay delivers the intent twice; one workflow with id `mission-run:<run_id>`.
- Acceptance (`tests/acceptance/mission_control/test_chain_two_goal_loops.py`, real local stack, ticket D3): Mission 2's manifest with the Cursor Cloud lane stubbed by the Deep Agents lane (lane choice is a fixture override) runs `research`, accepts `evidence_map`, releases `ingestion`, whose first packet contains the supplied artifact (materialized) and the checkpoint reference; `missionctl chain inspect` shows `completed{accepted}`.
- Prior art: `tests/integration/temporal/test_linked_runs.py` (linked-run workflows, time skipping), `tests/integration/postgres/test_mission_control_lifecycle_postgres.py` (ledger lifecycle), `tests/unit/run_control/test_boundary_commands.py`.

## Out of Scope

`any`/quorum release expressions for consumers with several links; `on_upstream_not_accepted: continue_with_partial`; chain-level commands (`chain cancel`, `chain pause`); attaching an existing mission to a chain after submit; cross-application chains; wiring child mission invocation (the node) to `LinkedRunService`; Portal `peek_and_command` through chains; Nexus or cross-namespace links.

## Further Notes

Because the consumer is admitted and started by the reducer, a chain is the one place where a human does not call `start` for every run. The manifest's `controls.chain_autostart` (default `true`) makes that explicit and auditable; setting it `false` admits the consumer run and leaves it `pending` for a manual `start`, which is how an owner inserts a review between missions without a Human Gate node.

The `supplies` packet should be thought of as the chain's Stage handoff of the specification's "Stage handoff algorithm" lifted one level: producer acceptance, input manifest from accepted outputs, no shared mutable state.

## Tickets

- [FT-D1](issues/D1-chain-contracts-tables-compile.md) Chain contracts, tables and compile from a missions list.
- [FT-D2](issues/D2-chain-reducer-outbox-release-packet.md) Chain reducer releases the next run through the outbox with a packet.
- [FT-D3](issues/D3-acceptance-two-linked-goal-loops.md) Acceptance: two linked Goal Loops transfer state.

## Acceptance scenario (Mission 2)

1. `missionctl mission submit docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml` → `chain_id`, missions `research` (run `R1` pending) and `ingestion` (no run), link `L1 supplies research → ingestion on goal_accepted{evidence_map}` armed, `L2 depends_on` armed.
2. `missionctl mission start R1` → `research` runs its Goal Loop on `cursor_cloud`; iterations seal journal segments; outputs `evidence_map` (artifact) registered; completion candidate accepted; `disposition.recorded{goal evidence_map accepted}` commits.
3. In that commit: `L1 released`; `L2` waits for `run.completed`; on `run.completed{accepted}` of `R1`: `L2 released`; all incoming links released → packet built (`evidence_map` materialized at `/inputs/research.evidence_map/…`, acceptance disposition inline, checkpoint and journal digest references), `ingestion` run `R2` admitted, outbox `start_run` written, `chain_link.released` ×2 in both streams.
4. Relay starts `mission-run:R2` (`USE_EXISTING`); `ingestion` Goal Loop on `deep_agents` reads `.mission/context.md`, finds the materialized evidence map and the retrieval instruction for the research transcript.
5. `ingestion` accepts; `mission.closed{mission_accepted}` for both → `chain.completed{accepted}`; `missionctl chain inspect` shows it.

# Citations

- ADR-0029, ADR-0027, ADR-0004, ADR-0031, ADR-0008 (`docs/adr/`).
- `../mission-control-general/workflow-types/06-MISSION_INVOCATION.md` (relationships, Portal, invocation modes), `09-EVENTS_COMMANDS_AND_STREAMS.md` (event envelope, child streams), `01-STAGE_GRAPH.md` (handoff and provisional bindings), `../mission-control-general/general-mission-control/expansion/CONTEXT-STATE-AND-CONTROL.md` (stage handoff algorithm).
- [research/codebase-map.md](research/codebase-map.md) section 7 (`LinkedRunService`, `mission_relationship` kinds, no production caller) and section 6 (`append_events`, outbox).
- [research/temporal-lifecycle.md](research/temporal-lifecycle.md) section 8 (e) (outbox release with `USE_EXISTING`, `REJECT_DUPLICATE`; child workflows only inside a run).
- `packages/mission-control-db-contract/component/migrations/0005_*.sql` (`mission_relationship`), `0014_*.sql` (linked-run tables).
- `GLOSSARY.md` (Mission Chain, Chain Link, Context Packet, Portal, Mission Relationship, Run, Revision).
