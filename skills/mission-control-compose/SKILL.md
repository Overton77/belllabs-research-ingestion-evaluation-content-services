---
name: mission-control-compose
description: Compose several Mission Control missions into a Mission Chain (missions plus links in one manifest), decide between chaining, nesting a Goal Loop or a Stage Graph, and invoking a child mission, and move state across links as a Context Packet. Use when a human wants one workflow to feed another, wants research then ingestion, or asks how mission state transfers between linked workflows.
---

# Compose missions

Three shapes exist; pick by governance boundary, not by size.

| Need | Shape | Because |
| --- | --- | --- |
| Adaptive steps toward an existing objective | Nest a `goal_loop` node inside the program | Same mission, same budget and acceptance |
| A known route with dependencies | Nest a `stage_graph` node | Same mission; typed bindings between stages |
| Independent goals, own budget and acceptance, started in order | A Mission Chain (`missions:` plus `links:`) | Each mission is independently admitted and judged; the link releases the next |
| A node that must wait inside a program for another mission | `child_mission_invocation` node with `await` | The parent program blocks on the child's acceptance |

## 1. Write the chain

```yaml
manifest: mission/v1
missions:
  - key: research
    goals: [ { key: evidence_map, … } ]
    environment: { lane: cursor_cloud, … }
    program: { behavior: goal_loop, … , outputs: [ { name: sources, schema: source_manifest@1 } ] }
  - key: ingestion
    goals: [ { key: graph_updated, … } ]
    inputs: [ { name: sources, schema: source_manifest@1, expand: materialize } ]
    program: { behavior: goal_loop, … }
links:
  - { from: research.sources, to: ingestion.inputs.sources, kind: supplies,
      release_on: goal_accepted, on_upstream_cancel: cancel_downstream }
```

Rules: `supplies` binds one projected output to one declared input and releases the consumer
when the condition holds (`goal_accepted` by default, `mission_accepted`, `execution_complete`);
`depends_on` orders without data; cycles are a compile blocker; every link joins two missions
declared in the same file (cross-file links are not in v1; use `child_mission_invocation` with an
`existing_mission_ref` for that). A consumer with several incoming links releases when all are
released (`any` is deferred to v2). See
[chain-patterns.md](references/chain-patterns.md). Done when `missionctl mission compile` reports
no blockers and the `links` section of the report shows every link resolved.

## 2. Submit and start

`missionctl mission submit chain.yml --json` returns `chain_id` and one `mission_id` per mission.
`missionctl mission start <first mission_id>` starts only missions with no unreleased upstream
link; later missions start when their link releases. Availability: FT-E3 (submit), FT-D2
(release).

## 3. How state moves

When a link's condition commits, Mission Control builds a Context Packet for the consumer from
the supplying mission's accepted outputs (as declared `expand`), its final Continuation
Checkpoint reference and its Loop Journal digest, admits the consumer's run with that packet,
and the outbox starts it. No workspace is shared; nothing mutable crosses. The consumer sees the
packet in `.mission/context.md` and `/inputs/<name>/`.

## 4. Inspect

`missionctl chain inspect CHAIN_ID --json` (FT-D2) lists missions, link states (`armed |
released | detached | cancelled`), the condition each waits for and the packet digest it released
with. Cancelling an upstream mission applies each link's `on_upstream_cancel`.

## Boundaries

- A chain never widens authority: each mission's grants, budget and side effects are its own.
- Provisional outputs cannot release a link; only accepted ones do.
- Changing a link after submit is a new revision of the chain, not an edit.
