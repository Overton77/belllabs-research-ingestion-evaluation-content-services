# Chain patterns

## Research then ingestion (two Goal Loops)

```yaml
links:
  - { from: research.sources, to: ingestion.inputs.sources, kind: supplies, release_on: goal_accepted }
  - { from: research.claims,  to: ingestion.inputs.claims,  kind: supplies, release_on: goal_accepted }
```

Both links must release before `ingestion` starts; a consumer with several incoming links
releases when all of them are released (an `any` join is deferred to manifest v2). The
packer merges the supplied items into one Context Packet, namespaced by the supplying
mission key.

## Fan-in review

```yaml
missions: [ { key: sweep_a, … }, { key: sweep_b, … }, { key: review, … } ]
links:
  - { from: sweep_a.report, to: review.inputs.reports, kind: supplies, release_on: execution_complete }
  - { from: sweep_b.report, to: review.inputs.reports, kind: supplies, release_on: execution_complete }
```

A list-typed input collects every supplying output; `execution_complete` lets a review start
even when a sweep was `not_accepted`, and the packet labels each item with the supplier's outcome.

## Gate before an irreversible mission

```yaml
missions: [ { key: implement, … }, { key: release, … } ]
links:
  - { from: implement, to: release, kind: depends_on, release_on: mission_accepted }
```

`depends_on` with `mission_accepted` makes acceptance of the whole upstream mission (including
its human gates) the release condition; `release` carries its own `external_write_irreversible`
grant and its own human gate.

## Cancellation

| `on_upstream_cancel` | Effect on the consumer |
| --- | --- |
| `cancel_downstream` (default) | Consumer missions that have not started are closed `superseded`; running ones receive `cancel` with the chain as reason |
| `detach` | The link is marked `detached`; the consumer stays admitted and must be started explicitly with an operator-supplied input |

## What the consumer's packet contains

| Item | Tier |
| --- | --- |
| Each supplied output | As declared by the consumer's `expand` (`materialize` for files, `inline` for small typed outputs) |
| Upstream final Progress Review summary | `inline` |
| Upstream Loop Journal digest and Continuation Checkpoint reference | `reference` with retrieval instruction (`missionctl run transcript`) |
| Upstream mission, run and revision identities | `inline` in `.mission/context.md` provenance block |

Nothing else crosses: no transcript bodies, no workspace files beyond the supplied outputs,
no grants.

## Anti-patterns

- Chaining to split one goal across missions for budget accounting: use node-level
  `environment.budget` instead.
- Hiding a chain inside a subagent: independent goals need independent admission.
- Writing a link whose `from` is a node that is not projected as a mission output: the compiler
  rejects it with `OUTPUT_NOT_PROJECTED`.
