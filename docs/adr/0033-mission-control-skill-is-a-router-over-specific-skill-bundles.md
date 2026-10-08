---
type: Decision Record
title: "The mission-control skill is a router that names specific skill bundles for authoring, catalog, observing, intervening and composing; all are catalog capabilities themselves"
description: "skills/mission-control/SKILL.md becomes a short router that establishes scope and points to mission-control-author, mission-control-catalog, mission-control-observe, mission-control-intervene and mission-control-compose, each a versioned bundle with its own manifest; the bundles are seeded into the catalog as skill_bundle capabilities so a mission can pin the coordinator's own skills."
tags: [mission-control, adr, decision, skills]
status: accepted
source: fast-track interview 2026-10-07 (agent skill fleshed out); skills/mission-control; .agents/skills/mission-control-coordinator; writing-for-agents skill
---

# The mission-control skill is a router that names specific skill bundles for authoring, catalog, observing, intervening and composing; all are catalog capabilities themselves

One skill that teaches admission, authoring, search, inspection, intervention, transcripts and composition would sprawl past what an agent attends to. The router keeps the always-loaded cost to one description and a few lines, and each specific bundle carries only its branch: writing and compiling a manifest, searching and pinning capabilities, peeking state and transcripts and subscribing, queueing and interrupting and forking, and chaining missions. The old `.agents/skills/mission-control-coordinator` bundle is retired into `mission-control-author` and `mission-control-catalog` so there is one runtime authority. Each bundle has a `manifest.json` with file digests and a service contract range, and the seed loader registers them as `skill_bundle` rows, which is how a Deep Agents or Cursor mission gets the coordinator skills materialized like any other capability.

## Consequences

- The router's description is the only pointer most hosts load; its wording is tuned for the branches above.
- Commands a bundle describes that are not yet shipped carry an `Availability` line naming the delivering issue until it lands.
- `make skills-manifest` recomputes digests; a stale digest fails `make check`.
