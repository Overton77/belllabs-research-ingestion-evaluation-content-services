---
type: Specification Index
title: "Fast-track packet 2026-10: readiness for three missions"
description: "Index and reading order for the fast-track specifications, research notes, mission manifests, ticket drafts and team workspace that make Mission Control ready to run a Deep Agents research and ingestion mission, a Cursor Cloud research and ingestion chain, and a Cursor Local codebase feature mission."
tags: [mission-control, spec, fast-track, index]
---

# Fast-track packet 2026-10

Owner interview 2026-10-07; recommendations accepted outright. Decisions live in `docs/adr/0023` to `0034`; vocabulary in `GLOSSARY.md`. This directory is the implementation packet.

| Read | File | Why |
| --- | --- | --- |
| 1 | [00-ARCHITECTURE.md](00-ARCHITECTURE.md) | The synthesis: additions, contracts, tables, modules, lane matrix, flows, ticket plan |
| 2 | [TEAM-WORKSPACE.md](TEAM-WORKSPACE.md) | How the agent team executes it: ownership, waves, protocol, verification, kickoff prompt |
| 3 | [SPEC-01-capabilities-catalog.md](SPEC-01-capabilities-catalog.md) | Capability kinds, bundle custody, hybrid search, host projection, hook scripts, subagent profiles, plugins, seeds |
| 4 | [SPEC-02-context-packet.md](SPEC-02-context-packet.md) | Context Packet, packer, tiers, stage and iteration handoff, continuation checkpoint |
| 5 | [SPEC-03-mission-state-and-transcript.md](SPEC-03-mission-state-and-transcript.md) | Provider frames, reducer derivation, transcript, run search |
| 6 | [SPEC-04-mission-chains.md](SPEC-04-mission-chains.md) | Chains of missions, links, release through the outbox, state transfer |
| 7 | [SPEC-05-mission-manifest.md](SPEC-05-mission-manifest.md) | Mission Manifest v1 schema, inheritance, compile, submit, start |
| 8 | [SPEC-06-interventions-inspection-subscriptions.md](SPEC-06-interventions-inspection-subscriptions.md) | Real commands, delivery reports, inspection, subscriptions |
| 9 | [SPEC-07-harness-and-cursor-lane.md](SPEC-07-harness-and-cursor-lane.md) | AgentHarness protocol, lane activities, Cursor Local and Cloud, Temporal synthesis |
| 10 | [SPEC-08-agent-skills.md](SPEC-08-agent-skills.md) | Router skill and the five specific bundles |
| 11 | [missions/](missions/) | The three owner missions as manifests (acceptance fixtures) |
| 12 | [issues/](issues/) | One draft per ticket, mirrored in Linear |
| 13 | [research/](research/) | Primary-source notes: codebase map, Cursor platform, Temporal lifecycle, Deep Agents middleware, seed capabilities and formats |
| 14 | [OWNER-FIXTURE-RUNBOOK.md](OWNER-FIXTURE-RUNBOOK.md) | How the owner runs the three mission fixtures (I1 to I3) on the local real stack: readiness, blockers, release 1.1.0 install, commands, success criteria, budget, stopping, open decisions |

Status (2026-10-08): the 36 tickets A1 to H1 and a readiness pass are implemented and merged to `main` at `f8d325a`; no live mission has run (I1 to I3 are owner-run) and a live start is blocked (runbook section 1). The per-spec position and the remaining gates are in [../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md](../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md); the retrievable concepts are in [../../knowledge/index.md](../../knowledge/index.md).

Precedence on conflict: spec pack (`../mission-control-general`), then `docs/adr`, then this packet, then `docs/knowledge`, then implementation status. Report conflicts in the ticket rather than choosing silently.
