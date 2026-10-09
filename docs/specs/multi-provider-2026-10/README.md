---
type: Specification
title: "Multi-provider execution: architecture and implementation packet"
description: "Research-backed plan for Stage Graph, GoalDirected and linked missions across Deep Agents, Cursor, Claude Code and Codex, with local and provider-hosted cloud profiles."
tags: [mission-control, specifications, providers]
---

# Multi-provider execution packet — 2026-10-08

Status: implementation proposal commissioned by the owner; no new runtime support or live qualification is claimed. Baseline: `7c9b755d75c4417ae364f33c340b9891446a391a`. Research checked on October 8, 2026. The owner explicitly clarified: **cloud means provider-hosted cloud products only**. Running Claude or Codex on our own remote workers does not satisfy that requirement.

The architecture extends the existing AgentHarness, mission ledger, Temporal workflows, Context Packet, capability catalog and subscription outbox. The immediate deliverable is working Stage Graph, GoalDirected and Mission Chains with honest per-profile controls. Parallel Swarm and Evaluator Optimizer remain outside this increment. A coordinator may launch independent missions concurrently within admitted limits.

## Execution status

As of 2026-10-09: waves 0 to 2 (MP-01 to MP-06, MP-10, MP-13, MP-14, MP-16, MP-17, MP-22) are integrated as an uncommitted working state on the baseline, with the integrator's wiring; wave 3 (MP-07, MP-08, MP-09, MP-11, MP-12, MP-15) is in flight and not integrated; MP-18, MP-19 and MP-21 are blocked by Outcome 3. The timeline and per-ticket evidence are in the team ledger (`.scratch/multi-provider-2026-10-08/team/LEDGER.md`) and the [implementation status](../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md#multi-provider-packet-2026-10); the per-profile release statement is [multi-provider-2026-10](../../qualification/release/multi-provider-2026-10.md). No profile is newly qualified, and the packet is not all-provider complete. The planning content below is unchanged.

## Read in this order

1. [Architecture and current gaps](ARCHITECTURE.md).
2. [Provider evidence and capability matrix](RESEARCH.md).
3. [Runtime lifecycle, interventions and continuation](SPEC-01-runtime.md).
4. [Manifest, environments, worktrees and materialization](SPEC-02-environments.md).
5. [Human Gate and MCP/tool approvals](SPEC-03-human-control.md).
6. [Events, Socket.IO and coordinator subscriptions](SPEC-04-realtime.md).
7. [Qualification and acceptance](VALIDATION.md).
   Decision records: [ADR-0035](../../adr/0035-seven-lane-profiles-versioned-provider-contracts-and-mission-v2.md) (profiles and contracts, written by MP-01), [ADR-0036](../../adr/0036-mission-stream-is-socket-io-over-shared-handlers-postgres-replay-redis-fanout.md) (Socket.IO mission stream), [ADR-0037](../../adr/0037-mission-control-allocates-workspaces-fork-is-snapshot-plus-packet-plus-conversation.md) (workspaces and forks), [ADR-0038](../../adr/0038-two-origins-of-human-control-over-one-human-task-service.md) (human control), [ADR-0039](../../adr/0039-four-independent-progress-mechanisms-compaction-native-first-continuation-sealed.md) (continuation and compaction), [ADR-0040](../../adr/0040-native-frames-normalized-lifecycle-lineage-graph-bounded-coordinator-subscriptions.md) (frames, lineage, coordinator callbacks). All are `proposed` until the owner accepts them.
8. [Implementation team workspace](TEAM-WORKSPACE.md) and [Cursor kickoff: Fable, Opus and Sonnet](CURSOR-KICKOFF.md).
9. [Issue index](ISSUES.md) and individual issue files.
10. [Requirement coverage](REQUIREMENTS.md) and [delivery/validation record](DELIVERY.md).

## Delivery decisions

- Keep the existing profile IDs `deep_agents`, `cursor_local`, `cursor_cloud`; promote the existing projection IDs `claude_agent_sdk` and `codex` into local runtime profiles. Add `claude_cloud` and `codex_cloud` for the provider-hosted products.
- Use the Claude Python Agent SDK for the local Claude lane and the Codex app-server protocol for the local Codex lane. Qualify pinned versions; do not depend on documentation from a different SDK language.
- Start both hosted-provider capability investigations in the first wave. The researched public surfaces establish some hosted launch/follow-up/status capabilities, but do **not** establish the full observe/cancel/approval lifecycle required here. These are delivery blockers, not permission to substitute a local process on a VM.
- Complete production launch binding, Human Gate execution, continuation wiring and chain relay before claiming end-to-end parity. Existing schemas and merged tickets do not prove those paths run.
- Use Socket.IO over the same application services and persisted streams as HTTP/SSE/MCP. Redis coordinates fanout; PostgreSQL supplies replay and durable authority.
- Own worktree allocation explicitly. Provider conversation forks and Git snapshots are separate artifacts.
- Keep provider-native frames alongside small normalized lifecycle events. Do not flatten away useful provider details or stream every token into Temporal history.

## Scope and authority

The accepted sibling specification and ADRs remain authoritative. This packet proposes their implementation delta; it does not silently rewrite them. MP-01 reconciles the proposed profile/contract changes with the sibling runtime/workflow annex and ADR-0018/0019 before runtime implementation. The existing first Deep Agents parity milestone remains valid; the owner's expanded multi-provider target is a subsequent milestone.

Model IDs, account entitlements, cloud environment IDs and secret names must resolve from the actual deployment registry. Examples are design fixtures, not production bindings. Planning needs none of those secrets. The user's subscription reset explains priority; it does not establish that API-authenticated SDK execution consumes subscription allowance.

## Provenance

- Owner brief: attached `Pasted text.txt`, October 8, 2026; research/planning/specifications/issues/team workspace requested.
- Owner clarification in this chat: provider-hosted cloud products only.
- Source baseline and dirty-file preservation recorded in [ARCHITECTURE.md](ARCHITECTURE.md).
- No provider sessions, paid probes, infrastructure mutations, commits or deployments were performed for this packet.
