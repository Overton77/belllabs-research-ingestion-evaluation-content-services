---
type: Agent Configuration
title: "Ultra Code implementation team workspace"
description: "Ownership, dependency frontier, handoff protocol and ready-to-run kickoff for the multi-provider implementation team."
tags: [mission-control, agents, implementation]
---

# Implementation team workspace

This packet prepares the team; this planning session does not launch the implementation team or provider fixtures. “Ultra Code” is the owner's name for the execution setup, not a dependency or a guessed model identifier.

Owner update, October 8: execute this mission in Cursor with Fable 5.1 leading, Opus 5.5 specialists and Sonnet 5.5 cleanup passes. Use [the Cursor kickoff](CURSOR-KICKOFF.md) for this run; it supersedes the generic kickoff below for model routing and team sizing. Claude Code Ultra Code is reserved for separate work.

## Reading and authority

Read root/scoped `AGENTS.md`, the accepted sibling specification/runtime annex, `GLOSSARY.md`, then this packet's README, architecture, assigned specification and issue. Source baseline is `7c9b755`; recheck HEAD and dirty state before starting. Existing fast-track work is already in source and must be extended, not rebuilt. Read current Linear states and handoff evidence; generated local issue mirrors are not live status.

MP-01 freezes contracts and integrates the specification/ADR delta. Implementation agents can research/tests-scaffold against the proposed shapes, but must not merge divergent enum, migration or wire-contract definitions. Hosted feasibility MP-16/17 starts immediately in parallel with contract work because it can change what is achievable.

## Ownership

Use one integrator and dynamically size the specialist team to the dependency frontier, disjoint writable regions and available review capacity. The owner imposes no fixed agent-count or usage cap. Add agents when they can finish independent useful work; reduce concurrency when dependencies, shared-file contention, resource limits or integration backlog make it counterproductive. One writable owner per region. Shared paths are integrator-only unless explicitly leased in a claim.

| Role | Primary issues | Writable region |
| --- | --- | --- |
| Integrator/contracts | MP-01/02/20–23 | Shared domain contracts/enums, manifest/lowering, bootstrap composition, migration numbering, lockfile, shared tests/docs |
| Session/runtime | MP-06/07/08/09/12 | New `adapters/claude/`, `adapters/codex/`, existing `adapters/cursor/`, owned harness services and Temporal activity implementations |
| Environment/approval | MP-03/04/05/10/11 | Owned environment/workspace/auth services, projection changes, Human Task service and governed MCP gateway |
| Events/experience | MP-13/14/15 | Frame mapping/lineage, new socket interfaces, stream services, subscription delivery |
| Hosted specialists | MP-16/17/18/19 | Research evidence then distinct `adapters/claude_cloud/`, `adapters/codex_cloud/` |

Freeze shared paths explicitly: `domain/execution/lanes.py`, `domain/execution/contracts.py`, `domain/capabilities/host_support.py`, hook vocabulary, frame/profile enums, `domain/authoring/manifest.py`, all bootstrap files, shared family workflows, `pyproject.toml`, `uv.lock`, DB release metadata and public schema/SDK generation. Specialists send a small proposed patch to the integrator rather than edit shared files concurrently.

MP-02 owns launch/chain composition, MP-10 owns Human Gate behavior, MP-12 owns continuation behavior; all three coordinate shared family workflow edits through the integrator. Preserve patch/version markers and replay evidence. SQL authors use only allocated slots in `packages/mission-control-db-contract/component/migrations/`.

## Workspaces and claims

Allocate one isolated worktree per active ticket from the current integration base. Use a dedicated workspace root and deterministic branch names such as `mp/mp-07-claude-local`, verifying no existing branch/worktree collision. Worktree creation itself does not authorize changes to the owner's dirty checkout. Do not reset, stash, delete, force-remove or repin someone else's files. Commits/pushes/live rollout follow the current session's authorization, not an assumed approval inherited from this document.

Each claim records ticket, agent/role, base commit, worktree, writable paths, blockers and evidence, proposed tests, and any account/environment dependencies. Store artifacts beneath `.scratch/multi-provider-2026-10-08/<ticket>/`; publish a concise claim/handoff in the ticket when executing the authorized team workflow.

Each handoff records changed files/contracts, test commands and passed/failed/blocked/unrun results, saved replay/drill records, usage disposition, unresolved facts, API/schema compatibility and follow-on tickets. Mark In Review after checks. The integrator verifies on the integration branch before Done. `qualified=True` needs a cited live record for the exact profile, never just a passed fake.

## Dispatch

Use the native dependency graph mirrored in [ISSUES.md](ISSUES.md). “Ready for agent” means specified; it does not override open blockers. Hosted adapter issues have an additional evidence gate: their feasibility issue must positively establish the required controls. Closing a research issue with “unsupported” does not make its adapter implementable.

Prioritize a vertical slice: one production-bound local goal run, then review/resume and queue/cancel, then context rollover and chain release. Advance hosted research from the first wave, and avoid spending an entire coding window on disconnected adapter stubs. No broad rewrite of the already-working Deep Agents path is needed.

## Kickoff prompt

```text
Implement the multi-provider Mission Control packet in
C:\Users\Pinda\Proyectos\Biotech\mission-control.

Read docs/specs/multi-provider-2026-10/README.md, TEAM-WORKSPACE.md,
ISSUES.md, your ticket, and root/scoped AGENTS.md before editing.
Use Linear project Mission Control, team OVE, as the issue authority.
The packet's MP IDs map to Linear identifiers in ISSUES.md.

Cloud explicitly means provider-hosted Cursor, Claude Code and Codex products.
Do not substitute SDKs on our own cloud workers, managed-agent products, private
endpoints or browser automation. Research MP-16 and MP-17 immediately.

Keep Temporal as the sole mission scheduler and reducers as acceptance authority.
Reuse AgentHarness, Context Packet, capability projections, mailbox, stop fence,
frame store, Human Task tables, subscriptions and transactional outbox.
First wire production launch and chain inputs; implement the actual Human Gate
node and continuation call sites. Generated schemas alone are not runtime support.

Use an integrator and disjoint worktree owners. Freeze shared contracts first,
then dispatch only tickets whose blockers/evidence gates are satisfied. Keep
existing owner changes and don't merge/push/deploy without session authorization.
No provider/account/model/environment binding may be invented.

Complete ticket-specific offline tests, real DB/Temporal checks and history replay.
Qualify provider behavior with finite authorized drills and preserve redacted records.
Report accepted work, current frontier, blockers and spent/estimated/unknown usage.
Finish with per-profile parity evidence. Never declare all-provider completion
while either provider-hosted lifecycle remains unqualified.
```

## Inputs needed only for live qualification

Actual model/profile mappings, approved auth/billing routes, provider cloud environment IDs, repo/branch access, admitted callback/MCP endpoint reachability and finite drill limits. Build/validate configuration readers and deterministic fixtures without waiting for those values; fail launch readiness with exact missing-profile errors. Do not ask for secret values in chat.
