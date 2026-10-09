---
type: Specification
title: "Multi-provider issue index and dependency frontier"
description: "Published Linear issues and dependency waves for the multi-provider implementation packet."
tags: [mission-control, issues, agents]
---

# Issue index

Parent: [OVE-63](https://linear.app/overtonbell/issue/OVE-63/mp-epic-multi-provider-lifecycle-and-workflow-parity-local-and). Published 2026-10-08. Linear is canonical; the local files preserve the planning snapshot. All tickets were created in Backlog; no implementation was started.

23 tickets; 58 native blocking relations. Hosted adapters MP-18/19 additionally need positive feasibility evidence. A research result documenting missing vendor features does not clear that gate.

| Ticket | Linear | Owner | Blockers |
| --- | --- | --- | --- |
| [MP-01 — Freeze provider profiles and versioned execution contracts](issues/MP-01-freeze-provider-profiles-and-versioned-execution-contracts.md) | [OVE-64](https://linear.app/overtonbell/issue/OVE-64/mp-01-freeze-provider-profiles-and-versioned-execution-contracts) | Integrator | None |
| [MP-02 — Wire production launch inputs and the mission-chain relay](issues/MP-02-wire-production-launch-inputs-and-the-mission-chain-relay.md) | [OVE-65](https://linear.app/overtonbell/issue/OVE-65/mp-02-wire-production-launch-inputs-and-the-mission-chain-relay) | Integrator | MP-01 |
| [MP-03 — Extend capability projections and hosted environment materialization](issues/MP-03-extend-capability-projections-and-hosted-environment-materialization.md) | [OVE-66](https://linear.app/overtonbell/issue/OVE-66/mp-03-extend-capability-projections-and-hosted-environment) | Environment | MP-01 |
| [MP-04 — Generalize explicit workspace leases, worktrees and snapshots](issues/MP-04-generalize-explicit-workspace-leases-worktrees-and-snapshots.md) | [OVE-67](https://linear.app/overtonbell/issue/OVE-67/mp-04-generalize-explicit-workspace-leases-worktrees-and-snapshots) | Environment | MP-01 |
| [MP-05 — Qualify auth routes, subscription usage and account capabilities](issues/MP-05-qualify-auth-routes-subscription-usage-and-account-capabilities.md) | [OVE-68](https://linear.app/overtonbell/issue/OVE-68/mp-05-qualify-auth-routes-subscription-usage-and-account-capabilities) | Environment | MP-01 |
| [MP-06 — Harden shared session ownership, dispatch recovery and interventions](issues/MP-06-harden-shared-session-ownership-dispatch-recovery-and-interventions.md) | [OVE-69](https://linear.app/overtonbell/issue/OVE-69/mp-06-harden-shared-session-ownership-dispatch-recovery-and) | Runtime | MP-01, MP-04 |
| [MP-07 — Implement the local Claude Agent SDK lane](issues/MP-07-implement-the-local-claude-agent-sdk-lane.md) | [OVE-70](https://linear.app/overtonbell/issue/OVE-70/mp-07-implement-the-local-claude-agent-sdk-lane) | Runtime | MP-03, MP-05, MP-06 |
| [MP-08 — Implement the local Codex app-server lane](issues/MP-08-implement-the-local-codex-app-server-lane.md) | [OVE-71](https://linear.app/overtonbell/issue/OVE-71/mp-08-implement-the-local-codex-app-server-lane) | Runtime | MP-03, MP-05, MP-06 |
| [MP-09 — Close Cursor local/cloud parity and qualification gaps](issues/MP-09-close-cursor-local-cloud-parity-and-qualification-gaps.md) | [OVE-72](https://linear.app/overtonbell/issue/OVE-72/mp-09-close-cursor-localcloud-parity-and-qualification-gaps) | Runtime | MP-03, MP-06 |
| [MP-10 — Execute Human Gate nodes and review-with-feedback paths](issues/MP-10-execute-human-gate-nodes-and-review-with-feedback-paths.md) | [OVE-73](https://linear.app/overtonbell/issue/OVE-73/mp-10-execute-human-gate-nodes-and-review-with-feedback-paths) | Environment | MP-01, MP-02 |
| [MP-11 — Implement native approval bindings and governed MCP fallback](issues/MP-11-implement-native-approval-bindings-and-governed-mcp-fallback.md) | [OVE-74](https://linear.app/overtonbell/issue/OVE-74/mp-11-implement-native-approval-bindings-and-governed-mcp-fallback) | Environment | MP-06, MP-10 |
| [MP-12 — Wire continuation and independent compaction policies into workflows](issues/MP-12-wire-continuation-and-independent-compaction-policies-into-workflows.md) | [OVE-75](https://linear.app/overtonbell/issue/OVE-75/mp-12-wire-continuation-and-independent-compaction-policies-into) | Runtime | MP-02, MP-06 |
| [MP-13 — Extend event projections, native frames and subordinate lineage](issues/MP-13-extend-event-projections-native-frames-and-subordinate-lineage.md) | [OVE-76](https://linear.app/overtonbell/issue/OVE-76/mp-13-extend-event-projections-native-frames-and-subordinate-lineage) | Events | MP-01 |
| [MP-14 — Add scoped mission Socket.IO handlers with durable replay](issues/MP-14-add-scoped-mission-socket-io-handlers-with-durable-replay.md) | [OVE-77](https://linear.app/overtonbell/issue/OVE-77/mp-14-add-scoped-mission-socketio-handlers-with-durable-replay) | Events | MP-01, MP-13 |
| [MP-15 — Deliver bounded coordinator subscriptions and callbacks](issues/MP-15-deliver-bounded-coordinator-subscriptions-and-callbacks.md) | [OVE-78](https://linear.app/overtonbell/issue/OVE-78/mp-15-deliver-bounded-coordinator-subscriptions-and-callbacks) | Events | MP-13, MP-14 |
| [MP-16 — Qualify the Anthropic-hosted Claude Code control surface](issues/MP-16-qualify-the-anthropic-hosted-claude-code-control-surface.md) | [OVE-79](https://linear.app/overtonbell/issue/OVE-79/mp-16-qualify-the-anthropic-hosted-claude-code-control-surface) | Hosted | None |
| [MP-17 — Qualify the OpenAI-hosted Codex product control surface](issues/MP-17-qualify-the-openai-hosted-codex-product-control-surface.md) | [OVE-80](https://linear.app/overtonbell/issue/OVE-80/mp-17-qualify-the-openai-hosted-codex-product-control-surface) | Hosted | None |
| [MP-18 — Implement the qualified Claude-hosted lane](issues/MP-18-implement-the-qualified-claude-hosted-lane.md) | [OVE-81](https://linear.app/overtonbell/issue/OVE-81/mp-18-implement-the-qualified-claude-hosted-lane) | Hosted | MP-01, MP-03, MP-05, MP-06, MP-11, MP-12, MP-16 |
| [MP-19 — Implement the qualified Codex-hosted lane](issues/MP-19-implement-the-qualified-codex-hosted-lane.md) | [OVE-82](https://linear.app/overtonbell/issue/OVE-82/mp-19-implement-the-qualified-codex-hosted-lane) | Hosted | MP-01, MP-03, MP-05, MP-06, MP-11, MP-12, MP-17 |
| [MP-20 — Prove local and Cursor workflow parity across all three workflow forms](issues/MP-20-prove-local-and-cursor-workflow-parity-across-all-three-workflow-forms.md) | [OVE-83](https://linear.app/overtonbell/issue/OVE-83/mp-20-prove-local-and-cursor-workflow-parity-across-all-three-workflow) | Integrator | MP-02, MP-07, MP-08, MP-09, MP-10, MP-11, MP-12, MP-15, MP-22 |
| [MP-21 — Prove Claude and Codex hosted workflow parity](issues/MP-21-prove-claude-and-codex-hosted-workflow-parity.md) | [OVE-84](https://linear.app/overtonbell/issue/OVE-84/mp-21-prove-claude-and-codex-hosted-workflow-parity) | Integrator | MP-18, MP-19, MP-20 |
| [MP-22 — Prepare real local profiles and Temporal fallback operations](issues/MP-22-prepare-real-local-profiles-and-temporal-fallback-operations.md) | [OVE-85](https://linear.app/overtonbell/issue/OVE-85/mp-22-prepare-real-local-profiles-and-temporal-fallback-operations) | Integrator | MP-01, MP-02, MP-03, MP-05 |
| [MP-23 — Publish per-profile release evidence and reconcile documentation](issues/MP-23-publish-per-profile-release-evidence-and-reconcile-documentation.md) | [OVE-86](https://linear.app/overtonbell/issue/OVE-86/mp-23-publish-per-profile-release-evidence-and-reconcile-documentation) | Integrator | MP-20, MP-21, MP-22 |

## Computed dependency waves

| Wave | Tickets |
| --- | --- |
| 0 | MP-01, MP-16, MP-17 |
| 1 | MP-02, MP-03, MP-04, MP-05, MP-13 |
| 2 | MP-06, MP-10, MP-14, MP-22 |
| 3 | MP-07, MP-08, MP-09, MP-11, MP-12, MP-15 |
| 4 | MP-18, MP-19, MP-20 |
| 5 | MP-21 |
| 6 | MP-23 |

Waves are planning aids. Dispatch only after actual blockers are Done and evidence gates pass. MP-20 is the local/Cursor baseline; MP-21 and MP-23 retain the full provider-hosted objective.

## Existing work

Retain OVE-55 for Cursor qualification and OVE-59–61 for the existing owner mission fixtures. MP-02/09/22 supply remaining prerequisites; they do not erase those acceptance obligations. Do not reopen every completed fast-track issue or treat its Done state as proof of new hosted support.
