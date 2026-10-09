# [MP-21] Prove Claude and Codex hosted workflow parity

Linear: OVE-84 — https://linear.app/overtonbell/issue/OVE-84/mp-21-prove-claude-and-codex-hosted-workflow-parity

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** MP-18, MP-19, MP-20
**Specification:** docs/specs/multi-provider-2026-10/VALIDATION.md

## Problem

The all-provider objective requires the actual hosted products to satisfy the same workflow requirements.

## Work

- Run the three common workflow forms against each qualified hosted product with finite approved caps.
- Verify remote setup, child visibility, human gates, queue/cancel, continuation and accepted artifact transfer.
- Record unsupported requirements as failures/blockers and preserve hosted environment/account evidence.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `tests/acceptance/mission_control/ hosted fixtures`
- `docs/qualification/lanes/claude_cloud/`
- `docs/qualification/lanes/codex_cloud/`

## Acceptance

- [ ] Both hosted products pass required lifecycle and environment matrix cells with reproducible evidence.
- [ ] Network/caller loss and stream-expiry scenarios reconcile without duplicate tasks.
- [ ] No local VM/SDK or unrelated managed-agent service is counted toward hosted parity.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/VALIDATION.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
