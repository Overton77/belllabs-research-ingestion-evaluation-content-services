# [MP-08] Implement the local Codex app-server lane

Linear: OVE-71 — https://linear.app/overtonbell/issue/OVE-71/mp-08-implement-the-local-codex-app-server-lane

Status: ready-for-agent — subject to blockers

**Owner:** Runtime
**Blocked by:** MP-03, MP-05, MP-06
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

Codex file projection is not a lifecycle adapter; the product needs RPC/event/approval ownership.

## Work

- Build typed app-server transport from the pinned generated schema; separate requests, responses and server-originated requests.
- Map threads/turns/items, interrupts, expected-turn steering, history inspection and explicit compaction.
- Persist pending approval correlations; do not depend on unstable rollout parsing or assume hosted product access.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `adapters/codex/ (new)`
- `tests/unit/codex/ (new)`
- `tests/fixtures/provider_frames/codex/ (new)`

## Acceptance

- [ ] RPC ordering, lost responses, disconnect and concurrent completion/steer fixtures have deterministic outcomes.
- [ ] Queued instructions never accidentally become active-turn steering.
- [ ] Bounded local live drill proves approvals, subagent visibility and restart classification for the exact pin.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
