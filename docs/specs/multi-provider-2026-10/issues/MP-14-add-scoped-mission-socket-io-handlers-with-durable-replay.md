# [MP-14] Add scoped mission Socket.IO handlers with durable replay

Linear: OVE-77 — https://linear.app/overtonbell/issue/OVE-77/mp-14-add-scoped-mission-socketio-handlers-with-durable-replay

Status: ready-for-agent — subject to blockers

**Owner:** Events
**Blocked by:** MP-01, MP-13
**Specification:** docs/specs/multi-provider-2026-10/SPEC-04-realtime.md

## Problem

Existing runtime socket handlers do not provide mission-wide authenticated subscriptions/replay over the public API.

## Work

- Compose AsyncServer/ASGIApp around the public FastAPI app with lifecycle preserved.
- Implement subscribe/unsubscribe/ack, snapshot/high-watermark replay, mission and frame streams, typed errors and thin mutation handlers.
- Add reauthorization, private rooms, slow-consumer bounds and optional Redis multi-process fanout.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `interfaces/socketio/ (new)`
- `application/streams/ (new)`
- `bootstrap/realtime.py (new)`
- `adapters/realtime/`

## Acceptance

- [ ] Two clients reconnect across commit/fanout races without missing durable events.
- [ ] Forged scope/room/cursor and expired credentials cannot leak data or apply commands.
- [ ] FastAPI/MCP startup and shutdown run once; slow clients cannot block workers or exhaust unbounded memory.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-04-realtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
