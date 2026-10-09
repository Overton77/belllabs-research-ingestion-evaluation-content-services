# [MP-15] Deliver bounded coordinator subscriptions and callbacks

Linear: OVE-78 — https://linear.app/overtonbell/issue/OVE-78/mp-15-deliver-bounded-coordinator-subscriptions-and-callbacks

Status: ready-for-agent — subject to blockers

**Owner:** Events
**Blocked by:** MP-13, MP-14
**Specification:** docs/specs/multi-provider-2026-10/SPEC-04-realtime.md

## Problem

Coordinators need useful durable notifications without per-token wakeups or assumptions that MCP notifications awaken arbitrary agents.

## Work

- Extend existing outbox channels with bounded summary filters, durable inbox cursor and causation/recursion controls.
- Authenticate/sign webhook delivery and keep retries/dead-letter state separate from task execution.
- Support qualified connected MCP consumers and polling fallback; dispatch coordinator prompts only through admitted mailbox semantics.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/subscriptions/`
- `adapters/subscriptions/webhook.py`
- `interfaces/mcp/subscriptions.py`
- `application/execution/mailbox.py`

## Acceptance

- [ ] Offline coordinator recovers pending notifications once by ID; callback retries never rerun agent work.
- [ ] Notification-command feedback loops hit a defined recursion/rate bound.
- [ ] One run requiring approval produces actionable notification while token deltas remain off by default.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-04-realtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
