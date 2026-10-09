# [MP-11] Implement native approval bindings and governed MCP fallback

Linear: OVE-74 — https://linear.app/overtonbell/issue/OVE-74/mp-11-implement-native-approval-bindings-and-governed-mcp-fallback

Status: ready-for-agent — subject to blockers

**Owner:** Environment
**Blocked by:** MP-06, MP-10
**Specification:** docs/specs/multi-provider-2026-10/SPEC-03-human-control.md

## Problem

Provider permission callbacks, MCP elicitation and domain effect authorization must share durable tasks without assuming identical protocols.

## Work

- Persist approval origin/native identity/generation/input and policy digests; map provider approve/deny/question replies.
- Implement bounded callback wait and explicit expired/lost-request recovery.
- For owned effect tools add prepare/review/execute intent/receipt protocol; negotiate MCP elicitation and reject unenforceable requirements.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/human_tasks/`
- `application/execution/ approval ports`
- `interfaces/mcp/ governed gateway modules`
- `adapters/postgres/ approval correlation`

## Acceptance

- [ ] Restart/timeout never replays approval into a different request or modified arguments.
- [ ] Repeated pending/execute calls return stable pending state/receipt and never double-execute.
- [ ] Unsupported elicitation/gate coverage yields typed rejection; deny and cancel produce distinct outcomes.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-03-human-control.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
