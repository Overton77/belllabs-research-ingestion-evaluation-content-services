# [MP-07] Implement the local Claude Agent SDK lane

Linear: OVE-70 — https://linear.app/overtonbell/issue/OVE-70/mp-07-implement-the-local-claude-agent-sdk-lane

Status: ready-for-agent — subject to blockers

**Owner:** Runtime
**Blocked by:** MP-03, MP-05, MP-06
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

Claude projections exist but no production Claude harness is registered.

## Work

- Implement all supported AgentHarness/SessionLane methods against a pinned Python SDK/CLI pair.
- Map native session/tool/result/subagent identities and callbacks; persist local session state; drain interrupted responses.
- Integrate permission binding ports and native-compaction observations honestly; no TypeScript-only callback assumptions.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `adapters/claude/ (new)`
- `tests/unit/claude/ (new)`
- `tests/fixtures/provider_frames/claude/ (new)`

## Acceptance

- [ ] Recorded fixture conformance covers start/observe/cancel/resume/history loss and unknown frames.
- [ ] Two-turn interrupt/replacement reads the intended response, not leftover interrupted messages.
- [ ] Bounded live local drill proves effective config and controls before profile qualification.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
