# [MP-18] Implement the qualified Claude-hosted lane

Linear: OVE-81 — https://linear.app/overtonbell/issue/OVE-81/mp-18-implement-the-qualified-claude-hosted-lane

Status: needs-info — hosted capability evidence required

**Owner:** Hosted
**Blocked by:** MP-01, MP-03, MP-05, MP-06, MP-11, MP-12, MP-16
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

Implement the provider-hosted lane only after MP-16 establishes the required integration surface.

## Work

- Implement documented qualified methods behind SessionLane with environment/materialization receipts.
- Persist hosted native IDs independently of caller lifetime and reconcile ambiguous creates/sends.
- Map native controls, approvals, usage and child visibility with exact capability limits.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `adapters/claude_cloud/ (new)`
- `tests/fixtures/provider_frames/claude_cloud/ (new)`

## Acceptance

- [ ] Caller/worker restart does not start a duplicate cloud task.
- [ ] Admitted required features work against Anthropic-hosted Claude Code in a bounded drill.
- [ ] Any MP-16 missing operation remains a visible blocker; no self-hosted substitution or fake qualification.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
