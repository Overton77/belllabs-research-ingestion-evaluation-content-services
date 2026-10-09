# [MP-09] Close Cursor local/cloud parity and qualification gaps

Linear: OVE-72 — https://linear.app/overtonbell/issue/OVE-72/mp-09-close-cursor-localcloud-parity-and-qualification-gaps

Status: ready-for-agent — subject to blockers

**Owner:** Runtime
**Blocked by:** MP-03, MP-06
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

Existing Cursor adapters require completion and qualified limits, not a replacement.

## Work

- Retain cursor-sdk 1.0.37 unless an explicit separately tested upgrade is needed.
- Complete cloud snapshot production, option rehydration, stream-expiry reconciliation and headless approval coverage reporting.
- Handle Windows local worker constraint with explicit supported WSL/Linux path; preserve cloud/local distinction.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `adapters/cursor/`
- `application/execution/harness/describe.py`
- `tests/qualification/ or existing lane drill paths`

## Acceptance

- [ ] Busy cloud agent never causes duplicate send or tight retry loop; persisted cursor resumes correctly.
- [ ] Cloud frames/status reconcile after disconnect/retention expiry; unknown usage remains unknown.
- [ ] Qualify supported controls and leave unproven hook/gate coverage rejected; cite OVE-55 evidence.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
