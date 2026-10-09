# [MP-23] Publish per-profile release evidence and reconcile documentation

Linear: OVE-86 — https://linear.app/overtonbell/issue/OVE-86/mp-23-publish-per-profile-release-evidence-and-reconcile-documentation

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** MP-20, MP-21, MP-22
**Specification:** docs/specs/multi-provider-2026-10/ARCHITECTURE.md

## Problem

Complete release must reflect actual parity, API contracts and operational limits rather than merged adapter code alone.

## Work

- Update knowledge, operator guide, schema/CLI/MCP discovery and describe matrices from accepted evidence.
- Reconcile sibling normative annex/ADRs and mark hosted limitations explicitly if full release remains blocked.
- Integrate checks, historical replay, DB release artifacts and per-profile qualification report; retain prior provenance.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `docs/knowledge/`
- `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`
- `docs/MISSION_CONTROL_LOCAL_API.md`
- `GLOSSARY.md`
- `docs/adr/`
- `AGENTS.md generated index`

## Acceptance

- [ ] Docs links/OKF/index and required code checks pass with passed/failed/blocked/unrun results recorded.
- [ ] Every qualified flag links to exact profile/version/account/environment proof.
- [ ] All-provider completion remains open until MP-21 passes; no deployment or paid authorization is inferred from documentation.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/ARCHITECTURE.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
