# [MP-13] Extend event projections, native frames and subordinate lineage

Linear: OVE-76 — https://linear.app/overtonbell/issue/OVE-76/mp-13-extend-event-projections-native-frames-and-subordinate-lineage

Status: ready-for-agent — subject to blockers

**Owner:** Events
**Blocked by:** MP-01
**Specification:** docs/specs/multi-provider-2026-10/SPEC-04-realtime.md

## Problem

Mission aliases currently miss kernel events; new adapters need raw event fidelity and accurate child visibility.

## Work

- Add provider mapping fixtures, explicit unknown kinds, per-stream cursors and stable child/parent correlation.
- Implement public completion/opened aliases as derived notifications linked to canonical events.
- Compose full artifact body reading/transcript refresh; report visibility coverage and usage aggregation rules.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/frames/`
- `domain/frames/`
- `application/subscriptions/ filters`
- `adapters/postgres/frames/`
- `interfaces/http/transcript.py`

## Acceptance

- [ ] Requested run.completed/activation.completed/human_task.opened filters receive only correct derived transitions.
- [ ] Duplicate/reordered closing frames settle once; unknown/delta frames cannot terminalize work.
- [ ] Provider children, Agent Server children and linked missions remain distinct; no double-counted usage.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-04-realtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
