# [MP-01] Freeze provider profiles and versioned execution contracts

Linear: OVE-64 — https://linear.app/overtonbell/issue/OVE-64/mp-01-freeze-provider-profiles-and-versioned-execution-contracts

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** None
**Specification:** docs/specs/multi-provider-2026-10/ARCHITECTURE.md

## Problem

Reconcile the existing three runtime profiles with five projection profiles and add provider-hosted Claude/Codex without conflating placement or capability evidence.

## Work

- Define mc.lane_describe.v2, execution binding, approval correlation and mission/v2; retain exact v1 parsing/lowering and old compiled digests.
- Replace duplicated profile/hook vocabularies with checked derivations; distinguish implemented, qualified and account-enabled.
- Record a proposed/accepted-as-authorized ADR delta and reconcile sibling runtime annex; allocate common SQL migration slots.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `domain/execution/lanes.py`
- `domain/capabilities/host_support.py`
- `domain/frames/contracts.py`
- `domain/authoring/manifest.py`
- `contracts/schemas/`

## Acceptance

- [ ] Every runtime/projection/frame/manifest profile enum matches; required unsupported features reject with JSON pointers.
- [ ] Old v1 fixtures and saved workflow inputs remain readable; schema/SDK generation is reproducible.
- [ ] A reviewed shared contract fixture unblocks specialists without each editing shared enums.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/ARCHITECTURE.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
