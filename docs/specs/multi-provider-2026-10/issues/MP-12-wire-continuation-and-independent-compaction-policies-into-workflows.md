# [MP-12] Wire continuation and independent compaction policies into workflows

Linear: OVE-75 — https://linear.app/overtonbell/issue/OVE-75/mp-12-wire-continuation-and-independent-compaction-policies-into

Status: ready-for-agent — subject to blockers

**Owner:** Runtime
**Blocked by:** MP-02, MP-06
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

ContinuationService exists but worker/family call sites do not fulfill requests; context pressure must not be treated as goal progress.

## Work

- Register seal/transfer activities and per-lane hydrators; persist resumable phase progress and mailbox holds.
- Implement native compaction where qualified and checkpoint/hydration fallback at safe boundaries.
- Separate context occupancy, turn limits, goal iterations and Temporal history rollover; patch workflow changes.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/context/continuation.py`
- `adapters/temporal/activities/continuation.py`
- `adapters/temporal/workflows/goal_directed.py`
- `adapters/temporal/workflows/stagegraph.py`

## Acceptance

- [ ] Crash at every continuation phase yields one activated generation with matching packet/workspace digests.
- [ ] Provider compaction does not advance goal iteration or reset budget/retry counters.
- [ ] Old histories replay; pending handlers/commands survive Continue-As-New; unknown occupancy is never invented.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
