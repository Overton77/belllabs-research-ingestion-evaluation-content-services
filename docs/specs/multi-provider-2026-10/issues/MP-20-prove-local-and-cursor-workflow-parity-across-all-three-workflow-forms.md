# [MP-20] Prove local and Cursor workflow parity across all three workflow forms

Linear: OVE-83 — https://linear.app/overtonbell/issue/OVE-83/mp-20-prove-local-and-cursor-workflow-parity-across-all-three-workflow

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** MP-02, MP-07, MP-08, MP-09, MP-10, MP-11, MP-12, MP-15, MP-22
**Specification:** docs/specs/multi-provider-2026-10/VALIDATION.md

## Problem

Adapters must demonstrate Stage Graph, GoalDirected and linked-mission behavior through the actual production path.

## Work

- Parameterize a shared Stage Graph, multi-iteration goal and two-member chain over Deep Agents/Cursor/Claude local/Codex local and qualified Cursor Cloud.
- Exercise review, queue, interrupt/cancel, context rollover, artifacts and mixed-provider handoffs.
- Preserve old acceptance tests and compare resulting receipts/events, not just final assistant text.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `tests/acceptance/mission_control/`
- `tests/integration/temporal/`
- `docs/qualification/lanes/`

## Acceptance

- [ ] V02/V04–19 pass at their required offline/DB/Temporal/live levels with exact version records.
- [ ] No fixture-only LaunchInputPort or fake catalog is used to claim production readiness.
- [ ] Report per-profile unsupported features and usage dispositions; do not call new Claude/Codex hosted support complete.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/VALIDATION.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
