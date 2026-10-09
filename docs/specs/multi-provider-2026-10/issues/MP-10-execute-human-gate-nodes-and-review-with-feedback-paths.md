# [MP-10] Execute Human Gate nodes and review-with-feedback paths

Linear: OVE-73 — https://linear.app/overtonbell/issue/OVE-73/mp-10-execute-human-gate-nodes-and-review-with-feedback-paths

Status: ready-for-agent — subject to blockers

**Owner:** Environment
**Blocked by:** MP-01, MP-02
**Specification:** docs/specs/multi-provider-2026-10/SPEC-03-human-control.md

## Problem

HumanGateNode is an authoring shape today; run waits/runtime approval rows do not implement the general workflow node.

## Work

- Implement typed control activation lowering/execution using existing Human Task rows and resolution semantics.
- Add approve/reject/request-changes remediation with packet/version binding, deadlines and finite review rounds.
- Expose HTTP/MCP service parity and durable task events; no cognitive Activity slot held for human waiting.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `domain/programs/ control contracts`
- `application/programs/`
- `application/human_tasks/ (new or shared service)`
- `interfaces/http/human_tasks.py (new)`

## Acceptance

- [ ] Stage Graph and GoalDirected review waits survive worker/API restart and accept one attributed resolution.
- [ ] Concurrent/stale reviewers and changed artifact digests cannot reuse approval.
- [ ] Feedback reaches the declared remediation action without changing criteria or resetting budgets.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-03-human-control.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
