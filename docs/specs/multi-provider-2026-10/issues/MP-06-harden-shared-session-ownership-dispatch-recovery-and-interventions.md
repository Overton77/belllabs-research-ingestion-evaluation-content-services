# [MP-06] Harden shared session ownership, dispatch recovery and interventions

Linear: OVE-69 — https://linear.app/overtonbell/issue/OVE-69/mp-06-harden-shared-session-ownership-dispatch-recovery-and

Status: ready-for-agent — subject to blockers

**Owner:** Runtime
**Blocked by:** MP-01, MP-04
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

Segmented activities need durable native dispatch ownership and control semantics across local process and hosted caller failures.

## Work

- Implement worker-owned session manager with fenced ownership and routing; observation segment end must not destroy an active local session.
- Persist create/send intents and native acknowledgements; reconcile ambiguous outcomes before retry; no blind resend.
- Exercise queued boundary delivery, explicit steering versus cancel-and-replace, pause disclosure and four cancel timestamps.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/execution/harness/`
- `adapters/temporal/activities/lane_turn.py`
- `adapters/temporal/workflows/operation.py`
- `adapters/postgres/lanes/execution_state.py`

## Acceptance

- [ ] Crash between native send and local receipt creates no duplicate turn; unrecoverable uncertainty parks in_doubt.
- [ ] A resumed observation activity never resends prompt; stale owner/generation cannot settle results.
- [ ] Cancel races preserve pre-fence effects, reject post-fence governed effects and block replacement until settled.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
