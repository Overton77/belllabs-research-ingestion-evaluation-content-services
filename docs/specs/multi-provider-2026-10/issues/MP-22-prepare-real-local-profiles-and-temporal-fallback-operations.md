# [MP-22] Prepare real local profiles and Temporal fallback operations

Linear: OVE-85 — https://linear.app/overtonbell/issue/OVE-85/mp-22-prepare-real-local-profiles-and-temporal-fallback-operations

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** MP-01, MP-02, MP-03, MP-05
**Specification:** docs/specs/multi-provider-2026-10/VALIDATION.md

## Problem

Existing local fixture readiness has profile/catalog/pin/host blockers; local Temporal fallback needs explicit run binding.

## Work

- Expose exact missing model/auth/environment/capability inputs and compose approved real bindings without fixture stand-ins.
- Resolve current workspace pin/host readiness through authorized changes; document WSL/Linux local worker path.
- Document new-run local Temporal selection, durable local persistence and explicit reconciliation for active cloud-cluster outage; no automatic cross-cluster replay.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `deployments/`
- `bootstrap/preflight.py`
- `infra/`
- `docs/MISSION_CONTROL_LOCAL_API.md`
- `docs/specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md`

## Acceptance

- [ ] Deterministic public start and chain succeed on disposable real PostgreSQL/Temporal.
- [ ] Preflight rejects missing profile, unsupported OS, wrong DB release or environment digest before paid work.
- [ ] An outage drill never launches a second copy of an active mission in another cluster; known baseline failures reported separately.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/VALIDATION.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
