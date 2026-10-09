# [MP-02] Wire production launch inputs and the mission-chain relay

Linear: OVE-65 — https://linear.app/overtonbell/issue/OVE-65/mp-02-wire-production-launch-inputs-and-the-mission-chain-relay

Status: ready-for-agent — subject to blockers

**Owner:** Integrator
**Blocked by:** MP-01
**Specification:** docs/specs/multi-provider-2026-10/SPEC-01-runtime.md

## Problem

mission start has no production LaunchInputPort and chain start intents have no composed production relay.

## Work

- Implement configuration-backed model/auth/environment/capability resolution into StageGraphRunInput and GoalDirectedRunInput.
- Compose one production launch author for HTTP/CLI/MCP and ChainLaunchInputPort; run ChainIntentRelay in the correct process.
- Preserve submit-without-start, frozen resolutions and idempotent outbox release; report missing bindings rather than fabricate models.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/authoring/manifest_submit.py`
- `bootstrap/manifests.py`
- `application/chains/relay.py`
- `bootstrap/api.py`
- `bootstrap/worker.py`

## Acceptance

- [ ] A deterministic locally bound manifest starts through real public entrypoints, not a fixture-injected LaunchInputPort.
- [ ] Real PostgreSQL/Temporal two-member chain starts the consumer once under duplicate relay delivery.
- [ ] Missing model/auth/environment bindings fail before provider dispatch.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
