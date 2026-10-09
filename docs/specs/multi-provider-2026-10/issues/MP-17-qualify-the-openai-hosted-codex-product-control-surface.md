# [MP-17] Qualify the OpenAI-hosted Codex product control surface

Linear: OVE-80 — https://linear.app/overtonbell/issue/OVE-80/mp-17-qualify-the-openai-hosted-codex-product-control-surface

Status: ready-for-agent — subject to blockers

**Owner:** Hosted
**Blocked by:** None
**Specification:** docs/specs/multi-provider-2026-10/RESEARCH.md

## Problem

Cloud CLI start/list does not prove provider-hosted turn control, streamed children or programmatic approval resumption.

## Work

- Inspect documented cloud CLI/API schema, account auth and environment identity for the exact product.
- Prove status/replay, follow-up, cancel, approvals, output custody, config materialization and continuation, or record unavailable operations.
- Keep app-server/local SDK and OpenAI Agents API as distinct products; do not substitute them.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `docs/qualification/lanes/codex_cloud/ (new)`
- `adapters/codex_cloud/ protocol fixtures only`

## Acceptance

- [ ] Each required hosted operation has evidence and a reproducible bounded probe or explicit vendor blocker.
- [ ] This desktop app's internal chat tools are not assumed to be an external Mission Control API.
- [ ] A negative finding leaves MP-19 evidence-blocked; limited start/list does not certify parity.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/RESEARCH.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
