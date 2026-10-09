# [MP-16] Qualify the Anthropic-hosted Claude Code control surface

Linear: OVE-79 — https://linear.app/overtonbell/issue/OVE-79/mp-16-qualify-the-anthropic-hosted-claude-code-control-surface

Status: ready-for-agent — subject to blockers

**Owner:** Hosted
**Blocked by:** None
**Specification:** docs/specs/multi-provider-2026-10/RESEARCH.md

## Problem

Official hosted launch/message submission is documented but a full distributable observe/cancel/approval lifecycle has not been established.

## Work

- Verify exact documented/account-available API or CLI transport for launch, native identity, status, replay, cancel, follow-up and usage.
- Establish environment selection, configuration bootstrap, artifacts, human requests and subordinate visibility.
- Return a feature matrix with supported operation signatures and evidence, or precise vendor blockers; no private endpoint/browser/session-cookie fallback.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `docs/qualification/lanes/claude_cloud/ (new)`
- `adapters/claude_cloud/ protocol fixtures only`

## Acceptance

- [ ] Every required feature has a source/schema and observed finite drill result or an explicit blocked disposition.
- [ ] The evidence targets Anthropic-hosted Claude Code, not our own hosted SDK or another managed-agent product.
- [ ] A negative finding leaves MP-18 evidence-blocked even if this investigation is marked complete.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/RESEARCH.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
