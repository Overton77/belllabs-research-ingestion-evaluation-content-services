# [MP-03] Extend capability projections and hosted environment materialization

Linear: OVE-66 — https://linear.app/overtonbell/issue/OVE-66/mp-03-extend-capability-projections-and-hosted-environment

Status: ready-for-agent — subject to blockers

**Owner:** Environment
**Blocked by:** MP-01
**Specification:** docs/specs/multi-provider-2026-10/SPEC-02-environments.md

## Problem

Skills/MCP/plugins/hooks have renderers but runtime loading and hosted setup ordering need an attested contract.

## Work

- Materialize pinned complete bundles; validate paths, interpreters, names and plugin dependency expansion.
- Generate exact local provider configs and evidence-backed per-event hook mappings; remove assumed Claude Python/TypeScript/Codex equivalence.
- Resolve provider-hosted environment/setup refs and attest preparation; reject missing pre-agent setup/config injection.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/agentic_components/projections.py`
- `application/agentic_components/materialization.py`
- `domain/capabilities/hooks.py`
- `application/environments/ (new)`

## Acceptance

- [ ] Digest-identical inputs render identical bytes; traversal/collision/unsupported required hook cases fail.
- [ ] Actual adapter discovery fixtures prove skills, MCP and subagents load; native config parsability alone is insufficient.
- [ ] Hosted profiles with no proven bootstrap route remain unqualified; no first-prompt self-install of mandatory controls.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-02-environments.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
