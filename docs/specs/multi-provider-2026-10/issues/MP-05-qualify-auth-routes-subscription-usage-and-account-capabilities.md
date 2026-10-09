# [MP-05] Qualify auth routes, subscription usage and account capabilities

Linear: OVE-68 — https://linear.app/overtonbell/issue/OVE-68/mp-05-qualify-auth-routes-subscription-usage-and-account-capabilities

Status: ready-for-agent — subject to blockers

**Owner:** Environment
**Blocked by:** MP-01
**Specification:** docs/specs/multi-provider-2026-10/SPEC-02-environments.md

## Problem

The owner's subscription priority must not be mistaken for permission to silently use API billing or for all-provider entitlement.

## Work

- Define auth profile and account/billing identity refs separately from model and environment selection.
- Preflight supported owner-local authentication versus API routes and hosted product sign-in; redact all credentials.
- Add quota/rate-limit classification, finite usage bounds and unknown cost handling without resetting mission counters.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `bootstrap/settings.py`
- `application/execution/ admission services`
- `adapters/provider_auth/ (new)`
- `deployments/`

## Acceptance

- [ ] Invalid/missing auth rejects before launch; no automatic subscription-to-API fallback.
- [ ] Provider fixture records identify the route without exposing tokens; billing unavailable stays unknown.
- [ ] Rate-limit reset waits are bounded by deadline and cancellation still reaches pending work.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-02-environments.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
