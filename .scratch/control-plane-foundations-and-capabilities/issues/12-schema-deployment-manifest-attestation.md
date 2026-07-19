# 12 — Implement Schema Deployment Manifest attestation

**What to build:** Make the graph-schema deployment process the sole issuer of immutable attestations that identify the exact directive SDL hash successfully deployed to one graph environment.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/12


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Place manifest issuance in the graph-schema deployment result boundary, not in workflow, materialization, or graph-client code.
- Keep deployment evidence, active attestation, revocation, supersession, and diagnostic introspection as distinct immutable records.
- Define environment identity explicitly enough that a valid manifest cannot be reused against another graph target.

## Verification approach

Drive successful, duplicate, conflicting, failed, rolled-back, revoked, superseded, and unauthorized deployment outcomes through the issuer service and strict compatibility query.

## Explicit non-goals

- Performing schema deployment itself, replacing attestation with introspection, and assuming the manifest digest identifies schema content.
- Graph queries or graph mutation.

## Acceptance criteria

- [ ] A manifest records environment, deployment identity, Schema Definition reference, deployed SDL hash, authorized issuer, occurrence time, digest, and lineage.
- [ ] Issuance occurs only after successful deployment and exact artifact verification; failed or rolled-back deployments leave the prior active attestation unchanged.
- [ ] The same deployment identity/environment/hash is idempotent; conflicting attestation creates a typed reconciliation condition.
- [ ] Revocation and supersession are immutable authorized records preserving history.
- [ ] Modular authoring inputs either deterministically produce/verify the authoritative directive SDL or block deployment before attestation.
- [ ] Compatibility outcomes cover exact match, missing, revoked, ambiguous, wrong environment, wrong definition, and hash mismatch.
- [ ] Introspection observations may diagnose drift but cannot replace attestation or convert a failed strict comparison into success.

## Source basis

- Reusable Schema Catalog, Deployment Manifest, and Schema Workspace Materialization

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
