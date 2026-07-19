# 08 — Implement transactional durable artifact promotion

**What to build:** Provide the only path by which a workspace-local candidate becomes a durable, publicly consumable artifact with verified payload, immutable metadata, manifest linkage, producer provenance, and transactional event reference.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 07 — Implement isolated Run Workspace namespaces and materialization

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/8


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Model promotion as a retry-safe state machine spanning object payload staging, immutable metadata, manifest revision, validation, and transactional event publication.
- Centralize the visibility predicate so every consumer trusts the same admitted metadata-plus-payload-plus-manifest condition.
- Provide reconciliation inputs that distinguish safe completion from garbage collection without inferring admission from partial state.

## Verification approach

Run failure-injection integration tests at each state transition using test object storage, MongoDB, PostgreSQL outbox, and workspace manifests; query only through the public artifact seam.

## Explicit non-goals

- Workflow-specific artifact schemas, malware vendor selection, object multipart tuning, and general distributed transactions.
- Making arbitrary workspace files, metadata alone, or object writes alone visible.

## Acceptance criteria

- [ ] Promotion validates slot ownership, candidate identity/digest, media metadata, permissions, required checks, producer binding, output contract, and authority.
- [ ] A deterministic identity over run, semantic attempt, output slot, candidate, and digest makes exact retry return the prior artifact and conflicting content fail.
- [ ] Promotion records candidate, payload_staged, metadata_committed, admitted, rejected, and reconciliation_required states.
- [ ] Object storage owns payloads, MongoDB owns immutable metadata and manifests, and PostgreSQL owns transactional durable references and events.
- [ ] Only an admitted metadata revision whose digest matches the verified object and current manifest mapping is visible or obligation-eligible.
- [ ] Partial failures, orphaned payloads, incomplete metadata, and event retries remain invisible and can be reconciled or garbage-collected safely.
- [ ] Failure-injection tests prove retry safety, no early visibility, and no duplicate durable artifacts.

## Source basis

- Operation Runtime, Workspaces, Artifacts, and Snapshots
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
