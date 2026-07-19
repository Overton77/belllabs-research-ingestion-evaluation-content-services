# 19 — Implement governed Prompt and Agent Skill catalog intake

**What to build:** Create the shared governed catalog intake path and prove it with separate Prompt Definition and Agent Skill families from immutable source capture through quarantine, inspection, evaluation, authorized promotion, exact rendering/materialization, and historical retention.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 07 — Implement isolated Run Workspace namespaces and materialization

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/19


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Implement shared catalog identity, revision, source snapshot, quarantine, evaluation, promotion, alias, revocation, and search-projection machinery before family-specific behavior.
- Normalize Prompt and Skill assets into separate strict family contracts and keep all instructional content classified as untrusted catalog data.
- Run active Skill inspection/evaluation only in disposable least-privilege sandboxes and materialize promoted bundles by exact digest.

## Verification approach

Drive a Prompt and a multi-file Skill from raw snapshot through quarantine, findings, evaluation, promotion, exact render/materialization, alias movement, and revocation.

## Explicit non-goals

- Public marketplace behavior, arbitrary repository installation in production, automatic promotion, final search weights, and using Skill metadata as authority.
- MCP probing and plugin components, which belong to ticket 20.

## Acceptance criteria

- [ ] PostgreSQL owns stable identities, immutable revisions/search projections, aliases, provenance, evaluations, promotion/revocation, source cursors, and authorization; object storage owns large snapshots and bundles.
- [ ] Every import captures an immutable raw snapshot before normalization, quarantine, static inspection, least-privilege evaluation where needed, and authorized promotion.
- [ ] Prompt revisions declare typed variables, format, trust classes, rendering rules, attachments, evaluation references, provenance, and digest.
- [ ] Rendering uses authorized typed state and records source revision, compiler, segment trust, redacted variable provenance, truncation, and rendered digest without secrets.
- [ ] Skill revisions preserve complete normalized manifests, source revision, bundle digest, metadata/body, dependencies, inspections/evaluations, compatibility, and requirements.
- [ ] Skill intake rejects traversal/archive escape, modified bytes, undeclared executables, secret patterns, unsafe dependencies, and requirements above policy.
- [ ] Promoted exact Skill bundles mount read-only with digest verification; declared tools remain metadata requirements, not grants.
- [ ] Alias movement, deprecation, and revocation affect future use without rewriting historical selections, bindings, or evidence.
- [ ] Duplicate/concurrent synchronization, snapshots, evaluations, promotions, renders, and bundle writes are idempotent and conflict-safe.

## Source basis

- Governed Agentic Capability Catalogs and Exact Bindings
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
