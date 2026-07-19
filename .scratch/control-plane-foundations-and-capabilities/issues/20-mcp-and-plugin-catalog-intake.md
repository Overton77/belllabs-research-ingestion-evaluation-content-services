# 20 — Implement governed MCP and Plugin catalog intake

**What to build:** Extend governed intake to MCP Server and observed MCP Tool revisions plus Plugin Package, Installation, and independently authorized component revisions, using least-privilege probes and no ambient authority.

**Blocked by:**
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts
- 07 — Implement isolated Run Workspace namespaces and materialization
- 19 — Implement governed Prompt and Agent Skill catalog intake

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/20


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Reuse ticket 19's intake and governance pipeline while preserving separate MCP Server, MCP Tool, Plugin Package, Plugin Installation, and component identities.
- Generate tool inventory only from an isolated protocol handshake bound to an exact server revision; retain registry metadata as provenance, not observed truth.
- Authorize every plugin component independently and require qualified identities where package-local names could collide.

## Verification approach

Use a controlled fake MCP server, malicious probe fixtures, and disposable sandbox to cover schema drift, SSRF, ambient credentials, malformed tools, installation-without-authority, and retry.

## Explicit non-goals

- General package vulnerability management, automatic component enablement, arbitrary production installation, and defining every future plugin component type.
- Capability ranking and run compilation, which belong to ticket 21.

## Acceptance criteria

- [ ] MCP Server revisions preserve transport/connection recipe, secret references, network class, timeout/retry/approval/health policy, provenance, digest, and exposure ceiling.
- [ ] Only an isolated handshake/tools-list probe bound to an exact server revision creates tool revisions with observed schemas, annotations, digest, time, environment, and result.
- [ ] Probe drift, malformed schemas, duplicate names, unhealthy service, timeout, secret leakage, SSRF/internal access, and ambient credentials produce findings and no promotion.
- [ ] Plugin Package revision, scoped Installation, and effective component binding are separately versioned and audited.
- [ ] Every bundled prompt, Skill, MCP definition, hook, app, executable, and asset receives qualified identity, provenance, digest, compatibility, execution class, inspection, evaluation, authority, and promotion.
- [ ] Installation enables no component automatically; unknown files remain inert and name collisions resolve by qualified identity or fail.
- [ ] Promotion/revocation preserve evidence, scope, purpose, environment, conditions, expiry/review triggers, and historical bindings.
- [ ] Controlled fake-MCP and disposable-sandbox tests prove probe provenance, exact schemas, component isolation, idempotency, quarantine, and least privilege.

## Source basis

- Governed Agentic Capability Catalogs and Exact Bindings
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
