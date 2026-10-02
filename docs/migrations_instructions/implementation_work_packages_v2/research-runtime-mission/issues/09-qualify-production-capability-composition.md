# RRM-009 — Qualify the CP-050 capability prerequisite

**What to build:** a reproducible production-shaped composition can execute bounded technical operations with real persistence, frozen search/browser skills and MCP tools, sandbox/workspace, governed subagents, and durable artifacts.

**Blocked by:** RRM-001, RRM-002, RRM-004 and RRM-013.
**Status:** blocked
**Branch:** `wp/rrm-009-capability-composition`
**Authority:** accepted CP-010/040/045 and CP-050 authorized slice; DA-001–015 and capability-binding requirements; REQ-CP-DA-004 (persistent saver, clarified), REQ-CP-DA-016 (`durability="sync"`), REQ-CP-DA-019, REQ-CP-EXEC-015 (Search Attribute registration) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-009/`; reference later CP-050 aggregate evidence, do not mark CP-050 accepted here

Provide the deployment-supplied worker activity composition rather than relying on test injection. Resolve exact skills/tools/CLI/MCP/checkpointer/storage/workspace dependencies and qualify their actual availability. The uncommitted component harness may only be used after a reviewed ownership/dependency commit; aiengineer integration is optional, not an invented prerequisite.

- [ ] Real application PostgreSQL, Mongo immutable definitions/bindings, object artifact storage and persistent LangGraph saver/store are composed through existing ports.
- [ ] Root/family/operation workers and queues use the canonical registries and exact placement; no parallel demo runtime.
- [ ] Search and browser skill bundles/tools are digest/revision pinned, mounted/disclosed and actually invoked by the bounded runtime agent.
- [ ] Research outbound access is provided by a qualified mediated service or explicit constrained egress placement; existing network-disabled isolation is not removed wholesale.
- [ ] Report/output workspace slots and artifact promotion use existing governed contracts; worker-local files are not the only durable output.
- [ ] One sync subagent (in-process) and one async subagent on the RRM-013 Agent Server are qualified inside the production worker composition. Each has explicit capability slices, reservations, dependency/result decisions and cancellation/reconnect behavior. A fake Agent Protocol client does not satisfy this.
- [ ] Credential references, exact filters/tool schemas, runtime/placement digests, capability invocation and observed usage appear in sanitized lineage.
- [ ] Small technical API-to-Temporal provider/service qualification proves availability and accepted persistence; no Qualia/GenerationLab definitions, searches or report runs.
- [ ] Document deployment prerequisites, reproducible launch commands, explicit live opt-in flags and cleanup; replay/recovery regressions pass.

Out of scope: complete governed capability catalogs, generalizing frameworks, production infrastructure rollout and company missions. CP-050 full tracer acceptance is deferred to the separate fixture session.
