---
type: Workflow
title: Capabilities, bundles and host projection
description: The capability kinds and their provider-neutral cores, digest-addressed bundle custody, pins, hybrid search with lexical fallback, host projection per lane profile, hook scripts and Kernel Hooks, subagent profiles and plugins. Seeds, skill bundles and what is not yet published are in capability-seeds.
tags: [mission-control, capabilities, catalog, search, skills, mcp, hooks, implementation]
---

# Capabilities, bundles and host projection

A [Capability](../../GLOSSARY.md) is an installed, digest-pinned, admitted catalog asset a
mission may bind. The catalog is canonical for kinds (ADR-0020). The fast-track packet adds
the agent-composition kinds, custody for directory bundles, hybrid search and one projection
layer that renders any capability into a lane's native files (ADR-0023 to ADR-0026,
SPEC-01). Search produces candidates, not authority; external discovery is quarantined and
inspection evidence precedes any admitted version.

## Implemented

**Kinds and host support.** Migration 0025 extends `asset_version.kind` with `skill_bundle`,
`mcp_server`, `mcp_tool`, `hook_script`, `subagent_profile`, `plugin` and `middleware`
(which no longer maps to `hook`; the legacy literals `skill`, `tool` and `hook` stay valid
for existing rows). Each row also carries `host_support` (`mc.capability_host_support.v1`:
per lane profile `supported`, `unsupported` or `unqualified`, plus an optional overlay for
fields with no provider-neutral meaning, such as a Cursor subagent's `readonly`) and
`secret_refs`, which are names matching `^[A-Z][A-Z0-9_]*$`, never values
(`domain/capabilities/host_support.py`). A row whose host support excludes a node's lane is
a typed validation blocker, never a silent drop.

**Pins.** `<capability_id>@<version>#sha256:<manifest_digest>` is the only reference a
committed revision carries; the parser rejects a pin missing any part
(`domain/capabilities/pins.py`). `POST /catalog/pin` and `missionctl catalog pin` return
exactly one pin for a query or `AMBIGUOUS_CAPABILITY` (422); `GET /catalog/pins/{pin}`
resolves one.

**Bundle custody.** A skill bundle, hook script directory or long subagent prompt is stored
once in the private `capability-bundles` bucket under
`<application>/<kind>/<capability_id>/<version>/<manifest_sha256>/<path>`; the manifest
(`mc.capability_bundle_manifest.v1`) pins every file's sha256 and size
(`domain/capabilities/bundles.py`). `application/capabilities/bundle_custody.py` runs three
resumable steps: upload without overwrite (an existing object counts only after its bytes
are re-read and compared), register the row `proposed` (promotion to `admitted` is an
operator decision), and materialize through short-lived signed URLs with every file verified
(mismatch is `CAPABILITY_DRIFT`). Routes: `POST /catalog/publish:prepare` and
`publish:complete`; `missionctl catalog publish --dir --kind`. The settings select the
backend (`CAPABILITY_BUNDLE_BACKEND`, default `local`, filesystem store
`adapters/capabilities/local_bundle_store.py`; `supabase` uses
`adapters/supabase_storage/bundles.py` with publisher or reader tokens, never the service
key). The configured API composes the catalog service without a custody service
(`CatalogService.custody` defaults to `None`, nothing in `bootstrap/` sets it), so
`publish:prepare`, `publish:complete` and rendering a bundle's bytes answer
`catalog_publish_unavailable` or `CapabilityNotFound`; bundles are published today by
`scripts/seeds_publish_bundles.py`. The common seed `mc.storage.capability-bundles@1.0.0` creates the bucket and policies
and reports `blocked` on plain PostgreSQL; applying it to Supabase is not done.

**Hybrid search.** `application/capabilities/capability_search.py` filters first (scope,
admission, kind, lane-profile host support, side-effect class), then fuses up to three ranked
lists with weighted reciprocal rank fusion (k=60; weights 1.0 full-text, 0.5 trigram over
names and exact tool names, 1.0 pgvector). Without an embedding route, or when the query
embedding fails, it runs lexical-only and reports `search_mode`; it never fails for a
missing route. Hits are re-verified against the authoritative definition digest. Migration
0026 makes `search_document.embedding` nullable, adds `pg_trgm` surfaces (the extension must
be provisioned in schema `extensions` before the release is applied) and a partial HNSW
index; `scripts/rebuild_capability_search_projection.py --lexical-only` builds the projection
without a paid embedding batch. `CAPABILITY_EMBEDDING_PROFILE` unset means lexical search.
The projection is rebuildable and never an authorization store
([persistence](persistence.md)).

**Host projection.** `application/agentic_components/projections.py::render_host_files` is a
pure renderer: resolved pins plus a lane profile, the mission instruction, the Context Packet
index and the kernel hook set produce the files the lane's provider reads, and a report of
anything not projected (`domain/agentic_components/projection.py`). `deep_agents`: in-process
objects and `skills/mission/...`, `memory/AGENTS.md`. `cursor_local` and `cursor_cloud`:
`AGENTS.md`, `.cursor/rules/mc-mission.mdc`, `.cursor/skills`, `.cursor/agents`,
`.cursor/mcp.json`, `.cursor/hooks.json`. Claude and Codex layouts (`CLAUDE.md`,
`.claude/...`, `.codex/config.toml`, `.codex/agents`) render too and are pinned by golden
fixtures under `tests/fixtures/projections/`, though no lane runs them. Secrets appear only as
the lane's reference syntax (`${env:NAME}` on Cursor). Kernel hooks come first in every hook
array.

**Hooks.** One provider-neutral `HookEvent` vocabulary (`domain/capabilities/hooks.py`:
`session_start`, `before_tool`, `before_shell`, `after_compaction`, `stop` and others) maps to
each profile's native hook or a tool-name matcher; an event a profile cannot run is reported
`unsupported_on_lane` (a blocker when the hook is `fail_closed`). A hook script reads
`mc.hook_input.v1` on stdin and writes `mc.hook_result.v1`; exit code 2 denies; results merge
`deny` over `defer` over `allow` (`contracts/hooks.py`). File lanes call
`.mission/hooks/run.py` (`application/agentic_components/hook_runner.py`, standard library
only, scrubbed environment); Deep Agents runs `HookScriptMiddleware`. Mission Control's own
four Kernel Hooks (stop fence, operation intent, frame capture, usage) are not catalog items
and no manifest can remove them ([lanes and harness](lanes-and-harness.md),
[cursor lane](cursor-lane.md)).

**Subagent profiles and plugins.** `mc.subagent_profile.v1` is defined once and projected to
`.cursor/agents/<name>.md`, `.claude/agents`, `.codex/agents` or a Deep Agents `SubAgent`
dict (`domain/capabilities/subagents.py`). `mc.plugin_manifest.v1` is a manifest of exact
member pins with no install command or hook; compile expands members into the node's
environment with provenance `plugin:<alias>`, `capability_plugin_member` records the
expansion, and a plugin is admissible only while every member is admitted
(`domain/capabilities/plugins.py`, migration 0025).

**Seeds.** The common, `biotech` and `ai-engineer` agent-capability seed bundles admit the MCP
servers, skill bundles and the policy-template hook; see
[capability seeds and skill bundles](capability-seeds.md). The operational catalog surfaces are
unchanged (`missionctl catalog list|resolve|search|discover|inspect|components`) plus the new
`pin`, `render` and `publish` ([interfaces](interfaces.md)).

## Not wired or not published

The production seeds lack many capabilities the three fixture manifests search for (blocker B2),
the worker pin file is narrow and drifted (B3, B4), and live bucket policies and executable
read-only mounts are unqualified. Detail, with the seed inventory and the repository skill
bundles, is in [capability seeds and skill bundles](capability-seeds.md).

# Citations

- Spec: [SPEC-01](../specs/fast-track-2026-10/SPEC-01-capabilities-catalog.md),
  [SPEC-08](../specs/fast-track-2026-10/SPEC-08-agent-skills.md),
  [seed capabilities and formats](../specs/fast-track-2026-10/research/seed-capabilities-and-formats.md).
- ADRs: [0013](../adr/0013-catalog-search-postgres-fulltext-first.md),
  [0020](../adr/0020-capability-kinds-canonical-in-catalog.md),
  [0023](../adr/0023-capability-kinds-provider-neutral-core-host-projection.md),
  [0024](../adr/0024-skill-bundle-custody-supabase-storage-digest-paths.md),
  [0025](../adr/0025-hybrid-capability-search-rrf-with-lexical-fallback.md),
  [0026](../adr/0026-hook-scripts-one-event-vocabulary-projected-per-lane.md),
  [0033](../adr/0033-mission-control-skill-is-a-router-over-specific-skill-bundles.md).
- Code: [catalog application service](../../src/mission_control/application/capabilities/catalog.py),
  [hybrid search](../../src/mission_control/application/capabilities/capability_search.py),
  [bundle custody](../../src/mission_control/application/capabilities/bundle_custody.py),
  [bundle manifests](../../src/mission_control/domain/capabilities/bundles.py),
  [host support](../../src/mission_control/domain/capabilities/host_support.py),
  [pins](../../src/mission_control/domain/capabilities/pins.py),
  [hook events](../../src/mission_control/domain/capabilities/hooks.py),
  [subagent profiles](../../src/mission_control/domain/capabilities/subagents.py),
  [plugins](../../src/mission_control/domain/capabilities/plugins.py),
  [hook contracts](../../src/mission_control/contracts/hooks.py),
  [host projection](../../src/mission_control/application/agentic_components/projections.py),
  [projection contracts](../../src/mission_control/domain/agentic_components/projection.py),
  [hook runner](../../src/mission_control/application/agentic_components/hook_runner.py),
  [catalog HTTP facade](../../src/mission_control/interfaces/http/catalog.py),
  [local bundle store](../../src/mission_control/adapters/capabilities/local_bundle_store.py),
  [bundle custody adapter](../../src/mission_control/adapters/capabilities/capability_bundles.py),
  [PostgreSQL bundle admission](../../src/mission_control/adapters/postgres/capability_bundles.py),
  [migration 0025](../../packages/mission-control-db-contract/component/migrations/0025_capability_kinds_and_host_support.sql),
  [migration 0026](../../packages/mission-control-db-contract/component/migrations/0026_search_projection_nullable_embedding.sql).
- Tests: [host projection](../../tests/unit/agentic_components/test_host_projection.py),
  [hook runner](../../tests/unit/agentic_components/test_hook_runner.py),
  [bundle custody](../../tests/unit/capability/test_bundle_custody.py),
  [search](../../tests/unit/capability/test_capability_search.py),
  [search evaluation set](../../tests/unit/capability/test_search_eval.py),
  [catalog CLI](../../tests/unit/capability/test_catalog_cli.py),
  [hybrid search in PostgreSQL](../../tests/integration/postgres/test_capability_search_hybrid.py),
  [bundle publish in PostgreSQL](../../tests/integration/postgres/test_capability_bundles_publish.py),
  [bundle materialization](../../tests/integration/postgres/test_skill_bundle_materialization.py).
- Canonical runtime skill: `skills/mission-control/`; coordinator helper
  `.agents/skills/mission-control-coordinator/`.
