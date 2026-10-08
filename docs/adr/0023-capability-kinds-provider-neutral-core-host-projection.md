---
type: Decision Record
title: "Agent capabilities are provider-neutral catalog rows with per-lane host projections; plugins compose by exact pins"
description: "Skill bundles, MCP servers, hook scripts, subagent profiles and plugins are catalog capability kinds whose canonical record is provider-neutral; each lane profile renders them into its native files (host projection); a plugin is a pinned composition of other capabilities and never carries executable install hooks."
tags: [mission-control, adr, decision, capabilities]
status: accepted
source: fast-track interview 2026-10-07 (requirement 1); ADR-0020; docs/specs/fast-track-2026-10/research/codebase-map.md (two kind vocabularies, ComponentKind.PLUGIN without a binding); docs/research/2026-10-07-coding-lane-surfaces.md; docs/specs/fast-track-2026-10/research/seed-capabilities-and-formats.md
---

# Agent capabilities are provider-neutral catalog rows with per-lane host projections; plugins compose by exact pins

The catalog gains or confirms five agent-composition kinds: `skill_bundle` (the Agent Skills `SKILL.md` directory layout), `mcp_server` (with its `mcp_tool` children), `hook_script`, `subagent_profile` and `plugin`. Each row stores one provider-neutral core (identity, version, digest, description, inputs, side-effect class, secret references, search text) plus a `host_support` matrix over the lane profiles `deep_agents`, `cursor_local`, `cursor_cloud`, `claude_agent_sdk` and `codex`, and optional per-profile overlays for the few provider-specific fields (a Cursor subagent's `readonly` and `is_background`, a Claude hook's `matcher`). Materialization renders a row into a lane's native form through a host projection; nothing in a row is executed by the catalog itself. A plugin is a manifest of exact pins to other rows plus prompts and resources; installing a plugin installs its members and never runs a package hook.

We chose this over separate provider-specific kinds because the owner's capabilities are mostly general and a few are provider-specific, and one record with a support matrix keeps search, pinning and composition uniform while the projection layer absorbs format differences. The code's two kind vocabularies (`DefinitionKind` and `asset_version.kind`) are reconciled by a new migration and one mapping; `middleware` stops mapping to `hook`.

## Consequences

- One migration extends `asset_version.kind`; seeds for Tavily, Firecrawl, agent-browser, PubMed and EDGAR follow this shape.
- The existing `agentic_components` renderer becomes the host projection for every kind, not only MCP.
- A row whose `host_support` excludes the manifest's lane is a typed validation blocker, never a silent drop.
