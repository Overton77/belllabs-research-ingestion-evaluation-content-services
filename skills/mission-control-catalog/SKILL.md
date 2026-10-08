---
name: mission-control-catalog
description: Search, inspect, pin and publish Mission Control capabilities (skill bundles, MCP servers and tools, hook scripts, subagent profiles, plugins, model and sandbox profiles) with missionctl catalog, and compose plugins from exact pins. Use when a manifest needs a capability, when a human asks what tools or skills exist for a lane, when a new skill bundle must be published, or when an external MCP server or skill is discovered and must be quarantined.
---

# Find, pin and publish capabilities

The catalog is the per-application set of admitted, versioned capabilities. Search ranks
candidates; only an exact pin (`id@version#sha256:…`) is authority. Capability kinds are
canonical in the catalog (ADR-0020); the ones you compose into missions are described in
[kinds-and-projections.md](references/kinds-and-projections.md).

## 1. Search

```text
missionctl catalog search "pubmed literature retrieval" --kind mcp_server --host deep_agents --json
```

Availability: FT-A8 (today use `catalog search --request-file search.json` with
`{query, kinds, hosts, limit}`). Search is hybrid: a lexical and a vector ranking fused after
grant and `host_support` filtering; `search_mode: lexical` means no embedding route was
configured. Each hit carries `ref`, `kind`, `score`, `rank_provenance`, `availability`
(`available | unavailable | gated`), `host_support` and a safe excerpt. Done when you have
one to three candidates whose `availability` is `available` for the mission's lane.

## 2. Inspect

```text
missionctl catalog inspect --request-file inspect.json --json
```

Read the capability's inputs, side-effect class, secret references (names only), tool list
(for an MCP server) and host overlays. Done when you can state what the capability does, what it
may touch and which lanes can run it.

## 3. Pin

```text
missionctl catalog pin mcp.tavily@0.2.22#sha256:… --json
```

Availability: FT-A8 (today `catalog resolve --request-file ref.json`). A pin names version and
digest; `latest` is never resolved after admission. Selecting an MCP server never selects every
tool: freeze an explicit tool allowlist in the manifest entry (`tools: [tavily_search]`).

## 4. Compose a plugin

A plugin is a pinned composition of other capabilities (skill bundles, MCP servers, hook
scripts, subagent profiles, prompts, resources). Write `mc.plugin_manifest.v1` with exact
member pins and publish it like a bundle (step 5). Installing a plugin installs its members; it
never runs a package install hook.

## 5. Publish a skill bundle

```text
missionctl catalog publish --dir ./skills/my-skill --kind skill_bundle --json
```

Availability: FT-A2. Custody first (bytes to the `capability-bundles` bucket under a
digest-addressed path), then registration (catalog row with the manifest digest). The response
is the exact pin. A bundle is a directory with `SKILL.md` (frontmatter `name` equal to the
directory name, `description`), optional `scripts/`, `references/`, `assets/`, and a
`manifest.json` of file digests. Done when `catalog inspect` on the returned pin shows every
file digest you published.

## 6. Discovery stays quarantined

`catalog discover --request-file discovery.json` (source `mcp` or `skills`) returns external
candidates. Record them, run `catalog inspect` on them, and request promotion; never attach a
candidate to a mission, run its install command or treat its `tools/list` as trust.

## Seeded set

`mcp.tavily`, `mcp.firecrawl`, `mcp.firecrawl-search`, `mcp.pubmed`, `mcp.biomcp`,
`mcp.edgartools`, `plugin.agent-browser`, `mcp.agent-browser`, `skill.agent-browser`,
`skill.biomcp`, `skill.edgartools`, `plugin.research-web`, and the five `skill.mission-control-*`
bundles. Availability: FT-A6 (MCP servers), FT-A7 (skills and plugins). Secret references these
need (`TAVILY_API_KEY`, `FIRECRAWL_API_KEY`, `NCBI_API_KEY`, `EDGAR_IDENTITY`) are deployment
inputs; a manifest names them, never carries them.
