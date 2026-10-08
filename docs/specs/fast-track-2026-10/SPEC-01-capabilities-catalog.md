---
type: Specification
title: "SPEC-01: Agent capabilities in the catalog: kinds, custody, hybrid search, host projection, hook scripts, subagent profiles, plugins and seeds"
description: "Specification for ADR-0023, ADR-0024, ADR-0025 and ADR-0026: the five agent-composition capability kinds with a provider-neutral core and a host_support matrix, Skill Bundle custody in the capability-bundles bucket under digest paths, hybrid search with a lexical fallback, host projection of every kind into each lane profile's native files, the hook event vocabulary and hook script contracts, kernel hooks, subagent profiles, plugin composition, and the seed set (Tavily, Firecrawl, agent-browser, PubMed, BioMCP, edgartools). Tickets A1 to A8."
tags: [mission-control, spec, fast-track, capabilities]
---

# SPEC-01: Agent capabilities in the catalog

Decisions: [ADR-0023](../../adr/0023-capability-kinds-provider-neutral-core-host-projection.md), [ADR-0024](../../adr/0024-skill-bundle-custody-supabase-storage-digest-paths.md), [ADR-0025](../../adr/0025-hybrid-capability-search-rrf-with-lexical-fallback.md), [ADR-0026](../../adr/0026-hook-scripts-one-event-vocabulary-projected-per-lane.md); [ADR-0020](../../adr/0020-capability-kinds-canonical-in-catalog.md) still governs: the catalog, not this document, is the authority for kinds. Evidence: [research/seed-capabilities-and-formats.md](research/seed-capabilities-and-formats.md), [research/cursor-platform.md](research/cursor-platform.md) sections 1 and 2, [research/deepagents-middleware.md](research/deepagents-middleware.md) sections 2 and 6, [research/codebase-map.md](research/codebase-map.md) sections 1 and 2. Vocabulary: `GLOSSARY.md` (Capability, Capability Kind, Capability Pin, Skill Bundle, MCP Server, Plugin, Hook Script, Hook Event, Kernel Hook, Subagent Profile, Host Projection, Hybrid Search, Lane Profile, Materialization, Search Projection, Discovery).

## Problem Statement

The owner wants to store, search and compose the things an agent runs with: Agent Skills directories, MCP servers, plugins that bundle several of those, hook scripts that run before or after an agent action, and subagent definitions a main agent can delegate to. Today the catalog seeds only `skill`, `schema`, `profile`, `policy` and `blueprint`; the Python `DefinitionKind` and the SQL `asset_version.kind` disagree; `plugin_package` has no Definition class; nothing in the host-configuration renderer knows hooks, subagents, rules or plugins; `catalog search` returns 503 on the configured API because no embedding route is wired and the projection requires an embedding on every row; Skill Bundle bytes are read from `capability-bundles` but nothing uploads them. A coordinator cannot yet say "find me a PubMed capability that works on Cursor Cloud and pin it into this stage", and a lane cannot yet turn a catalog row into the files its provider reads.

## Solution

Five agent-composition kinds become first-class catalog rows: `skill_bundle`, `mcp_server` (with `mcp_tool` children), `hook_script`, `subagent_profile` and `plugin`. Every row has one provider-neutral core and a `host_support` matrix over the five lane profiles, with optional per-profile overlays. Skill Bundle bytes live once in the private `capability-bundles` bucket under digest-addressed paths and are fetched through short-lived signed URLs. Search is hybrid (full-text plus pgvector, fused by reciprocal rank fusion) after scope, admission, kind and host filtering, and degrades to lexical-only when no embedding route exists. One Host Projection module renders any row into the native files of the lane profile that will run it. Hook scripts run against one provider-neutral Hook Event vocabulary and one JSON contract, projected to Cursor, Claude Code and Codex command hooks and wrapped in a Deep Agents middleware; Mission Control's Kernel Hooks are composed first and cannot be removed. Plugins are manifests of exact pins. A seed set installs Tavily, Firecrawl, agent-browser, PubMed (two servers) and edgartools, so Mission 1 and Mission 3 have real capabilities to bind.

## User Stories

1. As a coordinator, I want to search "pubmed literature retrieval" and get ranked capabilities with their kind, version, digest and the lane profiles that support them, so that I can pin one into a manifest.
2. As a coordinator, I want search to tell me whether it ran hybrid or lexical-only, so that I know how to read the ranking.
3. As a coordinator, I want to filter search by kind (`mcp_server`, `skill_bundle`, `plugin`, `hook_script`, `subagent_profile`) and by lane profile, so that I never pin something the mission's lane cannot run.
4. As a coordinator, I want to resolve an alias or search to an exact Capability Pin (`id@version#sha256`) before submission, so that the committed revision never carries a mutable reference.
5. As an operator, I want to publish a Skill Bundle directory from my machine with one command and have it appear in the catalog as proposed, so that I can promote it after review.
6. As an operator, I want a bundle that was published once to be impossible to overwrite, so that a pinned run can never see different bytes.
7. As a worker, I want to download a bundle through a signed URL and verify every file against the manifest before mounting it read-only, so that drift is refused, not retried.
8. As a mission author, I want to declare an MCP server in a manifest by search or pin and have the lane render it into `.cursor/mcp.json`, `.mcp.json`, `.codex/config.toml` or an in-process adapter config, so that the agent gets the tools without me knowing the provider's file format.
9. As a mission author, I want secrets referenced by name in the catalog row and injected by the lane's native mechanism, so that no key ever appears in a rendered file, a frame or a transcript.
10. As a mission author, I want to attach a Hook Script to `before_shell` and `before_tool` and have it deny dangerous commands on every lane, so that policy travels with the mission, not with the provider.
11. As a mission author, I want to know at compile time which Hook Events a lane cannot run, so that a cloud mission does not silently lose its `session_start` hook.
12. As a mission author, I want to define a `verifier` subagent once and have it appear as `.cursor/agents/verifier.md` on Cursor and as a `SubAgent` dict on Deep Agents, so that delegation works the same on both.
13. As a mission author, I want a `plugin` that bundles Tavily, Firecrawl and the agent-browser skill, so that I pin one row for "web research" instead of three.
14. As a reviewer, I want to see a plugin's exact member pins, so that approving the plugin means approving known versions of each member.
15. As Mission Control, I want my own Kernel Hooks (stop fence, operation intent, usage, frame capture) to run before any catalog hook and to be unremovable from a manifest, so that governance does not depend on what an author declared.
16. As a Deep Agents lane, I want the same hook script that runs as a Cursor command hook to run inside my middleware with the same stdin and stdout JSON, so that one script works on both lanes.
17. As a Cursor Local lane, I want kernel hooks rendered as `failClosed` command hooks, so that an auto-approving headless run still cannot bypass the stop fence.
18. As a search maintainer, I want a fixed evaluation set of queries with expected capability ids, so that changing weights or embedding models is measured, not guessed.
19. As a projection maintainer, I want rows without an embedding to still rank lexically, so that a projection rebuild never blanks search.
20. As an agent using the mission-control-catalog skill, I want `missionctl catalog search --kind mcp_server --host cursor_cloud --json` to return typed hits with `pin` strings I can paste into a manifest, so that I do not have to construct digests by hand.
21. As an agent, I want `missionctl catalog inspect` of a plugin to list its members and their host support, so that I can judge fit before pinning.
22. As an application owner, I want the seeded capabilities admitted per application (`biotech` gets PubMed and BioMCP; both get Tavily, Firecrawl, agent-browser; `ai-engineer` gets edgartools), so that a catalog never offers what the application did not approve.
23. As an operator, I want the bucket and its policies created by a `mission-db` seed with no UPDATE or DELETE grant for writers, so that immutability is enforced by the database, not by convention.
24. As a compiler, I want a row whose `host_support` excludes the mission's lane to be a typed validation blocker (`CAPABILITY_UNAVAILABLE` with the lane named), so that it is never a silent drop.
25. As a developer, I want the two kind vocabularies (`DefinitionKind` and `asset_version.kind`) reconciled by one mapping and one migration, so that adding a kind is a one-place edit.
26. As a developer, I want `middleware` to stop mapping to the SQL kind `hook`, so that the word "hook" means Hook Script only.
27. As a Codex user, I want hook scripts rendered into `.codex/hooks.json` and MCP servers into `.codex/config.toml` with `env_vars` forwarding, so that Codex lanes (later) and Codex as an Agent Host today read the same catalog.
28. As a Claude Code user, I want subagent profiles that need `mcpServers` or `hooks` rendered into the project's `.claude/agents/` rather than a plugin, so that those fields are honoured.
29. As a Cursor user, I want instructions that have no SDK field rendered as an `alwaysApply` rule and `AGENTS.md`, so that the agent reads them on every turn.
30. As a security reviewer, I want hook stdin never to carry secrets and hooks that need the service to call back with a task-scoped token, so that a compromised script cannot exfiltrate credentials.
31. As a search user, I want exact tool names (`tavily_search`, `edgar_company`) to match lexically even when the embedding is weak, so that agents find tools by name.
32. As a catalog maintainer, I want discovery results (`npx skills find`, MCP registry) to stay quarantined candidates until inspected and published, so that search never returns an unreviewed external package as admitted.

## Implementation Decisions

### The five kinds and the provider-neutral core

Every capability row of an agent-composition kind carries the common core already defined by `CatalogAsset@1` (identity, immutable version, `manifest_digest`, title, description, tags, issuer, license, input and output schema refs, required grants, side-effect class, evidence refs, revocation state) plus:

- `host_support`: the `mc.capability_host_support.v1` matrix (below).
- `secret_refs`: named references the lane injects through its native mechanism; never values.
- `search_text`: the lexical surface (tool names, aliases, event names, prompt summary).
- A kind-specific body:

| Kind | Body | Notes |
| --- | --- | --- |
| `skill_bundle` | directory manifest (relative paths, per-file `sha256`, bytes), `computed_hash` (the Vercel `skills` CLI algorithm: files sorted by relative path with forward slashes, hashing path then bytes, skipping `.git` and `node_modules`), `SKILL.md` frontmatter (`name` must equal the directory name, 1 to 64 chars `a-z0-9-`; `description` 1 to 1024 chars; optional `license`, `compatibility`, `metadata`, `allowed-tools`), object prefix in the bucket | the existing `mc.skill_bundle.v1` manifest is the body; `computed_hash` is added so pins interoperate with `skills-lock.json` |
| `mcp_server` | `transport: stdio|streamable_http|sse`, `command`, `args`, `env_refs{name: secret_ref}`, `url`, `header_refs{header: secret_ref}`, `oauth{client_id_ref, client_secret_ref, scopes}`, `tools[]` (name, description, side-effect class, `read_only_hint`), `tool_allowlist` (optional), `package_pin` (`npm:tavily-mcp@0.2.22#sha512:…`, `pypi:biomcp-cli==0.9.1#sha256:…`, `oci:ghcr.io/…@sha256:…`), `tools_list_digest` for remote endpoints | each tool becomes an `mcp_tool` child row with `parent_server` (the projection CHECK already requires it) |
| `hook_script` | `events[]` (Hook Events), `matcher` (regex over the lane's matcher subject), `timeout_seconds`, `fail_closed: bool`, `side_effect_class`, script directory manifest (same shape as a skill bundle, bytes in the bucket), `entrypoint` (relative path), `interpreter: sh|bash|python|node`, `callback: none|service` | `callback: service` means the script needs the hook-callback endpoint and a task token |
| `subagent_profile` | `mc.subagent_profile.v1` (below) | |
| `plugin` | `mc.plugin_manifest.v1` (below) | members are exact pins; no executable install step exists |

`model_profile`, `sandbox_profile`, `workflow_template` and the other existing kinds are unchanged by this specification.

### Host support matrix

`host_support` is required on the five kinds. Lane profile identifiers are `deep_agents`, `cursor_local`, `cursor_cloud`, `claude_agent_sdk`, `codex`. A profile entry is `supported`, `unsupported` or `unqualified` (declared but no proof), plus an optional overlay. Overlays hold only fields that have no provider-neutral meaning: a Cursor subagent's `readonly` and `is_background`; a Claude hook's `matcher` syntax override and `async`; a Codex hook's `additionalContextLimit`; an MCP server's transport override (a stdio server that is exposed over streamable HTTP for cloud lanes). The compiler reads `host_support` to validate a manifest's lane per node (ADR-0034); the projection reads the overlay for the lane it renders.

### Custody in Supabase Storage

- Bucket `capability-bundles` is private, provisioned per application by a `mission-db` seed (`mc.storage.capability-bundles-1.0.0`): `storage.buckets` row with `public=false`, `file_size_limit` 50 MiB, `allowed_mime_types` null; `storage.objects` policies: INSERT for the publisher role where `bucket_id = 'capability-bundles' and (storage.foldername(name))[1] = <application>`; SELECT for the reader role on the same prefix; no UPDATE and no DELETE policies, so overwrite is possible only with the service key, which the runtime never holds.
- Path: `<application>/<kind>/<capability_id>/<version>/<manifest_sha256>/<relative file path>`. The manifest digest is the `sha256` over the canonical manifest JSON (sorted keys, no whitespace), which already pins every file digest, so a path collision implies identical bytes.
- Upload: standard upload for files up to 6 MiB, resumable (TUS, 6 MiB chunks, direct storage hostname) above that; `upsert` is never sent; an existing path returns `400 Asset Already Exists` and the publisher treats that as success only after re-downloading and comparing digests.
- Registration: after all objects exist, the catalog row is written with `status=proposed`, the object prefix and the manifest digest; promotion to `admitted` is the existing operator decision. Custody, registration and materialization are three resumable steps; a crash between them leaves unreferenced objects that an audited cleanup lists, never deletes automatically.
- Download: the service mints `create_signed_url(path, 300)` per file (or `create_signed_urls` for the manifest's file list); the worker downloads, verifies `sha256` and byte count per file, rejects traversal, symlinks and duplicate normalized paths (the existing `capability_bundles.py` rules), and mounts read-only. A mismatch is `CAPABILITY_DRIFT`.
- Hook script directories and the file parts of subagent profiles (prompt bodies longer than 8 KiB) use the same custody path under their own kind prefix.

### Hybrid search

- Candidate set first: scope (installation, application, tenant), `asset_version.status = admitted`, requested kinds, requested lane profiles (`host_support -> profile -> supported`), optional tags and side-effect classes. Only then rank.
- Lexical ranking: `ts_rank_cd(fts, websearch_to_tsquery('english', :q))` over the existing generated `fts` column (weights A for `title` and `logical_id`, B for `search_text`, C for `description`); add a `pg_trgm` similarity list over `logical_id`, tool names and aliases (`%` operator, threshold 0.3) so exact and near-exact tool names match.
- Vector ranking: `embedding <=> :query_embedding` with the existing HNSW cosine index, only for rows with a non-null embedding.
- Fusion: reciprocal rank fusion with `k = 60`, weights `lexical 1.0`, `trigram 0.5`, `vector 1.0`, each tunable per application through the existing projection generation metadata; cap candidates per list at `2 × max_results`, maximum 60.
- Verification: each hit is re-checked against the authoritative definition digest (existing `evaluate_selection` path).
- Modes: `hybrid` when an embedding route is configured and the query embedding succeeds; `lexical` otherwise. The response carries `search_mode`, and per hit the rank provenance (`lexical_rank`, `trigram_rank`, `vector_rank`, `fused_score`). The service never returns 503 for a missing embedding route; it returns 503 only when the projection itself is unavailable.
- Embedding route: a Model Profile row (`model_profile`, id `embedding.openai.text-embedding-3-small`, 1536 dimensions) referenced by application configuration; `embedding_model` and `embedding_dims` are recorded on every projected row so a model change triggers a re-embed instead of mixing spaces. The research note's 1024-dimension alternatives (bge-m3, voyage-4) are recorded as options; the first qualified route stays OpenAI because the adapter exists.
- Projection: `search_document.embedding` becomes nullable; the projection job writes lexical columns synchronously and embeds asynchronously in batches; the `active_generation` switch is unchanged.
- Agentic component search (`ComponentQuery`) stops substring matching in memory and queries the same projection with `kind` filters.
- Evaluation: `tests/fixtures/capability_search_eval.json` holds at least 25 queries with expected top-3 capability ids across kinds; a unit test asserts recall@3 ≥ 0.9 on the seeded catalog in lexical mode and a `common_db` test asserts it in hybrid mode with recorded embeddings.

### Host projection

`application/agentic_components/projections.py::render_host_files` becomes the single Host Projection for every kind and every lane profile. Input: a resolved set of capability rows (with overlays for the target profile), the mission's instruction text, the Context Packet index (SPEC-02) and the kernel hook set. Output: a list of `(relative_path, bytes, mode)` plus, for Deep Agents, in-process objects. Secrets are rendered as references in the lane's native syntax, never as values.

| Kind | deep_agents | cursor_local / cursor_cloud | claude_agent_sdk (and Claude Code as Agent Host) | codex |
| --- | --- | --- | --- | --- |
| `skill_bundle` | `/skills/<scope>/<name>/SKILL.md` on the backend; `skills=["/skills/kernel/", "/skills/mission/"]` (later source wins) | `.cursor/skills/<name>/` (also readable from `.agents/skills/`); cloud needs them committed to the branch | `.claude/skills/<name>/` | `.agents/skills/<name>/` |
| `mcp_server` | connection dict for `MCPAdapter` / `MultiServerMCPClient` resolved in-process to `tools=`; secrets read server-side | `.cursor/mcp.json` with `type`, `command`, `args`, `env: {"KEY": "${env:KEY}"}`, `url`, `headers: {"Authorization": "Bearer ${env:TOKEN}"}`; cloud: stdio `env` enters the VM so prefer remote endpoints with `headers`; local requires `setting_sources` including `project` | `.mcp.json` with `type`, `${VAR}` interpolation | `.codex/config.toml` `[mcp_servers.<name>]` with `env_vars = [...]`, `bearer_token_env_var`, `env_http_headers`; no string interpolation exists |
| `hook_script` | `HookScriptMiddleware` (below) | `.cursor/hooks.json` `{"version": 1, "hooks": {...}}` with the event mapping below; kernel hooks `failClosed: true` | `.claude/settings.json` `hooks` map (`type: command`, `args` exec form, `timeout`, `if`) | `.codex/hooks.json` (`command` handlers only; each hook must be trusted by hash, so the Validation Report flags `requires_trust`) |
| `subagent_profile` | `SubAgent{name, description, system_prompt, model?, tools?, skills?, permissions?, interrupt_on?}`; `readonly` → `FilesystemPermission(operations=[write, edit, execute], mode="deny")`; `background` → `AsyncSubAgent` only when an Agent Server is bound, else sync with a report entry | `.cursor/agents/<name>.md` (frontmatter `name`, `description`, `model`, `readonly`, `is_background`, body = prompt); also inline `AgentOptions.agents` when the profile is secret-free and needs neither `readonly` nor `is_background`; cloud `customSubagents` cap 20 | `.claude/agents/<name>.md` (`name`, `description`, `tools`, `model`, `permissionMode`, `skills`, `mcpServers`, `hooks`, `background`); project scope, never plugin scope, when `mcpServers`, `hooks` or `permissionMode` are set | `.codex/agents/<name>.md` (same frontmatter subset as Cursor; Cursor itself also reads this path) |
| `plugin` | members expanded then projected as above | members expanded; no Cursor plugin manifest is written (marketplace review is out of scope) | members expanded; optionally also `.claude-plugin/plugin.json` with `skills`, `agents`, `hooks`, `mcpServers` for Claude Code hosts | members expanded |
| instructions and packet | `system_prompt` plus `memory=["/memory/AGENTS.md"]` | `AGENTS.md` at workspace root plus `.cursor/rules/mc-mission.mdc` with `alwaysApply: true` | `CLAUDE.md` or `system_prompt` append | `AGENTS.md` (32 KiB cap applies) |

Path rules: normalize POSIX, reject traversal and duplicate normalized paths, never write into read-only seeds; the renderer is pure and deterministic (same inputs, same bytes), which is what `tests/unit/agentic_components/test_harness.py` already asserts for MCP and now asserts for every kind.

### Hook event vocabulary and lane mapping

| Hook Event | deep_agents (`HookScriptMiddleware`) | cursor (`hooks.json`) | claude_agent_sdk | codex |
| --- | --- | --- | --- | --- |
| `session_start` | `before_agent` | `sessionStart` (local only) | `SessionStart` | `SessionStart` |
| `session_end` | `after_agent` | `sessionEnd` (local only) | `SessionEnd` | `SessionEnd` |
| `before_prompt` | `before_agent` (first turn) | `beforeSubmitPrompt` | `UserPromptSubmit` | `UserPromptSubmit` |
| `before_model` | `before_model` | unsupported | unsupported | unsupported |
| `after_model` | `after_model` | `afterAgentResponse` (observe only) | unsupported | unsupported |
| `before_tool` | `wrap_tool_call` (pre) | `preToolUse` | `PreToolUse` | `PreToolUse` |
| `after_tool` | `wrap_tool_call` (post) | `postToolUse` | `PostToolUse` | `PostToolUse` |
| `after_tool_failure` | `wrap_tool_call` (exception) | `postToolUseFailure` | `PostToolUseFailure` | unsupported |
| `before_shell` | `wrap_tool_call` with tool name matcher `shell|execute` | `beforeShellExecution` | `PreToolUse` matcher `Bash` | `PreToolUse` matcher `Bash` |
| `after_shell` | `wrap_tool_call` (post, same matcher) | `afterShellExecution` | `PostToolUse` matcher `Bash` | `PostToolUse` matcher `Bash` |
| `before_mcp` | `wrap_tool_call` matcher on MCP tool names | `beforeMCPExecution` (local only) | `PreToolUse` matcher `mcp__.*` | `PreToolUse` matcher `mcp__.*` |
| `after_file_edit` | `wrap_tool_call` (post, filesystem write tools) | `afterFileEdit` | `PostToolUse` matcher `Edit|Write` | `PostToolUse` matcher `apply_patch` |
| `before_compaction` | Mission Control summarization wrapper (pre) | `preCompact` (observe only) | `PreCompact` | `PreCompact` |
| `after_compaction` | summarization wrapper (post) | unsupported | `PostCompact` | `PostCompact` |
| `subagent_start` | `wrap_tool_call` on the `task` tool (pre) | `subagentStart` | `SubagentStart` | `SubagentStart` |
| `subagent_stop` | `wrap_tool_call` on the `task` tool (post) | `subagentStop` | `SubagentStop` | `SubagentStop` |
| `stop` | `after_agent` | `stop` | `Stop` | `Stop` |

An event marked unsupported for the mission's lane produces `unsupported_on_lane` in the Validation Report with the hook id and event; the mission still compiles unless the hook declares `required_events`.

Result semantics per lane: `deny` → Cursor `permission: "deny"` or exit 2, Claude `permissionDecision: "deny"`, Codex `permissionDecision: "deny"`, Deep Agents a `ToolMessage(status="error")` returned without calling the handler. `defer` → Claude `permissionDecision: "defer"` (`pause_at_tool_gate`), Deep Agents `interrupt()` through `HumanInTheLoopMiddleware`, Cursor and Codex `deny` (Cursor accepts `ask` but does not enforce it headless; Codex marks `ask` failed). `updated_input` → Cursor `updated_input`, Claude and Codex `updatedInput`, Deep Agents `request.tool_call["args"]` replacement. `additional_context` → Cursor `additional_context` (post-tool) or `agent_message`, Claude and Codex `additionalContext`, Deep Agents an appended `HumanMessage` or `ToolMessage` suffix. `message` → Cursor `user_message`, Claude and Codex `systemMessage`, Deep Agents a frame only.

### Kernel hooks

Composed by Mission Control for every session, in this fixed order, before any catalog hook: (1) `mc.stop_fence` on `before_tool`, `before_shell`, `before_mcp`, `subagent_start`: denies when a Stop Fence exists for the run generation; (2) `mc.operation_intent` on the same events: writes the Operation Intent keyed on the tool-call id before returning allow, denies when the side-effect class is not allowed by the binding; (3) `mc.frame_capture` on every event: records the invocation as a Provider Frame (SPEC-03); (4) `mc.usage` on `after_model`, `after_tool`, `stop`: accounts usage where the lane exposes it. On Cursor they are command hooks with `failClosed: true` whose command is `python .mission/hooks/kernel.py <event>` calling `POST /v1/internal/hook-callback` with the task-scoped token from `.mission/hooks/token` (mode 0600, minted per attempt, revoked at end_session); on Deep Agents they are the first middleware in the list; on Claude and Codex they are command hooks first in each event array. The manifest schema has no field that removes or reorders them; the renderer appends catalog hooks after them.

### HookScriptMiddleware (Deep Agents)

One `AgentMiddleware` subclass per attempt, constructed with the ordered list of resolved hook scripts (kernel first). `before_agent`, `before_model`, `after_model`, `after_agent` and `wrap_tool_call` build the `mc.hook_input.v1` JSON for the mapped event, run matching scripts in order as subprocesses in the attempt's workspace (interpreter from the row, `timeout_seconds`, stdin JSON, stdout JSON, exit code 2 = deny), merge results (`deny` beats `defer` beats `allow`; `additional_context` concatenates; last `updated_input` wins) and apply them. `wrap_tool_call` covers `before_tool`, `after_tool`, `after_tool_failure`, `before_shell`, `after_shell`, `before_mcp`, `after_file_edit`, `subagent_start` and `subagent_stop` by matching `request.tool_call["name"]`. Compaction events come from Mission Control's `MissionSummarizationMiddleware`, a subclass of the deepagents `SummarizationMiddleware` keeping its `.name` so it replaces the default in place, which emits `before_compaction` before summarizing and `after_compaction` with the `_summarization_event` afterwards. The precedent is `deepagents-code`'s `ServerHooksMiddleware`; Mission Control runs scripts directly instead of through `interrupt()` because the worker owns the process.

### Subagent profiles

`mc.subagent_profile.v1`: `name` (1 to 64 chars `a-z0-9-`, not a built-in name on any lane: `explore`, `shell`, `bash`, `browser`, `debug`, `computerUse`, `cursorGuide`, `general-purpose`), `description` (1 to 1000 chars; the delegation trigger), `prompt` (1 to 8192 chars inline, or a bundle ref), `model: inherit | <model_profile ref>`, `tools[]` (capability refs or lane tool names), `skills[]`, `mcp_servers[]` (capability refs), `readonly: bool`, `background: bool`, `context_mode: isolated | fork`, `interrupt_on[]`, `max_turns`. Projection rules are in the table above; a profile with `background: true` on `deep_agents` without an Agent Server binding is projected synchronous and reported.

### Plugins

`mc.plugin_manifest.v1`: `members[]` of `{pin, role: skill | mcp_server | hook | subagent | prompt | resource, overlay?}`, `prompts[]` (inline text or bundle refs), `resources[]` (bundle refs), `host_support` computed as the intersection of members' support (a member unsupported on a profile makes the plugin unsupported there unless the member is marked `optional`). Compile expands members into the node's environment; a plugin never carries an install command, a postinstall script or a marketplace reference. `capability_plugin_member` records the expansion so inspection and review see exact pins.

### Seeds

Seed bundle `mc.catalog.agent-capabilities-1.0.0.json` (common) with per-application admission in `mc.app.<app>.agent-capabilities-1.0.0.json`. Pins are exact at seed time; `tools_list_digest` is recorded from a captured `tools/list` fixture under `tests/fixtures/mcp/<server>.tools.json`.

| Capability id | Kind | Pin | Secret refs | Tools (count) | host_support (supported) | Applications |
| --- | --- | --- | --- | --- | --- | --- |
| `mcp.tavily` | mcp_server | stdio `npm:tavily-mcp@0.2.22` (Node ≥ 20); remote `https://mcp.tavily.com/mcp/` with `Authorization: Bearer` | `TAVILY_API_KEY` | `tavily_search`, `tavily_extract`, `tavily_crawl`, `tavily_map`, `tavily_research` (5) | all five (cloud lanes via remote) | biotech, ai-engineer |
| `mcp.firecrawl` | mcp_server | stdio `npm:firecrawl-mcp@3.28.2`; remote `https://mcp.firecrawl.dev/v2/mcp` Bearer; `FIRECRAWL_NO_SEARCH_FEEDBACK=1`, `FIRECRAWL_NO_ENDPOINT_FEEDBACK=1` | `FIRECRAWL_API_KEY` | `firecrawl_scrape`, `firecrawl_map`, `firecrawl_search`, `firecrawl_crawl`, `firecrawl_check_crawl_status`, `firecrawl_parse`, `firecrawl_agent`, `firecrawl_agent_status`, `firecrawl_interact`, `firecrawl_interact_stop`, `firecrawl_research_search_papers`, `firecrawl_research_read_paper`, `firecrawl_research_inspect_paper`, `firecrawl_research_related_papers`, `firecrawl_research_search_github`, `firecrawl_developer_search`, `firecrawl_gov_search`, `firecrawl_find_tools`, `firecrawl_credit_usage`, monitor tools (8) (deprecated `firecrawl_extract` excluded by `tool_allowlist`) | all five | biotech, ai-engineer |
| `mcp.agent-browser` | mcp_server | `agent-browser mcp --tools core` from `npm:agent-browser@0.38.2`; precondition `agent-browser install` recorded as an Environment Profile requirement | none | `agent_browser_open`, `_snapshot`, `_click`, `_fill`, `_type`, `_press`, `_wait_for_selector`, `_screenshot`, `_get_url`, `_eval`, `_close`, `agent_browser_tools_profiles` | deep_agents (sandbox with Chrome), cursor_local, claude_agent_sdk, codex; cursor_cloud `unqualified` | biotech, ai-engineer |
| `skill.agent-browser` | skill_bundle | `vercel-labs/agent-browser` skill at the commit matching 0.38.2, `computed_hash` recorded | none | stub that runs `agent-browser skills get core` | all five | biotech, ai-engineer |
| `plugin.web-research` | plugin | members `mcp.tavily`, `mcp.firecrawl`, `skill.agent-browser`, `mcp.agent-browser` (optional) | union | — | intersection | biotech, ai-engineer |
| `mcp.pubmed` | mcp_server | stdio `npm:@cyanheads/pubmed-mcp-server@2.10.20` (Node ≥ 24 or Bun ≥ 1.4), `MCP_TRANSPORT_TYPE=stdio`; cloud lanes: self-hosted streamable HTTP, never the author's public endpoint | `NCBI_API_KEY`; config `NCBI_ADMIN_EMAIL`, `UNPAYWALL_EMAIL` | `pubmed_search_articles`, `pubmed_fetch_articles`, `pubmed_fetch_fulltext`, `pubmed_europepmc_search`, `pubmed_europepmc_fetch`, `pubmed_format_citations`, `pubmed_find_related`, `pubmed_spell_check`, `pubmed_lookup_mesh`, `pubmed_lookup_citation`, `pubmed_convert_ids` (11) | deep_agents, cursor_local, claude_agent_sdk, codex; cursor_cloud via self-hosted HTTP `unqualified` | biotech |
| `mcp.biomcp` | mcp_server | stdio `pypi:biomcp-cli==0.9.1` (`biomcp serve`) or `oci:ghcr.io/genomoncology/biomcp@sha256:…`; HTTP `biomcp serve-http` | optional `NCBI_API_KEY`, `S2_API_KEY`, `OPENFDA_API_KEY`, `NCI_API_KEY`, `ONCOKB_TOKEN`, `ALPHAGENOME_API_KEY`, `DISGENET_API_KEY` | `search`, `get`, `variant_normalize_car`, `variant_erepo`, `gene_cspec`, `variant_articles`, `biomcp` (7, all read-only) | same as `mcp.pubmed` | biotech |
| `skill.biomcp` | skill_bundle | output of `biomcp skill install` at 0.9.1, `computed_hash` | none | — | all five | biotech |
| `mcp.edgartools` | mcp_server | stdio `uvx --from "edgartools[ai]==5.61.1" edgartools-mcp`; HTTP `--transport streamable-http` | `EDGAR_IDENTITY` (an identity string, stored as a secret ref for uniformity) | `edgar_company`, `edgar_search`, `edgar_screen`, `edgar_text_search`, `edgar_monitor`, `edgar_filing`, `edgar_read`, `edgar_notes`, `edgar_trends`, `edgar_compare`, `edgar_ownership`, `edgar_fund`, `edgar_proxy` (13) | all five (cloud via self-hosted HTTP `unqualified`) | ai-engineer |
| `skill.edgartools` | skill_bundle | output of `edgar.ai.install_skill()` at 5.61.1, `computed_hash` | none | domains core, financials, holdings, ownership, reports, xbrl | all five | ai-engineer |
| `hook.mc-policy-template` | hook_script | Mission Control-authored `scripts/hooks/policy_template/`; events `before_shell`, `before_tool`; `fail_closed: true` | none | denies `rm -rf`, `git push --force`, writes outside declared paths | all five | biotech, ai-engineer |
| `skill.mission-control*` | skill_bundle | the router and five bundles of SPEC-08 | none | — | all five | biotech, ai-engineer |

Rejected and recorded as such in the seed notes: `sec-edgar-mcp` (AGPL-3.0, unauthenticated HTTP), `pubmedmcp` (stale), unscoped `pubmed-mcp-server`, the author-hosted `pubmed.caseyjhand.com` endpoint for production, Firecrawl's key-in-URL legacy form, Tavily's key-in-query form.

### Discovery stays quarantined

`catalog discover` results (MCP registry, `npx skills find`) remain `capability_discovery_record` rows with `TrustStage.quarantined`; `catalog inspect` produces evidence; `catalog publish` is the only path to a `proposed` row; promotion to `admitted` is an operator decision. Search filters on `admitted` only. Nothing in this specification changes that.

### Vocabulary reconciliation

`DefinitionKind` gains `hook_script`, `subagent_profile` and `plugin` (replacing the reference-only `plugin_package`), keeps `skill`, `mcp_server`, `mcp_tool`, and maps: `skill → skill_bundle`, `mcp_server → mcp_server`, `mcp_tool → mcp_tool`, `hook_script → hook_script`, `subagent_profile → subagent_profile`, `plugin → plugin`, `middleware → middleware` (new SQL value; it no longer maps to `hook`). `CapabilityKind` gains `hook_script`, `subagent_profile`, `plugin`. `ComponentKind` gains `hook_script`, `subagent_profile` and a typed `plugin` binding; `AgenticComponentRelease` gets the matching typed bindings.

## Contracts

```json
// mc.capability_host_support.v1
{
  "schema_version": "mc.capability_host_support.v1",
  "profiles": {
    "deep_agents":      {"status": "supported|unsupported|unqualified", "overlay": {}, "evidence_ref": "..."},
    "cursor_local":     {"status": "supported", "overlay": {"readonly": true, "is_background": false}},
    "cursor_cloud":     {"status": "unqualified", "overlay": {"transport": "streamable_http", "url_ref": "..."}},
    "claude_agent_sdk": {"status": "supported", "overlay": {"matcher": "Bash", "async": false}},
    "codex":            {"status": "supported", "overlay": {"additional_context_limit": 5000}}
  }
}
```

```json
// mc.hook_input.v1 (stdin to every hook script on every lane)
{
  "schema_version": "mc.hook_input.v1",
  "event": "before_shell",
  "lane_profile": "cursor_local",
  "scope": {"installation_id": "...", "application_id": "...", "tenant_id": "..."},
  "run_id": "...", "activation_id": "...", "attempt_no": 1, "generation": 3,
  "harness_execution_id": "...", "native_session_ref": "agent-...", "native_turn_ref": "run-...",
  "tool": {"name": "shell", "call_id": "call_...", "input": {"command": "pytest -q"}, "side_effect_class": "workspace_write"},
  "workspace_root": "/work",
  "provider_payload_digest": "sha256:...",
  "callback": {"url": "http://127.0.0.1:8731/v1/internal/hook-callback", "token_path": ".mission/hooks/token"}
}
```

```json
// mc.hook_result.v1 (stdout of a hook script; exit 2 is equivalent to decision deny)
{
  "schema_version": "mc.hook_result.v1",
  "decision": "allow|deny|defer",
  "reason": "string, required when deny or defer",
  "updated_input": {"command": "pytest -q -x"},
  "additional_context": "string ≤ 10000 chars",
  "message": "string shown to the human where the lane supports it"
}
```

```json
// mc.subagent_profile.v1
{
  "schema_version": "mc.subagent_profile.v1",
  "name": "verifier",
  "description": "Validates completed work. Use proactively after any task is marked done.",
  "prompt": "You are a skeptical validator...",
  "model": "inherit",
  "tools": [], "skills": ["skill.mission-control-observe@0.2.0#sha256:..."], "mcp_servers": [],
  "readonly": true, "background": false, "context_mode": "isolated",
  "interrupt_on": [], "max_turns": 30
}
```

```json
// mc.plugin_manifest.v1
{
  "schema_version": "mc.plugin_manifest.v1",
  "members": [
    {"pin": "mcp.tavily@0.2.22#sha256:...", "role": "mcp_server"},
    {"pin": "mcp.firecrawl@3.28.2#sha256:...", "role": "mcp_server"},
    {"pin": "skill.agent-browser@0.38.2#sha256:...", "role": "skill"},
    {"pin": "mcp.agent-browser@0.38.2#sha256:...", "role": "mcp_server", "optional": true}
  ],
  "prompts": [], "resources": []
}
```

Search request additions (`POST /catalog/search`): `kinds[]`, `host_profiles[]`, `side_effect_classes[]`, `include_plugins_members: bool`. Response additions: `search_mode: hybrid|lexical`, per hit `pin`, `kind`, `host_support`, `rank_provenance {lexical_rank, trigram_rank, vector_rank, fused_score}`, `availability`.

Capability Pin string: `<capability_id>@<version>#sha256:<manifest_digest>`; the parser rejects any pin without all three parts.

## Persistence

`0025_capability_kinds_and_host_support.sql`:

- `ALTER TABLE mission_control.asset_version DROP CONSTRAINT <kind check>; ADD CONSTRAINT ... CHECK (kind IN ('skill_bundle','mcp_server','mcp_tool','hook_script','subagent_profile','plugin','blueprint','profile','schema','policy','workflow_template','operation_binding','middleware','model_route','skill','tool','hook'))` (old literals retained for existing rows; new writers use the new literals; a follow-up migration retires `skill`, `tool`, `hook` once no row carries them).
- `ALTER TABLE mission_control.asset_version ADD COLUMN host_support jsonb NOT NULL DEFAULT '{"schema_version":"mc.capability_host_support.v1","profiles":{}}'`, with a CHECK that `host_support->>'schema_version' = 'mc.capability_host_support.v1'`.
- `ALTER TABLE mission_control.asset_version ADD COLUMN secret_refs text[] NOT NULL DEFAULT '{}'`.
- `CREATE TABLE mission_control.capability_plugin_member (plugin_asset_id, plugin_version, member_asset_id, member_version, member_digest, role, optional bool, position int, PRIMARY KEY (plugin_asset_id, plugin_version, position))` with RLS forced and the same `mc.*` scope policies as `asset_version`.
- Trigger: a plugin row cannot be `admitted` while any member is not `admitted` or is revoked.

`0026_search_projection_nullable_embedding.sql`:

- `ALTER TABLE mission_control_search.search_document ALTER COLUMN embedding DROP NOT NULL; ADD COLUMN embedding_model text, ADD COLUMN embedding_dims int, ADD COLUMN host_profiles text[] NOT NULL DEFAULT '{}', ADD COLUMN side_effect_class text, ADD COLUMN aliases text[] NOT NULL DEFAULT '{}'`.
- `CREATE EXTENSION IF NOT EXISTS pg_trgm` (trusted extension) and `CREATE INDEX ... USING gin (logical_id gin_trgm_ops)`, plus a generated `name_surface text` column over `logical_id || aliases || tool names` with its own trigram index.
- `CREATE INDEX ... ON search_document USING gin (host_profiles)`.
- Partial HNSW index `WHERE embedding IS NOT NULL` replaces the existing one.
- Seed bundle `mc.storage.capability-bundles-1.0.0.json`: `storage.buckets` insert and the four `storage.objects` policies described above, applied by `mission-db seed-apply` and verified by `mission-db qualify`; `mission-db` refuses to apply it when the Storage schema is absent (local disposable clusters without Storage record it as `blocked`).

## Interfaces

CLI (`missionctl`): `catalog search --query TEXT [--kind K ...] [--host P ...] [--limit N] --json` (request file remains supported); `catalog publish --dir PATH --kind skill_bundle|hook_script [--capability-id ID] [--version V]` (uploads, registers `proposed`, prints the pin); `catalog pin --query TEXT --kind K --host P` (returns exactly one pin or a typed ambiguity error); `catalog inspect --pin PIN` (shows body, host support, plugin members); `catalog components` reads the shared projection.

HTTP below `/v1/applications/{application_id}/catalog`: `POST /search` (extended), `POST /publish` (multipart or signed-URL handshake: `POST /publish:prepare` returns signed upload URLs per file, `POST /publish:complete` registers), `GET /pins/{pin}`; existing routes unchanged.

MCP (coordinator server): `search_capabilities` gains `kinds`, `host_profiles`; `get_capability` returns host support and plugin members; new `pin_capability`.

Make: `make skills-manifest` (SPEC-08) and `make seeds-validate` (asserts every seed pin parses and every `tools_list_digest` matches its fixture).

## Insertion points

- Kinds and definitions: `src/mission_control/domain/authoring/contracts.py` (`DefinitionKind`, `CapabilityKind`, `CapabilityRequirement.validate_policy`, `Definition` union, new `HookScriptDefinition`, `SubagentProfileDefinition`, `PluginDefinition` in `src/mission_control/domain/capabilities/{hooks,subagents,plugins}.py`); `src/mission_control/adapters/postgres/control_plane/catalog_assets.py::ASSET_KIND`.
- Custody: `src/mission_control/adapters/supabase_storage/bundles.py` (`SupabaseCapabilityBundleStore` gains `upload_bundle`, `signed_urls`), `src/mission_control/adapters/capabilities/capability_bundles.py` (verification rules reused), `src/mission_control/adapters/postgres/capability_bundles.py` (registration), new `missionctl catalog publish` in `src/mission_control/interfaces/cli/main.py`, `packages/mission-control-db-contract/seeds/common/mc.storage.capability-bundles-1.0.0.json`.
- Search: `src/mission_control/application/capabilities/capability_search.py` (`search` branches on embedding availability; filters), `src/mission_control/adapters/postgres/capability/capability_search_repository.py` (`lexical_search`, new `trigram_search`, `semantic_search` nullable-aware, fusion weights), `src/mission_control/adapters/capabilities/capability_embeddings.py` (model profile binding), `src/mission_control/bootstrap/api.py` and `bootstrap/catalog.py` (wire embeddings when configured, never 503), `src/mission_control/domain/coordinator/search_document.py` (new columns), `catalog_projection*.py` (async embed).
- Projection: `src/mission_control/application/agentic_components/projections.py` (`render_host_files`, `skill_target_path`, new renderers per kind and profile), `src/mission_control/domain/agentic_components/contracts.py` (`ComponentKind`, typed bindings, `AgentHost` gains lane-profile mapping), `src/mission_control/application/agentic_components/materialization.py` (`configure_host` steps per file).
- Hooks: new `src/mission_control/adapters/deep_agents/hooks.py` (`HookScriptMiddleware`, `MissionSummarizationMiddleware`), `src/mission_control/adapters/deep_agents/materializer.py` (middleware order: kernel first), `src/mission_control/contracts/hooks.py` (`mc.hook_input.v1`, `mc.hook_result.v1`), hook-callback endpoint is SPEC-07 (`interfaces/http/hook_callback.py`).
- Seeds: `packages/mission-control-db-contract/seeds/common/mc.catalog.agent-capabilities-1.0.0.json`, `seeds/biotech/...`, `seeds/ai-engineer/...`, fixtures `tests/fixtures/mcp/*.tools.json`, `tests/fixtures/capability_search_eval.json`.

## Testing Decisions

A good test exercises the public seam (service method, CLI command, HTTP route, rendered files) and asserts behaviour, never private structure. Prior art: `tests/unit/agentic_components/test_harness.py` (deterministic rendering), `tests/unit/control_plane/test_catalog_seed_bundles.py` (seed replay), `tests/integration/postgres/catalog_common.py` fixtures (`catalog_db`, `catalog_writer_pool`), `tests/unit/capability/` (search service), the seven real PostgreSQL bundle custody cases.

- Unit: `DefinitionKind` to `ASSET_KIND` mapping is total and injective for the new kinds; `host_support` validation; pin string parse and render round-trip; plugin `host_support` intersection; hook result merging (`deny` beats `defer` beats `allow`); `HookScriptMiddleware` with a fake subprocess runner for every mapped event; renderer golden files per kind and lane profile (`.cursor/hooks.json`, `.cursor/agents/verifier.md`, `.cursor/mcp.json`, `.mcp.json`, `.codex/config.toml`, `.codex/hooks.json`, `.claude/agents/verifier.md`, `AGENTS.md`, `.cursor/rules/mc-mission.mdc`), asserting no secret value appears; search in lexical mode on an in-memory projection with the eval fixture.
- Integration (`common_db`): migration 0025 and 0026 apply and replay as no-ops; plugin admission trigger refuses an un-admitted member; nullable embedding rows rank lexically; hybrid fusion with recorded embeddings reaches recall@3 ≥ 0.9 on the eval set; seed bundle applies with receipts; `catalog search` HTTP returns `search_mode: lexical` without an embedding route and never 503.
- Custody: upload to a disposable Supabase-compatible store (local `supabase start` or a recorded fake) writes digest paths, refuses overwrite, mints signed URLs; download verifies and rejects a tampered file with `CAPABILITY_DRIFT`.
- Contract parity: CLI, HTTP and MCP search return the same hit set for the same request (existing parity test pattern).

## Out of Scope

Cursor plugin marketplace manifests and review; Claude Code marketplace publication; executing discovery candidates; MCP OAuth interactive flows inside lanes (ADR-0015 covers the coordinator side); retiring the old SQL kind literals (`skill`, `tool`, `hook`) from existing rows; embedding model migration tooling beyond recording `embedding_model`; live paid embedding runs in CI; Cursor Cloud reading repository `.cursor/mcp.json` (UNVERIFIED, tracked in SPEC-07).

## Further Notes

- Codex hooks require per-hash trust; a rendered `.codex/hooks.json` is inert until a human trusts it, so the Validation Report carries `requires_trust` for Codex-hosted authoring and the Codex lane (later) will need `--dangerously-bypass-hook-trust` under policy.
- Cursor headless local runs auto-approve every tool call; the kernel hooks are the only governance boundary there, hence `failClosed: true` and SPEC-07's callback endpoint.
- `tools` and `disallowed_tools` on Cursor are local-only and not persisted across resume; projections re-send them on every `send`.
- Firecrawl's `firecrawl_extract` is deprecated and excluded through `tool_allowlist`; `firecrawl_interact` runs one prompt per call.
- NCBI allows 3 requests per second without a key and 10 with one; the `NCBI_API_KEY` secret ref is therefore required for Mission 1's budget.
- UNVERIFIED items to confirm during A6 and A7: whether `uvx --from "edgartools[ai]==5.61.1"` pins as expected; whether Cursor rules and skills load without `setting_sources=["project"]` (set it explicitly); the Supabase project's pgvector version (`select extversion from pg_extension where extname='vector'`); whether OpenAI embeddings are normalized (cosine opclass is used regardless).

## Tickets

| Id | Title | Blocked by |
| --- | --- | --- |
| A1 | Capability kinds migration, definitions and host support | — |
| A2 | Bundle custody: upload, digest paths, signed download, bucket seed | A1 |
| A3 | Hybrid search with lexical fallback wired into the public API | A1 |
| A4 | Host projection for skills, MCP, hooks, subagents and plugins per lane profile | A1 |
| A5 | Hook script contract and Deep Agents HookScriptMiddleware with kernel hooks | A1 |
| A6 | Seed MCP servers: Tavily, Firecrawl, PubMed, EDGAR | A1 |
| A7 | Seed skill bundles: agent-browser, edgartools, mission-control bundles | A2, H1 |
| A8 | Catalog CLI and MCP parity for kinds, pins and plugin composition | A3, A4 |

# Citations

- ADR-0020, ADR-0023, ADR-0024, ADR-0025, ADR-0026 in `docs/adr/`; `GLOSSARY.md`.
- `research/seed-capabilities-and-formats.md` (packages, tool lists, formats, storage limits, hybrid search pattern, seed table).
- `research/cursor-platform.md` sections 1 and 2 (subagent file format, rules, skills, `hooks.json` schema and payloads, `mcp.json`, plugins, `setting_sources`).
- `research/deepagents-middleware.md` sections 2 and 6 (`AgentMiddleware` surface, `SummarizationMiddleware`, `SkillsMiddleware`, `MemoryMiddleware`, `dcode` `ServerHooksMiddleware`).
- `research/codebase-map.md` sections 1 and 2 and gaps (a) and (b).
- `docs/research/2026-10-07-coding-lane-surfaces.md` (hook surfaces per provider).
- Spec pack: `../mission-control-general/general-mission-control/expansion/CATALOG-AND-ENVIRONMENTS.md`; `SPECIFICATION.md` ("Domain packages and admitted capabilities").
