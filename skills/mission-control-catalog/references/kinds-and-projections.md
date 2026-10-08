# Capability kinds and host projections

Kinds live in the catalog (ADR-0020, ADR-0023). This page explains what each kind is, what it
pins, and how a lane profile renders it into its native files (host projection). Lane profiles:
`deep_agents`, `cursor_local`, `cursor_cloud`, `claude_agent_sdk`, `codex`.

| Kind | What the row pins | Projection per lane |
| --- | --- | --- |
| `skill_bundle` | A directory with `SKILL.md`, optional `scripts/`, `references/`, `assets/`; manifest digest; bytes in `capability-bundles` | `deep_agents`: mounted read-only under `/skills/<name>/` and passed as a skills path. `cursor_*`: `.cursor/skills/<name>/` (also readable from `.agents/skills/`). `claude_agent_sdk`: `.claude/skills/<name>/`. `codex`: `.agents/skills/<name>/`. |
| `mcp_server` | Transport (`stdio` command and args, or `http` URL), secret references by name, declared tool list, package or image pin | `deep_agents`: resolved to tools by the worker and passed as `tools=`. `cursor_*`: `.cursor/mcp.json` entry. `claude_agent_sdk`: `.mcp.json` entry with `type`. `codex`: `.codex/config.toml` `[mcp_servers.<id>]`. |
| `mcp_tool` | One tool of a parent server (name, schema digest, side-effect class) | Appears as an allowlist entry on the parent server's projection. |
| `hook_script` | Script directory, events it runs at (`mc.hook_event`), matcher, timeout, `fail_closed`, side-effect class | `deep_agents`: `HookScriptMiddleware` runs the script with the same stdin JSON. `cursor_*`: `.cursor/hooks.json` command hook per mapped event. `claude_agent_sdk`: settings `hooks` entry. `codex`: `.codex/hooks.json` (trusted per content hash). |
| `subagent_profile` | Name, description, prompt body, model (`inherit` or id), tools, skills, `readonly`, `background`, `context_mode` | `deep_agents`: `SubAgent` dict (background only when an Agent Server is bound). `cursor_*`: `.cursor/agents/<name>.md` with `name`, `description`, `model`, `readonly`, `is_background`. `claude_agent_sdk`: `.claude/agents/<name>.md`. |
| `plugin` | `mc.plugin_manifest.v1`: exact member pins plus prompts and resources | Expanded into its members; each member is projected by its own kind. |
| `model_profile`, `sandbox_profile`, `environment_profile` | Provider route, limits, image, egress | Bound by the materializer; not files in the workspace. |

## `host_support`

Every row carries a matrix: for each lane profile `supported | unsupported | unqualified`, plus
an optional overlay with the few provider-specific fields. A manifest that pins a row whose
`host_support` excludes the node's lane fails compile with `CAPABILITY_UNAVAILABLE`; a hook
event the lane lacks appears in the report under `unsupported_on_lane`.

## Where hosts look for skills

| Host | Project path | Notes |
| --- | --- | --- |
| Claude Code | `.claude/skills/<name>/SKILL.md` | Does not read `.agents/skills`. |
| Cursor | `.agents/skills/`, `.cursor/skills/` | Also reads `.claude/skills/`, `.codex/skills/`. |
| Codex | `.agents/skills/` in every directory up to the repo root | Optional `agents/openai.yaml`. |
| Deep Agents | A backend path containing skill directories | Passed as `skills=[…]`. |

The projection writes to each host's documented path; the bundle itself never changes.

## Bundle manifest

```json
{
  "schema_version": "mc.skill_bundle.v1",
  "name": "my-skill",
  "version": "0.1.0",
  "service_contract_range": ">=0.1.0,<0.2.0",
  "files": {"SKILL.md": "sha256:…", "references/x.md": "sha256:…"}
}
```

`scripts/skills_manifest.py --check` verifies every bundle under `skills/`; `--write` refreshes it.
The `computedHash` of the Vercel `skills` CLI (sorted relative paths, path then bytes) is also
recorded at publish so pins interoperate with `skills-lock.json`.
