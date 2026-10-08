# [FT-A4] Host projection for skills, MCP, hooks, subagents and plugins per lane profile

Linear: OVE-25

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A1
**Status:** ready-for-agent

**What to build:** Given a resolved set of capability rows, a lane profile, the mission instruction text and the Context Packet index, one deterministic renderer produces every native file the lane's provider reads: `.cursor/{rules/mc-mission.mdc,skills/*,agents/*.md,mcp.json,hooks.json}` and `AGENTS.md` for Cursor Local and Cloud; `.claude/{skills,agents,settings.json hooks}` and `.mcp.json` for Claude; `.agents/skills`, `.codex/{config.toml,hooks.json,agents}` and `AGENTS.md` for Codex; backend paths plus in-process `SubAgent` dicts, MCP connection dicts and the hook middleware descriptor for Deep Agents. Plugins expand into their members. Secrets appear only as the lane's native reference syntax. Kernel hooks are rendered first in every hook array and as `failClosed` on Cursor.

**Spec sections:** SPEC-01 "Host projection", "Hook event vocabulary and lane mapping", "Kernel hooks", "Subagent profiles", "Plugins", "Insertion points" (projection).

**Writable regions:** `src/mission_control/application/agentic_components/projections.py`, `src/mission_control/application/agentic_components/materialization.py`, `src/mission_control/domain/agentic_components/contracts.py`, `tests/unit/agentic_components/`, golden fixtures under `tests/fixtures/projections/<profile>/`.

**Acceptance criteria:**
- [ ] `render_host_files(rows, profile, instruction, packet_index, kernel_hooks)` returns `(path, bytes, mode)` tuples for all five profiles and all five kinds per the SPEC-01 projection table; the function is pure (golden-file tests, two runs byte-identical).
- [ ] MCP rendering: Cursor `${env:NAME}` in `env`, `headers`, `auth`, with `type`; Claude `${VAR}` with `type`; Codex `[mcp_servers.<name>]` with `env_vars`, `bearer_token_env_var`, `env_http_headers` and no interpolation; Deep Agents connection dict with secret refs left unresolved; a test asserts no rendered byte equals a fixture secret value.
- [ ] Hook rendering: `.cursor/hooks.json` (`version: 1`, event map from the SPEC-01 mapping table, `matcher`, `timeout`, `failClosed`), `.claude/settings.json` `hooks` (exec-form `args`, `timeout`, `if`), `.codex/hooks.json` (`command` handlers only); kernel hooks first in every array; unsupported events for the profile are returned in a `ProjectionReport.unsupported_on_lane` list, not silently dropped.
- [ ] Subagent rendering: `.cursor/agents/<name>.md` with `name`, `description`, `model`, `readonly`, `is_background` and the prompt body; `.claude/agents/<name>.md` with `tools`, `model`, `permissionMode`, `skills`, `mcpServers`, `hooks`, `background` (project scope); `.codex/agents/<name>.md`; Deep Agents `SubAgent` dict with `readonly` mapped to a deny `FilesystemPermission` and `background` degraded to sync with a report entry when no Agent Server is bound; inline `AgentOptions.agents` form produced only when the profile is secret-free and needs neither `readonly` nor `is_background`.
- [ ] Skill rendering: `.cursor/skills/<name>/`, `.claude/skills/<name>/`, `.agents/skills/<name>/`, Deep Agents `/skills/<scope>/<name>/` with `name` equal to the directory and the `skills=[...]` source order (kernel before mission).
- [ ] Instruction rendering: `AGENTS.md` plus `.cursor/rules/mc-mission.mdc` (`alwaysApply: true`) for Cursor; `/memory/AGENTS.md` for Deep Agents; `AGENTS.md` within 32 KiB for Codex (overflow is a report entry).
- [ ] Plugin expansion renders members in position order and records the expansion in the `MaterializationPlan` steps; `ComponentKind` and `AgenticComponentRelease` gain `hook_script`, `subagent_profile` and a typed `plugin` binding.
- [ ] Path hygiene: POSIX normalization, traversal and duplicate-path rejection, no writes into read-only seeds (existing rules extended to new paths).

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/agentic_components -q`; regenerate goldens only through `uv run python -m tests.fixtures.projections.regen` and review the diff.

**Notes:** Cursor `tools` and `disallowed_tools` are not persisted across resume; the renderer emits them as per-send options for SPEC-07 to re-send. Cursor Cloud runs command hooks only and skips `sessionStart`, `sessionEnd` and MCP hooks; Codex hooks need per-hash trust (`requires_trust` report entry). Claude plugin-scoped subagents ignore `permissionMode`, `mcpServers` and `hooks`, so always render those to `.claude/agents/`. The hook-callback endpoint and token file (`.mission/hooks/token`) are provided by SPEC-07 G3; this ticket renders the commands that call it.
