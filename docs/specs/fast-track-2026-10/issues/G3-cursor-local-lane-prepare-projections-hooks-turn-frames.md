# [FT-G3] Cursor Local lane: prepare, projections, kernel hooks callback, turn, frames

Linear: OVE-52

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** FT-G2, FT-A4, FT-A5
**Status:** ready-for-agent

**What to build:** The `cursor_local` Lane Profile end to end for one turn: lease a git worktree Workspace, place the Host Projections rendered by FT-A4 (`AGENTS.md`, `.cursor/rules/mc-mission.mdc` with `alwaysApply`, `.cursor/skills`, `.cursor/agents`, `.cursor/mcp.json`, `.cursor/hooks.json` with Kernel Hooks first and fail-closed permission hooks), materialize the Context Packet under `.mission/` and `/inputs/`, launch the bridge with a pinned `state_root`, create the agent with `setting_sources=["project"]`, send the turn with an idempotency key, observe `run.observe(after_offset)` into Provider Frames, map hook invocations through the loopback callback endpoint (`POST /v1/internal/hook-callback` with a task token bound to attempt and generation; checks the Stop Fence and writes the Operation Intent), derive closing facts from `RunResult`, register outputs and the git patch, and release the lease. Driven by `lane.turn` from FT-G2 and proven on recorded frame fixtures.

**Spec sections:** SPEC-07 §5.1 to §5.5, §8 (lifecycle synthesis table), Contracts (`mc.cursor_binding.v1`, hook callback), Persistence (`workspace_lease`, `hook_task_token`).

**Writable regions:** `src/mission_control/adapters/cursor/{local.py,frames.py,projection.py,hooks_callback.py,binding.py}`, `src/mission_control/interfaces/http/hook_callback.py`, `migrations/0030_lane_bindings.sql` (lease and token tables), `tests/integration/cursor/fixtures/local/`, `pyproject.toml` (`cursor-sdk==1.0.37`). Shared, integrator-reviewed: `domain/execution/contracts.py` (`CursorExecutionBinding`), `adapters/temporal/deployment_composition.py` (wire when `CURSOR_API_KEY` bound), `bootstrap/worker.py` (start the loopback listener).

**Acceptance criteria:**
- [ ] `prepare` creates the worktree lease at the base ref, writes every projection, materializes the packet, records projection digests, and fails with `CAPABILITY_DRIFT` on a digest mismatch and `UNSUPPORTED_BEHAVIOR` when `sandbox_enabled` is requested on a host where the SDK raises `ConfigurationError`.
- [ ] `.cursor/hooks.json` lists Kernel Hooks first with `failClosed: true` on `preToolUse`, `beforeShellExecution`, `beforeMCPExecution`, `subagentStart`; catalog Hook Scripts (FT-A5 contract) follow.
- [ ] Hook callback: rejects a missing, expired or wrong-generation token; denies when a Stop Fence exists; writes an Operation Intent keyed on `tool_use_id` or shell-command digest before returning `allow`; persists the invocation as a frame of kind `hook`; the mapping table in SPEC-07 §5.4 is covered by one unit case per Cursor event.
- [ ] `start` persists `agent_id` and `run.id` on `harness_execution` before observation; `observe` maps SDK messages and `InteractionUpdate`s to frames with `provider_key = run_id:offset`; a resume from a stored offset yields no duplicate frames.
- [ ] Closing facts from `RunResult` cover `finished`, `error`, `cancelled`, `expired`; cost disposition is `estimated`; `usage()` upgrades to `settled` when `get_usage().cost` is present and stays `estimated` on `feature_unavailable`.
- [ ] `end_session` captures `git diff` plus untracked files as the patch artifact, registers `/outputs/` artifacts, releases the lease only after the patch is stored.
- [ ] Recorded fixtures (full run, error run, hook deny, preCompact) replay through the real adapter and reducer; secrets and user email are scrubbed from fixtures.
- [ ] `describe()` for `cursor_local` matches 00-ARCHITECTURE.md §6 and `qualified=False`.

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/harness/test_cursor_local.py tests/unit/harness/test_hook_callback.py -q`; `uv run --group biotech pytest tests/integration/cursor -q` (fixtures only); `uv run --group biotech pytest -m common_db tests/integration/postgres/test_workspace_lease.py`.

**Notes:** Python SDK 1.0.37 has no `system_prompt`; instructions go through `AGENTS.md` and the always-apply rule (SPEC-07 §5.1). Headless local runs approve every tool call on their own, which is why the permission hooks are fail-closed. `tools`, `disallowed_tools` and inline `mcp_servers` are not persisted across `Agent.resume`; re-supply them on reattach. UNVERIFIED: Windows sandbox; whether rules load without `setting_sources=["project"]` (always set it); `Run.request_id` in Python. Recording the fixtures is the only paid step; `CURSOR_API_KEY` is set and one real local run per fixture case is permitted (see TEAM-WORKSPACE.md, Environment). Mark any hand-authored fixture synthetic.
