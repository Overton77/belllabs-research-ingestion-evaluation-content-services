# [FT-I3] Mission 3 manifest and acceptance run on Cursor Local with interventions

Linear: OVE-61

**Epic:** Missions (00-ARCHITECTURE §1, missions/)
**Team:** Integrator
**Blocked by:** FT-G4, FT-E3, FT-F1, FT-F4
**Status:** ready-for-agent

**What to build:** The owner's third mission runs from `missions/03-codebase-feature-cursor-local.yml`: a Goal Loop on the `cursor_local` lane in a leased git worktree of the target repository, with the `agent-browser` skill, repository hook scripts, a `verifier` subagent profile projected to `.cursor/agents/verifier.md`, kernel hooks as `fail_closed` command hooks calling the worker back, and the `test_run` and `git_snapshot` deterministic executors. During the run the operator exercises every intervention from SPEC-06's scenarios: queue an instruction consumed at the next iteration, interrupt and inject a redirect (`cancel_and_replace`), cancel immediately with the Stop Fence denying a shell hook callback, and fork from the last Snapshot with a new instruction. The accepted output is a branch plus a test report artifact.

**Spec sections:** 00-ARCHITECTURE §1, §7.5 and §7.6; SPEC-07 (Cursor Local, controls), SPEC-06 (scenarios 1 to 4), SPEC-01 (projections, hook scripts, subagent profiles), SPEC-05 (manifest).

**Writable regions:** `docs/specs/fast-track-2026-10/missions/03-codebase-feature-cursor-local.yml` (fixture adjustments only), `tests/acceptance/mission_control/test_fast_track_mission_3.py` (new), `.scratch/fast-track-2026-10-07/I3/` (evidence).

**Acceptance criteria:**
- [ ] `mission compile` resolves the skill, hook script and subagent profile pins and reports Cursor Local `host_support` for each; `submit` and `start` produce a Run with lane `cursor_local`.
- [ ] The leased workspace contains `AGENTS.md`, `.cursor/rules/mc-mission.mdc` (`alwaysApply: true`), `.cursor/skills/agent-browser/`, `.cursor/agents/verifier.md`, `.cursor/mcp.json`, `.cursor/hooks.json` with kernel hooks first and `.mission/context.md`.
- [ ] Scenario 1: a queued instruction is consumed at the next iteration with Delivery Report `wait_then_send`; the transcript shows it before the turn's first frame.
- [ ] Scenario 2: `command inject` yields `cancel_and_replace` with cancelled and replacement turn refs and settled uncertain effects.
- [ ] Scenario 3: immediate cancel persists the Stop Fence first; a `beforeShellExecution` callback during the window is denied with `STOP_FENCED` (visible as a frame); the Run settles `cancelled` with four timestamps in inspection.
- [ ] Scenario 4: `run fork --instruction-file` creates a branch Run whose first packet restores the workspace snapshot; `run list --query` finds it by `mc_forked_from_run_id`.
- [ ] On the un-cancelled path the Run ends `accepted` with `git_snapshot` and `test_run` artifacts registered; frames and usage (`estimated` then `settled` where the account allows) persisted.
- [ ] One real Cursor Local run on the worker is the primary evidence (`CURSOR_API_KEY` is set), recorded into replayable fixtures; a repeat needs an approved cap in Linear; evidence under `.scratch/fast-track-2026-10-07/I3/`; `make check` passes.

**Verification:** `make infra-up && make temporal-up`; `uv run --group biotech pytest -m common_db tests/acceptance/mission_control/test_fast_track_mission_3.py -q`; CLI sequence `mission compile|submit|start`, `command queue|inject|cancel`, `run fork`, `run transcript`, `run inspect`.

**Notes:** Headless local Cursor runs auto-approve every tool call, so kernel hooks must be `fail_closed` and `setting_sources=["project"]` must be set (research/cursor-platform.md). Sandbox on Windows is UNVERIFIED; run the worker under WSL or Linux if the sandbox is required. `cursor-sdk==1.0.37` pinned; confirm `client.get_version()` bridge version at start.
