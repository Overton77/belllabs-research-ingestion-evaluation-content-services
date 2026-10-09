---
type: Concept
title: Cursor lane
description: The cursor_local and cursor_cloud lane profiles as built: bridge and Cloud Agents API harnesses, workspace leases and branch control, Kernel Hook callback, frame mapping, emulated snapshot, fork and continuation, the describe matrix, and why both profiles are still unqualified and blocked from a live mission.
tags: [mission-control, cursor, lanes, harness, implementation, qualification]
---

# Cursor lane

Cursor is one [Lane](../../GLOSSARY.md) with two [Lane Profiles](../../GLOSSARY.md),
`cursor_local` and `cursor_cloud` (ADR-0030). Both implement the protocol in
[lanes and harness](lanes-and-harness.md), run through the `lane.turn` segment loop
(always, whatever `MISSION_CONTROL_LANE_SEGMENT_LOOP` says), and write Provider Frames that
the reducer turns into mission events ([events and commands](events-and-commands.md)).
The package is `adapters/cursor/`; importing it imports no SDK (the SDK loads lazily in
`bridge.py`).

## Implemented

**Binding.** A Cursor execution binding carries a `mc.cursor_binding.v1`
(`domain/execution/lanes.py`: SDK, bridge, protocol and Cloud API pins, workspace with
`repo_url`, `base_ref`, `lease_root`, `branch_prefix` default `mc/`, sandbox and agent
options). It names native identity only, never a credential; the API key is passed in
agent options and never in a bridge environment (`bridge.py`).

**`cursor_local`** (`local.py::CursorLocalHarness`). `prepare` leases a detached git worktree
at the binding's base ref, or an initialized repository with one empty commit for research
missions (`workspace.py`, `application/execution/harness/leases.py`; leases persist in
`mission_control.workspace_lease`, `adapters/postgres/lanes/workspace_leases.py`; since MP-04
`GitWorktreeLeaser` delegates to the provider-neutral `WorkspaceAllocator` in
`application/workspaces/`), places the
Host Projection and the Context Packet (`projection.py`: `AGENTS.md`,
`.cursor/rules/mc-mission.mdc`, `.cursor/skills`, `.cursor/agents`, `.cursor/mcp.json`,
`.cursor/hooks.json` with Kernel Hooks first, plus `.mission/` and `inputs/`), and refuses
`CAPABILITY_DRIFT` when projection digests differ from the pins or `UNSUPPORTED_BEHAVIOR`
for a sandbox on a host without one. `start` launches `cursor-sdk==1.0.37` through the
`CursorBridgeLauncher` port with a pinned state root and creates the agent with
`setting_sources=["project"]`; `send_turn` uses idempotency key
`<execution>:<generation>:turn:<n>`; frames are keyed by bridge offset (`frames.py`).
Kernel Hooks run as `.mission/hooks/kernel.py` (`kernel_hook_script.py`), fail-closed on
permission events, and call the worker back on a loopback listener
(`MISSION_CONTROL_HOOK_CALLBACK_PORT`, default 47555) with a task token minted at `start`,
stored only as a digest and bound to scope, run, attempt, generation and execution
(`application/execution/harness/hook_callbacks.py`, `interfaces/http/hook_callback.py`,
`adapters/postgres/lanes/hook_tokens.py`). The callback consults the Stop Fence and writes the
Operation Intent before answering `allow`; `hooks_callback.py` maps Cursor events
(`preToolUse`, `beforeShellExecution`, `beforeMCPExecution`, `subagentStart`) to
`mc.hook_input.v1`.

**`cursor_cloud`** (`cloud.py::CursorCloudHarness`, `cloud_api.py`, `sse.py`, `scm.py`).
Cloud Agents API v1 over `httpx`. Because v1 has no branch-name field, `prepare` creates and
pushes branch `mc/<run_id>` carrying the Host Projection and Context Packet through the
worker's own git configuration, and the agent works on it; `end_session` fetches the branch
and takes the diff as the patch artifact. Create uses a client `agentId` (`409
agent_id_conflict` means reattach); a follow-up on a busy agent is `busy`
(`wait_then_send`); the SSE stream resumes with `Last-Event-ID`; artifacts and usage are
read through the API.

**Controls.** Both profiles declare `pause: unsupported` (refused mid-run, applied at the run
boundary only), `queue_instruction` and `resume` as `wait_then_send`, `interrupt_and_inject`
as `cancel_and_replace` (uncertain effects must settle before a replacement turn runs,
`application/execution/harness/inject.py`, `controls.py`), `snapshot`, `fork` and
`request_continuation` as `emulated`. `snapshot.py` freezes a lease as
`mc.cursor_snapshot.v1` (binary patch against the base commit including untracked files,
packet roots, native refs) named `cursor-snapshot:<manifest ref>`; `end_session` records that
ref on the released lease so a fork's workspace item can restore it (readiness commit
`37d7deb`). `controls.py` holds `CursorWorkspaceSnapshots` and `CursorSessionHydrator`:
a continuation always creates a new agent in the same lease and stages the hydration prompt
as the next turn ([context and continuation](context-and-continuation.md)).

**Honesty about hooks.** Cloud runs fire no `sessionStart`, `sessionEnd` or MCP hooks, and
the cloud VM cannot reach the worker's loopback callback, so `cursor_cloud` declares
`fail_closed=False`; only catalog command hooks run there and the Stop Fence reaches a cloud
run through the API cancel. Usage tokens are `settled_per_turn`, cost
`estimated_then_settled`.

## Qualification and known limits

Both profiles are `qualified=False`. Offline evidence (`make lane-qualify PROFILE=...`)
replays hand-authored fixtures through the real adapter, `lane.turn` and the reducer and
holds every `describe()` cell to its behavior; the paid drill that records real fixtures
and writes the qualification record is owner-run and has not run
(`docs/qualification/lanes/README.md`). Open items, each stated there or in the owner runbook:

- `cursor_local` needs a Proactor or Unix event loop; the worker uses a selector loop on
  Windows, so the bridge cannot launch there (run the worker under WSL or Linux). The
  Windows sandbox is unverified and refused.
- Fork restore works for `cursor_local` only. `cursor_cloud` expects a
  `branch:<branch>@<sha>` ref that nothing records yet.
- The production launch author binds `deep_agents` only, so a manifest node on a Cursor lane
  fails at its pointer ([mission manifest](mission-manifest.md)).
- Neither harness implements `DispatchReconcilingLane`, so an ambiguous send parks `in_doubt`
  rather than being reconciled ([session ownership and dispatch](session-ownership-and-dispatch.md)).
  `cursor_cloud` is not rewired onto `WorkspaceAllocator.allocate_provider_workspace`. MP-09
  (parity) is in flight and not integrated.
- Several SDK and API behaviors are `UNVERIFIED` until the drill (cloud idempotency window,
  concurrent local `send()`, rules without `setting_sources`, `run.git`, stream retention).

## Specified only

Real recordings that replace the synthetic fixtures, the flip of `qualified` through a
reviewed release (SPEC-07 section 12), and Windows sandbox qualification. The Claude Agent
SDK and Codex profiles have no harness on the integrated base ([lanes and harness](lanes-and-harness.md)).

# Citations

- Spec: [SPEC-07](../specs/fast-track-2026-10/SPEC-07-harness-and-cursor-lane.md);
  [Cursor platform research](../specs/fast-track-2026-10/research/cursor-platform.md);
  [00-ARCHITECTURE](../specs/fast-track-2026-10/00-ARCHITECTURE.md) (lane matrix).
- ADRs: [0018](../adr/0018-coding-agents-are-execution-lanes.md),
  [0019](../adr/0019-coding-lanes-worker-hosted-local-first.md),
  [0030](../adr/0030-cursor-lane-two-profiles-reducer-from-frames-hydrated-fork.md),
  [0031](../adr/0031-temporal-lifecycle-synthesis-observe-activity-updates-seeded-forks.md).
- Qualification: [lane qualification README](../qualification/lanes/README.md),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md).
- Code: [local lane](../../src/mission_control/adapters/cursor/local.py),
  [cloud lane](../../src/mission_control/adapters/cursor/cloud.py),
  [Cloud API client](../../src/mission_control/adapters/cursor/cloud_api.py),
  [SSE parser](../../src/mission_control/adapters/cursor/sse.py),
  [bridge](../../src/mission_control/adapters/cursor/bridge.py),
  [frame mapper](../../src/mission_control/adapters/cursor/frames.py),
  [projection](../../src/mission_control/adapters/cursor/projection.py),
  [workspace leases](../../src/mission_control/adapters/cursor/workspace.py),
  [SCM branch control](../../src/mission_control/adapters/cursor/scm.py),
  [snapshot](../../src/mission_control/adapters/cursor/snapshot.py),
  [controls and hydrator](../../src/mission_control/adapters/cursor/controls.py),
  [hook payload mapper](../../src/mission_control/adapters/cursor/hooks_callback.py),
  [kernel hook script](../../src/mission_control/adapters/cursor/kernel_hook_script.py),
  [hook callback service](../../src/mission_control/application/execution/harness/hook_callbacks.py),
  [hook callback route](../../src/mission_control/interfaces/http/hook_callback.py),
  [interrupt and inject](../../src/mission_control/application/execution/harness/inject.py),
  [lane controls](../../src/mission_control/application/execution/harness/controls.py),
  [describe matrices](../../src/mission_control/application/execution/harness/describe.py),
  [lane contracts](../../src/mission_control/domain/execution/lanes.py).
- Tests: [local lane](../../tests/unit/harness/test_cursor_local.py),
  [cloud lane](../../tests/unit/harness/test_cursor_cloud.py),
  [controls](../../tests/unit/harness/test_cursor_controls.py),
  [hook callback](../../tests/unit/harness/test_hook_callback.py),
  [describe honesty](../../tests/unit/harness/test_describe_honesty.py),
  [local fixtures](../../tests/integration/cursor/test_cursor_local_fixtures.py),
  [cloud fixtures](../../tests/integration/cursor/test_cursor_cloud_fixtures.py),
  [kernel hook round trip](../../tests/integration/cursor/test_kernel_hook_roundtrip.py),
  [lane controls in PostgreSQL](../../tests/integration/postgres/test_ft_g4_lane_controls_postgres.py),
  [workspace lease](../../tests/integration/postgres/test_workspace_lease.py).
