---
type: Verification Reference
title: cursor_local qualification status (MP-09 / OVE-72)
description: Per control, what the cursor_local lane profile implements, what is qualified by evidence, and what only an account can enable; the owner-run bounded drill; the WSL 2 or Linux worker path.
tags: [mission-control, qualification, lanes, cursor, cursor_local]
---

# `cursor_local` qualification status

`lane_profile.qualified` is **false**. It flips only through a reviewed release citing a
record written by the owner-run live drill (`docs/qualification/lanes/cursor_local-<date>.md`,
front matter `outcome: qualified`, `evidence: live_drill`; see [the lane README](../README.md),
FT-G6 / Linear **OVE-55**, In Review). Nothing in CI, and nothing MP-09 added, flips it.

Three facts are kept apart for every control:

- **implemented**: an adapter path exists (`src/mission_control/adapters/cursor/local.py`,
  `bridge.py`, `frames.py`, `hooks_callback.py`, `workspace.py`, `snapshot.py`, `controls.py`);
- **qualified**: proven by evidence. Today that is offline evidence only: hand-authored
  fixtures marked `synthetic` replayed through the real adapter, `lane.turn` and the reducer
  (OVE-55 suites), plus the MP-09 suites below. No live drill has run; no row says
  `qualified` here;
- **account-enabled**: observed at launch for the bound Cursor account (`CURSOR_API_KEY`
  route, local usage endpoint, sandbox); never baked into a describe. Nothing was observed:
  no paid or account-bound call was made for MP-09.

## Controls

| Control | Declared (`describe`, v1) | Implemented | Qualified (evidence) | Account-enabled |
| --- | --- | --- | --- | --- |
| `prepare` (worktree lease, projection, packet) | native | yes | offline: `tests/unit/harness/test_cursor_local.py`, `tests/unit/workspaces/**` (MP-04 real git) | n/a |
| `start` (bridge launch, agent create, hook token) | native | yes | offline only: `ReplayBridgeLauncher` fixture; the pinned `cursor-sdk==1.0.37` launch path is unexercised | unknown |
| `reattach` (resume + option rehydration) | emulated | yes: `Agent.resume` re-supplied the pinned `LocalAgentSpec` (tools, disallowed tools, subagents, setting sources, sandbox); the handle reports `rehydrated` | offline: `tests/unit/cursor/test_local_reconcile_and_rehydration.py`, `test_cursor_local.py::test_a_new_worker_reattaches_by_resume_and_re_supplies_tools` | unknown |
| `send_turn` (idempotency key `<execution>:<generation>:turn:<n>`) | native | yes | offline: `test_lane_qualification_fixtures.py` (full, error, cancelled, expired, hook_deny, busy, conflict) | unknown |
| `cancel_turn` | native | yes | offline: `test_lane_qualification_fixtures.py::test_cursor_local_conflicts_are_idempotent`, `test_describe_honesty.py` | unknown |
| `observe` (bridge offsets, resume from the persisted cursor) | native | yes | offline: `test_cursor_local.py::test_a_resume_from_a_stored_offset_stores_no_frame_twice`, `test_worker_loss_between_persist_and_heartbeat_does_not_resend`; real Temporal: `tests/integration/temporal/test_lane_turn.py` | n/a |
| `snapshot` / `fork` (`mc.cursor_snapshot.v1`) | emulated | yes | offline: `test_cursor_controls.py` (real git in tmp) | n/a |
| `usage` (`get_usage`; `feature_unavailable` keeps it estimated) | native | yes | offline: `test_cursor_local.py::test_error_run_fails_and_settles_cost_when_the_provider_reports_it` | unknown (the local usage endpoint is account-gated) |
| `end_session` (patch stored before the lease is released) | native | yes | offline: `test_cursor_local.py` | n/a |
| `pause` | unsupported | no (refused mid-run, boundary only) | `test_describe_honesty.py::test_pause_is_refused_mid_run_and_never_a_harness_operation` | n/a |
| `reconcile_dispatch` (MP-06 `DispatchReconcilingLane`) | not a describe cell (frozen `LANE_CONTROLS`) | yes, bounded: `found` only from this process's memory (the SDK store offers no lookup by idempotency key; a process death ends the agent loop with the run), otherwise `unknown`, which parks `in_doubt` | offline: `tests/unit/cursor/test_local_reconcile_and_rehydration.py` | n/a |
| Host gate (`LANE_UNSUPPORTED_OS`) | n/a | yes: `bridge.host_gate()`, enforced in `prepare`, `start` and `SdkBridgeLauncher.launch` | offline: `tests/unit/cursor/test_host_gate.py` (Windows refused on either loop, Linux/WSL 2/macOS pass); `tests/unit/runtime/test_mp22_local_readiness.py` (preflight) | n/a |

## Approval coverage (headless `ask` and tool gates)

Reported by `adapters/cursor/qualification.py::approval_coverage("cursor_local")`
(`mc.cursor_approval_coverage.v1`) and admitted by `admit_approval`. Unproven coverage is
**rejected**.

| Origin | Effect | Coverage | Implemented | Admitted | Evidence / note |
| --- | --- | --- | --- | --- | --- |
| `workflow_gate` | any | native (kernel Human Gate) | yes | **admitted** | ADR-0038; independent of the provider |
| `governed_effect` | shell, file, mcp, task | unqualified | yes: Kernel Hook command hooks, fail-closed, loopback callback (`preToolUse`, `beforeShellExecution`, `beforeMCPExecution`, `subagentStart`) | rejected | `tests/integration/cursor/test_kernel_hook_roundtrip.py`, `hook_deny` fixture; whether `beforeMCPExecution` fires in headless SDK runs is UNVERIFIED |
| `provider_permission` (headless `ask`) | any | unsupported on this pin | no | rejected | `cursor_sdk==1.0.37` exposes `SDKRequestMessage(request_id)` and no API that answers it; headless runs auto-approve every tool (ADR-0030) |
| `provider_question` | any | unsupported | no | rejected | no surface in the SDK or the API |
| `mcp_elicitation` | any | unqualified | no | rejected | the governed MCP gateway (MP-11), nothing Cursor-specific is proven |

## Declared describe (v2)

Integrated 2026-10-10: `application/execution/harness/describe.CURSOR_LOCAL_DESCRIBE` is the
`mc.lane_describe.v2` matrix (the FT-G1 v1 shape stays readable as `CURSOR_LOCAL_DESCRIBE_V1`;
`qualification.proposed_describe` returns the declared matrix). Controls, delivery and identity are
the v1 cells; implemented features state how they are supported (`native`, or `emulated` for
output custody and continuation) with `qualified=False` everywhere (no drill);
`approval_modes = ("workflow_gate",)`; `compaction_control = unsupported` (`preCompact` is
observe-only; no SDK or REST call compacts); `enforcement_coverage = {shell, file, mcp:
unqualified}`. Release 1.2.0 (migration 0033) revises the `lane_profile` row to this document.

## The supported worker path: WSL 2 or Linux

The bridge is a subprocess. The production worker (`bootstrap/worker.run_cli`) pins a
`SelectorEventLoop` on Windows (psycopg), and asyncio on Windows spawns subprocesses only on
the Proactor loop; the Windows sandbox is UNVERIFIED. So `cursor_local` is refused on a
Windows worker at three places that agree on one rule (`bootstrap/preflight.LANE_HOSTS`,
`adapters/cursor/bridge.host_gate`, `SdkBridgeLauncher.launch`), with code
`LANE_UNSUPPORTED_OS`:

1. Run the API and Temporal anywhere; run the worker that registers `cursor_local` inside
   WSL 2 (Ubuntu) or on Linux (OWNER-FIXTURE-RUNBOOK 2.8): clone the repository there,
   `uv sync --frozen`, export the same settings, reach the host's PostgreSQL and Temporal
   over the WSL network, and point `--workspace-root` at the WSL path of the workspace.
2. `uv run --frozen python -m mission_control.bootstrap.preflight readiness --profile
   <mc.local_run_profile.v1>` must report no `LANE_UNSUPPORTED_OS` for the lane.
3. The lease root and Mission 3's `repo.path` must be WSL paths (`/mnt/c/...` or a clone),
   never the owner's primary Windows checkout.

## Owner-run bounded drill (paid; not run for MP-09)

Nothing below ran. The drill is the only evidence that can flip `qualified`.

```sh
# 1. Offline evidence (CI, no credentials); must pass first.
make lane-qualify PROFILE=cursor_local
uv run --frozen --group biotech --group dbcontract pytest tests/unit/cursor -q -p no:cacheprovider

# 2. Approve a finite budget in a Linear comment on OVE-55, then on a WSL 2 / Linux worker:
export CURSOR_API_KEY=...            # never written to a file or printed
export MC_PAID_BUDGET_USD=<amount>   # finite; the drill refuses without it
export MC_LANE_DRILL_APPROVAL_URL=https://linear.app/...   # only for repeated busy/cancel drills
make lane-qualify PROFILE=cursor_local LIVE=1

# 3. Review: scrubbed recordings under tests/integration/cursor/recordings/local/<date>/,
#    the record docs/qualification/lanes/cursor_local-<date>.md (spent/reserved/unknown units),
#    then a reviewed release that sets qualified=True with a migration row naming the record.
```

Items the drill settles for this profile (SPEC-07, [lane README](../README.md)): Windows
sandbox, rules without `setting_sources`, concurrent local `send()`, `run.git`, whether
`beforeMCPExecution`/`preCompact`/`stop` fire in headless SDK runs, and whether the local
store deduplicates a send by idempotency key (which would let `reconcile_dispatch` answer
`found` after a process death instead of `unknown`).
