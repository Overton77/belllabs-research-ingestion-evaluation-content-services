---
type: Verification Reference
title: cursor_cloud qualification status (MP-09 / OVE-72)
description: Per control, what the cursor_cloud lane profile implements, what is qualified by evidence, and what only an account can enable; busy and capacity handling; stream expiry reconciliation; the owner-run bounded drill.
tags: [mission-control, qualification, lanes, cursor, cursor_cloud]
---

# `cursor_cloud` qualification status

`lane_profile.qualified` is **false**. It flips only through a reviewed release citing a
record written by the owner-run live drill (`docs/qualification/lanes/cursor_cloud-<date>.md`;
[lane README](../README.md), FT-G6 / Linear **OVE-55**, In Review). `cursor_cloud` inherits
nothing from `cursor_local` because both use the same SDK (SPEC-01): every cell below has its
own evidence.

- **implemented**: `src/mission_control/adapters/cursor/cloud.py`, `cloud_api.py`, `sse.py`,
  `scm.py`, `controls.py` (hydrator), `qualification.py`;
- **qualified**: offline only. The FIXTURE Cloud Agents API (`tests/fixtures/cursor_cloud.py`,
  `httpx.MockTransport`, a bare git remote) replays hand-authored `synthetic` fixtures
  (`tests/integration/cursor/fixtures/cloud/`) through the real adapter, `lane.turn`, the
  reducer and, for MP-09, the real local Temporal server and a disposable PostgreSQL 17.
  No live drill has run;
- **account-enabled**: `metadata`, `envVars`, `env.type pool|machine`, usage, the numeric
  rate limit and the stream retention window are account/plan facts observed only at launch.
  Nothing was observed: no Cursor API call was made for MP-09.

## Controls

| Control | Declared (`describe`, v1) | Implemented | Qualified (evidence) | Account-enabled |
| --- | --- | --- | --- | --- |
| `prepare` (branch `mc/<run>` with projection + packet; provider workspace lease) | native | yes; MP-09 records the lease through `WorkspaceAllocator.allocate_provider_workspace` (`CURSOR_CLOUD_POLICY`: `provider_workspace`, reuse `within_run`) | offline: `tests/unit/cursor/test_cloud_workspace_and_resume.py`, `tests/unit/harness/test_cursor_cloud.py` | repository push rights of the worker's git |
| `start` (client `agentId`; `409 agent_id_conflict` reattaches; `Idempotency-Key` with `envVars`) | native | yes | offline: `test_cursor_cloud.py::test_a_lost_create_response_reattaches_instead_of_creating_twice`, `::test_env_vars_create_with_an_idempotency_key_and_no_agent_id` | unknown (`metadata` may be `403 feature_unavailable`; recorded, not fatal) |
| `reattach` (by native identity; rehydrates branch, projection, lease, reconcile anchor) | native | yes | offline: `test_cloud_workspace_and_resume.py::test_a_fresh_worker_reattaches_rehydrates_resumes_the_cursor_and_ends_the_session` | n/a |
| `send_turn` (turn 1 = the create's run; later runs `POST .../runs`) | native | yes | offline: `test_lane_qualification_fixtures.py` | unknown |
| busy (`409 agent_busy`) | `wait_then_send` (ADR-0030) | yes: journaled `declined/busy`, no run, one request; the workflow polls `lane.status` at the boundary | offline: `tests/unit/cursor/test_cloud_capacity_and_busy.py`, `test_describe_honesty.py::test_queue_instruction_is_wait_then_send`; real Temporal busy path: `tests/integration/temporal/test_lane_turn.py::test_busy_waits_for_idle_then_sends_once` (scripted lane) | n/a |
| capacity (`429`, `Retry-After`) | not a describe cell | yes: `ProviderCapacityLimited(ProviderLimitSignal(kind=rate_limited, resets_at=now+Retry-After, source=cursor_cloud.http_429))`; journaled `declined/capacity`; the MP-05 Temporal timer waits; the next segment creates/sends once | offline: `test_cloud_capacity_and_busy.py`; real Temporal + PostgreSQL 17: `tests/integration/cursor/test_mp09_cloud_segment_loop_real_services.py::test_a_429_on_create_waits_on_a_temporal_timer_and_then_creates_once` | the numeric Cloud Agents limit is UNVERIFIED |
| `cancel_turn` (`POST .../cancel`; `run_not_cancellable` = already terminal) | native | yes | offline: `test_cursor_cloud.py::test_a_requested_cancel_posts_cancel_and_settles` | n/a |
| `observe` (SSE per-run keys `sse:<run>:<id>`, `Last-Event-ID`, heartbeats dropped) | native | yes | offline: `test_cursor_cloud.py::test_a_reconnect_with_last_event_id_stores_nothing_twice`; real services: `test_mp09_cloud_segment_loop_real_services.py` | retention window value is account-observed (`X-Cursor-Stream-Retention-Seconds`) |
| stream expiry vs task failure (`410 stream_expired`) | part of `observe` | yes: one `stream.expired` status frame (`sse-expired:<run>:<cursor>`), terminal truth from `GET .../runs/{runId}` (`run.final`, `observation.stream = expired`), a still-running run polled from the record (bounded, one `run-status` frame per change), `400 invalid_last_event_id` reopens from the start (keys dedupe) | offline: `tests/unit/cursor/test_cloud_stream_expiry.py` (FINISHED, ERROR with the provider's code, RUNNING then FINISHED, foreign cursor); real services: the V21 test above | n/a |
| `reconcile_dispatch` (MP-06 `DispatchReconcilingLane`) | not a describe cell | yes: create by deterministic client `agentId` (`404` = authoritative `not_received`; an `Idempotency-Key` create stays `unknown`); send by the agent's `latestRunId` against the runs this lane acknowledged (turn 1 = the create's run; a continuation = the hydrated agent's first run; no anchor = `unknown`) | offline: `tests/unit/cursor/test_cloud_reconcile.py` (V07 through `LaneTurnService`: one create, no `POST /runs`, journal acknowledged) | n/a |
| `usage` (`GET .../usage?runId=`) | native | yes; unknown stays unknown: agent totals are never attributed to a run, empty or `403` answers stay `unknown`, cost settles only from a reported value | offline: `tests/unit/cursor/test_cloud_usage_unknown.py` | unknown |
| `snapshot` (`branch:<branch>@<sha>`) | emulated | yes; MP-09 produces the ref on the lease (`WorkspaceAllocator.record_provider_snapshot`, ADR-0037's missing producer) | offline: `test_cloud_workspace_and_resume.py::test_snapshot_records_the_branch_head_on_the_lease_while_the_run_is_live` | n/a |
| `fork` (derived branch from the snapshot head) | emulated | yes | offline: `test_describe_honesty.py::test_fork_is_emulated_from_the_snapshot_with_a_new_agent` | n/a |
| `request_continuation` (files committed, fresh agent on the branch) | emulated | yes | offline: `test_describe_honesty.py::test_request_continuation_is_emulated_by_a_hydrated_agent` | n/a |
| `end_session` (SCM diff as patch, artifacts via presigned URLs, head snapshot, lease released after custody, archive last) | native | yes | offline: `test_cursor_cloud.py::test_a_cloud_turn_runs_on_the_pre_created_branch_and_settles`, `test_cloud_workspace_and_resume.py` | n/a |
| `pause` | unsupported | no | `test_describe_honesty.py` | n/a |

## Approval coverage (headless `ask` and tool gates)

`qualification.approval_coverage("cursor_cloud")`; unproven coverage is **rejected**.

| Origin | Effect | Coverage | Implemented | Admitted | Evidence / note |
| --- | --- | --- | --- | --- | --- |
| `workflow_gate` | any | native (kernel Human Gate) | yes | **admitted** | ADR-0038 |
| `governed_effect` | shell, file, task | unqualified | no: catalog command hooks run in the VM, but the VM cannot reach the worker's loopback callback, so no Stop Fence / Operation Intent gate runs there (`describe.hooks.fail_closed = False`) | rejected | CURSOR_SDK_FACTS section 7; `hook_deny_stream` fixture |
| `governed_effect` | mcp | unsupported | no | rejected | cloud agents fire no `beforeMCPExecution` / `afterMCPExecution` |
| `provider_permission` (headless `ask`) | any | unsupported | no | rejected | the run SSE stream carries no `request` event |
| `provider_question` | any | unsupported | no | rejected | no surface |
| `mcp_elicitation` | any | unqualified | no | rejected | `localhost` in the VM is not the worker; nothing proven |

## Declared describe (v2)

Integrated 2026-10-10: `CURSOR_CLOUD_DESCRIBE` is the `mc.lane_describe.v2` matrix (v1 readable as
`CURSOR_CLOUD_DESCRIBE_V1`): controls unchanged; implemented features `native`/`emulated` with
`qualified=False`; `approval_suspension = unsupported`; `approval_modes = ("workflow_gate",)`;
`compaction_control = unsupported`; `enforcement_coverage = {shell: unqualified, file:
unqualified, mcp: unsupported}`. Release 1.2.0 (migration 0033) revises the `lane_profile` row.

## Owner-run bounded drill (paid; not run for MP-09)

```sh
# 1. Offline evidence first (no credentials):
make lane-qualify PROFILE=cursor_cloud
uv run --frozen --group biotech --group dbcontract pytest tests/unit/cursor -q -p no:cacheprovider
# Real local services (Temporal 127.0.0.1:7233; disposable PostgreSQL 17 on 127.0.0.1:55433,
# DSN only in the environment of the command):
MISSION_CONTROL_TEST_ADMIN_DSN=postgresql://postgres:<pw>@127.0.0.1:55433/postgres \
  uv run --frozen --group biotech --group dbcontract pytest \
  tests/integration/cursor/test_mp09_cloud_segment_loop_real_services.py -q -p no:cacheprovider

# 2. Approve a finite budget in a Linear comment on OVE-55, then from any worker host:
export CURSOR_API_KEY=...                 # never written to a file or printed
export MC_PAID_BUDGET_USD=<amount>        # finite; the drill refuses without it
export MC_CURSOR_CLOUD_REPO=<throwaway repository the worker's git can push to>
export MC_LANE_DRILL_APPROVAL_URL=https://linear.app/...   # repeated busy/cancel drills only
make lane-qualify PROFILE=cursor_cloud LIVE=1

# 3. Review the scrubbed recordings (tests/integration/cursor/recordings/cloud/<date>/) and the
#    record docs/qualification/lanes/cursor_cloud-<date>.md; an `unknown` paid unit means stop,
#    check the Cursor dashboard and archive the agent before anything else.
```

Items the drill settles for this profile: the cloud `Idempotency-Key` window, the numeric
rate limit behind `429`, the retention window (`X-Cursor-Stream-Retention-Seconds`) and the
real `410` shape, `metadata`/`envVars` acceptance, `git.branches[]` attribution on a real
agent, and whether `POST .../runs` honours an idempotency key (which would make a lost
later-turn receipt reconcilable by key rather than by `latestRunId`).
