---
type: Verification Reference
title: Cursor lane qualification
description: How the cursor_local and cursor_cloud lane profiles are qualified - the offline fixture, describe-honesty and replay evidence that runs in CI, and the owner-run paid drill that records real fixtures and writes the qualification record.
tags: [mission-control, qualification, lanes, cursor]
---

# Cursor lane qualification (FT-G6)

`lane_profile.qualified` is `false` for `cursor_local` and `cursor_cloud`. It flips only through
a reviewed release that cites a record in this directory written by the live drill
(`<profile>-<date>.md`, front matter `outcome: qualified`, `evidence: live_drill`); the
describe-honesty suite fails if a profile is declared qualified without one. CI fixtures never
flip it.

## Offline evidence (every `make check`, no network, no credentials)

| Suite | What it proves |
| --- | --- |
| `tests/unit/harness/test_lane_qualification_fixtures.py` | Both profiles replay full, error, cancelled, busy, conflict, expired and hook-deny cases through the real adapter, `lane.turn` and the C2 reducer; every committed fixture is marked synthetic (or recorded) and carries no secret or e-mail address. |
| `tests/unit/harness/test_describe_honesty.py` | Every `describe()` cell of both profiles, exercised: native and emulated controls do what SPEC-07 section 7 says (cancel, reattach, snapshot, fork, `wait_then_send`, `cancel_and_replace`, `request_continuation`), `pause` is refused mid-run, hooks claim `fail_closed` only where Kernel Hooks run (`cursor_local`; the cloud VM cannot reach the loopback callback), cursors are per run. |
| `tests/integration/temporal/test_lane_replay_histories.py` | The captured FT-G2 and FT-G4 `mc.operation.v1` histories (`histories/ft_lanes/`) replay with `Replayer` on the current worker. Recapture with `MC_CAPTURE_LANE_HISTORIES=1 uv run --group biotech pytest tests/integration/temporal/test_lane_turn.py`. |

`make lane-qualify PROFILE=cursor_local` (or `cursor_cloud`) runs the three suites.

## The live drill (owner-run, paid)

The fixtures under `tests/integration/cursor/fixtures/` are hand-authored from
`docs/specs/fast-track-2026-10/research/cursor-platform.md`. The drill records real ones.

1. Approve a finite budget in a Linear comment on OVE-55 (FT-G6). Single fixture runs (one agent,
   one run per recording) are allowed by the TEAM-WORKSPACE budget policy; the repeated
   cancel-latency and busy drills need the approval comment URL.
2. Export the credentials in the shell that runs the drill (never commit or print them):
   `CURSOR_API_KEY`, `MC_PAID_BUDGET_USD=<amount>`, optionally
   `MC_LANE_DRILL_APPROVAL_URL=https://linear.app/...` for the repeated drills, and for the
   cloud profile `MC_CURSOR_CLOUD_REPO=<a throwaway repository the worker's git can push to>`.
   Run `cursor_local` on Linux or WSL (the bridge needs a Proactor or Unix event loop; the
   Windows sandbox is an open item).
3. Run `make lane-qualify PROFILE=cursor_local LIVE=1`, then `PROFILE=cursor_cloud LIVE=1`.
   The offline suites run first; the drill refuses to start without a finite budget.
4. Review what it wrote:
   - scrubbed recordings under `tests/integration/cursor/recordings/<local|cloud>/<date>/`
     (the API key, any bound secret and e-mail addresses removed; first line `recorded`);
   - the record `docs/qualification/lanes/<profile>-<date>.md`: checks, cancel latency, busy
     behavior, spent, reserved and unknown paid units, and the UNVERIFIED items as verified,
     refuted or open.
5. If a record says `unknown` paid units (an agent may exist without a recorded id), stop:
   check the Cursor dashboard, archive or delete it, and note it in Linear before anything else.
6. To qualify: move the recordings that should replace the synthetic fixtures into
   `tests/integration/cursor/fixtures/`, rerun `make check`, and land a reviewed change that sets
   `qualified=True` in `application/execution/harness/describe.py` together with a release
   migration row (`qualified_at`, `qualification_ref` naming the record).

## UNVERIFIED items the drill settles (SPEC-07)

| Item | Offline status | How the drill settles it |
| --- | --- | --- |
| Windows sandbox | open; the lane refuses `sandbox_options.enabled` on Windows | `sandbox_supported()` on the host, agent create with the sandbox |
| Rules without `setting_sources` | open; the lane always sets `["project"]` | not exercised by default |
| `Run.request_id` in Python | refuted by source: `cursor-sdk==1.0.37` exposes `request_id` only on `SDKRequestMessage`, not on runs | re-checked on the recorded run |
| Concurrent local `send()` | open | busy drill (approval): a second send while a run is active |
| Cloud `Idempotency-Key` window | open; the lane creates with a client `agentId` | repeated drill only |
| Local `run.git` | open; the lane computes the patch itself | recorded `RunResult.git` |
| REST `envVars` and `metadata` | open | `metadata` acceptance or `403 feature_unavailable` on create |
| Stream retention | open | `X-Cursor-Stream-Retention-Seconds` on the recorded stream |
