# [FT-G2] lane.turn, lane.status and lane.cancel activities with segmented heartbeats

Linear: OVE-51

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** FT-G1, FT-C1
**Status:** ready-for-agent

**What to build:** The three lane activities and the operation workflow segment loop that synthesize any lane's run into mission lifecycle. `lane.turn` starts or reattaches, streams Provider Frames through the FrameSink (FT-C1) before heartbeating the provider cursor, stops at a segment bound or a terminal frame, and returns only closing facts. `lane.status` reconciles, `lane.cancel` is idempotent. `mc.operation.v1` re-schedules segments, delivers mailbox items at boundaries (hook point only; FT-F1 fills it), checks `is_continue_as_new_suggested()` after draining handlers, carries `seen_cmds` across continue-as-new, and implements the cancel path (Update flag, activity cancel with `WAIT_CANCELLATION_COMPLETED`, `lane.cancel`, `lane.status` poll, settle). Deep Agents runs through this loop via `DeepAgentsHarness`; a worker crash mid-turn resumes from persisted frames without a second send.

**Spec sections:** SPEC-07 §4.1 (activities), §4.2 (workflow loop), §4.3 (cancellation), §4.4 (dedupe and continue-as-new), Contracts (activity payloads), Insertion points.

**Writable regions:** `src/mission_control/adapters/temporal/activities/lane_turn.py` (new), `src/mission_control/adapters/temporal/workflows/operation.py`, `src/mission_control/adapters/temporal/registration/activities.py`, `migrations/0030_lane_bindings.sql` (harness_execution columns). Shared, integrator-reviewed: `domain/execution/contracts.py` (activity payload contracts), `adapters/temporal/deployment_composition.py`, `adapters/temporal/operation_activities.py` (keep `operation.execute` registered until FT-G6).

**Acceptance criteria:**
- [ ] `lane.turn` persists every frame through the FrameSink before `activity.heartbeat(cursor, count)`; a fault injected between persist and heartbeat leaves no gap and no duplicate on resume (dedupe by provider key).
- [ ] A segment returns `done=False` at `max_duration_s` or `max_frames`; the workflow re-schedules with `phase="resume"` and the lane never receives a second `send_turn` for the same turn.
- [ ] Cancel Update: validator rejects duplicate `command_id` and terminal units; loop cancels the activity (`WAIT_CANCELLATION_COMPLETED`), activity calls provider cancel only when `cancellation_details().cancel_requested` is true (not on worker shutdown), then `lane.cancel` and `lane.status` run until terminal; the Delivery Report records `turn_boundary_guaranteed`.
- [ ] `seen_cmds` survives continue-as-new; a replayed command id after continue-as-new is rejected; `all_handlers_finished` is awaited before `continue_as_new` and completion.
- [ ] `harness_execution` gains `native_session_ref`, `native_turn_ref`, `provider_cursor`, `usage_disposition`, `last_segment_at` and is written at start, each segment and settlement.
- [ ] Deep Agents acceptance proofs (`test_wp_cp_040.py`, parity tests) pass through the segment loop; captured histories replay with `Replayer` without nondeterminism.
- [ ] Time-skipping tests cover: crash-resume, cancel path, duplicate command, continue-as-new carry-over, `in_doubt` after the status poll bound.

**Verification:** `make check`; `uv run --group biotech pytest tests/integration/temporal/test_lane_turn.py -q`; `uv run --group biotech pytest -m common_db tests/acceptance/control_plane/test_wp_cp_040.py`; `uv run --group biotech pytest tests/integration/temporal -k replay -q`.

**Notes:** Heartbeats are throttled (default 30 s, `min(heartbeat_timeout*0.8, 60 s)`), so the persisted frames, not the heartbeat details, are the resume truth. Set `heartbeat_timeout=30s` and `start_to_close` 30 to 60 min per segment. Use `workflow.patched("ft-g2-segment-loop")` around the loop so in-flight histories from the single-activity model replay. Depends on `temporalio>=1.34` for `cancellation_details()` (FT-G7 may land first; if not, gate on `hasattr`). Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear.
