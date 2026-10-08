# [FT-C2] Reducer derives turn facts from closing frames

Linear: OVE-35

**Epic:** Mission state (SPEC-03)
**Team:** T2
**Blocked by:** FT-C1
**Status:** ready-for-agent

**What to build:** Mission state now follows the provider's lifecycle through evidence. `application/frames/reducer.py::derive(closing_frames)` turns persisted closing frames into typed `FrameFacts` (session started, turn opened and closed with usage dispositions, tool effect settled or failed or denied, approval requested, compaction observed, execution outcome per workflow-types/05 §3.5, unknown state → in doubt), and the domain Reducer gains an `apply_frame_facts` action that writes the mission events `session.started`, `session.turn_started`, `session.turn_completed`, `session.compaction_observed`, `session.ended`, `tool_call.completed` and `attempt.completed`, each with `source.native_event_ref` pointing at the frame. Per-lane kind and dedupe tables live in one module so a new lane is new rows. Usage carries `settled | estimated | unknown` per dimension (Cursor cost `estimated` until `get_usage` settles). Demo: replaying the frames of C1's integration run through the reducer yields a deterministic event sequence whose `native_event_ref`s all resolve, and a provider `FINISHED` without a Completion Candidate does not produce `attempt.completed{succeeded}`.

**Spec sections:** SPEC-03 §Contracts (Usage disposition), §Implementation Decisions (Lifecycle synthesis table, rules 1-5), §Testing Decisions

**Writable regions:** `src/mission_control/application/frames/reducer.py`, `src/mission_control/application/frames/kinds.py` (extend), `tests/unit/frames/`, `tests/integration/postgres/`; shared (integrator-coordinated, additive): `src/mission_control/domain/policies/reducer.py` (`apply_frame_facts`), `src/mission_control/domain/policies/contracts.py` (`session.*`, `tool_call.*`, `attempt.completed` payload types and `FrameFacts`)

**Acceptance criteria:**
- [ ] `derive()` reads only frames with `closing = true`; a test feeds interleaved deltas and proves they change nothing.
- [ ] Every row of the SPEC-03 lifecycle synthesis table for `deep_agents` has a unit test (fixture frame → expected fact → expected event type and payload fields); `cursor_local` and `cursor_cloud` rows have fixture tests from the frame shapes in the research notes so G3/G5 plug in without reducer changes.
- [ ] 05 §3.5 mapping: `finished` → `succeeded` only with a Completion Candidate covering declared outputs (else `missing_output_policy` applies); `error` → `failed(provider_error)`; `expired` → `failed(timeout)`; `cancelled` caused by an admitted command → `cancelled(cancelled_by_command)`; unknown state → unit `in_doubt` through the existing reconciliation classification (no new path).
- [ ] Usage dispositions: Deep Agents tokens `settled`, cost `estimated`; Cursor cost `estimated` then flipped to `settled` by a later `usage` frame carrying `charged_cents`; `unknown` is never rendered as zero.
- [ ] Mission events carry references and digests only (no bodies); `source.native_event_ref` resolves to a stored frame for every emitted event (integration assertion).
- [ ] Frames of a non-current generation produce no facts (test).
- [ ] Determinism: replaying the same closing frames in arrival order yields byte-identical event payloads (excluding `event_id`, `recorded_at`).
- [ ] `apply_frame_facts` is the only place lifecycle fields change as a result of frames; `required_action_permissions` covers the new action; existing reducer tests pass unchanged.
- [ ] `make check` passes.

**Verification:** `make check`; `uv run pytest tests/unit/frames tests/unit/run_control -q`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k "frames and reducer" -q`

**Notes:** Do not add Temporal or provider imports to `domain/policies`; `FrameFacts` are plain typed records. The `approval_requested` fact sets phase `awaiting_human`; opening the Human Task remains the family's job. Coordinate the `attempt.completed` payload with T4 (G2 writes closing facts from `lane.turn`) so both sides agree on field names before either lands.
