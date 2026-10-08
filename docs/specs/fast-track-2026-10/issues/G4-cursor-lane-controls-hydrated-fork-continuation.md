# [FT-G4] Cursor lane controls: cancel, wait_then_send, cancel_and_replace, hydrated fork and continuation

Linear: OVE-53

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** FT-G3, FT-B4, FT-F2
**Status:** ready-for-agent

**What to build:** Every control in SPEC-07 §7 for `cursor_local`, with Delivery Reports that say what happened: `cancel_turn` native through `run.cancel()`; `send_turn` and `queue_instruction` as `wait_then_send` (the mailbox item rides the next send); `interrupt_and_inject` as `cancel_and_replace` using the FT-F2 path (cancel, settle any `tool_call{running}` without completion as an Uncertain Effect, replacement turn with the injected packet item); `pause` reported `unsupported` mid-run and applied only at run boundaries; `snapshot` as git patch plus untracked files plus `.mission/` digests; `fork` as a new run whose `prepare` restores the patch into a fresh lease and creates a new agent hydrated from the packet's `workspace` tier; `request_continuation` sealing a Continuation Checkpoint (FT-B4) and hydrating a fresh agent. Includes the `missing_output_policy` follow-up turn through the `stop` hook's `followup_message`.

**Spec sections:** SPEC-07 §4.3, §7, §8, Further Notes; SPEC-06 (Delivery Report, commands); SPEC-02 (packet `workspace` tier, Continuation Checkpoint).

**Writable regions:** `src/mission_control/adapters/cursor/{local.py,controls.py,snapshot.py}`, `tests/integration/cursor/`, `tests/unit/harness/`. Shared, integrator-reviewed: none expected; if `application/recovery/run_forks.py` needs a lane hook for restoring a patch, coordinate with the integrator.

**Acceptance criteria:**
- [ ] `cancel_turn` cancels the run, `lane.cancel` is idempotent on a finished run (`run_not_cancellable` or no-op), and the Attempt settles `cancelled(cancelled_by_command)`; usage recorded from the last `TurnEndedUpdate`.
- [ ] A queued instruction is consumed exactly once at the next send; Delivery Report `wait_then_send`; a second queued item waits for the following boundary.
- [ ] `interrupt_and_inject`: running tool call without completion becomes an Uncertain Effect requiring settlement before the replacement turn; the replacement turn's packet contains the injected item; Delivery Report `cancel_and_replace`.
- [ ] `pause` mid-run returns a typed rejection and `describe()` says `unsupported`; a pause at the run boundary stops the next segment from starting.
- [ ] `snapshot` manifest lists patch digest, untracked files, `.mission/` digests and the native refs; `fork` from it produces a new run whose first segment runs in a fresh lease containing the restored files and a new `agent_id`; the source run's mailbox is not cloned.
- [ ] `request_continuation` seals a checkpoint and the next segment runs a new agent hydrated from it; `session.transferred` is emitted.
- [ ] Follow-up turn: a run ending without a Completion Candidate gets exactly one follow-up asking for declared outputs, then `not_accepted(outputs_missing)`.
- [ ] Describe honesty test passes for every cell of the §7 table.

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/harness -k cursor -q`; `uv run --group biotech pytest tests/integration/cursor -q`; `uv run --group biotech pytest tests/integration/temporal/test_lane_turn.py -k "inject or fork or continuation" -q`.

**Notes:** `Agent.resume` continues the same Agent Session within one attempt only; fork and continuation always create a new agent. Local `run.git` population is UNVERIFIED; compute the patch yourself. Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear. `CURSOR_API_KEY` is set; record one real local run for the fixtures.
