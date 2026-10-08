# [FT-F2] interrupt_and_inject per lane with uncertain-effect settlement

Linear: OVE-45

**Epic:** Control (SPEC-06, SPEC-07)
**Team:** T4
**Blocked by:** FT-F1, FT-G1
**Status:** ready-for-agent

**What to build:** The `interrupt_and_inject` command becomes real: the mission service stops rejecting it, the reducer admits it with an `InstructionPayload` (content ref, digest, boundary) and consults the lane's `describe().delivery_semantics`; `cooperative_inject` where a lane reports it natively (none of the three current profiles), otherwise `cancel_and_replace`: the operation workflow cancels the current segment (FT-G2 cancel path), every `tool_running` frame without a closing frame becomes an Uncertain Effect that must settle (receipt lookup or operator reconciliation) before the replacement turn, then the next segment runs with the injected item added to the Context Packet through the FT-F1 mailbox. The Delivery Report records the semantics actually used and the settled or pending effect ids. Works for Deep Agents via `DeepAgentsHarness` and is the path FT-G4 reuses for Cursor.

**Spec sections:** SPEC-07 §4.3 (cancellation), §7 (controls); SPEC-06 (command lifecycle, Delivery Report, mailbox); ADR-0032; workflow-types/09 §7 and §8.

**Writable regions:** `src/mission_control/adapters/temporal/workflows/operation.py` (inject branch), `src/mission_control/adapters/temporal/activities/lane_turn.py`, `src/mission_control/application/execution/harness/inject.py` (new), `tests/integration/temporal/test_lane_turn.py`. Shared, integrator-reviewed: `application/missions/service.py::_action` (remove the `interrupt_and_inject` rejection; FT-F1 removes `queue_instruction`), `domain/policies/contracts.py` (`InterruptAndInjectAction`), `adapters/temporal/boundary_commands.py`.

**Acceptance criteria:**
- [ ] `POST .../runs/{id}/commands` with `kind: interrupt_and_inject` returns 202 and a command record; the reducer action exists with required permissions.
- [ ] Lane dispatch: the semantics chosen equal `describe().delivery_semantics.interrupt_and_inject`; a lane reporting `unsupported` rejects with a typed Delivery Report, not an exception.
- [ ] `cancel_and_replace`: segment cancelled, provider cancel attempted, `lane.cancel` and `lane.status` run, Uncertain Effects enumerated from frames; the replacement segment starts only after every effect is settled or the unit is parked `in_doubt` with an incident.
- [ ] The replacement turn's packet contains the injected item at the declared boundary and the mailbox marks it consumed exactly once.
- [ ] Delivery Report rows record `delivery_semantics`, settled effect ids, pending effect ids and the replacement generation.
- [ ] Time-skipping tests: inject with no running tool (immediate replace), inject with a running tool that completes during cancel (settled), inject with a running tool that never closes (parked `in_doubt`), duplicate command id rejected.

**Verification:** `make check`; `uv run --group biotech pytest tests/integration/temporal/test_lane_turn.py -k inject -q`; `uv run --group biotech pytest -m common_db tests/acceptance/control_plane/test_rrm_007_api.py -q`.

**Notes:** Coordinate with T5 on the `_action` edit: FT-F1 lands the mailbox and removes the `queue_instruction` rejection; this ticket removes only the `interrupt_and_inject` line. No lane currently reports `cooperative_inject`; keep the branch and a fake-harness test so a future Codex lane uses it. Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear.
