# [FT-F6] Inspection enrichment: lane, sessions, mailbox, delivery reports

Linear: OVE-49

**Epic:** Control (SPEC-06)
**Team:** T5
**Blocked by:** FT-C2
**Status:** ready-for-agent

**What to build:** `missionctl run inspect RUN --json` (and `GET /runs/{id}/inspection`, MCP `mission_run_inspect`) returns the existing `mc.inspection.v1` body plus optional sections `lane` (profile, describe digest, native refs), `sessions` (agent sessions with turn counts and usage disposition), `mailbox` (pending and consumed entries, reference-only), `delivery_reports` (requested and delivered semantics per Command), `frames_cursor` (transcript cursor for `run transcript --since`), `chain` (chain membership) and `subscriptions` (active count). `run inspect --wait` returns early on lifecycle, phase or terminal change. `run list --query` runs a Temporal visibility query over the registered search attributes.

**Spec sections:** SPEC-06 "Inspection", "Contracts" (`MissionInspection` additions), "Interfaces"; SPEC-03 (session and turn facts, transcript cursor); ADR-0031 (search attributes).

**Writable regions:** `src/mission_control/interfaces/http/mission_control.py` (inspection route), `application/missions/runtime.py` (inspection assembly), `interfaces/cli/main.py` (`run list`; integrator-coordinated). Shared: `contracts/contracts.py` (`MissionInspection`).

**Acceptance criteria:**
- [ ] Existing clients parsing `mc.inspection.v1` keep working (all new keys optional; contract snapshot test).
- [ ] `sessions` and `frames_cursor` are derived from the `session_turn` and `provider_frame` records written by FT-C1 and FT-C2, never from provider calls.
- [ ] `mailbox` lists entries with kind, boundary, state, sequence and digest only.
- [ ] `delivery_reports` shows requested and delivered semantics, outcome and native refs for every Command on the Run.
- [ ] `run inspect --wait 30` returns within one second of a phase change in an integration test.
- [ ] `run list --query "mc_lane='cursor_local' AND mc_phase='executing'"` returns matching runs (depends on FT-G7's registered attributes; test with the local Temporal dev server).
- [ ] `make check` passes.

**Verification:** `make check`; `uv run --group biotech pytest -m common_db tests/acceptance/control_plane/test_rrm_007_api.py tests/unit/run_control -q`.

**Notes:** `run search RUN --query` over the Transcript ships with FT-C3; this ticket only exposes the cursor. No paid provider call.
