# [FT-G6] Cursor lane qualification fixtures and describe honesty tests

Linear: OVE-55

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** FT-G4, FT-G5
**Status:** ready-for-agent

**What to build:** The qualification that flips `lane_profile.qualified` for `cursor_local` and `cursor_cloud`: a recorded-fixture suite replayed in CI (local JSONL frame streams and cloud SSE streams for full, error, cancelled, busy, conflict, expired-stream and hook-deny cases, scrubbed of secrets and user email), a describe-honesty suite asserting every `describe()` cell against observed behavior, a replay suite over captured Temporal histories, and the live drill (`make lane-qualify PROFILE=...`) that runs Mission 3's first iteration locally and one cloud run on a throwaway repository under an owner-approved finite budget, measures cancel latency and busy behavior, records the UNVERIFIED items' outcomes, and writes the qualification record. Also removes the legacy `operation.execute` activity once `DeepAgentsHarness` passes the same acceptance proofs through `lane.turn`.

**Spec sections:** SPEC-07 Testing Decisions, Qualification fixtures and budget, §7 (describe honesty), Insertion points (`operation.execute` removal).

**Writable regions:** `tests/integration/cursor/`, `tests/unit/harness/`, `tests/integration/temporal/` (replay histories), `scripts/lane_qualify.py`, `Makefile` (`lane-qualify` target, integrator-reviewed), `src/mission_control/adapters/temporal/operation_activities.py` (removal), `docs/qualification/lanes/` (records).

**Acceptance criteria:**
- [ ] Fixture suite covers the listed cases for both profiles and runs under `make check` with no network and no credentials.
- [ ] Describe honesty: for each profile and each control, a test exercises the control against fixtures and asserts the reported value (`native`, `emulated`, `unsupported`) and the Delivery Report semantics match SPEC-07 §7.
- [ ] Replay suite: captured histories from FT-G2 and FT-G4 replay with `Replayer` across the current worker code.
- [ ] Live drill script runs only when `CURSOR_API_KEY` is set and `MC_PAID_BUDGET_USD` names the cap (small fixture runs default to the policy cap; the repeated drill needs the Linear approval comment URL); it records spent, reserved and unknown units in the qualification record.
- [ ] The qualification record (`docs/qualification/lanes/<profile>-<date>.md`) resolves each UNVERIFIED item from SPEC-07 (Windows sandbox, rules without `setting_sources`, `Run.request_id`, concurrent local send, cloud idempotency window, local `run.git`, REST `envVars`/`metadata`, retention value) as verified, refuted or still open.
- [ ] `lane_profile.qualified` is set true only by the drill's recorded success; CI fixtures never flip it.
- [ ] `operation.execute` and its registration are removed; the Deep Agents acceptance proofs pass through `lane.turn`.

**Verification:** `make check`; `uv run --group biotech pytest tests/integration/cursor tests/unit/harness -q`; `uv run --group biotech pytest tests/integration/temporal -k replay -q`; `make lane-qualify PROFILE=cursor_local` (live, only with approved budget).

**Notes:** This ticket records the live fixtures: small real runs are permitted by default; the repeated latency and busy drills need an approved cap in Linear. Stop and record if any live step produces an ambiguous paid effect (an agent created without a recorded id). Preserve the pre-removal `operation.execute` source and digest under `.scratch/fast-track-2026-10-07/G6/` for the removal guide.
