# [FT-F4] Fork from snapshot with queued instruction on CLI, HTTP and MCP

Linear: OVE-47

**Epic:** Control (SPEC-06)
**Team:** T5
**Blocked by:** FT-B1, FT-F1
**Status:** ready-for-agent

**What to build:** `missionctl run fork RUN --from-snapshot ID --instruction-file FILE` (and `POST /runs/{id}/forks`, MCP `mission_run_fork`) takes or reuses a safe-boundary Snapshot through the existing fork saga, admits a new Run whose first Context Packet restores the Snapshot as its `workspace` tier item, writes the optional instruction as the new Run's first mailbox entry, records lineage (`mission_relationship` kind `fork` plus the `mc_forked_from_run_id` and `mc_forked_from_snapshot_id` search attributes), and leaves the source Run's mailbox, children and in-flight Commands untouched. `run list --query` finds the branch.

**Spec sections:** SPEC-06 "Fork", "Contracts" (`ForkPayload`), "Interfaces"; SPEC-02 (`workspace` tier); ADR-0031 (forks are new runs, never Temporal reset).

**Writable regions:** `src/mission_control/interfaces/http/mission_control.py` (fork route), `interfaces/cli/main.py` (`run fork` flags, integrator-coordinated), `application/recovery/run_forks.py` (instruction and lineage), `interfaces/mcp/coordinator_server.py` (new tool only). Shared: `contracts/contracts.py` (`ForkPayload`).

**Acceptance criteria:**
- [ ] Fork without `from_snapshot_id` uses the latest safe Snapshot; a Run with none is rejected with `CHECKPOINT_INVALID`.
- [ ] The new Run's first packet contains the Snapshot as a `workspace` item and the instruction as its first mailbox entry (state `queued`).
- [ ] The source Run's mailbox entries, async children and in-flight Commands are not copied (assert counts before and after).
- [ ] Lineage is recorded in `mission_relationship` and visible in `run inspect` of both Runs; search attributes are upserted on the new root (requires FT-G7's registration; skip-marked until then).
- [ ] CLI, HTTP and MCP produce identical results for the same request (contract parity test).
- [ ] Idempotent retry with the same `request_id` returns the same forked Run.
- [ ] `make check` passes; acceptance test on real PostgreSQL and Temporal reuses the StageGraph source-and-fork proof.

**Verification:** `make check`; `uv run --group biotech pytest -m common_db tests/unit/run_control/test_mission_control_runtime.py tests/acceptance/mission_control/test_postgres_runtime_parity.py -q`.

**Notes:** Forking launches nothing by itself; `run start` remains the separate authorized call (existing protocol). No paid provider call.
