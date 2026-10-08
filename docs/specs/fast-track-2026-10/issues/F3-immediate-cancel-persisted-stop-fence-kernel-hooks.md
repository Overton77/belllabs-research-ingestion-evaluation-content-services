# [FT-F3] Immediate cancel with persisted stop fence consulted by kernel hooks

Linear: OVE-46

**Epic:** Control (SPEC-06, SPEC-07)
**Team:** T4
**Blocked by:** FT-G1
**Status:** ready-for-agent

**What to build:** `cancel` with `urgency: immediate` becomes real per ADR-0008 and ADR-0032: the mission service stops rejecting it, the command handler's activity first persists a Stop Fence row for the run and generation (migration 0029 table owned by T5; this ticket adds the domain model and the write if T5 has not landed, coordinated through the integrator), then the operation workflow runs the FT-G2 cancel path. Kernel Hooks consult the fence before allowing any side effect: for Deep Agents through the `HookScriptMiddleware`'s `wrap_tool_call` kernel layer, for Cursor through the hook callback endpoint (FT-G3), so new effects are refused immediately even before the provider acknowledges. The Delivery Report distinguishes requested, fence persisted, provider acknowledged and settled.

**Spec sections:** SPEC-07 §4.3 step 5, §5.5 (callback consults the fence), §7 (`cancel` immediate row); SPEC-06 (Stop Fence, command lifecycle); ADR-0008; ADR-0032.

**Writable regions:** `src/mission_control/domain/policies/stop_fence.py` (new), `src/mission_control/adapters/postgres/run_control/stop_fence.py` (new), `src/mission_control/adapters/temporal/workflows/operation.py` (immediate branch), `src/mission_control/adapters/deep_agents/kernel_hooks.py` (fence check; file created by FT-A5, add the check), `tests/integration/postgres/test_stop_fence.py`. Shared, integrator-reviewed: `application/missions/service.py::_action` (remove the immediate-cancel rejection), `domain/policies/contracts.py`, `migrations/0029_*` (T5 owns the file; this ticket contributes the `stop_fence` DDL through the integrator).

**Acceptance criteria:**
- [ ] `POST .../runs/{id}/commands` with `kind: cancel, payload.urgency: immediate` returns 202; the command is admitted with the `mission.admin` requirement when side-effecting work is active (spec §9) and `mission.command` otherwise.
- [ ] A `stop_fence` row exists before any provider cancel is attempted; it is keyed by run and generation and is never deleted (superseded rows stay for audit).
- [ ] Deep Agents kernel layer: a tool call arriving after the fence is denied with a `fenced` reason and recorded as a frame; no Operation Intent is written.
- [ ] Cursor callback: a permission hook arriving after the fence returns `deny`; the frame records `fenced`.
- [ ] After the fence, the FT-G2 cancel path runs; the Delivery Report carries timestamps for requested, fence persisted, provider acknowledged, settled.
- [ ] A fence race test: a tool admission and a fence write committed concurrently never both succeed for the same effect id.
- [ ] Normal-urgency cancel behavior is unchanged (no fence written).

**Verification:** `make check`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_stop_fence.py -q`; `uv run --group biotech pytest tests/integration/temporal/test_lane_turn.py -k immediate -q`; `uv run --group biotech pytest tests/unit/deep_agents -k fence -q`.

**Notes:** `stop_now` guarantees no new effects, not that dispatched remote tools halt (ADR-0008); say so in the Delivery Report note. Hook callback and Deep Agents kernel hook files come from FT-G3 and FT-A5; if they are not merged yet, implement the fence check behind a port and add the call sites in those tickets. Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear.
